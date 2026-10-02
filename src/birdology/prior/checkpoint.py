"""Persist the expensive parse so a guard refusal costs minutes, not hours.

WHY (job 5345): the parse of a 219 GB archive took ~6.5h and the guard phase
~2.5h more -- and then the zero-match guard correctly refused, discarding
all of it. The refusal must stay (an unguarded artifact is worse than a
failed build); the WASTE must not. So the aggregated counts are written to
``--out`` BEFORE the guards run, and ``--resume`` starts the next
invocation at the guards.

FORMAT: one parquet file holding every resolution's PARSE_SCHEMA table
(stacked under an extra ``resolution`` column), with a JSON metadata blob in
the parquet schema metadata carrying schema version, source, archive path,
resolutions, the drop counters and the total row count. Parquet rather than
npz because the payload IS an Arrow table -- one serialisation, no dtype
round-trip through numpy object arrays for the nullable string columns.

STALENESS FAILS CLOSED AND NAMES THE FIELD. A checkpoint written for a
different archive, source or schema version is a parse of something else;
silently serving it would put another dataset's counts under this build's
provenance. ``StaleCheckpointError`` says WHICH field disagreed, because the
operator's next move differs per field (wrong archive: point --archive at
the file the checkpoint names, or delete the checkpoint; wrong version:
re-parse).

A checkpoint carrying MORE resolutions than requested serves the subset:
the data is a superset and a refusal would force a re-parse for nothing. A
checkpoint carrying FEWER refuses, naming the missing resolution.
"""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .arrow_strings import canonical_dictionary
from .parallel_parse import PARSE_SCHEMA, DROP_REASONS, TEXT_COLUMNS, ParseResult

#: Bumped when the checkpoint SHAPE changes. A reader that half-understands
#: a newer checkpoint would resume from fields that happened to parse, so an
#: unknown version refuses rather than guesses -- same policy as the build
#: manifest's ``MANIFEST_VERSION``.
#:
#: 2 (job 5722): the text columns are ``dictionary<int32, large_string>``,
#: not ``pa.string()``. A version-1 checkpoint is REFUSED as stale rather
#: than cast up: none can exist for the fleet run that needed this (it died
#: mid-parse), and casting a fleet-sized ``pa.string`` column is exactly
#: the operation whose offsets overflowed.
CHECKPOINT_SCHEMA_VERSION = 2

#: The parquet schema-metadata key the provenance blob lives under.
METADATA_KEY = b"birdology_parse_checkpoint"

#: The stacked on-disk schema: PARSE_SCHEMA plus the resolution column.
_DISK_SCHEMA = pa.schema(
    [("resolution", pa.int8()), *PARSE_SCHEMA]
)


class StaleCheckpointError(ValueError):
    """The checkpoint on disk is not the parse this invocation needs.

    A ``ValueError`` subclass per the house error contract. Always names the
    mismatched FIELD and both values, because 'stale checkpoint' alone sends
    the operator diffing two 200-byte JSON blobs at the end of a failed
    multi-hour job.
    """


def checkpoint_path(out_dir: Path, source: str) -> Path:
    """Where *source*'s parse checkpoint lives inside *out_dir*.

    Named by SOURCE so an ``eod`` and a ``dof`` build sharing one output
    directory cannot serve each other's parse.
    """
    return Path(out_dir) / f"parse_checkpoint_{source}.parquet"


def write_checkpoint(
    path: Path,
    result: ParseResult,
    *,
    source: str,
    archive: Path,
    resolutions: list[int],
) -> Path:
    """Write *result* with enough provenance to detect a stale read.

    The archive is recorded as a RESOLVED path string: the staleness check
    compares identities, and ``occ.zip`` from two working directories must
    not alias. Written via a temp file + ``os.replace`` so a crash mid-write
    leaves either the old checkpoint or none -- never a truncated parquet
    that a resume would fail on with a parquet decode error instead of a
    clear absence.
    """
    import os

    requested = sorted(int(r) for r in resolutions)
    missing = sorted(set(requested) - set(result.tables))
    if missing:
        raise ValueError(
            f"cannot checkpoint resolutions {missing}: the parse result does "
            f"not contain them (it has {sorted(result.tables)})"
        )

    pieces = []
    for resolution in requested:
        table = result.tables[resolution]
        pieces.append(
            table.add_column(
                0,
                "resolution",
                pa.array([resolution] * table.num_rows, pa.int8()),
            )
        )
    stacked = (
        pa.concat_tables(pieces)
        if pieces
        else _DISK_SCHEMA.empty_table()
    )
    meta = {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "source": source,
        "archive": str(Path(archive).resolve()),
        "resolutions": requested,
        "dropped": {reason: result.dropped[reason] for reason in DROP_REASONS},
        "total_rows_read": result.total,
    }
    stacked = stacked.replace_schema_metadata(
        {METADATA_KEY: json.dumps(meta).encode("utf-8")}
    )
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(stacked, tmp)
    os.replace(tmp, path)
    return path


def _refuse(path: Path, field: str, recorded, requested) -> None:
    raise StaleCheckpointError(
        f"checkpoint {path} is stale: its {field} is {recorded!r} but this "
        f"invocation needs {requested!r}. A checkpoint is the parse of ONE "
        "archive for ONE source; resuming from another's would put a "
        "different dataset's counts under this build's provenance. Delete "
        "the checkpoint (or drop --resume) to re-parse, or re-point the "
        "invocation at what the checkpoint records"
    )


def load_checkpoint(
    path: Path,
    *,
    source: str | None = None,
    archive: Path | None = None,
    resolutions: list[int] | None = None,
) -> tuple[dict, dict[int, pa.Table]]:
    """Read a checkpoint, refusing a stale one loudly.

    Returns ``(meta, tables)`` with ``tables`` keyed by int resolution.
    Each validation keyword is OPTIONAL and checked only when supplied, so
    tests (and tooling) can read a checkpoint's contents without asserting
    an invocation against it -- but ``main`` passes all three.

    The schema version is checked FIRST and unconditionally: fields of an
    unknown layout cannot be trusted to mean what their names say, so
    comparing them before the version check would validate against noise.
    """
    path = Path(path)
    if not path.exists():
        raise ValueError(
            f"no parse checkpoint at {path}; --resume needs the checkpoint a "
            "previous run of the SAME invocation wrote. Run once without "
            "--resume to create it"
        )
    stacked = pq.read_table(path)
    raw = (stacked.schema.metadata or {}).get(METADATA_KEY)
    if raw is None:
        raise StaleCheckpointError(
            f"checkpoint {path} carries no {METADATA_KEY.decode()} metadata; "
            "it was not written by write_checkpoint and cannot attest to "
            "which archive it parsed"
        )
    meta = json.loads(raw.decode("utf-8"))

    version = meta.get("schema_version")
    if version != CHECKPOINT_SCHEMA_VERSION:
        _refuse(path, "schema_version", version, CHECKPOINT_SCHEMA_VERSION)
    if source is not None and meta.get("source") != source:
        _refuse(path, "source", meta.get("source"), source)
    if archive is not None:
        recorded = meta.get("archive")
        requested = str(Path(archive).resolve())
        if recorded != requested:
            _refuse(path, "archive", recorded, requested)
    recorded_resolutions = [int(r) for r in meta.get("resolutions", [])]
    if resolutions is not None:
        missing = sorted(set(int(r) for r in resolutions)
                         - set(recorded_resolutions))
        if missing:
            _refuse(
                path, "resolutions", recorded_resolutions,
                sorted(int(r) for r in resolutions),
            )

    tables: dict[int, pa.Table] = {}
    column = stacked.column("resolution")
    for resolution in recorded_resolutions:
        import pyarrow.compute as pc

        mask = pc.equal(column, pa.scalar(resolution, pa.int8()))
        selected = (
            stacked.filter(mask)
            .drop_columns(["resolution"])
            .replace_schema_metadata(None)
        )
        # RE-CANONICALISE, never `.cast(PARSE_SCHEMA)`. Parquet merges the
        # resolutions' dictionaries into one per row group on write and
        # reads them back as `dictionary<int32, string>`, one dictionary per
        # row group: a resolution's slice then carries other resolutions'
        # values and possibly several dictionaries (which Arrow's group_by
        # refuses). `canonical_dictionary` restores the parse's exact
        # encoding -- sorted, used-only, shared -- chunk by chunk, so no
        # step concatenates the text into one 32-bit-offset buffer.
        columns = [
            canonical_dictionary(selected.column(name))
            if name in TEXT_COLUMNS
            else selected.column(name).cast(field.type)
            for name, field in zip(PARSE_SCHEMA.names, PARSE_SCHEMA)
        ]
        tables[resolution] = pa.Table.from_arrays(columns, schema=PARSE_SCHEMA)
    return meta, tables
