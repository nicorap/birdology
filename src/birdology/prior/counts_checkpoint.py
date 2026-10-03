"""Checkpoint the GUARDED counts so a re-run skips the parse AND the guards.

WHY (fleet job 6481): the sequential path (``--sequential``, the one that
completes on the global EOD archive) writes no parse checkpoint, so every
iteration re-paid ~4.5h of parse plus ~2h of guards per resolution before
reaching the stage that was being changed. The parse checkpoint
(``checkpoint.py``) would not help even where it exists: it sits BEFORE the
guards, which then have to run again.

WHAT IS PERSISTED: exactly what ``build_from_archive`` feeds
``target_group_ratios`` -- ``aggregate_counts(guarded.table)``, the
(species_key, cell, week, n) table carrying the leakage-guard marker -- one
file per (source, resolution), written once both escalation clauses have
passed. Everything after it (ratios, backoff, smoothing, the restriction to a
query set, the controls) is recomputed on resume, so changing the query set
or the shrinkage constants costs minutes.

STALENESS FAILS CLOSED AND NAMES THE FIELD, the discipline of
``checkpoint.py``: the identity is (source, archive, resolution, split sha256,
contributor mode). A different split or contributor mode is a different
guard, so its counts are a different dataset; serving them would put one
guard's survivors under another's provenance. ``StaleCheckpointError`` is
reused so one ``except``-free refusal path covers both checkpoints.

THE GUARD MARKER IS PROPAGATED, NOT MINTED. The write REFUSES a table that
does not carry it, and parquet round-trips Arrow schema metadata, so the
loaded table carries the marker because the written one did.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .checkpoint import StaleCheckpointError
from .leakage import guards_applied

#: Bumped when the on-disk shape changes; an unknown version refuses.
COUNTS_CHECKPOINT_SCHEMA_VERSION = 1

#: Parquet schema-metadata key carrying the provenance blob.
METADATA_KEY = b"birdology_counts_checkpoint"

#: The guard counts recorded beside the table, by name.
GUARD_FIELDS = (
    "banned_key_count",
    "matched_key_count",
    "xeno_canto_rows_removed",
    "contributor_rows_removed",
    "rows_before_contributor_guard",
    "rows_after_guards",
)


@dataclass(frozen=True)
class CountsIdentity:
    """What makes two guarded-count tables the same table."""

    source: str
    archive: str
    resolution: int
    split_sha256: str | None
    contributor_mode: str


def counts_checkpoint_path(out_dir: Path, source: str, resolution: int) -> Path:
    """Where (source, resolution)'s guarded counts live inside *out_dir*."""
    return Path(out_dir) / f"guarded_counts_{source}_r{int(resolution)}.parquet"


def write_counts_checkpoint(
    path: Path, counts: pa.Table, *, identity: CountsIdentity, guard: dict
) -> Path:
    """Write *counts* with its identity and guard counts. Atomic replace."""
    if not guards_applied(counts):
        raise ValueError(
            "refusing to checkpoint counts that do not carry the leakage-guard "
            "marker: a resumed build would hand them to target_group_ratios as "
            "if the guards had run"
        )
    missing = [f for f in GUARD_FIELDS if f not in guard]
    if missing:
        raise ValueError(f"guard record is missing {missing}")
    meta = {
        "schema_version": COUNTS_CHECKPOINT_SCHEMA_VERSION,
        "identity": asdict(identity),
        "guard": {f: int(guard[f]) for f in GUARD_FIELDS},
    }
    metadata = dict(counts.schema.metadata or {})
    metadata[METADATA_KEY] = json.dumps(meta).encode("utf-8")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(counts.replace_schema_metadata(metadata), tmp)
    os.replace(tmp, path)
    return path


def _refuse(path: Path, field: str, recorded, requested) -> None:
    raise StaleCheckpointError(
        f"guarded-counts checkpoint {path} is stale: its {field} is "
        f"{recorded!r} but this invocation needs {requested!r}. The counts are "
        "the survivors of ONE guard over ONE archive at ONE resolution; "
        "resuming from another's would put a different dataset under this "
        "build's provenance. Drop --resume-counts to re-parse, or re-point "
        "the invocation at what the checkpoint records"
    )


def load_counts_checkpoint(
    path: Path, *, identity: CountsIdentity
) -> tuple[dict, pa.Table]:
    """``(meta, counts)``, refusing a stale or foreign checkpoint loudly."""
    path = Path(path)
    if not path.exists():
        raise ValueError(
            f"no guarded-counts checkpoint at {path}; --resume-counts needs the "
            "file a previous run of this build wrote into --out after its "
            "guards passed. Run once without --resume-counts to create it"
        )
    table = pq.read_table(path)
    raw = (table.schema.metadata or {}).get(METADATA_KEY)
    if raw is None:
        raise StaleCheckpointError(
            f"{path} carries no {METADATA_KEY.decode()} metadata; it was not "
            "written by write_counts_checkpoint and cannot attest to its guard"
        )
    meta = json.loads(raw.decode("utf-8"))
    version = meta.get("schema_version")
    if version != COUNTS_CHECKPOINT_SCHEMA_VERSION:
        _refuse(path, "schema_version", version, COUNTS_CHECKPOINT_SCHEMA_VERSION)
    recorded = meta.get("identity") or {}
    for field, wanted in asdict(identity).items():
        if recorded.get(field) != wanted:
            _refuse(path, field, recorded.get(field), wanted)
    if not guards_applied(table):
        raise StaleCheckpointError(
            f"{path} has lost its leakage-guard marker; it cannot be handed to "
            "target_group_ratios as guarded counts"
        )
    metadata = {k: v for k, v in table.schema.metadata.items() if k != METADATA_KEY}
    return meta, table.replace_schema_metadata(metadata)
