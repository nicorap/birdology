"""Convert a GBIF SIMPLE_CSV archive to Parquet once, as raw text.

The eBird (EOD) archive is ONE 234.6 GB deflate member (949.7 GB of CSV,
1.78e9 rows). Inflating it runs at ~307 MB/s on one core (job 12539), so every
read of the zip costs ~52 min before any parsing, GPU or not, and the
row-by-row ``csv.DictReader`` of :func:`occurrences.read_occurrences` made it
4.4 h. Converting it once lets later builds read only the columns they need,
multi-threaded, and keeps the extra columns rarity flagging will want.

VALUES STAY RAW TEXT
--------------------
Nothing is parsed here: the drop rules (species key, coordinates,
day-precision date) stay in ``occurrences`` and apply to Parquet input
unchanged, and an empty source field stays ``""``, not null. Text is decoded
the way ``read_occurrences`` decodes it, UTF-8 with invalid bytes replaced by
U+FFFD, so the leakage guard's replacement-character count is the same on
either input. (Tabs and newlines are ASCII, so an invalid sequence never
straddles a field: decoding per field equals decoding the stream.)

LINES THE ARROW READER CANNOT HOLD ARE KEPT, NOT DROPPED
--------------------------------------------------------
A line whose field count differs from the header is not a row to Arrow's CSV
reader, while ``csv.DictReader`` keeps it (a short line pads with ``None``, a
long one's extra fields are set aside). Such lines go verbatim to
``rejects.parquet``, so Parquet rows plus rejects equal the rows
``read_occurrences`` iterates, which the manifest records and the
conversion job checks.
"""
from __future__ import annotations

import json
import logging
import time
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from birdology.prior.occurrences import REQUIRED_SOURCE_COLUMNS, _occurrence_member

logger = logging.getLogger(__name__)

#: Kept columns: what the prior reads, plus what rarity flagging is likely to
#: need (place names, counts, coordinate uncertainty, record type, GBIF issue
#: flags and the record ids to trace a flag back to its source).
KEEP_COLUMNS = (
    "gbifID",
    "datasetKey",
    "publishingOrgKey",
    "speciesKey",
    "species",
    "countryCode",
    "stateProvince",
    "decimalLatitude",
    "decimalLongitude",
    "coordinateUncertaintyInMeters",
    "eventDate",
    "occurrenceStatus",
    "individualCount",
    "basisOfRecord",
    "recordedBy",
    "issue",
)

MANIFEST = "manifest.json"
REJECTS = "rejects.parquet"


def _as_text(col: pa.ChunkedArray) -> pa.ChunkedArray:
    """Binary -> UTF-8 string, invalid bytes -> U+FFFD (as ``errors="replace"``)."""
    try:
        return col.cast(pa.string())
    except pa.ArrowInvalid:
        return pa.chunked_array(
            [pa.array([None if v is None else v.decode("utf-8", "replace") for v in col.to_pylist()], pa.string())]
        )


def convert_archive(
    zip_path: Path,
    out_dir: Path,
    *,
    rows_per_file: int = 100_000_000,
    row_group_rows: int = 1_000_000,
    block_size: int = 64 << 20,
) -> dict:
    """Write ``out_dir/part-NNNNN.parquet`` (+ rejects, manifest); return the manifest."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if any(out_dir.glob("part-*.parquet")):
        raise FileExistsError(f"{out_dir} already holds part files; convert into an empty directory")
    t0 = time.time()
    rejects: list[str] = []

    def on_invalid(row) -> str:
        rejects.append(row.text)
        return "skip"

    with zipfile.ZipFile(zip_path) as zf:
        member = _occurrence_member(zf)
        info = zf.getinfo(member)
        with zf.open(member) as raw:
            header = raw.readline().decode("utf-8", "replace").rstrip("\r\n").split("\t")
        missing_required = sorted(REQUIRED_SOURCE_COLUMNS - set(header))
        if missing_required:
            raise ValueError(f"occurrence table {member} is missing columns: {missing_required}")
        keep = [c for c in KEEP_COLUMNS if c in header]
        absent = [c for c in KEEP_COLUMNS if c not in header]
        if absent:
            logger.warning("%s lacks %s; they are left out of the Parquet", member, absent)

        with zf.open(member) as raw:
            reader = pcsv.open_csv(
                raw,
                read_options=pcsv.ReadOptions(block_size=block_size, use_threads=True),
                parse_options=pcsv.ParseOptions(
                    delimiter="\t", quote_char=False, escape_char=False, invalid_row_handler=on_invalid
                ),
                convert_options=pcsv.ConvertOptions(
                    include_columns=keep, column_types={c: pa.binary() for c in keep}
                ),
            )
            schema = pa.schema([(c, pa.string()) for c in keep])
            part, in_file, rows, pending, n_pending = 0, 0, 0, [], 0
            writer = None
            parts = []

            def flush():
                nonlocal writer, part, in_file, pending, n_pending
                if not pending:
                    return
                t = pa.Table.from_batches(pending)
                t = pa.table([_as_text(t.column(c)) for c in keep], schema=schema)
                if writer is None:
                    path = out_dir / f"part-{part:05d}.parquet"
                    writer = pq.ParquetWriter(path, schema, compression="zstd", use_dictionary=True)
                    parts.append(path.name)
                writer.write_table(t)
                in_file += t.num_rows
                pending, n_pending = [], 0
                if in_file >= rows_per_file:
                    writer.close()
                    writer, part, in_file = None, part + 1, 0

            for batch in reader:
                pending.append(batch)
                n_pending += batch.num_rows
                rows += batch.num_rows
                if n_pending >= row_group_rows:
                    flush()
            flush()
            if writer is not None:
                writer.close()

    pq.write_table(pa.table({"text": pa.array(rejects, pa.string())}), out_dir / REJECTS)
    manifest = {
        "source": str(zip_path),
        "member": member,
        "member_crc32": f"{info.CRC:08x}",
        "member_compressed_bytes": info.compress_size,
        "member_bytes": info.file_size,
        "columns": keep,
        "absent_columns": absent,
        "rows": rows,
        "rejected_lines": len(rejects),
        "source_rows": rows + len(rejects),
        "parts": parts,
        "pyarrow": pa.__version__,
        "seconds": round(time.time() - t0, 1),
    }
    (out_dir / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n")
    logger.info("converted %s: %d rows in %d part files, %d rejected lines, %.0f s",
                member, rows, len(parts), len(rejects), manifest["seconds"])
    return manifest
