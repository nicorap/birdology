"""The context manifest: Phase B's only contract with the corpus.

One row per usable recording, carrying where and when it was recorded, its
label, and its split. Phase B reads this and the embedding files; it never
opens a tar shard.

Every drop is COUNTED and the counts travel in the table's own metadata. A
manifest that silently lost 40% of a split looks identical to a complete one,
and the coverage a consumer got is not something they should have to re-derive.

The metadata also carries `distinct_weeks` — the number of distinct ISO weeks
among kept rows. A station id that is itself an in-window plausible date is
undetectable per-file, but it collapses every row onto one week; the driver
can refuse on low spread only if the spread travels with the table.
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import date
from enum import StrEnum

import h3
import pyarrow as pa

from ..prior.indexing import iso_week
from .soundscape_dates import UndatableFilename, parse_filename_date

MANIFEST_COLUMNS = (
    "filepath",
    "cell",
    "week",
    "ebird_code",
    "split",
    "lat",
    "lon",
)

log = logging.getLogger(__name__)


class DropReason(StrEnum):
    NO_DATE = "no_date"
    NO_COORDINATES = "no_coordinates"
    UNDATABLE_FILENAME = "undatable_filename"
    BAD_CELL = "bad_cell"


_SCHEMA = pa.schema(
    [
        ("filepath", pa.string()),
        ("cell", pa.string()),
        ("week", pa.int8()),
        ("ebird_code", pa.string()),
        ("split", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
    ]
)


def _xc_id(row: dict) -> int | None:
    raw = row.get("id")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    return None


def build_manifest(
    metadata: pa.Table,
    *,
    dates: dict[int, date] | None,
    resolution: int,
    split: str,
) -> tuple[pa.Table, dict[DropReason, int]]:
    """Join BirdSet metadata to (cell, week).

    `dates` is the XC id -> date map for a FOCAL split. Pass None for a
    SOUNDSCAPE split, whose dates come from the filename.
    """
    cols: dict[str, list] = {name: [] for name in MANIFEST_COLUMNS}
    drops: Counter[DropReason] = Counter()

    for row in metadata.to_pylist():
        path = row.get("filepath") or ""

        if dates is None:
            try:
                when = parse_filename_date(path)
            except UndatableFilename:
                drops[DropReason.UNDATABLE_FILENAME] += 1
                continue
        else:
            xc = _xc_id(row)
            when = dates.get(xc) if xc is not None else None
            if when is None:
                drops[DropReason.NO_DATE] += 1
                continue

        lat, lon = row.get("lat"), row.get("long")
        if lat is None or lon is None:
            drops[DropReason.NO_COORDINATES] += 1
            continue

        try:
            cell = h3.latlng_to_cell(float(lat), float(lon), resolution)
        except (ValueError, TypeError):
            drops[DropReason.BAD_CELL] += 1
            continue

        cols["filepath"].append(path)
        cols["cell"].append(cell)
        cols["week"].append(iso_week(when))
        cols["ebird_code"].append(row.get("ebird_code") or "")
        cols["split"].append(split)
        cols["lat"].append(float(lat))
        cols["lon"].append(float(lon))

    table = pa.table([cols[n] for n in MANIFEST_COLUMNS], schema=_SCHEMA)

    n_in = metadata.num_rows
    n_out = table.num_rows
    meta = {
        "input_rows": str(n_in),
        "kept_rows": str(n_out),
        "coverage": f"{(n_out / n_in) if n_in else 0.0:.6f}",
        "resolution": str(resolution),
        "split": split,
        "distinct_weeks": str(len(set(cols["week"]))),
    }
    for reason in DropReason:
        meta[f"dropped_{reason.value}"] = str(drops.get(reason, 0))

    log.info(
        "manifest %s: kept %d/%d (%.1f%%), drops=%s",
        split, n_out, n_in, 100 * (n_out / n_in if n_in else 0), dict(drops),
    )
    return table.replace_schema_metadata(meta), dict(drops)
