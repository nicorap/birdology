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
    keep_undated: bool = False,
) -> tuple[pa.Table, dict[DropReason, int]]:
    """Join BirdSet metadata to (cell, week).

    `dates` is the XC id -> date map for a FOCAL split. Pass None for a
    SOUNDSCAPE split, whose dates come from the filename.

    `keep_undated=True` (phase_c final review, C2+I5): rows lacking a
    date or coordinates are RETAINED with null cell/week (and null
    lat/lon when the coordinates are what's missing) instead of being
    dropped. Phase C needs them: the 7.5% undated XCM rows are the only
    thing that TRAINS the flag-0 null-context path (TokenAST's
    null_token learns only from flag-0 rows), and POW — all undated —
    must still yield a non-empty validation manifest. Kept-null rows
    are counted under `kept_null_{reason}` metadata keys, NEVER under
    `dropped_{reason}`: the bookkeeping invariant stays
    input_rows == kept_rows + sum(drops). The default is unchanged —
    phase_a/phase_b callers see byte-identical behavior.
    """
    cols: dict[str, list] = {name: [] for name in MANIFEST_COLUMNS}
    drops: Counter[DropReason] = Counter()
    kept_null: Counter[DropReason] = Counter()

    for row in metadata.to_pylist():
        path = row.get("filepath") or ""
        null_reason: DropReason | None = None

        when: date | None = None
        if dates is None:
            try:
                when = parse_filename_date(path)
            except UndatableFilename:
                null_reason = DropReason.UNDATABLE_FILENAME
        else:
            xc = _xc_id(row)
            when = dates.get(xc) if xc is not None else None
            if when is None:
                null_reason = DropReason.NO_DATE

        lat, lon = row.get("lat"), row.get("long")
        if lat is None or lon is None:
            lat_f: float | None = None
            lon_f: float | None = None
            if null_reason is None:
                null_reason = DropReason.NO_COORDINATES
        else:
            try:
                lat_f, lon_f = float(lat), float(lon)
            except (ValueError, TypeError):
                lat_f = lon_f = None
                if null_reason is None:
                    null_reason = DropReason.BAD_CELL

        cell: str | None = None
        if null_reason is None and lat_f is not None and lon_f is not None:
            try:
                cell = h3.latlng_to_cell(lat_f, lon_f, resolution)
            except (ValueError, TypeError):
                null_reason = DropReason.BAD_CELL

        if null_reason is not None:
            if not keep_undated:
                drops[null_reason] += 1
                continue
            kept_null[null_reason] += 1
            cell = None

        cols["filepath"].append(path)
        cols["cell"].append(cell)
        cols["week"].append(
            iso_week(when) if cell is not None and when is not None else None
        )
        cols["ebird_code"].append(row.get("ebird_code") or "")
        cols["split"].append(split)
        cols["lat"].append(lat_f)
        cols["lon"].append(lon_f)

    table = pa.table([cols[n] for n in MANIFEST_COLUMNS], schema=_SCHEMA)

    n_in = metadata.num_rows
    n_out = table.num_rows
    meta = {
        "input_rows": str(n_in),
        "kept_rows": str(n_out),
        "coverage": f"{(n_out / n_in) if n_in else 0.0:.6f}",
        "resolution": str(resolution),
        "split": split,
        "distinct_weeks": str(
            len({w for w in cols["week"] if w is not None})
        ),
    }
    for reason in DropReason:
        meta[f"dropped_{reason.value}"] = str(drops.get(reason, 0))
        meta[f"kept_null_{reason.value}"] = str(kept_null.get(reason, 0))

    log.info(
        "manifest %s: kept %d/%d (%.1f%%), drops=%s",
        split, n_out, n_in, 100 * (n_out / n_in if n_in else 0), dict(drops),
    )
    return table.replace_schema_metadata(meta), dict(drops)
