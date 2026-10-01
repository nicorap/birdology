#!/usr/bin/env python3
"""Build a CONTEXT-JOINED eval table for one test set (finding C1).

The raw BirdSet test-set metadata parquets carry labels and filepaths
but NEVER (cell, week) — so a conditioned eval fed one would silently
run present=0 on every row and the conditioned−twin headline would
measure training-side noise only (the C1 finding). This script joins
the label metadata with context:

* cell: H3 at ``--resolution`` from (lat, long) — the SAME
  ``h3.latlng_to_cell(lat, lon, resolution)`` call phase_a's
  ``build_manifest`` makes (lat FIRST, the argument-order pin phase_a's
  tests carry).
* week: ISO week via ``birdology.prior.indexing.iso_week`` (the week-53
  fold included) from the recording date, parsed from the FILENAME via
  ``birdology.phase_a.soundscape_dates.parse_filename_date`` — the
  soundscape test sets are not Xeno-Canto, their dates live in the
  filename (phase_a's measured contract).

Rows that cannot be dated or located are KEPT with null cell/week:
they are the measured degradation path, and dropping them would change
the eval population between the conditioned run and its twin.

    python scripts/phase_c_build_eval_ctx.py \
        --metadata /mnt/weka/ML/birdset/HSN/HSN/HSN_metadata_test_5s.parquet \
        --resolution 4 \
        --out /mnt/weka/ML/phase_c/eval_ctx/HSN_eval_ctx.parquet

Column names are configurable (``--filepath-col`` etc.) because the
BirdSet parquets are not perfectly uniform; the OUTPUT shape is fixed:
filepath, ebird_code_multilabel, cell, week — exactly what
``phase_c.evaluate.build_eval_dataset`` consumes.

Import-safe: argparse lives in main().
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import h3
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from birdology.phase_a.soundscape_dates import (
    UndatableFilename,
    parse_filename_date,
)
from birdology.prior.indexing import iso_week

log = logging.getLogger("phase_c_build_eval_ctx")


def build_eval_ctx(
    metadata: pa.Table,
    *,
    resolution: int,
    filepath_col: str = "filepath",
    labels_col: str = "ebird_code_multilabel",
    lat_col: str = "lat",
    lon_col: str = "long",
) -> pa.Table:
    """The context-joined eval table for one test-set metadata table.

    Reuses phase_a's own derivation functions (parse_filename_date,
    h3.latlng_to_cell, iso_week) — never re-implements them, so a
    manifest row and an eval row for the same file land in the same
    (cell, week) by construction. Undatable/unlocated rows are kept
    with null cell/week.
    """
    filepaths = metadata.column(filepath_col).to_pylist()
    labels = metadata.column(labels_col).to_pylist()
    lats = metadata.column(lat_col).to_pylist()
    lons = metadata.column(lon_col).to_pylist()

    cells: list[str | None] = []
    weeks: list[int | None] = []
    n_null = 0
    for path, lat, lon in zip(filepaths, lats, lons):
        cell: str | None = None
        week: int | None = None
        if lat is not None and lon is not None:
            try:
                when = parse_filename_date(path)
            except UndatableFilename:
                when = None
            if when is not None:
                try:
                    cell = h3.latlng_to_cell(
                        float(lat), float(lon), resolution
                    )
                    week = iso_week(when)
                except (ValueError, TypeError):
                    cell = week = None
        if cell is None:
            n_null += 1
        cells.append(cell)
        weeks.append(week)

    table = pa.table(
        {
            "filepath": pa.array(filepaths, pa.string()),
            "ebird_code_multilabel": pa.array(
                labels, pa.list_(pa.string())
            ),
            "cell": pa.array(cells, pa.string()),
            "week": pa.array(weeks, pa.int8()),
        }
    )
    n = table.num_rows
    log.info(
        "eval_ctx: %d rows, %d null-context (%.1f%%) at r%d",
        n, n_null, 100 * (n_null / n if n else 0.0), resolution,
    )
    return table.replace_schema_metadata(
        {
            "input_rows": str(n),
            "null_context_rows": str(n_null),
            "resolution": str(resolution),
        }
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--resolution", type=int, required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--filepath-col", default="filepath")
    parser.add_argument("--labels-col", default="ebird_code_multilabel")
    parser.add_argument("--lat-col", default="lat")
    parser.add_argument("--lon-col", default="long")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    metadata = pq.read_table(args.metadata)
    table = build_eval_ctx(
        metadata,
        resolution=args.resolution,
        filepath_col=args.filepath_col,
        labels_col=args.labels_col,
        lat_col=args.lat_col,
        lon_col=args.lon_col,
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out)
    print(f"PHASE_C_EVAL_CTX {table.num_rows} rows -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
