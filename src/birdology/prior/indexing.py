"""Map occurrences onto an (H3 cell, ISO week) grid and aggregate to counts.

H3 is used rather than a lat/lon grid because the data spans 0-60+ degrees of
latitude, where lat/lon cells differ in area by a factor of two. Hexagons are
equal-area and have six equidistant neighbours, which the smoothing kernel in
`smoothing.py` relies on.
"""
from __future__ import annotations

from datetime import date

import h3
import pyarrow as pa

WEEKS_PER_YEAR = 52


def iso_week(d: date) -> int:
    """ISO week number, with week 53 folded into 52.

    71 of every 400 years have an ISO week 53. Keeping it would create a 53rd
    index that the circular smoothing kernel would treat as a real week with a
    seventh of the usual data behind it.
    """
    week = d.isocalendar().week
    return WEEKS_PER_YEAR if week > WEEKS_PER_YEAR else week


def assign_cells(table: pa.Table, resolution: int) -> pa.Table:
    """Add `cell` (H3 index at *resolution*) and `week` columns."""
    lats = table.column("lat").to_pylist()
    lons = table.column("lon").to_pylist()
    dates = table.column("event_date").to_pylist()

    cells = [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in zip(lats, lons)]
    weeks = [iso_week(d) for d in dates]

    return table.append_column(
        "cell", pa.array(cells, pa.string())
    ).append_column("week", pa.array(weeks, pa.int8()))


def aggregate_counts(table: pa.Table) -> pa.Table:
    """Reduce to (species_key, cell, week) -> n."""
    grouped = (
        table.select(["species_key", "cell", "week"])
        .append_column("n", pa.array([1] * table.num_rows, pa.int64()))
        .group_by(["species_key", "cell", "week"])
        .aggregate([("n", "sum")])
    )
    return grouped.rename_columns(["species_key", "cell", "week", "n"]).select(
        ["species_key", "cell", "week", "n"]
    )
