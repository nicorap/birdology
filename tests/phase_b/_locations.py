"""Shared per-row LOCATION fixtures for the Phase B sweep tests.

Real Phase A embedding caches carry H3 cells at resolution 4 ONLY; the
sweep derives each row's cell at the SweepCell's resolution from the
row's lat/lon, joined by ``filepath`` to a Phase A manifest. Fixture
caches therefore carry REAL r4 H3 strings (derived from the points
below, exactly as the manifests' cells are), and a fixture locations
table maps every cache filepath back to its point.

The four points are continents apart, so they fall in four DISTINCT
cells at every resolution the sweep uses (3, 4, 5) — asserted at import.
"""
from __future__ import annotations

import h3
import pyarrow as pa

#: (lat, lon) per fixture cell slot — Copenhagen, New York, Lima, Sydney.
POINTS: list[tuple[float, float]] = [
    (55.6761, 12.5683),
    (40.7128, -74.0060),
    (-12.0464, -77.0428),
    (-33.8688, 151.2093),
]


def cells_at(resolution: int) -> list[str]:
    """The fixture points' H3 cells at *resolution*, in POINTS order."""
    return [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in POINTS]


#: What a Phase A cache's ``cell`` column holds: the r4 cell of the point.
R4_CELLS: list[str] = cells_at(4)

for _res in (3, 4, 5):
    assert len(set(cells_at(_res))) == len(POINTS), _res


def locations_for(table: pa.Table) -> pa.Table:
    """A manifest-shaped locations table (filepath, lat, lon) for every
    row of a fixture cache *table*, each row placed at the point whose r4
    cell the cache row carries."""
    point_of = dict(zip(R4_CELLS, POINTS))
    cells = table.column("cell").to_pylist()
    return pa.table(
        {
            "filepath": table.column("filepath"),
            "lat": pa.array([point_of[c][0] for c in cells], pa.float64()),
            "lon": pa.array([point_of[c][1] for c in cells], pa.float64()),
        }
    )
