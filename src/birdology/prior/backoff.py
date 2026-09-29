"""Three-level hierarchical backoff.

Most (cell, week, species) combinations are empty, and empty almost always
means "nobody looked" rather than "absent". A raw ratio would therefore report
0.0 for unobserved-but-present species and 1.0 for a cell with one record.

Shrink each estimate toward the next coarser level, with weight set by how much
data stands behind it:

    cell-week  ->  cell's annual profile  ->  species' global profile

A species with no occurrences anywhere still gets `floor`, never zero: the
label set can legitimately contain species the occurrence data misses.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

FLOOR = 1e-6


@dataclass(frozen=True)
class BackoffParams:
    """Shrinkage strengths. Higher = pull harder toward the coarser level."""

    cell_week_weight: float = 10.0
    cell_weight: float = 50.0
    floor: float = FLOOR


def _shrink(local_num: float, local_den: float, coarse_p: float, weight: float) -> float:
    """Posterior mean with a `weight`-strength pseudo-count at `coarse_p`."""
    return (local_num + weight * coarse_p) / (local_den + weight)


def hierarchical_estimate(
    ratios: pa.Table,
    species_keys: Sequence[int],
    params: BackoffParams = BackoffParams(),
    *,
    cells: Sequence[str] | None = None,
    weeks: Sequence[int] | None = None,
) -> pa.Table:
    """Estimate P(species | cell, week) over the full requested grid."""
    rows = ratios.to_pylist()

    cell_week_n: dict[tuple[str, int], float] = {}
    cell_week_species: dict[tuple[int, str, int], float] = {}
    cell_n: dict[str, float] = {}
    cell_species: dict[tuple[int, str], float] = {}
    global_n = 0.0
    global_species: dict[int, float] = {}

    for row in rows:
        sk, cell, week, n = row["species_key"], row["cell"], int(row["week"]), row["n"]
        cell_week_species[(sk, cell, week)] = cell_week_species.get((sk, cell, week), 0.0) + n
        cell_week_n[(cell, week)] = cell_week_n.get((cell, week), 0.0) + n
        cell_species[(sk, cell)] = cell_species.get((sk, cell), 0.0) + n
        cell_n[cell] = cell_n.get(cell, 0.0) + n
        global_species[sk] = global_species.get(sk, 0.0) + n
        global_n += n

    all_cells = list(cells) if cells is not None else sorted(cell_n)
    all_weeks = list(weeks) if weeks is not None else sorted({w for _, w in cell_week_n})
    if not all_weeks:
        all_weeks = [1]

    out_species: list[int] = []
    out_cells: list[str] = []
    out_weeks: list[int] = []
    out_p: list[float] = []

    for sk in species_keys:
        p_global = (global_species.get(sk, 0.0) / global_n) if global_n else 0.0
        p_global = max(p_global, params.floor)
        for cell in all_cells:
            p_cell = _shrink(
                cell_species.get((sk, cell), 0.0),
                cell_n.get(cell, 0.0),
                p_global,
                params.cell_weight,
            )
            for week in all_weeks:
                p = _shrink(
                    cell_week_species.get((sk, cell, int(week)), 0.0),
                    cell_week_n.get((cell, int(week)), 0.0),
                    p_cell,
                    params.cell_week_weight,
                )
                out_species.append(sk)
                out_cells.append(cell)
                out_weeks.append(int(week))
                out_p.append(min(max(p, params.floor), 1.0))

    return pa.table(
        {
            "species_key": pa.array(out_species, pa.int64()),
            "cell": pa.array(out_cells, pa.string()),
            "week": pa.array(out_weeks, pa.int8()),
            "p": pa.array(out_p, pa.float64()),
        }
    )
