"""Smooth the prior across neighbouring hexes and adjacent weeks.

Neighbours come from `h3.grid_disk`, never from lat/lon arithmetic: a hex at
the antimeridian or near a pole has six neighbours like any other, which a
naive +/- degree lookup would get wrong.

Weeks are circular — week 52 is adjacent to week 1 — because a migration
that peaks in late December does not restart in January.
"""
from __future__ import annotations

from dataclasses import dataclass

import h3
import pyarrow as pa

from .indexing import WEEKS_PER_YEAR


@dataclass(frozen=True)
class SmoothingParams:
    """Neighbour weights, relative to the cell's own value of 1.0."""

    space_weight: float = 0.3
    time_weight: float = 0.3


def _adjacent_weeks(week: int) -> tuple[int, int]:
    prev = week - 1 or WEEKS_PER_YEAR
    nxt = week + 1 if week < WEEKS_PER_YEAR else 1
    return prev, nxt


def smooth(
    estimates: pa.Table, params: SmoothingParams = SmoothingParams()
) -> pa.Table:
    """Weighted average of each point with its space and time neighbours."""
    rows = estimates.to_pylist()
    values = {
        (r["species_key"], r["cell"], int(r["week"])): r["p"] for r in rows
    }
    neighbours: dict[str, list[str]] = {}

    out_p: list[float] = []
    for row in rows:
        sk, cell, week = row["species_key"], row["cell"], int(row["week"])
        total = row["p"]
        weight = 1.0

        if params.space_weight:
            if cell not in neighbours:
                neighbours[cell] = [c for c in h3.grid_disk(cell, 1) if c != cell]
            for other in neighbours[cell]:
                if (sk, other, week) in values:
                    total += params.space_weight * values[(sk, other, week)]
                    weight += params.space_weight

        if params.time_weight:
            for other_week in _adjacent_weeks(week):
                if (sk, cell, other_week) in values:
                    total += params.time_weight * values[(sk, cell, other_week)]
                    weight += params.time_weight

        out_p.append(total / weight)

    return pa.table(
        {
            "species_key": estimates.column("species_key"),
            "cell": estimates.column("cell"),
            "week": estimates.column("week"),
            "p": pa.array(out_p, pa.float64()),
        }
    )
