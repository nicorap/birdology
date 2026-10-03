"""The backoff evaluated at an explicit POINT set, vectorised -- and exact.

`hierarchical_estimate` is the reference: it walks the RatioTable with
`to_pylist()` on EVERY call and evaluates a dense ``cells x weeks`` grid.
The restricted build needs the backoff only at the query contexts and their
one-step smoothing neighbours, over a RatioTable of up to ~1e8 rows, in
hundreds of species chunks. `PointStatistics` derives the table's sums ONCE
with numpy and evaluates any (species, point) set against them; these tests
pin it to the reference BIT FOR BIT, and pin that it refuses every malformed
table the reference refuses.
"""
import sys
from pathlib import Path

import h3
import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.backoff import BackoffParams, hierarchical_estimate
from birdology.prior.effort import target_group_ratios
from birdology.prior.leakage import mark_guards_applied
from birdology.prior.point_backoff import PointStatistics

CELLS = sorted(h3.grid_disk(h3.latlng_to_cell(55.7, 12.5, 4), 1))


def _ratios(rows):
    return target_group_ratios(
        mark_guards_applied(
            pa.table(
                {
                    "species_key": pa.array([r[0] for r in rows], pa.int64()),
                    "cell": pa.array([r[1] for r in rows], pa.string()),
                    "week": pa.array([r[2] for r in rows], pa.int8()),
                    "n": pa.array([r[3] for r in rows], pa.int64()),
                }
            )
        )
    )


def _random_rows(seed, n_rows=400):
    rng = np.random.default_rng(seed)
    seen = set()
    rows = []
    while len(rows) < n_rows:
        key = (
            int(rng.integers(1, 9)),
            CELLS[int(rng.integers(0, len(CELLS)))],
            int(rng.integers(1, 53)),
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append((*key, int(rng.integers(1, 40))))
    return rows


def _grid_index(table):
    cols = [table.column(k).to_pylist() for k in ("species_key", "cell", "week", "p")]
    return {(s, c, w): p for s, c, w, p in zip(*cols)}


@pytest.mark.parametrize("seed", [0, 1, 2])
@pytest.mark.parametrize(
    "params",
    [
        BackoffParams(),
        BackoffParams(cell_week_weight=0.0, cell_weight=0.0),
        BackoffParams(cell_week_weight=0.5, cell_weight=500.0, floor=1e-3),
    ],
)
def test_point_estimates_EQUAL_the_reference_grid_bit_for_bit(seed, params):
    ratios = _ratios(_random_rows(seed))
    # Species 0 and 99 are on no row (global floor path); a cell outside the
    # table is evaluated too (cell_n == 0 path).
    species = [3, 0, 1, 99, 8]
    outside = h3.latlng_to_cell(40.0, -3.0, 4)
    grid_cells = [*CELLS, outside]
    reference = _grid_index(
        hierarchical_estimate(
            ratios, species, params, cells=grid_cells, weeks=list(range(1, 53))
        )
    )
    rng = np.random.default_rng(seed + 100)
    points = sorted(
        {
            (grid_cells[int(rng.integers(0, len(grid_cells)))], int(rng.integers(1, 53)))
            for _ in range(150)
        }
    )
    got = PointStatistics(ratios).estimate(species, points, params)
    assert got.shape == (len(species), len(points))
    assert got.dtype == np.float64
    for i, sk in enumerate(species):
        for j, (cell, week) in enumerate(points):
            assert got[i, j] == reference[(sk, cell, week)], (sk, cell, week)


def test_an_empty_species_or_point_list_gives_an_empty_block():
    stats = PointStatistics(_ratios(_random_rows(0, 20)))
    assert stats.estimate([], [(CELLS[0], 3)]).shape == (0, 1)
    assert stats.estimate([1], []).shape == (1, 0)


def test_the_statistics_are_independent_of_row_order():
    rows = _random_rows(5)
    a = PointStatistics(_ratios(rows))
    b = PointStatistics(_ratios(rows[::-1]))
    points = [(CELLS[0], 1), (CELLS[3], 52), (CELLS[6], 30)]
    assert np.array_equal(a.estimate([1, 2, 3], points), b.estimate([1, 2, 3], points))


def test_the_ratio_row_count_is_exposed_for_the_peak_projection():
    rows = _random_rows(1, 37)
    assert PointStatistics(_ratios(rows)).n_ratio_rows == 37


# -- refusals: the same contract `hierarchical_estimate` enforces -----------


def _raw(rows, totals=None):
    """A RatioTable built by hand, so a malformed one can be made."""
    totals = totals or [r[3] + 5 for r in rows]
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "n": pa.array([r[3] for r in rows], pa.int64()),
            "total": pa.array(totals, pa.int64()),
            "ratio": pa.array([0.1] * len(rows), pa.float64()),
        }
    )


@pytest.mark.parametrize(
    "table, match",
    [
        (_raw([(1, CELLS[0], 0, 1)]), "outside"),
        (_raw([(1, CELLS[0], 3, 1)], totals=[0]), "total"),
        (_raw([(1, CELLS[0], 3, 0)]), "n="),
        (_raw([(1, CELLS[0], 3, 9)], totals=[4]), "exceeds"),
        (_raw([(1, CELLS[0], 3, 1), (2, CELLS[0], 3, 1)], totals=[5, 6]), "two different totals"),
        (_raw([(1, CELLS[0], 3, 1), (1, CELLS[0], 3, 2)], totals=[9, 9]), "duplicate"),
        (_raw([(1, CELLS[0], 3, 6), (2, CELLS[0], 3, 6)], totals=[10, 10]), "summing"),
    ],
)
def test_a_malformed_ratio_table_is_refused_like_the_reference(table, match):
    with pytest.raises(ValueError, match=match):
        hierarchical_estimate(table, [1])
    with pytest.raises(ValueError, match=match):
        PointStatistics(table)


def test_a_wrongly_typed_ratio_table_is_refused():
    table = _raw([(1, CELLS[0], 3, 1)]).set_column(
        2, "week", pa.array([3], pa.int64())
    )
    with pytest.raises(ValueError, match="week"):
        PointStatistics(table)


def test_bad_params_and_points_are_refused():
    stats = PointStatistics(_ratios(_random_rows(0, 20)))
    with pytest.raises(ValueError, match="floor"):
        stats.estimate([1], [(CELLS[0], 3)], BackoffParams(floor=0.0))
    with pytest.raises(ValueError, match="duplicate"):
        stats.estimate([1], [(CELLS[0], 3), (CELLS[0], 3)])
    with pytest.raises(ValueError, match="duplicate"):
        stats.estimate([1, 1], [(CELLS[0], 3)])
    with pytest.raises(ValueError, match="outside"):
        stats.estimate([1], [(CELLS[0], 53)])


def test_an_UNKNOWN_cell_takes_the_reference_shrink_not_the_bare_global():
    """`_shrink(0, 0, p_global, w)` is `(w * p_global) / w`, which differs
    from `p_global` in the last bit for ~15% of values at w = 50. Species 1
    here has p_global = 1/4099, one such value."""
    ratios = _ratios([(1, CELLS[0], 3, 1), (2, CELLS[0], 3, 4098)])
    outside = h3.latlng_to_cell(40.0, -3.0, 4)
    p_global = 1 / 4099
    assert (50.0 * p_global) / 50.0 != p_global  # the fixture's premise
    reference = _grid_index(
        hierarchical_estimate(ratios, [1], cells=[outside], weeks=[3])
    )
    got = PointStatistics(ratios).estimate([1], [(outside, 3)])
    assert got[0, 0] == reference[(1, outside, 3)]
