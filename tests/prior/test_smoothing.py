"""Neighbour smoothing over H3 rings and adjacent weeks."""
import math
import sys
import tracemalloc
from pathlib import Path

import h3
import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.smoothing import SmoothingParams, smooth


def _est(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "p": pa.array([r[3] for r in rows], pa.float64()),
        }
    )


def _lookup(t):
    return {(r["species_key"], r["cell"], r["week"]): r["p"] for r in t.to_pylist()}


CPH = h3.latlng_to_cell(55.6867, 12.5700, 4)
NEIGHBOUR = [c for c in h3.grid_disk(CPH, 1) if c != CPH][0]


def test_neighbouring_cell_pulls_value_up():
    t = _est([(1, CPH, 20, 1.0), (1, NEIGHBOUR, 20, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, NEIGHBOUR, 20)] > 0.0


def test_adjacent_week_pulls_value_up():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 21)] > 0.0


def test_week_52_and_week_1_are_adjacent():
    """The year is circular: late December informs early January."""
    t = _est([(1, CPH, 52, 1.0), (1, CPH, 1, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 1)] > 0.0


def test_distant_week_is_not_pulled():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 40, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 40)] == pytest.approx(0.0, abs=1e-9)


def test_antimeridian_cell_has_six_neighbours():
    """Review Focus 4: H3 handles the seam; a lat/lon grid would not."""
    seam = h3.latlng_to_cell(0.0, 179.9, 4)
    assert len([c for c in h3.grid_disk(seam, 1) if c != seam]) == 6


def test_antimeridian_cell_smooths_without_error():
    seam = h3.latlng_to_cell(0.0, 179.9, 4)
    other = [c for c in h3.grid_disk(seam, 1) if c != seam][0]
    out = _lookup(smooth(_est([(1, seam, 20, 1.0), (1, other, 20, 0.0)])))
    assert out[(1, other, 20)] > 0.0


def test_zero_weights_are_a_noop():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.0, time_weight=0.0)))
    assert out[(1, CPH, 20)] == pytest.approx(1.0)
    assert out[(1, CPH, 21)] == pytest.approx(0.0)


def test_schema_is_preserved():
    t = _est([(1, CPH, 20, 0.5)])
    assert smooth(t).schema.names == ["species_key", "cell", "week", "p"]


# --------------------------------------------------------------------------
# Contract 3: the circular 52 <-> 1 seam is THIS module's job. Nothing
# upstream blends weeks (backoff asserts that it does not), so each half of
# the wrap is pinned on its own: a mutant that drops only the `prev` wrap or
# only the `next` wrap must still be caught.
# --------------------------------------------------------------------------
def test_week_1_is_pulled_by_week_52_across_the_seam():
    """`prev` of week 1 is week 52, not week 0."""
    out = _lookup(smooth(_est([(1, CPH, 52, 1.0), (1, CPH, 1, 0.0)])))
    assert out[(1, CPH, 1)] == pytest.approx(0.3 / 1.3, rel=1e-12)


def test_week_52_is_pulled_by_week_1_across_the_seam():
    """`next` of week 52 is week 1, not week 53."""
    out = _lookup(smooth(_est([(1, CPH, 52, 0.0), (1, CPH, 1, 1.0)])))
    assert out[(1, CPH, 52)] == pytest.approx(0.3 / 1.3, rel=1e-12)


def test_the_seam_is_blended_with_the_same_kernel_as_any_other_week():
    """Weeks 51-52-1 behave exactly like weeks 19-20-21."""
    seam = _lookup(smooth(_est([(1, CPH, 51, 1.0), (1, CPH, 52, 0.0), (1, CPH, 1, 1.0)])))
    mid = _lookup(smooth(_est([(1, CPH, 19, 1.0), (1, CPH, 20, 0.0), (1, CPH, 21, 1.0)])))
    assert seam[(1, CPH, 52)] == pytest.approx(mid[(1, CPH, 20)], rel=1e-12)
    assert seam[(1, CPH, 52)] == pytest.approx(0.6 / 1.6, rel=1e-12)


def test_week_1_is_not_blended_with_week_51():
    """Wrapping must reach exactly one week back, not two."""
    out = _lookup(smooth(_est([(1, CPH, 51, 1.0), (1, CPH, 1, 0.0)])))
    assert out[(1, CPH, 1)] == pytest.approx(0.0, abs=1e-12)


def test_week_2_is_not_blended_with_week_52():
    """The wrap is a seam, not a shortcut: week 52 and week 2 are two apart."""
    out = _lookup(smooth(_est([(1, CPH, 52, 1.0), (1, CPH, 2, 0.0)])))
    assert out[(1, CPH, 2)] == pytest.approx(0.0, abs=1e-12)


def test_a_week_is_never_its_own_neighbour():
    """A single-week grid keeps its value: week 1's prev/next are not week 1."""
    out = _lookup(smooth(_est([(1, CPH, 1, 0.4)])))
    assert out[(1, CPH, 1)] == pytest.approx(0.4, rel=1e-12)


def test_a_requested_week_SUBSET_still_smooths_without_week_1_present():
    """Contract 7: the grid may hold only some weeks; week 1 can be absent."""
    t = _est([(1, CPH, 40, 1.0), (1, CPH, 41, 0.0)])
    assert 1 not in t.column("week").to_pylist()
    out = _lookup(smooth(t))
    assert out[(1, CPH, 41)] == pytest.approx(0.3 / 1.3, rel=1e-12)


def test_the_temporal_neighbour_must_be_the_SAME_cell():
    """Adjacent weeks blend within a cell, not across cells."""
    t = _est([(1, CPH, 20, 1.0), (1, NEIGHBOUR, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.0, time_weight=0.3)))
    assert out[(1, NEIGHBOUR, 21)] == pytest.approx(0.0, abs=1e-12)


def test_the_temporal_neighbour_must_be_the_SAME_species():
    t = _est([(1, CPH, 20, 1.0), (2, CPH, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.0, time_weight=0.3)))
    assert out[(2, CPH, 21)] == pytest.approx(0.0, abs=1e-12)


# --------------------------------------------------------------------------
# Contract 2: `p` is NEVER renormalised over species.
# --------------------------------------------------------------------------
def test_smoothing_does_NOT_renormalise_over_species():
    """Values that sum above 1.0 in a cell-week stay above 1.0.

    `p` is a per-species marginal, not a distribution over the label set: an
    absent species contributes a floor on top. Scaling a cell-week to sum to
    1.0 would silently redefine the quantity.
    """
    rows = [(sk, CPH, 20, 0.6) for sk in (1, 2, 3)]
    t = _est(rows)
    assert sum(r[3] for r in rows) > 1.0
    out = _lookup(smooth(t))
    assert sum(out[(sk, CPH, 20)] for sk in (1, 2, 3)) == pytest.approx(1.8, rel=1e-12)


def test_a_species_SUBSET_summing_below_one_is_not_scaled_up():
    """One species of a large label set keeps its small value."""
    out = _lookup(smooth(_est([(1, CPH, 20, 0.02)])))
    assert out[(1, CPH, 20)] == pytest.approx(0.02, rel=1e-12)


def test_the_smoothed_value_is_a_convex_combination_of_its_inputs():
    """Never above the largest input nor below the smallest: no rescaling."""
    t = _est([(1, CPH, 20, 0.9), (1, NEIGHBOUR, 20, 0.1), (1, CPH, 21, 0.5)])
    out = _lookup(smooth(t))
    for value in out.values():
        assert 0.1 <= value <= 0.9


# --------------------------------------------------------------------------
# Contract 6: a cell may have NO neighbour in the grid. The own-value weight
# of 1.0 is load-bearing.
# --------------------------------------------------------------------------
def test_an_isolated_cell_keeps_its_own_value_and_is_not_nan():
    """Starting the weight at 0.0 would divide by zero here."""
    t = _est([(1, CPH, 20, 0.42)])
    in_grid = set(t.column("cell").to_pylist())
    assert not (set(h3.grid_disk(CPH, 1)) - {CPH}) & in_grid, "fixture must be isolated"
    value = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.0)))[
        (1, CPH, 20)
    ]
    assert math.isfinite(value)
    assert value == pytest.approx(0.42, rel=1e-12)


def test_an_absent_neighbour_is_skipped_not_imputed_as_zero():
    """"Not in this grid" is not "absent here"."""
    t = _est([(1, CPH, 20, 1.0), (1, NEIGHBOUR, 20, 0.0)])
    present = {c for c in h3.grid_disk(CPH, 1) if c != CPH} & set(
        t.column("cell").to_pylist()
    )
    assert len(present) == 1, "fixture must leave five of six neighbours absent"
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.0)))
    assert out[(1, CPH, 20)] == pytest.approx(1.0 / 1.3, rel=1e-12)


def test_an_absent_temporal_neighbour_is_skipped_not_imputed_as_zero():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.0, time_weight=0.3)))
    assert out[(1, CPH, 20)] == pytest.approx(1.0 / 1.3, rel=1e-12)


# --------------------------------------------------------------------------
# The spatial kernel.
# --------------------------------------------------------------------------
def test_a_cell_does_not_count_itself_as_its_own_spatial_neighbour():
    """`grid_disk` includes the cell; double-counting it reweights the ring."""
    t = _est([(1, CPH, 20, 0.0), (1, NEIGHBOUR, 20, 1.0)])
    assert CPH in h3.grid_disk(CPH, 1), "grid_disk must include the origin"
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.0)))
    assert out[(1, CPH, 20)] == pytest.approx(0.3 / 1.3, rel=1e-12)


def test_a_cell_two_rings_away_is_not_a_neighbour():
    far = [c for c in h3.grid_disk(CPH, 2) if c not in set(h3.grid_disk(CPH, 1))][0]
    t = _est([(1, CPH, 20, 1.0), (1, far, 20, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, far, 20)] == pytest.approx(0.0, abs=1e-12)


def test_the_spatial_neighbour_must_be_the_SAME_week():
    """A neighbour's week-1 value must not be read for week 40.

    The grid holds the neighbour at week 1 precisely so that a lookup hard-
    coded to week 1 would find something: without that row the defect is
    unreachable and the test proves nothing.
    """
    t = _est([(1, CPH, 1, 1.0), (1, NEIGHBOUR, 40, 0.0)])
    assert (1, CPH, 1) in _lookup(t), "fixture must offer a week-1 neighbour"
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.3)))
    assert out[(1, NEIGHBOUR, 40)] == pytest.approx(0.0, abs=1e-12)


def test_the_spatial_neighbour_must_be_the_SAME_species():
    t = _est([(1, CPH, 20, 1.0), (2, NEIGHBOUR, 20, 0.0)])
    out = _lookup(smooth(t))
    assert out[(2, NEIGHBOUR, 20)] == pytest.approx(0.0, abs=1e-12)


def test_all_six_neighbours_contribute_not_just_the_first():
    ring = [c for c in h3.grid_disk(CPH, 1) if c != CPH]
    assert len(ring) == 6
    t = _est([(1, CPH, 20, 0.0)] + [(1, c, 20, 1.0) for c in ring])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.0)))
    assert out[(1, CPH, 20)] == pytest.approx((6 * 0.3) / (1 + 6 * 0.3), rel=1e-12)


def test_cells_with_different_neighbour_counts_are_smoothed_independently():
    """Three cells where A touches both B and C but B does not touch C.

    The ring matrix is ragged, so the shorter rows are padded with an
    "absent" marker. If that marker collided with a real cell code, B and C
    would each pick up a phantom neighbour; if the padding were dropped, B
    and C would be blended with each other. Both are silent.
    """
    ring = [c for c in h3.grid_disk(CPH, 1) if c != CPH]
    pairs = [
        (a, b)
        for i, a in enumerate(ring)
        for b in ring[i + 1 :]
        if b not in h3.grid_disk(a, 1)
    ]
    assert pairs, "res-4 ring must contain two mutually non-adjacent cells"
    b_cell, c_cell = pairs[0]
    assert b_cell in h3.grid_disk(CPH, 1) and c_cell in h3.grid_disk(CPH, 1)
    assert c_cell not in h3.grid_disk(b_cell, 1), "fixture precondition"
    t = _est([(1, CPH, 20, 1.0), (1, b_cell, 20, 0.0), (1, c_cell, 20, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.0)))
    # A has two neighbours in the grid; B and C have exactly one (A).
    assert out[(1, CPH, 20)] == pytest.approx(1.0 / 1.6, rel=1e-12)
    assert out[(1, b_cell, 20)] == pytest.approx(0.3 / 1.3, rel=1e-12)
    assert out[(1, c_cell, 20)] == pytest.approx(0.3 / 1.3, rel=1e-12)


def test_the_absent_neighbour_sentinel_cannot_alias_another_species_row():
    """The `+ 1` headroom in `cell_stride` is load-bearing, not decoration.

    A cell whose ring is not fully present in the grid gets its ring row
    PADDED with the sentinel code `len(cells)`, one past the last real cell
    code. The packed key is mixed-radix, so the cell digit needs a radix of
    `len(cells) + 1` for that sentinel to occupy a digit of its own. With a
    radix of `len(cells)` the sentinel carries into the SPECIES digit and the
    "no such neighbour" key lands exactly on a real row of the next species:
    a boundary cell is then blended with a stranger of another species, with
    no error, no nan and no missing row.

    Reaching it needs both conditions at once — two species, so there is a
    real row for the sentinel to alias, AND a ragged ring, so the sentinel is
    actually emitted beside a real neighbour code.
    """
    ring = [c for c in h3.grid_disk(CPH, 1) if c != CPH]
    pairs = [
        (a, b)
        for i, a in enumerate(ring)
        for b in ring[i + 1 :]
        if b not in h3.grid_disk(a, 1)
    ]
    assert pairs, "res-4 ring must contain two mutually non-adjacent cells"
    n1, n2 = pairs[0]
    rows = [(1, CPH, 20, 1.0), (1, n1, 20, 0.5), (2, CPH, 20, 0.6), (2, n2, 20, 0.6)]

    # Preconditions, asserted on the fixture itself rather than on a proxy.
    assert len({r[0] for r in rows}) >= 2, "fixture must carry >= 2 species"
    in_table = {r[1] for r in rows}
    absent_neighbours = {
        c: [n for n in h3.grid_disk(c, 1) if n != c and n not in in_table]
        for c in in_table
    }
    assert all(absent_neighbours.values()), (
        "every fixture cell must have at least one in-ring neighbour missing "
        "from the table, so the ring matrix is padded with the sentinel"
    )
    present_counts = {
        c: sum(1 for n in h3.grid_disk(c, 1) if n != c and n in in_table)
        for c in in_table
    }
    assert len(set(present_counts.values())) > 1, (
        "the ring matrix must be RAGGED (cells with different in-grid "
        "neighbour counts) or the sentinel never occupies a padded slot"
    )

    out = smooth(_est(rows), SmoothingParams(space_weight=0.3, time_weight=0.0))
    # Species 1: CPH and n1 are adjacent and blend with each other. Species 2:
    # CPH and n2 are NOT adjacent (n2 was chosen non-adjacent to n1, and CPH
    # is adjacent to both — so within species 2 the pair CPH/n2 IS adjacent).
    assert out.column("p").to_pylist() == [
        pytest.approx((1.0 + 0.3 * 0.5) / 1.3, rel=1e-12),
        pytest.approx((0.5 + 0.3 * 1.0) / 1.3, rel=1e-12),
        pytest.approx((0.6 + 0.3 * 0.6) / 1.3, rel=1e-12),
        pytest.approx((0.6 + 0.3 * 0.6) / 1.3, rel=1e-12),
    ]


def test_a_neighbour_is_read_from_the_row_that_ACTUALLY_HOLDS_it():
    """The neighbour's value, not whichever row sits at that sort position.

    Neighbours are found by binary search over sorted keys, so the search
    result must be mapped back through the sort order. Using the raw insertion
    point as a row index reads a plausible number from the wrong row. The
    fixture is built so that mapping and not-mapping disagree: the rows are in
    descending week order, so the internal permutation is NOT the identity,
    and the two candidate rows carry different values.
    """
    near = [c for c in h3.grid_disk(CPH, 1) if c != CPH][0]
    t = _est([(1, CPH, 21, 0.0), (1, CPH, 20, 0.8), (1, near, 20, 0.0)])
    assert t.column("week").to_pylist() == [21, 20, 20], "fixture must be unsorted"
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.5, time_weight=0.0)))
    # `near` at week 20 must read CPH's WEEK-20 value (0.8), not its week-21
    # value (0.0), which is the row that sits at the raw insertion point.
    assert out[(1, near, 20)] == pytest.approx((0.5 * 0.8) / 1.5, rel=1e-12)


def test_a_pentagon_cell_has_five_neighbours_and_smooths():
    """Twelve cells per resolution have five neighbours, not six."""
    pentagon = sorted(h3.get_pentagons(4))[0]
    ring = [c for c in h3.grid_disk(pentagon, 1) if c != pentagon]
    assert len(ring) == 5, "fixture must actually be a pentagon"
    t = _est([(1, pentagon, 20, 0.0)] + [(1, c, 20, 1.0) for c in ring])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.0)))
    assert out[(1, pentagon, 20)] == pytest.approx((5 * 0.3) / (1 + 5 * 0.3), rel=1e-12)


# --------------------------------------------------------------------------
# The two weights are independent and applied to their own axis.
# --------------------------------------------------------------------------
def test_the_space_weight_is_applied_to_the_SPATIAL_term():
    t = _est([(1, CPH, 20, 1.0), (1, NEIGHBOUR, 20, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.5, time_weight=0.0)))
    assert out[(1, NEIGHBOUR, 20)] == pytest.approx(0.5 / 1.5, rel=1e-12)


def test_the_time_weight_is_applied_to_the_TEMPORAL_term():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.0, time_weight=0.5)))
    assert out[(1, CPH, 21)] == pytest.approx(0.5 / 1.5, rel=1e-12)


def test_a_zero_space_weight_leaves_the_temporal_term_working():
    t = _est([(1, CPH, 20, 1.0), (1, NEIGHBOUR, 20, 0.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.0, time_weight=0.3)))
    assert out[(1, NEIGHBOUR, 20)] == pytest.approx(0.0, abs=1e-12)
    assert out[(1, CPH, 21)] == pytest.approx(0.3 / 1.3, rel=1e-12)


def test_a_zero_time_weight_leaves_the_spatial_term_working():
    t = _est([(1, CPH, 20, 1.0), (1, NEIGHBOUR, 20, 0.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.0)))
    assert out[(1, CPH, 21)] == pytest.approx(0.0, abs=1e-12)
    assert out[(1, NEIGHBOUR, 20)] == pytest.approx(0.3 / 1.3, rel=1e-12)


def test_space_and_time_neighbours_are_pooled_into_one_average():
    t = _est([(1, CPH, 20, 0.0), (1, NEIGHBOUR, 20, 1.0), (1, CPH, 21, 1.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 20)] == pytest.approx(0.6 / 1.6, rel=1e-12)


def test_a_negative_weight_is_rejected():
    """A negative weight can drive the total weight to zero, or push `p`
    outside [0, 1] — a smoother that sharpens, with no error."""
    t = _est([(1, CPH, 20, 1.0)])
    for params in (
        SmoothingParams(space_weight=-0.3, time_weight=0.3),
        SmoothingParams(space_weight=0.3, time_weight=-0.3),
    ):
        with pytest.raises(ValueError, match="negative"):
            smooth(t, params)


def test_a_nan_weight_is_rejected():
    """`if params.space_weight` is True for nan, which would turn the whole
    column into nan rather than raising."""
    t = _est([(1, CPH, 20, 1.0)])
    for params in (
        SmoothingParams(space_weight=float("nan"), time_weight=0.3),
        SmoothingParams(space_weight=0.3, time_weight=float("inf")),
    ):
        with pytest.raises(ValueError, match="finite"):
            smooth(t, params)


# --------------------------------------------------------------------------
# Contract 5 / input validation: cells are opaque, case-sensitive, and at
# ONE resolution. Every failure mode here is silent without a guard.
# --------------------------------------------------------------------------
def test_a_synthetic_non_h3_cell_is_rejected():
    """Upstream fixtures use names like 'c'; `grid_disk` cannot answer for
    them, and a swallowed lookup would make the spatial term a no-op."""
    with pytest.raises(ValueError):
        smooth(_est([(1, "c", 20, 0.5)]))


def test_a_synthetic_cell_is_rejected_even_when_the_space_weight_is_zero():
    """The guard is validation, not an incidental side effect of the lookup."""
    with pytest.raises(ValueError):
        smooth(
            _est([(1, "c", 20, 0.5)]),
            SmoothingParams(space_weight=0.0, time_weight=0.3),
        )


def test_cells_at_two_h3_resolutions_are_rejected():
    """A res-4 and a res-5 hex are never neighbours, so mixing them would
    turn the spatial term into a silent no-op over the whole table."""
    coarse = h3.latlng_to_cell(55.6867, 12.5700, 4)
    fine = h3.latlng_to_cell(55.6867, 12.5700, 5)
    with pytest.raises(ValueError, match="resolution"):
        smooth(_est([(1, coarse, 20, 1.0), (1, fine, 20, 0.0)]))


def test_an_upper_case_cell_is_rejected_rather_than_silently_unmatched():
    """Contract 5 keeps `cell` byte-for-byte, so this module must not
    casefold — but `grid_disk` emits lower case, so an upper-case row would
    match no neighbour at all. Raise instead of smoothing nothing."""
    with pytest.raises(ValueError, match="canonical"):
        smooth(_est([(1, CPH.upper(), 20, 1.0), (1, NEIGHBOUR, 20, 0.0)]))


def test_a_week_outside_1_52_is_rejected():
    """Weeks are folded upstream; week 0 or 53 has no defined neighbours and
    would index the wrap table at a row that means a different week."""
    for bad in (0, 53, -1):
        with pytest.raises(ValueError, match="week"):
            smooth(_est([(1, CPH, bad, 0.5)]))


def test_a_table_missing_the_p_column_is_rejected():
    t = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([CPH], pa.string()),
            "week": pa.array([20], pa.int8()),
        }
    )
    with pytest.raises(ValueError, match="p"):
        smooth(t)


def test_a_wrong_input_dtype_is_rejected_not_cast():
    t = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([CPH], pa.string()),
            "week": pa.array([20], pa.int64()),
            "p": pa.array([0.5], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="week"):
        smooth(t)


def test_a_null_in_a_key_or_in_p_is_rejected():
    for column, value in (
        ("species_key", pa.array([None], pa.int64())),
        ("cell", pa.array([None], pa.string())),
        ("week", pa.array([None], pa.int8())),
        ("p", pa.array([None], pa.float64())),
    ):
        columns = {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([CPH], pa.string()),
            "week": pa.array([20], pa.int8()),
            "p": pa.array([0.5], pa.float64()),
        }
        columns[column] = value
        with pytest.raises(ValueError, match="null"):
            smooth(pa.table(columns))


def test_a_non_finite_p_is_rejected():
    """The null guard's own message says a null `p` "poisons every neighbour
    that reads it". A nan does exactly that and Arrow's `null_count` cannot
    see it: one nan at the centre of a ring turns every cell that reads it
    into nan, with no error and no missing row. `inf` the same.

    Fixture violates ONLY the finiteness clause: every other value is a
    well-formed probability, and nan/inf are not "outside [0, 1]" in the
    comparison sense (`nan < 0.0` is False), so the range clause below cannot
    mask this one.
    """
    for bad in (float("nan"), float("inf"), float("-inf")):
        rows = [(1, CPH, 20, bad), (1, NEIGHBOUR, 20, 0.5)]
        t = _est(rows)
        assert t.column("p").null_count == 0, "must not be the NULL defect"
        values = t.column("p").to_pylist()
        assert not all(math.isfinite(v) for v in values), "fixture precondition"
        assert all(
            0.0 <= v <= 1.0 for v in values if math.isfinite(v)
        ), "the finite values must be in range, so only finiteness is violated"
        with pytest.raises(ValueError, match="non-finite"):
            smooth(t)


def test_a_p_below_zero_is_rejected():
    """`p` is a probability and this module does not clamp, so a negative
    input is carried straight into the output (-5.0 emits -3.73 and -0.77).

    Fixture violates ONLY the lower bound: every value is finite and none
    exceeds 1.0.
    """
    rows = [(1, CPH, 20, -5.0), (1, NEIGHBOUR, 20, 0.5)]
    t = _est(rows)
    values = t.column("p").to_pylist()
    assert all(math.isfinite(v) for v in values), "must not be the nan defect"
    assert min(values) < 0.0, "fixture precondition: a value below zero"
    assert max(values) <= 1.0, "fixture must not also violate the upper bound"
    with pytest.raises(ValueError, match=r"outside \[0\.0, 1\.0\]"):
        smooth(t)


def test_a_p_above_one_is_rejected():
    """The mirror bound, mutated separately: one comparison can be lost
    without the other. `p = 7.0` emits 5.5 and 2.0 with no error.

    Fixture violates ONLY the upper bound.
    """
    rows = [(1, CPH, 20, 7.0), (1, NEIGHBOUR, 20, 0.5)]
    t = _est(rows)
    values = t.column("p").to_pylist()
    assert all(math.isfinite(v) for v in values), "must not be the nan defect"
    assert max(values) > 1.0, "fixture precondition: a value above one"
    assert min(values) >= 0.0, "fixture must not also violate the lower bound"
    with pytest.raises(ValueError, match=r"outside \[0\.0, 1\.0\]"):
        smooth(t)


def test_the_bounds_of_p_are_INCLUSIVE():
    """0.0 and 1.0 are legitimate probabilities and the backoff emits both
    ends of the range; a `<=`/`<` slip would reject conforming input."""
    t = _est([(1, CPH, 20, 0.0), (1, NEIGHBOUR, 20, 1.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.3, time_weight=0.0)))
    assert out[(1, CPH, 20)] == pytest.approx(0.3 / 1.3, rel=1e-12)


def test_a_duplicate_species_cell_week_row_is_rejected():
    """Two values for one grid point mean a neighbour reads whichever copy it
    finds first, so the output depends on row order."""
    with pytest.raises(ValueError, match="duplicate"):
        smooth(_est([(1, CPH, 20, 1.0), (1, CPH, 20, 0.0)]))


def test_an_empty_table_smooths_to_an_empty_table():
    out = smooth(_est([]))
    assert out.num_rows == 0
    assert out.schema.names == ["species_key", "cell", "week", "p"]


def test_a_TABLE_TOO_SHORT_TO_HOLD_A_DUPLICATE_needs_no_size_guard():
    """M5. The duplicate check used to read
    `sorted_keys.size > 1 and np.any(np.diff(sorted_keys) == 0)`, and the
    conjunct was DEAD: `np.diff` of a 0- or 1-element array is empty and
    `np.any` of an empty array is False, so the short-table cases already take
    the correct path on the arithmetic alone. `tensor._reject_duplicates` says
    so in its docstring and carries no such conjunct, so the two modules
    disagreed about one arithmetic fact.

    The conjunct is gone. This pins the behaviour it *claimed* to protect
    directly, at both lengths that could reach it, so the removal is a
    measured fact rather than an argument -- and so it cannot be reinstated as
    a cargo-culted guard.
    """
    assert np.any(np.diff(np.array([], dtype=np.int64)) == 0) is np.False_
    assert np.any(np.diff(np.array([7], dtype=np.int64)) == 0) is np.False_

    assert smooth(_est([])).num_rows == 0
    single = smooth(_est([(1, CPH, 20, 0.42)]))
    assert single.num_rows == 1
    assert single.column("p").to_pylist() == pytest.approx([0.42])


# --------------------------------------------------------------------------
# Output shape: Task 8 builds a tensor by position.
# --------------------------------------------------------------------------
def test_output_schema_types_are_exact():
    out = smooth(_est([(1, CPH, 20, 0.5)]))
    assert out.schema.field("species_key").type == pa.int64()
    assert out.schema.field("cell").type == pa.string()
    assert out.schema.field("week").type == pa.int8()
    assert out.schema.field("p").type == pa.float64()


def test_row_ORDER_is_preserved_exactly():
    """Output row i is input row i.

    The key columns are passed through unchanged, so a `p` computed in sorted
    key order and attached to the original keys would mislabel every value
    with nothing raised. The fixture is in DESCENDING week order so that the
    internal sort really does permute it: an ascending fixture would leave the
    permutation an identity and the defect unreachable.
    """
    rows = [(1, CPH, 21, 0.2), (1, CPH, 20, 0.1)]
    t = _est(rows)
    assert t.column("week").to_pylist() == [21, 20], "fixture must be descending"
    out = smooth(t)
    assert out.column("species_key").to_pylist() == [r[0] for r in rows]
    assert out.column("cell").to_pylist() == [r[1] for r in rows]
    assert out.column("week").to_pylist() == [r[2] for r in rows]
    # Week 21 sits above week 20 on the way in and must still be above it on
    # the way out; a sorted-order `p` swaps the two.
    smoothed = out.column("p").to_pylist()
    assert smoothed[0] == pytest.approx((0.2 + 0.3 * 0.1) / 1.3, rel=1e-12)
    assert smoothed[1] == pytest.approx((0.1 + 0.3 * 0.2) / 1.3, rel=1e-12)
    assert smoothed[0] > smoothed[1]


def test_a_CHUNKED_input_smooths_identically_to_a_contiguous_one():
    rows = [(1, CPH, 20, 1.0), (1, NEIGHBOUR, 20, 0.0), (1, CPH, 21, 0.0)]
    contiguous = _est(rows)
    chunked = pa.concat_tables([_est(rows[:1]), _est(rows[1:])])
    assert chunked.column("cell").num_chunks == 2, "fixture must be multi-chunk"
    assert contiguous.column("cell").num_chunks == 1
    assert _lookup(smooth(chunked)) == _lookup(smooth(contiguous))


def test_smoothing_is_not_applied_twice_to_its_own_output():
    """Idempotence is NOT claimed: smoothing again must smooth further, so a
    caller cannot accidentally double-apply the kernel unnoticed."""
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    once = _lookup(smooth(t))
    twice = _lookup(smooth(smooth(t)))
    assert twice[(1, CPH, 21)] > once[(1, CPH, 21)]


# --------------------------------------------------------------------------
# Scale: the input is the DENSE grid (species x cells x 52).
# --------------------------------------------------------------------------
def test_a_dense_grid_smooths_without_materialising_every_row_in_python():
    """40 species x 120 cells x 52 weeks = 249 600 rows. A dict keyed by
    every (species, cell, week) tuple costs ~100 MB here and scales linearly
    into the national run; the array path stays an order of magnitude below.
    """
    cells = sorted(h3.grid_disk(CPH, 6))[:120]
    species = list(range(1, 41))
    weeks = list(range(1, WEEKS_PER_YEAR + 1))
    n = len(species) * len(cells) * len(weeks)
    t = pa.table(
        {
            "species_key": pa.array(
                [s for s in species for _ in cells for _ in weeks], pa.int64()
            ),
            "cell": pa.array(
                [c for _ in species for c in cells for _ in weeks], pa.string()
            ),
            "week": pa.array(
                [w for _ in species for _ in cells for w in weeks], pa.int8()
            ),
            "p": pa.array([0.01] * n, pa.float64()),
        }
    )
    assert t.num_rows == n >= 200_000
    tracemalloc.start()
    try:
        out = smooth(t)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert out.num_rows == n
    assert peak < 40_000_000, f"peak {peak / 1e6:.0f} MB for {n} rows"
