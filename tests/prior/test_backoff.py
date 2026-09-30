"""Three-level hierarchical backoff for a very sparse grid."""
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.backoff import (
    FLOOR,
    BackoffParams,
    hierarchical_estimate,
)
from birdology.prior.effort import target_group_ratios
from birdology.prior.leakage import mark_guards_applied


def _counts(rows):
    # STAMPED: `target_group_ratios` refuses a CountTable that does not carry
    # the leakage-guard marker (T10's acceptance criterion). These fixtures
    # stand in for the post-guard table the real pipeline hands it, so they
    # carry the marker the real one would. The refusal itself is tested in
    # `test_effort.py`, on a deliberately UNSTAMPED table.
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


def _lookup(table):
    return {(r["species_key"], r["cell"], r["week"]): r["p"] for r in table.to_pylist()}


def test_dense_cell_week_dominated_by_local_estimate():
    """With 1000 local records, the local ratio should win."""
    rows = [(1, "c", 5, 1000)]
    rows += [(2, "other", 5, 1000)]
    out = _lookup(hierarchical_estimate(_counts(rows), [1, 2]))
    assert out[(1, "c", 5)] > 0.9


def test_sparse_cell_week_shrinks_toward_coarser_level():
    """One local record must not produce a probability of 1.0."""
    rows = [(1, "c", 5, 1), (2, "c", 40, 100), (1, "c", 40, 1)]
    out = _lookup(hierarchical_estimate(_counts(rows), [1, 2]))
    assert out[(1, "c", 5)] < 1.0


def test_species_absent_everywhere_gets_floor_not_zero():
    """Review Focus 1: a label-set species with no occurrence data."""
    out = _lookup(hierarchical_estimate(_counts([(1, "c", 5, 10)]), [1, 999]))
    assert out[(999, "c", 5)] >= FLOOR
    assert out[(999, "c", 5)] > 0.0


def test_species_absent_everywhere_does_not_raise():
    hierarchical_estimate(_counts([(1, "c", 5, 10)]), [1, 999, 1000])


def test_empty_cell_week_backs_off_without_dividing_by_zero():
    """Review Focus 2: a (cell, week) nobody visited."""
    ratios = _counts([(1, "a", 5, 10)])
    out = hierarchical_estimate(ratios, [1], cells=["a", "empty"], weeks=[5])
    values = _lookup(out)
    assert values[(1, "empty", 5)] >= FLOOR
    assert all(v == v for v in values.values())  # no NaN


def test_probabilities_are_bounded():
    rows = [(1, "c", 5, 7), (2, "c", 5, 3)]
    for p in _lookup(hierarchical_estimate(_counts(rows), [1, 2])).values():
        assert 0.0 <= p <= 1.0


def test_stronger_shrinkage_pulls_toward_global():
    rows = [(1, "c", 5, 1), (2, "c", 40, 100)]
    weak = _lookup(
        hierarchical_estimate(_counts(rows), [1, 2], params=BackoffParams(cell_week_weight=0.1))
    )
    strong = _lookup(
        hierarchical_estimate(_counts(rows), [1, 2], params=BackoffParams(cell_week_weight=1000.0))
    )
    assert strong[(1, "c", 5)] < weak[(1, "c", 5)]


# ===========================================================================
# Upstream-contract tests the brief does not specify (contract points 1-9).
# ===========================================================================


def _p(rows, species, *, cells=None, weeks=None, params=BackoffParams()):
    return _lookup(
        hierarchical_estimate(
            _counts(rows), species, params=params, cells=cells, weeks=weeks
        )
    )


# ---- contract 5: parent levels are recomputed from parent COUNTS ----------


def test_cell_profile_is_recomputed_from_COUNTS_not_averaged_from_RATIOS():
    """Contract 5. THE test that catches averaging child ratios.

    Cell `c` has two weeks with wildly DIFFERENT effort:

      week  5: species 1 alone, n=1     -> ratio 1.00  (one lucky record)
      week 40: species 1 n=1 of 101     -> ratio ~0.0099

    Recomputed from counts the cell profile is 2/102 = 0.0196. The
    unweighted mean of the two child ratios is (1.0 + 0.0099)/2 = 0.505 --
    25x larger, and it is 25x larger *because* the one-record week is given
    the same voice as the hundred-record week. That is exactly the
    observer-effort bias Task 5 removes, silently reintroduced.

    A week nobody visited (week 20) is scored purely from the cell profile,
    so it reads the parent value directly with no local data mixed in. It
    must land near 0.0196, and must NOT be anywhere near 0.505.
    """
    rows = [(1, "c", 5, 1), (1, "c", 40, 1), (2, "c", 40, 100)]
    out = _p(rows, [1], cells=["c"], weeks=[5, 20, 40])

    from_counts = 2 / 102
    from_averaged_ratios = (1.0 + 1 / 101) / 2
    assert from_averaged_ratios > 20 * from_counts  # the fixture discriminates

    # week 20 has no local data: p == the shrunk cell profile, unmixed.
    assert out[(1, "c", 20)] == pytest.approx(from_counts, rel=1e-9)
    assert out[(1, "c", 20)] < 0.05


def test_global_profile_is_recomputed_from_COUNTS_not_averaged_from_CELL_RATIOS():
    """Contract 5, one level up: the national/continental profile.

    Cell `tiny` holds a single record of species 1 (ratio 1.0); cell `busy`
    holds 1 of species 1 among 1000 (ratio 0.001). Recomputed from counts
    the global share is 2/1002 = 0.002. Averaged over the two cells it is
    0.5005 -- 250x larger, driven entirely by the one-record cell.

    An entirely unobserved cell gets nothing but the global profile, so it
    reads that value directly.
    """
    rows = [(1, "tiny", 5, 1), (1, "busy", 5, 1), (2, "busy", 5, 999)]
    out = _p(rows, [1], cells=["nowhere"], weeks=[5])

    from_counts = 2 / 1001
    from_averaged_cell_ratios = (1.0 + 1 / 1000) / 2
    assert from_averaged_cell_ratios > 200 * from_counts  # discriminating

    assert out[(1, "nowhere", 5)] == pytest.approx(from_counts, rel=1e-9)
    assert out[(1, "nowhere", 5)] < 0.01


def test_a_cells_own_rows_do_not_change_its_profile_when_regrouped():
    """Contract 5 again, as an invariant rather than a value.

    Splitting one cell-week's records across more weeks changes every child
    RATIO but not the cell's total counts. A count-derived cell profile is
    therefore invariant; a ratio-averaged one is not.
    """
    one_week = [(1, "c", 5, 4), (2, "c", 5, 96)]
    four_weeks = [
        (1, "c", 5, 1),
        (2, "c", 5, 24),
        (1, "c", 6, 1),
        (2, "c", 6, 24),
        (1, "c", 7, 1),
        (2, "c", 7, 24),
        (1, "c", 8, 1),
        (2, "c", 8, 24),
    ]
    # An unvisited week reads the cell profile with no local data.
    a = _p(one_week, [1], cells=["c"], weeks=[30])[(1, "c", 30)]
    b = _p(four_weeks, [1], cells=["c"], weeks=[30])[(1, "c", 30)]
    assert a == pytest.approx(b, rel=1e-12)


# ---- contract 4: `total` is the confidence weight, not the ratio ----------


def test_two_identical_ratios_with_different_effort_get_different_estimates():
    """Contract 4. ratio 1.0 from 1 record != ratio 1.0 from 1000 records.

    Both weeks below have species 1 as 100% of their records -- identical
    `ratio`, identical `n/total`. Only `total` distinguishes them, and it
    must: shrinking on the ratio alone lets a single lucky record claim the
    same confidence as a thousand.
    """
    rows = [(1, "c", 5, 1), (1, "c", 6, 1000), (2, "c", 7, 10000)]
    ratios = _counts(rows)

    # PRECONDITION: the two weeks really do carry the same share.
    shares = {
        (r["cell"], r["week"]): r["ratio"]
        for r in ratios.to_pylist()
        if r["species_key"] == 1
    }
    assert shares[("c", 5)] == pytest.approx(1.0)
    assert shares[("c", 6)] == pytest.approx(1.0)

    out = _lookup(hierarchical_estimate(ratios, [1], cells=["c"], weeks=[5, 6]))
    assert out[(1, "c", 6)] > out[(1, "c", 5)] + 0.5


def test_a_single_record_cell_week_cannot_reach_certainty():
    """Contract 4: `total = 1` must stay heavily shrunk even at ratio 1.0."""
    rows = [(1, "c", 5, 1), (2, "c", 40, 10000)]
    out = _p(rows, [1], cells=["c"], weeks=[5])
    assert out[(1, "c", 5)] < 0.5


def test_more_local_records_move_the_estimate_monotonically():
    """Contract 4: confidence in the local share grows with `total`."""
    seen = []
    for n in (1, 10, 100, 1000):
        rows = [(1, "c", 5, n), (2, "c", 40, 10000)]
        seen.append(_p(rows, [1], cells=["c"], weeks=[5])[(1, "c", 5)])
    assert seen == sorted(seen)
    assert seen[0] < seen[-1]


# ---- contract 3: absence is row absence, never ratio == 0 ----------------


def test_an_unobserved_cell_week_is_not_scored_as_certainly_absent():
    """Contract 3. The materialised grid MUST distinguish two things:

      * a cell-week nobody visited (no row upstream), and
      * a cell-week that was visited and did not hold the species.

    Both are dense-grid cells here. The unvisited one must fall back to the
    parent, NOT be zero-filled; the visited-without-the-species one may sit
    lower because its own effort argues against presence. Neither may be
    0.0, because 0.0 is the value a zero-fill would produce.
    """
    rows = [(1, "c", 5, 50), (2, "c", 5, 50), (2, "c", 6, 10000)]
    out = _p(rows, [1], cells=["c"], weeks=[5, 6, 30])

    unvisited = out[(1, "c", 30)]
    visited_without_species = out[(1, "c", 6)]

    assert unvisited > 0.0
    assert visited_without_species > 0.0
    assert unvisited > visited_without_species, (
        "a week nobody surveyed must not be scored as confidently as a week "
        "with 10000 records and no sighting"
    )


def test_an_unobserved_week_backs_off_to_its_CELL_not_straight_to_GLOBAL():
    """Contract 3 + the middle level exists at all.

    Species 1 is half of cell `c` (50 of 100) and a twentieth of the world
    (50 of 1100). An unvisited week in `c` must inherit `c`'s profile, not
    the global one -- otherwise the three-level hierarchy is two levels.
    """
    rows = [(1, "c", 5, 50), (2, "c", 5, 50), (2, "far", 5, 1000)]
    out = _p(rows, [1], cells=["c"], weeks=[30])
    global_level = 50 / 1100
    assert out[(1, "c", 30)] > 5 * global_level


def test_a_species_observed_nowhere_stays_at_the_floor_not_above_it():
    """Contract 3: the floor is a floor for genuinely-absent species.

    `hierarchical_estimate` must not manufacture a visible probability for a
    species with no records at all -- it gets `floor`, and the shrinkage
    chain must not amplify it.
    """
    rows = [(1, "c", 5, 1000)]
    out = _p(rows, [1, 999], cells=["c"], weeks=[5])
    assert out[(999, "c", 5)] == pytest.approx(FLOOR, rel=1e-6)


def test_the_floor_is_configurable_and_respected():
    out = _p(
        [(1, "c", 5, 1000)], [1, 999], cells=["c"], weeks=[5],
        params=BackoffParams(floor=1e-3),
    )
    assert out[(999, "c", 5)] == pytest.approx(1e-3, rel=1e-6)


def test_an_empty_ratio_table_gives_every_species_the_floor():
    """No occurrence data at all: no division by zero, no NaN, no crash."""
    empty = _counts([])
    out = _lookup(
        hierarchical_estimate(empty, [1, 2], cells=["a", "b"], weeks=[1, 52])
    )
    assert len(out) == 8
    assert all(v == pytest.approx(FLOOR) for v in out.values())


# ---- contract 6: weeks are int8 1..52, and 52/1 adjacency is NOT ours ----


def test_week_52_and_week_1_are_not_blended_here():
    """Contract 6: circular adjacency is Task 7's job, not this module's.

    Week 52 is dense with species 1; week 1 was never visited. If this
    module silently smoothed across the seam, week 1 would sit above the
    cell profile it is entitled to.
    """
    rows = [(1, "c", 52, 100), (2, "c", 30, 900)]
    out = _p(rows, [1], cells=["c"], weeks=[1, 52])
    cell_profile = (100 + 50.0 * (100 / 1000)) / (1000 + 50.0)
    assert out[(1, "c", 1)] == pytest.approx(cell_profile, rel=1e-9)
    assert out[(1, "c", 52)] > out[(1, "c", 1)]


def test_output_week_is_int8_and_survives_the_round_trip():
    """Contract 6: Task 7 and Task 8 key on `week`; the dtype must not drift."""
    out = hierarchical_estimate(
        _counts([(1, "c", 52, 5)]), [1], cells=["c"], weeks=[1, 26, 52]
    )
    assert out.schema.field("week").type == pa.int8()
    assert sorted(out.column("week").to_pylist()) == [1, 26, 52]


def test_a_week_outside_1_52_is_rejected():
    """Contract 6: week 0 and week 53 do not exist, and an int8 grid would
    silently accept them. Emitting them would hand Task 7 a seam it cannot
    close and Task 8 an index out of range."""
    ratios = _counts([(1, "c", 5, 5)])
    for bad in (0, 53, -1, 128):
        with pytest.raises(ValueError, match="week"):
            hierarchical_estimate(ratios, [1], cells=["c"], weeks=[bad])


# ---- contract 8: row order is unspecified upstream ----------------------


def test_the_result_is_invariant_under_input_ROW_ORDER():
    """Contract 8. Upstream order is Arrow hash-group order, i.e. arbitrary.

    Every level's counts are sums, so order cannot matter -- unless the
    implementation ever starts taking a first/last value per group.
    """
    rows = [(1, "c", 5, 3), (2, "c", 5, 7), (1, "d", 9, 11), (2, "d", 9, 2)]
    a = _p(rows, [1, 2], cells=["c", "d"], weeks=[5, 9])
    b = _p(list(reversed(rows)), [1, 2], cells=["c", "d"], weeks=[5, 9])
    assert a.keys() == b.keys()
    for k in a:
        assert a[k] == pytest.approx(b[k], rel=1e-12), k


def test_the_result_is_invariant_under_input_CHUNKING():
    """Contract 8: a multi-chunk RatioTable must give the same answer."""
    rows = [(1, "c", 5, 3), (2, "c", 5, 7), (1, "d", 9, 11), (2, "d", 9, 2)]
    single = _counts(rows)
    chunked = pa.concat_tables([_counts(rows[:2]), _counts(rows[2:])])
    # PRECONDITION: the fixture really is multi-chunk, else this tests nothing.
    assert chunked.column("n").num_chunks > 1
    assert single.column("n").num_chunks == 1

    a = _lookup(hierarchical_estimate(single, [1, 2], cells=["c", "d"], weeks=[5, 9]))
    b = _lookup(hierarchical_estimate(chunked, [1, 2], cells=["c", "d"], weeks=[5, 9]))
    for k in a:
        assert a[k] == pytest.approx(b[k], rel=1e-12), k


# ---- contract 7: cells are opaque strings at ONE resolution --------------


def test_cell_strings_are_passed_through_verbatim():
    """Contract 7: no local casefolding. The fixture is deliberately
    contract-violating in case only, so a redundant second normalisation is
    VISIBLE instead of being a no-op on lowercase input."""
    cell = "8A1Fb46622dFFFF"
    out = hierarchical_estimate(_counts([(1, cell, 5, 3)]), [1])
    assert out.column("cell").to_pylist() == [cell]


def test_differently_cased_cells_are_different_cells():
    """Contract 7: a local casefold would merge these two profiles."""
    rows = [(1, "8A1F", 5, 1), (2, "8a1f", 5, 1000)]
    out = _p(rows, [1], cells=["8A1F", "8a1f"], weeks=[5])
    assert out[(1, "8A1F", 5)] > out[(1, "8a1f", 5)]


# ---- shape / grid materialisation ----------------------------------------


def test_the_output_grid_is_the_full_cartesian_product():
    out = hierarchical_estimate(
        _counts([(1, "c", 5, 1)]), [1, 2, 3], cells=["c", "d"], weeks=[5, 6, 7]
    )
    assert out.num_rows == 3 * 2 * 3
    assert len(set(zip(*[out.column(c).to_pylist() for c in ("species_key", "cell", "week")]))) == 18


def test_output_schema_is_exact():
    out = hierarchical_estimate(_counts([(1, "c", 5, 1)]), [1])
    assert out.schema.names == ["species_key", "cell", "week", "p"]
    assert out.schema.types == [pa.int64(), pa.string(), pa.int8(), pa.float64()]


def test_a_duplicate_species_key_is_rejected():
    """A repeated key would emit duplicate grid rows, and Task 8's tensor
    would silently keep whichever it indexed last."""
    with pytest.raises(ValueError, match="duplicate"):
        hierarchical_estimate(_counts([(1, "c", 5, 1)]), [1, 2, 1])


def test_a_duplicate_cell_is_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        hierarchical_estimate(_counts([(1, "c", 5, 1)]), [1], cells=["c", "c"])


def test_a_duplicate_week_is_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        hierarchical_estimate(_counts([(1, "c", 5, 1)]), [1], weeks=[5, 5])


def test_a_ratio_table_missing_required_columns_is_rejected():
    """Contract 1/4: this module needs `n` AND `total`. A table without them
    is not a RatioTable, and reading `ratio` alone would drop the confidence
    weight that contract 4 exists to preserve."""
    bare = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "ratio": pa.array([1.0], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="n"):
        hierarchical_estimate(bare, [1])


def test_every_probability_is_finite():
    rows = [(1, "c", 5, 1), (2, "d", 40, 1000)]
    out = hierarchical_estimate(_counts(rows), [1, 2, 999])
    for p in out.column("p").to_pylist():
        assert p == p and p not in (float("inf"), float("-inf"))
        assert FLOOR <= p <= 1.0


def test_zero_weights_return_the_raw_local_share():
    """With no shrinkage at all the estimate is the plain cell-week share --
    a sanity anchor showing the shrinkage is the only thing moving values."""
    rows = [(1, "c", 5, 3), (2, "c", 5, 7)]
    out = _p(
        rows, [1, 2], cells=["c"], weeks=[5],
        params=BackoffParams(cell_week_weight=0.0, cell_weight=0.0),
    )
    assert out[(1, "c", 5)] == pytest.approx(0.3)
    assert out[(2, "c", 5)] == pytest.approx(0.7)


def test_no_shrinkage_makes_the_grid_sum_to_one_over_species():
    """Contract 2, stated honestly: the raw shares sum to ~1.0 over the
    species present in a cell-week, and this is asserted with approx on a
    tiny fixture ONLY. It is not a property of the shrunk output, which
    deliberately does NOT normalise -- see the next test."""
    rows = [(1, "c", 5, 3), (2, "c", 5, 7)]
    out = _p(
        rows, [1, 2], cells=["c"], weeks=[5],
        params=BackoffParams(cell_week_weight=0.0, cell_weight=0.0),
    )
    assert sum(out.values()) == pytest.approx(1.0)


def test_the_shrunk_output_is_NOT_normalised_over_species():
    """The shrunk `p` is a per-species marginal, not a distribution over the
    species axis, and it is NOT renormalised over `species_keys`.

    Worth stating carefully, because shrinkage happens to be affine: when the
    species list holds every species in the data, the shrunk values still sum
    to ~1.0, which looks like normalisation and is not. The two cases below
    are the ones that can only hold if nothing renormalises:

      * a SUBSET of the species sums to clearly less than 1.0;
      * adding a species with no records pushes the sum ABOVE 1.0 by `floor`.
    """
    rows = [(1, "c", 5, 3), (2, "c", 5, 7)]
    zero = BackoffParams(cell_week_weight=0.0, cell_weight=0.0)

    subset = _p(rows, [1], cells=["c"], weeks=[5], params=zero)
    assert sum(subset.values()) == pytest.approx(0.3)

    with_absent = _p(rows, [1, 2, 999], cells=["c"], weeks=[5], params=zero)
    assert sum(with_absent.values()) > 1.0


def test_default_grid_covers_every_observed_cell_and_week():
    rows = [(1, "c", 5, 1), (2, "d", 40, 1)]
    out = hierarchical_estimate(_counts(rows), [1])
    assert sorted(set(out.column("cell").to_pylist())) == ["c", "d"]
    assert sorted(set(out.column("week").to_pylist())) == [5, 40]


def test_an_empty_species_list_returns_an_empty_table_with_the_full_schema():
    out = hierarchical_estimate(_counts([(1, "c", 5, 1)]), [])
    assert out.num_rows == 0
    assert out.schema.names == ["species_key", "cell", "week", "p"]
    assert out.schema.types == [pa.int64(), pa.string(), pa.int8(), pa.float64()]


# ---- contract 4 + rule (C): the denominator is `total`, never re-summed ---


def test_the_denominator_is_the_TOTAL_COLUMN_not_a_re_sum_of_n():
    """Contract 4 and rule (C), together, on the most dangerous input shape.

    `total` is the effort proxy, computed ONCE upstream over ALL bird records
    in the cell-week. This module must READ it, never re-derive it by summing
    the `n` values it happens to have been handed.

    The two agree only when the table still holds every species. They diverge
    the moment anything filters it -- and filtering to the label set is a
    perfectly ordinary pipeline step. Here species 1 is 1 record in a
    cell-week of 100, i.e. a share of 0.01; after filtering to species 1 the
    rows sum to 1, so a re-summed denominator says the share is 1.00. A 100x
    error, in the direction of confident presence, with no error raised.

    The upstream module documents this as rule (C): a value derived once
    upstream must not be derived a second time locally.
    """
    full = _counts([(1, "c", 5, 1), (2, "c", 5, 99), (2, "d", 40, 10000)])
    filtered = full.filter(pc.equal(full.column("species_key"), 1))

    # PRECONDITIONS: the fixture must actually discriminate the two readings.
    assert filtered.num_rows == 1
    assert filtered.column("total").to_pylist() == [100]
    assert pc.sum(filtered.column("n")).as_py() == 1, (
        "the re-summed denominator must differ from `total`, or this test "
        "cannot distinguish reading the column from re-deriving it"
    )
    assert filtered.column("ratio").to_pylist() == [pytest.approx(0.01)]

    out = _lookup(hierarchical_estimate(filtered, [1], cells=["c"], weeks=[5]))
    # Reading `total`: 1 record in 100, shrunk -- must stay small.
    # Re-summing `n`: 1 record in 1, ratio 1.0 -- would land above 0.5.
    assert out[(1, "c", 5)] < 0.2, (
        "the cell-week denominator was re-derived from the `n` values present "
        "rather than read from `total`"
    )


def test_a_total_inconsistent_with_the_rows_is_still_honoured_at_every_level():
    """Rule (C) at the CELL and GLOBAL levels too, not just the cell-week.

    The parent denominators must also come from `total`, summed over
    cell-weeks rather than over rows. With the table filtered to one species
    the distinction is visible at all three levels at once: every level's
    denominator is 1 if re-summed from `n`, so every level would report
    certainty.
    """
    full = _counts(
        [
            (1, "c", 5, 1),
            (2, "c", 5, 99),
            (1, "c", 6, 1),
            (2, "c", 6, 99),
            (2, "d", 40, 800),
        ]
    )
    filtered = full.filter(pc.equal(full.column("species_key"), 1))
    assert filtered.num_rows == 2
    assert pc.sum(filtered.column("n")).as_py() == 2

    # An unvisited week reads the CELL profile; an unvisited cell reads the
    # GLOBAL profile. Neither may approach certainty.
    out = _lookup(
        hierarchical_estimate(filtered, [1], cells=["c", "z"], weeks=[30])
    )
    assert out[(1, "c", 30)] < 0.2, "cell-level denominator re-summed from n"
    assert out[(1, "z", 30)] < 0.2, "global-level denominator re-summed from n"


def test_a_cell_week_whose_total_disagrees_between_rows_is_rejected():
    """`total` is a property of the (cell, week), so two rows sharing a
    cell-week must carry the same value. Disagreement means the table was
    stitched together from incompatible sources, and silently picking one
    would make the denominator depend on row order (contract 8)."""
    bad = pa.table(
        {
            "species_key": pa.array([1, 2], pa.int64()),
            "cell": pa.array(["c", "c"], pa.string()),
            "week": pa.array([5, 5], pa.int8()),
            "n": pa.array([1, 2], pa.int64()),
            "total": pa.array([10, 20], pa.int64()),
            "ratio": pa.array([0.1, 0.1], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="total"):
        hierarchical_estimate(bad, [1, 2])


def test_a_numerator_exceeding_its_own_total_is_rejected():
    """n > total cannot happen on a real RatioTable; it means numerator and
    denominator are no longer paired, and it would emit p > 1 before the
    clamp hides it.

    Asserted by MESSAGE, and it has to be. As a rejection PREDICATE the
    per-row check is subsumed by the cell-week coherence check: every `n` is
    >= 1, so a single row exceeding its own total also makes the cell-week sum
    exceed it, and removing the per-row check still raises -- from the other
    guard, with a message about an aggregate. What the per-row check uniquely
    does is LOCALISE the defect to a (species, cell, week), which is the
    difference between "this table is incoherent somewhere" and "this row is
    wrong". Matching its own wording keeps the two guards distinguishable.
    """
    bad = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([50], pa.int64()),
            "total": pa.array([10], pa.int64()),
            "ratio": pa.array([5.0], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match=r"has n=50 which exceeds its own total=10") as caught:
        hierarchical_estimate(bad, [1])
    # PRECONDITION: it is the ROW-PAIRING guard talking, not the cell-week
    # coherence guard standing in for it.
    assert "summing to" not in str(caught.value)
    assert "(1, 'c', 5)" in str(caught.value)


def test_a_non_positive_total_is_rejected_not_divided_by():
    """The message must name the EFFORT, not just fail somehow.

    `n=1, total=0` also violates `n <= total`, so a generic `match="total"`
    passes even when the effort check itself is gone. Matching the effort
    check's own wording keeps the two guards distinguishable.
    """
    bad = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([0], pa.int64()),
            "ratio": pa.array([0.0], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="effort"):
        hierarchical_estimate(bad, [1])


def test_a_null_in_a_key_or_a_count_is_rejected():
    """A null cell/week silently vanishes from the grid; a null n/total
    poisons a denominator into None and crashes far from the cause."""
    for col, arr in (
        ("cell", pa.array([None], pa.string())),
        ("week", pa.array([None], pa.int8())),
        ("n", pa.array([None], pa.int64())),
        ("total", pa.array([None], pa.int64())),
        ("species_key", pa.array([None], pa.int64())),
    ):
        cols = {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
            "ratio": pa.array([1.0], pa.float64()),
        }
        cols[col] = arr
        with pytest.raises(ValueError, match="null"):
            hierarchical_estimate(pa.table(cols), [1])


def test_a_ratio_table_row_for_a_week_outside_1_52_is_rejected():
    """Contract 6 on the INPUT side. Upstream promises 1..52; if that ever
    breaks, the profile for week 0 or 53 would be built and then silently
    never emitted, quietly deleting records from the cell profile."""
    bad = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([0], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
            "ratio": pa.array([1.0], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="week"):
        hierarchical_estimate(bad, [1])


def test_a_wrong_input_dtype_is_rejected_not_cast():
    """Rule (C): an int64 `week` means someone bypassed the pipeline. Casting
    here would be a second derivation of a value derived once upstream."""
    bad = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int64()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
            "ratio": pa.array([1.0], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="int8"):
        hierarchical_estimate(bad, [1])


def test_effort_contract_errors_are_not_swallowed():
    """Contract 9: this module contains no try/except around upstream.

    Checked on the PARSED module, not on its text: the docstring says the
    words "try/except" and a substring search on the source would match its
    own documentation and pass regardless of the code.
    """
    import ast

    src = (
        Path(__file__).parent.parent.parent
        / "src"
        / "birdology"
        / "prior"
        / "backoff.py"
    ).read_text()
    tree = ast.parse(src)
    handlers = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.Try, ast.ExceptHandler))
    ]
    assert handlers == [], (
        "a try/except here would turn an upstream contract violation into a "
        "quietly wrong prior"
    )
    # PRECONDITION: the checker really can see a handler when one exists.
    assert len(
        [
            n
            for n in ast.walk(ast.parse("try:\n    pass\nexcept ValueError:\n    pass\n"))
            if isinstance(n, (ast.Try, ast.ExceptHandler))
        ]
    ) == 2


def test_the_shrunk_estimate_is_the_documented_formula():
    """An explicit numeric anchor for the whole three-level chain, so a
    refactor that changes the arithmetic cannot pass on inequalities alone.

    The cell deliberately holds a SECOND week, so its annual denominator
    (110) differs from the leaf week's (10). Without that the two levels
    share both numerator and denominator, and the formula is symmetric
    enough that SWAPPING the two shrinkage weights gives a bit-identical
    answer -- a fixture that anchors nothing. Asserted below.
    """
    rows = [(1, "c", 5, 3), (2, "c", 5, 7), (1, "c", 9, 1), (2, "c", 9, 99), (2, "d", 20, 90)]
    out = _p(rows, [1], cells=["c"], weeks=[5])

    p_global = 4 / 200
    p_cell = (4 + 50.0 * p_global) / (110 + 50.0)
    p_week = (3 + 10.0 * p_cell) / (10 + 10.0)
    assert out[(1, "c", 5)] == pytest.approx(p_week, rel=1e-12)

    # PRECONDITION: the fixture must distinguish the two weights' ROLES.
    swapped_cell = (4 + 10.0 * p_global) / (110 + 10.0)
    swapped_week = (3 + 50.0 * swapped_cell) / (10 + 50.0)
    assert swapped_week != pytest.approx(p_week, rel=1e-9), (
        "swapping cell_weight and cell_week_weight leaves this fixture "
        "unchanged, so it cannot anchor which weight applies at which level"
    )


def test_a_non_positive_n_is_rejected():
    """Contract 3, on the input side and with teeth.

    `target_group_ratios` cannot produce `n <= 0`, so this row can only come
    from a hand-built table -- which is exactly how a zero-filled "absence"
    would arrive. Accepting it conflates row-absence (nobody looked) with an
    observed zero, the failure that corrupts a target-group prior with no
    error anywhere.
    """
    for bad_n in (0, -5):
        bad = pa.table(
            {
                "species_key": pa.array([1], pa.int64()),
                "cell": pa.array(["c"], pa.string()),
                "week": pa.array([5], pa.int8()),
                "n": pa.array([bad_n], pa.int64()),
                "total": pa.array([10], pa.int64()),
                "ratio": pa.array([0.0], pa.float64()),
            }
        )
        with pytest.raises(ValueError, match="n="):
            hierarchical_estimate(bad, [1])


# ===========================================================================
# Tests added to close mutation survivors. Each names the mutant it kills.
# ===========================================================================


def test_an_absent_species_is_at_the_floor_in_every_cell_INCLUDING_rich_ones():
    """The floor holds wherever the species is missing, not only in empty cells."""
    rows = [(1, "c", 5, 1000), (2, "c", 5, 1000), (1, "d", 9, 1)]
    out = _p(rows, [999], cells=["c", "d", "empty"], weeks=[5, 30])
    assert out and all(v == pytest.approx(FLOOR) for v in out.values())


def test_the_floor_on_p_global_CHANGES_the_answer_above_one_over_floor():
    """Kills B10 by name: the floor dropped from `p_global`.

    This mutant was once retired as "equivalent" on a sweep measuring a maximum
    difference of 3.1e-10. The sweep was wrong, and wrong in the way an
    equivalence argument fails most dangerously: it never entered the regime
    where the mutated branch is LIVE.

    `max(p_global, floor)` only does anything when the raw global share falls
    below `floor`, i.e. when `global_n > 1/floor` -- above a million records.
    Every fixture in that sweep was tiny, so `p_global` was always already
    above the floor and the two chains were arithmetically identical. That is
    not equivalence; it is a fixture that never reaches the code.

    Above the threshold the difference is in the FIFTH significant digit, and
    it is a difference in the direction of under-confidence for a species that
    genuinely holds the whole of its cell-week. The regime is the normal GBIF
    one, so it is pinned exactly.
    """
    rows = [(2, "big", 5, 2_000_000), (1, "c", 5, 1)]
    params = BackoffParams()

    # PRECONDITIONS: the mutated branch must be REACHABLE in this fixture.
    global_n = 2_000_000 + 1
    p_global_raw = 1 / global_n
    assert global_n > 1 / params.floor, (
        "the global denominator is below 1/floor, so `max(p_global, floor)` is "
        "a no-op here and this fixture cannot see the floor at all"
    )
    assert p_global_raw < params.floor

    # The full chain, written out with the floor and without it. `p` for
    # species 1 in cell `c` is not near the floor -- it is ~0.109 -- so this is
    # not a contrived probe of a clamped value.
    p_cell = (1 + params.cell_weight * params.floor) / (1 + params.cell_weight)
    expected = (1 + params.cell_week_weight * p_cell) / (
        1 + params.cell_week_weight
    )
    unfloored_cell = (1 + params.cell_weight * p_global_raw) / (
        1 + params.cell_weight
    )
    unfloored = (1 + params.cell_week_weight * unfloored_cell) / (
        1 + params.cell_week_weight
    )
    assert unfloored != pytest.approx(expected, rel=1e-9), (
        "with and without the `p_global` floor this fixture gives the same "
        "answer at rel=1e-9, so it cannot pin the floor"
    )

    out = _p(rows, [1], cells=["c"], weeks=[5], params=params)
    assert out[(1, "c", 5)] == pytest.approx(expected, rel=1e-9)
    assert out[(1, "c", 5)] == pytest.approx(0.10873529411764705, rel=1e-12)


def test_the_absence_distinction_is_STRICT_below_the_floor_threshold():
    """The ordering the floor exists to protect, asserted STRICTLY.

    Three quantities for one species in one cell, on a deliberately LARGE
    denominator (900 000 records -- just below `1/floor`):

      * a week that WAS surveyed and did not hold the species: lowest, because
        its own effort argues against presence;
      * a week nobody surveyed: the cell profile itself, higher;

    and the surveyed-absent value must sit strictly BELOW the cell profile
    while staying strictly ABOVE the floor. If either inequality degenerates
    to equality the prior has stopped distinguishing "looked and did not find"
    from "nobody looked", which is the whole point of the level structure.

    The right-hand relation is an IDENTITY, not an inequality: an unvisited
    cell-week has no local data, so `_shrink(0, 0, p_cell, w)` is exactly
    `p_cell`. It is asserted as such rather than dressed up as `<`.
    """
    rows = [(1, "c", 5, 250_000), (2, "c", 5, 250_000), (2, "c", 6, 400_000)]
    params = BackoffParams()
    out = _p(rows, [1], cells=["c"], weeks=[5, 6, 30], params=params)

    global_n = 500_000 + 400_000
    # PRECONDITION: BELOW the threshold, which is the only regime where the
    # distinction survives at all (see the collapse test below).
    assert global_n < 1 / params.floor
    p_global = 250_000 / global_n
    assert p_global > params.floor
    p_cell = (250_000 + params.cell_weight * p_global) / (
        global_n + params.cell_weight
    )

    visited_without_species = out[(1, "c", 6)]
    unvisited = out[(1, "c", 30)]

    assert unvisited == pytest.approx(p_cell, rel=1e-12)
    assert visited_without_species < p_cell
    assert params.floor < visited_without_species, (
        "the surveyed-absent value has been clamped to the floor, so it is no "
        "longer distinguishable from a species never recorded anywhere"
    )
    assert visited_without_species < unvisited


def test_the_absence_distinction_COLLAPSES_above_1_over_floor():
    """The stated LIMITATION, tested so the boundary is known rather than met.

    Above `global_n > 1/floor` the floor clamps both ends of the ordering the
    previous test asserts, and a species SURVEYED AND NOT FOUND becomes
    bit-identical to a species NEVER RECORDED ANYWHERE. This is a property of
    the method, recorded in the module docstring and in the design spec.

    It is asserted here rather than left as a surprise: if a future change to
    `floor` or to the shrinkage moves this boundary, this test says so instead
    of the paper quietly claiming a distinction it does not have.
    """
    rows = [(1, "c", 5, 1), (2, "c", 5, 999_999), (2, "c", 6, 500_000)]
    params = BackoffParams()
    out = _p(rows, [1, 999], cells=["c"], weeks=[5, 6, 30], params=params)

    global_n = 1_000_000 + 500_000
    # PRECONDITION: ABOVE the threshold. Below it the values differ.
    assert global_n > 1 / params.floor

    surveyed_absent = out[(1, "c", 6)]
    never_recorded = out[(999, "c", 30)]
    assert surveyed_absent == params.floor
    assert never_recorded == params.floor
    assert surveyed_absent == never_recorded, (
        "if these have become distinguishable the stated limitation in the "
        "module docstring and the design spec is now wrong and must be revised"
    )


def test_cell_week_counts_summing_above_their_own_total_are_rejected():
    """Kills B46 by name: the cell-week count-coherence guard removed.

    `n <= total` per row, one `total` per cell-week and no duplicate
    (species, cell, week) key are each satisfiable by an INCOHERENT table:
    two partial RatioTables covering DISJOINT species of the same cell-week.
    Concatenated they carry no duplicate key, agreeing totals and a legal `n`
    on every row -- and 60 + 80 records against a total of 100 emits
    p = [0.6, 0.8], shares summing to 1.4.

    `total` counts ALL bird records in the cell-week, so the species present
    in it cannot out-count it. This is the last door left open onto the
    `pa.concat_tables`-of-partials hazard the module docstring names.
    """
    incoherent = pa.table(
        {
            "species_key": pa.array([1, 2], pa.int64()),
            "cell": pa.array(["c", "c"], pa.string()),
            "week": pa.array([5, 5], pa.int8()),
            "n": pa.array([60, 80], pa.int64()),
            "total": pa.array([100, 100], pa.int64()),
            "ratio": pa.array([0.6, 0.8], pa.float64()),
        }
    )
    # PRECONDITIONS: every OTHER guard is satisfied, so only the coherence
    # check can be what rejects this.
    keys = [(r["species_key"], r["cell"], r["week"]) for r in incoherent.to_pylist()]
    assert len(keys) == len(set(keys)), "a duplicate key would reject this first"
    assert set(incoherent.column("total").to_pylist()) == {100}
    assert all(
        r["n"] <= r["total"] for r in incoherent.to_pylist()
    ), "a per-row n > total would reject this first"
    assert sum(incoherent.column("n").to_pylist()) == 140 > 100

    with pytest.raises(ValueError, match="summing to 140"):
        hierarchical_estimate(incoherent, [1, 2], cells=["c"], weeks=[5])


def test_a_coherent_cell_week_summing_exactly_to_its_total_is_accepted():
    """The coherence guard must be `>`, not `>=`.

    A cell-week where every record belongs to a listed species sums EXACTLY to
    its total, and that is the ordinary complete-table case -- rejecting it
    would reject the module's main input.
    """
    exact = _counts([(1, "c", 5, 40), (2, "c", 5, 60)])
    assert exact.column("total").to_pylist() == [100, 100]
    out = _p([(1, "c", 5, 40), (2, "c", 5, 60)], [1, 2], cells=["c"], weeks=[5])
    assert len(out) == 2


def test_an_empty_table_with_DEFAULT_weeks_yields_an_EMPTY_grid():
    """Kills B48 by name: a fallback week invented for an empty table.

    The default axes are the OBSERVED axes. An empty table has no observed
    weeks, so the grid is empty -- even when `cells=` names cells explicitly.
    The module deliberately does NOT invent a week here: week 1 (or any other)
    would be an arbitrary shape decision made silently on the caller's behalf,
    and a caller who wants a grid over an empty table must name its `weeks=`.

    Pinned in both directions, because the previous implementation's
    `if not all_weeks: all_weeks = [1]` was reachable only this way and no test
    saw it: replacing that condition with `if False:` survived the whole suite.
    """
    empty = _counts([])
    assert empty.num_rows == 0

    assert hierarchical_estimate(empty, [1]).num_rows == 0
    # `cells=` alone is not enough: the WEEK axis is what is empty.
    assert hierarchical_estimate(empty, [1], cells=["a", "b"]).num_rows == 0

    # Naming the weeks is how a caller gets a grid, and it still gets floor.
    named = _lookup(hierarchical_estimate(empty, [1], cells=["a"], weeks=[3]))
    assert named == {(1, "a", 3): pytest.approx(FLOOR)}


def test_a_wrong_input_dtype_is_not_silently_CAST_to_the_required_schema():
    """Kills B47 by name: `_validate_table` replaced by a silent cast.

    Rejecting an int64 `week` and CASTING it are two different behaviours, and
    only one of them raises. A cast is rule (C)'s second derivation: `week` and
    `cell` are derived exactly once, upstream, and a table arriving with the
    wrong dtype means something bypassed that derivation. Casting would make
    the bypass invisible and produce a plausible prior from a table nobody
    validated.

    The existing dtype test asserts the raise; this one asserts the cast is not
    quietly equivalent, by checking the message names the dtype rather than
    accepting any failure.
    """
    wrong = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int64()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
            "ratio": pa.array([1.0], pa.float64()),
        }
    )
    # PRECONDITION: a cast really would succeed, so raising is a CHOICE.
    castable = wrong.cast(
        pa.schema(
            [
                pa.field("species_key", pa.int64()),
                pa.field("cell", pa.string()),
                pa.field("week", pa.int8()),
                pa.field("n", pa.int64()),
                pa.field("total", pa.int64()),
                pa.field("ratio", pa.float64()),
            ]
        )
    )
    assert castable.column("week").to_pylist() == [5]

    with pytest.raises(ValueError, match="must be int8, got int64"):
        hierarchical_estimate(wrong, [1])


def test_a_negative_weight_is_REJECTED_rather_than_silently_clamped():
    """The negative-weight path that used to be merely BOUNDED is now closed.

    This test replaces `test_the_upper_clamp_holds_when_a_weight_is_negative`
    and the replacement is deliberate rather than incidental, so the history
    is recorded here: the old test passed a `cell_week_weight` of -5.0 and
    asserted that the output clamp bounded the damage at 1.0. That fixture
    really did drive the unclamped value to 1.75 (the arithmetic is
    reproduced below and still asserted), and the clamp really did bound it
    -- but a caller who asks for a negative pseudo-count is asking for the
    coarser level's evidence to be SUBTRACTED, and an artifact of clamped
    1.0s is exactly the plausible-looking constant this round exists to stop.

    The consequence for the mutation harness is recorded there: with negative
    weights rejected, no validated input can reach the upper clamp, so B11
    (the clamp removed) is retired as EQUIVALENT with a reachability
    argument rather than a survivor.
    """
    p_global = 10 / 100
    p_cell = (10 + 50.0 * p_global) / (10 + 50.0)
    unclamped = (10 + -5.0 * p_cell) / (10 + -5.0)
    assert unclamped > 1.0, (
        "the fixture no longer drives p above 1, so it no longer demonstrates "
        "what the rejection is protecting against"
    )

    rows = [(1, "c", 5, 10), (2, "d", 9, 90)]
    with pytest.raises(ValueError, match="negative"):
        _p(
            rows, [1], cells=["c"], weeks=[5],
            params=BackoffParams(cell_week_weight=-5.0),
        )


# ===========================================================================
# THE PARAMETER CONTRACT (round 10 / brief A).
#
# `SmoothingParams` was validated and `BackoffParams` was not. Measured at
# HEAD edd0723: `cell_weight=0.0`, `cell_weight=-1.0`, `floor=0.0`,
# `floor=-1.0` and `floor=2.0` all returned a plausible table with NOTHING
# raised, and `floor=2.0` made every value exactly 1.0. Two values raised at
# all, both a bare ZeroDivisionError from inside `_shrink`.
#
# Each test below asserts on the MESSAGE, not on a bare `ValueError`: a
# `ValueError` from this module could come from any of the two dozen table
# checks above, so matching the type alone would let a test pass while the
# parameter guard did not exist.
# ===========================================================================


_PARAM_ROWS = [(1, "c", 5, 10), (2, "c", 5, 90)]


def _with(**kwargs):
    """Run the module's normal path with one parameter overridden."""
    return _p(_PARAM_ROWS, [1, 2], cells=["c"], weeks=[5], params=BackoffParams(**kwargs))


# ---- the two weights: finiteness, then range, checked separately ----------


def test_a_non_finite_shrinkage_weight_is_rejected():
    """nan and inf, for BOTH weights.

    Separate from the range test because `nan` fails every comparison:
    `nan < 0.0` is False, so an ordering check alone passes a nan straight
    through to the arithmetic, where it makes every emitted probability nan
    with no error. `smoothing.py` splits these two clauses for exactly this
    reason and the same reasoning applies here.
    """
    for field in ("cell_week_weight", "cell_weight"):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ValueError, match="not finite"):
                _with(**{field: bad})


def test_a_nan_weight_really_would_poison_every_value_without_the_guard():
    """The harm the finiteness clause prevents, demonstrated not asserted.

    A nan weight does not raise anywhere in the arithmetic: it propagates
    through `_shrink` into every emitted `p`, and `min(max(nan, floor), 1.0)`
    returns nan rather than the floor, so the artifact is silently all-nan.
    """
    nan = float("nan")
    p_cell = (10 + nan * 0.1) / (100 + nan)
    poisoned = min(max(p_cell, FLOOR), 1.0)
    assert poisoned != poisoned, (
        "a nan weight no longer produces nan, so the finiteness clause is "
        "guarding against something that cannot happen"
    )


def test_a_negative_shrinkage_weight_is_rejected():
    """A pseudo-count below zero SUBTRACTS the coarser level's evidence.

    Both fields, and at values that behaved DIFFERENTLY at HEAD: -1.0 and
    -3.0 returned a plausible table with no error at all, while -4.0 raised a
    bare ZeroDivisionError purely because that fixture's denominator was 4.
    The guard must not depend on which of those a caller happens to hit.
    """
    for field in ("cell_week_weight", "cell_weight"):
        for bad in (-1.0, -3.0, -4.0, -1e-12):
            with pytest.raises(ValueError, match="negative"):
                _with(**{field: bad})


def test_the_weight_guard_covers_BOTH_weights_not_only_the_first():
    """A guard that loops over one field is a guard over half the contract.

    Asserted as a pair so a mutant that drops `cell_weight` from the loop is
    caught by name: the two fields are checked with the same bad value, and
    the messages must name the field that was actually wrong.
    """
    with pytest.raises(ValueError, match="cell_week_weight=-1.0"):
        _with(cell_week_weight=-1.0)
    with pytest.raises(ValueError, match="cell_weight=-1.0"):
        _with(cell_weight=-1.0)


# ---- zero shrinkage is ACCEPTED, and the empty level it implies is defined -


def test_a_zero_shrinkage_weight_is_ACCEPTED():
    """`0.0` means "no shrinkage at this level", which a sweep may ask for.

    The published sweep already goes down to `cell_week_weight = 0.5`; zero
    is the limit of that direction, and a guard that rejected it would be
    narrower than the sweep it exists to protect. It is accepted for both
    fields, together and separately.
    """
    for params in (
        BackoffParams(cell_week_weight=0.0),
        BackoffParams(cell_weight=0.0),
        BackoffParams(cell_week_weight=0.0, cell_weight=0.0),
    ):
        out = _p(_PARAM_ROWS, [1], cells=["c"], weeks=[5], params=params)
        assert all(v == v for v in out.values())


def test_zero_shrinkage_on_an_EMPTY_cell_week_returns_the_PARENT_not_zero():
    """The 0/0 that zero shrinkage used to hit is DEFINED, not repaired.

    With `cell_week_weight = 0.0` and a cell-week nobody visited, `_shrink`
    is handed `local_den = 0` and `weight = 0`: at HEAD that was a bare
    `ZeroDivisionError`. A level with no records carries no information, so
    the answer is the COARSER level's value -- the same limit the expression
    has as `local_den -> 0` for any positive weight.

    THE FIXTURE IS BUILT SO THE FLOOR CANNOT SPOOF IT. If the empty case
    returned `0.0` instead, the output clamp `min(max(p, floor), 1.0)` would
    silently raise it to `floor` and an assertion like `> 0` would still
    pass. The cell profile here is 0.1, five orders of magnitude above the
    default floor, and it is asserted EXACTLY.
    """
    out = _p(
        _PARAM_ROWS, [1], cells=["c"], weeks=[40],
        params=BackoffParams(cell_week_weight=0.0),
    )
    p_global = 10 / 100
    p_cell = (10 + 50.0 * p_global) / (100 + 50.0)
    assert p_cell > 1000 * FLOOR, (
        "the parent value is too near the floor to distinguish it from a "
        "zero that the output clamp lifted"
    )
    assert out[(1, "c", 40)] == pytest.approx(p_cell, rel=1e-12)


def test_zero_shrinkage_on_an_EMPTY_CELL_returns_the_GLOBAL_profile():
    """The same 0/0, one level up: a requested cell with no records at all."""
    out = _p(
        _PARAM_ROWS, [1], cells=["empty"], weeks=[5],
        params=BackoffParams(cell_weight=0.0, cell_week_weight=0.0),
    )
    p_global = 10 / 100
    assert p_global > 1000 * FLOOR
    assert out[(1, "empty", 5)] == pytest.approx(p_global, rel=1e-12)


# ---- the floor is a probability, strictly inside (0, 1) -------------------


def test_a_non_finite_floor_is_rejected():
    """A nan floor is a SILENT NO-OP, which is why finiteness is its own check.

    `max(p, nan)` returns `p` and `min(p, 1.0)` leaves it, so a nan floor
    does not poison anything and does not raise -- the floor the module
    guarantees simply stops existing. An ordering check cannot see it.
    """
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match=r"floor=.* is not finite"):
            _with(floor=bad)


def test_a_nan_floor_really_would_silently_disable_the_floor():
    """The harm the floor's finiteness clause prevents, demonstrated."""
    nan = float("nan")
    assert min(max(0.0, nan), 1.0) == 0.0, (
        "a nan floor no longer passes a hard zero through, so the floor's "
        "finiteness clause is guarding against something that cannot happen"
    )


def test_a_floor_of_zero_is_rejected():
    """`0.0` re-admits the hard zero the module's docstring exists to forbid."""
    with pytest.raises(ValueError, match="re-admits the hard zero"):
        _with(floor=0.0)


def test_a_negative_floor_is_rejected():
    with pytest.raises(ValueError, match="re-admits the hard zero"):
        _with(floor=-1.0)


def test_a_floor_of_one_is_rejected():
    """Exactly 1.0 IS a probability and still flattens the artifact.

    `min(max(p, 1.0), 1.0)` is the constant 1.0, so a floor of certainty
    produces the identical information-free artifact that `floor=2.0` does.
    Admitting it because it happens to lie in [0, 1] would leave the
    documented harm reachable through the boundary.
    """
    with pytest.raises(ValueError, match="flattens the whole artifact"):
        _with(floor=1.0)


def test_a_floor_above_one_is_rejected():
    """The reproduced defect: `floor=2.0` made every value exactly 1.0."""
    with pytest.raises(ValueError, match="flattens the whole artifact"):
        _with(floor=2.0)


def test_a_floor_of_two_really_would_flatten_every_value():
    """The harm, demonstrated on the emitted expression rather than claimed."""
    assert {min(max(p, 2.0), 1.0) for p in (1e-9, 0.3, 0.5, 1.0)} == {1.0}


# ---- the accepted domain: the guard must not be too strict to sweep with ---


def test_the_swept_parameter_values_are_all_ACCEPTED():
    """The sweep is a real use and the guard must not break it.

    These are the values the retention table in `BackoffParams`' docstring
    was measured at, plus the floor's two documented regimes. A guard that
    rejected any of them would make the paper's sweep unbuildable, which is
    a worse failure than the one it is fixing.
    """
    accepted = [
        BackoffParams(cell_week_weight=0.5),
        BackoffParams(cell_week_weight=2.0),
        BackoffParams(cell_week_weight=10.0),
        BackoffParams(cell_week_weight=50.0),
        BackoffParams(cell_weight=5.0),
        BackoffParams(cell_weight=50.0),
        BackoffParams(cell_weight=500.0),
        BackoffParams(floor=1e-6),
        BackoffParams(floor=1e-3),
        BackoffParams(floor=0.5),
        # The open boundaries themselves, from both sides.
        BackoffParams(cell_week_weight=0.0, cell_weight=0.0),
        BackoffParams(floor=5e-324),
        BackoffParams(floor=1.0 - 1e-16),
    ]
    for params in accepted:
        out = _p(_PARAM_ROWS, [1, 2], cells=["c"], weeks=[5], params=params)
        assert out, params


def test_the_defaults_are_unchanged_and_accepted():
    """This round adds a guard; it does not retune the artifact."""
    assert BackoffParams().cell_week_weight == 10.0
    assert BackoffParams().cell_weight == 50.0
    assert BackoffParams().floor == FLOOR == 1e-6
    _p(_PARAM_ROWS, [1, 2], cells=["c"], weeks=[5], params=BackoffParams())


def test_a_rejected_parameter_is_never_repaired_into_a_default():
    """No clamp, no cast, no substitution: the call must not succeed at all.

    A silent repair is the worst available outcome here, because
    `save_prior` writes the params object into the sidecar: the artifact
    would carry an honest record of constants the build did not use.
    """
    for params in (
        BackoffParams(floor=2.0),
        BackoffParams(floor=0.0),
        BackoffParams(cell_weight=-1.0),
        BackoffParams(cell_week_weight=float("nan")),
    ):
        with pytest.raises(ValueError):
            _p(_PARAM_ROWS, [1], cells=["c"], weeks=[5], params=params)


def test_the_parameter_guard_runs_before_any_estimate_is_produced():
    """Rejected on an EMPTY table too: it is validation, not a side effect.

    An empty table takes the short path through the row loop and emits the
    floor everywhere, so a guard that only fired once some arithmetic went
    wrong would miss it entirely.
    """
    with pytest.raises(ValueError, match="flattens the whole artifact"):
        hierarchical_estimate(
            _counts([]), [1], BackoffParams(floor=2.0), cells=["c"], weeks=[5]
        )


def test_a_duplicate_species_cell_week_ROW_is_rejected():
    """A RatioTable has ONE row per (species, cell, week).

    `aggregate_counts` group_by guarantees it upstream, but concatenating two
    independently-computed RatioTables does not: the duplicate rows each carry
    their own partial `n` against a `total` that already covers the whole
    cell-week. Summing them double-counts the numerator against a single
    denominator (a share of 0.6 where the truth is 0.3); keeping one silently
    discards records. Neither is recoverable here, so it is rejected.

    This is also why the mutants on this guard come in a PAIR: B27 removes the
    check and ACCUMULATES the numerator (0.6 where the truth is 0.3), B27b
    removes it and OVERWRITES (records silently discarded). Both are killed
    here; neither is equivalent once the guard itself is the thing mutated.
    """
    dup = pa.concat_tables(
        [
            _counts([(1, "c", 5, 3), (2, "c", 5, 7)]),
            _counts([(1, "c", 5, 3), (2, "c", 5, 7)]),
        ]
    )
    # PRECONDITION: the key really does repeat.
    keys = [(r["species_key"], r["cell"], r["week"]) for r in dup.to_pylist()]
    assert len(keys) != len(set(keys))

    with pytest.raises(ValueError, match="duplicate"):
        hierarchical_estimate(dup, [1], cells=["c"], weeks=[5])


def test_a_ratio_table_missing_only_TOTAL_is_rejected():
    """Kills B31: the required-column check narrowed to skip `total`.

    The existing missing-columns test drops `n` AND `total` together, so a
    check that covers only `n` still passes it. `total` is the effort weight
    that contract 4 exists to preserve, so its absence must be named
    specifically.
    """
    no_total = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "ratio": pa.array([1.0], pa.float64()),
        }
    )
    # PRECONDITION: `n` is present, so only `total` can be the cause.
    assert "n" in no_total.column_names
    assert "total" not in no_total.column_names
    with pytest.raises(ValueError, match="total"):
        hierarchical_estimate(no_total, [1])


def test_a_total_of_zero_is_rejected_even_when_n_is_zero_too():
    """Kills B34: the `total >= 1` row check removed.

    With `n=1, total=0` the `n <= total` guard fires first, so removing the
    effort check is invisible. `n=0, total=0` is the shape that isolates it --
    and it is the realistic one, being exactly what a zero-filled dense grid
    produces for a cell-week nobody visited (contract 3).

    Both guards are asserted by MESSAGE, so neither can stand in for the
    other.
    """
    zero_fill = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([0], pa.int64()),
            "total": pa.array([0], pa.int64()),
            "ratio": pa.array([0.0], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="effort") as caught:
        hierarchical_estimate(zero_fill, [1])
    # PRECONDITION: it is the EFFORT guard talking, not the pairing guard.
    assert "exceeds" not in str(caught.value)


def test_the_effort_guard_RAISES_rather_than_printing():
    """Kills B37: `raise ValueError(` replaced by `print(`.

    A warn-and-continue path leaves the function returning a table built from
    a zero denominator -- the exact pattern the plan's rule B forbids, and one
    that a `pytest.raises` on a DIFFERENT guard cannot detect. Assert that
    nothing is returned and nothing is printed.
    """
    bad = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([0], pa.int64()),
            "total": pa.array([0], pa.int64()),
            "ratio": pa.array([0.0], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="effort"):
        hierarchical_estimate(bad, [1])


def test_the_cell_level_weight_applies_at_the_CELL_level(capsys):
    """Kills B13 and B14 by naming the level each weight governs.

    `cell_weight` must move an estimate that is scored PURELY from the cell
    profile (an unvisited week), and `cell_week_weight` must not. The two are
    distinguishable only on a fixture where the cell's denominator differs
    from the leaf week's, which is asserted.
    """
    rows = [(1, "c", 5, 1), (2, "c", 5, 99), (2, "d", 40, 900)]
    # An unvisited week reads the cell profile; only `cell_weight` moves it.
    weak = _p(
        rows, [1], cells=["c"], weeks=[30], params=BackoffParams(cell_weight=0.1)
    )[(1, "c", 30)]
    strong = _p(
        rows, [1], cells=["c"], weeks=[30], params=BackoffParams(cell_weight=1000.0)
    )[(1, "c", 30)]
    assert strong < weak, "cell_weight does not govern the cell level"

    # `cell_week_weight` cannot move a week with no local data at all.
    a = _p(
        rows, [1], cells=["c"], weeks=[30],
        params=BackoffParams(cell_week_weight=0.1),
    )[(1, "c", 30)]
    b = _p(
        rows, [1], cells=["c"], weeks=[30],
        params=BackoffParams(cell_week_weight=1000.0),
    )[(1, "c", 30)]
    assert a == pytest.approx(b, rel=1e-12), (
        "cell_week_weight changed a week that has no local data, so it is "
        "being applied at the cell level"
    )


def test_an_unvisited_cell_week_is_scored_from_the_parent_not_from_zero_over_zero(
):
    """Kills B17 by name: `_shrink` reduced to the raw local share.

    The existing no-NaN test is satisfied by the `0.0` fallback such a
    reduction would use, so it never names this. The parent value is asserted
    exactly: an unvisited week must EQUAL its cell profile, which a raw local
    share cannot produce.
    """
    rows = [(1, "c", 5, 50), (2, "c", 5, 50), (2, "d", 40, 900)]
    out = _p(rows, [1], cells=["c"], weeks=[30])
    p_global = 50 / 1000
    p_cell = (50 + 50.0 * p_global) / (100 + 50.0)
    assert out[(1, "c", 30)] == pytest.approx(p_cell, rel=1e-12)
    assert out[(1, "c", 30)] > 0.0


def test_a_cell_profile_sums_its_weeks_rather_than_keeping_the_last(
):
    """Kills B28 by name: the cell numerator overwritten instead of summed.

    Two weeks contribute unequally to the cell profile; keeping only the last
    row's `n` would score the cell from whichever week Arrow emitted last.
    The expected value is asserted exactly, so 'last wins' cannot coincide.
    """
    rows = [(1, "c", 5, 1), (2, "c", 5, 99), (1, "c", 9, 40), (2, "c", 9, 60)]
    out = _p(rows, [1], cells=["c"], weeks=[30])
    p_global = 41 / 200
    p_cell = (41 + 50.0 * p_global) / (200 + 50.0)
    assert out[(1, "c", 30)] == pytest.approx(p_cell, rel=1e-12)


def test_the_requested_WEEKS_axis_is_honoured_even_when_all_are_unobserved():
    """Kills B41 by name: the `weeks=` argument ignored.

    The empty-cell-week test also exercises `cells=`, so a mutant that keeps
    `cells=` and drops `weeks=` fails it for the wrong reason. Here every
    requested week is unobserved, which isolates the weeks axis.
    """
    ratios = _counts([(1, "c", 5, 10)])
    out = hierarchical_estimate(ratios, [1], cells=["c"], weeks=[30, 31])
    assert sorted(out.column("week").to_pylist()) == [30, 31]
    assert 5 not in out.column("week").to_pylist()


# ===========================================================================
# THE END-TO-END ASSERTION (round 10 / brief A, item 4).
#
# A unit test on `BackoffParams` does NOT prove the CLI is protected:
# `build_from_archive` constructs its own params object and hands it to
# `estimate_over_full_grid`, which hands it to `hierarchical_estimate`. A
# guard placed anywhere short of that last call would leave the reproduced
# defect live through the only path that writes artifacts.
#
# The reproduced defect, at HEAD edd0723, res 3, 300 records, 3 species:
# `BackoffParams(floor=2.0)` wrote SIX valid, fully provenanced `.npz` files
# -- DOI, manifest, licence, an honest sidecar record of the parameters -- in
# which every value was exactly 1.0. So the assertion is not merely that an
# exception is raised: it is that NOTHING WAS WRITTEN.
#
# No network: the archive is a local zip built here by `zipfile.writestr`,
# exactly as `test_build_prior.py` does it. `build_from_archive` does not
# import a download client.
# ===========================================================================

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

_E2E_HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)

#: Four well-separated Danish points and three dates in distinct ISO weeks,
#: so the build has a real grid rather than one cell-week.
_E2E_POINTS = [(55.6867, 12.5700), (55.9000, 12.5000), (56.1629, 10.2039)]
_E2E_DATES = ("2025-01-07", "2025-06-10", "2025-11-14")
_E2E_SPECIES = (1001, 1002, 1003)

#: Records per (species, point, date), DELIBERATELY UNBALANCED.
#:
#: With a flat four-records-each fixture every species' share is exactly 1/3
#: in every cell-week, so the artifact is a GLOBALLY CONSTANT grid -- and a
#: constant grid is returned unchanged by every permutation, so both shuffle
#: controls came out byte-identical to the full prior. That is finding I1
#: arriving through the fixture: this "the good build works" control test was
#: passing while producing two controls that could not fail, and
#: `controls._refuse_an_unmoved_shuffle` now refuses them.
#:
#: The balance varies by species AND by week, so no level of the hierarchy
#: collapses to a shared number: a constant balance makes an observed
#: cell-week numerically indistinguishable from a copied cell profile
#: (`test_build_prior.py::COUNTS_BY_WEEK` records the same lesson).
_E2E_COUNTS = {
    1001: (7, 4, 2),
    1002: (2, 6, 5),
    1003: (3, 2, 8),
}


def _e2e_archive(tmp_path):
    import zipfile

    rows = []
    i = 0
    for di, date in enumerate(_E2E_DATES):
        for pi, (lat, lon) in enumerate(_E2E_POINTS):
            for sk in _E2E_SPECIES:
                # Rotate the per-week balance by point as well, so the grid
                # carries a space x time INTERACTION and not just two
                # marginals: `separable` of a purely separable prior is that
                # prior, which would be a third degenerate control.
                n = _E2E_COUNTS[sk][(di + pi) % len(_E2E_DATES)]
                for _ in range(n):
                    i += 1
                    rows.append(
                        "\t".join(
                            [str(i), "ds", "pub", str(sk), f"sp{sk}",
                             str(lat + i * 1e-7), str(lon), date, "obs"]
                        )
                    )
    zip_path = tmp_path / "occ.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([_E2E_HEADER, *rows]) + "\n")
    return zip_path


def _e2e_build(tmp_path, out, **kwargs):
    from build_prior import build_from_archive

    return build_from_archive(
        _e2e_archive(tmp_path),
        resolution=3,
        source="dof",
        test_recordings=[],
        out_dir=out,
        gbif_doi="10.15468/dl.test",
        **kwargs,
    )


def test_the_end_to_end_BUILD_writes_a_real_artifact_set_on_good_params():
    """The control: this fixture DOES build, so the refusals below mean something.

    Without it a refusal test could pass because the fixture never reaches
    the backoff at all -- the "coverage that does not exist" failure the
    mutation harness's header warns about, arriving through the fixture
    rather than through a label.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        out = tmp_path / "priors"
        paths = _e2e_build(tmp_path, out)
        assert len(paths) >= 2, paths
        assert sorted(out.glob("*.npz")) == sorted(paths)

        # AND the artifact is not already the constant the defect produces,
        # so `distinct == 1` below really is the defect's signature.
        from birdology.prior.tensor import load_prior

        values = load_prior(paths[0]).values.data
        assert len(set(values.tolist())) > 1, (
            "the good build already produces a constant, so a flattened "
            "artifact would be indistinguishable from a correct one"
        )


def test_a_FLATTENING_floor_cannot_produce_artifacts_through_the_CLI():
    """The reproduced defect, closed end to end: NOTHING is written.

    `floor=2.0` wrote six fully provenanced artifacts of constant 1.0 at
    HEAD. `build_from_archive` has no try/except anywhere on this path, so
    the guard's `ValueError` must reach the caller and the output directory
    must be left with no `.npz` in it -- a partial release under a
    normal-looking filename is the failure the refusal exists to prevent.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        out = tmp_path / "priors"
        with pytest.raises(ValueError, match="flattens the whole artifact"):
            _e2e_build(tmp_path, out, backoff_params=BackoffParams(floor=2.0))
        assert not out.exists() or not list(out.glob("*.npz")), (
            "the build refused but still left artifacts on disk"
        )
        assert not out.exists() or not list(out.glob("*.json")), (
            "the build refused but still left a sidecar or a manifest on disk"
        )


def test_every_rejected_parameter_is_rejected_through_the_CLI_too():
    """One bad value per field, through the real build path.

    `build_from_archive` constructs its own params object and passes it down
    two call layers; a guard that unit tests reach but the CLI does not
    would leave every one of these live in the only path that writes
    artifacts.
    """
    import tempfile

    for params in (
        BackoffParams(floor=0.0),
        BackoffParams(floor=float("nan")),
        BackoffParams(cell_weight=-1.0),
        BackoffParams(cell_week_weight=-1.0),
        BackoffParams(cell_week_weight=float("inf")),
    ):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "priors"
            with pytest.raises(ValueError):
                _e2e_build(tmp_path, out, backoff_params=params)
            assert not out.exists() or not list(out.glob("*.npz")), params
