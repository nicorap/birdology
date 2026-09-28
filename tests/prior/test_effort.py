"""Target-group background: correct for observer effort, not bird abundance."""
import sys
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.effort import target_group_ratios


def _counts(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "n": pa.array([r[3] for r in rows], pa.int64()),
        }
    )


def test_ratio_is_share_of_cell_week_total():
    t = _counts([(1, "c", 5, 3), (2, "c", 5, 1)])
    out = {r["species_key"]: r for r in target_group_ratios(t).to_pylist()}
    assert out[1]["total"] == 4
    assert out[1]["ratio"] == pytest.approx(0.75)
    assert out[2]["ratio"] == pytest.approx(0.25)


def test_ratios_in_a_cell_week_sum_to_one():
    t = _counts([(1, "c", 5, 3), (2, "c", 5, 1), (3, "c", 5, 4)])
    total = sum(r["ratio"] for r in target_group_ratios(t).to_pylist())
    assert total == pytest.approx(1.0)


def test_effort_cancels_between_busy_and_quiet_cells():
    """A city cell with 100x the records gives the same ratio as a rural one."""
    t = _counts(
        [
            (1, "city", 5, 750), (2, "city", 5, 250),
            (1, "rural", 5, 3), (2, "rural", 5, 1),
        ]
    )
    rows = {(r["species_key"], r["cell"]): r["ratio"] for r in target_group_ratios(t).to_pylist()}
    assert rows[(1, "city")] == pytest.approx(rows[(1, "rural")])


def test_cells_are_independent():
    t = _counts([(1, "a", 5, 1), (2, "b", 5, 1)])
    for row in target_group_ratios(t).to_pylist():
        assert row["ratio"] == pytest.approx(1.0)


def test_weeks_are_independent():
    t = _counts([(1, "c", 5, 1), (2, "c", 40, 1)])
    for row in target_group_ratios(t).to_pylist():
        assert row["ratio"] == pytest.approx(1.0)


def test_empty_table_returns_empty():
    assert target_group_ratios(_counts([])).num_rows == 0


# --------------------------------------------------------------------------
# Rule (D): a zero / absent denominator must be handled deliberately.
#
# The denominator is a SUM of the same `n` values that form the numerator, so
# it can only reach zero if some `n` is <= 0, or vanish entirely if the join
# key is null. Both are upstream-contract violations, and both would otherwise
# emit inf/nan or SILENTLY DROP the row -- a shrunken prior that looks mild.
# --------------------------------------------------------------------------


def test_non_positive_count_is_rejected_not_divided_by_zero():
    """n == 0 would make a lone cell-week total 0 and yield nan, not an error."""
    with pytest.raises(ValueError, match="n must be >= 1"):
        target_group_ratios(_counts([(1, "c", 5, 0)]))


def test_negative_count_is_rejected():
    """Two rows summing to zero: 3 + -3 == 0 -> inf ratios, no nan to notice."""
    with pytest.raises(ValueError, match="n must be >= 1"):
        target_group_ratios(_counts([(1, "c", 5, 3), (2, "c", 5, -3)]))


def test_null_cell_is_rejected_rather_than_silently_dropped():
    """A null join key matches nothing in an inner join: the row disappears."""
    t = pa.table(
        {
            "species_key": pa.array([1, 2], pa.int64()),
            "cell": pa.array(["c", None], pa.string()),
            "week": pa.array([5, 5], pa.int8()),
            "n": pa.array([3, 1], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="null"):
        target_group_ratios(t)


def test_null_week_is_rejected_rather_than_silently_dropped():
    t = pa.table(
        {
            "species_key": pa.array([1, 2], pa.int64()),
            "cell": pa.array(["c", "c"], pa.string()),
            "week": pa.array([5, None], pa.int8()),
            "n": pa.array([3, 1], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="null"):
        target_group_ratios(t)


def test_null_n_is_rejected():
    t = pa.table(
        {
            "species_key": pa.array([1, 2], pa.int64()),
            "cell": pa.array(["c", "c"], pa.string()),
            "week": pa.array([5, 5], pa.int8()),
            "n": pa.array([3, None], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="null"):
        target_group_ratios(t)


def test_every_input_row_survives_the_join():
    """The inner join must not drop or duplicate rows.

    A dropped row is the silent failure this whole guard exists for: the prior
    simply lacks a cell-week and nothing anywhere reports it.
    """
    t = _counts([(1, "a", 5, 2), (2, "a", 5, 2), (1, "b", 5, 1), (1, "a", 6, 7)])
    assert target_group_ratios(t).num_rows == t.num_rows


# --------------------------------------------------------------------------
# Rule (C): week/cell types are INHERITED, never re-derived here.
# --------------------------------------------------------------------------


def test_output_schema_is_exact():
    """RatioTable column names, order and types are the Task 6 contract."""
    out = target_group_ratios(_counts([(1, "c", 5, 3), (2, "c", 5, 1)]))
    assert out.schema.names == [
        "species_key",
        "cell",
        "week",
        "n",
        "total",
        "ratio",
    ]
    assert out.schema.field("species_key").type == pa.int64()
    assert out.schema.field("cell").type == pa.string()
    assert out.schema.field("week").type == pa.int8()
    assert out.schema.field("n").type == pa.int64()
    assert out.schema.field("total").type == pa.int64()
    assert out.schema.field("ratio").type == pa.float64()


def test_empty_output_schema_matches_populated_output_schema():
    """An empty result must still be a RatioTable, not a bare CountTable."""
    empty = target_group_ratios(_counts([]))
    populated = target_group_ratios(_counts([(1, "c", 5, 1)]))
    assert empty.schema == populated.schema


def test_week_is_not_re_derived_or_widened():
    """int8 in, int8 out: a local cast would be a second derivation."""
    out = target_group_ratios(_counts([(1, "c", 52, 1)]))
    assert out.schema.field("week").type == pa.int8()
    assert out.to_pylist()[0]["week"] == 52


def test_cell_string_is_passed_through_verbatim():
    """No local casefolding: the cell arrives already normalised upstream."""
    out = target_group_ratios(_counts([(1, "8a1fb46622dffff", 5, 1)]))
    assert out.to_pylist()[0]["cell"] == "8a1fb46622dffff"


def test_a_wrong_week_type_is_rejected_not_cast():
    """int64 week means someone bypassed aggregate_counts; do not paper over it."""
    t = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int64()),
            "n": pa.array([1], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="week"):
        target_group_ratios(t)


def test_a_missing_column_is_rejected():
    t = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
        }
    )
    with pytest.raises(ValueError, match="missing"):
        target_group_ratios(t)


# --------------------------------------------------------------------------
# Ratio arithmetic.
# --------------------------------------------------------------------------


def test_ratio_is_float_division_not_integer_division():
    """1/3 must be 0.333..., not 0. Integer division silently zeroes the prior."""
    out = target_group_ratios(_counts([(1, "c", 5, 1), (2, "c", 5, 2)]))
    rows = {r["species_key"]: r["ratio"] for r in out.to_pylist()}
    assert rows[1] == pytest.approx(1 / 3)
    assert rows[2] == pytest.approx(2 / 3)


def test_total_is_the_cell_week_sum_not_the_global_sum():
    """A global denominator would make effort cancel across cells, not within."""
    out = target_group_ratios(_counts([(1, "a", 5, 3), (2, "b", 5, 1)]))
    totals = {r["cell"]: r["total"] for r in out.to_pylist()}
    assert totals == {"a": 3, "b": 1}


def test_total_is_not_a_row_count():
    """`n` is a SUM upstream; totalling rows instead of `n` would give 2, not 10."""
    out = target_group_ratios(_counts([(1, "c", 5, 7), (2, "c", 5, 3)]))
    assert {r["total"] for r in out.to_pylist()} == {10}


def test_week_52_and_week_1_are_not_adjacent_here():
    """Circular 52<->1 smoothing is Task 7's job, not the denominator's."""
    out = target_group_ratios(_counts([(1, "c", 52, 1), (2, "c", 1, 3)]))
    totals = {r["week"]: r["total"] for r in out.to_pylist()}
    assert totals == {52: 1, 1: 3}


# --------------------------------------------------------------------------
# Rule (A): the effort denominator comes from the GUARDED table only.
# --------------------------------------------------------------------------


def test_denominator_uses_only_the_rows_it_was_given():
    """Guarded input -> guarded denominator.

    If effort were derived from a pre-guard table, the dropped rows would stay
    in the denominator and under-weight every guarded cell-week. Simulate it:
    the same cell-week with and without an extra row must give DIFFERENT
    totals, proving the denominator tracks the supplied rows exactly.
    """
    guarded = _counts([(1, "c", 5, 3)])
    unguarded = _counts([(1, "c", 5, 3), (99, "c", 5, 97)])

    assert target_group_ratios(guarded).to_pylist()[0]["total"] == 3
    assert target_group_ratios(guarded).to_pylist()[0]["ratio"] == pytest.approx(1.0)
    leaked = [r for r in target_group_ratios(unguarded).to_pylist() if r["species_key"] == 1]
    assert leaked[0]["total"] == 100
    assert leaked[0]["ratio"] == pytest.approx(0.03)


def test_module_does_not_read_any_external_source():
    """Rule (A): no path back to the raw archive to rebuild a denominator."""
    import birdology.prior.effort as effort

    source = Path(effort.__file__).read_text()
    for forbidden in ("open(", "read_parquet", "requests", "urllib", "gbif_download"):
        assert forbidden not in source, f"effort.py must not reference {forbidden}"


# --------------------------------------------------------------------------
# Rule (B): guard errors propagate; nothing here swallows them.
# --------------------------------------------------------------------------


def test_leakage_guard_errors_are_not_swallowed():
    """A ValueError raised while building the CountTable must reach the caller."""
    import birdology.prior.effort as effort

    source = Path(effort.__file__).read_text()
    assert "except" not in source, "effort.py must not catch anything"


def test_integration_with_aggregate_counts():
    """The real upstream output feeds straight in, with no adaptation."""
    from birdology.prior.indexing import aggregate_counts

    occurrences = pa.table(
        {
            "species_key": pa.array([1, 1, 2], pa.int64()),
            "cell": pa.array(["8a1fb46622dffff"] * 3, pa.string()),
            "week": pa.array([5, 5, 5], pa.int8()),
        }
    )
    out = target_group_ratios(aggregate_counts(occurrences)).to_pylist()
    rows = {r["species_key"]: r for r in out}
    assert rows[1]["n"] == 2
    assert rows[1]["total"] == 3
    assert rows[1]["ratio"] == pytest.approx(2 / 3)
    assert rows[2]["ratio"] == pytest.approx(1 / 3)
