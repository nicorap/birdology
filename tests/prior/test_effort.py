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


def test_a_cell_week_summing_to_zero_is_rejected_even_when_the_table_is_positive():
    """The guard is per-ROW (`min`), not per-TABLE (`sum`).

    A table-wide `sum(n) > 0` check would pass this fixture: 5 + -5 + 10 == 10.
    But cell 'c' totals ZERO, so its two rows divide by zero and emit
    `ratio = +inf` and `-inf` with `total = 0` -- exactly the poisoned output
    rule (D) exists to prevent. Only a `min` over every row sees the -5.
    """
    t = _counts([(1, "c", 5, 5), (2, "c", 5, -5), (3, "d", 5, 10)])
    assert sum(t.column("n").to_pylist()) > 0
    with pytest.raises(ValueError, match="n must be >= 1"):
        target_group_ratios(t)


# --------------------------------------------------------------------------
# Output post-condition: rows that are KEPT but poisoned.
#
# The post-join row-count invariant only sees rows that VANISH. A row with a
# null `total` or an inf/nan `ratio` keeps the count intact and reads
# downstream as a perfectly ordinary share.
# --------------------------------------------------------------------------


def test_a_zero_total_is_caught_on_output_even_if_validation_is_bypassed(monkeypatch):
    """Defence in depth for the poisoned-row mode.

    With `_validate` disabled, `n = [5, -5]` reaches the divide: the row count
    is preserved, so the join invariant is silent, and the module would emit
    `total = 0` with `ratio = +/-inf`. The output post-condition is the only
    thing standing between that and the prior.
    """
    import birdology.prior.effort as effort

    monkeypatch.setattr(effort, "_validate", lambda counts: None)
    t = _counts([(1, "c", 5, 5), (2, "c", 5, -5)])
    with pytest.raises(ValueError, match="total is null or < 1"):
        effort.target_group_ratios(t)


def test_a_non_finite_ratio_is_caught_on_output():
    """inf/nan must never be emitted as if it were a share.

    Checked on `_check_output` directly with a hand-built row, because the
    total guard fires first on every input that would actually divide by zero.
    Both guards are needed: a numerator/denominator misalignment can leave
    `total >= 1` and still produce a nan.
    """
    import birdology.prior.effort as effort

    bad = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([7], pa.int64()),
            "total": pa.array([1], pa.int64()),
            "ratio": pa.array([float("inf")], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="non-finite"):
        effort._check_output(bad)

    bad_nan = bad.set_column(
        bad.column_names.index("ratio"), "ratio", pa.array([float("nan")], pa.float64())
    )
    with pytest.raises(ValueError, match="non-finite"):
        effort._check_output(bad_nan)


def test_a_ratio_above_one_is_caught_on_output():
    """A share cannot exceed its own denominator.

    Built by hand-poisoning the post-condition's input, because no reachable
    code path produces it -- which is the point: it is the shape a numerator /
    denominator misalignment takes.
    """
    import birdology.prior.effort as effort

    bad = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([7], pa.int64()),
            "total": pa.array([3], pa.int64()),
            "ratio": pa.array([7 / 3], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match=r"outside \[0, 1\]"):
        effort._check_output(bad)


def test_a_null_total_is_caught_on_output():
    """The shape a left-outer join leaves behind when the count still matches."""
    import birdology.prior.effort as effort

    bad = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([7], pa.int64()),
            "total": pa.array([None], pa.int64()),
            "ratio": pa.array([None], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="total"):
        effort._check_output(bad)


def test_a_re_run_on_its_own_output_is_rejected():
    """A RatioTable fed back in would otherwise die on a raw pyarrow KeyError."""
    out = target_group_ratios(_counts([(1, "c", 5, 3)]))
    with pytest.raises(ValueError, match="already carries"):
        target_group_ratios(out)


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


def test_a_mixed_case_cell_is_passed_through_UNCHANGED():
    """Rule (C) with teeth: a REDUNDANT local normalisation must be visible.

    The contract says `cell` arrives already lowercased, so on a lowercase
    fixture any local `utf8_lower` is a no-op and no test can see it -- which
    is precisely how a second derivation sneaks in and then diverges from the
    upstream one. This fixture is deliberately contract-VIOLATING in case only:
    if the module ever re-normalises, the output cell differs from the input
    and this fails. The module must pass `cell` through byte-for-byte, not
    tidy it up.
    """
    cell = "8A1Fb46622dFFFF"
    out = target_group_ratios(_counts([(1, cell, 5, 3), (2, cell, 5, 1)]))
    rows = out.to_pylist()
    assert {r["cell"] for r in rows} == {cell}
    assert {r["total"] for r in rows} == {4}


def test_mixed_and_lower_case_cells_are_DIFFERENT_cells():
    """A local casefold would merge these two cell-weeks into one denominator."""
    out = target_group_ratios(
        _counts([(1, "8A1F", 5, 3), (1, "8a1f", 5, 1)])
    )
    totals = {r["cell"]: r["total"] for r in out.to_pylist()}
    assert totals == {"8A1F": 3, "8a1f": 1}


def test_ratio_is_aligned_with_its_own_row_in_a_multi_chunk_table():
    """The join REORDERS a multi-chunk table; numerator and denominator must
    still come from the same output row.

    Every fixture above is single-chunk, where the join preserves input order
    -- so taking the numerator from `counts` instead of `joined` is invisible.
    Making the table multi-chunk is NOT by itself enough: a 2-chunk, 2-groups
    table comes back in input order about half the time, and with
    `pa.cpu_count() == 1` Acero's hash join is sequential and NEVER reorders.
    Under those conditions `counts.column("n")` and `joined.column("n")` are
    element-wise identical and every assertion below still holds on the buggy
    code -- the exact way this test's coverage was found to be imaginary.

    So the reordering is asserted as an explicit PRECONDITION, not assumed:
    eight chunks over eight (cell, week) groups reorder on every observed run,
    and if a future pyarrow ever preserves that order the precondition fails
    loudly instead of quietly hollowing the test out.
    """
    # Eight chunks, each one (cell, week) group whose four species split it
    # 1:2:3:4. The four `n` values are SCALED PER GROUP, so the concatenated
    # `n` sequence is distinct group-to-group: a reorder is then visible in
    # `n` itself, not just in `species_key`. (With the same 1,2,3,4 in every
    # group the `n` sequence is reorder-invariant and the mutation stays
    # hidden even though the rows demonstrably move.)
    chunks = [
        _counts(
            [
                (10 * g + k, f"c{g}", 1 + g, k * (g + 1))
                for k in (1, 2, 3, 4)
            ]
        )
        for g in range(8)
    ]
    t = pa.concat_tables(chunks)
    assert t.column("n").num_chunks > 1

    if pa.cpu_count() < 2:
        pytest.skip(
            "single-threaded pyarrow: Acero's hash join is sequential and "
            "order-preserving, so no fixture can reach the misalignment path"
        )

    out = target_group_ratios(t)
    assert out.num_rows == t.num_rows

    # PRECONDITION. The bug this test exists to catch -- a numerator read from
    # the pre-join table -- is only observable when the join moves rows, i.e.
    # when the output's `n` sequence differs from the input's. Assert it.
    assert out.column("n").to_pylist() != t.column("n").to_pylist(), (
        "the join preserved row order, so a numerator taken from the pre-join "
        "table would be element-wise identical and this test cannot fail"
    )
    assert (
        out.column("species_key").to_pylist()
        != t.column("species_key").to_pylist()
    )

    for row in out.to_pylist():
        assert row["ratio"] == pytest.approx(row["n"] / row["total"]), row
    by_species = {r["species_key"]: r for r in out.to_pylist()}
    assert len(by_species) == t.num_rows
    for g in range(8):
        assert by_species[10 * g + 1]["ratio"] == pytest.approx(0.1)
        assert by_species[10 * g + 2]["ratio"] == pytest.approx(0.2)
        assert by_species[10 * g + 3]["ratio"] == pytest.approx(0.3)
        assert by_species[10 * g + 4]["ratio"] == pytest.approx(0.4)


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


def test_a_wrong_cell_type_is_rejected_not_cast():
    """A dictionary-encoded or binary cell means a different upstream path."""
    t = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([b"c"], pa.binary()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="cell"):
        target_group_ratios(t)


def test_a_wrong_n_type_is_rejected_not_cast():
    """A float `n` means `n` stopped being a count somewhere upstream."""
    t = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1.0], pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="'n'"):
        target_group_ratios(t)


def test_a_wrong_species_key_type_is_rejected_not_cast():
    """int32 species_key silently truncates GBIF keys above 2**31."""
    t = pa.table(
        {
            "species_key": pa.array([1], pa.int32()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="species_key"):
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


def test_join_drop_is_caught_even_if_validation_is_bypassed(monkeypatch):
    """Defence in depth for the silent-drop mode.

    `_validate` already rejects null keys, so the inner join can never lose a
    row in practice. That makes the join type unobservable -- a reviewer's
    `left outer` mutant survived the suite. This test disables validation and
    proves the post-join row-count invariant is what actually stops a dropped
    row, independently of the null check.
    """
    import birdology.prior.effort as effort

    monkeypatch.setattr(effort, "_validate", lambda counts: None)
    t = pa.table(
        {
            "species_key": pa.array([1, 2], pa.int64()),
            "cell": pa.array(["c", None], pa.string()),
            "week": pa.array([5, 5], pa.int8()),
            "n": pa.array([3, 1], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="join"):
        effort.target_group_ratios(t)
