"""Sparse prior tensor, round-tripped through disk.

The serialisation boundary: every downstream stage reads the prior through
`PriorTensor.vector()` / `lookup()`, so a defect here is invisible to them.
The tests are grouped by the property they pin, and each guard's message is
matched on wording specific to THAT check rather than on a bare word another
layer could also produce.
"""
import json
import re
import shutil
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.backoff import FLOOR
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import (
    LICENCE,
    LICENCE_URL,
    PriorSpec,
    PriorTensor,
    _cell_of_column,
    _column_index,
    load_prior,
    save_prior,
    to_tensor,
)

SPEC = PriorSpec(resolution=4, source="dof", variant="full")
DOI = "10.15468/dl.abc123"


def _table(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "p": pa.array([r[3] for r in rows], pa.float64()),
        }
    )


def _saveable(rows, doi=DOI):
    """A tensor with provenance, because `save_prior` refuses one without."""
    t = to_tensor(_table(rows), SPEC)
    t.gbif_doi = doi
    return t


# --------------------------------------------------------------------------
# lookup: the stored value; the FLOOR for an unstored context on both axes
# --------------------------------------------------------------------------
def test_lookup_returns_stored_value():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(0.25)


def test_a_context_absent_from_the_table_reads_the_FLOOR_not_a_hard_zero():
    """The reversal of T8-R5, pinned rather than only documented.

    Week 45 was never stored, but species 1 is on the species axis and
    `cell-a` is on the cell axis, so the context EXISTS -- nobody looked at it.
    `backoff.FLOOR` is the value the backoff defines for exactly that, and a
    hard 0.0 here would let the prior veto a correct detection in every
    cell-week the occurrence data never reached (which is most of the grid).
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert 1 in set(int(k) for k in t.species_keys)  # precondition: species on axis
    assert "cell-a" in t.cells  # precondition: cell on axis
    assert t.values.nnz == 1  # precondition: week 45 really is unstored
    assert t.lookup(1, "cell-a", 45) == pytest.approx(FLOOR, rel=1e-12)
    assert t.lookup(1, "cell-a", 45) != 0.0


def test_an_unstored_context_never_reads_zero_for_any_week_of_the_grid():
    """The property stated over the whole grid, not one convenient week.

    A `np.where` with an inverted condition, a floor applied to only the
    stored entries, or a floor of 0.0 all pass a single-week assertion by
    accident on some fixture; none survives every week of both cells.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (1, "cell-b", 3, 0.5)]), SPEC)
    for cell in ("cell-a", "cell-b"):
        for week in range(1, WEEKS_PER_YEAR + 1):
            value = t.lookup(1, cell, week)
            assert value > 0.0
            assert value >= FLOOR


def test_a_stored_value_is_not_raised_to_the_floor_nor_the_floor_spread_over_it():
    """Flooring the gaps must not touch a real estimate.

    A `maximum(values, FLOOR)` over the DENSE grid would give the same answer
    here; a `values + FLOOR` or a fill that overwrote stored entries would not.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (1, "cell-a", 47, 0.5)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == 0.25
    assert t.lookup(1, "cell-a", 47) == 0.5


def test_a_floor_valued_entry_survives_and_is_not_dropped_as_sparse_noise():
    """The floor must round-trip: it is what stops the prior vetoing a detection."""
    t = to_tensor(_table([(1, "cell-a", 46, FLOOR)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(FLOOR, rel=1e-12)
    assert t.values.nnz == 1


# --------------------------------------------------------------------------
# an UNKNOWN axis entry stays LOUD: it is a caller error, not absent evidence
# --------------------------------------------------------------------------
def test_lookup_of_an_unknown_species_key_raises_naming_the_species_axis():
    """A key not on the axis is a typo or a mismatched label set, not a gap.

    The axes are DERIVED from the data, so flooring an unknown key would turn
    a caller bug into a plausible probability that no downstream stage could
    distinguish from a real unobserved context.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert 999 not in set(int(k) for k in t.species_keys)  # precondition
    with pytest.raises(ValueError, match="is not on this tensor's species axis"):
        t.lookup(999, "cell-a", 46)


def test_lookup_of_an_unknown_cell_raises_naming_the_cell_axis():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert "cell-zz" not in t.cells  # precondition
    with pytest.raises(ValueError, match="is not on this tensor's cell axis"):
        t.lookup(1, "cell-zz", 46)


def test_vector_for_an_unknown_cell_raises_rather_than_returning_floors():
    """`vector` is the fusion sweep's interface and must not answer a typo.

    A vector of floors for a misspelled cell is the worst possible outcome: it
    is a well-shaped, plausible prior that silently replaces the real one.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (2, "cell-a", 46, 0.75)]), SPEC)
    assert "cell-zz" not in t.cells  # precondition
    with pytest.raises(ValueError, match="is not on this tensor's cell axis"):
        t.vector("cell-zz", 46)


def test_the_unknown_cell_message_names_the_offending_cell():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    with pytest.raises(ValueError, match="cell-zz"):
        t.lookup(1, "cell-zz", 46)


def test_the_unknown_species_message_names_the_offending_key():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    with pytest.raises(ValueError, match="999"):
        t.lookup(999, "cell-a", 46)


def test_an_empty_tensor_has_no_axis_entries_so_every_lookup_raises():
    """With empty axes there is no context to floor: everything is unknown."""
    t = to_tensor(_table([]), SPEC)
    assert t.cells == []
    assert t.species_keys.tolist() == []
    assert t.values.nnz == 0
    with pytest.raises(ValueError, match="is not on this tensor's cell axis"):
        t.lookup(1, "cell-a", 46)


# --------------------------------------------------------------------------
# T8-R1: the column packing aliases outside 1..52, so both bounds are guarded
# --------------------------------------------------------------------------
def test_lookup_rejects_a_week_above_52_which_would_alias_the_next_cell():
    """Measured: col("cell-a", 53) == col("cell-b", 1) == 52 with no guard."""
    t = to_tensor(
        _table([(1, "cell-a", 46, 0.25), (1, "cell-b", 1, 0.75)]), SPEC
    )
    assert t.cells == ["cell-a", "cell-b"]  # precondition: cell-b follows cell-a
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        t.lookup(1, "cell-a", WEEKS_PER_YEAR + 1)


def test_lookup_rejects_week_zero_which_would_alias_the_previous_cell():
    t = to_tensor(
        _table([(1, "cell-a", 46, 0.25), (1, "cell-b", 1, 0.75)]), SPEC
    )
    assert t.cells == ["cell-a", "cell-b"]  # precondition: cell-a precedes cell-b
    with pytest.raises(ValueError, match="alias the last week of the PREVIOUS cell"):
        t.lookup(1, "cell-b", 0)


def test_vector_rejects_an_out_of_range_week_too():
    """`vector` is the interface the fusion sweep calls; same packing, same bound."""
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        t.vector("cell-a", WEEKS_PER_YEAR + 1)


def test_an_out_of_range_week_raises_the_WEEK_error_even_for_an_unknown_cell():
    """Both are errors now, so the ORDER is what this pins.

    The week is validated first: a caller handed the cell error would go and
    fix a cell name that is not the actual defect, while the aliasing bug in
    its week arithmetic survives.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert "cell-zz" not in t.cells  # precondition: the cell really is unknown
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        t.lookup(1, "cell-zz", WEEKS_PER_YEAR + 1)


def test_an_out_of_range_week_raises_the_WEEK_error_even_for_an_unknown_species():
    """Same ordering argument on the species axis."""
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert 999 not in set(int(k) for k in t.species_keys)  # precondition
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        t.lookup(999, "cell-a", WEEKS_PER_YEAR + 1)


def test_to_tensor_rejects_a_week_above_52_in_the_table():
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        to_tensor(_table([(1, "cell-a", WEEKS_PER_YEAR + 1, 0.25)]), SPEC)


def test_to_tensor_rejects_week_zero_in_the_table():
    with pytest.raises(ValueError, match="alias the last week of the PREVIOUS cell"):
        to_tensor(_table([(1, "cell-a", 0, 0.25)]), SPEC)


# --------------------------------------------------------------------------
# M1: a non-whole week is REJECTED, not truncated into a different week
# --------------------------------------------------------------------------
def _two_week_tensor():
    """Weeks 5 and 46 stored, 47 NOT stored, so a truncation is observable.

    `int(46.9)` is 46, a week that HOLDS a value here: the pre-fix behaviour
    answered `lookup(..., 46.9)` with week 46's real probability. Storing 46
    rather than leaving it empty is what makes the repair visible as a WRONG
    ANSWER rather than as the floor.
    """
    t = to_tensor(
        _table([(1, "cell-a", 5, 0.5), (1, "cell-a", 46, 0.25)]), SPEC
    )
    # Precondition: week 46 really does carry a value distinct from week 5's,
    # so truncating 46.9 to 46 would hand back a DIFFERENT week's number.
    assert t.lookup(1, "cell-a", 46) == pytest.approx(0.25)
    assert t.lookup(1, "cell-a", 5) == pytest.approx(0.5)
    return t


def test_lookup_rejects_a_fractional_week_rather_than_truncating_it():
    """Measured before the fix: `lookup(1, 'cell-a', 46.9)` returned week 46's value.

    `_validate_week(int(week), ...)` validated the TRUNCATION, so the guard
    never saw what the caller passed: 46.9 was silently answered as 46. That is
    a repair at a boundary whose own docstring forbids repair, and its
    consequence is the shape the aliasing guards exist to prevent -- a real
    probability belonging to a DIFFERENT week, returned with nothing raised.
    """
    t = _two_week_tensor()
    with pytest.raises(ValueError, match="46.9.*is not a whole number"):
        t.lookup(1, "cell-a", 46.9)


def test_vector_rejects_a_fractional_week_too():
    """`vector` is the reader the fusion sweep calls; same guard, same refusal."""
    t = _two_week_tensor()
    with pytest.raises(ValueError, match="is not a whole number"):
        t.vector("cell-a", 46.9)


def test_lookup_rejects_a_bool_week_even_though_a_bool_IS_an_int():
    """`isinstance(True, int)` is True, so a type check alone would admit it.

    Before the fix `lookup(..., True)` was answered as week 1 and
    `lookup(..., False)` aliased the previous cell's last week. No caller means
    "the first week of the year" by `True`, so the bool arm is its own clause
    with its own fixture -- an int-only check cannot see it.
    """
    t = _two_week_tensor()
    with pytest.raises(ValueError, match="is a bool, not a week number"):
        t.lookup(1, "cell-a", True)


def test_a_false_week_is_rejected_as_a_bool_not_as_a_below_one_week():
    """`False` trips the BOOL clause, not the lower bound.

    `False < 1` is True, so the lower-bound guard would also fire -- and the
    caller would be told to fix an out-of-range week number when the defect is
    that a bool was passed at all. The clause ORDER is what this pins.
    """
    t = _two_week_tensor()
    with pytest.raises(ValueError, match="is a bool, not a week number"):
        t.lookup(1, "cell-a", False)


def test_lookup_rejects_a_non_numeric_week():
    """A string week was coerced: `lookup(..., '5')` returned week 5's value.

    The type clause is separate from the integrality one because `'5' % 1`
    raises `TypeError`, not the `ValueError` this module's contract promises,
    so the integrality check cannot stand in for it.
    """
    t = _two_week_tensor()
    with pytest.raises(ValueError, match="is not a numeric week"):
        t.lookup(1, "cell-a", "5")  # type: ignore[arg-type]  # deliberately wrong type


def test_an_integral_float_week_is_ACCEPTED_because_it_names_one_exact_week():
    """5.0 is week 5 under any reading, so admitting it converts nothing.

    The rejection above is of values whose acceptance would change WHICH week
    is answered. A guard that also refused 5.0 would be refusing a value it
    can honour exactly, which is a different defect from the one M1 reports.
    """
    t = _two_week_tensor()
    assert t.lookup(1, "cell-a", 5.0) == pytest.approx(0.5)
    assert t.lookup(1, "cell-a", np.float64(46.0)) == pytest.approx(0.25)


def test_a_numpy_integer_week_is_accepted():
    """`week` arrives as an int8 from Arrow upstream; numpy ints are real weeks."""
    t = _two_week_tensor()
    assert t.lookup(1, "cell-a", np.int8(5)) == pytest.approx(0.5)


def test_a_nan_week_is_rejected_rather_than_read_as_some_column():
    """`nan % 1` is `nan`, so the integrality clause catches it; `int(nan)` raised."""
    t = _two_week_tensor()
    with pytest.raises(ValueError, match="is not a whole number"):
        t.lookup(1, "cell-a", float("nan"))


def test_week_52_is_accepted_and_is_the_last_column_of_its_cell():
    """The upper bound is inclusive: 52 is a real ISO week, not an overflow."""
    t = to_tensor(
        _table([(1, "cell-a", WEEKS_PER_YEAR, 0.25), (1, "cell-b", 1, 0.75)]), SPEC
    )
    assert t.lookup(1, "cell-a", WEEKS_PER_YEAR) == pytest.approx(0.25)
    assert t.lookup(1, "cell-b", 1) == pytest.approx(0.75)


def test_the_two_cells_do_not_share_a_column():
    """The aliasing this guard prevents, stated as a positive property."""
    t = to_tensor(
        _table([(1, "cell-a", WEEKS_PER_YEAR, 0.25), (1, "cell-b", 1, 0.75)]), SPEC
    )
    assert t.values.nnz == 2


def test_week_52_in_a_single_cell_tensor_occupies_the_last_column():
    """The packing must be `cell * 52 + (week - 1)`, not `cell * 52 + week`.

    A single cell leaves no room for an off-by-one to hide in the next cell's
    block: column 52 does not exist in a 1 x 52 matrix.
    """
    t = to_tensor(_table([(1, "cell-a", WEEKS_PER_YEAR, 0.25)]), SPEC)
    assert t.values.get_shape() == (1, WEEKS_PER_YEAR)
    assert t.values.tocoo().col.tolist() == [WEEKS_PER_YEAR - 1]


def test_a_dense_two_cell_grid_fills_every_column_exactly_once():
    """A fully observed grid maps onto the shape exactly.

    Any stride other than WEEKS_PER_YEAR either overflows the matrix or leaves
    holes, so this pins the stride as well as the offset.
    """
    rows = [
        (1, cell, week, 0.5)
        for cell in ("cell-a", "cell-b")
        for week in range(1, WEEKS_PER_YEAR + 1)
    ]
    t = to_tensor(_table(rows), SPEC)
    assert t.values.get_shape() == (1, 2 * WEEKS_PER_YEAR)
    assert t.values.nnz == 2 * WEEKS_PER_YEAR
    assert sorted(t.values.tocoo().col.tolist()) == list(range(2 * WEEKS_PER_YEAR))


# --------------------------------------------------------------------------
# T8-R2: scipy sums duplicate coordinates silently
# --------------------------------------------------------------------------
def test_to_tensor_rejects_a_duplicate_species_cell_week_row():
    """Measured: two rows of p=0.4 become 0.8 with no error and no nan."""
    with pytest.raises(ValueError, match="duplicate .species_key, cell, week."):
        to_tensor(_table([(1, "cell-a", 46, 0.4), (1, "cell-a", 46, 0.4)]), SPEC)


def test_the_duplicate_message_names_the_offending_key():
    with pytest.raises(ValueError, match=r"\(1, 'cell-a', 46\)"):
        to_tensor(_table([(1, "cell-a", 46, 0.4), (1, "cell-a", 46, 0.4)]), SPEC)


def test_the_duplicate_message_names_the_cell_the_duplicate_is_actually_in():
    """The message's cell comes from the INVERSE of the packing, so it can be wrong.

    The duplicate is in `cell-b`, the MIDDLE cell of three, so an inverse that
    is off by one in either direction names a cell that really exists -- the
    message stays plausible and sends whoever reads it hunting for a duplicate
    in a cell that has none. A single-cell fixture cannot see this: any wrong
    index there is either out of range or the only cell there is.
    """
    rows = [
        (1, "cell-a", 46, 0.4),
        (1, "cell-b", 46, 0.4),
        (1, "cell-b", 46, 0.4),
        (1, "cell-c", 46, 0.4),
    ]
    with pytest.raises(ValueError) as excinfo:
        to_tensor(_table(rows), SPEC)
    message = str(excinfo.value)
    assert "'cell-b'" in message, message
    # Not merely "it raised": naming a NEIGHBOUR is the defect being pinned.
    assert "'cell-a'" not in message, message
    assert "'cell-c'" not in message, message


def test_the_column_packing_and_its_inverse_round_trip_over_the_whole_grid():
    """`_cell_of_column` is `_column_index`'s inverse for every real context.

    The two sit side by side in `tensor.py` precisely so this can be asserted
    directly rather than trusted: the inverse was previously spelled inline in
    the duplicate message, so a change to the forward packing would have left
    the message naming a different cell with nothing to catch it.
    """
    for cell_index in range(4):
        for week in range(1, WEEKS_PER_YEAR + 1):
            col = _column_index(cell_index, week)
            assert _cell_of_column(col) == cell_index, (cell_index, week, col)


def test_the_inverse_maps_a_cell_boundary_to_the_right_side_of_it():
    """The two columns either side of a cell boundary belong to different cells.

    The first and last week of a cell are where an off-by-one in the inverse
    shows up, and they are the exact columns the duplicate message is most
    likely to be reporting.
    """
    assert _cell_of_column(_column_index(1, 1)) == 1
    assert _cell_of_column(_column_index(1, WEEKS_PER_YEAR)) == 1
    assert _cell_of_column(_column_index(0, WEEKS_PER_YEAR)) == 0
    assert _cell_of_column(_column_index(2, 1)) == 2


def test_rows_differing_only_in_week_are_not_duplicates():
    t = to_tensor(_table([(1, "cell-a", 46, 0.4), (1, "cell-a", 47, 0.4)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(0.4)
    assert t.lookup(1, "cell-a", 47) == pytest.approx(0.4)


def test_rows_differing_only_in_species_are_not_duplicates():
    t = to_tensor(_table([(1, "cell-a", 46, 0.4), (2, "cell-a", 46, 0.4)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(0.4)
    assert t.lookup(2, "cell-a", 46) == pytest.approx(0.4)


def test_rows_differing_only_in_cell_are_not_duplicates():
    t = to_tensor(_table([(1, "cell-a", 46, 0.4), (1, "cell-b", 46, 0.4)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(0.4)
    assert t.lookup(1, "cell-b", 46) == pytest.approx(0.4)


# --------------------------------------------------------------------------
# T8-R4: `p` is validated here, not trusted from upstream
# --------------------------------------------------------------------------
def test_a_zero_p_is_rejected_not_silently_skipped():
    """An explicit stored 0.0 is read exactly as an unstored context would be.

    Since a missing context now reads FLOOR, storing a 0.0 would ALSO read
    FLOOR -- so an upstream stage that emitted a real zero would have it
    silently converted into "nobody looked". The guard is what keeps that a
    contract breach rather than a quiet reinterpretation.
    """
    with pytest.raises(ValueError, match="at or below 0.0"):
        to_tensor(_table([(1, "cell-a", 46, 0.0)]), SPEC)


def test_a_negative_p_is_rejected():
    with pytest.raises(ValueError, match="at or below 0.0"):
        to_tensor(_table([(1, "cell-a", 46, -0.25)]), SPEC)


def test_a_nan_p_is_rejected():
    """A nan passes a positivity comparison, so finiteness is its own clause."""
    with pytest.raises(ValueError, match="non-finite"):
        to_tensor(_table([(1, "cell-a", 46, float("nan"))]), SPEC)


def test_an_infinite_p_is_rejected():
    with pytest.raises(ValueError, match="non-finite"):
        to_tensor(_table([(1, "cell-a", 46, float("inf"))]), SPEC)


def test_a_p_above_one_is_rejected():
    with pytest.raises(ValueError, match="above 1.0"):
        to_tensor(_table([(1, "cell-a", 46, 1.5)]), SPEC)


def test_p_of_exactly_one_is_accepted():
    """The clamp is [floor, 1.0] inclusive: 1.0 is legal, 1.0 + eps is not."""
    t = to_tensor(_table([(1, "cell-a", 46, 1.0)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(1.0)


# --------------------------------------------------------------------------
# schema validation: the upstream contract, named
# --------------------------------------------------------------------------
def test_a_table_missing_the_p_column_is_rejected():
    table = _table([(1, "cell-a", 46, 0.25)]).drop_columns(["p"])
    assert "p" not in table.column_names  # precondition
    with pytest.raises(ValueError, match="missing the 'p' column"):
        to_tensor(table, SPEC)


def test_a_wrong_input_dtype_is_rejected_not_cast():
    """`week` is derived exactly once, upstream; a cast here is a second one."""
    table = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["cell-a"], pa.string()),
            "week": pa.array([46], pa.int64()),
            "p": pa.array([0.25], pa.float64()),
        }
    )
    assert table.schema.field("week").type == pa.int64()  # precondition
    with pytest.raises(ValueError, match="column 'week' must be int8"):
        to_tensor(table, SPEC)


def test_a_null_in_a_key_column_is_rejected():
    table = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([None], pa.string()),
            "week": pa.array([46], pa.int8()),
            "p": pa.array([0.25], pa.float64()),
        }
    )
    assert table.column("cell").null_count == 1  # precondition
    with pytest.raises(ValueError, match="column 'cell' contains null values"):
        to_tensor(table, SPEC)


def test_a_null_in_p_is_rejected():
    """A null `p` is NOT non-finite in the numpy sense until it is filled.

    Separate fixture from the nan test: a finiteness check on a column with a
    null beside finite values does not necessarily fire, so the null clause
    cannot be folded into it.
    """
    table = pa.table(
        {
            "species_key": pa.array([1, 2], pa.int64()),
            "cell": pa.array(["cell-a", "cell-a"], pa.string()),
            "week": pa.array([46, 46], pa.int8()),
            "p": pa.array([0.25, None], pa.float64()),
        }
    )
    assert table.column("p").null_count == 1  # precondition
    with pytest.raises(ValueError, match="column 'p' contains null values"):
        to_tensor(table, SPEC)


# --------------------------------------------------------------------------
# the axes: sorted, deduplicated, and byte-for-byte the cells we were given
# --------------------------------------------------------------------------
def test_species_keys_are_sorted_and_unique():
    t = to_tensor(
        _table([(7, "cell-a", 46, 0.2), (3, "cell-a", 46, 0.2), (7, "cell-a", 47, 0.2)]),
        SPEC,
    )
    assert t.species_keys.tolist() == [3, 7]


def test_cells_are_sorted_and_unique():
    t = to_tensor(
        _table([(1, "cell-b", 46, 0.2), (1, "cell-a", 46, 0.2), (1, "cell-b", 47, 0.2)]),
        SPEC,
    )
    assert t.cells == ["cell-a", "cell-b"]


def test_each_value_lands_under_its_own_key_when_the_input_is_unsorted():
    """Row order must not decide which value belongs to which context."""
    t = to_tensor(
        _table(
            [
                (7, "cell-b", 3, 0.11),
                (3, "cell-a", 46, 0.22),
                (7, "cell-a", 3, 0.33),
                (3, "cell-b", 46, 0.44),
            ]
        ),
        SPEC,
    )
    assert t.lookup(7, "cell-b", 3) == pytest.approx(0.11)
    assert t.lookup(3, "cell-a", 46) == pytest.approx(0.22)
    assert t.lookup(7, "cell-a", 3) == pytest.approx(0.33)
    assert t.lookup(3, "cell-b", 46) == pytest.approx(0.44)


def test_a_cell_string_is_passed_through_byte_for_byte():
    """`cell` is opaque and case-sensitive: no casefolding, no re-derivation.

    The lower-cased spelling is not merely a different value -- it is not on
    the axis at all, so it raises. A module that case-folded `cell` would
    publish the folded spelling on the axis and REJECT the exact bytes it was
    handed, which is why the raise is asserted on the folded form.
    """
    cell = "841f1ffFfffffff"
    t = to_tensor(_table([(1, cell, 46, 0.25)]), SPEC)
    assert t.cells == [cell]
    assert t.lookup(1, cell, 46) == pytest.approx(0.25)
    with pytest.raises(ValueError, match="is not on this tensor's cell axis"):
        t.lookup(1, cell.lower(), 46)


def test_two_cells_differing_only_in_case_are_not_merged():
    """Case-folding would collapse them into one axis entry and SUM their values.

    A distinct property from the pass-through test: that one would still pass
    if both spellings were folded to the same lower-case axis entry as long as
    the folded string were published.
    """
    upper, lower = "841f1ffFfffffff", "841f1ffffffffff"
    t = to_tensor(_table([(1, upper, 46, 0.25), (1, lower, 46, 0.5)]), SPEC)
    assert t.cells == sorted([upper, lower])
    assert t.values.nnz == 2
    assert t.lookup(1, upper, 46) == pytest.approx(0.25)
    assert t.lookup(1, lower, 46) == pytest.approx(0.5)


def test_a_chunked_input_packs_identically_to_a_contiguous_one():
    rows = [(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)]
    contiguous = _table(rows)
    chunked = pa.concat_tables([_table(rows[:1]), _table(rows[1:])])
    assert chunked.column("cell").num_chunks == 2  # precondition
    a, b = to_tensor(contiguous, SPEC), to_tensor(chunked, SPEC)
    assert a.cells == b.cells
    assert a.species_keys.tolist() == b.species_keys.tolist()
    assert (a.values != b.values).nnz == 0


def test_an_empty_table_yields_an_empty_tensor():
    t = to_tensor(_table([]), SPEC)
    assert t.cells == []
    assert t.species_keys.tolist() == []
    assert t.values.nnz == 0


# --------------------------------------------------------------------------
# vector(): one entry per species, in order, never renormalised (T8-R6)
# --------------------------------------------------------------------------
def test_vector_returns_one_entry_per_species_in_order():
    """The 0.25/0.75 fixture sums to 1.0 BY CONSTRUCTION, not by the code.

    See `test_vector_does_NOT_renormalise_over_species` for the property the
    pipeline actually relies on. Nothing here asserts a sum.
    """
    t = to_tensor(
        _table([(1, "cell-a", 46, 0.25), (2, "cell-a", 46, 0.75)]), SPEC
    )
    assert t.species_keys.tolist() == [1, 2]  # precondition: the order asserted below
    vec = t.vector("cell-a", 46)
    assert vec.shape == (2,)
    assert vec[0] == pytest.approx(0.25)
    assert vec[1] == pytest.approx(0.75)


def test_vector_does_NOT_renormalise_over_species():
    """`p` is a per-species marginal and is never renormalised, at any stage.

    0.3 and 0.9 sum to 1.2: they must come back untouched. A real context
    routinely sums above 1.0 (an absent species still contributes the floor on
    top) or far below it (only a subset of species was requested).
    """
    t = to_tensor(
        _table([(1, "cell-a", 46, 0.3), (2, "cell-a", 46, 0.9)]), SPEC
    )
    vec = t.vector("cell-a", 46)
    assert vec[0] == pytest.approx(0.3)
    assert vec[1] == pytest.approx(0.9)
    assert vec.sum() == pytest.approx(1.2)


def test_vector_floors_the_species_that_have_no_value_in_this_context():
    """One entry per species, and the unobserved ones carry the floor.

    This is the backoff's own rule restated at the boundary: a species the
    occurrence data missed in this cell-week still contributes `floor`, so the
    fusion cannot be vetoed by a structural hole.
    """
    t = to_tensor(
        _table([(1, "cell-a", 46, 0.25), (2, "cell-a", 3, 0.75)]), SPEC
    )
    assert t.species_keys.tolist() == [1, 2]  # precondition
    vec = t.vector("cell-a", 46)
    assert vec.shape == (2,)
    assert vec[0] == pytest.approx(0.25)
    assert vec[1] == pytest.approx(FLOOR, rel=1e-12)
    assert vec[1] != 0.0


def test_vector_and_lookup_agree_on_every_species_of_a_context():
    """The two readers share one definition of "unstored means the floor".

    A floor applied in `lookup` but not in `vector` (or vice versa) is exactly
    the writer/reader drift this module's layout comment warns about, and it
    would be invisible to any test that exercised only one of them.
    """
    t = to_tensor(
        _table([(1, "cell-a", 46, 0.25), (2, "cell-a", 3, 0.75), (3, "cell-b", 9, 0.5)]),
        SPEC,
    )
    for cell in t.cells:
        for week in (1, 3, 9, 46, WEEKS_PER_YEAR):
            vec = t.vector(cell, week)
            for i, key in enumerate(t.species_keys):
                assert vec[i] == t.lookup(int(key), cell, week)


def test_vector_reads_only_its_own_context():
    """The neighbouring week and the neighbouring cell must not bleed in."""
    t = to_tensor(
        _table(
            [
                (1, "cell-a", 46, 0.25),
                (1, "cell-a", 47, 0.5),
                (1, "cell-b", 46, 0.75),
                (2, "cell-a", 46, 0.125),
            ]
        ),
        SPEC,
    )
    assert t.vector("cell-a", 46).tolist() == pytest.approx([0.25, 0.125])
    assert t.vector("cell-a", 47).tolist() == pytest.approx([0.5, FLOOR])
    assert t.vector("cell-b", 46).tolist() == pytest.approx([0.75, FLOOR])


def test_vector_is_float64():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.vector("cell-a", 46).dtype == np.float64


# --------------------------------------------------------------------------
# dense(): the same semantics, matrix-shaped
# --------------------------------------------------------------------------
def test_dense_floors_every_unstored_context_and_preserves_the_stored_ones():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)]), SPEC)
    grid = t.dense()
    assert grid.shape == (2, 2 * WEEKS_PER_YEAR)
    assert grid.dtype == np.float64
    assert (grid > 0.0).all()
    assert grid[0, 45] == pytest.approx(0.25)
    assert grid[1, WEEKS_PER_YEAR + 2] == pytest.approx(0.5)
    assert np.count_nonzero(grid == FLOOR) == grid.size - 2


def test_dense_agrees_with_vector_column_by_column():
    """One definition, reached two ways: a divergence here is reader drift."""
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)]), SPEC)
    grid = t.dense()
    for ci, cell in enumerate(t.cells):
        for week in range(1, WEEKS_PER_YEAR + 1):
            col = ci * WEEKS_PER_YEAR + (week - 1)
            assert grid[:, col].tolist() == pytest.approx(t.vector(cell, week).tolist())


def test_dense_capacity_is_the_product_of_the_three_axes():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)]), SPEC)
    assert t.dense_capacity == 2 * 2 * WEEKS_PER_YEAR
    assert t.dense_capacity == t.dense().size


# --------------------------------------------------------------------------
# PriorTensor's own invariants (they guard load_prior's hand-built axes)
# --------------------------------------------------------------------------
def _tensor(species, cells, matrix):
    return PriorTensor(
        spec=SPEC,
        species_keys=np.asarray(species, dtype=np.int64),
        cells=list(cells),
        values=matrix,
    )


def test_a_duplicate_species_key_axis_is_rejected():
    with pytest.raises(ValueError, match="species_keys contains a duplicate"):
        _tensor([1, 1], ["cell-a"], sparse.csr_matrix((2, WEEKS_PER_YEAR)))


def test_a_duplicate_cell_axis_is_rejected():
    with pytest.raises(ValueError, match="cells contains a duplicate"):
        _tensor([1], ["cell-a", "cell-a"], sparse.csr_matrix((1, 2 * WEEKS_PER_YEAR)))


def test_a_matrix_with_the_wrong_row_count_is_rejected():
    with pytest.raises(ValueError, match="rows but there are"):
        _tensor([1, 2], ["cell-a"], sparse.csr_matrix((1, WEEKS_PER_YEAR)))


def test_a_matrix_with_the_wrong_column_count_is_rejected():
    with pytest.raises(ValueError, match="columns but"):
        _tensor([1], ["cell-a"], sparse.csr_matrix((1, WEEKS_PER_YEAR + 1)))


# --------------------------------------------------------------------------
# provenance is mandatory: a release with empty attribution must not be written
# --------------------------------------------------------------------------
def test_save_refuses_a_tensor_with_no_gbif_doi(tmp_path):
    """CC BY REQUIRES attribution; an empty DOI ships a release that has none.

    The failure this prevents is entirely silent: the artifact writes fine, the
    sidecar validates as JSON, and nothing states where the data came from.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.gbif_doi == ""  # precondition: no provenance was set
    with pytest.raises(ValueError, match="empty gbif_doi"):
        save_prior(t, tmp_path / "p.npz")


@pytest.mark.parametrize("blank", [" ", "   ", "\t", "\n", " \t \n "])
def test_save_refuses_a_tensor_whose_gbif_doi_is_only_whitespace(tmp_path, blank):
    """C2's library half: `'   '` is TRUTHY, and that is the whole defect.

    The emptiness check above passes a whitespace-only DOI, so before this
    guard `--gbif-doi '   '` -- what `--gbif-doi "$DOI"` produces from an unset
    shell variable -- wrote all five artifacts with sidecar `gbif_doi: '   '`
    and attribution `Derived from GBIF occurrence download    .` A CC BY 4.0
    breach in a published artifact, indistinguishable from a correct one to
    everyone except someone who looks at the whitespace.

    The build script also rejects this at parse time, and neither guard is
    redundant: that one protects the command line, this one protects every
    other caller of the library.

    REJECTED, NOT COERCED. `.strip()` appears only in the CONDITION; the value
    is never rewritten. Silently storing `'10.15468/dl.x'` for an input of
    `'  10.15468/dl.x  '` would make the artifact cite a string the caller
    never supplied, which is the repair-at-a-boundary this plan forbids.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    t.gbif_doi = blank
    with pytest.raises(ValueError, match="empty gbif_doi"):
        save_prior(t, tmp_path / "p.npz")
    assert not list(tmp_path.glob("*")), "a half-artifact was left behind"


def test_save_accepts_a_doi_with_INCIDENTAL_surrounding_whitespace(tmp_path):
    """...and does not silently trim it. Guards the clause against over-reach.

    A `.strip()` moved from the condition into an ASSIGNMENT would pass every
    rejection test above while quietly changing the attribution string. This
    asserts the value round-trips BYTE-FOR-BYTE: the sidecar must carry what
    the caller attested to, whitespace included, because normalising an
    attribution field is a decision about provenance and not a formatting
    choice.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    t.gbif_doi = "  10.15468/dl.abc123  "
    path = save_prior(t, tmp_path / "p.npz")
    assert load_prior(path).gbif_doi == "  10.15468/dl.abc123  "


def test_nothing_is_written_when_the_doi_check_refuses(tmp_path):
    """The guard runs BEFORE the matrix is written, so no half-artifact is left."""
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    with pytest.raises(ValueError, match="empty gbif_doi"):
        save_prior(t, tmp_path / "p.npz")
    assert list(tmp_path.iterdir()) == []


def test_a_whitespace_only_doi_is_still_a_doi_for_this_guard(tmp_path):
    """Documents the bound: the guard tests EMPTINESS, not DOI well-formedness.

    Whether the string names a real GBIF download is a pipeline-level property
    no test here can check, so this pins where the guard stops rather than
    implying it validates the DOI.
    """
    t = _saveable([(1, "cell-a", 46, 0.25)], doi="not-a-doi")
    assert save_prior(t, tmp_path / "p.npz").exists()


# --------------------------------------------------------------------------
# the on-disk format
# --------------------------------------------------------------------------
def test_round_trip_preserves_values(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)])
    path = save_prior(t, tmp_path / "p.npz")
    back = load_prior(path)
    assert back.lookup(1, "cell-a", 46) == pytest.approx(0.25)
    assert back.lookup(2, "cell-b", 3) == pytest.approx(0.5)


def test_round_trip_preserves_spec(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25)])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.spec == SPEC


def test_round_trip_preserves_the_axes_exactly(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.cells == t.cells
    assert back.species_keys.tolist() == t.species_keys.tolist()
    assert back.species_keys.dtype == np.int64


def test_round_trip_preserves_the_sparsity_pattern_and_the_floor(tmp_path):
    """`load` must NOT materialise the floor, and must not turn it into a zero.

    Two properties in one fixture because they are two halves of the same
    invariant: `nnz` still distinguishes an OBSERVED context from an
    unobserved one (so the published fill stays meaningful), while the
    unobserved context still READS as the floor rather than a hard zero. A
    loader that expanded the grid would pass the second and break the first; a
    loader that dropped the flooring would pass the first and break the second.
    """
    t = _saveable([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.values.nnz == 2
    assert back.lookup(1, "cell-a", 46) == pytest.approx(0.25)
    assert back.lookup(1, "cell-b", 3) == pytest.approx(FLOOR, rel=1e-12)
    assert back.lookup(1, "cell-b", 3) != 0.0


def test_a_loaded_tensor_never_reports_a_hard_zero_anywhere_on_its_grid(tmp_path):
    """The reversal, asserted after a round trip rather than only before it.

    A hard zero reintroduced on the READ side would be invisible to every
    in-memory test: this is the one that fails if someone puts it back.
    """
    t = _saveable([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert (back.dense() > 0.0).all()
    for cell in back.cells:
        for week in range(1, WEEKS_PER_YEAR + 1):
            assert not back.vector(cell, week).tolist() == pytest.approx(
                [0.0] * len(back.species_keys)
            )


def test_round_trip_preserves_full_float64_precision(tmp_path):
    """A float32 write would silently round the floor the backoff guarantees."""
    value = 1.0 / 3.0
    t = _saveable([(1, "cell-a", 46, value)])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.lookup(1, "cell-a", 46) == value


def test_sidecar_records_licence_and_doi(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25)])
    path = save_prior(t, tmp_path / "p.npz")
    meta = json.loads(path.with_suffix(".json").read_text())
    assert meta["licence"] == "CC BY 4.0"
    assert meta["gbif_doi"] == DOI


def test_the_licence_constant_is_the_one_written(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25)])
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert meta["licence"] == LICENCE


def test_the_sidecar_carries_an_attribution_naming_both_the_doi_and_the_licence(
    tmp_path,
):
    """CC BY is only satisfiable if the artifact SAYS what to attribute.

    A reader holding the `.npz` and the sidecar must be able to construct the
    citation without consulting this repository, so the attribution string has
    to name the download AND the terms, not merely record them in separate
    fields a reader might not know to combine.
    """
    t = _saveable([(1, "cell-a", 46, 0.25)])
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert DOI in meta["attribution"]
    assert LICENCE in meta["attribution"]
    assert meta["licence_url"] == LICENCE_URL
    assert LICENCE_URL in meta["attribution"]


def test_the_doi_survives_the_round_trip(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25)])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.gbif_doi == DOI


def test_the_sidecar_publishes_the_fill_of_the_artifact(tmp_path):
    """`nnz` against the dense capacity, so a release states its own coverage.

    Without both numbers a third party cannot tell a prior built from a dense
    national grid from one built from a handful of observed cells, and the two
    warrant very different confidence.
    """
    t = _saveable([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)])
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert meta["nnz"] == 2
    assert meta["dense_capacity"] == 2 * 2 * WEEKS_PER_YEAR
    assert meta["nnz"] == t.values.nnz
    assert meta["dense_capacity"] == t.dense_capacity


def test_the_sidecar_publishes_the_value_an_absent_context_takes(tmp_path):
    """A third party decoding the matrix must know an unstored entry is FLOOR.

    Decoded with a plain sparse reader and no sidecar convention, every
    unobserved context would read 0.0 and veto its species.
    """
    t = _saveable([(1, "cell-a", 46, 0.25)])
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert meta["missing_value"] == pytest.approx(FLOOR, rel=1e-12)


def test_a_sidecar_declaring_a_different_missing_value_is_rejected(tmp_path):
    """An artifact published under another convention must not be read silently."""
    t = _saveable([(1, "cell-a", 46, 0.25)])
    path = save_prior(t, tmp_path / "p.npz")
    sidecar = path.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    meta["missing_value"] = 0.0
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="missing_value"):
        load_prior(path)


def test_a_sidecar_whose_nnz_disagrees_with_the_matrix_is_rejected(tmp_path):
    """The two halves must be the same artifact, or the axes describe nothing."""
    t = _saveable([(1, "cell-a", 46, 0.25)])
    path = save_prior(t, tmp_path / "p.npz")
    sidecar = path.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    meta["nnz"] = 99
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="sidecar reports nnz"):
        load_prior(path)


def test_the_sidecar_records_the_week_stride(tmp_path):
    """The stride is half the column packing; without it the matrix is unreadable."""
    t = _saveable([(1, "cell-a", 46, 0.25)])
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert meta["weeks_per_year"] == WEEKS_PER_YEAR


def test_a_sidecar_with_a_different_week_stride_is_rejected(tmp_path):
    """Every column index would be read against the wrong stride."""
    path = save_prior(_saveable([(1, "cell-a", 46, 0.25)]), tmp_path / "p.npz")
    sidecar = path.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    meta["weeks_per_year"] = WEEKS_PER_YEAR + 1
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="weeks_per_year"):
        load_prior(path)


# --------------------------------------------------------------------------
# T8-R3: scipy APPENDS `.npz`, and `with_suffix` TRUNCATES at the last dot
# --------------------------------------------------------------------------
def test_save_returns_a_path_that_exists_for_a_non_npz_name(tmp_path):
    """scipy APPENDS `.npz`, so the returned path must name what it wrote."""
    t = _saveable([(1, "cell-a", 46, 0.25)])
    path = save_prior(t, tmp_path / "p.dat")
    assert path.exists()
    assert path.name == "p.dat.npz"


def test_load_reads_back_what_save_returned_for_a_non_npz_name(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25)])
    back = load_prior(save_prior(t, tmp_path / "p.dat"))
    assert back.lookup(1, "cell-a", 46) == pytest.approx(0.25)


def test_load_accepts_the_same_non_npz_name_that_save_was_given(tmp_path):
    """`load_prior` must normalise the suffix too, not only `save_prior`.

    A caller that holds the name it passed to `save_prior` -- a config entry,
    a CLI argument -- and hands the SAME name to `load_prior` must get its
    tensor back. Reading the sidecar beside the raw path instead of the
    normalised one makes that round trip fail on any name not ending in
    `.npz`, and the returned-path test cannot see it because the returned path
    already ends in `.npz`.
    """
    name = tmp_path / "p.dat"
    t = _saveable([(1, "cell-a", 46, 0.25)])
    save_prior(t, name)
    assert not name.exists()  # precondition: the raw name is NOT a file
    back = load_prior(name)
    assert back.lookup(1, "cell-a", 46) == pytest.approx(0.25)


# --------------------------------------------------------------------------
# I4: the two reads `load_prior` performs are pinned SEPARATELY
#
# `load_prior` normalises the path once and then reads TWO files from it: the
# `.npz` and the JSON sidecar. A single mutant that de-normalised both was
# credited with covering the sidecar half while its kill came entirely from
# the matrix half -- and under the OLD truncating `_npz_path` the sidecar half
# was unkillable at all, because `_sidecar_path(_npz_path('p.dat'))` and
# `_sidecar_path('p.dat')` were BOTH `p.json`.
#
# The `_npz_path` fix separated them: `p.dat` now normalises to `p.dat.npz`,
# whose sidecar is `p.dat.json`, while the raw path's sidecar is still
# `p.json`. The two reads therefore address four distinct filenames and each
# can be pinned on its own.
#
# Each test below plants a DECOY at the location the OTHER half's mutant would
# read, so that mutant still passes and the test fails for exactly one reason.
# Merely asserting "loading a non-`.npz` name works" cannot do that: it fails
# for both halves and so proves neither.
# --------------------------------------------------------------------------
def test_load_reads_the_MATRIX_from_the_normalised_path_not_the_raw_one(tmp_path):
    """A decoy matrix at the RAW name must not be the one that answers.

    The raw name `p.dat` is a legitimate file path, and a variant sweep that
    writes both `p.dat` and `p.dat.npz` is not exotic. Reading the matrix from
    the raw path then returns ANOTHER artifact's values against THIS
    artifact's axes, with the same nnz so the agreement check passes.

    A valid copy of the real sidecar is planted at `p.json` on purpose, so a
    mutant that de-normalises only the SIDECAR read still passes here: the
    failure this test reports is the matrix read and nothing else.
    """
    real = _saveable([(1, "cell-a", 46, 0.25)])
    npz = save_prior(real, tmp_path / "p.dat")
    assert npz.name == "p.dat.npz"  # precondition: the suffix was APPENDED

    decoy = _saveable([(1, "cell-a", 46, 0.75)])
    sparse.save_npz(str(tmp_path / "decoy.npz"), decoy.values.tocoo())
    shutil.copyfile(tmp_path / "decoy.npz", tmp_path / "p.dat")
    # Precondition: the decoy is a DIFFERENT artifact with the SAME nnz, so it
    # passes the sidecar's agreement check and is caught only by its value.
    assert (tmp_path / "p.dat").exists()
    assert decoy.values.nnz == real.values.nnz

    # The sidecar half is neutralised for this test: both filenames resolve to
    # an identical sidecar, so only the matrix read can fail.
    shutil.copyfile(tmp_path / "p.dat.json", tmp_path / "p.json")

    assert load_prior(tmp_path / "p.dat").lookup(1, "cell-a", 46) == pytest.approx(0.25)


def test_load_reads_the_SIDECAR_from_the_normalised_path_not_the_raw_one(tmp_path):
    """A decoy sidecar at the RAW name's sidecar location must not be the one read.

    `p.dat` normalises to `p.dat.npz`, whose sidecar is `p.dat.json`; the raw
    path's own sidecar would be `p.json`. A sidecar names which species a row
    is and which cell a column block belongs to, so reading the wrong one
    decodes a correct matrix against foreign axes -- every lookup by a real H3
    cell then raises or, worse, silently answers under another cell's name.

    A valid copy of the matrix is planted at the raw path `p.dat` on purpose,
    so a mutant that de-normalises only the MATRIX read still passes here.
    """
    real = _saveable([(1, "cell-a", 46, 0.25)])
    npz = save_prior(real, tmp_path / "p.dat")
    assert npz.name == "p.dat.npz"  # precondition: the suffix was APPENDED

    decoy_meta = json.loads((tmp_path / "p.dat.json").read_text(encoding="utf-8"))
    decoy_meta["cells"] = ["decoy-cell"]
    (tmp_path / "p.json").write_text(json.dumps(decoy_meta), encoding="utf-8")
    # Precondition: the decoy is a VALID sidecar for this matrix's shape -- same
    # axis lengths, same nnz -- so no agreement check rejects it and the only
    # thing wrong with it is WHICH cell it names.
    assert decoy_meta["nnz"] == real.values.nnz
    assert len(decoy_meta["cells"]) == len(real.cells)
    assert decoy_meta["cells"] != real.cells

    # The matrix half is neutralised: the raw path holds the same matrix.
    shutil.copyfile(tmp_path / "p.dat.npz", tmp_path / "p.dat")

    assert load_prior(tmp_path / "p.dat").cells == ["cell-a"]


def test_a_sidecar_declaring_a_different_axis_LENGTH_than_the_matrix_is_rejected(
    tmp_path,
):
    """M3: the I4 decoy varies the axis VALUES but not its declared LENGTH.

    `test_load_reads_the_SIDECAR_from_the_normalised_path_not_the_raw_one`
    plants a decoy whose cell axis is `["decoy-cell"]` -- a DIFFERENT cell, but
    still exactly one of them. So the decoy's DECLARED DIMENSIONS match the
    matrix's, and a reader that took the grid's shape from the sidecar rather
    than from the matrix bytes passes it. That is a real reader shape: the
    sidecar publishes `species_keys`, `cells` and `weeks_per_year`, from which
    `len(species_keys) x len(cells) * weeks_per_year` is the obvious way to
    size the grid, and this document tells third parties to do exactly that.

    This decoy therefore differs in LENGTH: two cells declared against a matrix
    that encodes one cell's 52 columns. The two halves are then not descriptions
    of the same artifact and `PriorTensor` must refuse them. A reader that
    instead RESHAPED the matrix to the declared dimensions would pad it with a
    silent second cell-block of implicit zeros -- which this boundary reads as
    the floor -- and hand back a plausible prior over a cell the data never had.

    The decoy is planted at the NORMALISED sidecar location, not at the raw
    one, precisely because the mutant it exists to kill only bites when the
    shape-mismatched sidecar is the one actually read.
    """
    real = _saveable([(1, "cell-a", 46, 0.25)])
    path = save_prior(real, tmp_path / "p.npz")
    sidecar = path.with_suffix(".json")
    meta = json.loads(sidecar.read_text(encoding="utf-8"))

    meta["cells"] = ["cell-a", "cell-b"]
    sidecar.write_text(json.dumps(meta), encoding="utf-8")

    # Preconditions: nnz still agrees and the stride is untouched, so neither
    # of those guards is what fires -- only the declared-vs-encoded SHAPE.
    assert meta["nnz"] == real.values.nnz
    assert meta["weeks_per_year"] == WEEKS_PER_YEAR
    assert real.values.get_shape()[1] == 1 * WEEKS_PER_YEAR
    assert len(meta["cells"]) * WEEKS_PER_YEAR != real.values.get_shape()[1]

    with pytest.raises(ValueError, match=r"cells x \d+ weeks needs"):
        load_prior(path)


def test_a_sidecar_declaring_a_different_SPECIES_axis_length_is_rejected(tmp_path):
    """The row half of the same hole; its own test because one cannot see the other.

    A matrix can have the right width and the wrong height. A reader sizing the
    grid from the sidecar pads a second species row of implicit zeros, which
    this boundary reads as the floor, and every lookup of that invented species
    is answered with a plausible prior.
    """
    real = _saveable([(1, "cell-a", 46, 0.25)])
    path = save_prior(real, tmp_path / "p.npz")
    sidecar = path.with_suffix(".json")
    meta = json.loads(sidecar.read_text(encoding="utf-8"))

    meta["species_keys"] = [1, 2]
    sidecar.write_text(json.dumps(meta), encoding="utf-8")

    assert meta["nnz"] == real.values.nnz  # precondition: the nnz guard is silent
    assert len(meta["species_keys"]) != real.values.get_shape()[0]

    with pytest.raises(ValueError, match="species keys"):
        load_prior(path)


def test_two_names_differing_only_after_a_dot_produce_two_distinct_artifacts(tmp_path):
    """`with_suffix` TRUNCATES at the last dot -- measured silent DATA LOSS.

    `Path('prior_v1.2').with_suffix('.npz')` is `prior_v1.npz`, and so is
    `Path('prior_v1.3').with_suffix('.npz')`. Saving both versions then left
    ONE file on disk holding the SECOND tensor, with no error raised anywhere,
    while a caller asking for `prior_v1.2` was handed `prior_v1.3`'s values. At
    a release boundary that is the worst class of defect: a wrong artifact
    under a right name. The suffix must be APPENDED.
    """
    first = _saveable([(1, "cell-a", 46, 0.25)])
    second = _saveable([(1, "cell-a", 46, 0.75)])
    a = save_prior(first, tmp_path / "prior_v1.2")
    b = save_prior(second, tmp_path / "prior_v1.3")

    assert a != b
    assert a.exists() and b.exists()
    assert sorted(f.name for f in tmp_path.iterdir()) == [
        "prior_v1.2.json",
        "prior_v1.2.npz",
        "prior_v1.3.json",
        "prior_v1.3.npz",
    ]
    # Each name must still hand back ITS OWN values, not the other's.
    assert load_prior(tmp_path / "prior_v1.2").lookup(1, "cell-a", 46) == pytest.approx(
        0.25
    )
    assert load_prior(tmp_path / "prior_v1.3").lookup(1, "cell-a", 46) == pytest.approx(
        0.75
    )


def test_a_dotted_name_keeps_its_sidecar_beside_its_own_matrix(tmp_path):
    """The sidecar must be as distinct as the `.npz`, or two matrices share axes."""
    save_prior(_saveable([(1, "cell-a", 46, 0.25)]), tmp_path / "prior_v1.2")
    save_prior(_saveable([(2, "cell-b", 3, 0.75)]), tmp_path / "prior_v1.3")
    assert load_prior(tmp_path / "prior_v1.2").species_keys.tolist() == [1]
    assert load_prior(tmp_path / "prior_v1.3").species_keys.tolist() == [2]
    assert load_prior(tmp_path / "prior_v1.2").cells == ["cell-a"]
    assert load_prior(tmp_path / "prior_v1.3").cells == ["cell-b"]


def test_the_sidecar_sits_beside_the_npz_that_was_written(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25)])
    path = save_prior(t, tmp_path / "p.dat")
    assert sorted(f.name for f in tmp_path.iterdir()) == ["p.dat.json", "p.dat.npz"]
    assert path.with_suffix(".json").exists()


def test_save_creates_missing_parent_directories(tmp_path):
    t = _saveable([(1, "cell-a", 46, 0.25)])
    path = save_prior(t, tmp_path / "deep" / "nested" / "p.npz")
    assert path.exists()
    assert load_prior(path).lookup(1, "cell-a", 46) == pytest.approx(0.25)


def test_a_loaded_tensor_still_guards_its_week_range(tmp_path):
    """The guard travels with the object, not just with `to_tensor`."""
    t = _saveable([(1, "cell-a", 46, 0.25)])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        back.lookup(1, "cell-a", WEEKS_PER_YEAR + 1)


def test_a_loaded_tensor_still_guards_its_axes(tmp_path):
    """And so does the unknown-cell guard, which is now an error too."""
    t = _saveable([(1, "cell-a", 46, 0.25)])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    with pytest.raises(ValueError, match="is not on this tensor's cell axis"):
        back.lookup(1, "cell-zz", 46)


def test_an_empty_tensor_round_trips(tmp_path):
    t = _saveable([])
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.cells == []
    assert back.species_keys.tolist() == []
    assert back.values.nnz == 0


def test_the_sidecar_is_valid_json_with_the_spec_fields_spelled_out(tmp_path):
    """The sidecar is the citable half of the release; a reader must parse it."""
    t = _saveable([(1, "cell-a", 46, 0.25)])
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert meta["spec"] == {"resolution": 4, "source": "dof", "variant": "full"}
    assert meta["species_keys"] == [1]
    assert meta["cells"] == ["cell-a"]


# --------------------------------------------------------------------------
# an INDEPENDENT reader: the container, decoded without scipy
# --------------------------------------------------------------------------
def test_the_npz_is_decodable_by_a_third_party_with_numpy_alone(tmp_path):
    """The released format, read the way a third party will actually read it.

    Every other test in this file round-trips through `save_prior` and
    `load_prior` TOGETHER, so any mutation consistent on BOTH sides survives
    them -- a different scipy format, a renamed member, or a TRANSPOSE. The
    last is the one that matters most: an artifact whose rows are cells and
    whose columns are species decodes and round-trips perfectly here while
    every external reader gets every value wrong.

    So this decodes the `.npz` with `np.load` and no scipy at all, asserts the
    member names, the format tag, the dtypes and the shape, and rebuilds the
    values using ONLY the packing the SIDECAR publishes -- which is the whole
    of what a third party is given.
    """
    rows = [
        (1, "cell-a", 46, 0.25),
        (2, "cell-b", 3, 0.5),
        (1, "cell-b", WEEKS_PER_YEAR, 0.125),
    ]
    t = _saveable(rows)
    npz = save_prior(t, tmp_path / "p.npz")
    meta = json.loads(npz.with_suffix(".json").read_text())

    with np.load(npz) as z:
        assert sorted(z.files) == ["col", "data", "format", "row", "shape"]
        assert bytes(z["format"]) == b"coo"
        data, row, col, shape = z["data"], z["row"], z["col"], z["shape"]
        assert data.dtype == np.float64
        assert shape.tolist() == [
            len(meta["species_keys"]),
            len(meta["cells"]) * meta["weeks_per_year"],
        ]

        # Orientation: rows index SPECIES, columns index cell-week. A
        # transposed artifact has these the other way round and fails here.
        assert int(shape[0]) == len(meta["species_keys"])
        assert int(shape[1]) == len(meta["cells"]) * meta["weeks_per_year"]
        assert row.max() < shape[0]
        assert col.max() < shape[1]

        # Decode every stored entry using the sidecar's own packing:
        #     row = index into species_keys
        #     col = index into cells * weeks_per_year + (week - 1)
        weeks_per_year = int(meta["weeks_per_year"])
        decoded = {}
        for r, c, v in zip(row.tolist(), col.tolist(), data.tolist()):
            species = meta["species_keys"][r]
            cell = meta["cells"][c // weeks_per_year]
            week = c % weeks_per_year + 1
            decoded[(species, cell, week)] = v

    assert decoded == {(k, cell, wk): p for k, cell, wk, p in rows}
    assert len(decoded) == meta["nnz"]
    # And the fill the sidecar publishes describes this very matrix.
    assert meta["dense_capacity"] == int(shape[0]) * int(shape[1])


def test_an_independent_reader_recovers_the_floor_from_the_sidecar(tmp_path):
    """A third party must reach the SAME answers as `lookup`, sidecar only.

    The independent decode above proves the stored entries are recoverable.
    This proves the ABSENT ones are too: `missing_value` is what the sidecar
    publishes for them, and filling the grid with it must reproduce
    `dense()` exactly. Without that field a third party reads 0.0 for most of
    the grid and silently vetoes every unobserved species.
    """
    t = _saveable([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)])
    npz = save_prior(t, tmp_path / "p.npz")
    meta = json.loads(npz.with_suffix(".json").read_text())

    with np.load(npz) as z:
        shape = tuple(int(x) for x in z["shape"])
        grid = np.full(shape, float(meta["missing_value"]), dtype=np.float64)
        grid[z["row"], z["col"]] = z["data"]

    reference = load_prior(npz).dense()
    assert grid.shape == reference.shape
    assert grid.ravel().tolist() == pytest.approx(reference.ravel().tolist())


# --------------------------------------------------------------------------
# I5: the RELEASED DOCUMENTATION, executed against a freshly saved artifact
#
# `docs/prior-artifact-format.md` is aimed at someone who has downloaded the
# files and is not reading this package. Prose that contradicts the code six
# months from now is worse than no prose at all, so the load-bearing facts in
# it are not trusted: the sidecar example is parsed and its KEYS compared with
# a real sidecar, the published constants are compared with the constants, and
# the decoding recipe is EXECUTED and required to reproduce `dense()` and
# `lookup()` exactly.
# --------------------------------------------------------------------------
ARTIFACT_DOC = Path(__file__).parent.parent.parent / "docs" / "prior-artifact-format.md"


def _fenced_blocks(text, language):
    """Every ```<language> fenced block in a markdown document, in order."""
    return re.findall(
        rf"^```{language}\n(.*?)^```", text, flags=re.DOTALL | re.MULTILINE
    )


def test_the_artifact_documentation_matches_a_freshly_saved_artifact(tmp_path):
    """The released documentation, checked against the artifact it describes.

    Every claim exercised here is one a third party would act on with their own
    tooling and could not detect being wrong: the sidecar's FIELD NAMES, the
    `.npz` MEMBER NAMES, the literal value of the floor, the licence terms, and
    the decoding recipe itself. If `save_prior` renames a field or the floor
    moves, this fails rather than the document quietly becoming a lie.
    """
    doc = ARTIFACT_DOC.read_text(encoding="utf-8")

    rows = [
        (1, "841f059ffffffff", 46, 0.25),
        (2, "841f05bffffffff", 3, 0.5),
        (1, "841f05bffffffff", WEEKS_PER_YEAR, 0.125),
    ]
    t = _saveable(rows)
    npz = save_prior(t, tmp_path / "prior_res4_dof_full.npz")
    sidecar = npz.with_suffix(".json")
    meta = json.loads(sidecar.read_text(encoding="utf-8"))

    # -- the sidecar example documents exactly the fields that are written --
    example = json.loads(_fenced_blocks(doc, "json")[0])
    assert sorted(example) == sorted(meta), (
        "the documented sidecar example and the real sidecar have different "
        "fields; a third party is being told about a field that does not "
        "exist, or not told about one that does"
    )
    assert sorted(example["spec"]) == sorted(meta["spec"])

    # -- and the constants it publishes are the constants --
    assert example["missing_value"] == FLOOR
    assert example["weeks_per_year"] == WEEKS_PER_YEAR
    assert example["licence"] == LICENCE
    assert example["licence_url"] == LICENCE_URL
    # The floor's literal value must appear in the PROSE too, not only in the
    # example: the one sentence a third party has to read is "unstored means
    # this number".
    assert repr(FLOOR) in doc
    assert LICENCE in doc and LICENCE_URL in doc

    # -- the `.npz` member names the document lists are the ones written --
    documented_members = re.search(r"<!-- npz-members: (.*?) -->", doc).group(1).split()
    with np.load(npz) as z:
        assert sorted(z.files) == sorted(documented_members)

    # -- the decoding recipe, EXECUTED --
    recipe = {}
    exec(_fenced_blocks(doc, "python")[0], recipe)  # noqa: S102 - the doc IS the test
    doc_meta, grid = recipe["load_dense"](npz, sidecar)
    assert doc_meta == meta

    reference = load_prior(npz).dense()
    assert grid.shape == reference.shape
    assert grid.ravel().tolist() == pytest.approx(reference.ravel().tolist())

    # A STORED context: the recipe's lookup agrees with ours.
    for key, cell, week, p in rows:
        assert recipe["lookup"](doc_meta, grid, key, cell, week) == pytest.approx(p)
        assert recipe["lookup"](doc_meta, grid, key, cell, week) == pytest.approx(
            t.lookup(key, cell, week)
        )

    # An UNSTORED context on both axes: the whole point of the document. A
    # reader following it must reach the FLOOR here, not 0.0.
    assert t.values[0, _column_index(0, 7)] == 0.0  # precondition: nothing stored
    assert recipe["lookup"](doc_meta, grid, 1, "841f059ffffffff", 7) == pytest.approx(
        FLOOR
    )
    assert recipe["lookup"](doc_meta, grid, 1, "841f059ffffffff", 7) != 0.0
    assert recipe["lookup"](doc_meta, grid, 1, "841f059ffffffff", 7) == pytest.approx(
        t.lookup(1, "841f059ffffffff", 7)
    )

    # And an off-axis key is an ERROR in the recipe too, not a floor: the
    # document tells third parties to raise, so the code it ships must.
    with pytest.raises(ValueError, match="species axis"):
        recipe["lookup"](doc_meta, grid, 999, "841f059ffffffff", 7)
    with pytest.raises(ValueError, match="cell axis"):
        recipe["lookup"](doc_meta, grid, 1, "not-a-cell", 7)


def test_the_documented_fill_arithmetic_is_the_one_the_sidecar_publishes(tmp_path):
    """`nnz / dense_capacity` is how the document says to recover coverage.

    `lookup` is total over the axis cross-product, so it can never report
    absence; the document directs readers to the stored coordinates and to this
    ratio instead. Both halves of it must therefore describe the real matrix.
    """
    doc = ARTIFACT_DOC.read_text(encoding="utf-8")
    assert "fill = nnz / dense_capacity" in doc

    t = _saveable([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)])
    npz = save_prior(t, tmp_path / "p.npz")
    meta = json.loads(npz.with_suffix(".json").read_text(encoding="utf-8"))

    with np.load(npz) as z:
        stored = len(z["data"])
        capacity = int(z["shape"][0]) * int(z["shape"][1])

    assert meta["nnz"] == stored
    assert meta["dense_capacity"] == capacity
    assert meta["nnz"] / meta["dense_capacity"] == pytest.approx(
        t.values.nnz / t.dense_capacity
    )
