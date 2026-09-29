"""Sparse prior tensor, round-tripped through disk.

The serialisation boundary: every downstream stage reads the prior through
`PriorTensor.vector()` / `lookup()`, so a defect here is invisible to them.
The tests are grouped by the property they pin, and each guard's message is
matched on wording specific to THAT check rather than on a bare word another
layer could also produce.
"""
import json
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
    PriorSpec,
    PriorTensor,
    load_prior,
    save_prior,
    to_tensor,
)

SPEC = PriorSpec(resolution=4, source="dof", variant="full")


def _table(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "p": pa.array([r[3] for r in rows], pa.float64()),
        }
    )


# --------------------------------------------------------------------------
# lookup: the stored value, and a hard zero for anything absent
# --------------------------------------------------------------------------
def test_lookup_returns_stored_value():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(0.25)


def test_lookup_unknown_species_returns_zero():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.lookup(999, "cell-a", 46) == 0.0


def test_lookup_unknown_cell_returns_zero():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.lookup(1, "cell-zz", 46) == 0.0


def test_a_context_absent_from_the_table_is_a_structural_zero_not_the_floor():
    """T8-R5, pinned rather than only documented.

    Week 45 is not in the table, so it is a STRUCTURAL zero and reads as a
    hard 0.0 -- not `backoff.FLOOR`. This is why the caller must hand
    `to_tensor` a dense grid; see the module docstring and Task 10's T10-R1.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert 1 in set(int(k) for k in t.species_keys)  # precondition: species present
    assert "cell-a" in t.cells  # precondition: cell present
    assert t.lookup(1, "cell-a", 45) == 0.0
    assert t.lookup(1, "cell-a", 45) != pytest.approx(FLOOR)


def test_a_floor_valued_entry_survives_and_is_not_dropped_as_sparse_noise():
    """The floor must round-trip: it is what stops the prior vetoing a detection."""
    t = to_tensor(_table([(1, "cell-a", 46, FLOOR)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(FLOOR, rel=1e-12)
    assert t.values.nnz == 1


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


def test_an_out_of_range_week_raises_even_for_an_unknown_cell():
    """The unknown-cell `None` path must not swallow the week guard.

    A caller cannot tell a legitimate miss from an aliasing bug, so the week
    is validated before the cell is looked up.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert "cell-zz" not in t.cells  # precondition: the cell really is unknown
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        t.lookup(1, "cell-zz", WEEKS_PER_YEAR + 1)


def test_to_tensor_rejects_a_week_above_52_in_the_table():
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        to_tensor(_table([(1, "cell-a", WEEKS_PER_YEAR + 1, 0.25)]), SPEC)


def test_to_tensor_rejects_week_zero_in_the_table():
    with pytest.raises(ValueError, match="alias the last week of the PREVIOUS cell"):
        to_tensor(_table([(1, "cell-a", 0, 0.25)]), SPEC)


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
    """The dense grid the caller must supply (T8-R5) maps onto the shape exactly.

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
    """A hard zero would become a structural zero, indistinguishable from absent."""
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
    """`cell` is opaque and case-sensitive: no casefolding, no re-derivation."""
    cell = "841f1ffFfffffff"
    t = to_tensor(_table([(1, cell, 46, 0.25)]), SPEC)
    assert t.cells == [cell]
    assert t.lookup(1, cell, 46) == pytest.approx(0.25)
    assert t.lookup(1, cell.lower(), 46) == 0.0


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
    assert t.lookup(1, "cell-a", 46) == 0.0


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


def test_vector_for_an_unknown_cell_is_all_zeros_of_the_right_length():
    t = to_tensor(
        _table([(1, "cell-a", 46, 0.25), (2, "cell-a", 46, 0.75)]), SPEC
    )
    vec = t.vector("cell-zz", 46)
    assert vec.shape == (2,)
    assert not vec.any()


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
    assert t.vector("cell-a", 47).tolist() == pytest.approx([0.5, 0.0])
    assert t.vector("cell-b", 46).tolist() == pytest.approx([0.75, 0.0])


def test_vector_is_float64():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.vector("cell-a", 46).dtype == np.float64


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
# the on-disk format
# --------------------------------------------------------------------------
def test_round_trip_preserves_values(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)]), SPEC)
    path = save_prior(t, tmp_path / "p.npz")
    back = load_prior(path)
    assert back.lookup(1, "cell-a", 46) == pytest.approx(0.25)
    assert back.lookup(2, "cell-b", 3) == pytest.approx(0.5)


def test_round_trip_preserves_spec(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.spec == SPEC


def test_round_trip_preserves_the_axes_exactly(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)]), SPEC)
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.cells == t.cells
    assert back.species_keys.tolist() == t.species_keys.tolist()
    assert back.species_keys.dtype == np.int64


def test_round_trip_preserves_structural_zeros(tmp_path):
    """A missing context must still read 0.0 after a round trip, not the floor."""
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)]), SPEC)
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.lookup(1, "cell-b", 3) == 0.0
    assert back.values.nnz == 2


def test_round_trip_preserves_full_float64_precision(tmp_path):
    """A float32 write would silently round the floor the backoff guarantees."""
    value = 1.0 / 3.0
    t = to_tensor(_table([(1, "cell-a", 46, value)]), SPEC)
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.lookup(1, "cell-a", 46) == value


def test_sidecar_records_licence_and_doi(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    t.gbif_doi = "10.15468/dl.abc123"
    path = save_prior(t, tmp_path / "p.npz")
    meta = json.loads(path.with_suffix(".json").read_text())
    assert meta["licence"] == "CC BY 4.0"
    assert meta["gbif_doi"] == "10.15468/dl.abc123"


def test_the_licence_constant_is_the_one_written(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert meta["licence"] == LICENCE


def test_the_doi_survives_the_round_trip(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    t.gbif_doi = "10.15468/dl.abc123"
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.gbif_doi == "10.15468/dl.abc123"


def test_the_sidecar_records_the_week_stride(tmp_path):
    """The stride is half the column packing; without it the matrix is unreadable."""
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert meta["weeks_per_year"] == WEEKS_PER_YEAR


def test_a_sidecar_with_a_different_week_stride_is_rejected(tmp_path):
    """Every column index would be read against the wrong stride."""
    path = save_prior(to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC), tmp_path / "p.npz")
    sidecar = path.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    meta["weeks_per_year"] = WEEKS_PER_YEAR + 1
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="weeks_per_year"):
        load_prior(path)


def test_save_returns_a_path_that_exists_for_a_non_npz_name(tmp_path):
    """T8-R3: scipy APPENDS `.npz`, so the suffix must be normalised first.

    Unnormalised, `save_npz` wrote `p.dat.npz` while the returned path and the
    sidecar named `p.npz`, so the return value pointed at nothing.
    """
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    path = save_prior(t, tmp_path / "p.dat")
    assert path.exists()
    assert path.name == "p.npz"
    assert not (tmp_path / "p.dat.npz").exists()


def test_load_reads_back_what_save_returned_for_a_non_npz_name(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    back = load_prior(save_prior(t, tmp_path / "p.dat"))
    assert back.lookup(1, "cell-a", 46) == pytest.approx(0.25)


def test_the_sidecar_sits_beside_the_npz_that_was_written(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    path = save_prior(t, tmp_path / "p.dat")
    assert sorted(f.name for f in tmp_path.iterdir()) == ["p.json", "p.npz"]
    assert path.with_suffix(".json").exists()


def test_save_creates_missing_parent_directories(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    path = save_prior(t, tmp_path / "deep" / "nested" / "p.npz")
    assert path.exists()
    assert load_prior(path).lookup(1, "cell-a", 46) == pytest.approx(0.25)


def test_a_loaded_tensor_still_guards_its_week_range(tmp_path):
    """The guard travels with the object, not just with `to_tensor`."""
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    with pytest.raises(ValueError, match="alias week 1 of the NEXT cell"):
        back.lookup(1, "cell-a", WEEKS_PER_YEAR + 1)


def test_an_empty_tensor_round_trips(tmp_path):
    t = to_tensor(_table([]), SPEC)
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.cells == []
    assert back.species_keys.tolist() == []
    assert back.lookup(1, "cell-a", 46) == 0.0


def test_the_sidecar_is_valid_json_with_the_spec_fields_spelled_out(tmp_path):
    """The sidecar is the citable half of the release; a reader must parse it."""
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    meta = json.loads(save_prior(t, tmp_path / "p.npz").with_suffix(".json").read_text())
    assert meta["spec"] == {"resolution": 4, "source": "dof", "variant": "full"}
    assert meta["species_keys"] == [1]
    assert meta["cells"] == ["cell-a"]
