"""The four control priors. A gain surviving all four is a real effect.

Task 9 is the credibility of the paper: a control that is subtly wrong makes
the headline claim wrong, and nothing downstream can detect it. So these tests
pin the two properties the brief's own code got wrong -- SCALE (T9-R2: a
control on a different scale from the full prior turns the comparison into a
scale difference) and OBSERVABILITY (T9-R1/T9-R3: a fixture that cannot show
the effect pins nothing) -- as hard as they pin the obvious ones.

The fixture is built once, deliberately, to distinguish the RIGHT reduction
from the wrong ones. Its preconditions are asserted rather than assumed
(`test_the_fixture_*`), because every value assertion below is only as strong
as the fixture that feeds it.

Guard messages are matched on wording specific to THAT check -- never a bare
word (`dense`, `zero`, `cells`) that another layer could also produce.
"""
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.controls import (
    CONTROLS,
    marginal_prior,
    shuffle_space,
    shuffle_time,
    uniform_prior,
)
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import PriorSpec, PriorTensor, to_tensor

SPEC = PriorSpec(resolution=4, source="dof", variant="full")

# Real lower-case canonical H3 resolution-4 spellings, so nothing here can pass
# by accident on a placeholder that a case-folding or re-derivation defect
# would leave alone. NOT pre-sorted: `to_tensor` sorts the axis, and a fixture
# that hands them in sorted order cannot tell a control that preserves the axis
# from one that re-derives it.
CELLS = ("8418c4bffffffff", "8418c1bffffffff", "8418c33ffffffff")
SPECIES = (5229499, 2482593)

# Species 2 carries exactly a quarter of species 1's mass in every context
# (T9-R1: the brief's fixture gave both species the same value MULTISET, so
# `marginal_prior` emitted an identical value for each and
# `test_marginal_still_varies_across_species` could not pass).
_MASS_RATIO = 0.25


def _p(cell_index: int, week: int, species_index: int) -> float:
    """A value that is distinct for (cell, week) and QUADRATIC in both.

    Quadratic in the week, not linear, so the per-species MEAN differs from the
    per-species MEDIAN: a `marginal_prior` that took a median would otherwise be
    indistinguishable from one that takes a mean, and only the mean preserves
    mass.

    Quadratic in the CELL index for a reason found the hard way: with a linear
    cell term, the middle cell's mean is exactly the mean over all cells, so
    `test_marginal_value_is_each_species_mean_over_contexts` could not tell a
    mean over all contexts from a mean over one cell's block. The cell axis is
    also handed in unsorted, and `to_tensor` sorts it, so WHICH cell lands in
    block 0 is not the one the fixture writes first.

    Monotone in the cell index and spanning a wide range within each cell, so
    the three cell blocks have different masses AND different weekly profiles:
    `shuffle_space` is then observable at all, and a permutation that moved a
    cell onto a lookalike block would not be.

    Every value stays inside `(0, 1]`, which `_validate_p` requires: the
    coefficients are scaled so the largest (cell 3, week 52) is well under one.
    """
    base = 0.01 + 0.02 * (cell_index + 1) ** 2 + 0.0001 * week * week
    return base * (_MASS_RATIO if species_index else 1.0)


def _rows():
    """A DENSE grid: every species x cell x week, with no holes.

    Dense because `tensor.py` makes an absent context a STRUCTURAL ZERO and the
    controls refuse one (see `controls.py`); and because a sparse fixture would
    let a mass assertion pass for the wrong reason.
    """
    return [
        (SPECIES[si], cell, week, _p(ci, week, si))
        for si in range(len(SPECIES))
        for ci, cell in enumerate(CELLS)
        for week in range(1, WEEKS_PER_YEAR + 1)
    ]


def _tensor():
    rows = _rows()
    return to_tensor(
        pa.table(
            {
                "species_key": pa.array([r[0] for r in rows], pa.int64()),
                "cell": pa.array([r[1] for r in rows], pa.string()),
                "week": pa.array([r[2] for r in rows], pa.int8()),
                "p": pa.array([r[3] for r in rows], pa.float64()),
            }
        ),
        SPEC,
    )


def _sparse_tensor():
    """The same grid with ONE context dropped -- a structural zero."""
    rows = [r for r in _rows() if not (r[1] == CELLS[0] and r[2] == 7)]
    return to_tensor(
        pa.table(
            {
                "species_key": pa.array([r[0] for r in rows], pa.int64()),
                "cell": pa.array([r[1] for r in rows], pa.string()),
                "week": pa.array([r[2] for r in rows], pa.int8()),
                "p": pa.array([r[3] for r in rows], pa.float64()),
            }
        ),
        SPEC,
    )


# ---------------------------------------------------------------------------
# The fixture's own preconditions. Every assertion below rests on these, and a
# fixture that stopped satisfying one would silently weaken its tests rather
# than fail. Asserted directly, never through a proxy.
# ---------------------------------------------------------------------------


def test_the_fixture_is_a_dense_grid_with_no_structural_zeros():
    t = _tensor()
    n_rows, n_cols = t.values.get_shape()
    assert n_cols == len(CELLS) * WEEKS_PER_YEAR
    assert t.values.nnz == n_rows * n_cols
    assert t.values.count_nonzero() == t.values.nnz


def test_the_fixture_values_are_probabilities():
    """`_validate_p` requires (0, 1]; a coefficient change that pushed the
    fixture above one would make every test below fail on the FIXTURE rather
    than on the behaviour it is meant to pin."""
    dense = _tensor().values.toarray()
    assert dense.min() > 0.0
    assert dense.max() <= 1.0


def test_the_fixture_gives_the_two_species_different_total_mass():
    """T9-R1: equal per-species mass makes the marginal control unobservable."""
    dense = _tensor().values.toarray()
    assert dense.shape[0] == 2
    assert not np.isclose(dense[0].sum(), dense[1].sum())


def test_the_fixture_distinguishes_a_mean_from_a_median_or_a_max():
    """A fixture where mean == median would not pin the mass-preserving choice."""
    dense = _tensor().values.toarray()
    for row in dense:
        assert not np.isclose(row.mean(), np.median(row))
        assert not np.isclose(row.mean(), row.max())
    assert not np.isclose(dense.mean(), np.median(dense))
    assert not np.isclose(dense.mean(), dense.max())


def test_the_fixture_distinguishes_a_global_mean_from_a_per_species_mean():
    """Otherwise `uniform_prior` and `marginal_prior` would be the same function."""
    dense = _tensor().values.toarray()
    for row in dense:
        assert not np.isclose(row.mean(), dense.mean())


def test_the_fixture_distinguishes_a_mean_over_contexts_from_a_per_cell_mean():
    """Found the hard way: with a LINEAR cell term the middle cell's mean is
    exactly the mean over all cells, and `marginal_prior` taking a mean over one
    cell's block instead of over every context was then unobservable. The cell
    axis is sorted by `to_tensor`, so this must hold for EVERY block, not just
    the one the fixture happens to write first."""
    dense = _tensor().values.toarray()
    blocks = dense.reshape(dense.shape[0], len(CELLS), WEEKS_PER_YEAR)
    for c in range(len(CELLS)):
        assert not np.allclose(dense.mean(axis=1), blocks[:, c, :].mean(axis=1))


def test_the_fixture_makes_the_uniform_fill_differ_from_one_over_n_species():
    """The T9-R2 defect: `1 / n_species` must be a DIFFERENT number here."""
    dense = _tensor().values.toarray()
    assert not np.isclose(dense.mean(), 1.0 / dense.shape[0])


def test_the_fixture_gives_every_cell_a_distinct_weekly_profile():
    """T9-R3: identical cell blocks make a space permutation invisible."""
    blocks = _tensor().values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for s in range(len(SPECIES)):
        for i in range(len(CELLS)):
            for j in range(i + 1, len(CELLS)):
                assert not np.allclose(blocks[s, i], blocks[s, j])


def test_the_fixture_gives_every_week_slot_a_distinct_value():
    """T9-R3: repeated weekly values would let a real permutation look like a no-op."""
    blocks = _tensor().values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for s in range(len(SPECIES)):
        for c in range(len(CELLS)):
            assert len(set(blocks[s, c].tolist())) == WEEKS_PER_YEAR


# ---------------------------------------------------------------------------
# uniform: no information at all
# ---------------------------------------------------------------------------


def test_uniform_is_flat_across_species():
    u = uniform_prior(_tensor())
    vec = u.vector(CELLS[0], 20)
    assert np.allclose(vec, vec[0])


def test_uniform_is_flat_across_contexts_too():
    """Flat across species is not enough: variation over cell or week is information."""
    u = uniform_prior(_tensor())
    reference = u.vector(CELLS[0], 1)
    for cell in CELLS:
        for week in (1, 7, 20, WEEKS_PER_YEAR):
            assert np.allclose(u.vector(cell, week), reference)


def test_uniform_variant_is_labelled():
    assert uniform_prior(_tensor()).spec.variant == "uniform"


def test_uniform_preserves_the_full_priors_total_mass():
    """T9-R2. The defect that would decide the paper's headline comparison.

    Every fusion method combines prior with posterior multiplicatively or via
    `log prior`, so a control at a different scale is not a null hypothesis:
    the comparison measures a scale difference and reports it as a context
    effect. Every value stays finite and in range, so no other test sees it.
    """
    t = _tensor()
    u = uniform_prior(t)
    assert u.values.sum() == pytest.approx(t.values.sum())


def test_uniform_does_not_fill_with_one_over_n_species():
    """The brief's own spelling, pinned as wrong rather than merely unused.

    `1 / n_species` makes every context sum to exactly 1.0 -- a renormalisation
    of a prior that is never renormalised at any stage.
    """
    t = _tensor()
    u = uniform_prior(t)
    vec = u.vector(CELLS[0], 20)
    assert not np.isclose(vec[0], 1.0 / len(t.species_keys))
    assert not np.isclose(vec.sum(), 1.0)


def test_uniform_fill_is_the_mean_over_the_whole_matrix():
    """Pins the exact constant, so a different mass-preserving-looking reduction
    (a per-cell mean, a median, a max) cannot pass."""
    t = _tensor()
    dense = t.values.toarray()
    assert uniform_prior(t).vector(CELLS[1], 33)[0] == pytest.approx(dense.mean())


def test_uniform_keeps_the_axes_byte_for_byte():
    """A control whose cells drifted would answer every real lookup with a miss,
    and the sweep would read that as the control scoring badly."""
    t = _tensor()
    u = uniform_prior(t)
    assert u.cells == t.cells
    assert list(u.species_keys) == list(t.species_keys)


def test_uniform_carries_the_provenance_of_the_prior_it_controls():
    t = _tensor()
    t.gbif_doi = "10.15468/dl.control9"
    assert uniform_prior(t).gbif_doi == "10.15468/dl.control9"
    assert uniform_prior(t).spec.resolution == t.spec.resolution
    assert uniform_prior(t).spec.source == t.spec.source


# ---------------------------------------------------------------------------
# marginal: species frequency only -- THE control
# ---------------------------------------------------------------------------


def test_marginal_ignores_cell_and_week():
    m = marginal_prior(_tensor())
    assert np.allclose(m.vector(CELLS[0], 20), m.vector(CELLS[1], 1))


def test_marginal_is_constant_across_every_context():
    m = marginal_prior(_tensor())
    reference = m.vector(CELLS[0], 1)
    for cell in CELLS:
        for week in (1, 7, 20, WEEKS_PER_YEAR):
            assert np.allclose(m.vector(cell, week), reference)


def test_marginal_still_varies_across_species():
    """The point of the control: label frequency survives, context does not."""
    m = marginal_prior(_tensor())
    vec = m.vector(CELLS[0], 20)
    assert not np.allclose(vec, vec[0])


def test_marginal_variant_is_labelled():
    assert marginal_prior(_tensor()).spec.variant == "marginal"


def test_marginal_preserves_the_full_priors_total_mass():
    """T9-R2: the brief's `per_species / total` put this control at 2.75x the
    full prior's mass on a three-context example."""
    t = _tensor()
    m = marginal_prior(t)
    assert m.values.sum() == pytest.approx(t.values.sum())


def test_marginal_preserves_each_species_own_mass():
    """Stronger than total mass, and the property that makes this control the
    label-frequency null: a reduction that preserved the TOTAL while moving mass
    BETWEEN species would have changed the very signal under test."""
    t = _tensor()
    m = marginal_prior(t)
    before = np.asarray(t.values.sum(axis=1)).ravel()
    after = np.asarray(m.values.sum(axis=1)).ravel()
    assert after == pytest.approx(before)


def test_marginal_does_NOT_renormalise_over_species():
    """The prior is never renormalised at any stage (backoff, smoothing,
    tensor). Dividing by the sum over species would also make the control's
    scale a function of how many species the tensor happens to hold."""
    t = _tensor()
    vec = marginal_prior(t).vector(CELLS[0], 20)
    assert not np.isclose(vec.sum(), 1.0)
    dense = t.values.toarray()
    ratio = dense.mean(axis=1)
    ratio = ratio / ratio.sum()
    assert not np.allclose(vec, ratio)


def test_marginal_value_is_each_species_mean_over_contexts():
    """Pins the mean specifically: a sum, a max or a per-cell mean would each
    give a different number on this fixture."""
    t = _tensor()
    dense = t.values.toarray()
    vec = marginal_prior(t).vector(CELLS[2], 44)
    assert vec == pytest.approx(dense.mean(axis=1))
    assert not np.allclose(vec, dense.sum(axis=1))
    assert not np.allclose(vec, dense.max(axis=1))
    blocks = dense.reshape(dense.shape[0], len(CELLS), WEEKS_PER_YEAR)
    assert not np.allclose(vec, blocks[:, 0, :].mean(axis=1))


def test_marginal_keeps_the_axes_byte_for_byte():
    t = _tensor()
    m = marginal_prior(t)
    assert m.cells == t.cells
    assert list(m.species_keys) == list(t.species_keys)


# ---------------------------------------------------------------------------
# shuffle_space: real values, wrong places
# ---------------------------------------------------------------------------


def test_shuffle_space_permutes_cells_deterministically():
    t = _tensor()
    a = shuffle_space(t, seed=0)
    b = shuffle_space(t, seed=0)
    assert np.allclose(a.vector(CELLS[0], 20), b.vector(CELLS[0], 20))


def test_shuffle_space_preserves_the_value_multiset():
    t = _tensor()
    s = shuffle_space(t, seed=1)
    assert sorted(t.values.toarray().ravel()) == pytest.approx(
        sorted(s.values.toarray().ravel())
    )


def test_shuffle_space_actually_moves_a_cell():
    """T9-R3: the multiset test above is satisfied by a NO-OP permutation.

    The seed is not trusted -- the precondition (this seed's permutation of the
    cell axis is not the identity) is asserted here, so a seed change cannot
    silently turn this into a test of nothing.
    """
    n_cells = len(CELLS)
    perm = np.random.default_rng(3).permutation(n_cells)
    assert not np.array_equal(perm, np.arange(n_cells)), "seed 3 must not be the identity"
    t = _tensor()
    s = shuffle_space(t, seed=3)
    moved = [
        c
        for i, c in enumerate(t.cells)
        if not np.allclose(s.vector(c, 20), t.vector(c, 20))
    ]
    assert moved, "shuffle_space left every cell's values where they were"


def test_shuffle_space_relocates_whole_cells_not_individual_values():
    """The construction T9-R3 names: one permutation of the cell axis, shared
    across every species, so each cell's weekly profile arrives intact."""
    t = _tensor()
    s = shuffle_space(t, seed=3)
    perm = np.random.default_rng(3).permutation(len(CELLS))
    before = t.values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    after = s.values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    assert after == pytest.approx(before[:, perm, :])


def test_shuffle_space_keeps_each_week_slot_in_its_own_week():
    """Space only. A value that also changed week would make a failed
    shuffle_space control unattributable to space."""
    t = _tensor()
    s = shuffle_space(t, seed=3)
    before = t.values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    after = s.values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for w in range(WEEKS_PER_YEAR):
        assert sorted(after[:, :, w].ravel()) == pytest.approx(
            sorted(before[:, :, w].ravel())
        )


def test_shuffle_space_preserves_total_mass():
    t = _tensor()
    assert shuffle_space(t, seed=1).values.sum() == pytest.approx(t.values.sum())


def test_shuffle_space_variant_is_labelled():
    assert shuffle_space(_tensor(), seed=0).spec.variant == "shuffle_space"


def test_shuffle_space_uses_its_seed():
    """Two seeds whose cell permutations differ must give different tensors,
    or the sweep's replicates would all be the same control."""
    t = _tensor()
    a = np.random.default_rng(2).permutation(len(CELLS))
    b = np.random.default_rng(3).permutation(len(CELLS))
    assert not np.array_equal(a, b), "seeds 2 and 3 must differ on the cell axis"
    assert not np.allclose(
        shuffle_space(t, seed=2).values.toarray(),
        shuffle_space(t, seed=3).values.toarray(),
    )


# ---------------------------------------------------------------------------
# shuffle_time: real values, wrong weeks
# ---------------------------------------------------------------------------


def test_shuffle_time_preserves_the_value_multiset():
    t = _tensor()
    s = shuffle_time(t, seed=1)
    assert sorted(t.values.toarray().ravel()) == pytest.approx(
        sorted(s.values.toarray().ravel())
    )


def test_shuffle_time_actually_moves_a_week():
    """T9-R3, the time mirror: the multiset test passes on a no-op permutation."""
    perm = np.random.default_rng(1).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(perm, np.arange(WEEKS_PER_YEAR)), "seed 1 must not be the identity"
    t = _tensor()
    s = shuffle_time(t, seed=1)
    moved = [
        w
        for w in range(1, WEEKS_PER_YEAR + 1)
        if not np.allclose(s.vector(CELLS[0], w), t.vector(CELLS[0], w))
    ]
    assert moved, "shuffle_time left every week's values where they were"


def test_shuffle_time_permutes_week_slots_with_one_shared_permutation():
    t = _tensor()
    s = shuffle_time(t, seed=1)
    perm = np.random.default_rng(1).permutation(WEEKS_PER_YEAR)
    before = t.values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    after = s.values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    assert after == pytest.approx(before[:, :, perm])


def test_shuffle_time_keeps_every_value_inside_its_own_cell():
    """Time only. A value that crossed into another cell would make a failed
    shuffle_time control unattributable to time -- and it is exactly what a
    reshape against the wrong stride would do."""
    t = _tensor()
    s = shuffle_time(t, seed=1)
    before = t.values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    after = s.values.toarray().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for c in range(len(CELLS)):
        assert sorted(after[:, c, :].ravel()) == pytest.approx(
            sorted(before[:, c, :].ravel())
        )


def test_shuffle_time_preserves_total_mass():
    t = _tensor()
    assert shuffle_time(t, seed=1).values.sum() == pytest.approx(t.values.sum())


def test_shuffle_time_variant_is_labelled():
    assert shuffle_time(_tensor(), seed=0).spec.variant == "shuffle_time"


def test_shuffle_time_is_deterministic_in_its_seed():
    t = _tensor()
    a = shuffle_time(t, seed=5)
    b = shuffle_time(t, seed=5)
    assert np.allclose(a.values.toarray(), b.values.toarray())


def test_shuffle_time_uses_its_seed():
    t = _tensor()
    assert not np.allclose(
        shuffle_time(t, seed=1).values.toarray(),
        shuffle_time(t, seed=2).values.toarray(),
    )


def test_shuffle_space_and_shuffle_time_are_different_controls():
    """Both reshape the same blocks; a copy-paste of the wrong axis would make
    two of the four controls the same experiment."""
    t = _tensor()
    assert not np.allclose(
        shuffle_space(t, seed=3).values.toarray(),
        shuffle_time(t, seed=3).values.toarray(),
    )


# ---------------------------------------------------------------------------
# The dense-grid guard: structural zeros must not silently dilute a control
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("control", list(CONTROLS.values()), ids=list(CONTROLS))
def test_a_tensor_with_a_structural_zero_is_rejected_by_every_control(control):
    """An absent context is a grid point the tensor was never handed. Averaging
    over it dilutes uniform/marginal by nnz/size; for the shuffles it is a hole
    that gets relocated onto a real context. All four refuse, so the sweep
    cannot receive a partially-valid control set."""
    with pytest.raises(ValueError, match="requires a DENSE grid"):
        control(_sparse_tensor())


def test_the_dense_guard_names_the_structural_zero_semantics():
    with pytest.raises(ValueError, match="STRUCTURAL ZERO"):
        uniform_prior(_sparse_tensor())


def test_an_explicitly_stored_zero_is_rejected():
    """Invisible to the nnz count, which counts STORED entries rather than
    non-zero ones, and it would drag the mean down by exactly the amount the
    dense-grid check exists to prevent."""
    t = _tensor()
    dense = t.values.toarray()
    # Built as COO so the zero is an EXPLICIT stored entry rather than a
    # structural one: `csr_matrix(dense)` would drop it and the tensor would
    # trip the dense-grid check instead of this one.
    rows, cols = np.indices(dense.shape)
    data = dense.copy()
    data[0, 3] = 0.0
    t.values = sparse.coo_matrix(
        (data.ravel(), (rows.ravel(), cols.ravel())), shape=dense.shape
    ).tocsr()
    assert t.values.nnz == dense.size, "the zero must be STORED, not dropped"
    assert t.values.count_nonzero() == dense.size - 1
    with pytest.raises(ValueError, match="storing an explicit zero"):
        marginal_prior(t)


def test_a_column_count_that_disagrees_with_the_cell_axis_is_rejected():
    """T9-R4. `PriorTensor` checks this at construction, but it is a mutable
    dataclass: reassigning `cells` re-opens the hole, and this module is the one
    that reshapes into (species, cell, week) blocks."""
    t = _tensor()
    t.cells = list(t.cells) + ["8418c53ffffffff"]
    with pytest.raises(ValueError, match="disagrees with the cell axis"):
        shuffle_space(t, seed=0)


def test_the_column_count_guard_runs_before_the_reshape():
    """Without it, a mismatch that happens to divide reshapes into a WRONG block
    structure and `shuffle_space` permutes something that is not a cell."""
    t = _tensor()
    t.cells = list(t.cells)[:-1]
    with pytest.raises(ValueError, match="disagrees with the cell axis"):
        shuffle_time(t, seed=0)


def test_the_column_count_guard_does_not_raise_on_a_valid_tensor():
    """The guard must not be so strict it rejects what `to_tensor` produces."""
    for control in CONTROLS.values():
        assert control(_tensor()).values.get_shape() == _tensor().values.get_shape()


# ---------------------------------------------------------------------------
# the registry
# ---------------------------------------------------------------------------


def test_controls_registry_covers_all_four():
    assert set(CONTROLS) == {"uniform", "marginal", "shuffle_space", "shuffle_time"}


def test_every_registered_control_labels_its_own_variant():
    """A registry whose key and whose function's variant disagreed would
    mislabel every row of the results table."""
    t = _tensor()
    for name, fn in CONTROLS.items():
        assert fn(t).spec.variant == name


def test_every_registered_control_is_callable_with_only_a_tensor():
    """The sweep calls them uniformly; a control that demanded a seed would
    break the loop that produces the paper's table."""
    t = _tensor()
    for fn in CONTROLS.values():
        assert isinstance(fn(t), PriorTensor)


def test_no_control_renormalises_a_context_to_one():
    """Three modules state the prior is never renormalised over species. A
    control that normalised would be the one place in the pipeline that does,
    and the comparison would measure that instead of a context effect."""
    t = _tensor()
    for name, fn in CONTROLS.items():
        vec = fn(t).vector(CELLS[0], 20)
        assert not np.isclose(vec.sum(), 1.0), name


def test_every_control_preserves_the_full_priors_total_mass():
    """The single T9-R2 property, asserted across all four at once: whatever the
    sweep compares, it compares at one scale."""
    t = _tensor()
    for name, fn in CONTROLS.items():
        assert fn(t).values.sum() == pytest.approx(t.values.sum()), name
