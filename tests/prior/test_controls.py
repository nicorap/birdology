"""The five control priors. A gain surviving all five is a real effect.

Task 9 is the credibility of the paper: a control that is subtly wrong makes
the headline claim wrong, and nothing downstream can detect it. So these tests
pin the two properties the brief's own code got wrong -- SCALE (T9-R2: a
control on a different scale from the full prior turns the comparison into a
scale difference) and OBSERVABILITY (T9-R1/T9-R3: a fixture that cannot show
the effect pins nothing) -- as hard as they pin the obvious ones.

THREE FIXTURES, AND THE PIPELINE ONE IS THE ANSWER TO C2
--------------------------------------------------------
`_tensor()` is a fully stored grid. It distinguishes the right reduction from
the wrong ones (mean vs median vs max vs sum vs per-cell mean) and it is where
the axis and permutation properties are pinned.

`_sparse_tensor()` stores 10 of 520 contexts -- 1.92% fill. It exists because a
previous revision of `controls.py` REFUSED any tensor with `nnz < size`, and
every test here passed anyway: the fixtures were all dense, so the suite was
green while all four controls raised on every realistic input. A property that
only ever sees a dense fixture is a property about the fixture.

`_pipeline_artifacts()` builds through `build_from_archive` -- the REAL CLI
path -- and is the answer to finding C2. BOTH hand-built fixtures above are
unrepresentative of a released artifact in the one way that matters most: a
pipeline build passes `cells=` and weeks `1..52` explicitly and `_validate_p`
forbids `p <= 0`, so every context is an explicit stored nonzero, fill is
EXACTLY 1.0000, and NOT ONE entry equals FLOOR. The released documentation and
this module's own docstrings previously quoted a "~98% of the grid is missing"
regime and a 7.943-nat log-space offset, both measured on the 1.92% fixture and
both false of anything shipped. Every NUMBER this file asserts about the
released artifact now comes from the pipeline fixture; the sparse fixture pins
only what it can legitimately speak for, and is labelled FIXTURE-ONLY where it
does.

Under `tensor.py`'s FLOOR-for-missing semantics the first two fixtures also
separate `t.dense()` from `t.values.toarray()`: on the dense fixture they are
the same array, so ONLY the sparse fixture can tell a control that reads the
prior the way the sweep reads it from one that re-derives it locally and
answers every unsurveyed context with a hard zero.

FIVE CELLS, NOT THREE, AND THE SEEDS ARE NOT TRUSTED
-----------------------------------------------------
An earlier revision of this file used a THREE-cell axis, and that single
choice opened two holes at once, both of the "test that cannot fail" class:

* on a 3-element axis `np.random.default_rng(1).permutation(3)` is `[0 1 2]`
  -- the IDENTITY -- so the two tests seeded with 1 compared an array to
  itself and a no-op `shuffle_space` survived them;
* HALF of all permutations of three elements are involutions (`[2 1 0]`,
  `[0 2 1]`, `[1 0 2]`), and the one test that recomputes the permutation and
  compares against it was seeded with 3, which is `[2 1 0]`. On an involution
  a gather (`blocks[:, p, :]`) and a scatter (`blocks[:, argsort(p), :]`) are
  the SAME array, so the direction of the permutation was unpinned and an
  inverse-direction mutant survived all 78 tests in this file.

Both are the root cause, not the symptom: the axis was too short for a random
seed to be trusted. The cell axis is now FIVE cells (only 26 of 120
permutations of five elements are involutions, and one in 120 is the
identity), and -- because "unlikely" is not a test -- every seeded shuffle
test asserts its own precondition inline, against the permutation the
implementation will draw. `SPACE_SEED` and `TIME_SEED` are pinned below with
both properties asserted directly.

Guard messages are matched on wording specific to THAT check -- never a bare
word (`dense`, `zero`, `cells`) that another layer could also produce.
"""
import datetime
import json
import math
import sys
import zipfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.backoff import FLOOR
from birdology.prior.controls import (
    CONTROLS,
    marginal_prior,
    separable_prior,
    shuffle_space,
    shuffle_time,
    uniform_prior,
)
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import PriorSpec, PriorTensor, load_prior, to_tensor

SPEC = PriorSpec(resolution=4, source="dof", variant="full")

# Real lower-case canonical H3 resolution-4 spellings, so nothing here can pass
# by accident on a placeholder that a case-folding or re-derivation defect
# would leave alone. NOT pre-sorted: `to_tensor` sorts the axis, and a fixture
# that hands them in sorted order cannot tell a control that preserves the axis
# from one that re-derives it.
#
# FIVE of them, not three: see the module docstring. On a three-element axis
# half of all permutations are involutions and one seed in six is the identity,
# which is what made two of the shuffle tests unable to fail.
CELLS = (
    "8418c4bffffffff",
    "8418c1bffffffff",
    "8418c33ffffffff",
    "8418c09ffffffff",
    "8418c47ffffffff",
)
SPECIES = (5229499, 2482593)

# The seeds the shuffle tests use. Both are asserted below against the two
# properties the tests actually depend on -- not the identity, and not its own
# inverse -- rather than being trusted because they looked fine once.
SPACE_SEED = 0
TIME_SEED = 1

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

    Every value is also many orders of magnitude above `FLOOR` (1e-06), which
    is what makes the sparse fixture's floor mass a genuinely small share of
    the total rather than the whole of it.
    """
    base = 0.01 + 0.02 * (cell_index + 1) ** 2 + 0.0001 * week * week
    return base * (_MASS_RATIO if species_index else 1.0)


def _to_tensor(rows):
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


def _rows():
    """A fully stored grid: every species x cell x week."""
    return [
        (SPECIES[si], cell, week, _p(ci, week, si))
        for si in range(len(SPECIES))
        for ci, cell in enumerate(CELLS)
        for week in range(1, WEEKS_PER_YEAR + 1)
    ]


def _tensor():
    return _to_tensor(_rows())


# One stored week per (species, cell), at a DIFFERENT week in each, so no two
# cell blocks and no two species rows are the same array. 10 of 2 x 5 x 52 =
# 520 contexts.
_OBSERVED_WEEKS = ((3, 17, 44, 26, 8), (9, 28, 51, 35, 14))


def _sparse_rows():
    return [
        (SPECIES[si], CELLS[ci], _OBSERVED_WEEKS[si][ci], _p(ci, _OBSERVED_WEEKS[si][ci], si))
        for si in range(len(SPECIES))
        for ci in range(len(CELLS))
    ]


def _sparse_tensor():
    """A REALISTICALLY sparse prior: 10 stored contexts of 520 (1.92% fill).

    Not a degenerate edge case -- this is the shape of the artifact. Every
    other context is on both axes and reads `FLOOR` through `tensor.py`'s
    FLOOR-for-missing rule, so the grid is complete; it is merely mostly
    "nobody looked".
    """
    return _to_tensor(_sparse_rows())


# ---------------------------------------------------------------------------
# The fixture's own preconditions. Every assertion below rests on these, and a
# fixture that stopped satisfying one would silently weaken its tests rather
# than fail. Asserted directly, never through a proxy.
# ---------------------------------------------------------------------------


def test_the_fixture_is_a_fully_stored_grid():
    t = _tensor()
    n_rows, n_cols = t.values.get_shape()
    assert n_cols == len(CELLS) * WEEKS_PER_YEAR
    assert t.values.nnz == n_rows * n_cols
    # Fully stored AND no stored value reads as missing, so `dense()` and
    # `toarray()` coincide here -- which is exactly why the SPARSE fixture is
    # needed to tell a control that reads through `dense()` from one that does
    # not.
    assert np.allclose(t.dense(), t.values.toarray())


def test_the_fixture_values_are_probabilities():
    """`_validate_p` requires (0, 1]; a coefficient change that pushed the
    fixture above one would make every test below fail on the FIXTURE rather
    than on the behaviour it is meant to pin."""
    dense = _tensor().dense()
    assert dense.min() > 0.0
    assert dense.max() <= 1.0


def test_the_fixture_gives_the_two_species_different_total_mass():
    """T9-R1: equal per-species mass makes the marginal control unobservable."""
    dense = _tensor().dense()
    assert dense.shape[0] == 2
    assert not np.isclose(dense[0].sum(), dense[1].sum())


def test_the_fixture_distinguishes_a_mean_from_a_median_or_a_max():
    """A fixture where mean == median would not pin the mass-preserving choice."""
    dense = _tensor().dense()
    for row in dense:
        assert not np.isclose(row.mean(), np.median(row))
        assert not np.isclose(row.mean(), row.max())
    assert not np.isclose(dense.mean(), np.median(dense))
    assert not np.isclose(dense.mean(), dense.max())


def test_the_fixture_distinguishes_a_global_mean_from_a_per_species_mean():
    """Otherwise `uniform_prior` and `marginal_prior` would be the same function."""
    dense = _tensor().dense()
    for row in dense:
        assert not np.isclose(row.mean(), dense.mean())


def test_the_fixture_distinguishes_a_mean_over_contexts_from_a_per_cell_mean():
    """Found the hard way: with a LINEAR cell term the middle cell's mean is
    exactly the mean over all cells, and `marginal_prior` taking a mean over one
    cell's block instead of over every context was then unobservable. The cell
    axis is sorted by `to_tensor`, so this must hold for EVERY block, not just
    the one the fixture happens to write first."""
    dense = _tensor().dense()
    blocks = dense.reshape(dense.shape[0], len(CELLS), WEEKS_PER_YEAR)
    for c in range(len(CELLS)):
        assert not np.allclose(dense.mean(axis=1), blocks[:, c, :].mean(axis=1))


def test_the_fixture_makes_the_uniform_fill_differ_from_one_over_n_species():
    """The T9-R2 defect: `1 / n_species` must be a DIFFERENT number here."""
    dense = _tensor().dense()
    assert not np.isclose(dense.mean(), 1.0 / dense.shape[0])


def test_the_fixture_gives_every_cell_a_distinct_weekly_profile():
    """T9-R3: identical cell blocks make a space permutation invisible."""
    blocks = _tensor().dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for s in range(len(SPECIES)):
        for i in range(len(CELLS)):
            for j in range(i + 1, len(CELLS)):
                assert not np.allclose(blocks[s, i], blocks[s, j])


def test_the_fixture_gives_every_week_slot_a_distinct_value():
    """T9-R3: repeated weekly values would let a real permutation look like a no-op."""
    blocks = _tensor().dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for s in range(len(SPECIES)):
        for c in range(len(CELLS)):
            assert len(set(blocks[s, c].tolist())) == WEEKS_PER_YEAR


# ---------------------------------------------------------------------------
# The SPARSE fixture's preconditions. These decide what the sparse tests can
# see at all, and one of them (`..._floor_does_not_dominate...`) is the
# evidence for the judgement that the floor mass participates.
# ---------------------------------------------------------------------------


def test_the_sparse_fixture_is_realistically_sparse():
    """Single-digit percent fill, which is what a national prior looks like.

    Pinned as a NUMBER rather than merely "not dense": a fixture that drifted
    to 90% fill would still be "sparse" and would stop distinguishing the dense
    denominator from the stored-entry one.
    """
    t = _sparse_tensor()
    assert t.values.nnz == 10
    assert t.dense_capacity == len(SPECIES) * len(CELLS) * WEEKS_PER_YEAR == 520
    assert 0.0 < t.values.nnz / t.dense_capacity < 0.05


def test_the_sparse_fixture_has_no_structural_zeros_to_speak_of():
    """Every context on the axes carries a value; most of them carry the floor.

    This is the reversal of the premise the earlier dense-grid refusal rested
    on, asserted directly rather than assumed from a docstring.
    """
    t = _sparse_tensor()
    dense = t.dense()
    assert dense.min() == FLOOR
    assert not (dense == 0.0).any()
    assert int((dense == FLOOR).sum()) == t.dense_capacity - t.values.nnz
    # And `lookup` agrees with the grid, so the controls and the sweep read the
    # same prior.
    assert t.lookup(SPECIES[0], CELLS[0], 1) == FLOOR


def test_the_sparse_fixture_separates_dense_from_toarray():
    """The whole point of this fixture: on the dense one these are equal, so
    only here can a locally re-derived grid be told from `t.dense()`."""
    t = _sparse_tensor()
    assert not np.allclose(t.dense(), t.values.toarray())
    assert t.dense().sum() > t.values.sum()


def test_the_sparse_fixture_floor_does_not_dominate_the_uniform_fill():
    """THE judgement, pinned as a number rather than argued in a docstring.

    Including the floor mass in `uniform` is only honest if the control stays
    DATA-DERIVED. `FLOOR` is 1e-06 and the observed values are of order 1e-01,
    so even at 1.92% fill the floor contributes a ~1e-04 share of total mass
    and the uniform constant lands three orders of magnitude ABOVE the floor.
    A fixture (or a `FLOOR`) that broke this would turn `uniform` into a flat
    1e-06 and make the control degenerate -- and every mass test would still
    pass, because a degenerate control preserves mass perfectly.
    """
    t = _sparse_tensor()
    dense = t.dense()
    floor_mass = (t.dense_capacity - t.values.nnz) * FLOOR
    assert floor_mass / dense.sum() < 1e-3
    assert dense.mean() > 1000 * FLOOR


def test_the_sparse_fixture_distinguishes_the_dense_denominator_from_nnz():
    """A mean over the stored entries only must be a DIFFERENT number here, or
    the wrong denominator could not be pinned as wrong."""
    t = _sparse_tensor()
    dense = t.dense()
    stored_mean = t.values.sum() / t.values.nnz
    assert not np.isclose(dense.mean(), stored_mean)
    # ~52x, the inflation factor a nnz denominator would introduce.
    assert stored_mean / dense.mean() > 10


def test_the_sparse_fixture_gives_the_two_species_different_total_mass():
    """T9-R1 on the sparse fixture too: otherwise `marginal` is unobservable
    exactly where it matters most."""
    dense = _sparse_tensor().dense()
    assert not np.isclose(dense[0].sum(), dense[1].sum())
    for row in dense:
        assert not np.isclose(row.mean(), dense.mean())


def test_the_sparse_fixture_distinguishes_a_per_cell_mean_from_a_mean_over_contexts():
    dense = _sparse_tensor().dense()
    blocks = dense.reshape(dense.shape[0], len(CELLS), WEEKS_PER_YEAR)
    for c in range(len(CELLS)):
        assert not np.allclose(dense.mean(axis=1), blocks[:, c, :].mean(axis=1))


def test_the_sparse_fixture_gives_every_cell_a_distinct_weekly_profile():
    """T9-R3 observability on the sparse fixture: each species stores a
    different week in each cell, so no two cell blocks are the same array and a
    space permutation is visible."""
    blocks = _sparse_tensor().dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for s in range(len(SPECIES)):
        for i in range(len(CELLS)):
            for j in range(i + 1, len(CELLS)):
                assert not np.allclose(blocks[s, i], blocks[s, j])


# ---------------------------------------------------------------------------
# The SEEDS' own preconditions. An earlier revision trusted them and two tests
# silently became tautologies: seed 1 on a 3-cell axis is the IDENTITY, and
# seed 3 on a 3-cell axis is its OWN INVERSE. Asserted here as a fixture
# property AND again inline in every test that depends on one, because a
# shared precondition that drifts weakens every test at once while this file
# stays green.
# ---------------------------------------------------------------------------


def test_the_cell_axis_is_long_enough_for_a_seed_to_be_worth_drawing():
    """On three cells, half of all permutations are involutions and one seed in
    six is the identity. Five cells is the root-cause fix; the inline
    preconditions are the belt to its braces."""
    assert len(CELLS) >= 5


def test_the_space_seed_is_not_the_identity_permutation():
    """A no-op `shuffle_space` reports the FULL prior's performance as the
    spatial null, and every multiset and mass assertion still passes."""
    perm = np.random.default_rng(SPACE_SEED).permutation(len(CELLS))
    assert not np.array_equal(perm, np.arange(len(CELLS)))


def test_the_space_seed_is_not_its_own_inverse():
    """On an involution a gather `blocks[:, p, :]` and a scatter
    `blocks[:, argsort(p), :]` are the SAME array, so no test that recomputes
    the permutation can pin which direction the implementation applies."""
    perm = np.random.default_rng(SPACE_SEED).permutation(len(CELLS))
    assert not np.array_equal(perm, np.argsort(perm))


def test_the_time_seed_is_not_the_identity_permutation():
    """A 52-element axis makes the identity vanishingly unlikely -- which is a
    reason to expect this to hold, not a reason to leave it unasserted."""
    perm = np.random.default_rng(TIME_SEED).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(perm, np.arange(WEEKS_PER_YEAR))


def test_the_time_seed_is_not_its_own_inverse():
    """The time twin of the direction precondition. The week axis has never
    had the space axis's problem, but the axis length is not the guarantee --
    the assertion is."""
    perm = np.random.default_rng(TIME_SEED).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(perm, np.argsort(perm))


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

    The quantity preserved is `t.dense().sum()` -- the prior AS THE SWEEP READS
    IT, floor mass included -- not `t.values.sum()`.
    """
    t = _tensor()
    u = uniform_prior(t)
    assert u.dense().sum() == pytest.approx(t.dense().sum())


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
    dense = t.dense()
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
    assert m.dense().sum() == pytest.approx(t.dense().sum())


def test_marginal_preserves_each_species_own_mass():
    """Stronger than total mass, and the property that makes this control the
    label-frequency null: a reduction that preserved the TOTAL while moving mass
    BETWEEN species would have changed the very signal under test."""
    t = _tensor()
    m = marginal_prior(t)
    assert m.dense().sum(axis=1) == pytest.approx(t.dense().sum(axis=1))


def test_marginal_does_NOT_renormalise_over_species():
    """The prior is never renormalised at any stage (backoff, smoothing,
    tensor). Dividing by the sum over species would also make the control's
    scale a function of how many species the tensor happens to hold."""
    t = _tensor()
    vec = marginal_prior(t).vector(CELLS[0], 20)
    assert not np.isclose(vec.sum(), 1.0)
    ratio = t.dense().mean(axis=1)
    ratio = ratio / ratio.sum()
    assert not np.allclose(vec, ratio)


def test_marginal_value_is_each_species_mean_over_contexts():
    """Pins the mean specifically: a sum, a max or a per-cell mean would each
    give a different number on this fixture."""
    t = _tensor()
    dense = t.dense()
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
    a = shuffle_space(t, seed=SPACE_SEED)
    b = shuffle_space(t, seed=SPACE_SEED)
    assert np.allclose(a.vector(CELLS[0], 20), b.vector(CELLS[0], 20))


def test_shuffle_space_preserves_the_value_multiset():
    """The precondition is asserted INLINE, not inherited from the seed being
    lucky: on the previous 3-cell axis this test's seed drew the IDENTITY, so
    it compared an array to itself and a no-op `shuffle_space` survived it."""
    perm = np.random.default_rng(SPACE_SEED).permutation(len(CELLS))
    assert not np.array_equal(
        perm, np.arange(len(CELLS))
    ), "the space seed must not be the identity, or this compares an array to itself"
    t = _tensor()
    s = shuffle_space(t, seed=SPACE_SEED)
    assert sorted(t.dense().ravel()) == pytest.approx(sorted(s.dense().ravel()))
    # And the array really did change, so the multiset equality above is a
    # statement about a permutation rather than about a no-op.
    assert not np.allclose(s.dense(), t.dense())


def test_shuffle_space_actually_moves_a_cell():
    """T9-R3: the multiset test above is satisfied by a NO-OP permutation.

    The seed is not trusted -- the precondition (this seed's permutation of the
    cell axis is not the identity) is asserted here, so a seed change cannot
    silently turn this into a test of nothing.
    """
    n_cells = len(CELLS)
    perm = np.random.default_rng(SPACE_SEED).permutation(n_cells)
    assert not np.array_equal(
        perm, np.arange(n_cells)
    ), "the space seed must not be the identity"
    t = _tensor()
    s = shuffle_space(t, seed=SPACE_SEED)
    moved = [
        c
        for i, c in enumerate(t.cells)
        if not np.allclose(s.vector(c, 20), t.vector(c, 20))
    ]
    assert moved, "shuffle_space left every cell's values where they were"


def test_shuffle_space_relocates_whole_cells_not_individual_values():
    """The construction T9-R3 names: one permutation of the cell axis, shared
    across every species, so each cell's weekly profile arrives intact.

    This is the ONLY test that recomputes the permutation and compares against
    it, so it is the only place the DIRECTION of the indexing is pinned -- and
    a direction is only pinned if the permutation is not its own inverse. On
    the previous 3-cell axis this test's seed drew `[2 1 0]`, an involution,
    and a scatter-instead-of-gather mutant (`np.argsort(...)`) survived the
    ENTIRE file. Both preconditions are therefore asserted inline.
    """
    n_cells = len(CELLS)
    perm = np.random.default_rng(SPACE_SEED).permutation(n_cells)
    assert not np.array_equal(
        perm, np.arange(n_cells)
    ), "the space seed must not be the identity"
    assert not np.array_equal(perm, np.argsort(perm)), (
        "the space seed must not be its own inverse, or gather and scatter are "
        "the same array here and the direction is unpinned"
    )
    t = _tensor()
    s = shuffle_space(t, seed=SPACE_SEED)
    before = t.dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    after = s.dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    assert after == pytest.approx(before[:, perm, :])
    # The mirror assertion, which is what the involution used to hide: the
    # INVERSE indexing must be a DIFFERENT array. Without this, "after equals
    # the gather" is compatible with the implementation scattering.
    assert not np.allclose(after, before[:, np.argsort(perm), :])


def test_shuffle_space_keeps_each_week_slot_in_its_own_week():
    """Space only. A value that also changed week would make a failed
    shuffle_space control unattributable to space."""
    t = _tensor()
    s = shuffle_space(t, seed=SPACE_SEED)
    before = t.dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    after = s.dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for w in range(WEEKS_PER_YEAR):
        assert sorted(after[:, :, w].ravel()) == pytest.approx(
            sorted(before[:, :, w].ravel())
        )


def test_shuffle_space_preserves_total_mass():
    """Inline precondition for the same reason as the multiset twin: on the
    identity this compares a sum to itself."""
    perm = np.random.default_rng(SPACE_SEED).permutation(len(CELLS))
    assert not np.array_equal(
        perm, np.arange(len(CELLS))
    ), "the space seed must not be the identity, or this compares a sum to itself"
    t = _tensor()
    s = shuffle_space(t, seed=SPACE_SEED)
    assert not np.allclose(s.dense(), t.dense())
    assert s.dense().sum() == pytest.approx(t.dense().sum())


def test_the_shuffles_store_every_context_so_a_grid_bypass_cannot_hide():
    """The single-definition rule made falsifiable on the shuffles.

    Flooring is permutation-invariant, so for the two shuffles reading
    `t.values.toarray()` instead of `t.dense()` is byte-identical ON THE DENSE
    VALUES -- and such a bypass would therefore be caught only by the
    column-count guard it also skips, never by the flooring it loses. That
    makes the single-definition rule unfalsifiable here, and it stops being a
    theorem the moment a shuffle stops being a pure permutation.

    It is NOT identical in what gets STORED: `_respec` re-sparsifies, so a
    bypassed shuffle keeps only the 10 originally-stored entries while the real
    one keeps all 520. One assertion, and the flooring becomes load-bearing for
    both shuffles.
    """
    t = _sparse_tensor()
    for out in (shuffle_space(t, seed=SPACE_SEED), shuffle_time(t, seed=TIME_SEED)):
        assert out.values.nnz == t.dense_capacity, out.spec.variant


def test_shuffle_space_variant_is_labelled():
    assert shuffle_space(_tensor(), seed=SPACE_SEED).spec.variant == "shuffle_space"


def test_shuffle_space_uses_its_seed():
    """Two seeds whose cell permutations differ must give different tensors,
    or the sweep's replicates would all be the same control."""
    t = _tensor()
    a = np.random.default_rng(2).permutation(len(CELLS))
    b = np.random.default_rng(3).permutation(len(CELLS))
    assert not np.array_equal(a, b), "seeds 2 and 3 must differ on the cell axis"
    assert not np.allclose(
        shuffle_space(t, seed=2).dense(), shuffle_space(t, seed=3).dense()
    )


# ---------------------------------------------------------------------------
# shuffle_time: real values, wrong weeks
# ---------------------------------------------------------------------------


def test_shuffle_time_preserves_the_value_multiset():
    """Inline precondition, exactly as for the space twin. A 52-element axis
    makes the identity vanishingly unlikely -- which is not the same as
    impossible, and an unasserted precondition is what made the space twin a
    tautology."""
    perm = np.random.default_rng(TIME_SEED).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(
        perm, np.arange(WEEKS_PER_YEAR)
    ), "the time seed must not be the identity, or this compares an array to itself"
    t = _tensor()
    s = shuffle_time(t, seed=TIME_SEED)
    assert sorted(t.dense().ravel()) == pytest.approx(sorted(s.dense().ravel()))
    assert not np.allclose(s.dense(), t.dense())


def test_shuffle_time_actually_moves_a_week():
    """T9-R3, the time mirror: the multiset test passes on a no-op permutation."""
    perm = np.random.default_rng(TIME_SEED).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(
        perm, np.arange(WEEKS_PER_YEAR)
    ), "the time seed must not be the identity"
    t = _tensor()
    s = shuffle_time(t, seed=TIME_SEED)
    moved = [
        w
        for w in range(1, WEEKS_PER_YEAR + 1)
        if not np.allclose(s.vector(CELLS[0], w), t.vector(CELLS[0], w))
    ]
    assert moved, "shuffle_time left every week's values where they were"


def test_shuffle_time_permutes_week_slots_with_one_shared_permutation():
    """The time mirror of the space direction test, with both preconditions
    asserted inline rather than inferred from the axis being 52 long."""
    perm = np.random.default_rng(TIME_SEED).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(
        perm, np.arange(WEEKS_PER_YEAR)
    ), "the time seed must not be the identity"
    assert not np.array_equal(perm, np.argsort(perm)), (
        "the time seed must not be its own inverse, or gather and scatter are "
        "the same array here and the direction is unpinned"
    )
    t = _tensor()
    s = shuffle_time(t, seed=TIME_SEED)
    before = t.dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    after = s.dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    assert after == pytest.approx(before[:, :, perm])
    assert not np.allclose(after, before[:, :, np.argsort(perm)])


def test_shuffle_time_keeps_every_value_inside_its_own_cell():
    """Time only. A value that crossed into another cell would make a failed
    shuffle_time control unattributable to time -- and it is exactly what a
    reshape against the wrong stride would do."""
    t = _tensor()
    s = shuffle_time(t, seed=TIME_SEED)
    before = t.dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    after = s.dense().reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for c in range(len(CELLS)):
        assert sorted(after[:, c, :].ravel()) == pytest.approx(
            sorted(before[:, c, :].ravel())
        )


def test_shuffle_time_preserves_total_mass():
    perm = np.random.default_rng(TIME_SEED).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(
        perm, np.arange(WEEKS_PER_YEAR)
    ), "the time seed must not be the identity, or this compares a sum to itself"
    t = _tensor()
    s = shuffle_time(t, seed=TIME_SEED)
    assert not np.allclose(s.dense(), t.dense())
    assert s.dense().sum() == pytest.approx(t.dense().sum())


def test_shuffle_time_variant_is_labelled():
    assert shuffle_time(_tensor(), seed=TIME_SEED).spec.variant == "shuffle_time"


def test_shuffle_time_is_deterministic_in_its_seed():
    t = _tensor()
    assert np.allclose(shuffle_time(t, seed=5).dense(), shuffle_time(t, seed=5).dense())


def test_shuffle_time_uses_its_seed():
    t = _tensor()
    a = np.random.default_rng(1).permutation(WEEKS_PER_YEAR)
    b = np.random.default_rng(2).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(a, b), "seeds 1 and 2 must differ on the week axis"
    assert not np.allclose(
        shuffle_time(t, seed=1).dense(), shuffle_time(t, seed=2).dense()
    )


def test_shuffle_space_and_shuffle_time_are_different_controls():
    """Both reshape the same blocks; a copy-paste of the wrong axis would make
    two of the four controls the same experiment."""
    t = _tensor()
    assert not np.allclose(
        shuffle_space(t, seed=3).dense(), shuffle_time(t, seed=3).dense()
    )


# ---------------------------------------------------------------------------
# THE SPARSE PRIOR. This section exists because a previous revision made all
# four controls RAISE on every realistic input while this whole file stayed
# green: the fixtures were dense and the defect was invisible to them.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", list(CONTROLS))
def test_every_control_runs_on_a_realistically_sparse_prior(name):
    """The regression. At 1.92% fill every control must COMPUTE, not refuse.

    A control that refuses here takes the paper's entire credibility section
    down before it produces a single number, and no dense fixture can see it.
    """
    t = _sparse_tensor()
    out = CONTROLS[name](t)
    assert isinstance(out, PriorTensor)
    assert out.values.get_shape() == t.values.get_shape()
    assert out.spec.variant == name
    assert np.isfinite(out.dense()).all()


@pytest.mark.parametrize("name", list(CONTROLS))
def test_every_control_preserves_the_sparse_priors_total_mass(name):
    """T9-R2 re-derived under FLOOR-for-missing: the mass to preserve is the
    mass of the prior AS READ, `t.dense().sum()`, which includes the floor mass
    of every unsurveyed context. On this fixture that is 98% of the grid, and a
    control built against `t.values.sum()` instead would sit below the full
    prior by exactly that amount."""
    t = _sparse_tensor()
    assert CONTROLS[name](t).dense().sum() == pytest.approx(t.dense().sum())


def test_uniform_on_a_sparse_prior_fills_with_the_mean_over_the_WHOLE_grid():
    """Not the mean over the stored entries. The dense denominator is now
    simply the count of contexts that carry a value, because every context on
    the axes does."""
    t = _sparse_tensor()
    dense = t.dense()
    fill = uniform_prior(t).vector(CELLS[1], 33)[0]
    assert fill == pytest.approx(dense.mean())
    assert fill == pytest.approx(dense.sum() / t.dense_capacity)


def test_uniform_on_a_sparse_prior_is_not_the_mean_over_stored_entries():
    """The nnz denominator, pinned as WRONG rather than merely unused: it would
    inflate the control by dense_capacity/nnz -- ~52x here, and far more at
    national scale -- and the sweep would report that as an enormous
    spatiotemporal effect."""
    t = _sparse_tensor()
    fill = uniform_prior(t).vector(CELLS[0], 1)[0]
    assert not np.isclose(fill, t.values.sum() / t.values.nnz)
    assert not np.isclose(fill, t.dense().sum() / t.values.nnz)


def test_uniform_on_a_sparse_prior_is_not_degenerately_the_floor():
    """The judgement, asserted on the OUTPUT rather than on the fixture.

    Including the floor mass is only honest while the control stays
    data-derived. If `uniform` ever collapsed to ~FLOOR the null would become
    "the prior is switched off" rather than "the prior's mass with no context
    information", and every mass test would still pass.
    """
    t = _sparse_tensor()
    fill = uniform_prior(t).vector(CELLS[0], 1)[0]
    assert fill > 1000 * FLOOR


def test_marginal_on_a_sparse_prior_preserves_each_species_own_mass():
    """The per-species property is what makes this the label-frequency null,
    and it is the one a stored-entries-only mean breaks: each species would be
    rescaled by its OWN fill, so the control would report a label frequency
    that is really a survey-effort frequency."""
    t = _sparse_tensor()
    m = marginal_prior(t)
    assert m.dense().sum(axis=1) == pytest.approx(t.dense().sum(axis=1))


def test_marginal_on_a_sparse_prior_still_varies_across_species():
    """The control must still carry label frequency at realistic sparsity, or
    it is not the null it claims to be."""
    t = _sparse_tensor()
    vec = marginal_prior(t).vector(CELLS[0], 20)
    assert not np.allclose(vec, vec[0])
    assert vec == pytest.approx(t.dense().mean(axis=1))


# ---------------------------------------------------------------------------
# marginal's CONTRAST, which is the only thing this control exists to carry.
# "Varies across species" is satisfied by a ratio of 1.0001. What the paper
# needs is that the RATIO between species survives, and the floor is a common
# ADDITIVE baseline in every row, so that ratio is squeezed toward 1 as fill
# drops. Measured here on a fixture whose observed ratio is exactly 4:1 by
# construction.
# ---------------------------------------------------------------------------

# Both species observe the SAME cells at the SAME weeks, so their observed
# masses differ by exactly `_MASS_RATIO` and the true contrast is a stated
# constant rather than something recomputed from the object under test.
_CONTRAST_WEEKS = (3, 17, 44, 26, 8)
_CONTRAST_RATIO = 1.0 / _MASS_RATIO  # 4.0


def _contrast_tensor():
    """1.92% fill, and an observed per-species mass ratio of exactly 4:1."""
    return _to_tensor(
        [
            (SPECIES[si], CELLS[ci], _CONTRAST_WEEKS[ci], _p(ci, _CONTRAST_WEEKS[ci], si))
            for si in range(len(SPECIES))
            for ci in range(len(CELLS))
        ]
    )


def test_the_contrast_fixture_has_an_observed_mass_ratio_of_exactly_four():
    """The known quantity the contrast test measures against, asserted on the
    STORED values so it cannot be a restatement of what `marginal` emits."""
    t = _contrast_tensor()
    assert t.values.nnz == 10
    assert t.dense_capacity == 520
    observed = np.asarray(t.values.toarray()).sum(axis=1)
    assert observed.max() / observed.min() == pytest.approx(_CONTRAST_RATIO)


def test_marginal_reproduces_a_known_species_mass_ratio_on_a_sparse_prior():
    """I4. The label-frequency signal, measured rather than assumed.

    At 1.92% fill with observed values of order 1e-01, `marginal` emits
    3.997887 for a true 4:1 -- a 5.3e-04 relative loss. Pinned at 1% so the
    test fails on a real degradation rather than on float noise, and pinned
    from BELOW as well: a control emitting MORE contrast than the data holds
    would be manufacturing label frequency, which is the opposite defect and
    equally invisible to every mass assertion.
    """
    t = _contrast_tensor()
    emitted = marginal_prior(t).vector(CELLS[0], 20)
    ratio = float(emitted.max() / emitted.min())
    assert ratio == pytest.approx(_CONTRAST_RATIO, rel=0.01)
    assert 3.9 < ratio <= _CONTRAST_RATIO


def test_marginals_contrast_is_squeezed_toward_one_by_the_floor():
    """The decay direction, pinned so the docstring's claim is falsifiable.

    The floor is a COMMON additive baseline in every row, so the emitted ratio
    is strictly below the observed one and approaches 1 as `fill * value`
    approaches FLOOR. It is asymmetric -- the full prior's label signal is
    concentrated undiluted in the contexts it observed, while `marginal`
    smears each species' mass across all of them -- which is why it is a
    property worth measuring on the real artifact rather than a shared
    honest limitation.
    """
    t = _contrast_tensor()
    emitted = marginal_prior(t).vector(CELLS[0], 20)
    ratio = float(emitted.max() / emitted.min())
    assert ratio < _CONTRAST_RATIO, "the floor can only squeeze the ratio, never widen it"
    assert ratio > 1.0, "at this fill the signal must still be there at all"
    # And the squeeze is monotone in the floor's share. A prior whose observed
    # values are 1000x fainter sits 1000x closer to the floor and keeps only
    # 2.760 of the same 4:1 -- a 31% loss, against 0.05% here. Built and
    # measured rather than quoted from the docstring.
    faint = _to_tensor(
        [
            (
                SPECIES[si],
                CELLS[ci],
                _CONTRAST_WEEKS[ci],
                _p(ci, _CONTRAST_WEEKS[ci], si) / 1000.0,
            )
            for si in range(len(SPECIES))
            for ci in range(len(CELLS))
        ]
    )
    faint_emitted = marginal_prior(faint).vector(CELLS[0], 20)
    faint_ratio = float(faint_emitted.max() / faint_emitted.min())
    assert faint_ratio < ratio
    assert faint_ratio == pytest.approx(2.760, rel=0.01)


def test_the_shuffles_move_values_on_a_sparse_prior():
    """A shuffle over a sparse prior must actually relocate something. It also
    relocates the FLOOR -- "nobody looked here" is one of the prior's values,
    and a shuffle that held it in place would leak the survey-effort map, which
    is exactly the spatial/temporal information these controls remove."""
    t = _sparse_tensor()
    before = t.dense()
    assert not np.allclose(shuffle_space(t, seed=SPACE_SEED).dense(), before)
    assert not np.allclose(shuffle_time(t, seed=TIME_SEED).dense(), before)


def test_the_controls_read_an_unsurveyed_context_as_the_floor_not_zero():
    """`_control_grid` must return `t.dense()`, not a local `toarray()`.

    A locally re-derived grid reads every unsurveyed context as 0.0 while the
    sweep reads it as FLOOR, so the shuffles would carry hard zeros the full
    prior never has -- and a hard zero VETOES a correct detection, which is the
    precise failure the floor exists to prevent.
    """
    t = _sparse_tensor()
    for name in CONTROLS:
        out = CONTROLS[name](t)
        assert out.dense().min() > 0.0, name
    # The shuffles carry the floor through as a real value rather than a hole.
    assert shuffle_space(t, seed=SPACE_SEED).dense().min() == pytest.approx(FLOOR)


# ---------------------------------------------------------------------------
# I1 (final gate): A SHUFFLE MAY NEVER BE THE PRIOR IT IS THE NULL FOR.
#
# `np.random.default_rng(0).permutation(n)` is the IDENTITY at n <= 2, and the
# gate built a single-cell prior end to end and got a
# `prior_dof_r4_shuffle_space.npz` with the FULL PRIOR'S SHA256 -- correctly
# packed, correctly provenanced, labelled `shuffle_space`. Nothing raised.
#
# This is the THIRD time a permutation draw has produced a control here that
# cannot fail. The first two were fixed by widening THIS FILE's cell axis,
# which left shipped code with no guard, so the defect moved from the fixture
# into the artifact. The guard is now in `src/` and these tests are what keep
# it honest -- including the two things it must NOT do: fire on `uniform` /
# `marginal` / `separable`, which legitimately coincide with a degenerate
# prior, and reject a merely WEAK permutation.
# ---------------------------------------------------------------------------

#: Ten cells, because a fixed-point RATE threshold is only testable where the
#: drawn permutation HAS fixed points: at seed 0, n=10 leaves 3 of 10 fixed
#: (and n=5, the main fixture, leaves none). Real lower-case canonical H3
#: res-4 spellings, like `CELLS`.
CELLS_TEN = (
    "8418c4bffffffff", "8418c1bffffffff", "8418c33ffffffff", "8418c09ffffffff",
    "8418c47ffffffff", "8418c53ffffffff", "8418c0bffffffff", "8418c17ffffffff",
    "8418c37ffffffff", "8418c43ffffffff",
)


def _degenerate_tensor(cells, p_of):
    """A fully stored grid over *cells* whose value is `p_of(cell_i, week)`."""
    return _to_tensor(
        [
            (SPECIES[si], cell, week, p_of(ci, week) * (_MASS_RATIO if si else 1.0))
            for si in range(len(SPECIES))
            for ci, cell in enumerate(cells)
            for week in range(1, WEEKS_PER_YEAR + 1)
        ]
    )


def _flat_in_cell_tensor(cells=CELLS):
    """Every cell block IDENTICAL; the weekly profile is real.

    A cell permutation maps this grid to itself for ANY permutation, identity
    or not -- which is why the guard is on the emitted grid and not on the
    permutation.
    """
    return _degenerate_tensor(cells, lambda ci, week: 0.02 + 0.0001 * week * week)


def _flat_in_week_tensor(cells=CELLS):
    """Every week slot IDENTICAL; the spatial profile is real. The mirror."""
    return _degenerate_tensor(cells, lambda ci, week: 0.01 + 0.02 * (ci + 1) ** 2)


def _constant_tensor(cells=CELLS):
    """One value per species, everywhere. Degenerate on BOTH axes at once.

    Per-species rather than globally constant, so it is `marginal`'s own fixed
    point and still varies across species -- a globally flat grid would make
    `marginal` coincide only because it had collapsed to `uniform`.
    """
    return _degenerate_tensor(cells, lambda ci, week: 0.2)


def _globally_constant_tensor(cells=CELLS):
    """ONE value across species as well: `uniform`'s own fixed point."""
    return _to_tensor(
        [
            (SPECIES[si], cell, week, 0.2)
            for si in range(len(SPECIES))
            for cell in cells
            for week in range(1, WEEKS_PER_YEAR + 1)
        ]
    )


def _separable_tensor(cells=CELLS):
    """A MULTIPLICATIVELY separable prior: `separable`'s own fixed point.

    `cellmean * weekmean / grandmean` reproduces a block of the form
    `c_term * w_term` exactly, so this grid has real space and real time
    structure and NO interaction between them.
    """
    return _degenerate_tensor(
        cells, lambda ci, week: (0.3 + 0.1 * ci) * (0.5 + 0.005 * week)
    )


#: The prior each non-shuffle control legitimately COINCIDES with. One per
#: control, because a single globally-flat fixture would let `marginal` and
#: `separable` pass by having collapsed to `uniform` rather than by being
#: their own fixed points.
_OWN_FIXED_POINT = {
    "uniform": _globally_constant_tensor,
    "marginal": _constant_tensor,
    "separable": _separable_tensor,
}


def test_the_degenerate_fixtures_really_are_degenerate_the_way_they_claim():
    """The precondition for everything below. A fixture that accidentally
    carried structure would make every refusal test below pass for the wrong
    reason -- and "the fixture could not show the effect" is the trap this
    branch has hit four times."""
    flat_cell = _flat_in_cell_tensor().dense()
    blocks = flat_cell.reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for ci in range(1, len(CELLS)):
        assert np.array_equal(blocks[:, ci, :], blocks[:, 0, :])
    # ...but it DOES vary over weeks, so `shuffle_time` has work to do on it.
    assert blocks[0, 0, :].std() > 0.0

    flat_week = _flat_in_week_tensor().dense()
    wblocks = flat_week.reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    for wi in range(1, WEEKS_PER_YEAR):
        assert np.array_equal(wblocks[:, :, wi], wblocks[:, :, 0])
    assert wblocks[0, :, 0].std() > 0.0

    const = _constant_tensor().dense()
    for row in const:
        assert np.array_equal(row, np.full(row.shape, row[0]))
    # ...but the two species still differ, so `marginal` coincides with it by
    # being its own fixed point rather than by collapsing to `uniform`.
    assert const[0, 0] != const[1, 0]

    flat = _globally_constant_tensor().dense()
    assert np.array_equal(flat, np.full(flat.shape, flat[0, 0]))

    # `separable`'s fixed point must have REAL space and REAL time structure
    # and no interaction between them, or the coincidence proves nothing.
    sep = _separable_tensor().dense()
    sblocks = sep.reshape(len(SPECIES), len(CELLS), WEEKS_PER_YEAR)
    assert sblocks[0].mean(axis=1).std() > 0.0, "no spatial structure"
    assert sblocks[0].mean(axis=0).std() > 0.0, "no seasonal structure"
    outer = np.outer(sblocks[0].mean(axis=1), sblocks[0].mean(axis=0))
    assert sblocks[0] == pytest.approx(outer / sblocks[0].mean())


@pytest.mark.parametrize("n_cells", [1, 2])
def test_shuffle_space_REFUSES_to_emit_the_prior_on_a_degenerate_cell_axis(n_cells):
    """The gate's finding, at both lengths where seed 0 draws the identity.

    At n=1 the identity is the ONLY permutation there is, so no seed can
    rescue it; at n=2 seed 0 happens to draw it. Both produce a control whose
    bytes are the prior's, which a sweep reads as "the spatial effect does not
    survive its control".
    """
    t = _to_tensor(
        [
            (SPECIES[si], cell, week, _p(ci, week, si))
            for si in range(len(SPECIES))
            for ci, cell in enumerate(CELLS[:n_cells])
            for week in range(1, WEEKS_PER_YEAR + 1)
        ]
    )
    perm = np.random.default_rng(SPACE_SEED).permutation(n_cells)
    assert np.array_equal(perm, np.arange(n_cells)), (
        "precondition: this test is about the regime where the draw IS the "
        "identity, and it must be asserted rather than assumed"
    )
    with pytest.raises(ValueError, match="byte-identical to the full prior"):
        shuffle_space(t, seed=SPACE_SEED)


def test_the_unmoved_shuffle_refusal_names_the_axis_its_LENGTH_and_the_SEED():
    """An operator's fix differs by cause -- a coarser build, a different
    seed, or a different control -- so the message has to say which axis
    collapsed, how long it was, and which seed drew the permutation. A bare
    "control is degenerate" would leave them guessing."""
    t = _flat_in_cell_tensor(CELLS[:1])
    with pytest.raises(ValueError) as excinfo:
        shuffle_space(t, seed=3)
    message = str(excinfo.value)
    assert "shuffle_space" in message
    assert "cell axis of length 1" in message
    assert "seed 3" in message
    # Both CAUSES are named, because the code cannot tell them apart and the
    # operator's remedy depends on which it is.
    assert "displaces no cell" in message
    assert "same values" in message


def test_shuffle_space_is_refused_on_an_UNMOVED_GRID_even_at_a_GOOD_permutation():
    """THE reason the guard is on the grid and not on the permutation.

    A check spelled `if np.array_equal(perm, np.arange(n))` passes here -- the
    draw displaces every one of the five cells -- and still ships a file
    byte-identical to the prior, because every cell block holds the same
    values. The grid-level check is strictly stronger and this fixture is what
    separates the two spellings.
    """
    perm = np.random.default_rng(SPACE_SEED).permutation(len(CELLS))
    assert not np.array_equal(perm, np.arange(len(CELLS)))
    assert int((perm == np.arange(len(CELLS))).sum()) == 0, (
        "precondition: the permutation must be entirely non-degenerate, so "
        "ONLY the grid-level check can refuse this"
    )
    with pytest.raises(ValueError, match="byte-identical to the full prior"):
        shuffle_space(_flat_in_cell_tensor(), seed=SPACE_SEED)


def test_shuffle_time_is_refused_on_a_PRIOR_WITH_NO_SEASON_at_all():
    """The time mirror, and the only route by which `shuffle_time` can
    degenerate: the week axis is a schema constant, so no input shrinks it,
    but a prior that is flat across weeks in every cell is returned unchanged
    by every week permutation."""
    perm = np.random.default_rng(TIME_SEED).permutation(WEEKS_PER_YEAR)
    assert not np.array_equal(perm, np.arange(WEEKS_PER_YEAR))
    with pytest.raises(ValueError, match="byte-identical to the full prior"):
        shuffle_time(_flat_in_week_tensor(), seed=TIME_SEED)
    with pytest.raises(ValueError, match="week axis of length 52"):
        shuffle_time(_flat_in_week_tensor(), seed=TIME_SEED)


def test_the_WEEK_axis_length_is_a_SCHEMA_CONSTANT_not_a_lucky_number():
    """Why `shuffle_time` is safer than `shuffle_space`, stated as a property
    rather than left as an accident of 52 being large.

    The asymmetry is not the size of the two axes. It is that `WEEKS_PER_YEAR`
    is fixed by the schema and no archive can shrink it, while the cell axis is
    a function of the data -- a single-region case study or a coarse resolution
    really does produce one or two cells. So the identity regime is
    UNREACHABLE on the week axis and reachable on the cell axis.
    """
    assert WEEKS_PER_YEAR == 52
    # The week permutation is drawn at a length that is the same for every
    # artifact this package can write, so it cannot enter the n <= 2 regime.
    assert WEEKS_PER_YEAR > 2
    # And the identity is astronomically unlikely there rather than merely
    # unlikely: 1/52! is ~8e-69. Checked over a wide seed sweep, because
    # "unlikely" is not an assertion.
    for seed in range(200):
        perm = np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)
        assert not np.array_equal(perm, np.arange(WEEKS_PER_YEAR)), seed


def test_a_permutation_with_FIXED_POINTS_is_deliberately_ACCEPTED():
    """The subtler half of I1, answered NO.

    At seed 0 a ten-cell axis leaves 3 of 10 cells fixed. That is not a
    defect: the grid still moved, so the control still ran the experiment. A
    rate threshold tight enough to reject it would reject ~8% of legitimate
    draws at EVERY axis length (see the Poisson test below), and a control
    drawn from fixed-point-free permutations is no longer a uniform random
    relabelling -- a different null, which is a subtler version of the very
    defect being fixed.
    """
    perm = np.random.default_rng(SPACE_SEED).permutation(len(CELLS_TEN))
    fixed = int((perm == np.arange(len(CELLS_TEN))).sum())
    assert fixed == 3, (
        f"precondition: this test is about a WEAK draw and the draw has "
        f"{fixed} fixed points; without any it pins nothing"
    )
    t = _degenerate_tensor(
        CELLS_TEN, lambda ci, week: 0.02 + 0.005 * ci + 0.0001 * week * week
    )
    dense = t.dense()
    blocks = dense.reshape(len(SPECIES), len(CELLS_TEN), WEEKS_PER_YEAR)
    assert len({tuple(b) for b in blocks[0]}) == len(CELLS_TEN), (
        "precondition: every cell block must be distinct, or a permutation "
        "that displaces seven of ten cells could still emit the same grid"
    )
    out = shuffle_space(t, seed=SPACE_SEED)
    assert out.spec.variant == "shuffle_space"
    # It moved, and it preserved mass -- the two things a control owes.
    assert not np.array_equal(out.dense(), dense)
    assert out.dense().sum() == pytest.approx(dense.sum())


def test_the_fixed_point_count_is_POISSON_ONE_so_a_RATE_THRESHOLD_pins_nothing():
    """Why there is no fixed-point rate check, as a measurement.

    The number of fixed points of a uniform random permutation is Poisson(1)
    for EVERY n -- mean 1, variance 1, and `P(F >= 3)` the same at n=10 as at
    n=897. So a rate threshold does not distinguish a degenerate axis from a
    healthy one; it just rejects a fixed fraction of perfectly good draws at
    any length, which is the arbitrary-constant trap this branch has hit
    before. Asserted as numbers so the next reader does not re-litigate it.
    """
    for n in (10, 27, 115):
        counts = np.array(
            [
                int((np.random.default_rng(s).permutation(n) == np.arange(n)).sum())
                for s in range(2000)
            ]
        )
        assert counts.mean() == pytest.approx(1.0, abs=0.1), n
        assert counts.var() == pytest.approx(1.0, abs=0.15), n
        # ~8% of draws have three or more fixed points AT EVERY n, so a
        # "reject 3 of 10" rule rejects 8% of valid draws on a 897-cell
        # national axis too.
        assert (counts >= 3).mean() == pytest.approx(0.080, abs=0.02), n
    # The one regime that IS structurally degenerate, and it is about length
    # rather than about a rate: at n <= 2 the identity has probability >= 1/2.
    assert np.array_equal(np.random.default_rng(0).permutation(1), np.arange(1))
    assert np.array_equal(np.random.default_rng(0).permutation(2), np.arange(2))


@pytest.mark.parametrize("name", list(_OWN_FIXED_POINT))
def test_the_unmoved_guard_does_NOT_MISFIRE_on_the_non_shuffle_controls(name):
    """The mirror obligation, and the reason the guard is not in `_respec`.

    `uniform` of an already-constant prior IS that prior, and that is the
    correct answer rather than a degeneracy: "the prior's mass with no
    information at all" genuinely equals a prior that carries no information.
    `marginal` of a per-species-constant prior and `separable` of a prior with
    no space x time interaction are the same statement one structure further
    along. Each coincidence MEANS something; a shuffle's does not.

    Each control gets ITS OWN fixed point rather than one shared flat grid, so
    `marginal` and `separable` cannot pass by having collapsed to `uniform`.
    """
    t = _OWN_FIXED_POINT[name]()
    out = CONTROLS[name](t)
    assert out.spec.variant == name
    assert np.allclose(out.dense(), t.dense()), (
        f"precondition: {name} must actually COINCIDE with this prior, or the "
        "test is not exercising the misfire it exists to rule out"
    )


def test_BOTH_shuffles_are_refused_on_the_prior_the_others_legitimately_match():
    """The same degenerate prior, from the shuffles' side: where `uniform`,
    `marginal` and `separable` correctly return it unchanged, a shuffle must
    refuse. That asymmetry is the whole design of the guard."""
    t = _constant_tensor()
    with pytest.raises(ValueError, match="byte-identical to the full prior"):
        shuffle_space(t, seed=SPACE_SEED)
    with pytest.raises(ValueError, match="byte-identical to the full prior"):
        shuffle_time(t, seed=TIME_SEED)


def test_an_ORDINARY_prior_is_not_refused_by_either_shuffle():
    """The guard must not be so strict it rejects the input the controls are
    defined on -- the C40/C60-shaped mirror, which takes the paper's
    credibility section down before it computes anything."""
    for fixture in (_tensor, _sparse_tensor):
        t = fixture()
        for seed in range(8):
            assert shuffle_space(t, seed=seed).spec.variant == "shuffle_space"
            assert shuffle_time(t, seed=seed).spec.variant == "shuffle_time"


def test_the_unmoved_shuffle_guard_RAISES_rather_than_warning_or_repairing():
    """The house rule: no silent repair, no warn-and-continue. A shuffle that
    re-drew until it moved would be sampling from a conditioned distribution
    and quietly reporting a different null; one that warned would leave the
    byte-identical file on disk."""
    t = _flat_in_cell_tensor(CELLS[:1])
    with pytest.raises(ValueError):
        shuffle_space(t, seed=SPACE_SEED)
    # Nothing was mutated on the way out: the input is still the full prior.
    assert t.spec.variant == "full"
    assert t.dense().sum() == pytest.approx(_flat_in_cell_tensor(CELLS[:1]).dense().sum())


# ---------------------------------------------------------------------------
# C1: a control of a control is refused. The consumed contract is a
# `PriorTensor` with `spec.variant == "full"`, and nothing used to enforce it:
# all four controls accepted a control as input and RELABELLED the output, so
# a `shuffle_space`-of-`uniform` landed in the results table under a label
# claiming it was a first-order spatial null. Mass-preserving, finite, in
# range, right shape, right axes -- the only evidence was the variant string
# this module then overwrote.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("outer", list(CONTROLS))
@pytest.mark.parametrize("inner", list(CONTROLS))
def test_a_control_of_a_control_is_refused(outer, inner):
    """All TWENTY-FIVE compositions, because each control is a different
    reduction and a guard that fired for four of them would still relabel the
    fifth."""
    control = CONTROLS[inner](_tensor())
    assert control.spec.variant == inner
    with pytest.raises(ValueError, match="a control of a control is meaningless"):
        CONTROLS[outer](control)


@pytest.mark.parametrize("inner", list(CONTROLS))
def test_the_control_of_a_control_message_names_the_variant_it_received(inner):
    """Naming the OUTER control alone would tell a caller which function
    raised and not which artifact they passed in -- and the whole failure mode
    is a sweep loop that lost track of which artifact it was holding."""
    control = CONTROLS[inner](_tensor())
    with pytest.raises(ValueError, match=f"already the '{inner}' control"):
        uniform_prior(control)


def test_the_full_prior_itself_is_not_refused():
    """The mirror of the guard: it must not be so strict it rejects the one
    input every control is defined on, on either fixture."""
    for fixture in (_tensor, _sparse_tensor):
        t = fixture()
        assert t.spec.variant == "full"
        for name, fn in CONTROLS.items():
            assert fn(t).spec.variant == name


def test_the_control_of_a_control_guard_runs_before_the_grid_is_materialised():
    """It is checked first, so the message that arrives names the contract the
    caller actually broke rather than a downstream reshape or numpy error."""
    control = uniform_prior(_tensor())
    control.cells = list(control.cells) + ["8418c53ffffffff"]
    with pytest.raises(ValueError, match="a control of a control is meaningless"):
        shuffle_space(control, seed=SPACE_SEED)


# ---------------------------------------------------------------------------
# What this module does NOT guard any more, pinned so it cannot silently
# return: the dense-grid refusal and the stored-zero refusal.
# ---------------------------------------------------------------------------


def test_a_stored_explicit_zero_reads_as_the_floor_rather_than_being_refused():
    """`tensor.py` owns this call and has already made it.

    `_floor_missing` maps an explicit stored 0.0 to FLOOR exactly as it maps an
    unstored context, so no reader -- including this module -- can tell the two
    apart. A refusal here would make the controls stricter than the
    serialisation boundary they read through, on a distinction that boundary
    has deliberately erased. `_validate_p` makes the call at write time.
    """
    t = _tensor()
    dense = t.values.toarray()
    # Built as COO so the zero is an EXPLICIT stored entry rather than a
    # structural one: `csr_matrix(dense)` would drop it.
    rows, cols = np.indices(dense.shape)
    data = dense.copy()
    data[0, 3] = 0.0
    t.values = sparse.coo_matrix(
        (data.ravel(), (rows.ravel(), cols.ravel())), shape=dense.shape
    ).tocsr()
    assert t.values.nnz == dense.size, "the zero must be STORED, not dropped"
    assert t.values.count_nonzero() == dense.size - 1
    m = marginal_prior(t)
    assert m.dense().sum(axis=1) == pytest.approx(t.dense().sum(axis=1))
    assert t.dense()[0, 3] == FLOOR


# ---------------------------------------------------------------------------
# The reshape guard (T9-R4). Independent of the FLOOR-for-missing reversal and
# kept exactly as it was, now asserted for ALL FOUR controls.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", list(CONTROLS))
def test_a_column_count_that_disagrees_with_the_cell_axis_is_rejected(name):
    """T9-R4. `PriorTensor` checks this at construction, but it is a mutable
    dataclass: reassigning `cells` re-opens the hole, and this module is the one
    that reshapes into (species, cell, week) blocks.

    Parametrized over all five because `uniform` and `marginal` do not reshape
    at all and would otherwise surface the mismatch two steps later, out of
    `_respec`, as `PriorTensor`'s own message -- which names a different
    contract and points the caller at the wrong layer. `separable` DOES
    reshape, so for it the guard is the difference between a named contract and
    an opaque numpy error (or, if the sizes divide, a silently wrong block
    structure whose "cell marginal" is a stride through different weeks).
    """
    t = _tensor()
    t.cells = list(t.cells) + ["8418c53ffffffff"]
    with pytest.raises(ValueError, match="disagrees with the cell axis"):
        CONTROLS[name](t)


def test_the_column_count_guard_runs_before_the_reshape():
    """Without it, a mismatch that happens to divide reshapes into a WRONG block
    structure and `shuffle_space` permutes something that is not a cell."""
    t = _tensor()
    t.cells = list(t.cells)[:-1]
    with pytest.raises(ValueError, match="disagrees with the cell axis"):
        shuffle_time(t, seed=0)


def test_the_column_count_guard_does_not_raise_on_a_valid_tensor():
    """The guard must not be so strict it rejects what `to_tensor` produces --
    on a fully stored grid OR on a realistically sparse one."""
    for fixture in (_tensor, _sparse_tensor):
        for control in CONTROLS.values():
            assert control(fixture()).values.get_shape() == fixture().values.get_shape()


# ---------------------------------------------------------------------------
# the registry
# ---------------------------------------------------------------------------


def test_controls_registry_covers_all_five():
    assert set(CONTROLS) == {
        "uniform", "marginal", "shuffle_space", "shuffle_time", "separable"
    }


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
    for fixture in (_tensor, _sparse_tensor):
        t = fixture()
        for name, fn in CONTROLS.items():
            vec = fn(t).vector(CELLS[0], 20)
            assert not np.isclose(vec.sum(), 1.0), name


def test_every_control_preserves_the_full_priors_total_mass():
    """The single T9-R2 property, asserted across all five at once: whatever the
    sweep compares, it compares at one scale."""
    t = _tensor()
    for name, fn in CONTROLS.items():
        assert fn(t).dense().sum() == pytest.approx(t.dense().sum()), name


# ---------------------------------------------------------------------------
# M1: `separable` -- the fifth control, which isolates the SPACE x TIME
# INTERACTION.
#
# Before it existed, NO control in the set preserved both marginals while
# destroying only their interaction: `uniform` destroys everything, `marginal`
# destroys space AND time AND the interaction together, and BOTH shuffles
# relabel one axis and carry the joint structure along with it, leaving the
# interaction's magnitude EXACTLY unchanged. A reviewer could fairly say the
# reported gain was never separated from "common in this region" plus "common
# in this week" acting independently.
#
# The measured numbers here are PIPELINE numbers -- see the pipeline fixture at
# the bottom of this file -- not fixture numbers.
# ---------------------------------------------------------------------------


def _blocks(dense, n_cells=None):
    n_cells = len(CELLS) if n_cells is None else n_cells
    return dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)


def _interaction(block):
    """The two-way ANOVA residual of one species' (cell x week) block."""
    return (
        block
        - block.mean(axis=1, keepdims=True)
        - block.mean(axis=0, keepdims=True)
        + block.mean()
    )


def _interaction_std(dense, n_cells=None, log=False):
    b = _blocks(np.log(dense) if log else dense, n_cells)
    return float(
        np.concatenate([_interaction(b[s]).ravel() for s in range(b.shape[0])]).std()
    )


# A SEPARATE fixture, and finding out why is the point. `_p` is
# `cell_term + week_term`, multiplied by a per-species constant, so it is
# ADDITIVELY separable: its two-way ANOVA residual is 4.9e-17, i.e. ZERO. That
# makes it useless as the fixture for this control in BOTH directions -- there
# is no arithmetic interaction to observe being destroyed, and (measured) the
# multiplicative surrogate actually INCREASES the arithmetic residual there,
# from 4.9e-17 to 3.2e-02, because an additively separable grid is not a
# multiplicatively separable one.
#
# `_p` is NOT changed: the existing four controls' reduction tests
# (mean-vs-median, per-cell-mean, quadratic-in-cell) all depend on its precise
# shape. This fixture is added beside it, with a phase-shifted seasonal peak per
# CELL, which is what makes a grid genuinely non-separable in both senses: WHEN
# a species peaks depends on WHERE, which is the real ecological joint structure
# (a wader's passage weeks differ between the west coast and the Sound) and is
# precisely the term the fifth control exists to isolate.
_INTERACTION_PHASE_STEP = 9.0


def _interacting_p(cell_index: int, week: int, species_index: int) -> float:
    """A value whose seasonal PEAK WEEK depends on the cell. Non-separable.

    Two Gaussian peaks half a year apart, whose phase advances with the cell
    index, plus a small monotone week term so that no two week slots inside a
    block carry the same value (a repeated value would let a real permutation
    look like a no-op). Every value stays inside (0, 1] as `_validate_p`
    requires, and the per-species scaling is `_MASS_RATIO` as elsewhere.
    """
    phase = 6.0 + _INTERACTION_PHASE_STEP * cell_index
    seasonal = math.exp(-0.5 * ((week - phase) / 5.0) ** 2) + math.exp(
        -0.5 * ((week - (phase + 26)) / 5.0) ** 2
    )
    base = 0.02 + 0.0002 * week + 0.5 * seasonal
    return base * (_MASS_RATIO if species_index else 1.0)


def _interacting_tensor():
    """A fully stored grid carrying a REAL space x time interaction."""
    return _to_tensor(
        [
            (SPECIES[si], cell, week, _interacting_p(ci, week, si))
            for si in range(len(SPECIES))
            for ci, cell in enumerate(CELLS)
            for week in range(1, WEEKS_PER_YEAR + 1)
        ]
    )


def test_the_MAIN_fixture_has_NO_arithmetic_interaction_so_it_cannot_pin_this():
    """The trap, pinned so nobody re-uses `_tensor()` for a separable test.

    `_p` is `cell_term + week_term` times a per-species constant, so its
    two-way ANOVA residual is ZERO to float rounding: there is no arithmetic
    interaction for the fifth control to be seen destroying. Worse than merely
    vacuous -- the multiplicative surrogate INCREASES the residual on this
    fixture, so a naive "the interaction shrinks" assertion would FAIL here for
    a correct implementation and invite someone to weaken it.

    Asserted rather than discovered a second time, and it is why
    `_interacting_tensor()` exists.
    """
    dense = _tensor().dense()
    assert _interaction_std(dense) == pytest.approx(0.0, abs=1e-12)
    # Additively separable is NOT multiplicatively separable: the log-space
    # interaction is real here, and the control removes exactly that.
    assert _interaction_std(dense, log=True) > 0.1
    after = separable_prior(_tensor()).dense()
    assert _interaction_std(after, log=True) == pytest.approx(0.0, abs=1e-12)
    # The arithmetic residual GROWS, which is the trap stated as a number.
    assert _interaction_std(after) > _interaction_std(dense)


def test_the_interacting_fixture_really_does_have_an_interaction_to_destroy():
    """The precondition without which every `separable` assertion is vacuous.

    Measured on this fixture: interaction residual standard deviation 0.1179
    arithmetic and 0.929 in log space, against `_tensor()`'s 4.9e-17.
    """
    dense = _interacting_tensor().dense()
    assert _interaction_std(dense) > 0.1
    assert _interaction_std(dense, log=True) > 0.5
    # And it is a real share of the grid's own spread, not float noise.
    assert _interaction_std(dense) > 0.1 * dense.std()


def test_the_interacting_fixture_satisfies_the_shared_fixture_preconditions():
    """It is a legitimate prior and it is observable, asserted directly rather
    than inherited from `_tensor()`'s own precondition tests."""
    t = _interacting_tensor()
    dense = t.dense()
    assert dense.min() > 0.0 and dense.max() <= 1.0
    assert t.values.nnz == t.dense_capacity
    assert not np.isclose(dense[0].sum(), dense[1].sum())
    blocks = _blocks(dense)
    for s in range(len(SPECIES)):
        for c in range(len(CELLS)):
            assert len(set(blocks[s, c].tolist())) == WEEKS_PER_YEAR
        for i in range(len(CELLS)):
            for j in range(i + 1, len(CELLS)):
                assert not np.allclose(blocks[s, i], blocks[s, j])


def test_separable_variant_is_labelled():
    assert separable_prior(_interacting_tensor()).spec.variant == "separable"


def test_separable_preserves_every_species_CELL_marginal_exactly():
    """Half of the contract, and the half a within-cell week permutation also
    satisfies -- which is why the week marginal below is the discriminating
    one."""
    t = _interacting_tensor()
    before = _blocks(t.dense())
    after = _blocks(separable_prior(t).dense())
    assert after.mean(axis=2) == pytest.approx(before.mean(axis=2))


def test_separable_preserves_every_species_WEEK_marginal_exactly():
    """The other half, and THE reason the outer product was chosen over an
    independently-drawn within-cell week permutation: that alternative was
    built and measured and destroys this marginal (max relative error 1.383 on
    a pipeline artifact, against float rounding here), because each cell's
    weekly values land under different week labels."""
    t = _interacting_tensor()
    before = _blocks(t.dense())
    after = _blocks(separable_prior(t).dense())
    assert after.mean(axis=1) == pytest.approx(before.mean(axis=1))


def test_separable_preserves_each_species_own_mass_and_the_total():
    """Follows from either marginal, asserted directly because it is the T9-R2
    scale property and must not depend on a derivation."""
    t = _interacting_tensor()
    s = separable_prior(t)
    assert s.dense().sum(axis=1) == pytest.approx(t.dense().sum(axis=1))
    assert s.dense().sum() == pytest.approx(t.dense().sum())


def test_separable_DESTROYS_the_interaction_it_exists_to_isolate():
    """The point of the control, and not implied by any marginal assertion: a
    control that returned its input unchanged would pass every preservation
    test above -- and on `_tensor()` it WOULD be the identity, which is why this
    uses the interacting fixture.

    The construction is exact in LOG space -- a product is additive there -- so
    the log-space interaction residual must vanish to float rounding while the
    arithmetic one merely shrinks (measured: 0.1179 -> 0.0107, a factor 0.09).
    """
    t = _interacting_tensor()
    before = t.dense()
    after = separable_prior(t).dense()
    assert _interaction_std(before) > 0.1, "precondition: there is one to destroy"
    assert _interaction_std(after, log=True) == pytest.approx(0.0, abs=1e-12)
    assert _interaction_std(after) < 0.2 * _interaction_std(before)
    # And the output is NOT the input: the preservation tests above are all
    # satisfied by the identity function.
    assert not np.allclose(after, before)


def test_separable_is_NOT_a_shuffle_and_NOT_the_marginal_control():
    """Three controls could be confused for one another by a mass assertion.

    Both shuffles preserve the interaction's magnitude exactly (they permute
    it), and `marginal` destroys the marginals too. This separates all three.
    """
    t = _interacting_tensor()
    before = t.dense()
    sep = separable_prior(t).dense()
    for shuffled in (
        shuffle_space(t, seed=SPACE_SEED).dense(),
        shuffle_time(t, seed=TIME_SEED).dense(),
    ):
        assert _interaction_std(shuffled) == pytest.approx(_interaction_std(before))
        assert _interaction_std(sep) < _interaction_std(shuffled)
    # `marginal` flattens both marginals; `separable` keeps them.
    b = _blocks(before)
    m = _blocks(marginal_prior(t).dense())
    assert not np.allclose(m.mean(axis=2), b.mean(axis=2))
    assert np.allclose(_blocks(sep).mean(axis=2), b.mean(axis=2))


def test_separable_uses_EACH_SPECIES_OWN_marginals_not_pooled_ones():
    """Pooling across species would mix label frequency into the marginals and
    the control would stop preserving each species' own mass -- while still
    preserving the TOTAL, so the total-mass test could not see it."""
    t = _interacting_tensor()
    dense = t.dense()
    b = _blocks(dense)
    expected = (
        b.mean(axis=2, keepdims=True)
        * b.mean(axis=1, keepdims=True)
        / b.mean(axis=(1, 2), keepdims=True)
    ).reshape(dense.shape)
    assert separable_prior(t).dense() == pytest.approx(expected)
    # A POOLED version is a different array on this fixture, so "each species'
    # own" is pinned rather than assumed.
    pooled = (
        b.mean(axis=(0, 2), keepdims=True)
        * b.mean(axis=(0, 1), keepdims=True)
        / b.mean()
    )
    assert not np.allclose(
        expected, np.broadcast_to(pooled, b.shape).reshape(dense.shape)
    )


def test_separable_runs_on_a_realistically_sparse_prior_too():
    """The C3-class regression, for the fifth control: the floor participates
    and the grand mean is over the FLOORED grid, so a sparse prior must
    compute rather than refuse or divide by zero."""
    t = _sparse_tensor()
    s = separable_prior(t)
    assert s.dense().min() > 0.0
    assert np.isfinite(s.dense()).all()
    assert s.dense().sum() == pytest.approx(t.dense().sum())
    assert s.values.nnz == t.dense_capacity


def test_separable_REFUSES_a_prior_whose_outer_product_leaves_the_unit_interval():
    """No silent repair at a boundary.

    The outer product can exceed 1.0 even when every input is a probability: a
    block whose mass is concentrated in ONE cell AND ONE week has a large cell
    marginal and a large week marginal in the same place, and their product
    over a small grand mean is large. Clamping would break BOTH marginals and
    mass preservation -- the invariants the control is defined by -- and the
    sweep would read a quietly renormalised control as a weak spatiotemporal
    effect. So it raises, naming the check.
    """
    rows = []
    for sk in SPECIES:
        for ci, cell in enumerate(CELLS):
            for week in range(1, WEEKS_PER_YEAR + 1):
                hot = ci == 0 or week == 1
                rows.append((sk, cell, week, 0.6 if hot else 1e-06))
    t = _to_tensor(rows)
    # Precondition: every INPUT value is a legitimate probability, so the
    # refusal is about the CONSTRUCTION and not about bad input.
    assert 0.0 < t.dense().min() and t.dense().max() <= 1.0
    with pytest.raises(ValueError, match="outer product of its marginals reaches"):
        separable_prior(t)


def test_separable_does_not_refuse_an_ordinary_prior():
    """The mirror of the guard: it must not be so strict it rejects what the
    pipeline or any of the hand-built fixtures produce."""
    for fixture in (_tensor, _sparse_tensor, _interacting_tensor):
        out = separable_prior(fixture())
        assert out.dense().max() <= 1.0
        assert out.spec.variant == "separable"


# ---------------------------------------------------------------------------
# C2: THE PIPELINE FIXTURE.
#
# Every fixture above is hand-built and NONE of them is representative of a
# released artifact in the way that matters: a pipeline build stores EVERY
# context. The released documentation claimed the opposite for both, and the
# doc's own pinning test could not catch it because it also saves a hand-built
# sparse fixture. This section builds through `build_from_archive` -- the real
# CLI path -- and asserts the documented regime against what it writes.
#
# Deliberately small (6 species x 3 points x 52 weeks) so it runs in well under
# a second, but built through the SAME function `scripts/build_prior.py` calls,
# so the fill regime it exhibits is the pipeline's and not a fixture's.
# ---------------------------------------------------------------------------

_PIPE_HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)
_PIPE_DOI = "10.15468/dl.pipeline5"
#: Two residents (flat season), two summer migrants (peak week 26), two waders
#: (double passage peaks, west-coast biased). THREE archetypes so the artifact
#: carries a real space x time INTERACTION -- with one archetype the grid would
#: be near-separable and `separable` would be near-identity.
_PIPE_RESIDENT = (2001, 2002)
_PIPE_SUMMER = (3001, 3002)
_PIPE_WADER = (4001, 4002)
_PIPE_SPECIES = _PIPE_RESIDENT + _PIPE_SUMMER + _PIPE_WADER
#: Three Danish longitudes at four latitudes: west Jutland, Funen, Zealand.
_PIPE_POINTS = [
    (lat, lon) for lat in (54.9, 55.7, 56.5, 57.2) for lon in (8.4, 10.5, 12.6)
]


def _pipe_lambda(sk, week, lon):
    if sk in _PIPE_RESIDENT:
        return 5.0
    if sk in _PIPE_SUMMER:
        season = math.exp(-0.5 * ((week - 26) / 6.0) ** 2) + 0.05
        return 5.0 * season * (0.2 + (lon - 8.4) / 4.2)
    season = (
        math.exp(-0.5 * ((week - 17) / 3.0) ** 2)
        + math.exp(-0.5 * ((week - 35) / 3.0) ** 2)
        + 0.05
    )
    return 5.0 * season * (0.1 + math.exp(-((lon - 8.4) ** 2) / 0.8))


def _pipeline_archive(tmp_path):
    rng = np.random.default_rng(7)
    rows = []
    gid = 0
    for week in range(1, WEEKS_PER_YEAR + 1):
        day = datetime.date.fromisocalendar(2025, week, 3).isoformat()
        for lat, lon in _PIPE_POINTS:
            for sk in _PIPE_SPECIES:
                for _ in range(int(rng.poisson(_pipe_lambda(sk, week, lon)))):
                    gid += 1
                    rows.append("\t".join([
                        str(gid), "ds", "pub", str(sk), f"sp{sk}",
                        f"{lat + rng.normal(0, 0.01):.6f}",
                        f"{lon + rng.normal(0, 0.01):.6f}", day, "obs",
                    ]))
    path = tmp_path / "pipeline_occ.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("occurrence.txt", "\n".join([_PIPE_HEADER, *rows]) + "\n")
    return path


def _pipeline_artifacts(tmp_path):
    """variant -> (PriorTensor, sidecar dict), via the real build function."""
    from build_prior import build_from_archive

    out = tmp_path / "priors"
    paths = build_from_archive(
        _pipeline_archive(tmp_path),
        resolution=4,
        source="dof",
        test_recordings=[],
        out_dir=out,
        gbif_doi=_PIPE_DOI,
    )
    artifacts = {}
    for p in paths:
        t = load_prior(p)
        artifacts[t.spec.variant] = (
            t,
            json.loads(p.with_suffix(".json").read_text(encoding="utf-8")),
        )
    return artifacts


def test_the_pipeline_fixture_writes_the_full_prior_and_all_five_controls(tmp_path):
    """The precondition for everything below, and the registry's reach into the
    build loop: `build_prior.py` iterates `CONTROLS.items()`, so a fifth
    registry entry must appear on disk with no build-script edit."""
    artifacts = _pipeline_artifacts(tmp_path)
    assert set(artifacts) == {
        "full", "uniform", "marginal", "shuffle_space", "shuffle_time", "separable"
    }
    for variant, (t, meta) in artifacts.items():
        assert t.gbif_doi == _PIPE_DOI, variant
        assert meta["spec"]["variant"] == variant


def test_the_documented_fill_regime_holds_on_a_PIPELINE_artifact(tmp_path):
    """C2. `docs/prior-artifact-format.md` section 2a, pinned to a real build.

    The document previously asserted that treating unstored entries as 0.0
    "gives the wrong answer on roughly 98% of the grid" and that "a legitimate
    national-scale prior really does have a fill of a couple of percent". Both
    are FALSE of every artifact the pipeline writes: `build_prior.py` passes
    `cells=` and weeks 1..52 explicitly and `_validate_p` forbids `p <= 0`, so
    every context is an explicit stored nonzero.

    The doc's existing pinning test in `test_tensor.py` cannot catch this
    because it saves a hand-built SPARSE fixture. This one builds through
    `build_from_archive`, so the document is pinned to a PIPELINE artifact and
    the error cannot recur.
    """
    doc = (
        Path(__file__).parent.parent.parent / "docs" / "prior-artifact-format.md"
    ).read_text(encoding="utf-8")
    # The document must state the regime it is now pinned to.
    assert "nnz == dense_capacity" in doc
    assert "fill == 1.0000" in doc
    # And must NOT re-assert the corrected claims.
    assert "roughly 98% of the grid" not in doc
    assert "a fill of a couple of percent" not in doc

    artifacts = _pipeline_artifacts(tmp_path)
    for variant, (t, meta) in artifacts.items():
        dense = t.dense()
        assert meta["nnz"] == meta["dense_capacity"], variant
        assert meta["nnz"] == t.dense_capacity == dense.size, variant
        assert meta["nnz"] / meta["dense_capacity"] == 1.0, variant
        # The load-bearing half: NOTHING reads as the floor, so a decoder that
        # wrongly fills with 0.0 differs from a correct one by exactly zero.
        assert int((dense == FLOOR).sum()) == 0, variant
        assert dense.min() > 0.0, variant
        # `missing_value` is still published, for forward compatibility and for
        # hand-built sparse tensors.
        assert meta["missing_value"] == FLOOR, variant


def test_the_uniform_log_space_offset_is_ONE_nat_on_a_pipeline_artifact(tmp_path):
    """C2, second site. `controls.py` quoted 7.943 nats, measured on the
    1.92%-fill fixture, which is ~8x the released magnitude.

    The AM-GM argument is unchanged and its SIGN is structural: among all fills
    with the same arithmetic total the constant one maximises the geometric
    mean, so `uniform` sits ABOVE the full prior in log space. The MAGNITUDE is
    a property of the artifact, and on a fully materialised one it is of order
    one nat -- measured 0.681 on this fixture's geometry -- because there is no
    floor mass to drag the prior's mean log down.
    """
    artifacts = _pipeline_artifacts(tmp_path)
    full = np.log(artifacts["full"][0].dense()).mean()
    uniform = np.log(artifacts["uniform"][0].dense()).mean()
    offset = float(uniform - full)
    assert offset > 0.0, "AM-GM: the constant fill maximises the geometric mean"
    # Order ONE nat, not order eight. Bounded on both sides so a drift toward
    # either the old fixture figure or zero fails.
    assert 0.2 < offset < 2.5
    # And the fixture's own figure is a DIFFERENT number, so the two cannot
    # drift into standing for one another.
    t = _sparse_tensor()
    fixture_offset = float(
        np.log(uniform_prior(t).dense()).mean() - np.log(t.dense()).mean()
    )
    assert fixture_offset > 4 * offset


def test_marginal_contrast_is_EXACT_on_a_pipeline_artifact(tmp_path):
    """C2, third site. The contrast-decay table quoted a decay to 1.007 and a
    3.994-from-4.000 loss, both fixture figures.

    With no floor mass there is no common additive baseline to dilute, so the
    ratio `marginal` emits equals the full prior's per-species-mean ratio
    EXACTLY -- not to within 1%, exactly. The decay is real and is a property
    of a hand-built SPARSE tensor, which `test_marginals_contrast_is_squeezed_
    toward_one_by_the_floor` still pins.
    """
    artifacts = _pipeline_artifacts(tmp_path)
    full = artifacts["full"][0].dense()
    emitted = artifacts["marginal"][0].dense()[:, 0]
    reference = full.mean(axis=1)
    assert emitted == pytest.approx(reference, rel=0, abs=1e-15)
    # Precondition: there IS a contrast to preserve, so "exact" is not exact
    # agreement between two constants.
    assert reference.max() / reference.min() > 2.0
    assert float(emitted.max() / emitted.min()) == pytest.approx(
        float(reference.max() / reference.min()), rel=0, abs=1e-12
    )


def test_the_interaction_carries_a_real_share_of_a_PIPELINE_artifact(tmp_path):
    """M1's premise, measured rather than asserted.

    If the interaction were negligible on a real artifact, the fifth control
    would be answering a question nobody needs to ask. Measured on this build:
    the interaction carries 20-57% of each species' total variation (mean 32%).
    Pinned as a floor on the MINIMUM across species, so a smoothing change that
    flattened the joint term would fail here rather than quietly making the
    control vacuous.
    """
    full = _pipeline_artifacts(tmp_path)["full"][0]
    b = _blocks(full.dense(), len(full.cells))
    shares = []
    for s in range(b.shape[0]):
        blk = b[s]
        total = ((blk - blk.mean()) ** 2).sum()
        shares.append((_interaction(blk) ** 2).sum() / total)
    shares = np.asarray(shares)
    assert shares.min() > 0.10, f"interaction shares {shares}"
    assert shares.mean() > 0.20


def test_only_separable_destroys_the_interaction_on_a_PIPELINE_artifact(tmp_path):
    """M1's conclusion, on the artifact rather than on a fixture.

    Both shuffles leave the interaction standard deviation IDENTICAL to the
    full prior's -- they relabel an axis and carry the joint structure along
    with it. `uniform` and `marginal` destroy it along with both marginals.
    Only `separable` removes it while keeping the marginals, which is what
    makes the reported gain attributable to the joint term.
    """
    artifacts = _pipeline_artifacts(tmp_path)
    full = artifacts["full"][0]
    n_cells = len(full.cells)
    base = _interaction_std(full.dense(), n_cells)
    assert base > 0.0

    def istd(variant):
        return _interaction_std(artifacts[variant][0].dense(), n_cells)

    # The two shuffles: unchanged, which is the finding.
    assert istd("shuffle_space") == pytest.approx(base)
    assert istd("shuffle_time") == pytest.approx(base)
    # The two blunt controls: gone, but so are the marginals.
    assert istd("uniform") == pytest.approx(0.0, abs=1e-12)
    assert istd("marginal") == pytest.approx(0.0, abs=1e-12)
    # `separable`: interaction gone (exactly, in log space), marginals kept.
    assert istd("separable") < 0.6 * base
    sep = artifacts["separable"][0].dense()
    assert _interaction_std(sep, n_cells, log=True) == pytest.approx(0.0, abs=1e-12)
    b = _blocks(full.dense(), n_cells)
    sb = _blocks(sep, n_cells)
    assert sb.mean(axis=2) == pytest.approx(b.mean(axis=2))
    assert sb.mean(axis=1) == pytest.approx(b.mean(axis=1))


def test_every_control_preserves_a_PIPELINE_priors_mass(tmp_path):
    """T9-R2 on the artifact the sweep will actually read, all five at once."""
    artifacts = _pipeline_artifacts(tmp_path)
    reference = artifacts["full"][0].dense().sum()
    for variant, (t, _meta) in artifacts.items():
        assert t.dense().sum() == pytest.approx(reference), variant
