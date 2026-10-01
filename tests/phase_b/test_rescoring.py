"""Bayesian rescoring + the four controls (Task 4).

The first fusion arm: training-free rescoring of encoder posteriors by the
occurrence prior in log space, and the four controls (uniform / marginal /
space-shuffled / time-shuffled) whose defining properties make any measured
gain credible. Fixtures build tiny in-memory PriorTensors through the
dataclass directly — never load_prior file I/O, never the network.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_b import rescoring
from birdology.phase_b.rescoring import (
    control_prior,
    prior_for_rows,
    rescore,
    train_marginal_from_prior,
)
from birdology.prior import tensor as tensor_module
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import FLOOR, PriorSpec, PriorTensor


def _prior(
    dense: np.ndarray,
    cells: list[str],
    species_keys: list[int],
    variant: str = "full",
) -> PriorTensor:
    """A tiny in-memory PriorTensor; __post_init__ validates axes/matrix.

    *dense* is (n_species, n_cells * WEEKS_PER_YEAR) with values in (0, 1];
    a 0.0 entry would be stored as "unstored" and read back as FLOOR, so
    fixtures keep every value strictly positive unless that is the point.
    """
    return PriorTensor(
        spec=PriorSpec(resolution=4, source="eod", variant=variant),
        species_keys=np.asarray(species_keys, dtype=np.int64),
        cells=list(cells),
        values=sparse.csr_matrix(np.asarray(dense, dtype=np.float64)),
    )


def _asymmetric_prior(n_cells: int = 2) -> PriorTensor:
    """3 species x *n_cells* cells, every (species, cell, week) value
    DISTINCT — the same-multiset trap from the occurrence-prior plan:
    species have genuinely different context profiles, so the marginal
    control varies by species and a shuffled tensor cannot silently equal
    the original.
    """
    n_species = 3
    n_cols = n_cells * WEEKS_PER_YEAR
    dense = (1.0 + np.arange(n_species * n_cols, dtype=np.float64)).reshape(
        n_species, n_cols
    ) / (n_species * n_cols + 1.0)
    cells = [f"cell{chr(ord('a') + i)}" for i in range(n_cells)]
    return _prior(dense, cells, [111 * (i + 1) for i in range(n_species)])


def _cell_block(prior: PriorTensor, cell: str) -> np.ndarray:
    """The (n_species, WEEKS_PER_YEAR) dense block of one cell."""
    ci = prior.cells.index(cell)
    return prior.dense()[:, ci * WEEKS_PER_YEAR : (ci + 1) * WEEKS_PER_YEAR]


# ---------------------------------------------------------------------------
# 1. tau == 0 short-circuits — scores returned BIT-identical
# ---------------------------------------------------------------------------


def test_tau_zero_returns_scores_bit_identical_including_dtype():
    """tau == 0.0 must short-circuit: no log round-trip, no float64
    upcast — the returned array is bit-identical to the input, float32
    dtype and all. This pins the sweep's tau=0 column to EXACTLY the raw
    encoder scores, so a fusion gain at tau>0 is measured against the
    true baseline rather than a re-encoded copy of it.
    """
    rng = np.random.default_rng(20261001)
    scores = rng.random((5, 4)).astype(np.float32)
    prior_probs = rng.random((5, 4))
    marginal = rng.random(4)
    out = rescore(scores, prior_probs, 0.0, marginal)
    assert out.dtype == scores.dtype == np.float32
    assert np.array_equal(out, scores)


# ---------------------------------------------------------------------------
# 2. floor before log — and FLOOR is IMPORTED, never re-defined
# ---------------------------------------------------------------------------


def test_prior_prob_of_zero_rescores_finite_floor_applied_before_log():
    """A hard 0.0 prior probability (and a 0.0 training marginal) must be
    floored BEFORE the log: every rescored value finite, no -inf/nan
    anywhere. A -inf here would let the prior veto a correct detection —
    exactly what the upstream floor exists to prevent.
    """
    scores = np.array([[0.9, 0.5], [0.2, 0.8]])
    prior_probs = np.array([[0.0, 0.3], [0.4, 0.0]])
    marginal = np.array([0.0, 0.25])
    out = rescore(scores, prior_probs, 1.0, marginal)
    assert np.all(np.isfinite(out))


def test_floor_is_the_prior_tensor_modules_object_not_a_redefinition():
    """rescoring.FLOOR must be THE SAME OBJECT as prior.tensor's FLOOR: a
    re-defined constant could drift from the artifact's convention and
    floor against a different value than the tensor reads for "no
    evidence".
    """
    assert rescoring.FLOOR is tensor_module.FLOOR


# ---------------------------------------------------------------------------
# 3. prior_for_rows — partial bridge, unknown cells: floored and COUNTED,
#    never a KeyError, never a silent drop
# ---------------------------------------------------------------------------


def test_prior_for_rows_floors_and_counts_every_unusable_lookup():
    """Three distinct failure shapes, all floored + surfaced, none raising:

    * row with an UNKNOWN cell -> whole row FLOOR;
    * scoreable code ABSENT from code_to_gbif ('sp3') -> FLOOR column,
      named in missing_species;
    * scoreable code whose GBIF key is NOT on prior.species_keys
      ('sp2' -> 999) -> same handling.

    Fixture: species 111/222 on the prior, cells 'cella'/'cellb', every
    stored value well above FLOOR. Rows: ('cella', 10) known and
    ('nowhere', 3) unknown. scoreable = [sp1, sp2, sp3, sp4] with
    sp1->111, sp2->999 (off-axis), sp3 unmapped, sp4->222.

    n_floored = 4 (unknown-cell row) + 2 (sp2, sp3 on the known row) = 6.
    """
    n_cols = 2 * WEEKS_PER_YEAR
    dense = np.full((2, n_cols), 0.25)
    dense[0, 9] = 0.7  # (111, 'cella', week 10)
    dense[1, 9] = 0.6  # (222, 'cella', week 10)
    prior = _prior(dense, ["cella", "cellb"], [111, 222])

    scoreable = ["sp1", "sp2", "sp3", "sp4"]
    code_to_gbif = {"sp1": 111, "sp2": 999, "sp4": 222}
    lookup = prior_for_rows(
        prior,
        cells=["cella", "nowhere"],
        weeks=[10, 3],
        scoreable=scoreable,
        code_to_gbif=code_to_gbif,
    )

    assert lookup.probs.shape == (2, 4)
    assert lookup.probs.dtype == np.float64
    # Known row, usable species: the tensor's own values.
    assert lookup.probs[0, 0] == pytest.approx(prior.lookup(111, "cella", 10))
    assert lookup.probs[0, 3] == pytest.approx(prior.lookup(222, "cella", 10))
    # Unusable species columns are FLOOR on the known row too.
    assert lookup.probs[0, 1] == FLOOR
    assert lookup.probs[0, 2] == FLOOR
    # Unknown cell: the entire row is FLOOR.
    assert np.all(lookup.probs[1] == FLOOR)
    # Counted and named — never silently dropped.
    assert lookup.n_floored == 6
    assert lookup.missing_species == ["sp2", "sp3"]


# ---------------------------------------------------------------------------
# 4. the four controls' DEFINING properties
# ---------------------------------------------------------------------------


def test_uniform_control_is_constant_one_over_n_species_everywhere():
    """Every (cell, week) vector identical AND constant 1/n_species: the
    no-context control carries no information at all."""
    prior = _asymmetric_prior()
    ctrl = control_prior("uniform", prior, np.random.default_rng(0))
    assert ctrl.spec.variant == "uniform"
    assert ctrl.spec.resolution == prior.spec.resolution
    assert ctrl.spec.source == prior.spec.source
    np.testing.assert_array_equal(ctrl.dense(), 1.0 / 3.0)
    for cell in ctrl.cells:
        np.testing.assert_array_equal(
            ctrl.vector(cell, 1), np.full(3, 1.0 / 3.0)
        )


def test_marginal_control_varies_by_species_constant_across_contexts():
    """Each species' vector entry equals that species' GLOBAL marginal
    (mean of the full prior over ALL cell-week columns): it varies across
    species — the asymmetric fixture's teeth — but is constant across
    every (cell, week) context, so it isolates "which species are common"
    from "where and when".
    """
    prior = _asymmetric_prior()
    ctrl = control_prior("marginal", prior, np.random.default_rng(0))
    assert ctrl.spec.variant == "marginal"
    expected = prior.dense().mean(axis=1)
    # Genuinely species-dependent (the same-multiset trap defused).
    assert len(np.unique(expected)) == 3
    dense = ctrl.dense()
    # Constant across contexts, per species...
    np.testing.assert_allclose(
        dense, np.broadcast_to(expected[:, None], dense.shape), rtol=1e-12
    )
    # ...and the constant IS the global marginal.
    np.testing.assert_allclose(
        ctrl.vector("cella", 7), expected, rtol=1e-12
    )


def test_space_shuffled_control_moves_cell_vectors_without_changing_them():
    """The cells axis is permuted: the MULTISET of per-cell blocks is
    preserved (same vectors) while the cell -> vector mapping changes
    (wrong places; at least one block moved). Per-species GLOBAL marginal
    unchanged — the control destroys 'where' while keeping 'how much'.

    Seed 3 is verified to swap a 2-element permutation.
    """
    prior = _asymmetric_prior()
    ctrl = control_prior("space_shuffled", prior, np.random.default_rng(3))
    assert ctrl.spec.variant == "shuffle_space"
    before = {c: _cell_block(prior, c) for c in prior.cells}
    after = {c: _cell_block(ctrl, c) for c in ctrl.cells}
    # Multiset of cell blocks preserved...
    assert sorted(b.tobytes() for b in before.values()) == sorted(
        a.tobytes() for a in after.values()
    )
    # ...and at least one cell's vector MOVED.
    assert any(
        not np.array_equal(before[c], after[c]) for c in prior.cells
    )
    # Per-species global marginal unchanged.
    np.testing.assert_allclose(
        ctrl.dense().mean(axis=1), prior.dense().mean(axis=1), rtol=1e-12
    )


def test_time_shuffled_control_permutes_weeks_within_each_cell():
    """Weeks are permuted WITHIN each cell: per-cell week-multiset
    preserved and order changed (circular seasonal structure destroyed),
    while cell identity is kept — each cell's annual profile (mean over
    weeks, per species) is unchanged.
    """
    prior = _asymmetric_prior()
    ctrl = control_prior("time_shuffled", prior, np.random.default_rng(3))
    assert ctrl.spec.variant == "shuffle_time"
    moved = False
    for cell in prior.cells:
        before = _cell_block(prior, cell)
        after = _cell_block(ctrl, cell)
        # Same multiset of weekly values per species, within the cell...
        np.testing.assert_array_equal(
            np.sort(before, axis=1), np.sort(after, axis=1)
        )
        # ...annual profile untouched...
        np.testing.assert_allclose(
            before.mean(axis=1), after.mean(axis=1), rtol=1e-12
        )
        if not np.array_equal(before, after):
            moved = True
    # ...and the order genuinely changed somewhere.
    assert moved


# ---------------------------------------------------------------------------
# 5. shuffles are deterministic in the rng
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["space_shuffled", "time_shuffled"])
def test_shuffles_deterministic_same_seed_different_across_seeds(kind):
    """Same seed -> bit-identical shuffled tensor; different seed ->
    different permutation. 12 cells of fully distinct values: identical-
    permutation collision odds are nil, so a difference in values proves
    a difference in permutation.
    """
    prior = _asymmetric_prior(n_cells=12)
    a = control_prior(kind, prior, np.random.default_rng(42))
    b = control_prior(kind, prior, np.random.default_rng(42))
    c = control_prior(kind, prior, np.random.default_rng(43))
    np.testing.assert_array_equal(a.dense(), b.dense())
    assert not np.array_equal(a.dense(), c.dense())


# ---------------------------------------------------------------------------
# 6. the rescore math, pinned by hand on a 2x2
# ---------------------------------------------------------------------------


def test_rescore_log_space_formula_hand_worked_two_by_two():
    """log s + tau * (log p - log m) at tau=1 collapses to
    log(s * p / m); worked by hand:

        s = [[0.5, 0.25],   p = [[0.2, 0.4],   m = [0.25, 0.25]
             [0.1, 0.8 ]]        [0.5, 0.1]]

        (0,0): 0.5  * 0.2 / 0.25 = 0.4   -> ln 0.4
        (0,1): 0.25 * 0.4 / 0.25 = 0.4   -> ln 0.4
        (1,0): 0.1  * 0.5 / 0.25 = 0.2   -> ln 0.2
        (1,1): 0.8  * 0.1 / 0.25 = 0.32  -> ln 0.32
    """
    scores = np.array([[0.5, 0.25], [0.1, 0.8]])
    prior_probs = np.array([[0.2, 0.4], [0.5, 0.1]])
    marginal = np.array([0.25, 0.25])
    out = rescore(scores, prior_probs, 1.0, marginal)
    assert out.dtype == np.float64
    np.testing.assert_allclose(
        out, np.log([[0.4, 0.4], [0.2, 0.32]]), rtol=1e-12
    )


def test_rescore_tau_scopes_the_whole_log_ratio_hand_worked_at_tau_two():
    """tau must scope the WHOLE bracket (log p - log m), not log p alone.

    The mutant ``log s + tau*log p - log m`` is identical at tau=1,
    bypassed by the tau=0 short-circuit, and invisible under constant
    p/m (argsort preserved) — so this test uses tau=2.0 with NON-constant
    p and m and asserts elementwise VALUES, hand-worked:

        s = [[0.5, 0.25],   p = [[0.2, 0.4],   m = [0.25, 0.2]
             [0.1, 0.8 ]]        [0.5, 0.1]]

    correct = log s + 2*(log p - log m) = ln(s * (p/m)^2):
        (0,0): 0.5  * (0.2/0.25)^2 = 0.5  * 0.64 = 0.32 -> ln 0.32
        (0,1): 0.25 * (0.4/0.2 )^2 = 0.25 * 4    = 1.0  -> ln 1.0
        (1,0): 0.1  * (0.5/0.25)^2 = 0.1  * 4    = 0.4  -> ln 0.4
        (1,1): 0.8  * (0.1/0.2 )^2 = 0.8  * 0.25 = 0.2  -> ln 0.2

    The mutant would give e.g. (0,0): ln(0.5 * 0.2^2 / 0.25) = ln 0.08
    != ln 0.32 — elementwise equality kills it.
    """
    scores = np.array([[0.5, 0.25], [0.1, 0.8]])
    prior_probs = np.array([[0.2, 0.4], [0.5, 0.1]])
    marginal = np.array([0.25, 0.2])
    out = rescore(scores, prior_probs, 2.0, marginal)
    np.testing.assert_allclose(
        out, np.log([[0.32, 1.0], [0.4, 0.2]]), rtol=1e-12
    )


def test_prior_for_rows_folds_iso_week_53_into_52_no_raise():
    """A late-December recording can carry ISO week 53 (71 of every 400
    years have one). ``prior.vector`` raises for week 53 — the tensor's
    axis is 52 weeks — so prior_for_rows must FOLD 53 -> 52 (the same
    convention as ``birdology.prior.indexing.iso_week``) rather than let
    one recording crash a whole sweep batch: no raise, and the week-53
    row reads EXACTLY the week-52 vector.
    """
    prior = _asymmetric_prior()
    scoreable = ["sp1", "sp2", "sp3"]
    code_to_gbif = {"sp1": 111, "sp2": 222, "sp3": 333}
    lookup = prior_for_rows(
        prior,
        cells=["cella", "cella"],
        weeks=[53, 52],
        scoreable=scoreable,
        code_to_gbif=code_to_gbif,
    )
    np.testing.assert_array_equal(lookup.probs[0], lookup.probs[1])
    np.testing.assert_allclose(lookup.probs[1], prior.vector("cella", 52))


# ---------------------------------------------------------------------------
# 7. uniform control end-to-end: rank-identical to raw scores
# ---------------------------------------------------------------------------


def test_uniform_control_rescoring_is_rank_identical_to_raw_scores():
    """The no-context control must change NOTHING: with the uniform
    control's probs and its own train marginal (both constant 1/n), the
    log-space correction is identically zero, so per-row rankings equal
    the raw scores' rankings exactly. This pins the sweep's credibility
    floor — a 'gain' under the uniform control would be an artifact.
    """
    prior = _asymmetric_prior()
    ctrl = control_prior("uniform", prior, np.random.default_rng(0))
    scoreable = ["sp1", "sp2", "sp3"]
    code_to_gbif = {"sp1": 111, "sp2": 222, "sp3": 333}
    cells = ["cella", "cellb", "cella", "cellb"]
    weeks = [1, 13, 26, 52]
    lookup = prior_for_rows(ctrl, cells, weeks, scoreable, code_to_gbif)
    marginal = train_marginal_from_prior(ctrl, scoreable, code_to_gbif)

    rng = np.random.default_rng(20261001)
    scores = rng.permutation(12).reshape(4, 3).astype(np.float32)
    scores = (scores + 1.0) / 13.0  # distinct, strictly positive
    out = rescore(scores, lookup.probs, 1.7, marginal)
    assert out.dtype == np.float64
    np.testing.assert_array_equal(
        np.argsort(out, axis=1), np.argsort(scores, axis=1)
    )
