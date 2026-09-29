#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/controls.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_controls_mutants.py
    uv run python tests/prior/mutation/run_controls_mutants.py --only C1 C7
    uv run python tests/prior/mutation/run_controls_mutants.py --verify-labels

Structure is copied from `run_tensor_mutants.py`, including the five things
that make the result trustworthy:

1. it refuses to start unless `git status --porcelain` is clean for the target
   file, so a mutant can never be committed by accident;
2. every pattern must occur EXACTLY ONCE, and NOT-APPLIED is reported
   separately from SURVIVED -- a stale pattern that matched nothing would
   otherwise masquerade as a kill -- and NOT-APPLIED makes the harness EXIT
   NON-ZERO, so it cannot hide under a "Survivors: none" banner;
3. it clears every `__pycache__` BEFORE and AFTER each mutant. CPython
   invalidates a `.pyc` on (mtime, size), so a SAME-LENGTH mutation can leave
   a mutant `.pyc` that outlives its own source restoration and fake either a
   kill or a survival;
4. every mutant NAMES the test that kills it, and that name is CHECKED at
   runtime: a mutant that fails only OTHER tests is scored MISLABELLED, not a
   kill, and also exits non-zero;
5. `--verify-labels` prints the unified diff each mutant actually applies, so
   each LABEL can be read against its EDIT rather than trusted.

A mutant is KILLED if at least one test fails AND the named killer is among
the failures. Zero failures is a SURVIVOR: the suite does not pin that
behaviour.

WHY THIS FILE IS THE MOST IMPORTANT MUTATION TABLE IN THE PLAN
--------------------------------------------------------------
Task 9 is where the paper's headline claim is decided. Every defect in
`controls.py` is SILENT by construction: a control that is subtly on the wrong
scale, or that permutes the wrong axis, still emits finite probabilities in
range, with the right shape and the right axes. There is no downstream
consistency check that could catch it -- the sweep simply reports a smaller
gap and the paper reports a weaker or stronger effect than exists. So the
mutants below concentrate on the failure modes that produce a plausible wrong
number rather than an error:

* THE GRID (C5-C10). Every control must read the prior through
  `PriorTensor.dense()`, the one definition of "an unstored context reads the
  floor". A control that re-derives the grid locally answers every unsurveyed
  context with a hard 0.0 while the sweep answers it with FLOOR, which puts
  the control below the full prior's scale by exactly the floor mass.
* SCALE (T9-R2). C11-C22 reintroduce, one at a time, every spelling that
  rescales a control away from the full prior: `1 / n_species`, the
  `per_species / total` renormalisation, a median, a max, a sum, a per-cell
  denominator, and -- the two the FLOOR-for-missing reversal makes newly
  possible -- an `nnz` denominator in place of the dense capacity.
* AXIS (T9-R3/T9-R4). C23-C33 permute the wrong axis, permute per-species
  instead of once, collapse the two shuffles into one, drop the seed, or
  reshape against a transposed stride.
* THE INTERACTION (M1). C50-C62 cover the fifth control, `separable`, whose
  entire definition is WHICH AXIS each marginal is taken over: they drop it
  from the registry, bypass the grid, swap the two marginals' axes, drop or
  pool the normalisation, make it the identity, and remove, invert or SILENTLY
  CLAMP its above-one refusal. C57 is the important one -- the identity
  satisfies every preservation property this control claims.

THE TWO REGRESSION MUTANTS (C3, C4) ARE THE POINT OF THIS REVISION
-------------------------------------------------------------------
An earlier revision of `controls.py` REFUSED any tensor with `nnz < size`, on
the then-current premise that an absent context was a structural zero. Under
`tensor.py`'s FLOOR-for-missing semantics that refusal makes all four controls
raise on every realistic prior -- and the whole of `test_controls.py` stayed
GREEN, because every fixture in it was dense. A dense-only fixture set cannot
see a defect that only fires on sparse input.

C3 re-introduces the dense-grid refusal and C4 the stored-zero refusal, each
killed by a test that runs a control on a 1.92%-full tensor. They are the
mutants that make that class of defect unable to return unnoticed, and they
are the reason `_sparse_tensor()` is not an optional extra fixture.

COMPOUND CONDITIONS ARE MUTATED PER DISJUNCT
--------------------------------------------
`_control_grid` carries TWO independent guard clauses, and each gets its own
mutants with its own fixture violating only that clause:

* the control-of-a-control check (`spec.variant == "full"`): removed (C39),
  inverted so the full prior is refused (C40), and with the message naming the
  wrong variant (C41);
* the column-count / cell-axis agreement: removed (C1) and inverted so a valid
  tensor is rejected (C2).

The two clauses this function used to carry -- the dense-grid and stored-zero
refusals -- are gone; C3/C4 pin their ABSENCE instead, which is the mirror
obligation.

THE THREE DEFECTS THIS REVISION OF THE TABLE EXISTS FOR
-------------------------------------------------------
C39-C41 close the one stated contract that had no guard: every control
accepted a CONTROL as input and relabelled the output, so a
`shuffle_space`-of-`uniform` reached the results table claiming to be a
first-order spatial null.

C42-C46 close two "test that cannot fail" holes that the previous three-cell
fixture opened. `np.random.default_rng(1).permutation(3)` is the IDENTITY, so
the two tests seeded with 1 compared an array to itself and the no-op mutant
survived them; and `np.random.default_rng(3).permutation(3)` is its own
INVERSE, so the one test that recomputes the permutation could not tell a
gather from a scatter and the argsort mutant survived ALL 78 tests in the
file. The root-cause fix is a five-cell axis plus an asserted precondition in
every seeded test; C42/C43 (identity, one per formerly-tautological test) and
C44/C45 (scatter instead of gather, both axes) are what keep them closed.

C47/C48 measure marginal's CONTRAST, in both directions. Every previous
assertion about `marginal` compared it against `dense.mean(axis=1)`
recomputed from the same grid, so a reduction that systematically squeezed or
amplified the between-species ratio while preserving total mass exactly was
invisible.

WHAT THIS SET DOES NOT COVER -- stated so the table is not read as more than
it is
---------------------------------------------------------------------------
* WHETHER THESE FIVE ARE THE RIGHT CONTROLS. Every mutant here asks whether
  each control computes what it claims. That the set {uniform, marginal,
  shuffle_space, shuffle_time, separable} is SUFFICIENT to establish a
  spatiotemporal effect is a claim about experimental design, not about this
  module, and no mutation of it can speak to it. `separable` was added because
  finding M1 showed the previous four could not separate the joint space x time
  term from the two marginals acting independently -- and nothing in this table
  would have noticed its absence. A reviewer could reasonably demand a SIXTH
  (e.g. a phylogenetically-matched species swap), and nothing here would notice
  that either.
* THE SWEEP THAT CONSUMES THEM. `CONTROLS` is pinned as a registry of five
  callables with matching variant labels, and the pipeline tests prove
  `build_prior.py` writes all five to disk, but nothing here proves the sweep
  actually RUNS all five in every cell (the spec's "run in every cell"), nor
  that it compares them against the full prior on the same fold. A sweep that
  silently skipped `marginal` would pass every test in this file. That
  obligation lives downstream and is only as strong as its own tests.
* THE FLOORING IS PERMUTATION-INVARIANT ON THE VALUES, SO THE SHUFFLES SEE IT
  ONLY IN WHAT THEY STORE. Mapping "unstored" to FLOOR commutes with a
  permutation of the grid, so for `shuffle_space` and `shuffle_time` reading
  `t.values.toarray()` instead of `t.dense()` produces DENSE VALUES that are
  byte-identical. What it does NOT produce is the same stored matrix: `_respec`
  re-sparsifies, so a bypassed shuffle keeps only the originally-stored entries
  while the real one keeps every context. C9/C10 are therefore killed by an
  `nnz == dense_capacity` assertion -- the flooring they lose, not merely the
  column-count guard they skip. The value-level invariance remains a theorem
  about the CURRENT construction: it would stop holding the moment a shuffle
  stopped being a pure permutation (a partial shuffle, a within-block shuffle,
  a resample), and at that point the values would start differing too.
* THE CONTRAST DECAY IS MEASURED ON A FIXTURE, NOT ON THE ARTIFACT. C47/C48
  pin that `marginal` reproduces a known 4:1 ratio to within 1% at 1.92% fill
  with observed values of order 1e-01. The decay is a function of
  `fill * value` against FLOOR, so at real national sparsity the loss is
  larger than anything measured here -- the fixture pins the reduction, not
  the artifact's contrast. That obligation belongs to the build script.
* FLOATING-POINT MASS AT NATIONAL SCALE. The mass-preservation tests compare
  with `pytest.approx` on a 520-entry matrix. `mean * size == sum` is exact
  only up to summation order; at ~10^8 contexts the drift is real but bounded
  and far below any effect size the paper reports. Nothing here measures it,
  and a mutant introducing a float32 intermediate inside the mean would be
  caught only if it drifted past `approx`'s default tolerance.
* THE FLOOR'S SHARE AT REAL SCALE. `test_the_sparse_fixture_floor_does_not_
  dominate_the_uniform_fill` pins that the floor mass is a ~2.7e-4 share of the
  total AT 1.92% FILL WITH VALUES OF ORDER 1e-1. A real national grid is
  sparser still and its observed values may be smaller, so the floor's share
  is larger there than anything measured here. The judgement that `uniform`
  stays data-derived rather than collapsing to FLOOR is pinned on this
  fixture's numbers, not on the artifact's.
* THE SEED POLICY. C29/C30 prove the seed is USED and that two seeds differ,
  and the shuffles are deterministic in it. WHICH seeds the paper reports, and
  whether one shuffle replicate is enough to claim a null, is a statistical
  question this table cannot reach: a single-replicate shuffle control is weak
  evidence regardless of how correctly it is computed.
* MEMORY. Every control materialises `t.dense()`, which is
  `dense_capacity` float64s. At H3 resolution 4 over Denmark that is small; at
  national resolution 7 with 10^3 species it is ~1 GB, and nothing here has a
  peak-allocation test (the smoothing harness does, S31). A control that OOMs
  is a loud failure, not a silent one, which is why it is out of scope rather
  than untested by oversight -- but it is a real operational limit and the
  controls are now the only stage that pays it.
"""
# ---------------------------------------------------------------------------
# TEST SCOPE (final-gate finding I1)
# ---------------------------------------------------------------------------
# Every mutant is confronted with the WHOLE `tests/prior` directory, not just
# this module's own test file. Before the change each harness set `TESTS` to
# one file, so any contract whose violation is observable only in a DOWNSTREAM
# CONSUMER was invisible -- proved on `indexing.py`, where making the guard
# marker re-stamp unconditional passed all 21 of `test_indexing.py` and was
# killed only by `test_effort.py`.
#
# Measured cost per mutant (wall clock, `uv run pytest -q`, includes uv
# startup): one file 0.19-0.70s, whole directory 1.27s for 730 tests. The
# directory is chosen over a derived per-target consumer set because the
# difference is under a second per mutant and a hand-maintained consumer list
# is a guess that rots. The consumer graph WAS derived (`grep` of every
# `from birdology.prior...` and `from .` import under src/, scripts/ and
# tests/prior) and is recorded below for what the widening buys, not as a
# filter:
#
#   indexing   -> backoff, controls, smoothing, tensor, effort(runtime),
#                 leakage(tests), build_prior      [WEEKS_PER_YEAR + the marker
#                                                   re-stamp + aggregate_counts]
#   leakage    -> indexing, effort, build_prior
#   backoff    -> tensor (FLOOR), build_prior
#   tensor     -> controls, build_prior
#   effort     -> backoff(tests), build_prior
#   smoothing  -> build_prior
#   controls   -> build_prior
#   occurrences-> indexing(tests), build_prior
#
# `TESTS` below is now this module's OWN test file and is NOT the scope: the
# runner uses it only to report which mutants are pinned ONLY from outside it.
# `--tests tests/prior/test_x.py` reproduces the old narrow behaviour, and
# exists so the cost of the widening can be re-measured.
# ---------------------------------------------------------------------------

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _harness  # noqa: E402  -- sibling module; this file runs as a script

REPO = _harness.REPO
TARGET = Path("src/birdology/prior/controls.py")
TESTS = "tests/prior/test_controls.py"

# (id, description, killer_test, [(old, new), ...])
#
# `killer_test` is the test whose failure DEMONSTRATES the mutant is caught.
# It is verified at runtime: a mutant that fails only other tests is reported
# MISLABELLED rather than counted as a kill.
MUTANTS: list[tuple[str, str, str, list[tuple[str, str]]]] = [
    # ---- T9-R4: the reshape needs n_cols == n_cells * WEEKS_PER_YEAR -------
    (
        "C1",
        "the COLUMN-COUNT agreement check removed, so a cell axis that "
        "disagrees with the matrix reshapes into a WRONG block structure (or "
        "dies on an opaque numpy message naming no contract) and "
        "`shuffle_space` permutes something that is not a cell",
        "test_a_column_count_that_disagrees_with_the_cell_axis_is_rejected",
        [("    if n_cols != expected_cols:", "    if False:")],
    ),
    (
        "C2",
        "the expected column count computed with a hard-coded 53 instead of "
        "WEEKS_PER_YEAR, so a correctly built tensor is REJECTED -- the "
        "mirror of C1, and the ISO-53 spelling this plan folds away",
        "test_the_column_count_guard_does_not_raise_on_a_valid_tensor",
        [
            (
                "    expected_cols = len(t.cells) * WEEKS_PER_YEAR",
                "    expected_cols = len(t.cells) * 53",
            )
        ],
    ),
    # ---- THE REGRESSION PAIR: the refusals that must never come back -------
    (
        "C3",
        "the DENSE-GRID REFUSAL re-introduced. This is the live defect this "
        "revision fixes: under FLOOR-for-missing there are no structural "
        "zeros left to refuse, so the guard makes all four controls RAISE on "
        "every realistic prior and the paper's credibility section fails "
        "before it computes anything. It survived the previous suite entirely "
        "because every fixture in it was dense",
        "test_every_control_runs_on_a_realistically_sparse_prior",
        [
            (
                "    return t.dense()",
                "    _size = t.values.get_shape()[0] * n_cols\n"
                "    if t.values.nnz != _size:\n"
                "        raise ValueError(\n"
                '            f"the {where} control requires a DENSE grid, but this "\n'
                '            f"tensor stores {t.values.nnz} of {_size} contexts"\n'
                "        )\n"
                "    return t.dense()",
            )
        ],
    ),
    (
        "C4",
        "the STORED-ZERO REFUSAL re-introduced. Separate from C3 because a "
        "sparse fixture does not exercise it: an explicit stored 0.0 passes "
        "any nnz count. `tensor.py`'s `_floor_missing` maps it to FLOOR "
        "exactly as it maps an unstored context, so refusing here would make "
        "the controls stricter than the boundary they read through, on a "
        "distinction that boundary has deliberately erased",
        "test_a_stored_explicit_zero_reads_as_the_floor_rather_than_being_refused",
        [
            (
                "    return t.dense()",
                "    if t.values.count_nonzero() != t.values.nnz:\n"
                "        raise ValueError(\n"
                '            f"the {where} control was given a tensor storing an "\n'
                '            "explicit zero"\n'
                "        )\n"
                "    return t.dense()",
            )
        ],
    ),
    # ---- the grid itself: one definition of the flooring, upstream ---------
    (
        "C5",
        "`_control_grid` re-derives the grid LOCALLY with `t.values.toarray()` "
        "instead of returning `t.dense()`. Every unsurveyed context then reads "
        "a hard 0.0 here while the sweep reads it as FLOOR, so the controls "
        "sit below the full prior's scale by exactly the floor mass. On a "
        "fully stored fixture the two are the SAME ARRAY, which is why only "
        "the sparse fixture can see this",
        "test_uniform_on_a_sparse_prior_fills_with_the_mean_over_the_WHOLE_grid",
        [
            (
                "    return t.dense()",
                "    return np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    (
        "C6",
        "`_control_grid` RE-IMPLEMENTS the flooring locally with a drifted "
        "constant (1e-09 instead of `backoff.FLOOR`). A second definition of "
        "an upstream value -- the defect this plan has found most often -- and "
        "it looks entirely correct at the call site",
        "test_every_control_preserves_the_sparse_priors_total_mass",
        [
            (
                "    return t.dense()",
                "    _raw = np.asarray(t.values.toarray(), dtype=np.float64)\n"
                "    return np.where(_raw == 0.0, 1e-09, _raw)",
            )
        ],
    ),
    (
        "C7",
        "`uniform_prior` bypasses `_control_grid` entirely -- so the control "
        "that DEFINES the scale is the one that loses both the flooring and "
        "the column-count guard",
        "test_uniform_on_a_sparse_prior_fills_with_the_mean_over_the_WHOLE_grid",
        [
            (
                '    dense = _control_grid(t, "uniform")',
                "    dense = np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    (
        "C8",
        "`marginal_prior` bypasses `_control_grid` -- THE control, the one a "
        "reviewer will demand. Each species is then rescaled by its OWN fill, "
        "so the control reports a label frequency that is really a "
        "survey-effort frequency",
        "test_marginal_on_a_sparse_prior_preserves_each_species_own_mass",
        [
            (
                '    dense = _control_grid(t, "marginal")',
                "    dense = np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    (
        "C9",
        "`shuffle_space` bypasses `_control_grid`, losing the flooring before "
        "a permutation AND the column-count guard before a reshape. The "
        "flooring is permutation-invariant, so the DENSE VALUES it produces "
        "are byte-identical -- but `_respec` re-sparsifies, so a bypassed "
        "shuffle stores only the originally-stored entries where the real one "
        "stores every context. That `nnz` difference is what makes the "
        "single-definition rule falsifiable here rather than merely "
        "load-bearing-in-principle",
        "test_the_shuffles_store_every_context_so_a_grid_bypass_cannot_hide",
        [
            (
                '    dense = _control_grid(t, "shuffle_space")',
                "    dense = np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    (
        "C10",
        "`shuffle_time` bypasses `_control_grid` -- the time mirror of C9",
        "test_the_shuffles_store_every_context_so_a_grid_bypass_cannot_hide",
        [
            (
                '    dense = _control_grid(t, "shuffle_time")',
                "    dense = np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    # ---- T9-R2: SCALE. Each of these is the defect that would decide the
    # ---- paper's headline comparison, and each is individually plausible.
    (
        "C11",
        "the brief's own spelling restored: `uniform_prior` fills with "
        "`1 / n_species`. Every context then sums to exactly 1.0 -- a "
        "renormalisation of a prior that is never renormalised at any stage. "
        "Every fusion method combines prior and posterior multiplicatively or "
        "via `log prior`, so the comparison measures a scale difference and "
        "reports it as a context effect",
        "test_uniform_preserves_the_full_priors_total_mass",
        [
            (
                "    fill = float(dense.mean())",
                "    fill = 1.0 / max(dense.shape[0], 1)",
            )
        ],
    ),
    (
        "C12",
        "`uniform_prior` fills with the MEDIAN instead of the mean. Looks like "
        "a robustness improvement and is not mass-preserving: on a skewed "
        "prior -- which an occurrence prior always is -- the median is far "
        "below the mean, so the control sits systematically low",
        "test_uniform_preserves_the_full_priors_total_mass",
        [
            (
                "    fill = float(dense.mean())",
                "    fill = float(np.median(dense))",
            )
        ],
    ),
    (
        "C13",
        "`uniform_prior` fills with the MAXIMUM, so the control sits far above "
        "the full prior's scale. The opposite sign of C12, and a separate "
        "mutant because a test that catches a low fill need not catch a high "
        "one",
        "test_uniform_preserves_the_full_priors_total_mass",
        [
            (
                "    fill = float(dense.mean())",
                "    fill = float(dense.max())",
            )
        ],
    ),
    (
        "C14",
        "`uniform_prior` fills with the mean over ONE CELL's block rather than "
        "over the whole matrix. Still flat across species and across "
        "contexts, still finite and in range, and mass-preserving only if "
        "every cell happens to have the same mass -- which is exactly the "
        "spatial structure the prior exists to capture",
        "test_uniform_fill_is_the_mean_over_the_whole_matrix",
        [
            (
                "    fill = float(dense.mean())",
                "    fill = float(dense[:, :WEEKS_PER_YEAR].mean())",
            )
        ],
    ),
    (
        "C15",
        "`uniform_prior` divides by `nnz` instead of by the dense capacity -- "
        "THE scale defect the FLOOR-for-missing reversal makes newly "
        "available, and the one a reader who still thinks in structural zeros "
        "would reach for. It inflates the control by dense_capacity/nnz: ~52x "
        "on this fixture and far more at national scale, which the results "
        "table would report as an enormous spatiotemporal effect. On a fully "
        "stored fixture it is EXACTLY the correct value",
        "test_uniform_on_a_sparse_prior_is_not_the_mean_over_stored_entries",
        [
            (
                "    fill = float(dense.mean())",
                "    fill = float(dense.sum() / max(t.values.nnz, 1))",
            )
        ],
    ),
    (
        "C16",
        "the brief's `per_species / total` renormalisation restored in "
        "`marginal_prior`. The divisor depends on HOW MANY SPECIES the tensor "
        "holds, so the control's scale becomes a function of the label set "
        "and the label-frequency null is no longer comparable to the full "
        "prior it is the null for",
        "test_marginal_preserves_the_full_priors_total_mass",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)\n",
                "    per_species = dense.mean(axis=1, keepdims=True)\n"
                "    _total = per_species.sum()\n"
                "    if _total > 0:\n"
                "        per_species = per_species / _total\n",
            )
        ],
    ),
    (
        "C17",
        "`marginal_prior` uses each species' SUM over contexts instead of its "
        "mean, inflating the control by the number of contexts. Preserves the "
        "RANKING of species exactly -- so every label-frequency assertion "
        "still holds -- while being wrong about scale by a factor of "
        "n_cells * 52",
        "test_marginal_preserves_the_full_priors_total_mass",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)",
                "    per_species = dense.sum(axis=1, keepdims=True)",
            )
        ],
    ),
    (
        "C18",
        "`marginal_prior` uses each species' MEDIAN over contexts. Preserves "
        "the species ranking and is not mass-preserving on a skewed prior. "
        "Separate mutant from C17 because a sum is caught by magnitude while "
        "a median is caught only by a fixture whose mean and median differ",
        "test_marginal_preserves_the_full_priors_total_mass",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)",
                "    per_species = np.median(dense, axis=1, keepdims=True)",
            )
        ],
    ),
    (
        "C19",
        "`marginal_prior` uses the GLOBAL mean for every species, collapsing "
        "it into `uniform_prior`. Total mass is still preserved exactly, so "
        "the mass tests cannot see it -- but the label-frequency signal the "
        "control exists to retain is gone, and a gain would then appear to "
        "survive the marginal control for the wrong reason",
        "test_marginal_still_varies_across_species",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)",
                "    per_species = np.full((dense.shape[0], 1), dense.mean())",
            )
        ],
    ),
    (
        "C20",
        "`marginal_prior` takes each species' mean over ONE CELL's block "
        "rather than over every context, so the control keeps that cell's "
        "spatial signal. Per-species mass is wrong and the control is no "
        "longer space-free -- the one property its name promises",
        "test_marginal_preserves_each_species_own_mass",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)",
                "    per_species = dense[:, :WEEKS_PER_YEAR].mean(axis=1, keepdims=True)",
            )
        ],
    ),
    (
        "C21",
        "`marginal_prior` averages over SPECIES instead of over contexts "
        "(axis=0), so the control varies with cell and week and is constant "
        "across species -- the two properties exactly inverted, while the "
        "shape, the range and the total mass all stay plausible",
        "test_marginal_ignores_cell_and_week",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)\n"
                '    return _respec(t, "marginal", np.tile(per_species, (1, dense.shape[1])))',
                "    _per_context = dense.mean(axis=0, keepdims=True)\n"
                '    return _respec(t, "marginal", np.tile(_per_context, (dense.shape[0], 1)))',
            )
        ],
    ),
    (
        "C22",
        "`marginal_prior` divides each species' mass by ITS OWN stored-entry "
        "count rather than by the number of contexts -- the per-species twin "
        "of C15. The control then reports each species scaled by how much "
        "anyone LOOKED for it, which is survey effort wearing label "
        "frequency's clothes, and survey effort is precisely the spatial "
        "signal the marginal control exists to remove",
        "test_marginal_on_a_sparse_prior_preserves_each_species_own_mass",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)",
                "    _per_row_nnz = np.maximum(np.diff(t.values.indptr), 1).reshape(-1, 1)\n"
                "    per_species = dense.sum(axis=1, keepdims=True) / _per_row_nnz",
            )
        ],
    ),
    # ---- T9-R3: the axes, the shared permutation, the seed ----------------
    (
        "C23",
        "`shuffle_space` permutes the WEEK axis, making it a duplicate of "
        "`shuffle_time`. Two of the four controls then run the same "
        "experiment and the space null is never tested at all -- while every "
        "value-multiset and mass assertion still passes",
        "test_shuffle_space_relocates_whole_cells_not_individual_values",
        [
            (
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
            )
        ],
    ),
    (
        "C24",
        "`shuffle_time` permutes the CELL axis -- the mirror of C23, and a "
        "separate mutant because one collapse cannot detect the other",
        "test_shuffle_time_permutes_week_slots_with_one_shared_permutation",
        [
            (
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
            )
        ],
    ),
    (
        "C25",
        "`shuffle_space` becomes a NO-OP: the permutation replaced by "
        "`arange`. The value multiset, the total mass and the determinism "
        "tests all pass unchanged -- which is precisely why T9-R3 demands a "
        "not-the-identity test. A no-op control reports the FULL prior's "
        "performance as the spatial null, so any real spatial effect "
        "disappears from the paper",
        "test_shuffle_space_actually_moves_a_cell",
        [
            (
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    blocks = blocks[:, np.arange(n_cells), :]",
            )
        ],
    ),
    (
        "C26",
        "`shuffle_time` becomes a NO-OP -- the time mirror of C25",
        "test_shuffle_time_actually_moves_a_week",
        [
            (
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    blocks = blocks[:, :, np.arange(WEEKS_PER_YEAR)]",
            )
        ],
    ),
    (
        "C27",
        "`shuffle_space` draws a FRESH permutation per species instead of "
        "sharing one (T9-R3). It also destroys which species co-occur in a "
        "cell, so a gain could fail this control for a reason that has "
        "nothing to do with space -- and the per-species value multisets, the "
        "mass and the determinism are all still intact",
        "test_shuffle_space_relocates_whole_cells_not_individual_values",
        [
            (
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    _rng = np.random.default_rng(seed)\n"
                "    blocks = np.stack(\n"
                "        [blocks[s][_rng.permutation(n_cells), :] for s in range(blocks.shape[0])]\n"
                "    )",
            )
        ],
    ),
    (
        "C28",
        "`shuffle_time` draws a fresh permutation per CELL rather than sharing "
        "one across cells, so each cell's weeks are scrambled independently. "
        "Every per-cell multiset and every mass assertion still holds",
        "test_shuffle_time_permutes_week_slots_with_one_shared_permutation",
        [
            (
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    _rng = np.random.default_rng(seed)\n"
                "    blocks = np.stack(\n"
                "        [\n"
                "            np.stack(\n"
                "                [blocks[s, c][_rng.permutation(WEEKS_PER_YEAR)] for c in range(n_cells)]\n"
                "            )\n"
                "            for s in range(blocks.shape[0])\n"
                "        ]\n"
                "    )",
            )
        ],
    ),
    (
        "C29",
        "the SEED IGNORED in `shuffle_space` (a fixed default), so every "
        "replicate of the spatial control is the identical permutation and a "
        "sweep over seeds reports one draw as many",
        "test_shuffle_space_uses_its_seed",
        [
            (
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    blocks = blocks[:, np.random.default_rng(0).permutation(n_cells), :]",
            )
        ],
    ),
    (
        "C30",
        "the seed ignored in `shuffle_time` -- the time mirror of C29",
        "test_shuffle_time_uses_its_seed",
        [
            (
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    blocks = blocks[:, :, np.random.default_rng(0).permutation(WEEKS_PER_YEAR)]",
            )
        ],
    ),
    (
        "C31",
        "`shuffle_space` reshaped against a TRANSPOSED (week, cell) stride "
        "rather than (cell, week). True-looking and a silent layout drift: "
        "the 'cell' it permutes is a stride through different weeks",
        "test_shuffle_space_relocates_whole_cells_not_individual_values",
        [
            (
                "    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)\n"
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    blocks = dense.reshape(dense.shape[0], WEEKS_PER_YEAR, n_cells)\n"
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(n_cells)]",
            )
        ],
    ),
    (
        "C32",
        "`shuffle_time` reshaped with the cell and week axes TRANSPOSED, so "
        "the 'week' it permutes is actually a stride through different cells "
        "and values cross cell boundaries -- a time control that is also a "
        "space control, so a failure cannot be attributed to either",
        "test_shuffle_time_keeps_every_value_inside_its_own_cell",
        [
            (
                "    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)\n"
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    blocks = dense.reshape(dense.shape[0], WEEKS_PER_YEAR, n_cells)\n"
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR), :]",
            )
        ],
    ),
    (
        "C33",
        "`shuffle_space` permutes the SPECIES axis instead of the cell axis. "
        "The tensor's total mass and its global value multiset are both "
        "unchanged, and every context still holds real probabilities -- but "
        "the control now tests label assignment rather than space",
        "test_shuffle_space_relocates_whole_cells_not_individual_values",
        [
            (
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    blocks = blocks[np.random.default_rng(seed).permutation(blocks.shape[0]), :, :]",
            )
        ],
    ),
    # ---- the axes, the labels and the provenance the sweep reads ----------
    (
        "C34",
        "`_respec` keeps the INPUT's variant, so every control artifact claims "
        "to be the full prior. A sweep over variants would then attribute "
        "every control's result to the full prior and the results table would "
        "be silently uninterpretable",
        "test_every_registered_control_labels_its_own_variant",
        [
            (
                "        spec=PriorSpec(t.spec.resolution, t.spec.source, variant),",
                "        spec=PriorSpec(t.spec.resolution, t.spec.source, t.spec.variant),",
            )
        ],
    ),
    (
        "C35",
        "`_respec` case-folds the cell axis -- a second normalisation of a "
        "value derived exactly once upstream. Every lookup by a real "
        "lower-case H3 cell still hits here, but an upper-case release would "
        "miss, and the sweep would read a total miss as the control scoring "
        "badly rather than as a bug",
        "test_uniform_keeps_the_axes_byte_for_byte",
        [
            (
                "        cells=t.cells,",
                "        cells=[c.upper() for c in t.cells],",
            )
        ],
    ),
    (
        "C36",
        "the GBIF DOI dropped from every control tensor, so a released "
        "control artifact cannot be traced to the download it was built from "
        "-- the provenance a Datasets & Benchmarks submission must carry",
        "test_uniform_carries_the_provenance_of_the_prior_it_controls",
        [
            (
                "        gbif_doi=t.gbif_doi,",
                '        gbif_doi="",',
            )
        ],
    ),
    (
        "C37",
        "the `marginal` entry dropped from the registry, so the ONE control a "
        "reviewer is guaranteed to demand is never run by a sweep that "
        "iterates `CONTROLS`. The function still exists and still passes its "
        "own tests",
        "test_controls_registry_covers_all_five",
        [
            (
                '    "marginal": marginal_prior,\n',
                "",
            )
        ],
    ),
    (
        "C38",
        "the registry maps `shuffle_time` to `shuffle_space`, so the time null "
        "is never evaluated and its row in the results table is a second copy "
        "of the space null -- which no shape, range or mass check can detect",
        "test_every_registered_control_labels_its_own_variant",
        [
            (
                '    "shuffle_time": shuffle_time,',
                '    "shuffle_time": shuffle_space,',
            )
        ],
    ),
    # ---- C1 (the FINDING, not the mutant id): a control of a control -------
    (
        "C39",
        "the CONTROL-OF-A-CONTROL guard removed, so `uniform_prior(uniform_"
        "prior(t))` and all fifteen other compositions are accepted and "
        "RELABELLED with the outer variant. Every composition is "
        "mass-preserving, finite, in range, right-shaped and right-axed -- a "
        "`shuffle_space`-of-`uniform` is simply `uniform` again -- so the "
        "results table gains a row that claims to be a first-order spatial "
        "null and nothing downstream can contradict it, because the only "
        "evidence was the variant string this module overwrote",
        "test_a_control_of_a_control_is_refused",
        [('    if t.spec.variant != "full":', "    if False:")],
    ),
    (
        "C40",
        "the control-of-a-control guard INVERTED, so the FULL prior is the one "
        "input refused and every control raises on the only tensor it is "
        "defined for -- the mirror of C39, and the same shape of defect as the "
        "dense-grid refusal C3 re-introduces: a guard that takes the paper's "
        "credibility section down before it computes anything",
        "test_the_full_prior_itself_is_not_refused",
        [('    if t.spec.variant != "full":', '    if t.spec.variant == "full":')],
    ),
    (
        "C41",
        "the control-of-a-control message reports the OUTER control's name in "
        "place of the variant it actually received, so a caller holding four "
        "saved artifacts is told which function raised and not which file they "
        "passed in -- and the failure mode this guard exists for is precisely "
        "a loop that lost track of which artifact it was holding",
        "test_the_control_of_a_control_message_names_the_variant_it_received",
        [
            (
                "            f\"already the {t.spec.variant!r} control: a control of a control \"",
                "            f\"already the {where!r} control: a control of a control \"",
            )
        ],
    ),
    # ---- I1/I2: the two holes the previous revision's 3-cell axis opened ---
    (
        "C42",
        "`shuffle_space` becomes a NO-OP, labelled by the multiset test that "
        "USED TO SURVIVE it. On the previous 3-cell axis that test's seed drew "
        "the identity permutation, so it compared an array to itself and this "
        "exact mutation passed it. The fixture is now five cells with an "
        "asserted non-identity seed and an explicit \"the array changed\" "
        "assertion, so the tautology is closed at the cause",
        "test_shuffle_space_preserves_the_value_multiset",
        [
            (
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    blocks = blocks[:, np.arange(n_cells), :]",
            )
        ],
    ),
    (
        "C43",
        "`shuffle_space` becomes a NO-OP, labelled by the total-mass test that "
        "also used to survive it -- the second of the two seed-1 tautologies, "
        "and a separate mutant because each must kill this mutation on its own "
        "rather than leaning on a neighbour in the same file",
        "test_shuffle_space_preserves_total_mass",
        [
            (
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    blocks = blocks[:, np.arange(n_cells), :]",
            )
        ],
    ),
    (
        "C44",
        "`shuffle_space` SCATTERS where it should gather: the cell axis indexed "
        "by the INVERSE permutation (`np.argsort`). Both directions are "
        "statistically valid shuffles, so this is not wrong in itself -- but "
        "it means the implementation is not the construction the tests claim, "
        "and on the previous 3-cell axis the only test that recomputes the "
        "permutation was seeded with an INVOLUTION, on which gather and "
        "scatter are the same array. This mutant SURVIVED all 78 tests. The "
        "five-cell axis, the asserted not-its-own-inverse precondition and the "
        "mirror assertion against `argsort` close it",
        "test_shuffle_space_relocates_whole_cells_not_individual_values",
        [
            (
                "    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]",
                "    blocks = blocks[:, np.argsort(np.random.default_rng(seed).permutation(n_cells)), :]",
            )
        ],
    ),
    (
        "C45",
        "`shuffle_time` scatters where it should gather -- the time mirror of "
        "C44. The 52-element week axis never had the space axis's involution "
        "problem, but the axis length was the only thing standing between this "
        "mutant and survival, and an axis length is not an assertion",
        "test_shuffle_time_permutes_week_slots_with_one_shared_permutation",
        [
            (
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    blocks = blocks[:, :, np.argsort(np.random.default_rng(seed).permutation(WEEKS_PER_YEAR))]",
            )
        ],
    ),
    (
        "C46",
        "`shuffle_time` becomes a NO-OP, labelled by the multiset test -- the "
        "time twin of C42. The week axis was never the one with the identity "
        "seed, which is exactly why its precondition needed asserting rather "
        "than inferring",
        "test_shuffle_time_preserves_the_value_multiset",
        [
            (
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    blocks = blocks[:, :, np.arange(WEEKS_PER_YEAR)]",
            )
        ],
    ),
    # ---- I4: marginal's CONTRAST, the only signal it exists to carry -------
    (
        "C47",
        "`marginal_prior` SHRINKS each species' mean halfway toward the global "
        "mean -- the shape of a plausible \"stabilise a sparse estimate\" "
        "improvement. TOTAL mass is preserved EXACTLY (the rows are all the "
        "same length, so the average of the shrunk means is the global mean), "
        "the control still varies across species, every value is finite and in "
        "range -- and the 4:1 label-frequency ratio the control exists to "
        "carry is reported as 1.86:1. Without a contrast measurement against a "
        "KNOWN ratio, the only tests that could see this compare against "
        "`dense.mean(axis=1)` recomputed from the same grid",
        "test_marginal_reproduces_a_known_species_mass_ratio_on_a_sparse_prior",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)",
                "    per_species = 0.5 * dense.mean(axis=1, keepdims=True) + 0.5 * dense.mean()",
            )
        ],
    ),
    (
        "C48",
        "`marginal_prior` AMPLIFIES the contrast instead, squaring each "
        "species' mean and rescaling so total mass is preserved exactly. The "
        "opposite sign of C47 and a separate mutant because a test that "
        "catches a squeezed ratio need not catch a manufactured one -- and a "
        "control reporting MORE label frequency than the data holds makes a "
        "real gain look like it failed the marginal null",
        "test_marginal_reproduces_a_known_species_mass_ratio_on_a_sparse_prior",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)",
                "    _m = dense.mean(axis=1, keepdims=True)\n"
                "    per_species = _m**2 * (_m.sum() / (_m**2).sum())",
            )
        ],
    ),
    (
        "C49",
        "`marginal_prior` shrinks each species' mean 0.5% toward the global "
        "mean -- the same defect class as C47 but an order of magnitude "
        "smaller, sized DELIBERATELY to slip through the fixture's 1% "
        "tolerance. Total mass is preserved exactly and the fixture contrast "
        "test (`rel=0.01` on a 4:1 ratio) still passes. It dies only to the "
        "PIPELINE contrast test, which asserts EXACT equality because a fully "
        "materialised artifact has no floor mass to excuse any loss. This is "
        "the mutant that makes finding C2's correction load-bearing rather "
        "than merely accurate: before the pipeline test existed, a systematic "
        "sub-percent contrast loss on every released artifact was invisible",
        "test_marginal_contrast_is_EXACT_on_a_pipeline_artifact",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)",
                "    per_species = 0.995 * dense.mean(axis=1, keepdims=True) "
                "+ 0.005 * dense.mean()",
            )
        ],
    ),
    # ---- M1: `separable`, the fifth control -------------------------------
    (
        "C50",
        "the `separable` entry dropped from the registry, so the ONE control "
        "that isolates the space x time interaction is never run by a sweep "
        "that iterates `CONTROLS` -- and `build_prior.py` iterates it, so the "
        "artifact simply never appears on disk. The function still exists and "
        "still passes every test that calls it directly. This is the M1 "
        "finding restored: no control in the remaining four separates the "
        "joint term from the two marginals acting independently",
        "test_controls_registry_covers_all_five",
        [('    "separable": separable_prior,\n', "")],
    ),
    (
        "C51",
        "`separable_prior` bypasses `_control_grid`, losing the flooring "
        "before the outer product AND the column-count guard before the "
        "reshape -- the fifth control's version of C7-C10. On a sparse prior "
        "every unsurveyed context reads a hard 0.0, so the cell and week "
        "marginals are computed over a grid the sweep never sees and the "
        "control lands below the full prior's scale",
        "test_a_column_count_that_disagrees_with_the_cell_axis_is_rejected",
        [
            (
                '    dense = _control_grid(t, "separable")',
                "    dense = np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    (
        "C52",
        "`separable_prior` computes the WEEK marginal over the week axis "
        "instead of the cell axis (`axis=2` twice), so the surrogate is the "
        "square of the cell marginal over the grand mean: CONSTANT in the "
        "week, which destroys the week marginal it exists to preserve. Every "
        "value stays finite, positive and plausible, and the CELL marginal is "
        "still right, so half the contract still holds",
        "test_separable_preserves_every_species_WEEK_marginal_exactly",
        [
            (
                "    week_mean = blocks.mean(axis=1, keepdims=True)",
                "    week_mean = blocks.mean(axis=2, keepdims=True)",
            )
        ],
    ),
    (
        "C53",
        "`separable_prior` computes the CELL marginal over the cell axis "
        "instead of the week axis -- the mirror of C52, and a separate mutant "
        "because a test that catches a destroyed week marginal need not catch "
        "a destroyed cell marginal. The surrogate becomes constant across "
        "cells, which is `shuffle_space`'s target rather than this control's",
        "test_separable_preserves_every_species_CELL_marginal_exactly",
        [
            (
                "    cell_mean = blocks.mean(axis=2, keepdims=True)",
                "    cell_mean = blocks.mean(axis=1, keepdims=True)",
            )
        ],
    ),
    (
        "C54",
        "the `/ grand_mean` normalisation DROPPED from the outer product. The "
        "surrogate is then a bare product of two means, so every value is "
        "wrong by a factor of the grand mean -- which SHRINKS the control by "
        "orders of magnitude and is exactly the T9-R2 scale defect the whole "
        "module is built to prevent. Both marginals stay PROPORTIONAL to the "
        "right thing, so their shape survives and only the scale is wrong: the "
        "most plausible-looking failure in this construction",
        "test_separable_preserves_each_species_own_mass_and_the_total",
        [
            (
                "    surrogate = cell_mean * week_mean / grand_mean",
                "    surrogate = cell_mean * week_mean",
            )
        ],
    ),
    (
        "C55",
        "`separable_prior` divides by the GLOBAL mean over every species "
        "rather than by each species' own grand mean. Total mass is NOT "
        "preserved and, worse, each species is rescaled by the ratio of its "
        "own mean to the global one -- so the control silently re-weights "
        "label frequency, which is the signal `marginal` exists to isolate "
        "and this control must leave untouched",
        "test_separable_preserves_each_species_own_mass_and_the_total",
        [
            (
                "    grand_mean = blocks.mean(axis=(1, 2), keepdims=True)",
                "    grand_mean = blocks.mean()",
            )
        ],
    ),
    (
        "C56",
        "`separable_prior` pools BOTH marginals across species instead of "
        "using each species' own. Every species then gets the same spatial and "
        "the same seasonal profile, so the control carries a pooled community "
        "average rather than each species' own marginals -- and the TOTAL mass "
        "is still preserved, so no total-mass assertion can see it",
        "test_separable_uses_EACH_SPECIES_OWN_marginals_not_pooled_ones",
        [
            (
                "    cell_mean = blocks.mean(axis=2, keepdims=True)\n"
                "    week_mean = blocks.mean(axis=1, keepdims=True)\n"
                "    grand_mean = blocks.mean(axis=(1, 2), keepdims=True)",
                "    cell_mean = blocks.mean(axis=(0, 2), keepdims=True)\n"
                "    week_mean = blocks.mean(axis=(0, 1), keepdims=True)\n"
                "    grand_mean = blocks.mean()",
            )
        ],
    ),
    (
        "C57",
        "`separable_prior` becomes the IDENTITY -- it returns the full prior's "
        "own grid, relabelled `separable`. EVERY preservation property this "
        "control claims (both marginals, per-species mass, total mass, "
        "positivity, fill) holds PERFECTLY on the identity, so the entire "
        "preservation half of its test set passes. The results table then "
        "carries a row claiming the interaction was removed when the full "
        "prior was shipped under a control's label, which reads as the "
        "interaction contributing NOTHING -- the opposite of the measured "
        "20-57%",
        "test_separable_DESTROYS_the_interaction_it_exists_to_isolate",
        [
            (
                "    surrogate = cell_mean * week_mean / grand_mean",
                "    surrogate = blocks",
            )
        ],
    ),
    (
        "C58",
        "the above-one REFUSAL removed, so a prior whose marginals multiply "
        "past 1.0 produces a control holding values that are not "
        "probabilities. `save_prior` would write it -- it validates on the way "
        "IN, not on a control's way out -- and a fusion step consuming it "
        "would see a prior above one, which inflates rather than tempers the "
        "acoustic likelihood",
        "test_separable_REFUSES_a_prior_whose_outer_product_leaves_the_unit_interval",
        [("    if surrogate.max() > 1.0:", "    if False:")],
    ),
    (
        "C59",
        "the above-one refusal replaced by a SILENT CLAMP -- the no-silent-"
        "repair violation, and the shape of a well-meaning fix. A clamped "
        "surrogate reproduces NEITHER marginal and no longer preserves mass, "
        "so the control quietly stops being the null it claims to be while "
        "every value remains a plausible probability. Separate mutant from "
        "C58 because a test that catches a missing guard need not catch a "
        "guard replaced by a coercion",
        "test_separable_REFUSES_a_prior_whose_outer_product_leaves_the_unit_interval",
        [
            (
                "    if surrogate.max() > 1.0:",
                "    surrogate = np.minimum(surrogate, 1.0)\n    if False:",
            )
        ],
    ),
    (
        "C60",
        "the above-one refusal INVERTED, so every ordinary prior is rejected "
        "and only a pathological one is accepted -- the C40-shaped mirror, "
        "which takes the paper's credibility section down before it computes "
        "anything rather than corrupting a number",
        "test_separable_does_not_refuse_an_ordinary_prior",
        [("    if surrogate.max() > 1.0:", "    if surrogate.max() <= 1.0:")],
    ),
    (
        "C61",
        "the registry maps `separable` to `marginal_prior`, so the "
        "interaction-isolating control's row in the results table is a second "
        "copy of the label-frequency null. Mass-preserving, finite, in range "
        "and correctly labelled by `_respec` -- and it reports that removing "
        "the interaction destroys both marginals too, which would make the "
        "joint term look far more important than it is",
        "test_every_registered_control_labels_its_own_variant",
        [
            (
                '    "separable": separable_prior,',
                '    "separable": marginal_prior,',
            )
        ],
    ),
    (
        "C62",
        "`separable_prior` reshaped against a TRANSPOSED (week, cell) stride, "
        "so the 'cell marginal' is a stride through different weeks and the "
        "'week marginal' through different cells -- the C31/C32-shaped layout "
        "drift, on the one control whose entire definition is which axis each "
        "marginal is taken over. The output is still positive, still "
        "mass-preserving in total, and still looks like a separable surrogate",
        "test_separable_preserves_every_species_CELL_marginal_exactly",
        [
            (
                "    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)\n"
                "    # Per SPECIES, never pooled",
                "    blocks = dense.reshape(dense.shape[0], WEEKS_PER_YEAR, n_cells)\n"
                "    # Per SPECIES, never pooled",
            )
        ],
    ),
]


def main() -> int:
    return _harness.main(
        target=TARGET,
        mutants=MUTANTS,
        own_tests=TESTS,
        allow_unnamed=False,
    )


if __name__ == "__main__":
    raise SystemExit(main())
