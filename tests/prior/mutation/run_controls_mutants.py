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
mutants below concentrate on the two failure modes that produce a plausible
wrong number rather than an error:

* SCALE (T9-R2). C10..C16 reintroduce, one at a time, every spelling that
  rescales a control away from the full prior: `1 / n_species`, the
  `per_species / total` renormalisation, a median, a max, a sum, a per-cell
  denominator. Each is individually defensible-looking and each turns the
  comparison into a scale difference reported as a context effect.
* AXIS (T9-R3/T9-R4). C17..C25 permute the wrong axis, permute per-species
  instead of once, reuse one permutation for both shuffles, drop the seed, or
  reshape against a hard-coded stride.

COMPOUND CONDITIONS ARE MUTATED PER DISJUNCT
--------------------------------------------
`_require_dense_grid`'s three clauses are written as SEPARATE statements so
that a fixture tripping one never exercises another, and each gets its own
mutant with a fixture violating ONLY that clause:

* the column-count / cell-axis agreement -> C1, C2
* `nnz != size` (structural zeros)       -> C3, C4
* `count_nonzero != nnz` (stored zero)   -> C5, C6

WHAT THIS SET DOES NOT COVER -- stated so the table is not read as more than
it is
---------------------------------------------------------------------------
* WHETHER THESE FOUR ARE THE RIGHT CONTROLS. Every mutant here asks whether
  each control computes what it claims. That the set {uniform, marginal,
  shuffle_space, shuffle_time} is SUFFICIENT to establish a spatiotemporal
  effect is a claim about experimental design, not about this module, and no
  mutation of it can speak to it. A reviewer could reasonably demand a fifth
  control (e.g. a phylogenetically-matched species swap, or a shuffle that
  permutes space and time jointly), and nothing here would notice its absence.
* THE SWEEP THAT CONSUMES THEM. `CONTROLS` is pinned as a registry of four
  callables with matching variant labels, but nothing here proves the sweep
  actually RUNS all four in every cell (the spec's "run in every cell"), nor
  that it compares them against the full prior on the same fold. A sweep that
  silently skipped `marginal` would pass every test in this file. That
  obligation lives downstream and is only as strong as its own tests.
* THE DENSE-GRID OBLIGATION AT ITS SOURCE. C3/C4 prove these controls REFUSE a
  tensor with structural zeros, which is the strongest thing this module can
  do. Whether the pipeline actually hands them a dense grid is `to_tensor`'s
  caller's obligation (tensor.py's STRUCTURAL ZEROS section, T10-R1). If it
  does not, the controls now raise rather than silently dilute -- but the run
  fails loudly instead of producing a control set, and nothing here tests the
  recovery path, because there is deliberately none.
* FLOATING-POINT MASS AT NATIONAL SCALE. The mass-preservation tests compare
  with `pytest.approx` on a 312-entry matrix. `mean * size == sum` is exact
  only up to summation order; at ~10^8 contexts the drift is real but bounded
  and far below any effect size the paper reports. Nothing here measures it,
  and a mutant introducing a float32 intermediate inside the mean would be
  caught only if it drifted past `approx`'s default tolerance.
* THE SEED POLICY. C22/C24 prove the seed is USED and that two seeds differ,
  and the shuffles are deterministic in it. WHICH seeds the paper reports, and
  whether one shuffle replicate is enough to claim a null, is a statistical
  question this table cannot reach: a single-replicate shuffle control is weak
  evidence regardless of how correctly it is computed.
* SPARSITY AS INFORMATION, ON THE SHUFFLES. `uniform` and `marginal` are dense
  by construction and the module explains why. The shuffles inherit whatever
  pattern the input had -- which the dense-grid guard forces to be complete --
  so nothing here exercises a shuffle over a genuinely sparse prior, because
  that input is now rejected. If a future task relaxes the guard, C3/C4 would
  need to be rewritten rather than merely re-run.
* MEMORY. Every control materialises `t.values.toarray()`. At H3 resolution 4
  over Denmark that is small; at national resolution 7 with 10^3 species it is
  not, and nothing here has a peak-allocation test (the smoothing harness does,
  S31). A control that OOMs is a loud failure, not a silent one, which is why
  it is out of scope rather than untested by oversight.
"""
from __future__ import annotations

import argparse
import difflib
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
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
    # ---- structural zeros must not silently dilute a control --------------
    (
        "C3",
        "the DENSE-GRID check removed, so a tensor with structural zeros is "
        "accepted and every control is diluted by nnz/size relative to the "
        "full prior. This is the T9-R2 defect arriving through the input "
        "rather than through the formula: nothing errors, every value is "
        "plausible, and the paper's comparison silently becomes a scale test",
        "test_a_tensor_with_a_structural_zero_is_rejected_by_every_control",
        [("    if t.values.nnz != size:", "    if False:")],
    ),
    (
        "C4",
        "the dense-grid check weakened to `>` so a grid with MISSING contexts "
        "(nnz < size) passes while only an impossible over-full one is "
        "rejected -- the exact direction that matters, since nnz can never "
        "exceed size",
        "test_a_tensor_with_a_structural_zero_is_rejected_by_every_control",
        [
            (
                "    if t.values.nnz != size:",
                "    if t.values.nnz > size:",
            )
        ],
    ),
    (
        "C5",
        "the STORED-ZERO check removed. Separate mutant from C3: an explicit "
        "0.0 is counted by `nnz`, so the dense-grid clause passes on it, and "
        "it drags the control's mean below the full prior's scale. A `p` at "
        "or below 0.0 is an upstream contract breach per `_validate_p`",
        "test_an_explicitly_stored_zero_is_rejected",
        [
            (
                "    if t.values.count_nonzero() != t.values.nnz:",
                "    if False:",
            )
        ],
    ),
    (
        "C6",
        "the size computed from `nnz` rather than from the shape, making the "
        "dense-grid clause a tautology (`nnz != nnz` is never true) while it "
        "still LOOKS like a check. The most plausible-looking way to disable "
        "C3 without deleting a line",
        "test_a_tensor_with_a_structural_zero_is_rejected_by_every_control",
        [
            (
                "    size = int(n_rows) * int(n_cols)",
                "    size = int(t.values.nnz)",
            )
        ],
    ),
    (
        "C7",
        "the guard not CALLED by `uniform_prior` at all, so the control that "
        "defines the scale is the one control that accepts a diluted input",
        "test_a_tensor_with_a_structural_zero_is_rejected_by_every_control",
        [
            (
                '    dense = _require_dense_grid(t, "uniform")',
                "    dense = np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    (
        "C8",
        "the guard not called by `marginal_prior` -- THE control, the one a "
        "reviewer will demand, and a separate mutant because C7's deletion "
        "cannot detect this one's loss",
        "test_a_tensor_with_a_structural_zero_is_rejected_by_every_control",
        [
            (
                '    dense = _require_dense_grid(t, "marginal")',
                "    dense = np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    (
        "C9",
        "the guard not called by `shuffle_space`, so a structural hole is "
        "RELOCATED onto a real context: the shuffled control then carries a "
        "hard zero where the full prior had an estimate, which can veto a "
        "correct detection and make the control look worse than chance",
        "test_a_tensor_with_a_structural_zero_is_rejected_by_every_control",
        [
            (
                '    dense = _require_dense_grid(t, "shuffle_space")',
                "    dense = np.asarray(t.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    (
        "C10",
        "the guard not called by `shuffle_time` -- the time mirror of C9",
        "test_a_tensor_with_a_structural_zero_is_rejected_by_every_control",
        [
            (
                '    dense = _require_dense_grid(t, "shuffle_time")',
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
        "renormalisation of a prior that is never renormalised at any stage "
        "-- and on the plan's own 3-context example the control sits at 2.75x "
        "the full prior's mass. Every fusion method combines prior and "
        "posterior multiplicatively or via `log prior`, so the comparison "
        "measures a scale difference and reports it as a context effect",
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
        "the brief's `per_species / total` renormalisation restored in "
        "`marginal_prior`. This is the T9-R2 defect in the control that "
        "matters most: the divisor depends on HOW MANY SPECIES the tensor "
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
        "C16",
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
        "C17",
        "`marginal_prior` uses each species' MEDIAN over contexts. Preserves "
        "the species ranking and is not mass-preserving on a skewed prior. "
        "Separate mutant from C16 because a sum is caught by magnitude while "
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
        "C18",
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
        "C19",
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
        "C20",
        "`marginal_prior` averages over SPECIES instead of over contexts "
        "(axis=0), so the control varies with cell and week and is constant "
        "across species -- the two properties exactly inverted, while the "
        "shape, the range and the total mass all stay plausible",
        "test_marginal_ignores_cell_and_week",
        [
            (
                "    per_species = dense.mean(axis=1, keepdims=True)\n"
                "    return _respec(t, \"marginal\", np.tile(per_species, (1, dense.shape[1])))",
                "    _per_context = dense.mean(axis=0, keepdims=True)\n"
                "    return _respec(t, \"marginal\", np.tile(_per_context, (dense.shape[0], 1)))",
            )
        ],
    ),
    # ---- T9-R3: the axes, the shared permutation, the seed ----------------
    (
        "C21",
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
        "C22",
        "`shuffle_time` permutes the CELL axis -- the mirror of C21, and a "
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
        "C23",
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
        "C24",
        "`shuffle_time` becomes a NO-OP -- the time mirror of C23",
        "test_shuffle_time_actually_moves_a_week",
        [
            (
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    blocks = blocks[:, :, np.arange(WEEKS_PER_YEAR)]",
            )
        ],
    ),
    (
        "C25",
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
        "C26",
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
        "C27",
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
        "C28",
        "the seed ignored in `shuffle_time` -- the time mirror of C27",
        "test_shuffle_time_uses_its_seed",
        [
            (
                "    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]",
                "    blocks = blocks[:, :, np.random.default_rng(0).permutation(WEEKS_PER_YEAR)]",
            )
        ],
    ),
    (
        "C29",
        "`shuffle_space` reshaped against a hard-coded 52 rather than "
        "WEEKS_PER_YEAR. True today and a silent layout drift the moment the "
        "stride changes -- the defect this plan forbids a literal 52 to "
        "prevent. Caught here only because the reshape then disagrees with "
        "the guard's expected column count",
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
        "C30",
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
        "C31",
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
        "C32",
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
        "C33",
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
        "C34",
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
        "C35",
        "the `marginal` entry dropped from the registry, so the ONE control a "
        "reviewer is guaranteed to demand is never run by a sweep that "
        "iterates `CONTROLS`. The function still exists and still passes its "
        "own tests",
        "test_controls_registry_covers_all_four",
        [
            (
                '    "marginal": marginal_prior,\n',
                "",
            )
        ],
    ),
    (
        "C36",
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
]


def _clear_pycache() -> None:
    for d in REPO.rglob("__pycache__"):
        shutil.rmtree(d, ignore_errors=True)


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout


def _require_clean() -> None:
    dirty = [
        line
        for line in _git("status", "--porcelain").splitlines()
        if line[3:].strip() == str(TARGET)
    ]
    if dirty:
        sys.exit(f"refusing to run: {TARGET} is dirty ({dirty!r}); commit or restore it")


def _run_tests() -> tuple[int, str, set[str]]:
    """Return (failure count, output, names of failing tests)."""
    proc = subprocess.run(
        ["uv", "run", "pytest", TESTS, "-q", "--no-header", "-p", "no:cacheprovider"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    out = proc.stdout + proc.stderr
    failed = set(re.findall(r"^(?:FAILED|ERROR) \S+::(\w+)", out, re.MULTILINE))
    match = re.search(r"(\d+) failed", out)
    if match:
        return int(match.group(1)), out, failed
    # A collection error or an uncaught crash reports no "N failed" line but is
    # still a kill; treat any non-zero exit as at least one failure so that a
    # broken run can never be scored as a survivor.
    return (0 if proc.returncode == 0 else 1), out, failed


def _apply(source: str, edits: list[tuple[str, str]]) -> str | None:
    mutated = source
    for old, new in edits:
        if mutated.count(old) != 1:
            return None
        mutated = mutated.replace(old, new)
    return mutated


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*", default=None, help="mutant ids to run")
    parser.add_argument(
        "--verify-labels",
        action="store_true",
        help="print the diff each mutant applies and exit, so every LABEL can "
        "be read against its actual EDIT rather than trusted",
    )
    args = parser.parse_args()

    source = (REPO / TARGET).read_text()
    test_source = (REPO / TESTS).read_text()
    selected = [m for m in MUTANTS if args.only is None or m[0] in args.only]

    if args.verify_labels:
        bad = []
        for mid, description, killer, edits in selected:
            mutated = _apply(source, edits)
            print(f"\n{'=' * 72}\n{mid}: {description}\n  killer: {killer}\n{'=' * 72}")
            if mutated is None:
                print("  !! NOT APPLICABLE: a pattern does not occur exactly once")
                bad.append(mid)
                continue
            if f"def {killer}(" not in test_source:
                print(f"  !! killer test {killer!r} does not exist in {TESTS}")
                bad.append(mid)
            print(
                "".join(
                    difflib.unified_diff(
                        source.splitlines(keepends=True),
                        mutated.splitlines(keepends=True),
                        "original",
                        mid,
                        n=1,
                    )
                )
            )
        print(f"\nunapplicable/mislabelled: {bad or 'none'}")
        return 1 if bad else 0

    _require_clean()

    _clear_pycache()
    baseline, out, _ = _run_tests()
    _clear_pycache()
    if baseline:
        sys.exit(f"refusing to run: baseline is not green ({baseline} failed)\n{out}")
    print(f"baseline: green ({out.strip().splitlines()[-1]})\n")

    rows: list[tuple[str, str, str]] = []
    survivors: list[str] = []
    not_applied: list[str] = []
    mislabelled: list[str] = []

    for mid, description, killer, edits in selected:
        if f"def {killer}(" not in test_source:
            print(f"{mid}: MISLABELLED — killer test {killer!r} does not exist")
            rows.append((mid, description, "MISLABELLED"))
            mislabelled.append(mid)
            continue

        mutated = _apply(source, edits)
        if mutated is None:
            counts = [source.count(old) for old, _ in edits]
            print(f"{mid}: NOT APPLIED — pattern occurrence counts {counts}")
            rows.append((mid, description, "NOT APPLIED"))
            not_applied.append(mid)
            continue

        (REPO / TARGET).write_text(mutated)
        _clear_pycache()
        try:
            failures, out, failed_names = _run_tests()
        finally:
            _git("checkout", "--", str(TARGET))
            _clear_pycache()
            assert (REPO / TARGET).read_text() == source, "restore failed"

        if not failures:
            verdict = "SURVIVED"
            survivors.append(mid)
        elif killer not in failed_names:
            # The mutant died, but not to the test this row CLAIMS covers it.
            # That is the "label does not match the coverage" failure mode from
            # earlier in this plan, and it must not read as a kill.
            verdict = f"MISLABELLED ({failures} failed, not {killer})"
            mislabelled.append(mid)
        else:
            verdict = str(failures)
        print(f"{mid}: {verdict:>10}  {description[:70]}")
        rows.append((mid, description, verdict))

    _require_clean()
    _clear_pycache()
    final, out, _ = _run_tests()
    _clear_pycache()
    print(f"\nrestored run: {'GREEN' if not final else f'{final} FAILED'}")

    print("\n| # | Mutation | Killed by | Tests killed |")
    print("|---|---|---|---|")
    by_id = {m[0]: m[2] for m in MUTANTS}
    for mid, description, verdict in rows:
        print(f"| {mid} | {description} | `{by_id[mid]}` | {verdict} |")
    print(f"\nSurvivors: {survivors or 'none'}")
    # NOT-APPLIED and MISLABELLED are FAILURES, not neutral rows. A pattern
    # that has gone stale after a refactor tests nothing, and a mutant killed
    # only by an unrelated test credits coverage that does not exist.
    print(f"NOT APPLIED: {not_applied or 'none'}")
    print(f"MISLABELLED: {mislabelled or 'none'}")
    return 1 if survivors or not_applied or mislabelled or final else 0


if __name__ == "__main__":
    raise SystemExit(main())
