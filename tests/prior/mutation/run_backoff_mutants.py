#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/backoff.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_backoff_mutants.py
    uv run python tests/prior/mutation/run_backoff_mutants.py --only B1 B7

Structure is copied from `run_effort_mutants.py` and `run_leakage_mutants.py`,
including the three things that make the result trustworthy:

1. it refuses to start unless `git status --porcelain` is clean for the target
   file, so a mutant can never be committed by accident;
2. every pattern must occur EXACTLY ONCE, and NOT-APPLIED is reported
   separately from SURVIVED -- a stale pattern that matched nothing would
   otherwise masquerade as a kill -- and NOT-APPLIED makes the harness EXIT
   NON-ZERO, so it cannot hide under a "Survivors: none" banner;
3. it clears every `__pycache__` BEFORE and AFTER each mutant. CPython
   invalidates a `.pyc` on (mtime, size), so a SAME-LENGTH mutation can leave a
   mutant `.pyc` that outlives its own source restoration and fake either a
   kill or a survival. This happened in Task 3 and misled two agents.

A mutant is KILLED if at least one test fails. Zero failures is a SURVIVOR and
means the suite does not pin that behaviour.

THE THREE-INSTANCE LESSON FROM THIS PLAN -- read before trusting a green table
-----------------------------------------------------------------------------
Three mutants earlier in this plan produced a passing table that meant nothing:
M33 and E20 had LABELS that did not match the edits they made, and E33's label
and test were both right but its FIXTURE never reached the mutated path.

THE RULE: a green table means nothing unless each mutant's label matches its
edit AND some fixture actually reaches the mutated path. Two guards here:

* `--verify-labels` prints the unified diff each mutant actually applies, so
  the label can be read against the edit rather than trusted;
* every mutant below names the test that kills it, and that name is CHECKED at
  runtime -- if the named test is not among the failures, the mutant is scored
  MISLABELLED (not a kill), which also exits non-zero. A mutant killed only by
  some unrelated test is exactly the "coverage that does not exist" failure
  mode, and this turns it from a silent pass into a loud one.

AN EQUIVALENCE ARGUMENT IS ONLY AS GOOD AS THE REGIME IT SAMPLED
---------------------------------------------------------------
B10 (the floor dropped from `p_global`) was retired here as EQUIVALENT on the
evidence "max difference 3.1e-10 over 1.2M configurations". The sweep was real
and the number was real, and the conclusion was still false: the mutated branch
is only LIVE when the raw global share falls below the floor, i.e. when
`global_n > 1/floor = 1e6`, and the sweep never sampled a global denominator
above a million. In the regime it never entered -- the normal GBIF regime -- the
two chains differ by 4.46e-07, which is 1438x the "bound", a difference in the
FIFTH significant digit. B10 is back, live, with a killer test whose fixture
holds a 2,000,000-record cell-week.

THE RULE, therefore: before retiring a mutant as equivalent, STATE THE
PARAMETER RANGE SWEPT and VERIFY THE MUTATED BRANCH IS REACHABLE WITHIN IT. A
sweep that never enters the branch's live regime proves nothing -- it measures
the difference between two identical computations and reports it as a bound.
This failure mode is MORE DANGEROUS than a mislabelled mutant, because it
arrives with a number attached and the number is not wrong, only irrelevant.
Retire nothing on an aggregate statistic alone: name the condition under which
the mutation changes behaviour, and show a fixture that satisfies it (then the
mutant is live and needs a test) or prove no input can (then it is equivalent).

WHAT THIS SET DOES NOT COVER -- stated so the table is not read as more than it
is
------------------------------------------------------------------------------
* NUMERICAL CALIBRATION. The default weights (10.0 / 50.0) are not pinned to
  any particular value; they are a modelling choice, and B12/B13 only prove
  that changing them changes the answer in the documented direction. Whether
  10.0 is the RIGHT strength for GBIF-scale data is an empirical question no
  mutation can answer.
* FLOAT ASSOCIATIVITY. Summing a parent denominator in a different row order
  gives a bit-different float64; the suite asserts invariance at rel=1e-12,
  not bit-identity, so a reassociation mutant would survive. Accepted: at
  1e-12 the prior is unaffected.
* SCALE. Most fixtures are tiny (single-digit cells, <= 8 chunks). Three are
  deliberately LARGE (900k and 2M records) because the floor's live regime
  starts at 1e6 -- see B10 and the two absence-distinction tests. Nothing here
  would still catch a defect that only appears at millions of DISTINCT
  cell-weeks, e.g. a dict that grows without bound or an int64 overflow in a
  global denominator.
* THE UPSTREAM/DOWNSTREAM SEAM. Mutating `effort.py` or a future
  `smoothing.py` is out of scope; this harness only mutates `backoff.py`, so a
  contract drift on either side is caught only insofar as `test_backoff.py`
  builds its fixtures through `target_group_ratios` (it does).
* PARAMETER PLUMBING PER LEVEL. `cell_weight` and `cell_week_weight` are
  pinned individually (B12/B13/B14), but a mutant that swapped the two
  DEFAULTS and simultaneously swapped the two call sites would be a pure
  relabelling and is not tested.
* THE PARAMETER CONTRACT'S *VALUES*. B49-B58b (round 10) pin that each bound
  of `_validate_params` is load-bearing -- one mutant per clause, in both the
  too-loose and (where meaningful) the too-strict direction. They do NOT pin
  that the bounds are the RIGHT ones. Specifically, nothing here would catch:
  - a floor of 0.9 being accepted. It is a probability, it is inside the
    guard, and it produces an artifact that is 1.0 almost everywhere. The
    guard rejects only the degenerate endpoint, not "absurdly high but
    legal", because no principled threshold exists between them and an
    invented one would break a sweep;
  - a weight of 1e9, which shrinks every cell-week to its global profile and
    produces a spatially and seasonally flat artifact with no error. Same
    reason: there is no bound short of `inf` that is not a guess;
  - the sidecar recording DIFFERENT params from the ones the build used
    while both are individually valid. That is `tensor.py`'s contract and is
    mutated in `run_tensor_mutants.py`, not here;
  - a caller mutating a `BackoffParams` between validation and use. The
    dataclass is frozen, which makes this hard rather than impossible
    (`object.__setattr__`), and no mutant models it.
* THE CLI SEAM, beyond one call. B49's killer is the end-to-end test, so the
  guard is proved to be reachable from `build_from_archive`. Nothing here
  proves the OTHER entry points a future round might add are covered: the
  guard lives in `hierarchical_estimate`, and a new caller that reimplements
  the backoff rather than calling it would bypass it silently.
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
TARGET = Path("src/birdology/prior/backoff.py")
TESTS = "tests/prior/test_backoff.py"

# (id, description, killer_test, [(old, new), ...])
#
# `killer_test` is the test whose failure DEMONSTRATES the mutant is caught. It
# is verified at runtime: a mutant that fails only other tests is reported
# MISLABELLED rather than counted as a kill.
MUTANTS: list[tuple[str, str, str, list[tuple[str, str]]]] = [
    # ---- rule (C) / contract 4: the denominator is `total`, not a re-sum ----
    (
        "B1",
        "the cell-week denominator RE-DERIVED by summing the `n` values "
        "present, instead of reading the `total` column. Agrees with `total` "
        "only while the table holds every species; after a filter to the "
        "label set it reports a share of ~1.0 where the truth is 0.01",
        "test_the_denominator_is_the_TOTAL_COLUMN_not_a_re_sum_of_n",
        [
            (
                "        cell_week_total[key] = total",
                "        cell_week_total[key] = cell_week_total.get(key, 0) + n",
            )
        ],
    ),
    (
        "B2",
        "the cell-week `total` counted ONCE PER ROW when summing the parent "
        "denominators, so a cell-week with k species present contributes k "
        "times its own effort and every parent profile is diluted",
        "test_cell_profile_is_recomputed_from_COUNTS_not_averaged_from_RATIOS",
        [
            (
                "    for (cell, _week), total in cell_week_total.items():\n"
                "        cell_n[cell] = cell_n.get(cell, 0.0) + total\n"
                "        global_n += total",
                "    for row in rows:\n"
                '        cell_n[row["cell"]] = cell_n.get(row["cell"], 0.0) + row["total"]\n'
                '        global_n += row["total"]',
            )
        ],
    ),
    (
        "B3",
        "the cell-week numerator uses `total` instead of `n`, so every "
        "observed cell-week reports a share of 1.0",
        "test_the_shrunk_estimate_is_the_documented_formula",
        [
            (
                "        cell_week_species[row_key] = float(n)",
                "        cell_week_species[row_key] = float(total)",
            )
        ],
    ),
    # ---- contract 5: parent levels from parent COUNTS, not averaged ratios --
    (
        "B4",
        "the CELL profile computed as the unweighted MEAN OF ITS CHILD "
        "RATIOS instead of a ratio of sums. Re-introduces the observer-effort "
        "bias the target-group correction removes: a one-record week gets the "
        "same voice as a hundred-record week",
        "test_cell_profile_is_recomputed_from_COUNTS_not_averaged_from_RATIOS",
        [
            (
                "            p_cell = _shrink(\n"
                "                cell_species.get((sk, cell), 0.0),\n"
                "                cell_n.get(cell, 0.0),\n"
                "                p_global,\n"
                "                params.cell_weight,\n"
                "            )",
                "            _kids = [\n"
                "                cell_week_species.get((sk, cell, w), 0.0) / t\n"
                "                for (c, w), t in cell_week_total.items()\n"
                "                if c == cell\n"
                "            ]\n"
                "            _mean = (sum(_kids) / len(_kids)) if _kids else p_global\n"
                "            p_cell = _shrink(\n"
                "                _mean * cell_n.get(cell, 0.0),\n"
                "                cell_n.get(cell, 0.0),\n"
                "                p_global,\n"
                "                params.cell_weight,\n"
                "            )",
            )
        ],
    ),
    (
        "B5",
        "the GLOBAL profile computed as the unweighted MEAN OF THE PER-CELL "
        "RATIOS instead of a ratio of sums, so a single-record cell drags the "
        "national profile up by two orders of magnitude",
        "test_global_profile_is_recomputed_from_COUNTS_not_averaged_from_CELL_RATIOS",
        [
            (
                "        p_global = (global_species.get(sk, 0.0) / global_n) if global_n else 0.0",
                "        _per_cell = [\n"
                "            cell_species.get((sk, c), 0.0) / d\n"
                "            for c, d in cell_n.items()\n"
                "            if d\n"
                "        ]\n"
                "        p_global = (sum(_per_cell) / len(_per_cell)) if _per_cell else 0.0",
            )
        ],
    ),
    (
        "B6",
        "the CELL denominator summed over the WHOLE TABLE rather than over "
        "the cell's own cell-weeks, so every cell profile equals the global "
        "one and the middle level of the hierarchy disappears",
        "test_an_unobserved_week_backs_off_to_its_CELL_not_straight_to_GLOBAL",
        [
            (
                "    for (cell, _week), total in cell_week_total.items():\n"
                "        cell_n[cell] = cell_n.get(cell, 0.0) + total\n"
                "        global_n += total",
                "    for (_cell, _week), total in cell_week_total.items():\n"
                "        global_n += total\n"
                "    for _cell, _week in cell_week_total:\n"
                "        cell_n[_cell] = global_n",
            )
        ],
    ),
    (
        "B7",
        "the cell-week level shrinks straight toward the GLOBAL profile, "
        "skipping the cell's annual profile: a three-level hierarchy silently "
        "becomes two levels, and an unvisited week in a species-rich cell is "
        "scored from the national average",
        "test_an_unobserved_week_backs_off_to_its_CELL_not_straight_to_GLOBAL",
        [
            (
                "                    p_cell,\n"
                "                    params.cell_week_weight,",
                "                    p_global,\n"
                "                    params.cell_week_weight,",
            )
        ],
    ),
    (
        "B8",
        "the CELL profile ignores the global level entirely (shrinks toward "
        "0.0), so a cell with no records of a species reports ~0 rather than "
        "the species' national rate",
        "test_global_profile_is_recomputed_from_COUNTS_not_averaged_from_CELL_RATIOS",
        [
            (
                "                cell_n.get(cell, 0.0),\n"
                "                p_global,\n"
                "                params.cell_weight,",
                "                cell_n.get(cell, 0.0),\n"
                "                0.0,\n"
                "                params.cell_weight,",
            )
        ],
    ),
    # ---- contract 3: absence is row absence; the floor ---------------------
    (
        "B9",
        "the floor removed from the emitted value, so a species with no "
        "records anywhere gets exactly 0.0 -- 'nobody looked' recorded as "
        "'certainly absent'",
        "test_species_absent_everywhere_gets_floor_not_zero",
        [
            (
                "                out_p.append(min(max(p, params.floor), 1.0))",
                "                out_p.append(min(p, 1.0))",
            )
        ],
    ),
    (
        "B10",
        "the floor dropped from `p_global`, so a species whose raw global "
        "share falls BELOW the floor is scored from that raw share instead. "
        "Live only when `global_n > 1/floor` (1e6 records) -- the normal GBIF "
        "regime, and the regime an earlier equivalence sweep never sampled",
        "test_the_floor_on_p_global_CHANGES_the_answer_above_one_over_floor",
        [
            (
                "        p_global = max(p_global, params.floor)",
                "        pass  # p_global = max(p_global, params.floor)",
            )
        ],
    ),
    # B11 RETIRED as EQUIVALENT, with a REACHABILITY PROOF rather than a
    # sampled statistic (see this file's header on B10 for why the
    # distinction matters).
    #
    # B11 removed the UPPER CLAMP in `min(max(p, params.floor), 1.0)`. Its
    # killer test drove `p` to 1.75 by passing `cell_week_weight = -5.0`,
    # i.e. the mutated branch was live ONLY under a negative shrinkage
    # weight. Round 10 REJECTS negative weights at the entry point, so that
    # input can no longer reach the arithmetic and the mutant became
    # unkillable by construction.
    #
    # THE CONDITION UNDER WHICH THE MUTATION CHANGES BEHAVIOUR is `p > 1.0`
    # at the emit site. No validated input can satisfy it:
    #
    #   * `_validate_table` enforces `1 <= n <= total` per row and
    #     `sum(n) <= total` per cell-week, so every ratio of sums in the
    #     chain is <= 1;
    #   * `_validate_params` enforces `floor` strictly inside (0, 1), so
    #     `p_global = max(raw_share, floor)` is <= 1;
    #   * `_validate_params` enforces both weights finite and >= 0, so each
    #     `_shrink` is a convex combination of two quantities that are both
    #     <= 1 (or, on an empty level, the coarse value itself) and is
    #     therefore <= 1.
    #
    # Hence `max(p, floor) <= 1.0` for every accepted input and the clamp is
    # a no-op. It is KEPT in the source as a bound on any future path that
    # reintroduces an unbounded weight; it is not mutated, because a mutant
    # no input can distinguish is a survivor that means nothing.
    #
    # The test that made B11 live is now
    # `test_a_negative_weight_is_REJECTED_rather_than_silently_clamped`,
    # which pins the rejection AND re-asserts the 1.75 arithmetic, so the
    # premise of this equivalence argument fails loudly if the rejection is
    # ever relaxed.
    # ---- THE PARAMETER CONTRACT (round 10 / brief A) -----------------------
    #
    # ONE MUTANT PER VALIDATION CLAUSE. A single mutant deleting the whole
    # guard proves only that the guard exists, not that each bound is
    # load-bearing -- and each bound here closes a DIFFERENT reproduced
    # silent failure (see the module docstring's measurements at HEAD
    # edd0723).
    #
    # Each clause is mutated in both directions where both are meaningful:
    # REMOVED (the bad value is re-admitted) and TIGHTENED (a swept value
    # that must work is rejected). The second direction matters because
    # these constants are swept to produce the paper's effect size, so a
    # guard that is too strict is a defect of the same family as one that is
    # too loose -- it just fails the build instead of the artifact.
    (
        "B49",
        "`_validate_params` never called, so the whole parameter contract "
        "disappears: floor=2.0 again writes a fully provenanced artifact in "
        "which every value is exactly 1.0. Killed BY THE END-TO-END TEST on "
        "purpose -- a unit test on `BackoffParams` does not prove the CLI is "
        "protected, since `build_from_archive` builds its own params object "
        "and passes it down two call layers",
        "test_a_FLATTENING_floor_cannot_produce_artifacts_through_the_CLI",
        [
            (
                "    _validate_params(params)",
                "    pass  # _validate_params(params)",
            )
        ],
    ),
    (
        "B49b",
        "the parameter guard made CONDITIONAL on the table having rows, so an "
        "empty RatioTable is scored with any parameters at all -- the path "
        "that emits the floor everywhere and touches none of the arithmetic "
        "a lazier guard would have waited for",
        "test_the_parameter_guard_runs_before_any_estimate_is_produced",
        [
            (
                "    _validate_params(params)",
                "    if ratios.num_rows:\n        _validate_params(params)",
            )
        ],
    ),
    (
        "B50",
        "the WEIGHTS' FINITENESS clause removed. `nan` fails every comparison, "
        "so the range clause below cannot see it: a nan weight is accepted "
        "and every emitted probability becomes nan with no error. Separate "
        "mutant from B51 because neither clause can detect the other's loss",
        "test_a_non_finite_shrinkage_weight_is_rejected",
        [
            (
                "        if not isfinite(value):",
                "        if False:",
            )
        ],
    ),
    (
        "B51",
        "the WEIGHTS' NON-NEGATIVITY clause removed, so a negative "
        "pseudo-count SUBTRACTS the coarser level's evidence. At HEAD "
        "cell_week_weight=-1.0 and -3.0 returned a plausible table with no "
        "error at all. Separate mutant from B50: a finiteness test passes on "
        "-1.0 and a range test passes on nan",
        "test_a_negative_shrinkage_weight_is_rejected",
        [
            (
                "        if value < 0.0:",
                "        if False:",
            )
        ],
    ),
    (
        "B51b",
        "the weights' range clause TIGHTENED to reject 0.0, which is the "
        "sweep's endpoint (`cell_week_weight = 0` means no shrinkage) and a "
        "documented accepted value. The guard would then be narrower than "
        "the sweep it exists to protect",
        "test_a_zero_shrinkage_weight_is_ACCEPTED",
        [
            (
                "        if value < 0.0:",
                "        if value <= 0.0:",
            )
        ],
    ),
    (
        "B52",
        "the weight guard applied to `cell_week_weight` ONLY, so a nan or "
        "negative `cell_weight` slips through the per-field loop -- the "
        "parameter-plumbing form of the defect, invisible to any test that "
        "only ever passes a bad cell_week_weight",
        "test_the_weight_guard_covers_BOTH_weights_not_only_the_first",
        [
            (
                '        ("cell_week_weight", params.cell_week_weight),\n'
                '        ("cell_weight", params.cell_weight),',
                '        ("cell_week_weight", params.cell_week_weight),',
            )
        ],
    ),
    (
        "B53",
        "the FLOOR'S FINITENESS clause removed. A nan floor is a SILENT "
        "NO-OP rather than a poison -- `max(p, nan)` returns `p` -- so the "
        "floor the module guarantees simply stops existing, with no error "
        "and no nan anywhere to notice. The range clauses cannot see it",
        "test_a_non_finite_floor_is_rejected",
        [
            (
                "    if not isfinite(params.floor):",
                "    if False:",
            )
        ],
    ),
    (
        "B54",
        "the FLOOR > 0 clause removed, re-admitting the hard zero the "
        "module's entire docstring exists to forbid: an unobserved cell-week "
        "would be recorded as certain absence and could veto a correct "
        "detection outright",
        "test_a_floor_of_zero_is_rejected",
        [
            (
                "    if params.floor <= 0.0:",
                "    if False:",
            )
        ],
    ),
    (
        "B54b",
        "the FLOOR > 0 clause weakened from `<=` to `<`, admitting EXACTLY "
        "0.0 while still rejecting -1.0. The boundary is the whole point: "
        "0.0 is the hard zero, and a guard that rejects only negative floors "
        "leaves the documented harm reachable",
        "test_a_floor_of_zero_is_rejected",
        [
            (
                "    if params.floor <= 0.0:",
                "    if params.floor < 0.0:",
            )
        ],
    ),
    (
        "B55",
        "the FLOOR < 1 clause removed. This is the reproduced end-to-end "
        "defect: floor=2.0 wrote six valid, fully provenanced artifacts -- "
        "DOI, manifest, licence, honest sidecar -- in which every value was "
        "exactly 1.0, with nothing raised anywhere in the pipeline",
        "test_a_floor_above_one_is_rejected",
        [
            (
                "    if params.floor >= 1.0:",
                "    if False:",
            )
        ],
    ),
    (
        "B55b",
        "the FLOOR < 1 clause weakened from `>=` to `>`, admitting EXACTLY "
        "1.0 -- which IS a probability and still flattens the artifact to "
        "the same constant, since `min(max(p, 1.0), 1.0)` is 1.0 for every "
        "p. The boundary admits the identical information-free artifact",
        "test_a_floor_of_one_is_rejected",
        [
            (
                "    if params.floor >= 1.0:",
                "    if params.floor > 1.0:",
            )
        ],
    ),
    (
        "B55c",
        "the FLOOR < 1 clause TIGHTENED to reject any floor at or above 0.5, "
        "which is a legitimate probability and a buildable configuration. "
        "The over-strict direction of the same bound: it fails the build "
        "rather than the artifact, and no test of a REJECTION can see it",
        "test_the_swept_parameter_values_are_all_ACCEPTED",
        [
            (
                "    if params.floor >= 1.0:",
                "    if params.floor >= 0.5:",
            )
        ],
    ),
    (
        "B56",
        "the parameter guard WARNS instead of raising (the warn-and-continue "
        "pattern the plan's rule B forbids), so the flattening floor is "
        "announced on stdout and then used anyway",
        "test_a_floor_above_one_is_rejected",
        [
            (
                "    if params.floor >= 1.0:\n        raise ValueError(",
                "    if params.floor >= 1.0:\n        print(",
            )
        ],
    ),
    (
        "B57",
        "the parameter guard CLAMPS instead of raising -- the silent-repair "
        "form, and the dangerous one: the build succeeds with a floor the "
        "caller never asked for, while `save_prior` writes the ORIGINAL "
        "params object into the sidecar. The artifact would carry an honest "
        "record of constants the build did not use",
        "test_a_rejected_parameter_is_never_repaired_into_a_default",
        [
            (
                "    if not isfinite(params.floor):",
                "    params = BackoffParams(\n"
                "        cell_week_weight=max(params.cell_week_weight, 0.0),\n"
                "        cell_weight=max(params.cell_weight, 0.0),\n"
                "        floor=min(max(params.floor, FLOOR), 1.0 - 1e-12),\n"
                "    )\n"
                "    if not isfinite(params.floor):",
            )
        ],
    ),
    (
        "B58",
        "the EMPTY-LEVEL definition in `_shrink` returns 0.0 instead of the "
        "coarse value, so zero shrinkage on an unvisited cell-week reports a "
        "hard zero. The output clamp then lifts it to `floor`, which is why "
        "an assertion like `p > 0` cannot see this and the killer test "
        "asserts the PARENT value exactly on a fixture whose parent is five "
        "orders of magnitude above the floor",
        "test_zero_shrinkage_on_an_EMPTY_cell_week_returns_the_PARENT_not_zero",
        [
            (
                "    if denominator == 0.0:\n        return coarse_p",
                "    if denominator == 0.0:\n        return 0.0",
            )
        ],
    ),
    (
        "B58b",
        "the empty-level guard removed entirely, restoring the bare "
        "`ZeroDivisionError` that `cell_week_weight = 0.0` on an unvisited "
        "cell-week raised at HEAD -- an error naming neither the parameter "
        "nor the cell-week, from a configuration the sweep is entitled to ask "
        "for",
        "test_zero_shrinkage_on_an_EMPTY_cell_week_returns_the_PARENT_not_zero",
        [
            (
                "    denominator = local_den + weight\n"
                "    if denominator == 0.0:\n"
                "        return coarse_p\n"
                "    return (local_num + weight * coarse_p) / denominator",
                "    return (local_num + weight * coarse_p) / (local_den + weight)",
            )
        ],
    ),
    # ---- contract 4: `total` really is the confidence weight ---------------
    (
        "B12",
        "the cell-week shrinkage weight hard-coded to 0.0 (`params."
        "cell_week_weight` ignored), so a single lucky record yields a "
        "probability of 1.0",
        "test_a_single_record_cell_week_cannot_reach_certainty",
        [
            (
                "                    p_cell,\n"
                "                    params.cell_week_weight,",
                "                    p_cell,\n"
                "                    0.0,",
            )
        ],
    ),
    (
        "B13",
        "the CELL shrinkage weight hard-coded to 0.0 (`params.cell_weight` "
        "ignored), so a cell with one record trusts that record completely",
        "test_the_cell_level_weight_applies_at_the_CELL_level",
        [
            (
                "                p_global,\n"
                "                params.cell_weight,",
                "                p_global,\n"
                "                0.0,",
            )
        ],
    ),
    (
        "B14",
        "the two shrinkage weights SWAPPED at the call sites, so the leaf is "
        "shrunk five times harder than its own parent -- the hierarchy pulls "
        "in the wrong proportion",
        "test_the_cell_level_weight_applies_at_the_CELL_level",
        [
            (
                "                p_global,\n"
                "                params.cell_weight,",
                "                p_global,\n"
                "                params.cell_week_weight,",
            ),
            (
                "                    p_cell,\n"
                "                    params.cell_week_weight,",
                "                    p_cell,\n"
                "                    params.cell_weight,",
            ),
        ],
    ),
    (
        "B15",
        "`_shrink` ignores the local DENOMINATOR (uses the weight alone), so "
        "1 record in 1 and 1000 records in 1000 shrink identically -- the "
        "effort proxy stops being a confidence weight",
        "test_two_identical_ratios_with_different_effort_get_different_estimates",
        [
            (
                "    denominator = local_den + weight",
                "    denominator = local_num + weight",
            )
        ],
    ),
    (
        "B16",
        "`_shrink` drops the pseudo-count from the DENOMINATOR only, so the "
        "estimate is inflated by the prior mass without paying for it and a "
        "one-record cell-week exceeds its own share",
        "test_a_single_record_cell_week_cannot_reach_certainty",
        [
            (
                "    denominator = local_den + weight",
                "    denominator = local_den + 0.0000001",
            )
        ],
    ),
    (
        "B17",
        "`_shrink` returns the raw local share, ignoring the coarse level "
        "entirely: no backoff at all, so an unvisited cell-week is 0/0",
        "test_an_unvisited_cell_week_is_scored_from_the_parent_not_from_zero_over_zero",
        [
            (
                "    denominator = local_den + weight\n"
                "    if denominator == 0.0:\n"
                "        return coarse_p\n"
                "    return (local_num + weight * coarse_p) / denominator",
                "    return local_num / local_den if local_den else 0.0",
            )
        ],
    ),
    (
        "B18",
        "`_shrink` returns the COARSE value unchanged, discarding the local "
        "data: a cell-week with 1000 records is scored from its cell average",
        "test_dense_cell_week_dominated_by_local_estimate",
        [
            (
                "    denominator = local_den + weight\n"
                "    if denominator == 0.0:\n"
                "        return coarse_p\n"
                "    return (local_num + weight * coarse_p) / denominator",
                "    return coarse_p",
            )
        ],
    ),
    # ---- contract 6: weeks ---------------------------------------------------
    (
        "B19",
        "the SHARED `_validate_week` body disabled, which removes BOTH ends of "
        "the range check at once -- the requested-grid one and the input-side "
        "one (B22's territory). The weakest form of the check, kept because a "
        "refactor that guts the helper must be loud",
        "test_a_week_outside_1_52_is_rejected",
        [
            (
                "    if not 1 <= week <= WEEKS_PER_YEAR:",
                "    if False:",
            )
        ],
    ),
    (
        "B19b",
        "only the REQUESTED-GRID call site removed (`_validate_week` itself "
        "intact, so the input side still checks). Week 0 / 53 are then emitted "
        "on the output axis; int8 accepts them and the smoothing stage "
        "inherits a seam it cannot close",
        "test_a_week_outside_1_52_is_rejected",
        [
            (
                '            _validate_week(int(week), "the requested weeks")',
                '            pass  # _validate_week(int(week), "the requested weeks")',
            )
        ],
    ),
    (
        "B20",
        "the week check's upper bound raised to 53, so ISO week 53 (which "
        "upstream folds into 52) is accepted as a real 53rd week with a "
        "seventh of the usual data behind it",
        "test_a_week_outside_1_52_is_rejected",
        [
            (
                "    if not 1 <= week <= WEEKS_PER_YEAR:",
                "    if not 1 <= week <= WEEKS_PER_YEAR + 1:",
            )
        ],
    ),
    (
        "B21",
        "the week check's lower bound dropped to 0, so week 0 passes and "
        "silently occupies an index no upstream week ever produces",
        "test_a_week_outside_1_52_is_rejected",
        [
            (
                "    if not 1 <= week <= WEEKS_PER_YEAR:",
                "    if not 0 <= week <= WEEKS_PER_YEAR:",
            )
        ],
    ),
    (
        "B22",
        "the INPUT-side week check removed (the `_validate_week` call inside "
        "the row loop), so a RatioTable row for week 0 builds a profile that "
        "is then never emitted -- deleting those records from the cell "
        "profile with no error",
        "test_a_ratio_table_row_for_a_week_outside_1_52_is_rejected",
        [
            (
                '        _validate_week(week, "the RatioTable")',
                '        pass  # _validate_week(week, "the RatioTable")',
            )
        ],
    ),
    (
        "B23",
        "the output `week` widened to int64, breaking the dtype the smoothing "
        "and tensor stages key on",
        "test_output_week_is_int8_and_survives_the_round_trip",
        [
            (
                '            "week": pa.array(out_weeks, pa.int8()),',
                '            "week": pa.array(out_weeks, pa.int64()),',
            )
        ],
    ),
    (
        "B24",
        "circular week smoothing ADDED here: each week's numerator absorbs "
        "week-1 and week+1. Blending neighbours is the smoothing stage's job; "
        "doing it here double-counts it and makes the backoff levels a lie",
        "test_week_52_and_week_1_are_not_blended_here",
        [
            (
                "                p = _shrink(\n"
                "                    cell_week_species.get((sk, cell, week), 0.0),\n"
                "                    cell_week_total.get((cell, week), 0.0),",
                "                _prev = week - 1 or WEEKS_PER_YEAR\n"
                "                _next = week + 1 if week < WEEKS_PER_YEAR else 1\n"
                "                p = _shrink(\n"
                "                    cell_week_species.get((sk, cell, week), 0.0)\n"
                "                    + cell_week_species.get((sk, cell, _prev), 0.0)\n"
                "                    + cell_week_species.get((sk, cell, _next), 0.0),\n"
                "                    cell_week_total.get((cell, week), 0.0),",
            )
        ],
    ),
    # ---- contract 7: cells are opaque strings ------------------------------
    (
        "B25",
        "`cell` LOWERCASED locally before it is grouped and emitted -- a "
        "redundant second normalisation of an already-normalised value, which "
        "merges two distinct cells' profiles into one",
        "test_cell_strings_are_passed_through_verbatim",
        [
            (
                '        cell = row["cell"]',
                '        cell = row["cell"].lower()',
            )
        ],
    ),
    (
        "B26",
        "`cell` lowercased on the OUTPUT axis only, so the grid is keyed by "
        "one spelling and scored from another: every requested mixed-case "
        "cell silently reads an empty profile",
        "test_differently_cased_cells_are_different_cells",
        [
            (
                '            "cell": pa.array(out_cells, pa.string()),',
                '            "cell": pa.array([c.lower() for c in out_cells], pa.string()),',
            )
        ],
    ),
    # ---- contract 8: order / chunking invariance ---------------------------
    (
        "B27",
        "the duplicate-ROW check removed and the numerator ACCUMULATED, so a "
        "table concatenated from two partial RatioTables double-counts each "
        "numerator against a `total` that already covers the whole cell-week "
        "(a share of 0.6 where the truth is 0.3)",
        "test_a_duplicate_species_cell_week_ROW_is_rejected",
        [
            (
                "        if row_key in cell_week_species:",
                "        if False:",
            ),
            (
                "        cell_week_species[row_key] = float(n)",
                "        cell_week_species[row_key] = (\n"
                "            cell_week_species.get(row_key, 0.0) + n\n"
                "        )",
            ),
        ],
    ),
    (
        "B27b",
        "the duplicate-ROW check removed and the numerator OVERWRITTEN, so a "
        "concatenated table silently discards the records of every duplicate "
        "row but the last -- the same corruption in the opposite direction",
        "test_a_duplicate_species_cell_week_ROW_is_rejected",
        [
            (
                "        if row_key in cell_week_species:",
                "        if False:",
            ),
        ],
    ),
    (
        "B28",
        "the CELL numerator takes the last row's `n` instead of summing over "
        "the cell's weeks, so a cell's profile is whichever week happened to "
        "be last",
        "test_a_cell_profile_sums_its_weeks_rather_than_keeping_the_last",
        [
            (
                "        cell_species[(sk, cell)] = cell_species.get((sk, cell), 0.0) + n",
                "        cell_species[(sk, cell)] = float(n)",
            )
        ],
    ),
    (
        "B29",
        "the conflicting-`total` check disabled, so the assignment on the next "
        "line silently keeps the LAST value seen and the denominator of a "
        "stitched-together table depends on row order",
        "test_a_cell_week_whose_total_disagrees_between_rows_is_rejected",
        [
            (
                "        if key in cell_week_total and cell_week_total[key] != total:",
                "        if False:",
            )
        ],
    ),
    (
        "B29b",
        "the conflicting-`total` check disabled AND the assignment made "
        "first-wins, the same order-dependence in the opposite direction -- so "
        "the test cannot pass by happening to prefer whichever value the "
        "shipped code keeps",
        "test_a_cell_week_whose_total_disagrees_between_rows_is_rejected",
        [
            (
                "        if key in cell_week_total and cell_week_total[key] != total:",
                "        if False:",
            ),
            (
                "        cell_week_total[key] = total",
                "        cell_week_total[key] = cell_week_total.get(key, total)",
            ),
        ],
    ),
    # ---- input validation ---------------------------------------------------
    (
        "B30",
        "`_validate_table` never called: a table without `n`/`total` dies on "
        "a raw KeyError deep in the row loop instead of a ValueError naming "
        "the contract",
        "test_a_ratio_table_missing_required_columns_is_rejected",
        [
            (
                "    _validate_table(ratios)",
                "    pass  # _validate_table(ratios)",
            )
        ],
    ),
    (
        "B31",
        "the required-column check narrowed to drop `total`, so a table "
        "carrying only `ratio` passes validation and then fails elsewhere",
        "test_a_ratio_table_missing_only_TOTAL_is_rejected",
        [
            (
                '        if name not in ratios.column_names:',
                '        if name != "total" and name not in ratios.column_names:',
            )
        ],
    ),
    (
        "B32",
        "the dtype check DISABLED, so an int64 `week` from a bypassed pipeline "
        "passes validation unremarked (the cast form is B47)",
        "test_a_wrong_input_dtype_is_rejected_not_cast",
        [
            (
                "        if actual != expected:",
                "        if False and actual != expected:",
            )
        ],
    ),
    (
        "B47",
        "the dtype check CASTS instead of raising -- rule (C)'s SECOND "
        "DERIVATION, the dangerous form: the table is silently coerced to the "
        "required schema, so a bypassed pipeline produces a plausible prior "
        "with nothing raised anywhere",
        "test_a_wrong_input_dtype_is_not_silently_CAST_to_the_required_schema",
        [
            (
                "    _validate_table(ratios)",
                "    ratios = ratios.cast(\n"
                "        pa.schema(\n"
                "            [\n"
                "                pa.field(_f.name, _REQUIRED_TYPES.get(_f.name, _f.type))\n"
                "                for _f in ratios.schema\n"
                "            ]\n"
                "        )\n"
                "    )",
            )
        ],
    ),
    (
        "B33",
        "the null check removed: a null `cell`/`week` vanishes from the grid "
        "and a null `n`/`total` poisons a denominator, neither with an error",
        "test_a_null_in_a_key_or_a_count_is_rejected",
        [
            (
                "        if ratios.column(name).null_count:",
                "        if False:",
            )
        ],
    ),
    (
        "B34",
        "the `total >= 1` row check removed, so a zero denominator reaches "
        "the arithmetic (and the pseudo-count hides it as a plausible number "
        "rather than an inf)",
        "test_a_total_of_zero_is_rejected_even_when_n_is_zero_too",
        [
            (
                "        if total < 1:",
                "        if False:",
            )
        ],
    ),
    (
        "B35",
        "the `n >= 1` row check removed, so a zero-filled 'absence' row is "
        "accepted as an observation -- the exact conflation of row-absence "
        "with observed-zero that corrupts a target-group prior",
        "test_a_non_positive_n_is_rejected",
        [
            (
                "        if n < 1:",
                "        if False:",
            )
        ],
    ),
    (
        "B36",
        "the `n <= total` ROW-PAIRING check removed. Subsumed as a rejection "
        "PREDICATE by the cell-week coherence check (B46) -- with every n >= 1, "
        "a row exceeding its own total also makes its cell-week sum exceed it -- "
        "so the kill is by MESSAGE: the row check LOCALISES the defect to a "
        "(species, cell, week) where the aggregate one can only say 'somewhere'",
        "test_a_numerator_exceeding_its_own_total_is_rejected",
        [
            (
                "        if n > total:",
                "        if False:",
            )
        ],
    ),
    (
        "B37",
        "the validation WARNS instead of raising (the warn-and-continue "
        "pattern the plan's rule B forbids)",
        "test_the_effort_guard_RAISES_rather_than_printing",
        [
            (
                "        if total < 1:\n            raise ValueError(",
                "        if total < 1:\n            print(",
            )
        ],
    ),
    # ---- output grid shape -------------------------------------------------
    (
        "B38",
        "the duplicate-axis check disabled, so a repeated species/cell/week "
        "emits two rows for one grid point and a downstream tensor keeps "
        "whichever it indexed last",
        "test_a_duplicate_species_key_is_rejected",
        [
            (
                "        if value in seen:",
                "        if False:",
            )
        ],
    ),
    (
        "B39",
        "the duplicate check applied to the species axis ONLY, so a repeated "
        "`cells` or `weeks` entry still duplicates grid rows",
        "test_a_duplicate_cell_is_rejected",
        [
            (
                "    if cells is not None:\n"
                '        _validate_grid_axis(list(cells), "cell")',
                "    if cells is not None:\n"
                '        pass  # _validate_grid_axis(list(cells), "cell")',
            )
        ],
    ),
    (
        "B40",
        "the default grid drops unobserved CELLS by iterating the rows' cells "
        "instead of the requested axis, so `cells=` is ignored and an empty "
        "cell never appears in the output at all",
        "test_empty_cell_week_backs_off_without_dividing_by_zero",
        [
            (
                "    all_cells = list(cells) if cells is not None else sorted(cell_n)",
                "    all_cells = sorted(cell_n)",
            )
        ],
    ),
    (
        "B41",
        "the requested `weeks` axis ignored in favour of the observed weeks, "
        "so a week nobody visited is never scored -- the sparse-grid case "
        "this module exists for",
        "test_the_requested_WEEKS_axis_is_honoured_even_when_all_are_unobserved",
        [
            (
                "    all_weeks = (\n"
                "        [int(w) for w in weeks]\n"
                "        if weeks is not None\n"
                "        else sorted({w for _, w in cell_week_total})\n"
                "    )",
                "    all_weeks = sorted({w for _, w in cell_week_total})",
            )
        ],
    ),
    (
        "B42",
        "the output emits only the (species, cell, week) combinations present "
        "in the input, i.e. no dense grid at all: the table becomes a "
        "reweighted copy of its input and every sparse cell is missing",
        "test_the_output_grid_is_the_full_cartesian_product",
        [
            (
                "                out_species.append(sk)",
                "                if (sk, cell, week) not in cell_week_species:\n"
                "                    continue\n"
                "                out_species.append(sk)",
            )
        ],
    ),
    (
        "B43",
        "the output `p` column renamed to `ratio`, breaking the schema the "
        "smoothing and tensor stages consume",
        "test_output_schema_is_exact",
        [
            (
                '            "p": pa.array(out_p, pa.float64()),',
                '            "ratio": pa.array(out_p, pa.float64()),',
            )
        ],
    ),
    (
        "B44",
        "the output `p` emitted as float32, so the floor (1e-6) and the "
        "shrunk values lose precision silently",
        "test_output_schema_is_exact",
        [
            (
                '            "p": pa.array(out_p, pa.float64()),',
                '            "p": pa.array(out_p, pa.float32()),',
            )
        ],
    ),
    (
        "B45",
        "the estimate RENORMALISED over the requested species so each "
        "cell-week sums to 1.0. Looks tidier and is wrong: `p` is a "
        "per-species marginal, and a label set that omits most species would "
        "inflate the survivors to fill the missing mass",
        "test_the_shrunk_output_is_NOT_normalised_over_species",
        [
            (
                "    return pa.table(\n"
                "        {\n"
                '            "species_key": pa.array(out_species, pa.int64()),',
                "    _mass: dict[tuple[str, int], float] = {}\n"
                "    for _i in range(len(out_p)):\n"
                "        _k = (out_cells[_i], out_weeks[_i])\n"
                "        _mass[_k] = _mass.get(_k, 0.0) + out_p[_i]\n"
                "    for _i in range(len(out_p)):\n"
                "        _k = (out_cells[_i], out_weeks[_i])\n"
                "        if _mass[_k]:\n"
                "            out_p[_i] = out_p[_i] / _mass[_k]\n"
                "    return pa.table(\n"
                "        {\n"
                '            "species_key": pa.array(out_species, pa.int64()),',
            )
        ],
    ),
    # ---- cell-week count coherence (the last concat-of-partials door) -------
    (
        "B46",
        "the cell-week COUNT-COHERENCE guard removed, so two partial "
        "RatioTables covering DISJOINT species of one cell-week are accepted: "
        "no duplicate key, agreeing totals, every row satisfying n <= total, "
        "and 60 + 80 against a total of 100 emits shares summing to 1.4",
        "test_cell_week_counts_summing_above_their_own_total_are_rejected",
        [
            (
                "        if n_sum > cell_week_total[(cell, week)]:",
                "        if False:",
            )
        ],
    ),
    (
        "B46b",
        "the coherence guard weakened from `>` to `>=`, which REJECTS the "
        "ordinary complete-table case where a cell-week's listed species "
        "account for every one of its records -- the module's main input",
        "test_a_coherent_cell_week_summing_exactly_to_its_total_is_accepted",
        [
            (
                "        if n_sum > cell_week_total[(cell, week)]:",
                "        if n_sum >= cell_week_total[(cell, week)]:",
            )
        ],
    ),
    (
        "B46c",
        "the coherence sum re-derived from the LAST row per cell-week instead "
        "of accumulated, so the guard only ever sees one species' `n` and the "
        "incoherent concat slips past it",
        "test_cell_week_counts_summing_above_their_own_total_are_rejected",
        [
            (
                "        cell_week_n_sum[key] = cell_week_n_sum.get(key, 0) + n",
                "        cell_week_n_sum[key] = n",
            )
        ],
    ),
    # ---- default axes: no invented fallback week ---------------------------
    (
        "B48",
        "a FALLBACK WEEK invented when the observed week set is empty (the "
        "`if not all_weeks: all_weeks = [1]` this module deliberately does "
        "NOT have), so an empty table under default axes silently returns a "
        "species x cells grid at an arbitrary week 1 instead of no rows",
        "test_an_empty_table_with_DEFAULT_weeks_yields_an_EMPTY_grid",
        [
            (
                "    out_species: list[int] = []",
                "    if not all_weeks:\n"
                "        all_weeks = [1]\n"
                "    out_species: list[int] = []",
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
