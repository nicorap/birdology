#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/smoothing.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_smoothing_mutants.py
    uv run python tests/prior/mutation/run_smoothing_mutants.py --only S1 S7
    uv run python tests/prior/mutation/run_smoothing_mutants.py --verify-labels

Structure is copied from `run_backoff_mutants.py`, including the five things
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
   kill or a survival. This happened in Task 3 and misled two agents;
4. every mutant NAMES the test that kills it, and that name is CHECKED at
   runtime: a mutant that fails only OTHER tests is scored MISLABELLED, not a
   kill, and also exits non-zero. A mutant killed by an unrelated test is
   exactly the "coverage that does not exist" failure mode;
5. `--verify-labels` prints the unified diff each mutant actually applies, so
   each LABEL can be read against its EDIT rather than trusted.

A mutant is KILLED if at least one test fails AND the named killer is among
the failures. Zero failures is a SURVIVOR: the suite does not pin that
behaviour.

COMPOUND CONDITIONS ARE MUTATED PER DISJUNCT
--------------------------------------------
An earlier mutant in this plan deleted a whole `if` and therefore could not
detect the weakening of ONE disjunct inside its condition. Every compound
condition in the target is split here:

* `weeks.min() < 1 or weeks.max() > WEEKS_PER_YEAR`  -> S20, S21 (per clause)
  plus S22 (the whole guard)
* `c != cell and c in code_of`                        -> S6, S7 (per clause)
* `not np.isfinite(value)` / `value < 0.0`            -> S17, S18 (separately)
* `len(resolutions) > 1` / the canonical-spelling
  check sharing its loop                              -> S25, S26 (separately)
* the `p` guard: `not np.all(np.isfinite(values))`
  -> S44; `values.min() < 0.0` -> S45;
  `values.max() > 1.0` -> S46; the whole range
  guard -> S47; the bounds made exclusive -> S48.
  The finiteness clause is mutated apart from the
  range clause because they do NOT subsume each
  other: `nan < 0.0` is False, so a range test
  passes on a nan column, and a finiteness test
  passes on a column of -5.0.

The `if params.space_weight:` / `if params.time_weight:` short-circuits are
NOT mutated per clause, because they carry no semantics: see the S13 note.

WHAT THIS SET DOES NOT COVER -- stated so the table is not read as more than
it is
---------------------------------------------------------------------------
* NUMERICAL CALIBRATION. The default weights (0.3 / 0.3) are a modelling
  choice, not a pinned constant. S11/S12 only prove that each weight reaches
  its own axis and changes the answer; whether 0.3 is the right bandwidth for
  GBIF-scale data is an empirical question no mutation can answer. Likewise
  the CHOICE of a flat ring-1 kernel over a distance-decayed or ring-2 one is
  a modelling decision: a mutant widening the kernel to `grid_disk(cell, 2)`
  IS killed (S8), but only because the tests encode ring-1 as the contract.
* THE BANDWIDTH SWEEP. The plan sweeps `space_weight`/`time_weight`; this
  module only has to honour whatever it is handed. Nothing here checks that
  the sweep's chosen values are sensible.
* FLOAT ASSOCIATIVITY AND ORDER. Accumulating the ring in a different slot
  order gives a bit-different float64. The suite asserts at rel=1e-12, not
  bit-identity, so a reassociation mutant would survive. Accepted: at 1e-12
  the prior is unaffected.
* THE DEFAULT WEIGHTS AS DEFAULTS. S15/S16 change a default and are caught
  only by the one test that relies on both defaults at once. Every other test
  passes explicit weights, which is deliberate (they pin the kernel, not the
  calibration) but means a change to a default is thinly covered: one test
  stands between 0.3 and any other value.
* SCALE, beyond one 249 600-row fixture. `test_a_dense_grid_...` pins peak
  Python allocation under 40 MB, which is enough to catch a re-introduced
  `to_pylist()` dict (103 MB) -- S30 proves that. It would NOT catch a defect
  appearing only at tens of millions of rows, e.g. an int64 overflow in the
  packed key (the packing is bounded by n_cells * 53 * n_species, which
  overflows only far beyond any plausible grid) or quadratic behaviour in
  `_neighbour_codes` for a cell count in the millions.
* THE UPSTREAM SEAM. This harness mutates only `smoothing.py`. That the
  backoff really does emit a dense grid with `p` unnormalised is pinned by
  `test_backoff.py`, not here; `test_smoothing.py` builds its own tables
  rather than running the backoff, so a contract drift in `backoff.py`'s
  OUTPUT would be caught there and not in this table.
* H3 ITSELF. `test_antimeridian_cell_has_six_neighbours` and
  `test_a_pentagon_cell_has_five_neighbours_and_smooths` assert h3's own
  behaviour, so they pin the library version's adjacency but no mutant here
  can change it.
* SEMANTIC EQUIVALENTS OF THE KERNEL. A mutant that renamed both weights and
  swapped both call sites consistently would be a pure relabelling and is not
  tested (S14 catches the swap only because the two axes are tested with
  different values).
* THE SENTINEL HEADROOM AT SCALE. S43 proves the cell radix must be
  `len(cells) + 1` on a 2-species, 4-row ragged fixture. It does NOT prove
  the packed key is free of int64 overflow at national scale (see the SCALE
  bullet), and no mutant here distinguishes `len(cells) + 1` from any LARGER
  radix: over-wide headroom is wasteful but not wrong, so a mutant raising it
  would be equivalent.
* THE `p` GUARD'S REACH. S44-S48 pin that a non-finite or out-of-range `p`
  is REJECTED. They do not pin anything about `p` values that are finite, in
  range and still wrong — a mis-shrunk probability from the backoff is a
  conforming input here and this module has no way to see it. Nor is the
  guard's COST measured: it is one `np.isfinite` pass and two reductions over
  the dense column, and no test bounds that against the 40 MB / dense-grid
  budget beyond the existing `test_a_dense_grid_...` passing with it in place.
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
TARGET = Path("src/birdology/prior/smoothing.py")
TESTS = "tests/prior/test_smoothing.py"

# (id, description, killer_test, [(old, new), ...])
#
# `killer_test` is the test whose failure DEMONSTRATES the mutant is caught.
# It is verified at runtime: a mutant that fails only other tests is reported
# MISLABELLED rather than counted as a kill.
MUTANTS: list[tuple[str, str, str, list[tuple[str, str]]]] = [
    # ---- contract 3: the circular 52 <-> 1 seam, which is THIS module's ----
    (
        "S1",
        "the PREV wrap dropped: `prev` of week 1 becomes week 0, which is in "
        "no grid, so week 1 is smoothed from week 2 alone and late December "
        "never informs early January",
        "test_week_1_is_pulled_by_week_52_across_the_seam",
        [
            (
                "    prev = week - 1 or WEEKS_PER_YEAR",
                "    prev = week - 1",
            )
        ],
    ),
    (
        "S2",
        "the NEXT wrap dropped: `next` of week 52 becomes week 53, in no "
        "grid, so the seam is open from the other side",
        "test_week_52_is_pulled_by_week_1_across_the_seam",
        [
            (
                "    nxt = week + 1 if week < WEEKS_PER_YEAR else 1",
                "    nxt = week + 1",
            )
        ],
    ),
    (
        "S3",
        "the wrap made a SHORTCUT rather than a seam: `prev` of week 1 is "
        "week 51, so the two weeks either side of New Year are blended with "
        "the wrong weeks -- a seam that is closed in the wrong place",
        "test_week_1_is_not_blended_with_week_51",
        [
            (
                "    prev = week - 1 or WEEKS_PER_YEAR",
                "    prev = week - 1 or WEEKS_PER_YEAR - 1",
            )
        ],
    ),
    (
        "S4",
        "the adjacency table's row 0 placeholder REMOVED, so `_ADJACENT[week]` "
        "is off by one and every week is blended with the neighbours of the "
        "week before it",
        "test_the_seam_is_blended_with_the_same_kernel_as_any_other_week",
        [
            (
                "    [(0, 0), *(_adjacent_weeks(w) for w in range(1, WEEKS_PER_YEAR + 1))],",
                "    [*(_adjacent_weeks(w) for w in range(1, WEEKS_PER_YEAR + 1))],",
            )
        ],
    ),
    (
        "S5",
        "only ONE side of the temporal kernel applied (the loop stops after "
        "`prev`), so every week is pulled by its predecessor only -- a "
        "one-sided filter that shifts the whole seasonal profile forward",
        "test_the_seam_is_blended_with_the_same_kernel_as_any_other_week",
        [
            (
                "        for side in range(_ADJACENT.shape[1]):",
                "        for side in range(1):",
            )
        ],
    ),
    # ---- the spatial kernel: h3.grid_disk, ring 1, origin excluded --------
    (
        "S6",
        "the origin cell KEPT in its own ring (`c != cell` dropped from the "
        "compound condition), so each cell counts itself twice and the ring's "
        "influence is diluted by a phantom seventh neighbour",
        "test_a_cell_does_not_count_itself_as_its_own_spatial_neighbour",
        [
            (
                "        [code_of[c] for c in h3.grid_disk(cell, 1) if c != cell and c in code_of]",
                "        [code_of[c] for c in h3.grid_disk(cell, 1) if c in code_of]",
            )
        ],
    ),
    (
        "S7",
        "the in-grid clause (`c in code_of`) dropped from the same compound "
        "condition, so a neighbour outside the grid raises a KeyError instead "
        "of being skipped -- the isolated/boundary-cell case",
        "test_an_absent_neighbour_is_skipped_not_imputed_as_zero",
        [
            (
                "        [code_of[c] for c in h3.grid_disk(cell, 1) if c != cell and c in code_of]",
                "        [code_of[c] for c in h3.grid_disk(cell, 1) if c != cell]",
            )
        ],
    ),
    (
        "S8",
        "the spatial kernel widened to ring 2, so cells two hexes away (up to "
        "~90 km at res 4) are averaged in as if adjacent and the prior is "
        "over-smoothed",
        "test_a_cell_two_rings_away_is_not_a_neighbour",
        [
            (
                "for c in h3.grid_disk(cell, 1) if c != cell",
                "for c in h3.grid_disk(cell, 2) if c != cell",
            )
        ],
    ),
    (
        "S9",
        "only the FIRST ring slot accumulated, so exactly one of the six "
        "neighbours contributes and which one depends on h3's ring order",
        "test_all_six_neighbours_contribute_not_just_the_first",
        [
            (
                "        for slot in range(ring.shape[1]):",
                "        for slot in range(1):",
            )
        ],
    ),
    (
        "S10",
        "the spatial neighbour looked up at the WRONG WEEK (week 1 instead of "
        "the row's own week), so a cell is blended with its neighbours' New "
        "Year values all year long",
        "test_the_spatial_neighbour_must_be_the_SAME_week",
        [
            (
                "            index, found = neighbour_values(pack(ring[cell_code, slot], weeks))",
                "            index, found = neighbour_values(\n"
                "                pack(ring[cell_code, slot], np.ones_like(weeks))\n"
                "            )",
            )
        ],
    ),
    # ---- the two weights reach their own axis ------------------------------
    (
        "S11",
        "`params.space_weight` replaced by the hard-coded default 0.3 in the "
        "SPATIAL accumulation, so a swept bandwidth is silently ignored",
        "test_the_space_weight_is_applied_to_the_SPATIAL_term",
        [
            (
                "            total[found] += params.space_weight * p[index[found]]\n"
                "            weight[found] += params.space_weight",
                "            total[found] += 0.3 * p[index[found]]\n"
                "            weight[found] += 0.3",
            )
        ],
    ),
    (
        "S12",
        "`params.time_weight` replaced by the hard-coded default 0.3 in the "
        "TEMPORAL accumulation, same defect on the other axis",
        "test_the_time_weight_is_applied_to_the_TEMPORAL_term",
        [
            (
                "            total[found] += params.time_weight * p[index[found]]\n"
                "            weight[found] += params.time_weight",
                "            total[found] += 0.3 * p[index[found]]\n"
                "            weight[found] += 0.3",
            )
        ],
    ),
    # S13 REMOVED as an EQUIVALENT MUTANT, not as a kill.
    #
    # It weakened the `params.space_weight` short-circuit to always-true. That
    # guard is a COST guard, not a semantic one: with a weight of 0.0 the loop
    # adds 0.0 * neighbour and 0.0 to the total weight, so the emitted numbers
    # are bit-identical either way.
    #
    # That equivalence is now UNCONDITIONAL over every input this module
    # accepts. It was previously conditional: the argument "0.0 * neighbour
    # adds nothing" fails when the neighbour is nan or inf (0.0 * nan is nan,
    # not 0.0), so before F2's guard the mutant was distinguishable on an
    # input the module then accepted. The verification itself sampled only
    # finite `p` — bit-identical over 270 comparisons — so it covered only the
    # regime it sampled. F2's finiteness clause (S44) is what closes the gap:
    # a non-finite `p` can no longer reach the loop at all, so the sampled
    # regime is now the whole accepted domain and no fixture can distinguish
    # the mutant.
    #
    # No fixture can distinguish it, and a contrived one would be exactly the
    # "coverage that does not exist" this harness exists to prevent. The same
    # applied to the `and p.size` clause, which was DEAD: the empty-table path
    # is correct without it, so the clause was deleted from the source rather
    # than papered over with a mutant. The observable behaviour that the
    # zero-weight path must still VALIDATE its cells is pinned by
    # `test_a_synthetic_cell_is_rejected_even_when_the_space_weight_is_zero`
    # and mutated by S24.
    (
        "S14",
        "the two weights SWAPPED at their accumulation sites, so a sweep that "
        "widens space actually widens time -- the bandwidths are mislabelled "
        "and every swept result is attributed to the wrong axis",
        "test_the_space_weight_is_applied_to_the_SPATIAL_term",
        # Staged through a sentinel: a direct two-step swap would make the
        # second pattern ambiguous after the first edit, and `_apply` would
        # (correctly) report NOT APPLIED.
        [
            (
                "            total[found] += params.space_weight * p[index[found]]\n"
                "            weight[found] += params.space_weight",
                "            total[found] += params.SWAP_weight * p[index[found]]\n"
                "            weight[found] += params.SWAP_weight",
            ),
            (
                "            total[found] += params.time_weight * p[index[found]]\n"
                "            weight[found] += params.time_weight",
                "            total[found] += params.space_weight * p[index[found]]\n"
                "            weight[found] += params.space_weight",
            ),
            (
                "            total[found] += params.SWAP_weight * p[index[found]]\n"
                "            weight[found] += params.SWAP_weight",
                "            total[found] += params.time_weight * p[index[found]]\n"
                "            weight[found] += params.time_weight",
            ),
        ],
    ),
    (
        "S15",
        "the default space_weight raised to 1.0, weighting a neighbour hex as "
        "heavily as the cell itself. Caught by the test that pools one spatial "
        "and one temporal neighbour: the two axes must contribute 0.3 each, so "
        "raising only space breaks the pooled total (the single-axis tests pass "
        "an explicit weight and so cannot see a changed DEFAULT)",
        "test_space_and_time_neighbours_are_pooled_into_one_average",
        [
            (
                "    space_weight: float = 0.3",
                "    space_weight: float = 1.0",
            )
        ],
    ),
    (
        "S16",
        "the default time_weight raised to 1.0, same defect on the temporal "
        "axis, and likewise invisible to the tests that pass an explicit "
        "weight",
        "test_space_and_time_neighbours_are_pooled_into_one_average",
        [
            (
                "    time_weight: float = 0.3",
                "    time_weight: float = 1.0",
            )
        ],
    ),
    (
        "S17",
        "the FINITENESS clause of the weight guard removed, so a nan weight "
        "(truthy) turns the entire `p` column into nan with no error",
        "test_a_nan_weight_is_rejected",
        [
            (
                "        if not np.isfinite(value):",
                "        if False:",
            )
        ],
    ),
    (
        "S18",
        "the NON-NEGATIVITY clause of the same guard removed, so a negative "
        "weight subtracts its neighbour's evidence and can push `p` outside "
        "[0, 1] or divide by a zero total weight. Separate mutant from S17 "
        "because one `if` deletion cannot detect the other clause's loss",
        "test_a_negative_weight_is_rejected",
        [
            (
                "        if value < 0.0:",
                "        if False:",
            )
        ],
    ),
    (
        "S19",
        "the weight guard applied to space_weight ONLY, so a nan or negative "
        "TIME weight slips through the per-axis loop",
        "test_a_negative_weight_is_rejected",
        [
            (
                '        ("space_weight", params.space_weight),\n'
                '        ("time_weight", params.time_weight),',
                '        ("space_weight", params.space_weight),',
            )
        ],
    ),
    # ---- weeks are 1..52 (compound condition, per clause) ------------------
    (
        "S20",
        "the LOWER clause of the week-range guard weakened, so week 0 or a "
        "negative week reaches `_ADJACENT[weeks]` and indexes the table from "
        "the wrong end -- week -1 silently reads week 52's neighbours",
        "test_a_week_outside_1_52_is_rejected",
        [
            (
                "    if weeks.size and (weeks.min() < 1 or weeks.max() > WEEKS_PER_YEAR):",
                "    if weeks.size and (False or weeks.max() > WEEKS_PER_YEAR):",
            )
        ],
    ),
    (
        "S21",
        "the UPPER clause of the same guard weakened, so week 53 (which "
        "upstream folds into 52) indexes past the adjacency table. Mutated "
        "per clause: S22 deletes the whole guard and could not distinguish "
        "the loss of one bound",
        "test_a_week_outside_1_52_is_rejected",
        [
            (
                "    if weeks.size and (weeks.min() < 1 or weeks.max() > WEEKS_PER_YEAR):",
                "    if weeks.size and (weeks.min() < 1 or False):",
            )
        ],
    ),
    (
        "S22",
        "the whole week-range guard removed",
        "test_a_week_outside_1_52_is_rejected",
        [
            (
                "    if weeks.size and (weeks.min() < 1 or weeks.max() > WEEKS_PER_YEAR):",
                "    if False:",
            )
        ],
    ),
    # ---- cells: opaque, canonical, one resolution -------------------------
    (
        "S23",
        "`cell` LOWERCASED before it is coded, a redundant second "
        "normalisation of an already-normalised value that merges two "
        "distinct cells' values into one",
        "test_an_upper_case_cell_is_rejected_rather_than_silently_unmatched",
        [
            (
                '    cell_code, cells = _codes(_single_array(estimates.column("cell")))',
                "    cell_code, cells = _codes(\n"
                '        pa.compute.utf8_lower(_single_array(estimates.column("cell")))\n'
                "    )",
            )
        ],
    ),
    (
        "S24",
        "the cell guard skipped entirely, so a mixed-resolution grid or a "
        "non-canonical spelling matches no neighbour and the spatial term "
        "becomes a table-wide no-op. A synthetic name like the 'c' upstream "
        "fixtures use still raises from `grid_disk`, so the killer is the "
        "ZERO-SPACE-WEIGHT case, where nothing else ever touches h3",
        "test_a_synthetic_cell_is_rejected_even_when_the_space_weight_is_zero",
        [
            (
                "    _validate_cells(cells)",
                "    pass  # _validate_cells(cells)",
            )
        ],
    ),
    (
        "S25",
        "the MIXED-RESOLUTION clause of the cell guard removed, so a res-4 "
        "and a res-5 hex coexist: `grid_disk` answers for both but they are "
        "never each other's neighbours, so smoothing silently does nothing",
        "test_cells_at_two_h3_resolutions_are_rejected",
        [
            (
                "    if len(resolutions) > 1:",
                "    if False:",
            )
        ],
    ),
    (
        "S26",
        "the CANONICAL-SPELLING clause of the same guard removed (a separate "
        "mutant from S25, which shares its loop), so an upper-case cell is "
        "accepted, matches no lower-case neighbour and is left unsmoothed",
        "test_an_upper_case_cell_is_rejected_rather_than_silently_unmatched",
        [
            (
                "        if h3.int_to_str(h3.str_to_int(cell)) != cell:",
                "        if False:",
            )
        ],
    ),
    (
        "S27",
        "the species digit dropped from the packed key, so two species in the "
        "same cell-week collide: each is blended with the other's neighbours "
        "and the duplicate guard fires on legitimate multi-species grids",
        "test_the_spatial_neighbour_must_be_the_SAME_species",
        [
            (
                "        return (species * cell_stride + cell_digit) * week_stride + week_digit",
                "        return cell_digit * week_stride + week_digit",
            )
        ],
    ),
    (
        "S28",
        "the 'absent neighbour' sentinel collides with a real cell code "
        "(`absent = len(cells)` becomes 0), so a row whose neighbour is "
        "missing is instead blended with cell 0. Needs a RAGGED ring (a cell "
        "with two in-grid neighbours beside two cells with one each), or the "
        "padding column never exists and the mutant is unreachable",
        "test_cells_with_different_neighbour_counts_are_smoothed_independently",
        [
            (
                "    absent = len(cells)",
                "    absent = 0",
            )
        ],
    ),
    # ---- contracts 2 and 6: never renormalise; the own-weight is 1.0 ------
    (
        "S29",
        "the own-value weight initialised to 0.0 instead of 1.0. An isolated "
        "cell then divides by zero and emits nan; every other cell is "
        "reweighted so its own value counts for nothing",
        "test_an_isolated_cell_keeps_its_own_value_and_is_not_nan",
        [
            (
                "    weight = np.ones(p.shape, dtype=np.float64)",
                "    weight = np.zeros(p.shape, dtype=np.float64)",
            )
        ],
    ),
    (
        "S30",
        "the output RENORMALISED over the requested species so each cell-week "
        "sums to 1.0. Looks tidier and is wrong: `p` is a per-species "
        "marginal, so a label set that omits most species would inflate the "
        "survivors to fill the missing mass",
        "test_smoothing_does_NOT_renormalise_over_species",
        [
            (
                "    return pa.table(\n"
                "        {\n"
                '            "species_key": estimates.column("species_key"),',
                "    _smoothed = total / weight\n"
                "    _cw = cell_code * (WEEKS_PER_YEAR + 1) + weeks\n"
                "    if _smoothed.size:\n"
                "        _mass = np.zeros(int(_cw.max()) + 1, dtype=np.float64)\n"
                "        np.add.at(_mass, _cw, _smoothed)\n"
                "        _denominator = _mass[_cw]\n"
                "        _smoothed = np.where(\n"
                "            _denominator > 0, _smoothed / _denominator, _smoothed\n"
                "        )\n"
                "    total, weight = _smoothed, np.ones_like(_smoothed)\n"
                "    return pa.table(\n"
                "        {\n"
                '            "species_key": estimates.column("species_key"),',
            )
        ],
    ),
    (
        "S31",
        "the dict-of-every-cell-week implementation restored: `to_pylist()` "
        "over the DENSE grid with a Python tuple key per row. Numerically "
        "identical and 4x the peak memory, growing linearly into the national "
        "run. This is the brief's own draft, kept as a mutant so the scale "
        "bound is proven reachable rather than asserted",
        "test_a_dense_grid_smooths_without_materialising_every_row_in_python",
        [
            (
                "    weeks = _numeric(estimates.column(\"week\"), np.int64)",
                "    _rows = estimates.to_pylist()\n"
                "    _values = {\n"
                '        (r["species_key"], r["cell"], int(r["week"])): r["p"]\n'
                "        for r in _rows\n"
                "    }\n"
                "    weeks = _numeric(estimates.column(\"week\"), np.int64)",
            )
        ],
    ),
    # ---- input validation --------------------------------------------------
    (
        "S32",
        "`_validate_table` never called: a table missing `p` or carrying an "
        "int64 `week` dies on a raw KeyError/ArrowInvalid deep inside the "
        "arithmetic instead of a ValueError naming the contract",
        "test_a_table_missing_the_p_column_is_rejected",
        [
            (
                "    _validate_table(estimates)",
                "    pass  # _validate_table(estimates)",
            )
        ],
    ),
    (
        "S33",
        "the dtype check CASTS instead of raising -- a second derivation of a "
        "value derived once upstream, accepting an int64 `week` from a "
        "bypassed pipeline",
        "test_a_wrong_input_dtype_is_rejected_not_cast",
        [
            (
                "        if actual != expected:",
                "        if False and actual != expected:",
            )
        ],
    ),
    (
        "S34",
        "the null check removed: a null key matches no neighbour and a null "
        "`p` propagates nan into every neighbour that reads it, neither with "
        "an error",
        "test_a_null_in_a_key_or_in_p_is_rejected",
        [
            (
                "        if estimates.column(name).null_count:",
                "        if False:",
            )
        ],
    ),
    # The `sorted_keys.size > 1 and` conjunct S35's anchor used to carry is
    # GONE FROM THE SOURCE, and no mutant replaces it -- deliberately, and for
    # the same reason as the `and p.size` clause noted at S13. `np.diff` of a
    # 0- or 1-element array is empty and `np.any` of an empty array is False,
    # so the conjunct could never decide the branch: every input it was
    # supposed to protect already takes the correct path without it. A mutant
    # of a clause that cannot change an output would be unkillable by
    # construction and would sit in this table as permanent NOT APPLIED or a
    # survivor. What IS mutable is the check itself, which S35 covers; the
    # short-table behaviour it used to claim to guard is pinned directly by
    # `test_a_TABLE_TOO_SHORT_TO_HOLD_A_DUPLICATE_needs_no_size_guard`, which
    # asserts the numpy fact and both reachable table lengths.
    (
        "S35",
        "the duplicate-grid-point check removed, so a table concatenated from "
        "two partial grids lets each neighbour read whichever copy "
        "`searchsorted` lands on -- an output that depends on row order",
        "test_a_duplicate_species_cell_week_row_is_rejected",
        [
            (
                "    if np.any(np.diff(sorted_keys) == 0):",
                "    if False:",
            )
        ],
    ),
    (
        "S36",
        "the validation WARNS instead of raising (the warn-and-continue "
        "pattern the plan's rule B forbids)",
        "test_a_null_in_a_key_or_in_p_is_rejected",
        [
            (
                "        if estimates.column(name).null_count:\n            raise ValueError(",
                "        if estimates.column(name).null_count:\n            print(",
            )
        ],
    ),
    # ---- output shape ------------------------------------------------------
    (
        "S37",
        "the smoothed `p` attached in SORTED key order rather than input row "
        "order, so every value is paired with the wrong (species, cell, week) "
        "-- the keys are passed through unchanged, so nothing raises. The "
        "fixture must be in DESCENDING week order or the permutation is the "
        "identity and the mutant is unreachable",
        "test_row_ORDER_is_preserved_exactly",
        [
            (
                '            "p": pa.array(total / weight, pa.float64()),',
                '            "p": pa.array((total / weight)[order], pa.float64()),',
            )
        ],
    ),
    (
        "S38",
        "the output `p` emitted as float32, so the floor (1e-6) the backoff "
        "guarantees and the smoothed values lose precision silently",
        "test_output_schema_types_are_exact",
        [
            (
                '            "p": pa.array(total / weight, pa.float64()),',
                '            "p": pa.array(total / weight, pa.float32()),',
            )
        ],
    ),
    (
        "S39",
        "the output `p` column renamed to `ratio`, breaking the schema the "
        "tensor stage consumes",
        "test_schema_is_preserved",
        [
            (
                '            "p": pa.array(total / weight, pa.float64()),',
                '            "ratio": pa.array(total / weight, pa.float64()),',
            )
        ],
    ),
    (
        "S40",
        "the chunked input read from its FIRST CHUNK only, so a multi-chunk "
        "table silently smooths a prefix of itself and the row count changes",
        "test_a_CHUNKED_input_smooths_identically_to_a_contiguous_one",
        [
            (
                "        return column.combine_chunks()",
                "        return column.chunk(0) if column.num_chunks else column.combine_chunks()",
            )
        ],
    ),
    (
        "S41",
        "the neighbour's value read from the WRONG ROW (the searchsorted "
        "insertion point used as a row index without mapping back through the "
        "sort order), so each cell is blended with whatever row happens to "
        "sit at that position in sorted order",
        "test_a_neighbour_is_read_from_the_row_that_ACTUALLY_HOLDS_it",
        [
            (
                "        return order[position], sorted_keys[position] == wanted",
                "        return position, sorted_keys[position] == wanted",
            )
        ],
    ),
    (
        "S42",
        "the found-mask dropped (every lookup treated as a hit), so a "
        "neighbour outside the grid contributes the value of whichever row "
        "`searchsorted` clamped to -- a boundary cell blended with a "
        "stranger, with no error",
        "test_an_absent_temporal_neighbour_is_skipped_not_imputed_as_zero",
        [
            (
                "        return order[position], sorted_keys[position] == wanted",
                "        return order[position], np.ones(wanted.shape, dtype=bool)",
            )
        ],
    ),
    # ---- the sentinel headroom in the packed key --------------------------
    (
        "S43",
        "the SENTINEL HEADROOM removed from the cell radix (`len(cells) + 1` "
        "-> `len(cells)`), so the 'no such neighbour' code `len(cells)` no "
        "longer has a digit of its own: it carries into the SPECIES digit and "
        "the absent-neighbour key lands on a real row of another species. A "
        "boundary cell is silently blended with a stranger -- no error, no "
        "nan, no missing row. Reaching it needs >= 2 species AND a ragged "
        "ring at once; every older multi-species fixture is 2 rows, so the "
        "sentinel had no real row to alias and this SURVIVED the pre-fix "
        "suite",
        "test_the_absent_neighbour_sentinel_cannot_alias_another_species_row",
        [
            (
                "    cell_stride = len(cells) + 1",
                "    cell_stride = len(cells)",
            )
        ],
    ),
    # The mirror mutant `week_stride = WEEKS_PER_YEAR + 1` -> `WEEKS_PER_YEAR`
    # is NOT registered: it is EQUIVALENT. Weeks are validated to 1..52 before
    # the key is packed and there is no week sentinel, so week digit 0 is
    # unreachable and `WEEKS_PER_YEAR` is an equally injective radix. Verified
    # empirically: 0 divergences over 60 dense grids. The two `+ 1`s look
    # symmetric and are not -- see the comment at that line in the target.
    # ---- `p` is a probability: finite and in [0, 1] (per disjunct) --------
    (
        "S44",
        "the FINITENESS clause of the `p` guard removed, so a nan or inf `p` "
        "is accepted: one nan at the centre of a ring turns every cell that "
        "reads it into nan, with no error. Arrow's `null_count` cannot see "
        "this -- it is the same defect as a null `p` in a form the null guard "
        "misses",
        "test_a_non_finite_p_is_rejected",
        [
            (
                "    if not np.all(np.isfinite(values)):",
                "    if False:",
            )
        ],
    ),
    (
        "S45",
        "the LOWER bound of the `p` range guard weakened, so a negative `p` "
        "reaches the kernel; this module deliberately does not clamp, so "
        "`p = -5.0` is carried into the output as -3.73 and -0.77. Separate "
        "from S44: nan does not compare below zero, so a finiteness test "
        "cannot detect the loss of this clause",
        "test_a_p_below_zero_is_rejected",
        [
            (
                "    if values.size and (values.min() < 0.0 or values.max() > 1.0):",
                "    if values.size and (False or values.max() > 1.0):",
            )
        ],
    ),
    (
        "S46",
        "the UPPER bound of the same guard weakened, so `p = 7.0` is accepted "
        "and emitted as 5.5 and 2.0. Mutated separately from S45 because one "
        "comparison can be lost without the other, and separately from S47 "
        "(the whole guard), which could not distinguish the loss of one bound",
        "test_a_p_above_one_is_rejected",
        [
            (
                "    if values.size and (values.min() < 0.0 or values.max() > 1.0):",
                "    if values.size and (values.min() < 0.0 or False):",
            )
        ],
    ),
    (
        "S47",
        "the whole `p` range guard removed",
        "test_a_p_below_zero_is_rejected",
        [
            (
                "    if values.size and (values.min() < 0.0 or values.max() > 1.0):",
                "    if False:",
            )
        ],
    ),
    (
        "S48",
        "the range bounds made EXCLUSIVE, rejecting `p = 0.0` and `p = 1.0`. "
        "Both are legitimate probabilities the backoff emits, so this turns a "
        "guard against malformed input into a guard against conforming input",
        "test_the_bounds_of_p_are_INCLUSIVE",
        [
            (
                "    if values.size and (values.min() < 0.0 or values.max() > 1.0):",
                "    if values.size and (values.min() <= 0.0 or values.max() >= 1.0):",
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
