#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/effort.py`.

Committed deliberately: the leakage harness was initially left uncommitted and
that was a review finding. A mutation table that cannot be re-derived is a
claim, not evidence.

Run from the repository root:

    uv run python tests/prior/mutation/run_effort_mutants.py
    uv run python tests/prior/mutation/run_effort_mutants.py --only E3 E4

Structure is copied from `run_leakage_mutants.py`, including the three things
that make the result trustworthy:

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
Three mutants in this plan produced a passing table that meant nothing, each in
a different way:

* M33 (leakage): the mutant's LABEL did not match the edit it actually made,
  so the row credited a kill to behaviour nothing tested.
* E20 (effort): same mismatch, and the mutation the label DESCRIBED was a
  no-op on every fixture -- it could not have been killed as written.
* E33 (effort): label and edit agreed, and the test was correctly written, but
  its FIXTURE never reached the mutated path (the multi-chunk join happened to
  preserve row order), so the coverage was imaginary and E33 survived.

All three create the same false confidence. THE RULE: a green mutation table
means nothing unless each mutant's label matches its edit AND some fixture
actually reaches the mutated path. Where a test depends on a precondition
(multi-chunk, reordering, mixed case, non-empty, multi-threaded), ASSERT THE
PRECONDITION in the test -- and where the precondition is a property of the
MACHINE rather than of the fixture, FORCE it (see the `parallel_arrow`
fixture) rather than skipping, since a skip hides the lost coverage under a
"Survivors: none" banner just as effectively as a vacuous pass.

THE FOURTH VARIANT -- ONE MUTANT PER DISJUNCT, NOT PER STATEMENT
----------------------------------------------------------------
A mutant that deletes a WHOLE `if` cannot detect the weakening of ONE
DISJUNCT inside its condition. E29/E30/E31 each replace an entire condition
with `False`, so they prove the check EXISTS -- not that every clause of it is
load-bearing. Coarse mutants give coarse guarantees.

This was not theoretical. With E29/E30/E31 all killed and the table green,
deleting just `ratio.null_count or` from the finiteness guard left the whole
suite passing, and so did deleting just `pc.min(ratio).as_py() < 0.0 or` from
the range guard. In the first case the surviving clause genuinely cannot
substitute: `pc.all(pc.is_finite([0.5, None]))` is **True**, because Arrow's
`is_finite` returns null for a null element and `pc.all` skips nulls. A null
share with a valid total escaped the post-condition entirely.

The reason is always the same: the existing fixture trips BOTH disjuncts at
once, so either one alone still raises and the deletion is invisible.

    RULE: for any compound condition (`a or b`, `a and b`), add one mutant
    PER DISJUNCT, not one per statement. Each such mutant needs a fixture in
    which ONLY that disjunct can fire -- the other clause must be SATISFIED,
    not merely absent.

E29a/E29b, E30a/E30b, E31a/E31b and E10a/E10b below are that per-disjunct
layer. Where a disjunct is genuinely unreachable in isolation, say so in the
label with the reason rather than inventing a contrived assertion: an
honestly-recorded equivalent mutant is a real result, a fabricated kill is not.
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
TARGET = Path("src/birdology/prior/effort.py")
TESTS = "tests/prior/test_effort.py"

# (id, description, [(old, new), ...])
MUTANTS: list[tuple[str, str, list[tuple[str, str]]]] = [
    # ---- the ratio itself -------------------------------------------------
    (
        "E1",
        "ratio inverted: total/n instead of n/total",
        [
            (
                "    ratio = pc.divide(\n"
                '        pc.cast(joined.column("n"), pa.float64()),\n'
                '        pc.cast(joined.column("total"), pa.float64()),\n'
                "    )",
                "    ratio = pc.divide(\n"
                '        pc.cast(joined.column("total"), pa.float64()),\n'
                '        pc.cast(joined.column("n"), pa.float64()),\n'
                "    )",
            )
        ],
    ),
    (
        "E2",
        "INTEGER division: the float64 casts dropped, so 1/3 becomes 0 "
        "(a silently all-zero prior)",
        [
            (
                "    ratio = pc.divide(\n"
                '        pc.cast(joined.column("n"), pa.float64()),\n'
                '        pc.cast(joined.column("total"), pa.float64()),\n'
                "    )",
                "    ratio = pc.cast(\n"
                '        pc.divide(joined.column("n"), joined.column("total")), pa.float64()\n'
                "    )",
            )
        ],
    ),
    (
        "E3",
        "ratio is the raw count: no effort correction at all",
        [
            (
                "    ratio = pc.divide(\n"
                '        pc.cast(joined.column("n"), pa.float64()),\n'
                '        pc.cast(joined.column("total"), pa.float64()),\n'
                "    )",
                '    ratio = pc.cast(joined.column("n"), pa.float64())',
            )
        ],
    ),
    # ---- the denominator's grouping --------------------------------------
    #
    # HONEST LABELLING NOTE (round-1 finding F6). E4 and E5 both regroup AND
    # change the totals table's width, so the join fails and they are killed
    # by the post-join ROW-COUNT INVARIANT, not by any assertion on the
    # denominator's value. They are kept because a real refactor would break
    # this way, but the grouping itself is pinned by E4b/E5b/E5c below, which
    # preserve row counts and therefore reach the ratio assertions.
    (
        "E4",
        "denominator grouped by CELL only, width-changing -- killed by the "
        "post-join ROW-COUNT INVARIANT, not by a wrong-denominator assertion",
        [
            (
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])',
                '        .group_by(["cell"])\n'
                '        .aggregate([("n", "sum"), ("week", "min")])\n'
                '        .rename_columns(["cell", "total", "week"])',
            )
        ],
    ),
    (
        "E5",
        "denominator grouped by WEEK only, width-changing -- killed by the "
        "post-join ROW-COUNT INVARIANT, not by a wrong-denominator assertion",
        [
            (
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])',
                '        .group_by(["week"])\n'
                '        .aggregate([("n", "sum"), ("cell", "min")])\n'
                '        .rename_columns(["week", "total", "cell"])',
            )
        ],
    ),
    (
        "E4b",
        "ROW-COUNT-PRESERVING true CELL-ONLY pooling: the total is summed "
        "over every week in the cell, so a species' share is diluted by the "
        "rest of the year. The join still matches on (cell, week), so this "
        "is killed by the RATIO VALUES, not by the row-count invariant",
        [
            (
                '        counts.select(["cell", "week", "n"])\n'
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])',
                '        counts.select(["cell", "week", "n"])\n'
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])\n'
                "    )\n"
                "    _cell_only = (\n"
                '        counts.select(["cell", "n"])\n'
                '        .group_by(["cell"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "total"])\n'
                "    )\n"
                "    _m = dict(\n"
                "        zip(\n"
                '            _cell_only.column("cell").to_pylist(),\n'
                '            _cell_only.column("total").to_pylist(),\n'
                "        )\n"
                "    )\n"
                "    totals = totals.set_column(\n"
                '        totals.column_names.index("total"),\n'
                '        "total",\n'
                "        pa.array(\n"
                '            [_m[c] for c in totals.column("cell").to_pylist()], pa.int64()\n'
                "        ),\n"
                "    )\n"
                "    totals = (\n"
                "        totals",
            )
        ],
    ),
    (
        "E5b",
        "ROW-COUNT-PRESERVING true WEEK-ONLY pooling: the total is summed "
        "over every cell in the week, so Copenhagen's effort leaks into west "
        "Jutland's ratios. Killed by the RATIO VALUES",
        [
            (
                '        counts.select(["cell", "week", "n"])\n'
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])',
                '        counts.select(["cell", "week", "n"])\n'
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])\n'
                "    )\n"
                "    _week_only = (\n"
                '        counts.select(["week", "n"])\n'
                '        .group_by(["week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["week", "total"])\n'
                "    )\n"
                "    _m = dict(\n"
                "        zip(\n"
                '            _week_only.column("week").to_pylist(),\n'
                '            _week_only.column("total").to_pylist(),\n'
                "        )\n"
                "    )\n"
                "    totals = totals.set_column(\n"
                '        totals.column_names.index("total"),\n'
                '        "total",\n'
                "        pa.array(\n"
                '            [_m[w] for w in totals.column("week").to_pylist()], pa.int64()\n'
                "        ),\n"
                "    )\n"
                "    totals = (\n"
                "        totals",
            )
        ],
    ),
    (
        "E5c",
        "ROW-COUNT-PRESERVING GLOBAL denominator: every row divided by the "
        "table-wide sum of n, so effort cancels across the whole dataset "
        "instead of within a cell-week. Killed by the RATIO VALUES",
        [
            (
                '        counts.select(["cell", "week", "n"])\n'
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])',
                '        counts.select(["cell", "week", "n"])\n'
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])\n'
                "    )\n"
                '    _g = pc.sum(counts.column("n")).as_py()\n'
                "    totals = totals.set_column(\n"
                '        totals.column_names.index("total"),\n'
                '        "total",\n'
                "        pa.array([_g] * totals.num_rows, pa.int64()),\n"
                "    )\n"
                "    totals = (\n"
                "        totals",
            )
        ],
    ),
    (
        "E6",
        "denominator grouped by (cell, week, species): every ratio becomes "
        "1.0 -- the correction is a no-op that still looks like a share",
        [
            (
                '        counts.select(["cell", "week", "n"])\n'
                '        .group_by(["cell", "week"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])',
                '        counts.select(["cell", "week", "species_key", "n"])\n'
                '        .group_by(["cell", "week", "species_key"])\n'
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "species_key", "total"])',
            ),
            (
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")',
                "    joined = counts.join(\n"
                '        totals, keys=["cell", "week", "species_key"], join_type="inner"\n'
                "    )",
            ),
        ],
    ),
    # ---- the aggregation function ----------------------------------------
    (
        "E7",
        "total is a MAX, not a sum: the busiest species defines the "
        "denominator (upstream contract note 3)",
        [('        .aggregate([("n", "sum")])', '        .aggregate([("n", "max")])')],
    ),
    (
        "E8",
        "total is a ROW COUNT, not a sum of n",
        [
            (
                '        .aggregate([("n", "sum")])\n'
                '        .rename_columns(["cell", "week", "total"])',
                '        .aggregate([("n", "count")])\n'
                '        .rename_columns(["cell", "week", "total"])',
            )
        ],
    ),
    (
        "E9",
        "total is a MIN of n",
        [('        .aggregate([("n", "sum")])', '        .aggregate([("n", "min")])')],
    ),
    # ---- rule (D): zero / absent denominator ------------------------------
    (
        "E10",
        "(D) the n >= 1 check removed entirely: 0/0 -> nan flows into the prior",
        [
            (
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:',
                "    if False:",
            )
        ],
    ),
    (
        "E11",
        "(D) off-by-one: n == 0 accepted, so a lone zero-count cell-week "
        "yields nan instead of raising",
        [
            (
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:',
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 0:',
            )
        ],
    ),
    (
        "E12",
        "(D) the n check uses MAX, so it only fires when EVERY count is "
        "non-positive -- a single poisoned row slips through",
        [
            (
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:',
                '    if counts.num_rows and pc.max(counts.column("n")).as_py() < 1:',
            )
        ],
    ),
    (
        "E13",
        "(D) the n check WARNS instead of raising (the warn-and-continue "
        "pattern rule B forbids)",
        [
            (
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:\n'
                "        raise ValueError(",
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:\n'
                "        print(",
            )
        ],
    ),
    (
        "E13b",
        "(D) the n check weakened from a PER-ROW min to a TABLE-WIDE sum: "
        "n=[5,-5,10] then passes (sum 10 > 0) while cell 'c' totals ZERO, "
        "emitting total=0 with ratio=+inf and -inf",
        [
            (
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:',
                '    if counts.num_rows and pc.sum(counts.column("n")).as_py() < 1:',
            )
        ],
    ),
    # PER-DISJUNCT layer for `counts.num_rows and pc.min(n) < 1` (see header).
    (
        "E10a",
        "(per-disjunct) the `counts.num_rows` guard deleted, leaving the bare "
        "`pc.min(n) < 1`. On an EMPTY table `pc.min` is None and the "
        "comparison raises TypeError, so the empty-input contract dies on a "
        "raw TypeError instead of returning an empty RatioTable",
        [
            (
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:',
                '    if pc.min(counts.column("n")).as_py() < 1:',
            )
        ],
    ),
    (
        "E10b",
        "(per-disjunct) the `pc.min(n) < 1` half deleted, leaving `if "
        "counts.num_rows:` -- EVERY non-empty table is now rejected, so the "
        "guard fires on valid input rather than on a violated contract",
        [
            (
                '    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:',
                "    if counts.num_rows:",
            )
        ],
    ),
    # ---- rule (D): null keys silently drop rows ---------------------------
    (
        "E14",
        "(D) the null check removed: a null cell/week joins to nothing and "
        "the row VANISHES from the prior with no error",
        [
            (
                '    for name in ("cell", "week", "n"):\n'
                "        if counts.column(name).null_count:",
                '    for name in ("cell", "week", "n"):\n'
                "        if False:",
            )
        ],
    ),
    (
        "E15",
        "(D) null check covers cell only, not week or n",
        [
            (
                '    for name in ("cell", "week", "n"):',
                '    for name in ("cell",):',
            )
        ],
    ),
    (
        "E16",
        "(D) null check covers week and n but not cell",
        [
            (
                '    for name in ("cell", "week", "n"):',
                '    for name in ("week", "n"):',
            )
        ],
    ),
    # PER-COLUMN layer for the null loop. E15/E16 each drop TWO columns at
    # once, so a kill only proves that SOME dropped column was checked. These
    # three skip exactly one column each, the way E18a-E18d do for dtypes.
    (
        "E14a",
        "(per-column) the null check SKIPS `cell` only: a null cell joins to "
        "nothing and the row vanishes from the prior",
        [
            (
                '    for name in ("cell", "week", "n"):\n'
                "        if counts.column(name).null_count:",
                '    for name in ("cell", "week", "n"):\n'
                '        if counts.column(name).null_count and name != "cell":',
            )
        ],
    ),
    (
        "E14b",
        "(per-column) the null check SKIPS `week` only",
        [
            (
                '    for name in ("cell", "week", "n"):\n'
                "        if counts.column(name).null_count:",
                '    for name in ("cell", "week", "n"):\n'
                '        if counts.column(name).null_count and name != "week":',
            )
        ],
    ),
    (
        "E14c",
        "(per-column) the null check SKIPS `n` only: a null count reaches the "
        "sum, where Arrow skips it, so the denominator silently omits a row",
        [
            (
                '    for name in ("cell", "week", "n"):\n'
                "        if counts.column(name).null_count:",
                '    for name in ("cell", "week", "n"):\n'
                '        if counts.column(name).null_count and name != "n":',
            )
        ],
    ),
    (
        "E17",
        "(D) a LEFT OUTER join hides the dropped-row failure by keeping the "
        "row with a null total instead. NOTE: equivalent under `_validate` "
        "alone -- only the post-join row-count invariant makes it observable",
        [
            (
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")',
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="left outer")',
            )
        ],
    ),
    (
        "E17b",
        "(D) the post-join row-count invariant removed: with validation "
        "bypassed, a null key silently drops the row from the prior",
        [
            (
                "    if joined.num_rows != counts.num_rows:",
                "    if False:",
            )
        ],
    ),
    (
        "E17c",
        "(D) the row-count invariant compares against ITSELF, so it can "
        "never fire (a vacuous guard that reads as coverage)",
        [
            (
                "    if joined.num_rows != counts.num_rows:",
                "    if joined.num_rows != joined.num_rows:",
            )
        ],
    ),
    # ---- rule (C): no local re-derivation of week / cell ------------------
    (
        "E18",
        "(C) ALL FOUR dtype checks disabled at once: a wrong type is "
        "accepted rather than rejected. NOTE: this mutant does not cast "
        "anything -- the per-column mutants E18a-E18d below are what pin the "
        "individual checks",
        [
            (
                "        actual = counts.schema.field(name).type\n"
                "        if actual != expected:",
                "        actual = counts.schema.field(name).type\n"
                "        if False:",
            )
        ],
    ),
    (
        "E18a",
        "(C) the `week` dtype check skipped: an int64 week is accepted",
        [
            (
                "        actual = counts.schema.field(name).type\n"
                "        if actual != expected:",
                "        actual = counts.schema.field(name).type\n"
                '        if actual != expected and name != "week":',
            )
        ],
    ),
    (
        "E18b",
        "(C) the `cell` dtype check skipped: a binary/dictionary cell is "
        "accepted and joins by a different comparison",
        [
            (
                "        actual = counts.schema.field(name).type\n"
                "        if actual != expected:",
                "        actual = counts.schema.field(name).type\n"
                '        if actual != expected and name != "cell":',
            )
        ],
    ),
    (
        "E18c",
        "(C) the `n` dtype check skipped: a float n is accepted, so `n` has "
        "quietly stopped being a count",
        [
            (
                "        actual = counts.schema.field(name).type\n"
                "        if actual != expected:",
                "        actual = counts.schema.field(name).type\n"
                '        if actual != expected and name != "n":',
            )
        ],
    ),
    (
        "E18d",
        "(C) the `species_key` dtype check skipped: an int32 key silently "
        "truncates GBIF keys above 2**31",
        [
            (
                "        actual = counts.schema.field(name).type\n"
                "        if actual != expected:",
                "        actual = counts.schema.field(name).type\n"
                '        if actual != expected and name != "species_key":',
            )
        ],
    ),
    (
        "E19",
        "(C) week widened to int64 on output",
        [
            (
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")',
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")\n'
                "    joined = joined.set_column(\n"
                '        joined.column_names.index("week"),\n'
                '        "week",\n'
                '        pc.cast(joined.column("week"), pa.int64()),\n'
                "    )",
            )
        ],
    ),
    (
        "E20",
        "(C) cell UPPERCASED locally, breaking the join against `totals`. "
        "RELABELLED in round 1 (finding F3): the old label claimed this was "
        "a casefold, which is the opposite transform",
        [
            (
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")',
                "    counts = counts.set_column(\n"
                '        counts.column_names.index("cell"),\n'
                '        "cell",\n'
                '        pc.utf8_upper(counts.column("cell")),\n'
                "    )\n"
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")',
            )
        ],
    ),
    (
        "E20b",
        "(C) cell LOWERCASED locally -- a genuinely REDUNDANT second "
        "normalisation of an already-normalised value. This is the mutation "
        "E20's old label described; it survived until the suite gained a "
        "mixed-case fixture, since on lowercase input it is a pure no-op",
        [
            (
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")',
                "    counts = counts.set_column(\n"
                '        counts.column_names.index("cell"),\n'
                '        "cell",\n'
                '        pc.utf8_lower(counts.column("cell")),\n'
                "    )\n"
                '    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")',
            )
        ],
    ),
    (
        "E20c",
        "(C) cell lowercased on BOTH sides of the join, so the join still "
        "matches and only the emitted `cell` string changes -- the quietest "
        "form of a second derivation",
        [
            (
                '    totals = (\n        counts.select(["cell", "week", "n"])',
                "    counts = counts.set_column(\n"
                '        counts.column_names.index("cell"),\n'
                '        "cell",\n'
                '        pc.utf8_lower(counts.column("cell")),\n'
                "    )\n"
                "    totals = (\n"
                '        counts.select(["cell", "week", "n"])',
            )
        ],
    ),
    (
        "E21",
        "the missing-column check removed: a KeyError surfaces later, or "
        "worse, a partial table passes",
        [
            (
                "        if name not in counts.column_names:",
                "        if False and name not in counts.column_names:",
            )
        ],
    ),
    (
        "E22",
        "_validate never called",
        [("    _validate(counts)", "    pass  # _validate(counts)")],
    ),
    # ---- output shape ------------------------------------------------------
    (
        "E23",
        "output column ORDER scrambled (Task 6 reads positionally at its peril)",
        [
            (
                '        ["species_key", "cell", "week", "n", "total", "ratio"]',
                '        ["species_key", "cell", "week", "total", "n", "ratio"]',
            )
        ],
    ),
    (
        "E24",
        "the `total` column dropped from the output",
        [
            (
                '        ["species_key", "cell", "week", "n", "total", "ratio"]',
                '        ["species_key", "cell", "week", "n", "ratio"]',
            )
        ],
    ),
    (
        "E25",
        "empty input returns a bare CountTable (no total/ratio columns), so "
        "Task 6 sees a schema that changes with the data",
        [
            (
                "        return counts.append_column(\n"
                '            "total", pa.array([], pa.int64())\n'
                '        ).append_column("ratio", pa.array([], pa.float64()))',
                "        return counts",
            )
        ],
    ),
    (
        "E26",
        "empty input gets a float64 `total` instead of int64: the empty and "
        "populated schemas disagree",
        [
            (
                '            "total", pa.array([], pa.int64())\n'
                '        ).append_column("ratio", pa.array([], pa.float64()))',
                '            "total", pa.array([], pa.float64())\n'
                '        ).append_column("ratio", pa.array([], pa.float64()))',
            )
        ],
    ),
    (
        "E27",
        "empty input raises instead of returning an empty RatioTable",
        [
            (
                "    if counts.num_rows == 0:",
                "    if counts.num_rows == 0:\n"
                '        raise ValueError("empty")\n'
                "    if False:",
            )
        ],
    ),
    # ---- the OUTPUT post-condition (round-1 finding F2) --------------------
    (
        "E28",
        "the output post-condition never called: a KEPT-but-poisoned row "
        "(total=0, ratio=+/-inf) flows into the prior with the row count "
        "intact, so the join invariant stays silent",
        [("    _check_output(out)", "    pass  # _check_output(out)")],
    ),
    (
        "E29",
        "the post-condition's `total >= 1` check removed: a zero or negative "
        "denominator is emitted",
        [
            (
                "    if total.null_count or pc.min(total).as_py() < 1:",
                "    if False:",
            )
        ],
    ),
    (
        "E30",
        "the post-condition's finiteness check removed: inf/nan ratios are "
        "emitted as if they were shares",
        [
            (
                "    if ratio.null_count or not pc.all(pc.is_finite(ratio)).as_py():",
                "    if False:",
            )
        ],
    ),
    (
        "E31",
        "the post-condition's [0, 1] range check removed: a ratio larger "
        "than its own denominator is emitted",
        [
            (
                "    if pc.min(ratio).as_py() < 0.0 or pc.max(ratio).as_py() > 1.0:",
                "    if False:",
            )
        ],
    ),
    # ---- PER-DISJUNCT layer for the three _check_output guards -------------
    #
    # E29/E30/E31 above delete a WHOLE condition, which proves only that the
    # check exists. These six delete ONE CLAUSE each. E29a and E31a were
    # SURVIVORS when first written -- the round-3 finding that motivated the
    # rule in this module's header.
    (
        "E29a",
        "(per-disjunct) `total.null_count or` deleted. `pc.min` SKIPS nulls, "
        "so on total=[None, 2] the surviving clause sees 2 and is satisfied: "
        "a null denominator reaches the prior",
        [
            (
                "    if total.null_count or pc.min(total).as_py() < 1:",
                "    if pc.min(total).as_py() < 1:",
            )
        ],
    ),
    (
        "E29b",
        "(per-disjunct) `pc.min(total).as_py() < 1` deleted: a zero or "
        "negative denominator with no nulls anywhere is emitted",
        [
            (
                "    if total.null_count or pc.min(total).as_py() < 1:",
                "    if total.null_count:",
            )
        ],
    ),
    (
        "E30a",
        "(per-disjunct) `ratio.null_count or` deleted -- the round-3 F9 "
        "SURVIVOR. NOT redundant with the clause beside it: "
        "pc.all(pc.is_finite([0.5, None])) is TRUE, because is_finite returns "
        "null for a null element and pc.all skips nulls. A null share with a "
        "valid total escapes _check_output entirely",
        [
            (
                "    if ratio.null_count or not pc.all(pc.is_finite(ratio)).as_py():",
                "    if not pc.all(pc.is_finite(ratio)).as_py():",
            )
        ],
    ),
    (
        "E30b",
        "(per-disjunct) the finiteness clause deleted, leaving only the null "
        "check: inf/nan ratios are emitted as if they were shares",
        [
            (
                "    if ratio.null_count or not pc.all(pc.is_finite(ratio)).as_py():",
                "    if ratio.null_count:",
            )
        ],
    ),
    (
        "E31a",
        "(per-disjunct) `pc.min(ratio).as_py() < 0.0 or` deleted -- the "
        "round-3 F10 SURVIVOR. Every pre-existing range fixture used "
        "ratio > 1, which trips only the max half, so the negative half was "
        "asserted by nobody and a negative share passed",
        [
            (
                "    if pc.min(ratio).as_py() < 0.0 or pc.max(ratio).as_py() > 1.0:",
                "    if pc.max(ratio).as_py() > 1.0:",
            )
        ],
    ),
    (
        "E31b",
        "(per-disjunct) `pc.max(ratio).as_py() > 1.0` deleted: a share larger "
        "than its own denominator is emitted",
        [
            (
                "    if pc.min(ratio).as_py() < 0.0 or pc.max(ratio).as_py() > 1.0:",
                "    if pc.min(ratio).as_py() < 0.0:",
            )
        ],
    ),
    (
        "E32",
        "the post-condition WARNS instead of raising (rule B's "
        "warn-and-continue pattern)",
        [
            (
                "    if total.null_count or pc.min(total).as_py() < 1:\n"
                "        raise ValueError(",
                "    if total.null_count or pc.min(total).as_py() < 1:\n"
                "        print(",
            )
        ],
    ),
    # ---- numerator/denominator alignment (round-1 finding F4) --------------
    (
        "E33",
        "numerator taken from `counts` instead of `joined`: on a MULTI-CHUNK "
        "table the join reorders rows, so each species' ratio is paired with "
        "another species' count. Invisible on single-chunk fixtures",
        [
            (
                '        pc.cast(joined.column("n"), pa.float64()),\n'
                '        pc.cast(joined.column("total"), pa.float64()),',
                '        pc.cast(counts.column("n"), pa.float64()),\n'
                '        pc.cast(joined.column("total"), pa.float64()),',
            )
        ],
    ),
    # ---- re-run / round-trip rejection -------------------------------------
    (
        "E34",
        "the already-has-total/ratio check removed: feeding a RatioTable "
        "back in dies on a raw pyarrow KeyError instead of a ValueError",
        [
            (
                '    for name in ("total", "ratio"):\n'
                "        if name in counts.column_names:",
                '    for name in ("total", "ratio"):\n'
                "        if False:",
            )
        ],
    ),
    # PER-NAME layer for the re-run guard: E34 disables both names at once.
    (
        "E34a",
        "(per-name) the re-run guard ignores `total`, catching only `ratio`",
        [
            (
                '    for name in ("total", "ratio"):',
                '    for name in ("ratio",):',
            )
        ],
    ),
    (
        "E34b",
        "(per-name) the re-run guard ignores `ratio`, catching only `total`",
        [
            (
                '    for name in ("total", "ratio"):',
                '    for name in ("total",):',
            )
        ],
    ),
]


def main() -> int:
    return _harness.main(
        target=TARGET,
        mutants=MUTANTS,
        own_tests=TESTS,
        allow_unnamed=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
