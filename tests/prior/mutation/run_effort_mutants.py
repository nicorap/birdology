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
   otherwise masquerade as a kill;
3. it clears every `__pycache__` BEFORE and AFTER each mutant. CPython
   invalidates a `.pyc` on (mtime, size), so a SAME-LENGTH mutation can leave a
   mutant `.pyc` that outlives its own source restoration and fake either a
   kill or a survival. This happened in Task 3 and misled two agents.

A mutant is KILLED if at least one test fails. Zero failures is a SURVIVOR and
means the suite does not pin that behaviour.
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
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
    (
        "E4",
        "denominator grouped by CELL only: weeks pooled, so a species' share "
        "is diluted by every other week in the cell",
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
        "denominator grouped by WEEK only: cells pooled, so Copenhagen's "
        "effort leaks into west Jutland's ratios",
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
        "(C) week silently CAST to int8 instead of the wrong type being "
        "rejected -- a second derivation of an upstream value",
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
        "(C) cell casefolded locally (a second normalisation of an already "
        "normalised value)",
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


def _run_tests() -> tuple[int, str]:
    proc = subprocess.run(
        ["uv", "run", "pytest", TESTS, "-q", "--no-header", "-p", "no:cacheprovider"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    out = proc.stdout + proc.stderr
    match = re.search(r"(\d+) failed", out)
    if match:
        return int(match.group(1)), out
    # A collection error or an uncaught crash reports no "N failed" line but is
    # still a kill; treat any non-zero exit as at least one failure so that a
    # broken run can never be scored as a survivor.
    return (0 if proc.returncode == 0 else 1), out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*", default=None, help="mutant ids to run")
    args = parser.parse_args()

    _require_clean()
    source = (REPO / TARGET).read_text()

    _clear_pycache()
    baseline, out = _run_tests()
    _clear_pycache()
    if baseline:
        sys.exit(f"refusing to run: baseline is not green ({baseline} failed)\n{out}")
    print(f"baseline: green ({out.strip().splitlines()[-1]})\n")

    selected = [m for m in MUTANTS if args.only is None or m[0] in args.only]
    rows: list[tuple[str, str, str]] = []
    survivors: list[str] = []

    for mid, description, edits in selected:
        mutated = source
        applied = True
        for old, new in edits:
            count = mutated.count(old)
            if count != 1:
                print(f"{mid}: NOT APPLIED — pattern occurs {count}x: {old[:60]!r}")
                applied = False
                break
            mutated = mutated.replace(old, new)
        if not applied:
            rows.append((mid, description, "NOT APPLIED"))
            continue

        (REPO / TARGET).write_text(mutated)
        _clear_pycache()
        try:
            failures, out = _run_tests()
        finally:
            _git("checkout", "--", str(TARGET))
            _clear_pycache()
            assert (REPO / TARGET).read_text() == source, "restore failed"

        verdict = str(failures) if failures else "SURVIVED"
        if not failures:
            survivors.append(mid)
        print(f"{mid}: {verdict:>10}  {description}")
        rows.append((mid, description, verdict))

    _require_clean()
    _clear_pycache()
    final, out = _run_tests()
    _clear_pycache()
    print(f"\nrestored run: {'GREEN' if not final else f'{final} FAILED'}")

    print("\n| # | Mutation | Tests killed |")
    print("|---|---|---|")
    for mid, description, verdict in rows:
        print(f"| {mid} | {description} | {verdict} |")
    print(f"\nSurvivors: {survivors or 'none'}")
    return 1 if survivors or final else 0


if __name__ == "__main__":
    raise SystemExit(main())
