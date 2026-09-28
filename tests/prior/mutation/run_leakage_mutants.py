#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/leakage.py`.

Committed deliberately (round-1 finding F7): a mutation table that cannot be
re-derived is a claim, not evidence.

Run from the repository root:

    uv run python tests/prior/mutation/run_leakage_mutants.py
    uv run python tests/prior/mutation/run_leakage_mutants.py --only M16 M17

Each mutant is an exact-string edit on the source. The harness:

1. refuses to start unless `git status --porcelain` is clean for the target
   file, so a mutant can never be committed by accident;
2. requires every pattern to occur EXACTLY ONCE, and reports NOT-APPLIED
   separately from SURVIVED — a stale pattern that silently matched nothing
   would otherwise masquerade as a kill;
3. clears every `__pycache__` BEFORE and AFTER each run. CPython invalidates a
   `.pyc` on (mtime, size), so a SAME-LENGTH mutation — `(lon, lat)` for
   `(lat, lon)`, say — can leave a mutant `.pyc` that outlives its own source
   restoration and fake either a kill or a survival. This actually happened in
   Task 3 and misled two agents;
4. restores the file with `git checkout --` and verifies the tree is clean
   again before moving on.

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
TARGET = Path("src/birdology/prior/leakage.py")
TESTS = "tests/prior/test_leakage.py"

# (id, description, [(old, new), ...])
MUTANTS: list[tuple[str, str, list[tuple[str, str]]]] = [
    # ---- guard 1: the Xeno-Canto publisher key ---------------------------
    (
        "M1",
        "XC key -> the brief's e2e717bf... (which is Cornell Lab / eBird)",
        [
            (
                'XENO_CANTO_PUBLISHER_KEYS = frozenset({"1f00d75c-f6fc-4224-a595-975e82d7689c"})',
                'XENO_CANTO_PUBLISHER_KEYS = frozenset({"e2e717bf-551a-4917-bdc9-4fa0f342c530"})',
            )
        ],
    ),
    (
        "M2",
        "XC guard inverted: keep ONLY Xeno-Canto rows",
        [
            (
                '    mask = pc.invert(pc.is_in(table.column("publisher_key"), value_set=keys))',
                '    mask = pc.is_in(table.column("publisher_key"), value_set=keys)',
            )
        ],
    ),
    (
        "M3",
        "XC guard disabled (no-op)",
        [
            (
                '    keys = pa.array(sorted(XENO_CANTO_PUBLISHER_KEYS), pa.string())',
                '    return table\n    keys = pa.array(sorted(XENO_CANTO_PUBLISHER_KEYS), pa.string())',
            )
        ],
    ),
    # ---- TestRecording validation ---------------------------------------
    (
        "M4",
        "blank recorded_by accepted by TestRecording",
        [
            (
                "        if normalise_contributor(self.recorded_by) is None:\n"
                "            raise ValueError(\n"
                '                "TestRecording.recorded_by must be a non-blank contributor name; "',
                "        if False:\n"
                "            raise ValueError(\n"
                '                "TestRecording.recorded_by must be a non-blank contributor name; "',
            )
        ],
    ),
    (
        "M5",
        "blank cell accepted by TestRecording",
        [
            (
                "        if not isinstance(self.cell, str) or not self.cell.strip():",
                "        if False:",
            )
        ],
    ),
    (
        "M6",
        "out-of-range week accepted by TestRecording",
        [("        if not 1 <= week <= 52:", "        if False:")],
    ),
    # ---- contributor normalisation --------------------------------------
    (
        "M7",
        "no casefold in normalise_contributor",
        [
            (
                '    folded = unicodedata.normalize("NFKC", name).casefold()',
                '    folded = unicodedata.normalize("NFKC", name)',
            )
        ],
    ),
    (
        "M8",
        "no NFKC in normalise_contributor",
        [
            (
                '    folded = unicodedata.normalize("NFKC", name).casefold()',
                "    folded = name.casefold()",
            )
        ],
    ),
    (
        "M9",
        "no whitespace collapse in normalise_contributor",
        [
            (
                "    collapsed = \" \".join(_strip_format_characters(folded).split())",
                "    collapsed = _strip_format_characters(folded)",
            )
        ],
    ),
    (
        "M10",
        "contributor normalisation dropped on the OCCURRENCE side only",
        [
            (
                "        or (normalise_cell(cell), normalise_week(week), normalise_contributor(name))\n"
                "        not in banned",
                "        or (normalise_cell(cell), normalise_week(week), name) not in banned",
            )
        ],
    ),
    (
        "M11",
        "contributor normalisation dropped on the TEST-RECORDING side only",
        [
            (
                "        banned.add((cell, normalise_week(recording.week), who))",
                "        banned.add((cell, normalise_week(recording.week), recording.recorded_by))",
            )
        ],
    ),
    # ---- blank-contributor defence, two layers --------------------------
    (
        "M12",
        "layer 1: blank test recording NOT skipped (mass-deletion path)",
        [("        if who is None:", "        if False:")],
    ),
    (
        "M13",
        "layer 2: null recorded_by rows treated as matchable",
        [
            (
                "        normalise_contributor(name) is None\n"
                "        or (normalise_cell(cell), normalise_week(week), normalise_contributor(name))",
                "        (normalise_cell(cell), normalise_week(week), normalise_contributor(name))",
            )
        ],
    ),
    (
        "M13b",
        "layer 2 bypassed by reimplementing keep_mask INLINE without the blank "
        "clause (the exact reviewer mutation that survived in round 1)",
        [
            (
                "    keep = keep_mask(cells, weeks, who_raw, banned)",
                "    keep = [\n"
                "        (normalise_cell(c), normalise_week(w), normalise_contributor(n))\n"
                "        not in banned\n"
                "        for c, w, n in zip(cells, weeks, who_raw)\n"
                "    ]",
            )
        ],
    ),
    # ---- guard 2 shape ---------------------------------------------------
    (
        "M14",
        "contributor guard inverted: drop the NON-matching rows",
        [
            (
                "    kept = table.filter(pa.array(keep, pa.bool_()))",
                "    kept = table.filter(pc.invert(pa.array(keep, pa.bool_())))",
            )
        ],
    ),
    (
        "M15",
        "contributor guard disabled (no-op)",
        [
            (
                "    banned = banned_keys(test_recordings)\n    if not banned:",
                "    banned = banned_keys(test_recordings)\n    if True:",
            )
        ],
    ),
    (
        "M16",
        "cell dropped from the join key (both sides)",
        [
            (
                "        banned.add((cell, normalise_week(recording.week), who))",
                '        banned.add(("", normalise_week(recording.week), who))',
            ),
            (
                "        or (normalise_cell(cell), normalise_week(week), normalise_contributor(name))",
                '        or ("", normalise_week(week), normalise_contributor(name))',
            ),
        ],
    ),
    (
        "M17",
        "week dropped from the join key (both sides)",
        [
            (
                "        banned.add((cell, normalise_week(recording.week), who))",
                "        banned.add((cell, 0, who))",
            ),
            (
                "        or (normalise_cell(cell), normalise_week(week), normalise_contributor(name))",
                "        or (normalise_cell(cell), 0, normalise_contributor(name))",
            ),
        ],
    ),
    (
        "M18",
        "contributor dropped from the join key (whole cell-week wiped)",
        [
            (
                "        banned.add((cell, normalise_week(recording.week), who))",
                '        banned.add((cell, normalise_week(recording.week), ""))',
            ),
            (
                "        or (normalise_cell(cell), normalise_week(week), normalise_contributor(name))",
                '        or (normalise_cell(cell), normalise_week(week), "")',
            ),
        ],
    ),
    # ---- observability ---------------------------------------------------
    (
        "M19",
        "U+FFFD rows never flagged",
        [("    if flagged:", "    if False:")],
    ),
    (
        "M20",
        "U+FFFD flagged unconditionally (vacuous-warning check)",
        [("    if flagged:", "    if True:")],
    ),
    (
        "M21",
        "apply_leakage_guards skips the XC guard",
        [
            (
                "    return drop_test_contributor_cells(drop_xeno_canto(table), test_recordings)",
                "    return drop_test_contributor_cells(table, test_recordings)",
            )
        ],
    ),
    (
        "M22",
        "apply_leakage_guards skips the contributor guard",
        [
            (
                "    return drop_test_contributor_cells(drop_xeno_canto(table), test_recordings)",
                "    return drop_xeno_canto(table)",
            )
        ],
    ),
    (
        "M23",
        "empty-banned early return becomes a full wipe",
        [
            (
                "    if not banned:\n        return table",
                "    if not banned:\n        return table.slice(0, 0)",
            )
        ],
    ),
    (
        "M24",
        "keep_mask ignores `banned` entirely (all-True)",
        [
            (
                "        normalise_contributor(name) is None\n"
                "        or (normalise_cell(cell), normalise_week(week), normalise_contributor(name))\n"
                "        not in banned",
                "        True",
            )
        ],
    ),
    (
        "M25",
        "drop_test_contributor_cells bypasses keep_mask (keeps everything)",
        [
            (
                "    keep = keep_mask(cells, weeks, who_raw, banned)",
                "    keep = [True] * len(cells)",
            )
        ],
    ),
    # ---- round-2 findings ------------------------------------------------
    (
        "M26",
        "F1: cell NOT normalised on the test-recording side (banned_keys)",
        [
            (
                "        cell = normalise_cell(recording.cell)",
                "        cell = recording.cell",
            )
        ],
    ),
    (
        "M27",
        "F1: cell NOT normalised on the occurrence side (keep_mask)",
        [
            (
                "        or (normalise_cell(cell), normalise_week(week), normalise_contributor(name))",
                "        or (cell, normalise_week(week), normalise_contributor(name))",
            )
        ],
    ),
    (
        "M28",
        "F1: TestRecording stores the RAW cell instead of the normalised one",
        [('        object.__setattr__(self, "cell", cell)', "        pass")],
    ),
    (
        "M29",
        "F1: normalise_cell does not casefold",
        [
            (
                "    return _strip_format_characters(cell).strip().casefold()",
                "    return _strip_format_characters(cell).strip()",
            )
        ],
    ),
    (
        "M30",
        "F1: normalise_cell does not strip",
        [
            (
                "    return _strip_format_characters(cell).strip().casefold()",
                "    return _strip_format_characters(cell).casefold()",
            )
        ],
    ),
    (
        "M31",
        "F2: cross-resolution check removed entirely",
        [
            (
                "    _assert_one_resolution(cells, banned)",
                "    pass  # _assert_one_resolution(cells, banned)",
            )
        ],
    ),
    (
        "M32",
        "F2: cross-resolution mismatch WARNS instead of raising",
        [
            (
                "        if banned_resolution != table_resolution:\n            raise ValueError(",
                "        if banned_resolution != table_resolution:\n            logger.warning(",
            )
        ],
    ),
    (
        "M33",
        "F2: resolution check compares the RAW table cell, so an uppercase "
        "table cell makes it blow up / skip",
        [
            (
                "        if banned_resolution != table_resolution:",
                "        if False:",
            )
        ],
    ),
    (
        "M34",
        "F3: zero-match warning removed",
        [("    elif banned:", "    elif False:")],
    ),
    (
        "M35",
        "F3: zero-match warning fires unconditionally, including for an empty "
        "banned set (vacuous-warning check)",
        [
            (
                "    banned = banned_keys(test_recordings)\n    if not banned:\n        return table",
                "    banned = banned_keys(test_recordings)\n    if not banned:\n        pass",
            )
        ],
    ),
    (
        "M36",
        "F5: Cf/zero-width codepoints not stripped",
        [
            (
                '    return "".join(ch for ch in text if unicodedata.category(ch) != "Cf")',
                "    return text",
            )
        ],
    ),
    (
        "M37",
        "F5: Cf stripping applied to the cell only, not the contributor",
        [
            (
                '    collapsed = " ".join(_strip_format_characters(folded).split())',
                '    collapsed = " ".join(folded.split())',
            )
        ],
    ),
    (
        "M38",
        "F6: week silently coerced with int() instead of rejected",
        [
            (
                "    if not isinstance(week, SupportsIndex):",
                "    if False:",
            ),
            ("    return operator.index(week)", "    return int(week)"),
        ],
    ),
    (
        "M39",
        "F6: TestRecording does not store the coerced week",
        [('        object.__setattr__(self, "week", week)', "        pass")],
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
    return (int(match.group(1)) if match else 0), out


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
