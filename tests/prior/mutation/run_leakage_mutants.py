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
   would otherwise masquerade as a kill. NOT-APPLIED also makes the harness
   EXIT NON-ZERO, so it cannot hide under a "Survivors: none" banner;
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
        "XC guard disabled (no-op). Returns (table, 0) rather than `table`: "
        "the guard now REPORTS how many rows it removed, so a no-op that "
        "returned a bare table would crash the module instead of exercising "
        "the contract, and a crash is not a mutant",
        [
            (
                '    keys = pa.array(sorted(XENO_CANTO_PUBLISHER_KEYS), pa.string())',
                '    return table, 0\n    keys = pa.array(sorted(XENO_CANTO_PUBLISHER_KEYS), pa.string())',
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
                "        key = (normalise_cell(cell), normalise_week(week), who)",
                "        key = (normalise_cell(cell), normalise_week(week), name)",
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
        [
            (
                "        if who is None:\n"
                "            # Unreachable via TestRecording.__post_init__; belt-and-braces for",
                "        if False:\n"
                "            # Unreachable via TestRecording.__post_init__; belt-and-braces for",
            )
        ],
    ),
    (
        "M13",
        "layer 2: null recorded_by rows treated as matchable",
        [
            (
                "        if who is None:\n"
                "            # Layer 2 of the blank-contributor defence: ALWAYS kept, and the",
                "        if False:\n"
                "            # Layer 2 of the blank-contributor defence: ALWAYS kept, and the",
            )
        ],
    ),
    (
        "M13b",
        "layer 2 bypassed by reimplementing keep_mask INLINE without the blank "
        "clause (the exact reviewer mutation that survived in round 1)",
        [
            (
                "    keep, matched = _mask_and_matched_keys(cells, weeks, who_raw, banned)",
                "    keep = [\n"
                "        (normalise_cell(c), normalise_week(w), normalise_contributor(n))\n"
                "        not in banned\n"
                "        for c, w, n in zip(cells, weeks, who_raw)\n"
                "    ]\n"
                "    matched = {\n"
                "        (normalise_cell(c), normalise_week(w), normalise_contributor(n))\n"
                "        for c, w, n in zip(cells, weeks, who_raw)\n"
                "    } & banned",
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
                "        key = (normalise_cell(cell), normalise_week(week), who)",
                '        key = ("", normalise_week(week), who)',
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
                "        key = (normalise_cell(cell), normalise_week(week), who)",
                "        key = (normalise_cell(cell), 0, who)",
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
                "        key = (normalise_cell(cell), normalise_week(week), who)",
                '        key = (normalise_cell(cell), normalise_week(week), "")',
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
                "    after_xc, xc_removed = _drop_xeno_canto(table)",
                "    after_xc, xc_removed = table, 0",
            )
        ],
    ),
    (
        "M22",
        "apply_leakage_guards skips the contributor guard",
        [
            (
                "    kept, contributor_removed, banned_count, matched_count = (\n"
                "        _drop_test_contributor_cells(after_xc, test_recordings)\n"
                "    )",
                "    kept, contributor_removed, banned_count, matched_count = (\n"
                "        after_xc,\n"
                "        0,\n"
                "        0,\n"
                "        0,\n"
                "    )",
            )
        ],
    ),
    (
        "M23",
        "empty-banned early return becomes a full wipe",
        [
            (
                "    if not banned:\n        return table, 0, 0",
                "    if not banned:\n        return table.slice(0, 0), 0, 0",
            )
        ],
    ),
    (
        "M24",
        "keep_mask ignores `banned` entirely (all-True)",
        [
            (
                "        if key in banned:",
                "        if False:",
            )
        ],
    ),
    (
        "M25",
        "drop_test_contributor_cells bypasses keep_mask (keeps everything)",
        [
            (
                "    keep, matched = _mask_and_matched_keys(cells, weeks, who_raw, banned)",
                "    keep, matched = [True] * len(cells), set(banned)",
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
                "        key = (normalise_cell(cell), normalise_week(week), who)",
                "        key = (cell, normalise_week(week), who)",
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
        "F2: mismatch raise disabled while the per-banned-cell H3 validity "
        "check survives (distinct from M31, which removes the whole call, and "
        "from M32, which keeps the comparison and only downgrades the raise)",
        [
            (
                "        if banned_resolution != table_resolution:",
                "        if False:",
            )
        ],
    ),
    (
        "M40",
        "F8: resolution derivation compares the RAW table cell, so an "
        "occurrence-side cell carrying Cf/zero-width formatting blows up as "
        "'not a valid H3 index' instead of being normalised first",
        [
            (
                "            normalised = normalise_cell(cell)",
                "            normalised = cell",
            )
        ],
    ),
    (
        "M34",
        "F3: zero-match warning removed. It is now ONLY a warning: the build "
        "script escalates on `LeakageGuardResult.zero_match`, so deleting this "
        "deafens the log without disarming the guard. Both facts are pinned -- "
        "this mutant by the leakage suite's own warning test, and the guard's "
        "independence by "
        "`test_the_zero_match_signal_survives_logging_being_disabled_entirely`",
        [
            (
                "        logger.warning(\n"
                '            "drop_test_contributor_cells: %d banned (cell, week, contributor) "',
                "        _ = (\n"
                '            "drop_test_contributor_cells: %d banned (cell, week, contributor) "',
            )
        ],
    ),
    (
        "M35",
        "F3: zero-match warning fires vacuously — early return removed AND the "
        "warning made unconditional, so an EMPTY banned set warns too",
        [
            (
                "    banned = banned_keys(test_recordings)\n    if not banned:\n        return table, 0, 0",
                "    banned = banned_keys(test_recordings)\n    if False:\n        return table, 0, 0",
            ),
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
    # ---- C1: the zero-match signal is DATA, one mutant per field ----------
    #
    # `scripts/build_prior.py` escalates on `LeakageGuardResult.zero_match`.
    # Each field the property reads gets its own mutant, because a single
    # "counts are wrong" mutant cannot tell which half of the conjunction is
    # load-bearing -- and each half fails in the OPPOSITE direction:
    # `banned_key_count` too low disarms the guard on a real zero match, while
    # `contributor_rows_removed` too high does the same, and
    # `contributor_rows_removed` reported when nothing was removed breaks the
    # legitimate `--no-test-split` mode instead.
    (
        "M50",
        "C1: `banned_key_count` hard-wired to 0, so a supplied split that "
        "matched nothing reports itself as the legitimate no-split mode and "
        "the build script's zero-match escalation never fires. This is the "
        "exact end state of the old log-scraping detector -- a well-formed, "
        "publishable release whose leakage guard removed nothing -- reached "
        "through the data channel instead",
        [("    return kept, removed, len(banned)", "    return kept, removed, 0")],
    ),
    (
        "M51",
        "C1: `contributor_rows_removed` reported as 1 whatever happened, so "
        "`zero_match` is always false. Separate mutant from M50: that one "
        "makes the split look absent, this one makes the guard look like it "
        "bit. A fixture asserting only that a real zero match is DETECTED "
        "cannot tell the two apart, and each is reachable by its own "
        "one-line regression",
        [("    return kept, removed, len(banned)", "    return kept, 1, len(banned)")],
    ),
    (
        "M52",
        "C1: `zero_match` drops the `banned_key_count > 0` conjunct, so an "
        "EMPTY split -- the legitimate `--no-test-split` mode -- reports a "
        "zero match and every unguarded build fails. Its own mutant because "
        "a one-sided fixture cannot tell a guard that is too weak from one "
        "that is too strong, and the pressure from a guard that fires on a "
        "legal build is to delete the guard rather than fix it",
        [
            (
                "        return self.banned_key_count > 0 and self.contributor_rows_removed == 0",
                "        return self.contributor_rows_removed == 0",
            )
        ],
    ),
    (
        "M53",
        "C1: `zero_match` drops the `contributor_rows_removed == 0` conjunct, "
        "so ANY supplied split reports a zero match and a build whose guard "
        "worked perfectly fails. The mirror of M52: together they prove both "
        "conjuncts are load-bearing and neither is decoration",
        [
            (
                "        return self.banned_key_count > 0 and self.contributor_rows_removed == 0",
                "        return self.banned_key_count > 0",
            )
        ],
    ),
    (
        "M54",
        "C1: the XC guard's removal count folded into the contributor guard's, "
        "so a build in which the XC guard removed rows and the contributor "
        "guard removed NONE reports a non-zero contributor count and the "
        "zero-match escalation is silently disarmed. Two guards therefore need "
        "two counts: the XC guard runs on every build, including "
        "`--no-test-split`, so one combined total is satisfied by activity "
        "that has nothing to do with the split",
        [
            (
                "        contributor_rows_removed=contributor_removed,",
                "        contributor_rows_removed=contributor_removed + xc_removed,",
            )
        ],
    ),
    (
        "M55",
        "C1: `rows_before_contributor_guard` read from the PRE-XC table, so "
        "the count the build script reports in its failure message names a "
        "denominator the contributor guard never saw. Not a correctness bug in "
        "the escalation itself, which is why it is its own mutant: the field "
        "exists so the operator can read the removal RATE, and a wrong "
        "denominator there sends them looking at the wrong guard",
        [
            (
                "        rows_before_contributor_guard=after_xc.num_rows,",
                "        rows_before_contributor_guard=table.num_rows,",
            )
        ],
    ),
    # ---- C1 (final gate): the PER-KEY match count ------------------------
    #
    # `zero_match` is a claim about the WHOLE split and ONE live key satisfies
    # it. `matched_key_count` is the quantity that makes a partial failure
    # visible, so each way of making it lie gets its own mutant: too high
    # (disarms the build's rate clause), too low (fails a correct build),
    # counting the wrong thing (rows, or the banned set itself), and the
    # blank-contributor interaction that would credit the anonymous
    # population with matches it must never make.
    (
        "M56",
        "C1: `matched_key_count` reported as the FULL banned count, so every "
        "split looks perfectly live and the build script's rate clause can "
        "never fire. This is the exact end state of the reproduced defect -- "
        "a 100-key split with one live key releasing five artifacts -- "
        "reached through the count instead of through the condition",
        [
            (
                "    return kept, removed, len(banned), len(matched)",
                "    return kept, removed, len(banned), len(banned)",
            )
        ],
    ),
    (
        "M57",
        "C1: `matched_key_count` reported as ZERO whatever matched, so a "
        "perfectly healthy split fails the build's rate clause. The mirror of "
        "M56: together they prove the count tracks reality in both "
        "directions, and a one-sided fixture cannot tell a count that is too "
        "generous from one that is too mean. The pressure from a guard that "
        "fires on a legal build is to delete the guard",
        [
            (
                "    return kept, removed, len(banned), len(matched)",
                "    return kept, removed, len(banned), 0",
            )
        ],
    ),
    (
        "M58",
        "C1: matches counted as ROWS rather than KEYS -- the matched set "
        "replaced by the removal count. Numerically identical on every "
        "one-row-per-key fixture, and on a real archive a contributor with "
        "many occurrences in one cell-week pushes the rate above 1.0, so no "
        "threshold comparison means anything any more",
        [
            (
                "    return kept, removed, len(banned), len(matched)",
                "    return kept, removed, len(banned), removed",
            )
        ],
    ),
    (
        "M59",
        "C1: the matched set collects the BANNED key set rather than the keys "
        "that bit -- `matched.add` fed from `banned` on every matching row. "
        "Distinct from M56, which fakes the COUNT: this one keeps a real set "
        "and fills it wrongly, so any check that merely asserted the set's "
        "type or non-emptiness passes",
        [
            (
                "            matched.add(cast(\"tuple[str, int, str]\", key))",
                "            matched.update(banned)",
            )
        ],
    ),
    (
        "M60",
        "C1: a blank-contributor row counted as a MATCH -- the layer-2 "
        "short-circuit still keeps the row but credits the key. The rate then "
        "reads healthy on a split whose only 'matches' are the anonymous "
        "population the guard must never touch, which is the quietest way to "
        "disarm the rate clause: nothing is deleted and the number looks right",
        [
            (
                "            # Layer 2 of the blank-contributor defence: ALWAYS kept, and the\n"
                "            # key is never even formed, so a blank key that reached *banned*\n"
                "            # by some other route cannot match it either.\n"
                "            keep.append(True)\n"
                "            continue",
                "            matched.update(banned)\n"
                "            keep.append(True)\n"
                "            continue",
            )
        ],
    ),
    (
        "M61",
        "C1: `match_rate` RETURNS 1.0 for an empty split instead of raising. "
        "0/0 has no honest value: 1.0 claims a split that does not exist "
        "worked perfectly, and a caller that forgot its `banned_key_count > "
        "0` conjunct would then silently pass every `--no-test-split` build "
        "through a rate check that measured nothing",
        [
            (
                "        if self.banned_key_count <= 0:\n            raise ValueError(",
                "        if False:\n            raise ValueError(",
            )
        ],
    ),
    (
        "M62",
        "C1: `match_rate` divides by the MATCHED count instead of the banned "
        "count, giving 1.0 for every split that matched anything at all. Its "
        "own mutant because it is the plausible typo in a two-name division "
        "and it is invisible on any fixture where every key bites",
        [
            (
                "        return self.matched_key_count / self.banned_key_count",
                "        return self.matched_key_count / max(self.matched_key_count, 1)",
            )
        ],
    ),
    (
        "M63",
        "C1: the PARTIAL-match warning removed. It is ONLY a warning -- the "
        "build script escalates on `LeakageGuardResult.match_rate` -- so "
        "deleting it deafens the log without disarming the guard, exactly as "
        "M34 does for the zero-match line. Both facts are pinned: this mutant "
        "by the partial-warning test, and the guard's independence by "
        "`test_the_match_rate_survives_logging_being_disabled_entirely`",
        [
            (
                "            logger.warning(\n"
                '                "drop_test_contributor_cells: only %d of %d banned "',
                "            _ = (\n"
                '                "drop_test_contributor_cells: only %d of %d banned "',
            )
        ],
    ),
    (
        "M64",
        "C1: the partial-match warning fires UNCONDITIONALLY, so a split in "
        "which every key bit is reported as partial. A vacuous warning trains "
        "the operator to ignore the real one, which is how the zero-match "
        "line came to need a data channel beside it",
        [
            (
                "        if len(matched) < len(banned):",
                "        if True:",
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
    not_applied: list[str] = []

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
            not_applied.append(mid)
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
    # NOT-APPLIED is a FAILURE, not a neutral row (round-1 finding F7). A
    # pattern that has gone stale after a refactor tests nothing, and under a
    # "Survivors: none" banner it reads as coverage it never provided.
    print(f"NOT APPLIED: {not_applied or 'none'}")
    return 1 if survivors or not_applied or final else 0


if __name__ == "__main__":
    raise SystemExit(main())
