#!/usr/bin/env python3
"""Shared mutation-harness core for `tests/prior/mutation/run_*_mutants.py`.

Every harness in this directory used to carry its own copy of the runner. The
copies drifted: two of them (`run_effort_mutants.py`, `run_leakage_mutants.py`)
never gained the named-killer check, five never gained the CRASHED check, and
ALL SEVEN ran exactly ONE test file per mutant. That last one is finding I1 of
the final gate and it is the reason this module exists.

WHY THE SCOPE IS THE WHOLE `tests/prior` DIRECTORY (finding I1)
--------------------------------------------------------------
A mutant confronted only with its own module's tests cannot fail on a contract
whose violation is observable only in a DOWNSTREAM CONSUMER. The gate proved
the consequence: making the conditional guard-marker re-stamp at
`indexing.py:79-80` UNCONDITIONAL -- the edit whose own docstring says it
"would make this function authenticate ANY table that passed through it, which
is worse than no guard" --

    tests/prior/test_indexing.py alone   21 passed     <- mutant SURVIVES
    whole tests/prior/                    1 failed     <- test_effort.py kills it

so the highest-consequence guard on the branch was pinned only across a file
boundary no harness could see.

MEASURED COST, which is why the answer is the whole directory and not a
hand-picked consumer set:

    tests/prior/test_indexing.py          21 tests   0.19s wall (incl. `uv run`)
    tests/prior/test_effort.py            56 tests   0.22s
    tests/prior/test_controls.py         142 tests   0.70s
    tests/prior/  (whole directory)      730 tests   1.27s

The directory is 0.6-1.1s more per mutant than a single file. Across ~470
mutants that is roughly +5 to +8 minutes for the whole suite of harnesses --
cheap enough that deriving a per-target "transitive consumer" file set would
buy seconds and cost a guess. The import graph was derived anyway and is
recorded in each harness header, because it says WHAT the widening buys; it is
documentation, not a filter.

Pass `--tests <path>` to override the scope. That exists so the before/after
cost of this change can be re-measured (`--tests tests/prior/test_backoff.py`
reproduces the old, blind behaviour); it is NOT how the harness should be run.

WHAT A ROW CAN SAY, AND WHAT IT MUST NOT
----------------------------------------
1. NOT APPLIED -- a pattern did not occur exactly once. A stale pattern tests
   nothing and must never read as a kill. Exits non-zero.
2. MISLABELLED -- the mutant died, but not to the test the row NAMES as its
   killer (or that test does not exist anywhere in scope). A mutant killed by
   an unrelated test credits coverage that does not exist. Exits non-zero.
3. CRASHED -- the edit broke the target module (`AttributeError`, `NameError`,
   `ImportError`, `SyntaxError`, ...) instead of changing its behaviour. Such
   an edit takes out most of the suite, which trivially includes whatever test
   the row names, so without this check it is scored as a kill while
   exercising no contract at all. Scoped to the TARGET FILE: a mutant is
   entitled to make a *test* raise. Exits non-zero.
4. SURVIVED -- zero failures. Exits non-zero.
5. A number -- the failure count, with the named killer among them.

`__pycache__` is cleared before AND after every mutant: CPython invalidates a
`.pyc` on (mtime, size), so a SAME-LENGTH mutation can leave a mutant `.pyc`
that outlives its own source restoration and fake either a kill or a survival.

WHERE THE KILL CAME FROM, NOW THAT IT CAN COME FROM ANYWHERE
-----------------------------------------------------------
Because the scope is the whole directory, a kill may arrive from a file other
than the target's own. That is the point, but it is also information worth
printing rather than averaging away, so each row records the test FILES that
failed and the summary lists every mutant that was killed ONLY from outside
its own module's test file. Those are precisely the cross-module contracts the
old scope could not see.

UNNAMED KILLERS -- a known, reported gap
----------------------------------------
`run_effort_mutants.py` and `run_leakage_mutants.py` predate the named-killer
convention and their mutants carry `killer=None`. Such a row is scored a kill
on "at least one failure" alone, exactly as it was before this refactor, and is
listed under UNNAMED in the summary so the weaker attribution is visible in the
output rather than inferred from the source. A harness must opt in with
`allow_unnamed=True`; the default refuses, so a NEW mutant cannot quietly
arrive without a killer.
"""
from __future__ import annotations

import argparse
import atexit
import difflib
import re
import shutil
import signal
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

#: The scope every mutant is confronted with. See the module docstring.
DEFAULT_TESTS = "tests/prior"

#: Per-mutant wall-clock ceiling for the test run.
#
# A mutant CAN make the suite hang — remove a loop's exit condition and
# `poll_download` spins forever against a mocked `time.sleep`. Without a
# deadline the harness hangs with a mutant on disk, and if the operator then
# kills the harness the `finally` that restores the file never runs. That
# happened once while this module was being written and left a mutant in
# `gbif_download.py`. So the harness owns the deadline: a timeout is reported
# as a kill of the loudest kind (TIMEOUT, scored with CRASHED — the edit broke
# the module rather than changing its behaviour) and the restore still runs.
#
# Sized against the measured baseline: `tests/prior` is ~3s, so 300s is a
# hundredfold margin and only a genuine non-termination reaches it.
RUN_TIMEOUT_S = 300

# A mutant that BREAKS the target module rather than changing its behaviour is
# not a mutant at all. These are the exception types that mean "the edit
# referenced something that does not exist" or "it does not parse".
_CRASH_TYPES = (
    "AttributeError",
    "NameError",
    "ImportError",
    "ModuleNotFoundError",
    "SyntaxError",
    "IndentationError",
)
# NOT in the list, deliberately: `TypeError`, `ValueError`, `KeyError`. Those
# are the errors a contract-violating input is SUPPOSED to raise from inside
# the target, so treating them as a crash would score real kills as no-ops.


def clear_pycache() -> None:
    for d in REPO.rglob("__pycache__"):
        shutil.rmtree(d, ignore_errors=True)


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout


def require_clean(paths: set[Path]) -> None:
    dirty = [
        line
        for line in git("status", "--porcelain").splitlines()
        if line[3:].strip() in {str(p) for p in paths}
    ]
    if dirty:
        sys.exit(f"refusing to run: {dirty!r} is dirty; commit or restore it")


def restore(path: Path, source: str) -> None:
    """Put the ORIGINAL BYTES back, without going through git.

    The seven original harnesses ran `git checkout -- <target>`. That takes
    git's `index.lock`, and the round-7 process note records two harnesses
    racing each other's checkout in one worktree as the way a mutant gets left
    behind. Writing back the bytes read before the run needs no lock, cannot
    race a concurrent commit, and is verified byte-for-byte by the caller's
    assert. `require_clean` still runs against git before AND after, so the
    bytes being restored are provably HEAD's.
    """
    (REPO / path).write_text(source)


# Every file currently mutated, and its original bytes, so an INTERRUPTED
# harness still restores. The `finally` in `main` covers an exception; it does
# NOT cover SIGINT, SIGTERM, or the harness being killed for exceeding an
# operator's own timeout — and that is not hypothetical: it happened twice while
# this module was being written and left a mutant in `gbif_download.py` both
# times, which is precisely the "a mutant gets left behind" failure the ledger's
# process note warns about, arriving by a route the note did not name.
_IN_FLIGHT: dict[Path, str] = {}


def _restore_in_flight() -> None:
    for path, source in list(_IN_FLIGHT.items()):
        try:
            restore(path, source)
        except OSError:  # pragma: no cover - best effort on the way out
            pass
    _IN_FLIGHT.clear()


def _bail(signum, _frame):  # pragma: no cover - signal path
    _restore_in_flight()
    sys.exit(f"interrupted by signal {signum}; mutant restored")


atexit.register(_restore_in_flight)
for _sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
    signal.signal(_sig, _bail)


def _scope_files(tests: str) -> list[Path]:
    """Every test file the scope collects, for the killer-existence check."""
    root = REPO / tests
    if root.is_dir():
        return sorted(root.glob("test_*.py"))
    return [root]


def run_tests(tests: str) -> tuple[int, str, set[str], set[str]]:
    """Return (failure count, output, failing test names, failing test files)."""
    try:
        proc = subprocess.run(
            [
                "uv",
                "run",
                "pytest",
                tests,
                "-q",
                "--no-header",
                "-p",
                "no:cacheprovider",
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            timeout=RUN_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        # Non-termination. Reported through the sentinel below so the caller
        # scores it and, crucially, still reaches its restore.
        return 1, "__HARNESS_TIMEOUT__", set(), set()
    out = proc.stdout + proc.stderr
    names: set[str] = set()
    files: set[str] = set()
    for raw in re.findall(r"^(?:FAILED|ERROR) (\S+)", out, re.MULTILINE):
        if "::" not in raw:
            # A collection ERROR names a FILE and no test. Record the file so
            # the row still says where the breakage was, but contribute no
            # test name: a collection error names no test, so it can never
            # satisfy a named killer. (That is finding B17c's lesson: a guard
            # that runs at IMPORT time is unattributable, which is why
            # validation was moved out of import.)
            files.add(raw.split("::", 1)[0])
            continue
        path, _, tail = raw.partition("::")
        files.add(path)
        names.add(tail.rsplit("::", 1)[-1].split("[", 1)[0])
    match = re.search(r"(\d+) failed", out)
    if match:
        return int(match.group(1)), out, names, files
    # A collection error or an uncaught crash reports no "N failed" line but is
    # still a kill; treat any non-zero exit as at least one failure so that a
    # broken run can never be scored as a survivor.
    return (0 if proc.returncode == 0 else 1), out, names, files


def crash_in_target(out: str, target: Path) -> str | None:
    """The crash type, if the output shows one raised FROM THE TARGET MODULE.

    Scoped to the mutated file on purpose. A mutant is entitled to make a TEST
    raise `AttributeError` -- for instance one that removes a method a test
    calls, where that IS the behaviour under test. What is never legitimate is
    the mutated module failing to load or to resolve its own names, and that is
    what the traceback line `src/birdology/prior/tensor.py:NNN: SomeError`
    reports.
    """
    target = Path(target)
    for crash in _CRASH_TYPES:
        # pytest's one-line traceback summary: "<path>:<line>: <ErrorType>".
        if re.search(rf"^{re.escape(str(target))}:\d+: {crash}$", out, re.MULTILINE):
            return crash
        # A module that does not even import reports the error without a
        # "path:line:" prefix of its own, but names the file in the frame.
        if re.search(
            rf'File "[^"]*{re.escape(target.name)}".*\n(?:.*\n)??\s*{crash}:',
            out,
        ):
            return crash
    return None


def apply_edits(source: str, edits: list[tuple[str, str]]) -> str | None:
    mutated = source
    for old, new in edits:
        if mutated.count(old) != 1:
            return None
        mutated = mutated.replace(old, new)
    return mutated


def _unpack(mutant, default_target: Path):
    """(id, description, killer, edits, target) from a 3/4/5-tuple row.

    A 3-tuple is the pre-killer-name shape still used by the effort and
    leakage harnesses; its `killer` is None.
    """
    if len(mutant) == 3:
        mid, description, edits = mutant
        return mid, description, None, edits, default_target
    mid, description, killer, edits = mutant[:4]
    target = mutant[4] if len(mutant) > 4 else default_target
    return mid, description, killer, edits, Path(target)


def main(
    *,
    target: Path,
    mutants: list,
    own_tests: str,
    allow_unnamed: bool = False,
    argv: list[str] | None = None,
) -> int:
    """Run *mutants* against the whole `tests/prior` scope.

    `own_tests` is the target's OWN test file. It is not a filter -- the scope
    is the directory -- it is used to report which mutants are pinned ONLY
    from outside their own module, which is the class finding I1 is about.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*", default=None, help="mutant ids to run")
    parser.add_argument(
        "--tests",
        default=DEFAULT_TESTS,
        help="scope override, for re-measuring the cost of the wide scope; "
        "the default (the whole tests/prior directory) is what finding I1 "
        "requires and what a real run must use",
    )
    parser.add_argument(
        "--verify-labels",
        action="store_true",
        help="print the diff each mutant applies and exit, so every LABEL can "
        "be read against its actual EDIT rather than trusted",
    )
    args = parser.parse_args(argv)
    tests = args.tests

    targets = {_unpack(m, target)[4] for m in mutants}
    sources = {path: (REPO / path).read_text() for path in targets}
    killer_haystack = "\n".join(p.read_text() for p in _scope_files(tests))
    selected = [m for m in mutants if args.only is None or m[0] in args.only]

    if args.verify_labels:
        bad = []
        for mutant in selected:
            mid, description, killer, edits, tgt = _unpack(mutant, target)
            source = sources[tgt]
            mutated = apply_edits(source, edits)
            print(
                f"\n{'=' * 72}\n{mid} [{tgt}]: {description}\n"
                f"  killer: {killer}\n{'=' * 72}"
            )
            if mutated is None:
                print("  !! NOT APPLICABLE: a pattern does not occur exactly once")
                bad.append(mid)
                continue
            if killer is None and not allow_unnamed:
                print("  !! NO KILLER NAMED")
                bad.append(mid)
            elif killer is not None and f"def {killer}(" not in killer_haystack:
                print(f"  !! killer test {killer!r} does not exist under {tests}")
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

    require_clean(targets)

    clear_pycache()
    baseline, out, _, _ = run_tests(tests)
    clear_pycache()
    if baseline:
        sys.exit(f"refusing to run: baseline is not green ({baseline} failed)\n{out}")
    print(f"scope: {tests}")
    print(f"baseline: green ({out.strip().splitlines()[-1]})\n")

    rows: list[tuple[str, str, str]] = []
    survivors: list[str] = []
    not_applied: list[str] = []
    mislabelled: list[str] = []
    crashed: list[str] = []
    unnamed: list[str] = []
    cross_file_only: list[tuple[str, str]] = []

    for mutant in selected:
        mid, description, killer, edits, tgt = _unpack(mutant, target)
        source = sources[tgt]

        if killer is None and not allow_unnamed:
            print(f"{mid}: MISLABELLED — no killer test named")
            rows.append((mid, description, "MISLABELLED"))
            mislabelled.append(mid)
            continue
        if killer is not None and f"def {killer}(" not in killer_haystack:
            print(f"{mid}: MISLABELLED — killer test {killer!r} does not exist")
            rows.append((mid, description, "MISLABELLED"))
            mislabelled.append(mid)
            continue

        mutated = apply_edits(source, edits)
        if mutated is None:
            counts = [source.count(old) for old, _ in edits]
            print(f"{mid}: NOT APPLIED — pattern occurrence counts {counts}")
            rows.append((mid, description, "NOT APPLIED"))
            not_applied.append(mid)
            continue

        (REPO / tgt).write_text(mutated)
        _IN_FLIGHT[tgt] = source
        clear_pycache()
        try:
            failures, out, failed_names, failed_files = run_tests(tests)
        finally:
            restore(tgt, source)
            _IN_FLIGHT.pop(tgt, None)
            clear_pycache()
            assert (REPO / tgt).read_text() == source, "restore failed"

        crash = crash_in_target(out, tgt)
        if out == "__HARNESS_TIMEOUT__":
            verdict = f"TIMEOUT (>{RUN_TIMEOUT_S}s)"
            crashed.append(mid)
        elif crash is not None:
            verdict = f"CRASHED ({crash}, {failures} failed)"
            crashed.append(mid)
        elif not failures:
            verdict = "SURVIVED"
            survivors.append(mid)
        elif killer is None:
            verdict = f"{failures} (unnamed)"
            unnamed.append(mid)
        elif killer not in failed_names:
            verdict = f"MISLABELLED ({failures} failed, not {killer})"
            mislabelled.append(mid)
        else:
            verdict = str(failures)
        if failures and crash is None and failed_files and own_tests not in failed_files:
            cross_file_only.append((mid, ",".join(sorted(failed_files))))
        print(f"{mid}: {verdict:>22}  {description[:64]}")
        rows.append((mid, description, verdict))

    require_clean(targets)
    clear_pycache()
    final, out, _, _ = run_tests(tests)
    clear_pycache()
    print(f"\nrestored run: {'GREEN' if not final else f'{final} FAILED'}")

    print("\n| # | Mutation | Killed by | Tests killed |")
    print("|---|---|---|---|")
    by_id = {m[0]: _unpack(m, target)[2] for m in mutants}
    for mid, description, verdict in rows:
        print(f"| {mid} | {description} | `{by_id[mid]}` | {verdict} |")
    print(f"\nSurvivors: {survivors or 'none'}")
    # NOT-APPLIED, MISLABELLED and CRASHED are FAILURES, not neutral rows.
    print(f"NOT APPLIED: {not_applied or 'none'}")
    print(f"MISLABELLED: {mislabelled or 'none'}")
    print(f"CRASHED: {crashed or 'none'}")
    # Not failures, but both are weaker claims than the table's other rows and
    # neither should have to be inferred from the source.
    print(f"UNNAMED KILLER (kill counted on any failure): {unnamed or 'none'}")
    print(
        "PINNED ONLY OUTSIDE ITS OWN TEST FILE "
        f"(the class finding I1 is about): {cross_file_only or 'none'}"
    )
    return 1 if survivors or not_applied or mislabelled or crashed or final else 0
