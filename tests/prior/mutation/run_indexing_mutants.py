#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/indexing.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_indexing_mutants.py
    uv run python tests/prior/mutation/run_indexing_mutants.py --only X1 X7
    uv run python tests/prior/mutation/run_indexing_mutants.py --verify-labels

WHY THIS HARNESS EXISTS AT ALL — final-gate finding I1
-----------------------------------------------------
`indexing.py` is 81 lines and had NO harness through ten tasks and eight review
rounds, while owning THREE cross-module contracts:

* `WEEKS_PER_YEAR`, the single definition of the week stride, imported by
  `backoff.py`, `smoothing.py`, `tensor.py`, `controls.py` and the build
  script. Every one of them sizes an axis from it.
* the ISO week 53 -> 52 fold, which is why no downstream stage ever has to
  cope with a 53rd week holding a seventh of the usual data;
* the CONDITIONAL re-stamp of the leakage-guard marker (`indexing.py:79-80`),
  which is the highest-consequence guard on the branch.

The gate proved the consequence of the gap with the re-stamp, which is X1
below. Made UNCONDITIONAL:

    tests/prior/test_indexing.py alone   21 passed   <- mutant SURVIVES
    whole tests/prior/                    1 failed   <- test_effort.py kills it

`aggregate_counts` is on the path of every realistic build, so an
unconditional stamp would authenticate every table in the pipeline while
leaving the guard technically present. It is the exact edit whose own docstring
says it "would make this function authenticate ANY table that passed through
it, which is worse than no guard" — and it survived its own module's entire
test file.

THE SCOPE IS THE WHOLE `tests/prior` DIRECTORY, and this module is the reason.
Five of the sixteen mutants below are killed ONLY from outside
`test_indexing.py`; the runner prints exactly which, under "PINNED ONLY
OUTSIDE ITS OWN TEST FILE". Under the old one-file scope those five were
invisible rather than green.

WHAT THIS SET DOES NOT COVER — stated so the table is not read as more than it is
-------------------------------------------------------------------------------
* H3 ITSELF. `h3.latlng_to_cell` is a third-party library and is not mutated.
  What IS pinned is the ARGUMENT ORDER (X6) — h3 v4 does not validate the
  latitude range, so a swapped call returns a plausible cell in the wrong
  hemisphere rather than raising.
* THE CHOICE of 52 weeks. X9/X10 prove that CHANGING `WEEKS_PER_YEAR` changes
  the answer and is caught; whether 52 is the right stride for a bird prior is
  a modelling question no mutation can answer.
* SCALE. Fixtures here are single-digit rows. A defect appearing only at
  millions of distinct cell-weeks (a group_by spilling, an int64 overflow in
  `n`) is out of reach.
* THE ORDER of the output rows. `group_by` gives no ordering guarantee and
  nothing downstream depends on one (`backoff.py` builds dicts), so a mutant
  that only re-orders rows would be EQUIVALENT and none is listed.
* `iso_week` ON A `datetime`. The column is `date32`, so pyarrow yields
  `datetime.date` and a `datetime` can never arrive. Not mutated.
* A LOWER-BOUND FOLD. `iso_week` clamps only the upper end. A `week < 1`
  clamp would be dead code on every possible input, since
  `date.isocalendar().week` is never below 1, so there is no mutant to write:
  adding the check changes no behaviour and removing it is impossible. Stated
  here rather than listed as a mutant that would be expected to survive.
"""
# ---------------------------------------------------------------------------
# TEST SCOPE (final-gate finding I1)
# ---------------------------------------------------------------------------
# Every mutant is confronted with the WHOLE `tests/prior` directory, not just
# this module's own test file. Derived consumer graph for indexing.py, by grep
# of every `from .`/`from birdology.prior` import under src/, scripts/ and
# tests/prior:
#
#   src/birdology/prior/backoff.py     from .indexing import WEEKS_PER_YEAR
#   src/birdology/prior/controls.py    from .indexing import WEEKS_PER_YEAR
#   src/birdology/prior/smoothing.py   from .indexing import WEEKS_PER_YEAR
#   src/birdology/prior/tensor.py      from .indexing import WEEKS_PER_YEAR
#   scripts/build_prior.py             WEEKS_PER_YEAR, aggregate_counts,
#                                      assign_cells
#   tests/prior/test_effort.py         aggregate_counts (the marker re-stamp)
#   tests/prior/test_leakage.py        aggregate_counts, assign_cells
#   tests/prior/test_{tensor,smoothing,controls,build_prior}.py  WEEKS_PER_YEAR
#
# That is eight of the ten files under tests/prior, which is why no filter is
# applied: the "transitive consumers" of this module ARE nearly the directory.
# Measured cost: test_indexing.py alone 0.19s, whole directory 1.27s.
# ---------------------------------------------------------------------------

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _harness  # noqa: E402  -- sibling module; this file runs as a script

REPO = _harness.REPO
TARGET = Path("src/birdology/prior/indexing.py")
TESTS = "tests/prior/test_indexing.py"

# (id, description, killer_test, [(old, new), ...])
#
# `killer_test` is the test whose failure DEMONSTRATES the mutant is caught. It
# is verified at runtime against every test file in scope: a mutant that fails
# only other tests is reported MISLABELLED rather than counted as a kill.
MUTANTS: list[tuple[str, str, str, list[tuple[str, str]]]] = [
    # ---- the guard-marker re-stamp: the gate's own proof ------------------
    (
        "X1",
        "the guard-marker re-stamp made UNCONDITIONAL — the edit whose own "
        "docstring says it 'would make this function authenticate ANY table "
        "that passed through it, which is worse than no guard'. Every "
        "realistic build routes through `aggregate_counts`, so the "
        "`target_group_ratios` marker check would still raise on a table "
        "handed to it directly while being decorative for the whole pipeline. "
        "THE GATE'S PROOF OF I1: survives all of test_indexing.py, dies only "
        "in test_effort.py",
        "test_aggregate_counts_does_NOT_mint_a_marker_for_an_unstamped_input",
        [
            (
                "    if guards_applied(table):\n        counts = mark_guards_applied(counts)",
                "    counts = mark_guards_applied(counts)",
            )
        ],
    ),
    (
        "X2",
        "the re-stamp REMOVED entirely, the opposite disjunct of X1: the "
        "marker does not survive `group_by().aggregate()` (measured), so the "
        "chain guards -> aggregate -> ratios loses it in the middle and every "
        "GUARDED build is refused. A separate mutant from X1 because a test "
        "that only checked 'an unguarded input stays unguarded' would pass "
        "under this one",
        "test_aggregate_counts_propagates_the_marker_to_its_output",
        [
            (
                "    if guards_applied(table):\n        counts = mark_guards_applied(counts)",
                "    if False:\n        counts = mark_guards_applied(counts)",
            )
        ],
    ),
    (
        "X3",
        "the re-stamp condition INVERTED: an unguarded table is stamped and a "
        "guarded one is not. Both directions wrong at once, and it dies in "
        "BOTH test files — kept because the inversion is the single most "
        "likely typo at this site and it must not be the case that only one "
        "of the two conditional tests exists",
        "test_aggregate_counts_does_NOT_mint_a_marker_for_an_unstamped_input",
        [("    if guards_applied(table):", "    if not guards_applied(table):")],
    ),
    (
        "X4",
        "the marker read from the OUTPUT rather than the input "
        "(`guards_applied(counts)`). `counts` never carries metadata at that "
        "point — the aggregate dropped it — so the condition is always false "
        "and this is X2 by a subtler route: it LOOKS like a propagation check "
        "and propagates nothing",
        "test_aggregate_counts_propagates_the_marker_to_its_output",
        [("    if guards_applied(table):", "    if guards_applied(counts):")],
    ),
    # ---- the ISO week 53 -> 52 fold --------------------------------------
    (
        "X5",
        "the week-53 fold REMOVED, so ISO week 53 reaches the grid as a real "
        "53rd week with a seventh of the usual data behind it. `int8` accepts "
        "53 silently and `tensor.py` sizes its week axis from WEEKS_PER_YEAR, "
        "so the extra week is dropped or overflows a row downstream",
        "test_iso_week_53_folds_into_52",
        [
            (
                "    return WEEKS_PER_YEAR if week > WEEKS_PER_YEAR else week",
                "    return week",
            )
        ],
    ),
    (
        "X5b",
        "the fold CLAMPS DOWNWARD instead, mapping week 53 to week 1 rather "
        "than 52. Every value still lands in 1..52 so every range assertion "
        "passes; what breaks is that week 53's days (the last days of "
        "December) are counted as the first week of the year, which is a "
        "half-year phase error in exactly the season a wintering species is "
        "being estimated for",
        "test_iso_week_53_folds_into_52",
        [
            (
                "    return WEEKS_PER_YEAR if week > WEEKS_PER_YEAR else week",
                "    return 1 if week > WEEKS_PER_YEAR else week",
            )
        ],
    ),
    (
        "X5c",
        "the fold's threshold off by one (`> WEEKS_PER_YEAR + 1`), so week 53 "
        "passes through unfolded while the fold's SHAPE is intact — a reader "
        "checking 'there is a fold here' sees one, and only 54+ (which "
        "`isocalendar()` never produces) is ever clamped. Separate mutant from "
        "X5 because X5 deletes the fold, which a presence check would catch, "
        "and this one keeps it and moves its edge. NOTE on what is NOT here: "
        "`>` weakened to `>=` is EQUIVALENT — `WEEKS_PER_YEAR if week >= "
        "WEEKS_PER_YEAR` maps 52 to 52 and 53 to 52, exactly as `>` does — so "
        "there is no such mutant, rather than one expected to survive",
        "test_iso_week_53_folds_into_52",
        [
            (
                "    return WEEKS_PER_YEAR if week > WEEKS_PER_YEAR else week",
                "    return WEEKS_PER_YEAR if week > WEEKS_PER_YEAR + 1 else week",
            )
        ],
    ),
    (
        "X7",
        "`isocalendar().week` replaced by `isocalendar().weekday`, so every "
        "record's 'week' is its DAY OF WEEK, 1..7. Every value is inside "
        "1..52, every dtype is right, the table is well-formed, and the prior "
        "becomes a day-of-week profile — the failure mode a range check "
        "cannot see",
        "test_iso_week_ordinary_date",
        [
            (
                "    week = d.isocalendar().week",
                "    week = d.isocalendar().weekday",
            )
        ],
    ),
    # ---- WEEKS_PER_YEAR, the single definition of the stride --------------
    (
        "X8",
        "`WEEKS_PER_YEAR` raised to 53. The fold then stops folding, and "
        "because five other modules size their week axis from this ONE "
        "symbol, the whole pipeline silently becomes 53-week: the tensor "
        "stride, the smoothing kernel's wrap, the build script's week grid and "
        "the sidecar's `weeks_per_year` all move together, so no "
        "cross-checking assertion between them can see it",
        "test_iso_week_covers_every_day_of_a_53_week_year",
        [("WEEKS_PER_YEAR = 52", "WEEKS_PER_YEAR = 53")],
    ),
    (
        "X9",
        "`WEEKS_PER_YEAR` lowered to 51, the other direction: week 52 is now "
        "folded away too and a real week of observations is merged into its "
        "neighbour. Separate mutant from X8 because the fold is a one-sided "
        "clamp, so raising the constant disables it while lowering it makes "
        "it bite a week it should not",
        "test_iso_week_covers_every_day_of_a_53_week_year",
        [("WEEKS_PER_YEAR = 52", "WEEKS_PER_YEAR = 51")],
    ),
    # ---- assign_cells: the two derived columns ----------------------------
    (
        "X10",
        "`h3.latlng_to_cell` called LON-FIRST. h3 v4 does not validate the "
        "latitude range, so the call succeeds and returns a plausible cell in "
        "the wrong hemisphere — Copenhagen lands in the Arabian Sea — with no "
        "error anywhere and every dtype and shape assertion still passing",
        "test_cell_uses_lat_lon_order_not_lon_lat",
        [
            (
                "    cells = [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in zip(lats, lons)]",
                "    cells = [h3.latlng_to_cell(lon, lat, resolution) for lat, lon in zip(lats, lons)]",
            )
        ],
    ),
    (
        "X11",
        "`resolution` ignored and hard-coded to 4. Every experiment in the "
        "plan sweeps resolution 3/4/5, and the sidecar records the REQUESTED "
        "resolution, so a res-5 artifact would ship res-4 cells under a res-5 "
        "label — a provenance lie that no value check can detect",
        "test_cell_is_at_the_requested_resolution",
        [
            (
                "    cells = [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in zip(lats, lons)]",
                "    cells = [h3.latlng_to_cell(lat, lon, 4) for lat, lon in zip(lats, lons)]",
            )
        ],
    ),
    (
        "X12",
        "the `week` column MISALIGNED from its own row (the week list "
        "reversed). The SET of weeks is unchanged, every value is in range, "
        "the dtype is right and the row count is right — only the pairing of "
        "week to record is destroyed, which is the defect a set-based or "
        "single-row assertion cannot see",
        "test_week_and_cell_are_aligned_to_their_OWN_row",
        [("    weeks = [iso_week(d) for d in dates]", "    weeks = [iso_week(d) for d in dates][::-1]")],
    ),
    (
        "X13",
        "the `cell` column misaligned from its own row (the cell list "
        "reversed), the same defect on the other axis — separate mutant "
        "because the two columns are built from two independent comprehensions "
        "and a fixture that varied only in date would pin one and not the "
        "other",
        "test_week_and_cell_are_aligned_to_their_OWN_row",
        [
            (
                "    cells = [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in zip(lats, lons)]",
                "    cells = [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in zip(lats, lons)][::-1]",
            )
        ],
    ),
    (
        "X14",
        "the `week` column widened to int64, breaking the dtype `effort.py` "
        "validates and REFUSES rather than casting (its own test says an int64 "
        "week 'means someone bypassed aggregate_counts'). Killed only from "
        "outside this module's test file if this module did not pin its own "
        "output dtype — it does, which is the point of pinning a contract at "
        "its producer",
        "test_week_column_is_int8",
        [
            (
                '"week", pa.array(weeks, pa.int8())',
                '"week", pa.array(weeks, pa.int64())',
            )
        ],
    ),
    (
        "X15",
        "the `cell` column emitted as `large_string`. It holds the same bytes "
        "and compares equal element-wise, so every value assertion in the "
        "repository still passes, while `table.schema == other.schema` and "
        "`pc.is_in` value sets stop matching — the leakage guards' "
        "schema-preservation test is written against the schema OBJECT",
        "test_cell_column_is_string_not_large_string",
        [
            (
                '"cell", pa.array(cells, pa.string())',
                '"cell", pa.array(cells, pa.large_string())',
            )
        ],
    ),
    # ---- aggregate_counts: the reduction itself ---------------------------
    (
        "X16",
        "the aggregation keyed on (species, cell) only, dropping `week`, so "
        "every record of a species in a cell collapses into one row and the "
        "whole temporal axis — the reason this prior exists — disappears. The "
        "`week` column survives in the schema carrying whichever week the "
        "aggregate happened to keep",
        "test_aggregate_counts_keeps_distinct_weeks_apart",
        [
            (
                '.group_by(["species_key", "cell", "week"])',
                '.group_by(["species_key", "cell"])',
            )
        ],
    ),
    (
        "X17",
        "the aggregation keyed on (species, week) only, dropping `cell`: the "
        "spatial axis collapses instead. Separate disjunct from X16 because a "
        "test that only counted output rows on a single-cell fixture would "
        "pass under one and fail under the other",
        "test_aggregate_counts_keeps_distinct_cells_apart",
        [
            (
                '.group_by(["species_key", "cell", "week"])',
                '.group_by(["species_key", "week"])',
            )
        ],
    ),
    (
        "X18",
        "the count replaced by `min` instead of `sum`, so every cell-week "
        "reports n=1 however many records it holds. Every row is present, "
        "`n >= 1` holds, the dtype is right, and the target-group ratio "
        "becomes a presence indicator rather than a count — the effort "
        "correction silently stops correcting",
        "test_aggregate_counts_sums_duplicates",
        [('.aggregate([("n", "sum")])', '.aggregate([("n", "min")])')],
    ),
    (
        "X19",
        "PROVENANCE kept in the group key, so a species/cell/week observed by "
        "two contributors emits TWO rows for one grid point. The output SCHEMA "
        "is unchanged — the trailing `.select` trims `publisher_key` away — and "
        "every `n` is a true count, so nothing about the table looks wrong; "
        "what breaks is the uniqueness `backoff.py` REFUSES to re-derive "
        "(B27/B27b reject a duplicate row rather than accumulate it), so a "
        "guarded build dies one module downstream with a message about the "
        "RatioTable. NOTE: the naive form of this mutant — adding "
        "`publisher_key` to the first `select` only — is NOT a mutant at all: "
        "the trailing select removes it again, and the edit merely crashes "
        "downstream fixtures that have no such column",
        "test_aggregate_counts_emits_ONE_row_per_species_cell_week",
        [
            (
                '        table.select(["species_key", "cell", "week"])',
                '        table.select(["species_key", "cell", "week", "publisher_key"])',
            ),
            (
                '        .group_by(["species_key", "cell", "week"])',
                '        .group_by(["species_key", "cell", "week", "publisher_key"])',
            ),
        ],
    ),
    (
        "X20",
        "the output column order changed (week before cell). Every value is "
        "correct and every by-name read still works, so nothing that reads "
        "`row[\"cell\"]` can see it — but the schema is part of the published "
        "contract and `test_aggregate_counts_schema` asserts the list",
        "test_aggregate_counts_schema",
        [
            (
                '        ["species_key", "cell", "week", "n"]\n    )',
                '        ["species_key", "week", "cell", "n"]\n    )',
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
