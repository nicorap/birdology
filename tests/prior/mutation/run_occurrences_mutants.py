#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/occurrences.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_occurrences_mutants.py
    uv run python tests/prior/mutation/run_occurrences_mutants.py --only O1 O9
    uv run python tests/prior/mutation/run_occurrences_mutants.py --verify-labels

WHY THIS HARNESS EXISTS — final-gate finding I1
----------------------------------------------
194 lines, no harness through ten tasks. This module is the pipeline's ONLY
ingestion point, so every value it emits, every row it drops and every field it
nulls is invisible from here on: a defect here produces a well-formed archive
of wrong numbers, never an error.

Two of its contracts are load-bearing across module boundaries and neither is
re-derived downstream:

* NULL, NOT `""`, for absent free text. `leakage.py` filters on `recorded_by`
  and `publisher_key`; under Arrow semantics a null never equals anything,
  whereas `""` is a real value that matches every row whose source field was
  absent. An `""` would make the contributor guard remove every
  provenance-less record in the archive.
* ROW ABSENCE IS THE ONLY ABSENCE. `effort.py`'s docstring is explicit that a
  missing cell-week means "nobody looked", so a row kept with a sentinel
  species key (`-1`, `0`) or a coerced coordinate manufactures an observation.
  "Reject, do not coerce" is the branch's rule and this is where input arrives.

WHAT THIS SET DOES NOT COVER — stated so the table is not read as more than it is
-------------------------------------------------------------------------------
* `csv` AND `zipfile` THEMSELVES. Third-party/stdlib; not mutated. What IS
  pinned is the DIALECT (tab-delimited, QUOTE_NONE — O12/O13), because
  SIMPLE_CSV is unquoted TSV and a comma dialect yields an EMPTY table rather
  than an error.
* THE COLUMN NAMES GBIF USES. `speciesKey`, `decimalLatitude` etc. are GBIF's
  spelling and no mutation can tell us they are right; O14 only proves that
  REMOVING one from the required set stops the header check firing.
* SCALE AND MEMORY. The real archive is 38.1M rows streamed through a
  `DictReader`. Fixtures here are a handful of rows, so a defect that only
  appears when the reader is consumed lazily versus eagerly is out of reach.
  `read_occurrences` accumulates python lists and its peak memory is pinned
  only in `scripts/build_prior.py`'s measurement, not here.
* THE `errors="replace"` DECODE. O11 proves a non-UTF-8 byte does not abort
  the extract. WHICH replacement character lands in `recorded_by` is not
  pinned, and a mojibaked contributor name that silently stops matching an
  evaluation split is a real failure mode this harness cannot see. (It is the
  failure mode C1's match-rate guard exists to catch, one stage later.)
"""
# ---------------------------------------------------------------------------
# TEST SCOPE (final-gate finding I1)
# ---------------------------------------------------------------------------
# Every mutant is confronted with the WHOLE `tests/prior` directory. Derived
# consumers of occurrences.py, by grep of every import under src/, scripts/
# and tests/prior:
#
#   scripts/build_prior.py          from birdology.prior.occurrences import
#                                   read_occurrences  (the pipeline entry)
#   tests/prior/test_build_prior.py read_occurrences  (end-to-end fixtures)
#   tests/prior/test_indexing.py    OCCURRENCE_COLUMNS (the column contract
#                                   assign_cells appends to)
#
# No src module imports it — it is the head of the pipeline — so the only
# cross-module pins are the build script's and test_indexing's. Measured cost:
# test_occurrences.py alone 0.23s, whole directory 1.27s.
# ---------------------------------------------------------------------------

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _harness  # noqa: E402  -- sibling module; this file runs as a script

REPO = _harness.REPO
TARGET = Path("src/birdology/prior/occurrences.py")
TESTS = "tests/prior/test_occurrences.py"

# (id, description, killer_test, [(old, new), ...])
MUTANTS: list[tuple[str, str, str, list[tuple[str, str]]]] = [
    # ---- NULL, not "" -----------------------------------------------------
    (
        "O1",
        "`_optional` returns the EMPTY STRING instead of None for an absent "
        "free-text field. Arrow accepts it, the schema is unchanged, and "
        "`leakage.py`'s contributor filter — which relies on null never "
        "equalling anything — starts matching every provenance-less row in the "
        "archive against a blank split entry",
        "test_absent_text_field_is_null_not_empty_string",
        [
            (
                '    text = (value or "").strip()\n    return text or None',
                '    text = (value or "").strip()\n    return text',
            )
        ],
    ),
    (
        "O1b",
        "`_optional` stops stripping, so a whitespace-only source field "
        "becomes the literal `\"   \"` rather than null: the same defect for "
        "the realistic spelling of 'absent' in an exported TSV, and a "
        "separate disjunct because a fixture using `\"\"` alone passes under it",
        "test_whitespace_only_text_field_is_null",
        [
            (
                '    text = (value or "").strip()',
                '    text = value or ""',
            )
        ],
    ),
    (
        "O1c",
        "`_optional` bypassed for `recorded_by` only — the single column the "
        "leakage guard keys on. Pinned separately from O1 because the three "
        "optional columns go through one helper and a mutant on the helper "
        "cannot show that the CALL SITE for the load-bearing column is right",
        "test_null_recorded_by_never_equals_a_contributor",
        [
            (
                '        cols["recorded_by"].append(_optional(row.get("recordedBy")))',
                '        cols["recorded_by"].append(row.get("recordedBy"))',
            )
        ],
    ),
    (
        "O2",
        "a PRESENT text field nulled as well (`_optional` always returns "
        "None), the opposite disjunct of O1: the guard then matches nothing "
        "and the leakage protection is silently inert. A test asserting only "
        "'absent becomes null' passes under this",
        "test_present_text_fields_are_not_nulled",
        [
            (
                '    text = (value or "").strip()\n    return text or None',
                '    text = (value or "").strip()\n    return None',
            )
        ],
    ),
    # ---- reject, do not coerce -------------------------------------------
    (
        "O3",
        "the species key parsed via float, so `2492576.9` becomes 2492576 — a "
        "DIFFERENT taxon, silently. The row is kept, the schema is valid, and "
        "nothing downstream can tell the key was invented",
        "test_a_float_shaped_species_key_is_DROPPED_not_truncated",
        [("                    species_key = int(species_raw)", "                    species_key = int(float(species_raw))")],
    ),
    (
        "O4",
        "an unparseable species key replaced by a SENTINEL (-1) and the row "
        "KEPT. Row counts stay plausible, the schema is valid, and a phantom "
        "species accumulates a target-group share across every cell-week in "
        "the archive",
        "test_an_unusable_row_is_DROPPED_not_kept_with_a_sentinel",
        [
            (
                '                except ValueError:\n'
                '                    dropped["no_species_key"] += 1\n'
                "                    continue",
                '                except ValueError:\n'
                '                    dropped["no_species_key"] += 1\n'
                "                    species_key = -1",
            )
        ],
    ),
    (
        "O5",
        "the coordinate-range check REMOVED, so a latitude of 955.1 reaches "
        "`h3.latlng_to_cell`, which does not validate it either: the record "
        "lands in a real H3 cell somewhere impossible and the prior gains a "
        "cell no observer ever visited",
        "test_drops_out_of_range_or_non_finite_coordinates",
        [("    if not _coordinates_usable(lat, lon):", "    if False:")],
    ),
    (
        "O5b",
        "the coordinate bounds made EXCLUSIVE, which rejects the poles and the "
        "antimeridian — legitimate coordinates — so a real record is dropped "
        "for being exactly on a boundary. The opposite-direction disjunct of "
        "O5: a test that only checks 'out of range is dropped' passes under it",
        "test_keeps_coordinates_on_the_boundary",
        [
            (
                "        and -90.0 <= lat <= 90.0\n        and -180.0 <= lon <= 180.0",
                "        and -90.0 < lat < 90.0\n        and -180.0 < lon < 180.0",
            )
        ],
    ),
    (
        "O5c",
        "the BOUNDS dropped from `_coordinates_usable`, leaving only the "
        "finiteness half: a latitude of 955.1 is finite, so it passes, and "
        "`h3.latlng_to_cell` does not validate it either. The other disjunct "
        "of the same compound guard from O5 (which removes the whole call), "
        "and the one that leaves a check LOOKING present",
        "test_drops_out_of_range_or_non_finite_coordinates",
        [
            (
                "        and -90.0 <= lat <= 90.0\n        and -180.0 <= lon <= 180.0\n    )",
                "    )",
            )
        ],
    ),
    (
        "O6",
        "latitude and longitude SWAPPED at the point they are read, so every "
        "record's coordinates are transposed before `assign_cells` ever sees "
        "them. Both values stay in range for Denmark-shaped data only by luck; "
        "for Copenhagen (55.69N, 12.57E) the swap is in range and lands in the "
        "Arabian Sea. `test_indexing`'s lat/lon-order test cannot see this — it "
        "tests the H3 call, and this defect is upstream of it",
        "test_reads_good_row",
        [
            (
                '                lat_raw = (row.get("decimalLatitude") or "").strip()\n'
                '                lon_raw = (row.get("decimalLongitude") or "").strip()',
                '                lat_raw = (row.get("decimalLongitude") or "").strip()\n'
                '                lon_raw = (row.get("decimalLatitude") or "").strip()',
            )
        ],
    ),
    # ---- the date parse ---------------------------------------------------
    (
        "O7",
        "the timestamp split removed from `_parse_date`, so "
        "`2025-11-14T07:30:00` no longer parses and every record from a "
        "publisher that exports timestamps is dropped. Silent: the INFO line "
        "reports the loss, nothing raises, and the prior simply lacks those "
        "contributors",
        "test_event_date_accepts_timestamp_form",
        [
            (
                '    head = raw.split("/", 1)[0].split("T", 1)[0]',
                '    head = raw.split("/", 1)[0]',
            )
        ],
    ),
    (
        "O8",
        "an eventDate RANGE collapsed to its END date rather than its start. "
        "Both are valid dates and both land in range, so no structural check "
        "can see it; what changes is which ISO week a multi-week observation "
        "is attributed to — a phase error in the seasonal signal",
        "test_event_date_accepts_range_form",
        [
            (
                '    head = raw.split("/", 1)[0].split("T", 1)[0]',
                '    head = raw.split("/", 1)[-1].split("T", 1)[0]',
            )
        ],
    ),
    (
        "O9",
        "a coarse date (`2025-11`, `2025`) REPAIRED to the first of the "
        "period instead of dropped. Month- and year-precision records would "
        "then all pile into week 1 or the first week of each month, "
        "manufacturing a seasonal spike out of imprecision. The exact "
        "silent-repair-at-a-boundary the plan forbids",
        "test_coarse_precision_dates_are_dropped",
        [
            (
                '    except ValueError:\n        return None',
                '    except ValueError:\n        parts = head.split("-")\n'
                "        try:\n"
                "            return date(\n"
                "                int(parts[0]),\n"
                "                int(parts[1]) if len(parts) > 1 else 1,\n"
                "                1,\n"
                "            )\n"
                "        except (ValueError, IndexError):\n"
                "            return None",
            )
        ],
    ),
    (
        "O10",
        "the empty-eventDate early return removed. RETIRED AS EQUIVALENT, with "
        "the condition NAMED rather than an aggregate quoted: the branch is "
        "live only when `raw` is falsy, and then `head == ''`, and "
        "`date.fromisoformat('')` raises ValueError — which the very next "
        "`except` already catches and turns into the same `None`. Verified by "
        "execution, not assumed. The early return is a readability guard, not "
        "a behavioural one, so there is no fixture any test could use to "
        "distinguish the two and a live mutant here would sit in the table as "
        "a permanent survivor",
        "test_drops_rows_without_event_date",
        [("    if not raw:\n        return None", "    if False:\n        return None")],
    ),
    # ---- member selection and the header contract ------------------------
    (
        "O11",
        "`_occurrence_member` returns the FIRST csv member rather than the "
        "first one whose header validates. A SIMPLE_CSV download ships "
        "`citations.csv` and `rights.csv` beside the data and zip member order "
        "is not guaranteed, so this yields an EMPTY table from a perfectly "
        "good archive — or, worse, a ValueError naming the wrong member",
        "test_selects_data_table_when_citations_csv_comes_first",
        [
            (
                "    for name in csvs:\n        if _header_validates(zf, name):\n            return name",
                "    for name in csvs:\n        if True:\n            return name",
            )
        ],
    ),
    (
        "O12",
        "`occurrence.txt` no longer preferred, so a zip holding both "
        "`occurrence.txt` and a stray `.csv` may pick the csv. The real "
        "SIMPLE_CSV archive holds exactly one data member, so this is only "
        "reachable on a DwC-A-shaped archive — which is precisely the case the "
        "module's docstring says it does not handle and must not silently "
        "half-handle",
        "test_occurrence_txt_wins_over_a_stray_csv",
        [
            (
                '        if name.endswith("occurrence.txt"):\n            return name',
                '        if False:\n            return name',
            )
        ],
    ),
    (
        "O13",
        "the CSV delimiter changed to a comma. SIMPLE_CSV is tab-delimited, so "
        "every row becomes one giant field, every required column is 'missing' "
        "and the archive is refused — loud, but it pins that the dialect is "
        "part of the contract",
        "test_reads_good_row",
        [
            (
                'return csv.DictReader(stream, delimiter="\\t", quoting=csv.QUOTE_NONE)',
                'return csv.DictReader(stream, delimiter=",", quoting=csv.QUOTE_NONE)',
            )
        ],
    ),
    (
        "O14",
        "`QUOTE_NONE` dropped, so a stray double quote in a contributor name "
        "makes `csv` swallow the rest of the row and the following lines into "
        "one field. SIMPLE_CSV does not quote, so a quote is DATA; this is the "
        "defect that silently merges records",
        "test_quoted_field_is_taken_literally",
        [
            (
                'return csv.DictReader(stream, delimiter="\\t", quoting=csv.QUOTE_NONE)',
                'return csv.DictReader(stream, delimiter="\\t")',
            )
        ],
    ),
    (
        "O15",
        "the header validation REMOVED from `read_occurrences`, so an archive "
        "missing `decimalLatitude` produces an EMPTY table (every row fails "
        "the float parse) instead of a ValueError naming the missing column. A "
        "zero-row prior with no error anywhere",
        "test_occurrence_txt_with_wrong_headers_raises",
        [("            if missing:", "            if False:")],
    ),
    (
        "O16",
        "`eventDate` dropped from the REQUIRED column set, so an archive "
        "without it passes the header check and every row is then dropped for "
        "an unparseable date. One mutant per required column would be four "
        "near-identical rows; this is the one whose absence is silent rather "
        "than loud (a missing coordinate column also empties the table, but "
        "`speciesKey`'s absence is caught by the same check on the same line)",
        "test_member_with_unrelated_headers_raises_naming_missing_columns",
        [
            (
                '    {"speciesKey", "decimalLatitude", "decimalLongitude", "eventDate"}',
                '    {"speciesKey", "decimalLatitude", "decimalLongitude"}',
            )
        ],
    ),
    (
        "O17",
        "the non-UTF-8 decode made STRICT, so one latin-1 byte anywhere in a "
        "38-million-row archive aborts the whole extract with a "
        "UnicodeDecodeError. Danish contributor names make this near-certain",
        "test_latin1_byte_in_recorded_by_does_not_abort",
        [
            (
                'errors="replace"',
                'errors="strict"',
            )
        ],
    ),
    # ---- the emitted table -----------------------------------------------
    (
        "O18",
        "`event_date` emitted as a string rather than date32, so "
        "`indexing.iso_week` receives a `str` and `d.isocalendar()` raises. "
        "Loud — but it pins that the dtype, not just the value, is the "
        "contract `assign_cells` consumes",
        "test_table_schema_matches_declared_types",
        [('("event_date", pa.date32()),', '("event_date", pa.string()),')],
    ),
    (
        "O19",
        "`species_key` emitted as int32. Every GBIF key fits, so no value "
        "changes and no value-based test can see it; `effort.py` validates "
        "int64 and REFUSES rather than casting, so a bypassed pipeline would "
        "be rejected one stage later with a message about the wrong module",
        "test_table_schema_matches_declared_types",
        [('("species_key", pa.int64()),', '("species_key", pa.int32()),')],
    ),
    (
        "O20",
        "two columns of `OCCURRENCE_COLUMNS` transposed (`lat`/`lon`), which "
        "silently swaps the two columns' CONTENTS at construction because the "
        "table is built by zipping the name order against the schema order. "
        "Every value is a valid coordinate and the schema still declares "
        "float64 for both",
        "test_schema_is_the_contract_task_3_consumes",
        [
            (
                '    "lat",\n    "lon",\n    "event_date",',
                '    "lon",\n    "lat",\n    "event_date",',
            )
        ],
    ),
    (
        "O21",
        "the dropped-row counters incremented AFTER the filters rather than "
        "the total, so the INFO line reports `kept 1 of 1` for an archive "
        "whose other rows vanished. That line is the operator's only signal "
        "that an upstream schema change has started discarding most of a "
        "download",
        "test_the_dropped_row_TOTAL_counts_every_row_read",
        [
            (
                "            for row in reader:\n                total += 1",
                "            for row in reader:\n                pass",
            )
        ],
    ),
    (
        "O22",
        "the per-reason drop counters all incremented into ONE bucket, so the "
        "log reports five bad coordinates where the truth is one bad key, two "
        "bad coordinates and two bad dates — an operator diagnosing a broken "
        "download is sent to the wrong column",
        "test_logs_dropped_row_counts_by_reason",
        [
            (
                '                    dropped["no_species_key"] += 1',
                '                    dropped["bad_coordinates"] += 1',
            )
        ],
    ),
]

# O10 is retired as EQUIVALENT — the condition is named in its own description
# and was verified by execution, not inferred from an aggregate statistic. It
# stays in the table so the argument is on the record and a future reader can
# re-check it; it is removed from the LIVE set here, because a mutant no
# possible test can kill must not sit in the output as a survivor.
#
# The other equivalence the harness found and this file does NOT hide: the
# `math.isfinite` half of `_coordinates_usable` is subsumed by its bounds half
# (no float is non-finite AND inside -90..90 or -180..180; nan/inf/-inf all
# fail every comparison). So an isfinite-removal mutant is equivalent and is
# not listed; O5c mutates the disjunct that IS live — the bounds — which a
# finite 955.1 walks straight through.
MUTANTS = [m for m in MUTANTS if m[0] != "O10"]


def main() -> int:
    return _harness.main(
        target=TARGET,
        mutants=MUTANTS,
        own_tests=TESTS,
        allow_unnamed=False,
    )


if __name__ == "__main__":
    raise SystemExit(main())
