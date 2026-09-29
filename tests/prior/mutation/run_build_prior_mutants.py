#!/usr/bin/env python3
"""Mutation harness for `scripts/build_prior.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_build_prior_mutants.py
    uv run python tests/prior/mutation/run_build_prior_mutants.py --only B1 B7
    uv run python tests/prior/mutation/run_build_prior_mutants.py --verify-labels

Structure is copied from `run_tensor_mutants.py`, the most evolved harness in
this plan, including every property that makes its result trustworthy:

1. it refuses to start unless `git status --porcelain` is clean for the target
   file, so a mutant can never be committed by accident;
2. every pattern must occur EXACTLY ONCE, and NOT-APPLIED is reported
   separately from SURVIVED -- a stale pattern that matched nothing would
   otherwise masquerade as a kill -- and NOT-APPLIED makes the harness EXIT
   NON-ZERO;
3. it clears every `__pycache__` BEFORE and AFTER each mutant, with
   `shutil.rmtree`. CPython invalidates a `.pyc` on (mtime, size), so a
   SAME-LENGTH mutation can leave a mutant `.pyc` that outlives its own source
   restoration and fake either a kill or a survival;
4. every mutant NAMES the test that kills it, and that name is CHECKED at
   runtime: a mutant that fails only OTHER tests is scored MISLABELLED, not a
   kill, and also exits non-zero;
5. `--verify-labels` prints the unified diff each mutant actually applies, so
   each LABEL can be read against its EDIT rather than trusted;
6. a mutant whose output shows an `AttributeError`, `NameError`, `ImportError`
   or `SyntaxError` raised FROM THE TARGET MODULE is scored CRASHED, never a
   kill, and CRASHED exits non-zero. A crash-everything edit trivially
   contains any named killer among its failures, so the name check in (4)
   passes while the row proves nothing about the contract it claims.

A mutant is KILLED if at least one test fails, the named killer is among the
failures, and the failure is NOT a crash in the target module. Zero failures
is a SURVIVOR: the suite does not pin that behaviour.

ONE MUTANT PER DISJUNCT
-----------------------
* the two halves of T10-R1's grid -> B1 (`weeks=`), B2 (`cells=`)
* the DOI on the full prior vs on the CONTROLS -> B10, B11 (per artifact
  class: a full prior written with a DOI and controls written without it is
  a perfectly plausible half-provenanced release)
* `--test-split` / `--no-test-split` -> B20 (the requirement), B21 (the
  mutual exclusion)
* the per-resolution cell derivation -> B24 (hoisted out of the loop), B25
  (a fixed resolution inside it)
* the zero-match guard's two clauses -> B30 (the detector fires at all),
  B31 (the `and test_recordings` conjunct)
* `load_test_split`'s empty / missing-column checks -> B34, B35

WHAT THIS SET DOES NOT COVER -- stated so the table is not read as more than
it is
--------------------------------------------------------------------------
* WHETHER THE PRIOR IS CORRECT. Every mutant here concerns ORCHESTRATION:
  which axes are requested, which table reaches which stage, which provenance
  is attached, whether an error propagates. That the backoff shrinks
  correctly, that the smoother's seam is closed, that the tensor packs its
  columns right -- all of that is pinned by the six upstream harnesses, and
  no mutation of a build script can speak to it.
* THE `cells=` ARGUMENT'S END-TO-END VALUE. Measured: removing `cells=` from
  `build_from_archive` leaves the ENTIRE end-to-end suite green, because the
  requested cell axis and the backoff's observed-axes default are the same
  set -- both are derived from the same `counts` table. B2 therefore mutates
  the estimator function's contract (does it honour the axis it is GIVEN),
  which is observable, rather than claiming an end-to-end kill that does not
  exist. The `weeks=` half is genuinely different: B1 dies twice over.
  This asymmetry is real and is reported as such rather than papered over
  with a mutant that would have had to be labelled dishonestly.
* SCALE. The largest fixture is 2 species x 4 cells x 52 weeks = 416
  contexts. Nothing here would catch a defect that appears only at national
  scale -- and the memory behaviour of the full grid is exactly such a
  property. It is MEASURED in the task report (peak RSS against species and
  cell count, with the dominant term identified) rather than asserted by a
  test, because a peak-RSS assertion on a 416-context fixture would pin
  nothing and a national-scale one would take minutes per run.
* THAT THE DOI IS THE RIGHT ONE. B10/B11 prove the DOI the caller supplied
  reaches every artifact. That it NAMES the GBIF download this archive
  actually came from is not checkable by any test: `--gbif-doi` is a free
  string, and the guard tests emptiness, not correspondence. A build script
  run with last month's DOI produces a perfectly well-formed, perfectly
  mis-attributed release.
* THE ARCHIVE ITSELF. `read_occurrences` drops rows for three reasons and
  logs the counts; nothing here asserts that a real GBIF SIMPLE_CSV download
  has the columns it claims to. That is `test_occurrences.py`'s ground, and
  ultimately it is a property of GBIF rather than of this repository.
* CONCURRENCY. Two builds writing into one `--out` interleave. The sidecar's
  nnz check would catch the common case on load, but nothing here exercises
  it and no lock is taken.
* THE `_ZeroMatchDetector`'s COUPLING TO THE LOG MESSAGE. B32 pins that the
  marker constant is what is matched, so a reworded message cannot silently
  deafen the detector. What is NOT pinned is that the leakage module keeps
  EMITTING a warning at all in that situation: deleting the warning entirely
  is a mutation of `leakage.py`, and `run_leakage_mutants.py` owns it.
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
TARGET = Path("scripts/build_prior.py")
TESTS = "tests/prior/test_build_prior.py"

# (id, description, killer_test, [(old, new), ...], optional target path)
#
# `killer_test` is the test whose failure DEMONSTRATES the mutant is caught.
# It is verified at runtime: a mutant that fails only other tests is reported
# MISLABELLED rather than counted as a kill.
#
# The optional FIFTH element names the file the edit applies to; it defaults
# to `TARGET`. Almost every mutant edits the build script. One (B3b) edits the
# TEST file instead, to prove that a FIXTURE property is load-bearing: B3's
# chunk-scoping edit is EQUIVALENT on a fixture where every species occupies
# every cell-week, so the fixture's sparsity is part of what makes the suite
# able to catch B3 at all. A fixture weakness is a coverage defect exactly as
# a missing assertion is, and this is the only way to score one.
Mutant = tuple[str, str, str, list[tuple[str, str]]] | tuple[
    str, str, str, list[tuple[str, str]], Path
]
MUTANTS: list[Mutant] = [
    # ---- T10-R1: the full 52-week grid, passed explicitly ----------------
    (
        "B1",
        "`weeks=` DROPPED from the backoff call, so the grid falls back to "
        "the OBSERVED weeks -- three of 52 on the fixture. Every stored value "
        "stays correct and the artifact stays well-formed; what changes is "
        "that ~94% of the year is no longer ESTIMATED at all and reads back "
        "as a bare FLOOR ('no evidence') instead of a shrunk estimate. This "
        "is the exact defect T10-R1 exists to prevent, and it is invisible in "
        "the artifact except through its coverage",
        "test_an_unobserved_week_is_ESTIMATED_not_defaulted_to_the_floor",
        [("                weeks=weeks,\n", "")],
    ),
    (
        "B1b",
        "the week grid narrowed to a HALF-YEAR window (weeks 1..26) while "
        "still being passed explicitly. Separate mutant from B1 because B1's "
        "edit removes the argument, which a `weeks=` presence check would "
        "catch; this one keeps the argument and changes its EXTENT, which is "
        "the form a 'documented window' would take if it were ever adopted "
        "WITHOUT being stated in the sidecar. T10-R1 permits a window only if "
        "the artifact declares it, so an undeclared one must die",
        "test_every_context_of_the_grid_is_stored_not_merely_reachable",
        [
            (
                "    weeks = list(range(1, WEEKS_PER_YEAR + 1))",
                "    weeks = list(range(1, WEEKS_PER_YEAR // 2 + 1))",
            )
        ],
    ),
    (
        "B1c",
        "`WEEKS_PER_YEAR` replaced by a hard-coded 52 in the week grid. The "
        "NUMBER is identical today, so no value changes and NO value-based "
        "test can see it -- which is why the killer rebinds the symbol rather "
        "than comparing numbers. The plan's rule is that the week stride has "
        "ONE definition, in `indexing`, so a change to it propagates rather "
        "than leaving the build packing a 52-week grid into a tensor indexed "
        "by a different stride",
        "test_the_week_axis_follows_WEEKS_PER_YEAR_not_a_literal_52",
        [
            (
                "    weeks = list(range(1, WEEKS_PER_YEAR + 1))",
                "    weeks = list(range(1, 52 + 1))",
            )
        ],
    ),
    (
        "B2",
        "`cells=` DROPPED, so the backoff falls back to its observed-cell "
        "default. HONEST LABEL: in the real pipeline the two sets COINCIDE "
        "(both derive from the same `counts` table), so this edit changes no "
        "end-to-end value and its killer is the FUNCTION-level contract test, "
        "not a build one. What it proves is that `estimate_over_full_grid` "
        "honours the cell axis it is GIVEN rather than the one it can infer "
        "-- the property a caller building over a fixed national cell set "
        "would rely on. It does NOT prove the build would differ today",
        "test_the_requested_CELL_axis_is_honoured_not_the_observed_one",
        [("                cells=list(cells),\n", "")],
    ),
    # ---- chunking must not change a single value -------------------------
    (
        "B3",
        "each chunk's ratio table RESTRICTED to that chunk's own species, so "
        "the global and cell levels of the hierarchy are computed from a "
        "fraction of the data. Every value is still finite, in range and "
        "entirely plausible -- the prior simply becomes a function of the "
        "arbitrary `--species-chunk` the operator happened to pass, and two "
        "runs at different chunk sizes publish different numbers under the "
        "same name",
        "test_species_chunking_is_bit_exact",
        [
            (
                "                ratios,\n"
                "                ordered[start : start + species_chunk],",
                "                ratios.filter(\n"
                "                    pa.compute.is_in(\n"
                '                        ratios.column("species_key"),\n'
                "                        value_set=pa.array(\n"
                "                            ordered[start : start + species_chunk],\n"
                "                            pa.int64(),\n"
                "                        ),\n"
                "                    )\n"
                "                ),\n"
                "                ordered[start : start + species_chunk],",
            )
        ],
    ),
    (
        "B3b",
        "the SPARSE species removed from the fixture -- a mutation of the TEST "
        "rather than the target, applied here so the harness records why the "
        "fixture is shaped as it is. Measured: with every species present in "
        "every cell-week, B3's chunk-scoping edit changes nothing at all "
        "(max |diff| = 0), because the hierarchy's parent denominators sum "
        "`total` over cell-weeks and every chunk then sees the same key set. "
        "With one species in one cell-week the same edit moves its value by "
        "0.071. The fixture's sparsity is what makes B3 non-equivalent",
        "test_the_fixture_is_sparse_in_species_not_just_in_time",
        [
            (
                "            if wi == SPARSE_WEEK_INDEX and pi == SPARSE_POINT_INDEX:",
                "            if True:",
            )
        ],
        Path(TESTS),
    ),
    (
        "B4",
        "the chunk WINDOW made half-open at the wrong end -- "
        "`start : start + species_chunk - 1` -- so one species is silently "
        "dropped from every chunk. The build succeeds, the artifact is "
        "well-formed, and a fraction of the label set is simply absent from "
        "the prior's species axis with nothing raised",
        "test_species_chunking_is_bit_exact",
        [
            (
                "                ordered[start : start + species_chunk],",
                "                ordered[start : start + species_chunk - 1],",
            )
        ],
    ),
    (
        "B5",
        "the `species_chunk >= 1` guard removed, so a chunk size of 0 makes "
        "`range(0, n, 0)` raise a raw ValueError from the standard library "
        "with no message about the contract -- and a NEGATIVE chunk yields an "
        "EMPTY range, hence an empty grid, hence a prior with no species at "
        "all, written successfully",
        "test_a_species_chunk_below_one_is_rejected",
        [("    if species_chunk < 1:", "    if False:")],
    ),
    # ---- provenance: the DOI reaches EVERY artifact ----------------------
    (
        "B10",
        "the DOI dropped from the FULL prior, so `save_prior` refuses and the "
        "build fails -- loudly, which is the design. The mutant proves the "
        "refusal is reachable from this script at all: without it a release "
        "could ship a full prior whose sidecar names no download",
        "test_doi_travels_into_every_artifact",
        [("    full.gbif_doi = gbif_doi", '    full.gbif_doi = ""')],
    ),
    (
        "B11",
        "the DOI dropped from the CONTROLS only, by blanking it after "
        "`_respec` copied it. SEPARATE mutant from B10: a full prior with "
        "provenance and four controls without it is a half-provenanced "
        "release, and every test that only inspects the full prior passes on "
        "it. A control is published under the same CC BY 4.0 terms and "
        "appears in the same results table, so it is exactly as uncitable "
        "without a DOI",
        "test_doi_travels_into_every_artifact",
        [("        control.gbif_doi = gbif_doi", '        control.gbif_doi = ""')],
    ),
    (
        "B12",
        "only the FIRST control written, so the paper's credibility section "
        "ships one null hypothesis instead of four. Nothing raises: the three "
        "missing artifacts simply never appear in the output directory",
        "test_writes_full_prior_and_four_controls",
        [
            (
                "    for name, make in CONTROLS.items():",
                "    for name, make in list(CONTROLS.items())[:1]:",
            )
        ],
    ),
    (
        "B13",
        "the `--gbif-doi` flag made OPTIONAL with an empty default, the stale "
        "form the brief's own snippet carries. The build then runs the whole "
        "pipeline -- hours, at national scale -- before `save_prior` refuses "
        "at the write. The refusal is not lost, but the failure moves from an "
        "instant usage error to the end of a long job",
        "test_the_cli_requires_a_gbif_doi",
        [
            (
                '        "--gbif-doi",\n        required=True,',
                '        "--gbif-doi",\n        default="",',
            )
        ],
    ),
    (
        "B14",
        "the artifact stem stripped of its RESOLUTION, so a `--resolutions "
        "3 4 5` sweep writes all three grids to the same five filenames and "
        "each overwrites the last. The final directory holds one resolution "
        "claiming to be the release, with no error anywhere",
        "test_filenames_encode_source_resolution_and_variant",
        [
            (
                '    stem = f"prior_{source}_r{resolution}"',
                '    stem = f"prior_{source}"',
            )
        ],
    ),
    (
        "B15",
        "the output directory hard-coded to a cluster path instead of the "
        "`out_dir` PARAMETER -- the global constraint this plan states for "
        "every artifact path. On a developer machine it writes outside the "
        "tree; on the cluster it silently ignores `--out`",
        "test_artifact_paths_come_from_the_out_dir_parameter",
        [
            (
                '    written.append(save_prior(full, out_dir / f"{stem}_full.npz"))',
                "    written.append(\n"
                "        save_prior(\n"
                '            full, Path("/scratch/priors") / f"{stem}_full.npz"\n'
                "        )\n"
                "    )",
            )
        ],
    ),
    # ---- T10-R3: the CLI obligations -------------------------------------
    (
        "B20",
        "the split argument group made OPTIONAL, so a build with NEITHER "
        "`--test-split` nor `--no-test-split` runs with an empty split. "
        "`drop_test_contributor_cells` returns early and SILENTLY on an empty "
        "split, so the resulting prior is unguarded and byte-shaped exactly "
        "like a guarded one -- the highest-consequence failure in this plan, "
        "with no automated detector anywhere else",
        "test_the_cli_requires_a_split_decision",
        [
            (
                "    split = ap.add_mutually_exclusive_group(required=True)",
                "    split = ap.add_mutually_exclusive_group(required=False)",
            )
        ],
    ),
    (
        "B21",
        "the split flags made an ORDINARY group rather than a mutually "
        "exclusive one, so `--test-split s.csv --no-test-split` is accepted "
        "and the `--no-test-split` branch wins: the operator supplies a split "
        "and the build silently ignores it. Separate mutant from B20, which "
        "removes the REQUIREMENT: a fixture passing neither flag never "
        "exercises the exclusion, and one passing both never exercises the "
        "requirement",
        "test_the_cli_refuses_both_split_flags_at_once",
        [
            (
                "    split = ap.add_mutually_exclusive_group(required=True)",
                "    split = ap.add_argument_group()",
            )
        ],
    ),
    (
        "B22",
        "`logging.basicConfig` moved AFTER the pipeline loop. The call is "
        "still present, so a test that merely checks it happened passes -- "
        "but every WARNING the build emits, including the zero-match one that "
        "is the shared symptom of four silent failure modes, is discarded by "
        "the unconfigured root logger before it is ever configured",
        "test_main_configures_logging_before_any_pipeline_call",
        [
            (
                "    logging.basicConfig(\n"
                "        level=logging.INFO,\n"
                '        format="%(asctime)s %(levelname)s %(name)s: %(message)s",\n'
                "    )\n",
                "",
            ),
            (
                "        for path in paths:\n            print(path)",
                "        for path in paths:\n"
                "            print(path)\n"
                "    logging.basicConfig(\n"
                "        level=logging.INFO,\n"
                '        format="%(asctime)s %(levelname)s %(name)s: %(message)s",\n'
                "    )",
            ),
        ],
    ),
    (
        "B23",
        "the log level raised to ERROR, so INFO and WARNING are both "
        "suppressed. `basicConfig` is still called FIRST, so B22's ordering "
        "check passes; what is lost is the zero-match WARNING itself, which "
        "is the only observable symptom the leakage module emits",
        "test_main_configures_logging_before_any_pipeline_call",
        [("        level=logging.INFO,", "        level=logging.ERROR,")],
    ),
    (
        "B24",
        "the test recordings built ONCE, OUTSIDE the resolution loop, at the "
        "first resolution -- the stale form T10-R3 rejects. A "
        "`TestRecording`'s cell is fixed at the resolution it was built at, "
        "so every later resolution of the sweep hits the leakage guard's "
        "resolution check and RAISES. Before Task 4 it silently matched "
        "nothing instead, which is why the defect was invisible in the "
        "original draft",
        "test_a_sweep_with_a_split_succeeds_at_every_resolution",
        [
            (
                "    for resolution in args.resolutions:",
                "    _fixed = recordings_at_resolution(split, args.resolutions[0])\n"
                "    for resolution in args.resolutions:",
            ),
            (
                "            test_recordings=recordings_at_resolution(split, resolution),",
                "            test_recordings=_fixed,",
            ),
        ],
    ),
    (
        "B25",
        "the cell derived at a FIXED resolution 4 inside the loop, while "
        "`assign_cells` still uses the loop's resolution. Separate mutant "
        "from B24, which hoists the call: here the call IS inside the loop "
        "and is re-evaluated every pass, so a test that only checks WHERE the "
        "derivation happens cannot see it. What is wrong is the ARGUMENT",
        "test_the_split_cell_is_derived_at_each_resolution",
        [
            (
                "            cell=h3.latlng_to_cell(row[\"lat\"], row[\"lon\"], resolution),",
                "            cell=h3.latlng_to_cell(row[\"lat\"], row[\"lon\"], 4),",
            )
        ],
    ),
    (
        "B26",
        "a try/except wrapped around the resolution loop body so a guard "
        "ValueError is CAUGHT and the sweep carries on. This is obligation "
        "2's defect in the exact form it forbids: a caught H3 mismatch means "
        "the artifact for that resolution is written from a table whose "
        "leakage guard did nothing, under a filename indistinguishable from "
        "every correct one -- worse than the pre-fix silence because it looks "
        "deliberate",
        "test_the_pipeline_call_path_has_no_except_at_all",
        [
            (
                "        paths = build_from_archive(\n",
                "        try:\n            paths = build_from_archive(\n",
            ),
            (
                "            species_chunk=args.species_chunk,\n        )\n",
                "                species_chunk=args.species_chunk,\n"
                "            )\n"
                "        except ValueError as exc:\n"
                "            log.warning"
                '("resolution %s failed: %s", resolution, exc)\n'
                "            continue\n",
            ),
        ],
    ),
    (
        "B27",
        "a try/except inside `build_from_archive` that swallows a guard error "
        "and returns an empty path list. Separate mutant from B26, which "
        "wraps the CALL: a check that `main`'s loop is unwrapped cannot see a "
        "handler one frame deeper, and this form is worse -- the caller is "
        "told the build succeeded and simply wrote nothing",
        "test_no_except_handler_in_the_build_script_swallows_anything",
        [
            (
                "        table = assign_cells(read_occurrences(Path(zip_path)), resolution)\n"
                "        table = apply_leakage_guards(table, test_recordings)",
                "        try:\n"
                "            table = assign_cells(\n"
                "                read_occurrences(Path(zip_path)), resolution\n"
                "            )\n"
                "            table = apply_leakage_guards(table, test_recordings)\n"
                "        except ValueError:\n"
                "            return []",
            )
        ],
    ),
    # ---- the zero-match guard, one mutant per clause ---------------------
    (
        "B30",
        "the zero-match guard removed entirely, so a split that matches NO "
        "occurrence produces a complete, well-formed set of artifacts whose "
        "leakage guard removed nothing. One WARNING is logged and the build "
        "reports success -- the shared symptom of four silent failure modes, "
        "demoted back to a line in a log",
        "test_a_split_that_matches_nothing_fails_the_build",
        [
            (
                "    if detector.fired and test_recordings:",
                "    if False:",
            )
        ],
    ),
    (
        "B31",
        "the `and test_recordings` CONJUNCT removed. The guard still fires on "
        "a real zero-match, so B30's fixture dies to this mutant too -- what "
        "breaks is the legitimate `--no-test-split` mode, which would now "
        "fail whenever anything else in the leakage module emitted a matching "
        "warning. Its own mutant because a one-sided fixture cannot tell a "
        "guard that is too weak from one that is too strong, and the pressure "
        "from a guard that fires on a legal build is to delete the guard",
        "test_a_no_split_build_does_not_fail_on_the_zero_match_check",
        [
            (
                "    if detector.fired and test_recordings:",
                "    if detector.fired or not test_recordings:",
            )
        ],
    ),
    (
        "B32",
        "the detector's marker replaced by a HARD-CODED copy of the log "
        "message's wording instead of the constant `leakage` exports. It "
        "matches the same text TODAY, so every test passes -- and the day the "
        "message is reworded the detector goes silently deaf and the "
        "zero-match guard stops firing with nothing to notice. Killed by a "
        "deliberately MISMATCHED spelling, which is why the edit changes the "
        "wording rather than merely inlining it",
        "test_a_split_that_matches_nothing_fails_the_build",
        [
            (
                "        if ZERO_MATCH_WARNING_MARKER in message:",
                '        if "matched exactly zero of" in message:',
            )
        ],
    ),
    (
        "B33",
        "the detector attached but never REMOVED, and attached to the ROOT "
        "logger rather than the leakage one, so every later build in the same "
        "process inherits the previous build's fired list and the second "
        "resolution of a sweep fails on the first's warning. A cross-artifact "
        "contamination that only appears when more than one build runs in one "
        "process -- which is exactly what a `--resolutions 3 4 5` sweep does",
        "test_a_split_that_DOES_match_removes_rows_and_builds",
        [
            (
                "        logging.getLogger(\"birdology.prior.leakage\").removeHandler(detector)",
                "        pass",
            ),
            (
                "    detector = _ZeroMatchDetector()\n"
                '    logging.getLogger("birdology.prior.leakage").addHandler(detector)',
                "    global _DETECTOR\n"
                "    try:\n"
                "        detector = _DETECTOR\n"
                "    except NameError:\n"
                "        detector = _DETECTOR = _ZeroMatchDetector()\n"
                '        logging.getLogger("birdology.prior.leakage").addHandler(detector)',
            ),
        ],
    ),
    # ---- load_test_split: one mutant per check ---------------------------
    (
        "B34",
        "the EMPTY-split check removed, so a header-only CSV is accepted and "
        "`drop_test_contributor_cells` returns early and silently on its "
        "empty banned set. The operator passed `--test-split`, believes the "
        "prior is guarded, and it is not -- obligation 3's exact failure, and "
        "the one whose consequence is a paper's numbers computed against an "
        "unguarded prior",
        "test_an_empty_split_file_is_rejected_at_load",
        [("    if not rows:", "    if False:")],
    ),
    (
        "B35",
        "the MISSING-COLUMN check removed, so a split lacking `recorded_by` "
        "dies on a raw KeyError naming a dict key rather than a ValueError "
        "naming the contract and the file. Separate mutant from B34: a "
        "well-formed file with a missing column is not empty, and an empty "
        "file has every column",
        "test_a_split_missing_a_column_is_rejected",
        [("        if missing:", "        if False:")],
    ),
    (
        "B36",
        "the week coerced with `int(float(...))` instead of `int(...)`, so a "
        "fractional week in the split -- a day-of-year or a units bug -- is "
        "TRUNCATED to a neighbouring week instead of being rejected. The "
        "guard then bans the WRONG cell-week: real occurrences are deleted "
        "and the leaking ones survive, both silently",
        "test_a_FRACTIONAL_week_is_rejected_rather_than_truncated",
        [
            (
                '                week = int(str(row["week"]).strip())',
                '                week = int(float(str(row["week"]).strip()))',
            )
        ],
    ),
    (
        "B37",
        "a `cell` column ADMITTED into the split format, and used when "
        "present. That single line reintroduces the whole T10-R3 defect: a "
        "supplied cell is fixed at one resolution and the sweep raises on "
        "every other one. The format not admitting a cell is what makes the "
        "sweep safe by construction, rather than by every caller remembering",
        "test_the_split_format_carries_no_cell_column",
        [
            (
                'TEST_SPLIT_COLUMNS = ("lat", "lon", "week", "recorded_by")',
                'TEST_SPLIT_COLUMNS = ("lat", "lon", "week", "recorded_by", "cell")',
            )
        ],
    ),
    # ---- the guarded table is what reaches the effort stage --------------
    (
        "B40",
        "the leakage guards SKIPPED entirely, so the raw table reaches "
        "`aggregate_counts` and the unguarded CountTable reaches "
        "`target_group_ratios`. Since the guard marker is now required there, "
        "this fails loudly instead of producing the smooth, plausible, "
        "mildly-flat prior an unguarded denominator used to yield -- which is "
        "the entire point of T10's acceptance criterion",
        "test_the_table_reaching_the_effort_stage_is_the_guarded_one",
        [
            (
                "        table = apply_leakage_guards(table, test_recordings)",
                "        table = table",
            )
        ],
    ),
    (
        "B41",
        "the guards run but their RESULT discarded: the pre-guard table is "
        "what continues. The marker is therefore absent and the effort stage "
        "refuses. Separate mutant from B40, which does not call the guards at "
        "all: a test that merely asserts the function was CALLED passes on "
        "this one, and a Xeno-Canto record would sail straight into the prior",
        "test_a_xeno_canto_record_never_reaches_the_prior",
        [
            (
                "        table = apply_leakage_guards(table, test_recordings)",
                "        apply_leakage_guards(table, test_recordings)",
            )
        ],
    ),
    (
        "B42",
        "the species axis taken from the RATIO table's distinct keys via a "
        "set, UNSORTED. `to_tensor` sorts the axis itself, so no value moves "
        "-- but the species list handed to the backoff is then in an "
        "arbitrary, hash-seeded order, and with `--species-chunk` the CHUNK "
        "BOUNDARIES become nondeterministic run to run. Bit-exactness across "
        "chunk sizes is what makes chunking legitimate, so a mutant that "
        "makes chunking order-dependent must die",
        "test_chunk_boundaries_are_deterministic_across_runs",
        [
            (
                '    return sorted({int(k) for k in counts.column("species_key").to_pylist()})',
                '    return list({int(k) for k in counts.column("species_key").to_pylist()})[::-1]',
            )
        ],
    ),
    (
        "B43",
        "the CELL axis unsorted, by the same edit on `cell_axis`. Its own "
        "mutant because the two axes are derived by two functions and a fix "
        "to one does not touch the other: a build with a sorted species axis "
        "and a set-ordered cell axis is still not reproducible across "
        "processes, and the cell axis is the one that grows to thousands of "
        "entries at national scale",
        "test_the_cell_axis_is_sorted_too",
        [
            (
                '    return sorted({str(c) for c in counts.column("cell").to_pylist()})',
                '    return list({str(c) for c in counts.column("cell").to_pylist()})[::-1]',
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


def _require_clean(paths: set[Path]) -> None:
    tracked = {str(p) for p in paths}
    dirty = [
        line
        for line in _git("status", "--porcelain").splitlines()
        if line[3:].strip() in tracked
    ]
    if dirty:
        sys.exit(f"refusing to run: {dirty!r} is dirty; commit or restore it")


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


# A mutant that BREAKS the target module rather than changing its behaviour is
# not a mutant at all. `AttributeError`, `NameError` and `ImportError` mean the
# edit referenced something that does not exist, and
# `SyntaxError`/`IndentationError` mean it does not parse. Such an edit takes
# out most of the suite, which TRIVIALLY includes whatever test the row names
# as its killer -- so the name check in `main` passes and the row is credited
# with proving a contract it never exercised.
_CRASH_TYPES = (
    "AttributeError",
    "NameError",
    "ImportError",
    "ModuleNotFoundError",
    "SyntaxError",
    "IndentationError",
)


def _crash_in_target(out: str, target: Path) -> str | None:
    """The crash type, if the output shows one raised FROM THE TARGET MODULE.

    Scoped to the mutated file on purpose. A mutant is entitled to make a TEST
    raise `AttributeError` -- for instance one that removes a function a test
    calls, where that IS the behaviour under test. What is never legitimate is
    the mutated module failing to load or to resolve its own names, and that is
    what the traceback line `scripts/build_prior.py:NNN: SomeError` reports.
    """
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

    # Every file any mutant touches, read ONCE up front. The restore check
    # compares against these bytes, so a mutant can never be left behind.
    targets = {m[4] if len(m) > 4 else TARGET for m in MUTANTS}
    sources = {path: (REPO / path).read_text() for path in targets}
    test_source = (REPO / TESTS).read_text()
    selected = [m for m in MUTANTS if args.only is None or m[0] in args.only]

    if args.verify_labels:
        bad = []
        for mutant in selected:
            mid, description, killer, edits = mutant[:4]
            target = mutant[4] if len(mutant) > 4 else TARGET
            source = sources[target]
            mutated = _apply(source, edits)
            print(
                f"\n{'=' * 72}\n{mid} [{target}]: {description}\n"
                f"  killer: {killer}\n{'=' * 72}"
            )
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

    _require_clean(targets)

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
    crashed: list[str] = []

    for mutant in selected:
        mid, description, killer, edits = mutant[:4]
        target = mutant[4] if len(mutant) > 4 else TARGET
        source = sources[target]

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

        (REPO / target).write_text(mutated)
        _clear_pycache()
        try:
            failures, out, failed_names = _run_tests()
        finally:
            _git("checkout", "--", str(target))
            _clear_pycache()
            assert (REPO / target).read_text() == source, "restore failed"

        crash = _crash_in_target(out, target)
        if crash is not None:
            # NOT a kill, whatever the failure count says. The edit broke the
            # module instead of changing its behaviour, so it exercises no
            # contract -- and because it breaks nearly every test, the named
            # killer is among the failures by accident. See the module
            # docstring, item 6.
            verdict = f"CRASHED ({crash}, {failures} failed)"
            crashed.append(mid)
        elif not failures:
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

    _require_clean(targets)
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
    # NOT-APPLIED, MISLABELLED and CRASHED are FAILURES, not neutral rows. A
    # pattern that has gone stale after a refactor tests nothing; a mutant
    # killed only by an unrelated test credits coverage that does not exist;
    # and a mutant that crashes the module exercises no contract at all while
    # trivially containing every named killer.
    print(f"NOT APPLIED: {not_applied or 'none'}")
    print(f"MISLABELLED: {mislabelled or 'none'}")
    print(f"CRASHED: {crashed or 'none'}")
    return 1 if survivors or not_applied or mislabelled or crashed or final else 0


if __name__ == "__main__":
    raise SystemExit(main())
