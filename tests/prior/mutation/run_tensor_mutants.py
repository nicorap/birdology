#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/tensor.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_tensor_mutants.py
    uv run python tests/prior/mutation/run_tensor_mutants.py --only T1 T7
    uv run python tests/prior/mutation/run_tensor_mutants.py --verify-labels

Structure is copied from `run_smoothing_mutants.py`, including the five things
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
   kill or a survival;
4. every mutant NAMES the test that kills it, and that name is CHECKED at
   runtime: a mutant that fails only OTHER tests is scored MISLABELLED, not a
   kill, and also exits non-zero;
5. `--verify-labels` prints the unified diff each mutant actually applies, so
   each LABEL can be read against its EDIT rather than trusted.

A mutant is KILLED if at least one test fails AND the named killer is among
the failures. Zero failures is a SURVIVOR: the suite does not pin that
behaviour.

COMPOUND CONDITIONS ARE MUTATED PER DISJUNCT
--------------------------------------------
This module's guards are written as SEPARATE STATEMENTS rather than compound
conditions, precisely so that one fixture cannot mask the loss of another
clause, and each statement gets its own mutant:

* `week < 1` / `week > WEEKS_PER_YEAR`          -> T1, T2 (per bound)
* `isfinite` / `<= 0.0` / `> 1.0` on `p`        -> T8, T9, T10 (per clause)
* the schema loop's missing / dtype / null arms -> T14, T15, T16
* the two axis-vs-matrix shape checks           -> T21, T22

WHAT THIS SET DOES NOT COVER -- stated so the table is not read as more than
it is
---------------------------------------------------------------------------
* THE UPSTREAM SEAM. This harness mutates only `tensor.py`, and
  `test_tensor.py` builds its own Arrow tables rather than running `smooth()`.
  That the smoother really emits `(species_key, cell, week, p)` with those
  exact dtypes is pinned by `test_smoothing.py`, not here; a contract drift in
  its OUTPUT would be caught there and would show up in this table only as a
  schema rejection, which reads identically to a caller error.
* THE DENSE-GRID OBLIGATION (T8-R5). `test_a_context_absent_from_the_table_is
  _a_structural_zero_not_the_floor` pins that an absent context reads 0.0, and
  the module docstring states the caller must therefore supply a dense grid.
  NOTHING HERE ENFORCES THAT the caller did: `to_tensor` accepts a table with
  holes without complaint, by design (it cannot know which grid was intended).
  A pipeline that hands it a sparse grid produces a tensor full of structural
  zeros and every test in this file still passes. That obligation lives in
  Task 10 (T10-R1) and is only as strong as Task 10's own tests.
* WHETHER `p` IS THE RIGHT NUMBER. Every mutant here concerns LAYOUT and
  VALIDATION. The calibration of the prior -- whether the backoff weights and
  the smoothing bandwidth produce a useful probability -- is empirical and no
  mutation of a serialiser can speak to it.
* SCALE. The largest fixture is a 104-row dense grid over two cells. Nothing
  here would catch a defect that appears only at national scale: an int64
  overflow in the packed duplicate key (bounded by n_species * n_cells * 52,
  which overflows only far beyond any plausible grid), memory blow-up from
  `todense()` on a very wide `vector()` call, or `to_pylist()` creeping back
  into the hot path. The smoothing harness has a peak-allocation test (S31);
  this one does not, because this stage's cost is dominated by scipy's own
  COO->CSR conversion rather than by Python objects.
* THE `.npz` CONTAINER ITSELF. `save_prior`/`load_prior` are tested as a pair,
  so a mutant that changed BOTH the write and the read consistently (a
  different scipy format, a different key name) would round-trip and survive.
  Nothing here reads the `.npz` with an independent reader, so the on-disk
  format is pinned only up to "whatever this build of scipy writes and reads".
  An artifact released today and loaded by a future scipy is not covered.
* THE SIDECAR AS A CITATION. T24/T25 prove the licence and DOI reach the JSON
  and come back. That the DOI is the RIGHT one -- that it names the GBIF
  download this tensor was actually built from -- is a pipeline-level property
  no test in this file can check: `gbif_doi` is a free string the caller sets.
* JSON ENCODING EDGE CASES. The sidecar is written with the default encoder.
  A cell string containing an unusual codepoint round-trips through JSON, but
  nothing here exercises non-ASCII cells, because H3 cells are hexadecimal.
  A future non-H3 `cell` spelling would be untested.
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
TARGET = Path("src/birdology/prior/tensor.py")
TESTS = "tests/prior/test_tensor.py"

# (id, description, killer_test, [(old, new), ...])
#
# `killer_test` is the test whose failure DEMONSTRATES the mutant is caught.
# It is verified at runtime: a mutant that fails only other tests is reported
# MISLABELLED rather than counted as a kill.
MUTANTS: list[tuple[str, str, str, list[tuple[str, str]]]] = [
    # ---- T8-R1: the column packing aliases outside 1..52 ------------------
    (
        "T1",
        "the LOWER week bound removed, so week 0 packs to column "
        "`cell * 52 - 1` -- the LAST week of the PREVIOUS cell -- and a "
        "lookup returns a real probability belonging to a different cell",
        "test_lookup_rejects_week_zero_which_would_alias_the_previous_cell",
        [("    if week < 1:", "    if False:")],
    ),
    (
        "T2",
        "the UPPER week bound removed, so week 53 packs to `cell * 52 + 52`, "
        "which is EXACTLY week 1 of the next cell. Mutated separately from T1 "
        "because a fixture tripping one bound never exercises the other",
        "test_lookup_rejects_a_week_above_52_which_would_alias_the_next_cell",
        [("    if week > WEEKS_PER_YEAR:", "    if False:")],
    ),
    (
        "T3",
        "the upper bound made EXCLUSIVE, rejecting week 52 -- a real ISO week "
        "that upstream folds week 53 into. Every December estimate would be "
        "unreachable",
        "test_week_52_is_accepted_and_is_the_last_column_of_its_cell",
        [
            (
                "    if week > WEEKS_PER_YEAR:",
                "    if week >= WEEKS_PER_YEAR:",
            )
        ],
    ),
    (
        "T4",
        "the week guard moved BELOW the cell lookup in `_column`, so an "
        "out-of-range week on an unknown cell returns 0.0 through the `None` "
        "path instead of raising -- the caller cannot tell an aliasing bug "
        "from a legitimate miss",
        "test_an_out_of_range_week_raises_even_for_an_unknown_cell",
        [
            (
                '        _validate_week(int(week), "a tensor lookup")\n'
                "        ci = self._cell_index.get(cell)\n"
                "        if ci is None:\n"
                "            return None",
                "        ci = self._cell_index.get(cell)\n"
                "        if ci is None:\n"
                "            return None\n"
                '        _validate_week(int(week), "a tensor lookup")',
            )
        ],
    ),
    (
        "T5",
        "the week guard dropped from `to_tensor`'s input path, so a week-53 "
        "ROW is packed into the next cell's week-1 column at BUILD time -- "
        "the stored tensor is wrong before any lookup happens",
        "test_to_tensor_rejects_a_week_above_52_in_the_table",
        [
            (
                '        _validate_week(int(weeks.min()), "the prior table")\n'
                '        _validate_week(int(weeks.max()), "the prior table")',
                "        pass",
            )
        ],
    ),
    (
        "T6",
        "the `- 1` dropped from the column packing, so every week is stored "
        "one column to the right and week 52 lands in the next cell's block "
        "(or past the end of a single-cell matrix)",
        "test_week_52_in_a_single_cell_tensor_occupies_the_last_column",
        [
            (
                "    return cell_index * WEEKS_PER_YEAR + (week - 1)",
                "    return cell_index * WEEKS_PER_YEAR + week",
            )
        ],
    ),
    (
        "T7",
        "the column STRIDE changed from WEEKS_PER_YEAR to a hard-coded 53 "
        "(the ISO-53 spelling this plan folds away), so cell blocks no longer "
        "tile the matrix and the shape and the packing disagree",
        "test_a_dense_two_cell_grid_fills_every_column_exactly_once",
        [
            (
                "    return cell_index * WEEKS_PER_YEAR + (week - 1)",
                "    return cell_index * 53 + (week - 1)",
            )
        ],
    ),
    # ---- T8-R4: `p` validated here, one mutant per clause -----------------
    (
        "T8",
        "the FINITENESS clause of the `p` guard removed, so a nan is stored "
        "as an explicit sparse entry and read back as a probability. Separate "
        "from T9: a nan passes every comparison, so a positivity check alone "
        "does not catch it",
        "test_a_nan_p_is_rejected",
        [
            (
                "    if not np.all(np.isfinite(values)):",
                "    if False:",
            )
        ],
    ),
    (
        "T9",
        "the NON-POSITIVITY clause removed, so a `p` of exactly 0.0 -- an "
        "upstream contract breach, since the backoff clamps into "
        "[1e-06, 1.0] -- is stored as a structural zero indistinguishable "
        "from a context missing from the tensor entirely",
        "test_a_zero_p_is_rejected_not_silently_skipped",
        [
            (
                "    if np.any(values <= 0.0):",
                "    if False:",
            )
        ],
    ),
    (
        "T10",
        "the UPPER clause removed, so a `p` above 1.0 is stored as a "
        "probability. Separate mutant from T9: a non-positivity check cannot "
        "see a value above one",
        "test_a_p_above_one_is_rejected",
        [
            (
                "    if np.any(values > 1.0):",
                "    if False:",
            )
        ],
    ),
    (
        "T11",
        "the non-positivity clause weakened to strictly-negative, so exactly "
        "0.0 slips through -- the precise value T8-R4 exists to catch",
        "test_a_zero_p_is_rejected_not_silently_skipped",
        [
            (
                "    if np.any(values <= 0.0):",
                "    if np.any(values < 0.0):",
            )
        ],
    ),
    (
        "T12",
        "the upper clause made EXCLUSIVE, rejecting a `p` of exactly 1.0 -- a "
        "legal value: the backoff clamps into [floor, 1.0] INCLUSIVE, so a "
        "certain species in a certain cell-week would be refused",
        "test_p_of_exactly_one_is_accepted",
        [
            (
                "    if np.any(values > 1.0):",
                "    if np.any(values >= 1.0):",
            )
        ],
    ),
    (
        "T13",
        "the brief's original `if r['p'] == 0.0: continue` restored in spirit: "
        "non-positive rows are DROPPED instead of raising. Nothing errors, "
        "the tensor is one entry smaller, and the dropped context becomes a "
        "structural zero -- the silent failure T8-R4 forbids",
        "test_a_zero_p_is_rejected_not_silently_skipped",
        [
            (
                "    _validate_p(p)\n",
                "    _keep = np.isfinite(p) & (p > 0.0) & (p <= 1.0)\n"
                "    species, weeks, p = species[_keep], weeks[_keep], p[_keep]\n"
                "    table = table.filter(pa.array(_keep))\n",
            )
        ],
    ),
    # ---- the schema contract, one mutant per arm of the loop --------------
    (
        "T14",
        "the MISSING-COLUMN arm removed, so a table without `p` dies on a raw "
        "KeyError deep in the packing instead of a ValueError naming the "
        "contract",
        "test_a_table_missing_the_p_column_is_rejected",
        [
            (
                "        if name not in table.column_names:",
                "        if False:",
            )
        ],
    ),
    (
        "T15",
        "the DTYPE arm CASTS instead of raising -- a second derivation of a "
        "value derived exactly once upstream, accepting an int64 `week` from "
        "a bypassed pipeline",
        "test_a_wrong_input_dtype_is_rejected_not_cast",
        [
            (
                "        if actual != expected:",
                "        if False and actual != expected:",
            )
        ],
    ),
    (
        "T16",
        "the NULL arm removed. Separate mutant from T15 because a dtype check "
        "passes on a float64 column holding nulls: a null `p` becomes a nan "
        "in the packed array and a null `cell` becomes its own axis entry, "
        "neither with an error",
        "test_a_null_in_p_is_rejected",
        [
            (
                "        if table.column(name).null_count:",
                "        if False:",
            )
        ],
    ),
    (
        "T17",
        "`_validate_table` never called at all, so none of the three arms "
        "runs",
        "test_a_table_missing_the_p_column_is_rejected",
        [
            (
                "    _validate_table(table)",
                "    pass  # _validate_table(table)",
            )
        ],
    ),
    # ---- T8-R2: scipy SUMS duplicate coordinates silently ----------------
    (
        "T18",
        "the duplicate guard removed, so two rows for one "
        "(species, cell, week) are SUMMED by scipy: measured, 0.4 + 0.4 "
        "becomes 0.8, a plausible probability with no error and no nan",
        "test_to_tensor_rejects_a_duplicate_species_cell_week_row",
        [
            (
                "    _reject_duplicates(row_idx, col_idx, n_cols, species_keys, cells, weeks)",
                "    pass  # _reject_duplicates(...)",
            )
        ],
    ),
    (
        "T19",
        "the duplicate key packed WITHOUT its row (species) digit, so two "
        "different species in the same cell-week are reported as duplicates "
        "-- a false positive that rejects every legitimate multi-species grid",
        "test_rows_differing_only_in_species_are_not_duplicates",
        [
            (
                "    packed = row_idx.astype(np.int64) * n_cols + col_idx.astype(np.int64)",
                "    packed = col_idx.astype(np.int64)",
            )
        ],
    ),
    (
        "T20",
        "the duplicate key packed WITHOUT its column digit, so two rows for "
        "the same species in different cells or weeks collide -- the mirror "
        "false positive of T19",
        "test_rows_differing_only_in_week_are_not_duplicates",
        [
            (
                "    packed = row_idx.astype(np.int64) * n_cols + col_idx.astype(np.int64)",
                "    packed = row_idx.astype(np.int64) * n_cols",
            )
        ],
    ),
    # ---- the axes and the matrix must agree ------------------------------
    (
        "T21",
        "the ROW-COUNT agreement check removed, so a sidecar listing more "
        "species than the matrix has rows leaves every species past the end "
        "silently unreachable",
        "test_a_matrix_with_the_wrong_row_count_is_rejected",
        [
            (
                "        if n_rows != len(self.species_keys):",
                "        if False:",
            )
        ],
    ),
    (
        "T22",
        "the COLUMN-COUNT agreement check removed. Separate from T21: a "
        "matrix can have the right number of rows and the wrong width, and "
        "then every cell's block starts at the wrong offset",
        "test_a_matrix_with_the_wrong_column_count_is_rejected",
        [
            (
                "        if n_cols != len(self.cells) * WEEKS_PER_YEAR:",
                "        if False:",
            )
        ],
    ),
    (
        "T23",
        "the duplicate-cell-axis check removed, so a sidecar with a repeated "
        "cell silently shadows the first copy and every lookup in that cell "
        "reads ANOTHER cell's block of columns",
        "test_a_duplicate_cell_axis_is_rejected",
        [
            (
                "        if len(self._cell_index) != len(self.cells):",
                "        if False:",
            )
        ],
    ),
    (
        "T24",
        "the duplicate-species-axis check removed -- the same defect on the "
        "row axis, and a separate mutant because one deletion cannot detect "
        "the other's loss",
        "test_a_duplicate_species_key_axis_is_rejected",
        [
            (
                "        if len(self._species_index) != len(self.species_keys):",
                "        if False:",
            )
        ],
    ),
    # ---- the axes are sorted, and values follow their own keys -----------
    (
        "T25",
        "the species axis left in FIRST-SEEN order while `row_idx` is still "
        "computed by `searchsorted`, which requires a sorted axis. Every "
        "value is then filed under whichever species the binary search "
        "landed on",
        "test_each_value_lands_under_its_own_key_when_the_input_is_unsorted",
        [
            (
                "    species_keys = np.unique(species.astype(np.int64))",
                "    _seen = species.astype(np.int64).tolist()\n"
                "    species_keys = np.array(\n"
                "        sorted(set(_seen), key=_seen.index), dtype=np.int64\n"
                "    )",
            )
        ],
    ),
    (
        "T26",
        "the cell axis left in DICTIONARY order (Arrow's first-appearance "
        "order) while `cells` is still reported sorted, so an unsorted input "
        "files each value under the wrong cell",
        "test_each_value_lands_under_its_own_key_when_the_input_is_unsorted",
        [
            (
                "    cells = sorted(set(dictionary))",
                "    cells = list(dict.fromkeys(dictionary))",
            )
        ],
    ),
    (
        "T27",
        "the dictionary indices used DIRECTLY as cell indices without "
        "remapping onto the sorted axis, so the values are packed against "
        "Arrow's ordering and read back against the sorted one",
        "test_each_value_lands_under_its_own_key_when_the_input_is_unsorted",
        [
            (
                "    cell_idx = remap[encoded.indices.to_numpy(zero_copy_only=False).astype(np.int64)]",
                "    cell_idx = encoded.indices.to_numpy(zero_copy_only=False).astype(np.int64)",
            )
        ],
    ),
    (
        "T28",
        "`cell` LOWERCASED before it is encoded: a redundant second "
        "normalisation of a value normalised exactly once upstream, which "
        "merges two distinct cells and changes the bytes the release carries",
        "test_a_cell_string_is_passed_through_byte_for_byte",
        [
            (
                '    encoded = _single_array(table.column("cell")).dictionary_encode()',
                "    encoded = pa.compute.utf8_lower(\n"
                '        _single_array(table.column("cell"))\n'
                "    ).dictionary_encode()",
            )
        ],
    ),
    (
        "T29",
        "the chunked input read from its FIRST CHUNK only, so a multi-chunk "
        "table silently serialises a prefix of itself",
        "test_a_chunked_input_packs_identically_to_a_contiguous_one",
        [
            (
                "        return column.combine_chunks()",
                "        return column.chunk(0) if column.num_chunks else column.combine_chunks()",
            )
        ],
    ),
    # ---- vector(): order, width, no renormalisation ----------------------
    (
        "T30",
        "`vector` RENORMALISED over the requested species so each context "
        "sums to 1.0. Looks tidier and is wrong: `p` is a per-species "
        "marginal, never renormalised at any stage, so a label set omitting "
        "most species would inflate the survivors to fill the missing mass",
        "test_vector_does_NOT_renormalise_over_species",
        [
            (
                "        return np.asarray(self.values[:, [col]].todense(), dtype=np.float64).ravel()",
                "        _v = np.asarray(\n"
                "            self.values[:, [col]].todense(), dtype=np.float64\n"
                "        ).ravel()\n"
                "        _t = _v.sum()\n"
                "        return _v / _t if _t else _v",
            )
        ],
    ),
    (
        "T31",
        "`vector` for an UNKNOWN cell returns a zero-length array instead of "
        "one zero per species, so a downstream fusion that multiplies by the "
        "prior silently broadcasts to the wrong shape",
        "test_vector_for_an_unknown_cell_is_all_zeros_of_the_right_length",
        [
            (
                "            return np.zeros(len(self.species_keys), dtype=np.float64)",
                "            return np.zeros(0, dtype=np.float64)",
            )
        ],
    ),
    (
        "T32",
        "`vector` reads the column NEXT to the one asked for, so every "
        "context is answered with the following week's prior (and the last "
        "week of a cell with the next cell's first week)",
        "test_vector_reads_only_its_own_context",
        [
            (
                "        return np.asarray(self.values[:, [col]].todense(), dtype=np.float64).ravel()",
                "        return np.asarray(\n"
                "            self.values[:, [min(col + 1, self.values.get_shape()[1] - 1)]].todense(),\n"
                "            dtype=np.float64,\n"
                "        ).ravel()",
            )
        ],
    ),
    (
        "T33",
        "`vector` emitted as float32, halving the precision of a prior whose "
        "floor is 1e-06 and whose values span several orders of magnitude",
        "test_vector_is_float64",
        [
            (
                "        return np.asarray(self.values[:, [col]].todense(), dtype=np.float64).ravel()",
                "        return np.asarray(\n"
                "            self.values[:, [col]].todense(), dtype=np.float32\n"
                "        ).ravel()",
            )
        ],
    ),
    # ---- T8-R3: scipy APPENDS `.npz` --------------------------------------
    (
        "T34",
        "the brief's original suffix handling restored: `save_npz` is called "
        "with the RAW path, so `p.dat` is written as `p.dat.npz` while the "
        "returned path and the sidecar name `p.npz`. The return value then "
        "points at a file that does not exist and `load_prior` fails",
        "test_save_returns_a_path_that_exists_for_a_non_npz_name",
        [
            (
                "    npz = _npz_path(path)\n"
                "    npz.parent.mkdir(parents=True, exist_ok=True)\n"
                "    sparse.save_npz(npz, tensor.values.tocoo())",
                "    path.parent.mkdir(parents=True, exist_ok=True)\n"
                "    sparse.save_npz(path, tensor.values.tocoo())\n"
                "    npz = _npz_path(path)",
            )
        ],
    ),
    (
        "T35",
        "`_npz_path` made an identity, so a non-`.npz` name is saved (with "
        "scipy's appended suffix) to one file and the sidecar written beside "
        "a different one",
        "test_the_sidecar_sits_beside_the_npz_that_was_written",
        [
            (
                '    return path if path.suffix == ".npz" else path.with_suffix(".npz")',
                "    return path",
            )
        ],
    ),
    (
        "T36",
        "`load_prior` reads the sidecar beside the RAW path rather than the "
        "normalised one, so a tensor saved as `p.dat` cannot be reloaded from "
        "the path `save_prior` returned",
        "test_load_reads_back_what_save_returned_for_a_non_npz_name",
        [
            (
                "    npz = _npz_path(path)\n"
                '    meta = json.loads(npz.with_suffix(".json").read_text(encoding="utf-8"))',
                "    npz = path\n"
                '    meta = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))',
            )
        ],
    ),
    (
        "T37",
        "the parent-directory creation dropped, so saving into a fresh "
        "output directory raises FileNotFoundError rather than writing the "
        "artifact",
        "test_save_creates_missing_parent_directories",
        [
            (
                "    npz.parent.mkdir(parents=True, exist_ok=True)",
                "    pass",
            )
        ],
    ),
    # ---- the citable sidecar ---------------------------------------------
    (
        "T38",
        "the licence omitted from the sidecar. The matrix is then legally "
        "unusable as a release: nothing in the artifact states the terms",
        "test_the_licence_constant_is_the_one_written",
        [
            (
                '        "licence": LICENCE,\n',
                "",
            )
        ],
    ),
    (
        "T39",
        "the licence changed to a NON-COMMERCIAL variant, which is not what "
        "this release is under and would silently mislabel the artifact",
        "test_sidecar_records_licence_and_doi",
        [
            (
                'LICENCE = "CC BY 4.0"',
                'LICENCE = "CC BY-NC 4.0"',
            )
        ],
    ),
    (
        "T40",
        "the GBIF DOI dropped from the sidecar, so the artifact cannot be "
        "traced to the download it came from -- the provenance a Datasets & "
        "Benchmarks submission is required to carry",
        "test_sidecar_records_licence_and_doi",
        [
            (
                '        "gbif_doi": tensor.gbif_doi,\n',
                '        "gbif_doi": "",\n',
            )
        ],
    ),
    (
        "T41",
        "the DOI dropped on the READ side, so it survives to disk but is lost "
        "whenever a released tensor is loaded and re-saved",
        "test_the_doi_survives_the_round_trip",
        [
            (
                '        gbif_doi=meta.get("gbif_doi", ""),',
                '        gbif_doi="",',
            )
        ],
    ),
    (
        "T42",
        "the week stride omitted from the sidecar, so a reader has no way to "
        "know what the column packing was and cannot validate it",
        "test_the_sidecar_records_the_week_stride",
        [
            (
                '        "weeks_per_year": WEEKS_PER_YEAR,\n',
                "",
            )
        ],
    ),
    (
        "T43",
        "the stride-agreement check on load removed, so an artifact written "
        "with a different WEEKS_PER_YEAR is read against this build's stride "
        "and every column index silently points at the wrong week",
        "test_a_sidecar_with_a_different_week_stride_is_rejected",
        [
            (
                '    if int(meta["weeks_per_year"]) != WEEKS_PER_YEAR:',
                "    if False:",
            )
        ],
    ),
    (
        "T44",
        "the cell axis dropped from the sidecar and rebuilt as synthetic "
        "names on load. The matrix still loads and every lookup by a real H3 "
        "cell silently misses",
        "test_round_trip_preserves_the_axes_exactly",
        [
            (
                '        "cells": list(tensor.cells),',
                '        "cells": [f"cell-{i}" for i in range(len(tensor.cells))],',
            )
        ],
    ),
    (
        "T45",
        "the species axis written but read back as float, so `lookup(1, ...)` "
        "misses every key: the index is built from numpy floats while the "
        "caller passes an int",
        "test_round_trip_preserves_the_axes_exactly",
        [
            (
                '        species_keys=np.asarray(meta["species_keys"], dtype=np.int64),\n'
                '        cells=list(meta["cells"]),',
                '        species_keys=np.asarray(meta["species_keys"], dtype=np.float64),\n'
                '        cells=list(meta["cells"]),',
            )
        ],
    ),
    (
        "T46",
        "the matrix written as float32, so the floor (1e-06) and every "
        "smoothed value lose precision on the way to disk with no error",
        "test_round_trip_preserves_full_float64_precision",
        [
            (
                "    sparse.save_npz(npz, tensor.values.tocoo())",
                "    sparse.save_npz(npz, tensor.values.astype(np.float32).tocoo())",
            )
        ],
    ),
    (
        "T47",
        "`load_prior` IMPUTES the upstream floor for every structural zero. "
        "This is the T8-R5 violation in its most tempting form -- it looks "
        "like it is restoring the floor the backoff guarantees -- but this "
        "stage was never handed those contexts, so it would be inventing a "
        "probability for a grid point that does not exist, and `nnz` would "
        "stop distinguishing a real estimate from an absent one",
        "test_round_trip_preserves_structural_zeros",
        [
            (
                "        values=sparse.load_npz(npz).tocsr(),",
                "        values=sparse.csr_matrix(\n"
                "            np.where(\n"
                "                np.asarray(sparse.load_npz(npz).todense()) > 0.0,\n"
                "                np.asarray(sparse.load_npz(npz).todense()),\n"
                "                1e-6,\n"
                "            )\n"
                "        ),",
            )
        ],
    ),
    (
        "T48",
        "the spec written but not read back: every loaded tensor claims the "
        "same hard-coded variant, so a sweep over variants would attribute "
        "every result to one of them",
        "test_round_trip_preserves_spec",
        [
            (
                '        spec=PriorSpec(**meta["spec"]),',
                '        spec=PriorSpec(resolution=4, source="dof", variant="uniform"),',
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

    source = (REPO / TARGET).read_text()
    test_source = (REPO / TESTS).read_text()
    selected = [m for m in MUTANTS if args.only is None or m[0] in args.only]

    if args.verify_labels:
        bad = []
        for mid, description, killer, edits in selected:
            mutated = _apply(source, edits)
            print(f"\n{'=' * 72}\n{mid}: {description}\n  killer: {killer}\n{'=' * 72}")
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

    _require_clean()

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

    for mid, description, killer, edits in selected:
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

        (REPO / TARGET).write_text(mutated)
        _clear_pycache()
        try:
            failures, out, failed_names = _run_tests()
        finally:
            _git("checkout", "--", str(TARGET))
            _clear_pycache()
            assert (REPO / TARGET).read_text() == source, "restore failed"

        if not failures:
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

    _require_clean()
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
    # NOT-APPLIED and MISLABELLED are FAILURES, not neutral rows. A pattern
    # that has gone stale after a refactor tests nothing, and a mutant killed
    # only by an unrelated test credits coverage that does not exist.
    print(f"NOT APPLIED: {not_applied or 'none'}")
    print(f"MISLABELLED: {mislabelled or 'none'}")
    return 1 if survivors or not_applied or mislabelled or final else 0


if __name__ == "__main__":
    raise SystemExit(main())
