#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/tensor.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_tensor_mutants.py
    uv run python tests/prior/mutation/run_tensor_mutants.py --only T1 T7
    uv run python tests/prior/mutation/run_tensor_mutants.py --verify-labels

Structure is copied from `run_smoothing_mutants.py`, including the things
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
   each LABEL can be read against its EDIT rather than trusted;
6. a mutant whose output shows an `AttributeError`, `NameError`, `ImportError`
   or `SyntaxError` raised FROM THE TARGET MODULE is scored CRASHED, never a
   kill, and CRASHED exits non-zero. This exists because of a real defect in
   this very table: T28 was written as `pa.compute.utf8_lower(...)` while
   `tensor.py` imports only `pyarrow as pa`, and `pa.compute` is not an
   attribute without an explicit import. The edit therefore lowercased
   nothing; it raised `AttributeError` inside `to_tensor` and took out 48 of
   63 tests. The name-based check in (4) only asks whether the named killer is
   among the failures, and an edit that breaks EVERYTHING trivially contains
   any named killer -- so the row scored KILLED while proving nothing at all
   about the contract it claimed (byte-for-byte cell pass-through, this
   module's most load-bearing property). A crash mutant is not a behavioural
   mutation: it is a broken edit wearing a kill's clothes.

A mutant is KILLED if at least one test fails, the named killer is among the
failures, and the failure is NOT a crash in the target module. Zero failures is
a SURVIVOR: the suite does not pin that behaviour.

COMPOUND CONDITIONS ARE MUTATED PER DISJUNCT
--------------------------------------------
This module's guards are written as SEPARATE STATEMENTS rather than compound
conditions, precisely so that one fixture cannot mask the loss of another
clause, and each statement gets its own mutant:

* `week < 1` / `week > WEEKS_PER_YEAR`          -> T1, T2 (per bound)
* the week guard's BOOL / NUMERIC-TYPE / INTEGRALITY clauses
                                                -> T64, T66, T67 (per clause),
  plus T65 for their ORDER and T68 for the truncate-then-validate form the
  clauses replace
* `isfinite` / `<= 0.0` / `> 1.0` on `p`        -> T8, T9, T10 (per clause)
* the schema loop's missing / dtype / null arms -> T14, T15, T16
* the two axis-vs-matrix shape checks           -> T21, T22
* a sidecar whose DECLARED axis length differs from the one the matrix
  encodes                                       -> T71, T72 (per axis)
* the unknown-cell and unknown-species guards   -> T49, T50 (per axis)
* `load_prior`'s MATRIX read and its SIDECAR read -> T36a, T36b (per read)

WHAT THIS SET DOES NOT COVER -- stated so the table is not read as more than
it is
---------------------------------------------------------------------------
* THE UPSTREAM SEAM. This harness mutates only `tensor.py`, and
  `test_tensor.py` builds its own Arrow tables rather than running `smooth()`.
  That the smoother really emits `(species_key, cell, week, p)` with those
  exact dtypes is pinned by `test_smoothing.py`, not here; a contract drift in
  its OUTPUT would be caught there and would show up in this table only as a
  schema rejection, which reads identically to a caller error.
* WHETHER THE FLOOR IS THE RIGHT VALUE FOR AN UNOBSERVED CONTEXT. T47/T51/T52
  pin that an unstored context on both axes reads `backoff.FLOOR` rather than
  0.0, and that `lookup`, `vector` and `dense` agree on it. That `FLOOR = 1e-6`
  is a good choice of prior for "nobody looked" is a modelling decision made in
  `backoff.py` and defended there; no mutation of a serialiser can speak to it.
  What this set DOES cover is that the choice is single-sourced: a mutant that
  hard-codes a different constant here dies.
* THE SIZE OF THE UNOBSERVED REGION. Nothing here checks that the fill is
  reasonable. `nnz` and `dense_capacity` are PUBLISHED (T53) so a reader can
  see a prior that observed 0.01% of its own grid, but no test rejects one:
  that judgement belongs to whoever reads the sidecar, and a legitimate
  national-scale prior really is that sparse.
* WHETHER `p` IS THE RIGHT NUMBER. Every mutant here concerns LAYOUT and
  VALIDATION. The calibration of the prior -- whether the backoff weights and
  the smoothing bandwidth produce a useful probability -- is empirical and no
  mutation of a serialiser can speak to it.
* SCALE. The largest fixture is a 104-row dense grid over two cells. Nothing
  here would catch a defect that appears only at national scale: an int64
  overflow in the packed duplicate key (bounded by n_species * n_cells * 52,
  which overflows only far beyond any plausible grid), memory blow-up from
  `todense()` on a very wide `vector()` call or from `dense()` on a full grid
  (which at national scale really is ~750 MB and is the inherent cost of
  asking for every context), or `to_pylist()` creeping back into the hot path.
  The smoothing harness has a peak-allocation test (S31); this one does not,
  because this stage's cost is dominated by scipy's own COO->CSR conversion
  rather than by Python objects.
* THE `.npz` CONTAINER BEYOND WHAT ONE INDEPENDENT READ PROVES.
  `test_the_npz_is_decodable_by_a_third_party_with_numpy_alone` decodes the
  artifact with `np.load` and no scipy, pinning the member names, the `coo`
  format tag, the float64 dtype, the ORIENTATION and the sidecar's packing --
  so a consistent-both-sides mutant (a different format, a renamed member, a
  transpose) now dies. What remains uncovered is FORWARD COMPATIBILITY: that a
  future scipy still writes these five members under these names is not
  something a test run today can establish.
* THE SIDECAR AS A CITATION. T38-T41 and T55 prove the licence, the DOI and
  the attribution string reach the JSON and come back, and T54 pins that
  `save_prior` REFUSES an empty DOI. (An earlier version of this list credited
  "T24/T25" with that coverage; those are the duplicate-species-axis and
  species-axis-ORDER mutants and have nothing to do with the citation. A label
  naming the wrong mutant is a claim of coverage that cannot be checked against
  the thing it names, which is the same defect class as T28's.) That the DOI is
  the RIGHT one -- that it names the GBIF
  download this tensor was actually built from -- is a pipeline-level property
  no test in this file can check: `gbif_doi` is a free string the caller sets,
  and the guard tests emptiness, not well-formedness.
* THE RELEASED DOCUMENTATION BEYOND ITS LOAD-BEARING FACTS.
  `docs/prior-artifact-format.md` is checked by
  `test_the_artifact_documentation_matches_a_freshly_saved_artifact`, which
  parses its sidecar example, compares the FIELD SET with a real sidecar,
  compares the published constants with `FLOOR`/`LICENCE`/`WEEKS_PER_YEAR`, and
  EXECUTES its numpy-only decoding recipe against a freshly saved artifact,
  requiring it to reproduce `dense()` and `lookup()` (including the floor for
  an unstored context). T73 is the rot mutant: a sidecar field the document
  does not mention. What is NOT covered is the document's PROSE -- an
  explanation can be misleading while every fact in it is true, and no test can
  see that.
* JSON ENCODING EDGE CASES. The sidecar is written with the default encoder.
  A cell string containing an unusual codepoint round-trips through JSON, but
  nothing here exercises non-ASCII cells, because H3 cells are hexadecimal.
  A future non-H3 `cell` spelling would be untested.
* CONCURRENT WRITES. Two processes saving to the same path interleave a
  matrix from one with a sidecar from the other. The nnz-agreement check on
  load would catch the common case, but nothing here exercises it and no lock
  is taken.
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
        "out-of-range week on an unknown cell raises the CELL error instead of "
        "the WEEK one -- the caller is pointed at a cell name that is not the "
        "defect while the aliasing bug in its week arithmetic survives",
        "test_an_out_of_range_week_raises_the_WEEK_error_even_for_an_unknown_cell",
        [
            (
                '        _validate_week(week, "a tensor lookup")\n'
                "        ci = self._cell_index.get(cell)",
                "        ci = self._cell_index.get(cell)",
            ),
            (
                "        return int(_column_index(ci, int(week)))",
                '        _validate_week(week, "a tensor lookup")\n'
                "        return int(_column_index(ci, int(week)))",
            ),
        ],
    ),
    (
        "T63",
        "the species lookup moved BEFORE the cell/week one in `lookup`, so an "
        "out-of-range week combined with an unknown species raises the SPECIES "
        "error instead of the WEEK one. Separate mutant from T4, which reorders "
        "the CELL guard: a fixture with an unknown cell never reaches the "
        "species lookup, so neither deletion can detect the other's",
        "test_an_out_of_range_week_raises_the_WEEK_error_even_for_an_unknown_species",
        [
            (
                "        col = self._column(cell, week)\n"
                "        row = self._row(species_key)",
                "        row = self._row(species_key)\n"
                "        col = self._column(cell, week)",
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
        "[1e-06, 1.0] -- is stored as an explicit zero, which `_floor_missing` "
        "reads exactly as it reads a context that was never stored: the real "
        "zero is silently reinterpreted as \"nobody looked\"",
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
        "`_validate_p` replaced by a `_keep` MASK that silently DROPS every "
        "row the guard would have rejected -- non-finite, non-positive AND "
        "above-one alike, since the mask is the conjunction of all three "
        "clauses. Nothing errors, the tensor is smaller than its input, and "
        "each dropped context now reads the FLOOR -- so an upstream stage that "
        "emitted a real zero has it silently converted into \"nobody looked\", "
        "the failure T8-R4 forbids",
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
        "the DTYPE arm DISABLED, so a column of the wrong type is ACCEPTED "
        "rather than rejected -- no cast is performed here, the check simply "
        "stops firing, and an int64 `week` from a bypassed pipeline is then "
        "packed as if it had been the int8 the contract requires",
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
        "order) rather than sorted. The values still land correctly, because "
        "the remap is derived from whatever `cells` says -- but the AXIS the "
        "sidecar publishes then depends on the row order of the input table, "
        "so two runs over the same data in a different order produce "
        "artifacts whose columns mean different things. Deliberately labelled "
        "with the ORDER test, not the placement test: an earlier draft of "
        "this row claimed a placement defect and was scored MISLABELLED",
        "test_cells_are_sorted_and_unique",
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
        "merges two distinct cells and changes the bytes the release carries. "
        "Written with `pc.utf8_lower` and its own `import pyarrow.compute as "
        "pc`, because the original spelling `pa.compute.utf8_lower` raised "
        "AttributeError (tensor.py imports only `pyarrow as pa`) and so "
        "lowercased NOTHING while crashing 48 of 63 tests -- scored KILLED "
        "while proving nothing about this module's most load-bearing contract",
        "test_a_cell_string_is_passed_through_byte_for_byte",
        [
            (
                '    encoded = _single_array(table.column("cell")).dictionary_encode()',
                "    import pyarrow.compute as pc\n"
                "    encoded = pc.utf8_lower(\n"
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
                "        return _floor_missing(stored)",
                "        _v = _floor_missing(stored)\n"
                "        _t = _v.sum()\n"
                "        return _v / _t if _t else _v",
            )
        ],
    ),
    (
        "T31",
        "the unknown-CELL guard in `_column` replaced by a silent FLOOR "
        "vector: `vector` answers a misspelled or wrong-resolution cell with a "
        "well-shaped, plausible prior instead of raising. This is the most "
        "dangerous form of the reversal taken too far -- the axes are derived "
        "from the data, so an unknown cell is a CALLER error, and flooring it "
        "substitutes a fabricated prior for the real one with nothing raised",
        "test_vector_for_an_unknown_cell_raises_rather_than_returning_floors",
        [
            (
                "        ci = self._cell_index.get(cell)\n"
                "        if ci is None:\n"
                "            raise ValueError(",
                "        ci = self._cell_index.get(cell)\n"
                "        if ci is None:\n"
                "            return 0\n"
                "        if False:\n"
                "            raise ValueError(",
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
                "        stored = np.asarray(self.values[:, [col]].todense(), dtype=np.float64).ravel()",
                "        stored = np.asarray(\n"
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
                "        return _floor_missing(stored)",
                "        return _floor_missing(stored).astype(np.float32)",
            )
        ],
    ),
    # ---- T8-R3: scipy APPENDS `.npz` --------------------------------------
    (
        "T34",
        "the brief's original suffix handling restored: `save_npz` is called "
        "with the RAW path while the sidecar and the returned path are "
        "computed elsewhere, so the artifact and its sidecar can name "
        "different files and the return value point at nothing",
        "test_the_sidecar_sits_beside_the_npz_that_was_written",
        [
            (
                "    npz = _npz_path(path)\n"
                "    npz.parent.mkdir(parents=True, exist_ok=True)\n"
                "    sparse.save_npz(npz, tensor.values.tocoo())",
                "    path.parent.mkdir(parents=True, exist_ok=True)\n"
                "    sparse.save_npz(path, tensor.values.tocoo())\n"
                "    npz = path.with_suffix(\".npz\")",
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
                '    return path if path.suffix == ".npz" else path.with_name(path.name + ".npz")',
                "    return path",
            )
        ],
    ),
    (
        "T49",
        "`_npz_path` reverted to the SUBSTITUTING `with_suffix` form, which "
        "TRUNCATES at the last dot: `prior_v1.2` and `prior_v1.3` both become "
        "`prior_v1.npz`, so saving the second OVERWRITES the first with no "
        "error and a caller asking for v1.2 is handed v1.3's values. Measured "
        "silent DATA LOSS at the release boundary -- a wrong artifact under a "
        "right name",
        "test_two_names_differing_only_after_a_dot_produce_two_distinct_artifacts",
        [
            (
                '    return path if path.suffix == ".npz" else path.with_name(path.name + ".npz")',
                '    return path if path.suffix == ".npz" else path.with_suffix(".npz")',
            )
        ],
    ),
    (
        "T36a",
        "`load_prior` reads the MATRIX from the RAW path rather than the "
        "normalised one, so a file that happens to sit at the un-suffixed name "
        "is decoded against THIS artifact's sidecar. Split from T36b because "
        "the single T36 that de-normalised BOTH reads took its kill entirely "
        "from this half while its label named the sidecar: under the old "
        "truncating `_npz_path` the two sidecar names COINCIDED, so the "
        "sidecar half was unkillable by any fixture and the row credited "
        "coverage that did not exist",
        "test_load_reads_the_MATRIX_from_the_normalised_path_not_the_raw_one",
        [
            (
                "    matrix = sparse.load_npz(npz).tocsr()",
                "    matrix = sparse.load_npz(path).tocsr()",
            )
        ],
    ),
    (
        "T36b",
        "`load_prior` reads the SIDECAR from the RAW path rather than the "
        "normalised one, so a correct matrix is decoded against FOREIGN axes: "
        "`p.dat` normalises to `p.dat.npz` whose sidecar is `p.dat.json`, "
        "while the raw path's sidecar is `p.json`. Killable only since the "
        "`_npz_path` fix separated those two names, and only by a fixture that "
        "plants a valid decoy sidecar at the raw location -- a test that merely "
        "reloads a non-`.npz` name dies to T36a too and so proves neither half",
        "test_load_reads_the_SIDECAR_from_the_normalised_path_not_the_raw_one",
        [
            (
                '    meta = json.loads(_sidecar_path(npz).read_text(encoding="utf-8"))',
                '    meta = json.loads(_sidecar_path(path).read_text(encoding="utf-8"))',
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
        "the HARD ZERO reintroduced on the read path: `load_prior` drops the "
        "flooring, so a context that is on both axes but was never stored "
        "reads 0.0 instead of `backoff.FLOOR`. This is the ruling this plan "
        "REVERSED, in the form it would come back: it looks like refusing to "
        "invent data, but the backoff's own docstring defines FLOOR as the "
        "value for \"nobody looked\", and a zero there lets the prior VETO a "
        "correct detection across most of the grid -- which is exactly what "
        "the floor exists to prevent. Nothing raises; every fusion score just "
        "gets quietly worse",
        "test_round_trip_preserves_the_sparsity_pattern_and_the_floor",
        [
            (
                "def _floor_missing(values):",
                "def _floor_missing(values):\n"
                "    return values\n"
                "\n"
                "\n"
                "def _floor_missing_unused(values):",
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
    # ---- the reversal: FLOOR for an unstored context, LOUD for an unknown axis
    (
        "T50",
        "the unknown-SPECIES guard removed and replaced by a silent floor, so "
        "`lookup` answers a typo'd or out-of-label-set species key with a "
        "plausible probability. Separate mutant from T31: one deletion cannot "
        "detect the other's loss, and a fixture with an unknown CELL never "
        "reaches the species lookup at all",
        "test_lookup_of_an_unknown_species_key_raises_naming_the_species_axis",
        [
            (
                "        row = self._species_index.get(int(species_key))\n"
                "        if row is None:\n"
                "            raise ValueError(",
                "        row = self._species_index.get(int(species_key))\n"
                "        if row is None:\n"
                "            return 0\n"
                "        if False:\n"
                "            raise ValueError(",
            )
        ],
    ),
    (
        "T51",
        "the floor HARD-CODED here instead of imported from `backoff`, at a "
        "value one order of magnitude away. The semantics survive -- an "
        "unstored context still reads a small positive number -- but the "
        "boundary and the backoff now disagree about what \"no evidence\" is "
        "worth, so a context the smoother emitted at the floor and one it "
        "never emitted read DIFFERENTLY, which is the distinction the reversal "
        "exists to erase",
        "test_a_context_absent_from_the_table_reads_the_FLOOR_not_a_hard_zero",
        [
            (
                "    return np.where(values == 0.0, FLOOR, values)",
                "    return np.where(values == 0.0, 1e-5, values)",
            )
        ],
    ),
    (
        "T52",
        "the flooring applied in `lookup` but NOT in `vector`, so the two "
        "public readers disagree about an unobserved context: a scalar query "
        "gets the floor and the vector query the fusion sweep actually uses "
        "gets a hard zero. Precisely the writer/reader drift the single "
        "`_floor_missing` definition exists to prevent, and invisible to any "
        "test that exercises only one reader",
        "test_vector_and_lookup_agree_on_every_species_of_a_context",
        [
            (
                "        stored = np.asarray(self.values[:, [col]].todense(), dtype=np.float64).ravel()\n"
                "        return _floor_missing(stored)",
                "        return np.asarray(\n"
                "            self.values[:, [col]].todense(), dtype=np.float64\n"
                "        ).ravel()",
            )
        ],
    ),
    (
        "T53",
        "`nnz` and the dense capacity dropped from the sidecar, so a released "
        "artifact carries no visible evidence of its own fill. A third party "
        "then cannot tell a prior built from a dense national grid from one "
        "built from a handful of observed cells, and the two warrant very "
        "different confidence",
        "test_the_sidecar_publishes_the_fill_of_the_artifact",
        [
            (
                '        "nnz": int(tensor.values.nnz),\n'
                '        "dense_capacity": int(tensor.dense_capacity),\n',
                "",
            )
        ],
    ),
    (
        "T54",
        "the empty-DOI refusal removed, so a release ships with empty "
        "provenance SILENTLY: the `.npz` writes fine, the sidecar is valid "
        "JSON, and nothing in the artifact states which GBIF download it came "
        "from. CC BY requires attribution, so this is a licence breach that no "
        "reader of the artifact can detect",
        "test_save_refuses_a_tensor_with_no_gbif_doi",
        [
            (
                "    if not tensor.gbif_doi or not tensor.gbif_doi.strip():\n"
                "        raise ValueError(",
                "    if False:\n        raise ValueError(",
            )
        ],
    ),
    (
        "T54b",
        "C2: only the WHITESPACE half of the refusal removed, so `\'\'` is still "
        "rejected and `\'   \'` ships the whole release. This is the exact "
        "released defect: `--gbif-doi \'   \'` wrote all five artifacts with "
        "attribution `Derived from GBIF occurrence download    .` A separate "
        "mutant from T54 because `\'   \'` is TRUTHY -- the emptiness check "
        "passes it -- so a fixture using only `\'\'` cannot tell the two "
        "disjuncts apart, and the whitespace one is the half an unset shell "
        "variable produces",
        "test_save_refuses_a_tensor_whose_gbif_doi_is_only_whitespace",
        [
            (
                "    if not tensor.gbif_doi or not tensor.gbif_doi.strip():",
                "    if not tensor.gbif_doi:",
            )
        ],
    ),
    (
        "T55",
        "the attribution string reduced to the licence alone, dropping the "
        "DOI. Both fields are still present separately, so T40 and T38 pass; "
        "what breaks is the one string a third party can actually cite from "
        "without consulting this repository",
        "test_the_sidecar_carries_an_attribution_naming_both_the_doi_and_the_licence",
        [
            (
                '            f"Derived from GBIF occurrence download {tensor.gbif_doi}. "\n'
                '            f"Released under {LICENCE} ({LICENCE_URL}); cite that DOI and this "\n'
                '            "licence when redistributing or building on this artifact."',
                '            f"Released under {LICENCE}."',
            )
        ],
    ),
    (
        "T56",
        "the matrix TRANSPOSED on write and transposed back on read: "
        "consistent on BOTH sides, so every round-trip test in the suite "
        "passes while the released `.npz` has rows indexing cell-weeks and "
        "columns indexing species. Every third-party reader decoding it "
        "against the sidecar's published packing gets every value wrong. Only "
        "the independent numpy reader can see this",
        "test_the_npz_is_decodable_by_a_third_party_with_numpy_alone",
        [
            (
                "    sparse.save_npz(npz, tensor.values.tocoo())",
                "    sparse.save_npz(npz, tensor.values.T.tocoo())",
            ),
            (
                "    matrix = sparse.load_npz(npz).tocsr()",
                "    matrix = sparse.load_npz(npz).T.tocsr()",
            ),
        ],
    ),
    (
        "T57",
        "the artifact written in scipy's CSR format rather than COO -- "
        "consistent on both sides, so it round-trips perfectly, but the "
        "on-disk members change from (data, row, col, shape) to (data, "
        "indices, indptr, shape). A third-party reader written against the "
        "published COO layout finds no `row` member at all",
        "test_the_npz_is_decodable_by_a_third_party_with_numpy_alone",
        [
            (
                "    sparse.save_npz(npz, tensor.values.tocoo())",
                "    sparse.save_npz(npz, tensor.values.tocsr())",
            )
        ],
    ),
    (
        "T58",
        "the sidecar's `missing_value` dropped, so a third party decoding the "
        "matrix has no way to know an unstored entry means the floor. They "
        "fill with 0.0, and every unobserved context silently vetoes its "
        "species -- the reversal's whole point, lost at the boundary where it "
        "matters most",
        "test_the_sidecar_publishes_the_value_an_absent_context_takes",
        [
            (
                '        "missing_value": FLOOR,\n',
                "",
            )
        ],
    ),
    (
        "T59",
        "the missing-value agreement check on load removed, so an artifact "
        "published under a DIFFERENT absent-context convention is read against "
        "this build's floor and every unobserved context silently changes "
        "value",
        "test_a_sidecar_declaring_a_different_missing_value_is_rejected",
        [
            (
                '    if float(meta["missing_value"]) != FLOOR:',
                "    if False:",
            )
        ],
    ),
    (
        "T60",
        "the nnz-agreement check on load removed, so a sidecar paired with a "
        "DIFFERENT matrix loads without complaint: the axes then describe a "
        "matrix they do not belong to and every lookup reads the wrong row or "
        "column",
        "test_a_sidecar_whose_nnz_disagrees_with_the_matrix_is_rejected",
        [
            (
                '    if int(meta["nnz"]) != matrix.nnz:',
                "    if False:",
            )
        ],
    ),
    (
        "T61",
        "`dense_capacity` computed WITHOUT the week axis, so the published "
        "fill overstates coverage by a factor of 52 and a release looks 52x "
        "denser than it is",
        "test_dense_capacity_is_the_product_of_the_three_axes",
        [
            (
                "        return len(self.species_keys) * len(self.cells) * WEEKS_PER_YEAR",
                "        return len(self.species_keys) * len(self.cells)",
            )
        ],
    ),
    (
        "T62",
        "`dense()` skips the flooring, so the one matrix-shaped reader "
        "disagrees with `lookup` and `vector` about every unobserved context. "
        "Separate from T52, which breaks `vector` instead: each reader needs "
        "its own mutant because one's loss cannot detect another's",
        "test_dense_floors_every_unstored_context_and_preserves_the_stored_ones",
        [
            (
                "        return _floor_missing(np.asarray(self.values.toarray(), dtype=np.float64))",
                "        return np.asarray(self.values.toarray(), dtype=np.float64)",
            )
        ],
    ),
    # ---- M1: a non-whole week is REJECTED, not truncated (one per clause) --
    (
        "T64",
        "the BOOL clause of the week guard removed, so `lookup(..., True)` is "
        "answered as week 1 and `lookup(..., False)` as week 0. Its own mutant "
        "because `isinstance(True, int)` is True in Python: the numeric-type "
        "clause below ADMITS a bool, and `True % 1 == 0` and `1 <= True <= 52` "
        "both hold, so every other clause in the guard passes a bool through",
        "test_lookup_rejects_a_bool_week_even_though_a_bool_IS_an_int",
        [
            (
                "    if isinstance(week, bool):",
                "    if False:",
            )
        ],
    ),
    (
        "T65",
        "the bool clause moved BELOW both bounds, so `False` trips the "
        "below-1 guard first and the caller is told to fix an out-of-range "
        "WEEK NUMBER when the defect is that a bool was passed at all. `True` "
        "still dies to the relocated clause, so this is the ORDER and nothing "
        "else -- a separate mutant from T64, which deletes the clause outright",
        "test_a_false_week_is_rejected_as_a_bool_not_as_a_below_one_week",
        [
            (
                "    if isinstance(week, bool):\n"
                "        raise ValueError(\n"
                '            f"week {week!r} in {where} is a bool, not a week number; Python "\n'
                '            "counts True as 1 and False as 0, so a bool week would be answered "\n'
                '            f"as week {int(week)} -- a week the caller never asked for -- and "\n'
                '            "this boundary does not repair the value it was handed"\n'
                "        )\n"
                "    if not isinstance(week, (int, float, np.integer, np.floating)):",
                "    if not isinstance(week, (int, float, np.integer, np.floating)):",
            ),
            (
                '            f"week 53 is folded into {WEEKS_PER_YEAR} upstream"\n'
                "        )",
                '            f"week 53 is folded into {WEEKS_PER_YEAR} upstream"\n'
                "        )\n"
                "    if isinstance(week, bool):\n"
                "        raise ValueError(\n"
                '            f"week {week!r} in {where} is a bool, not a week number; Python "\n'
                '            "counts True as 1 and False as 0, so a bool week would be answered "\n'
                '            f"as week {int(week)} -- a week the caller never asked for -- and "\n'
                '            "this boundary does not repair the value it was handed"\n'
                "        )",
            ),
        ],
    ),
    (
        "T66",
        "the NUMERIC-TYPE clause removed, so a string week reaches `week % 1` "
        "and dies on a raw TypeError instead of the ValueError this module's "
        "contract promises for every violation. Separate from T67 because the "
        "integrality clause cannot stand in for it: `'5' % 1` does not "
        "evaluate at all",
        "test_lookup_rejects_a_non_numeric_week",
        [
            (
                "    if not isinstance(week, (int, float, np.integer, np.floating)):",
                "    if False:",
            )
        ],
    ),
    (
        "T67",
        "the INTEGRALITY clause removed, so week 46.9 passes both bounds and "
        "`int(_column_index(ci, 46.9))` truncates to week 46's column: the "
        "caller is handed a real probability belonging to a DIFFERENT week with "
        "nothing raised. Separate from T64 and T66, whose clauses see nothing "
        "wrong with a plain in-range float",
        "test_lookup_rejects_a_fractional_week_rather_than_truncating_it",
        [
            (
                "    if week % 1 != 0:",
                "    if False:",
            )
        ],
    ),
    (
        "T68",
        "the TRUNCATE-THEN-VALIDATE call site restored: `_column` validates "
        "`int(week)` instead of the value the caller passed, so the guard never "
        "sees 46.9 at all -- it approves week 46 and the lookup answers week "
        "46. This is the defect M1 reports, in the exact form it had: the "
        "clauses can all be present and still be bypassed by truncating before "
        "they run, which is why the call site needs its own mutant",
        "test_lookup_rejects_a_fractional_week_rather_than_truncating_it",
        [
            (
                '        _validate_week(week, "a tensor lookup")',
                '        _validate_week(int(week), "a tensor lookup")',
            )
        ],
    ),
    # ---- M2: the packing's inverse, single-sourced beside the forward form -
    (
        "T69",
        "`_cell_of_column` off by one cell, so the duplicate-row message names "
        "the cell BEFORE the one the duplicate is actually in. Nothing else in "
        "the module reads the inverse, so the only visible damage is a "
        "contract-breach message that sends whoever reads it to hunt for a real "
        "duplicate in a cell that has none -- which is why the test must assert "
        "WHICH cell the message names, not merely that it raises",
        "test_the_duplicate_message_names_the_cell_the_duplicate_is_actually_in",
        [
            (
                "    return col // WEEKS_PER_YEAR",
                "    return col // WEEKS_PER_YEAR - 1",
            )
        ],
    ),
    (
        "T70",
        "the INVERSE's stride hard-coded to 53 while the forward packing keeps "
        "WEEKS_PER_YEAR -- the drift the two being adjacent exists to prevent. "
        "It happens to name the right cell for many columns (97 // 53 == 1), so "
        "only a test that round-trips EVERY (cell, week) of a grid catches it; "
        "a single-duplicate fixture would pass",
        "test_the_column_packing_and_its_inverse_round_trip_over_the_whole_grid",
        [
            (
                "    return col // WEEKS_PER_YEAR",
                "    return col // 53",
            )
        ],
    ),
    # ---- M3: the DECLARED dimensions vs the ENCODED ones ------------------
    #
    # The I4 sidecar decoy differs in axis VALUES but not in axis LENGTH, so a
    # reader that sized the grid from the sidecar's declared dimensions while
    # decoding the matrix bytes passed it. These two mutants ARE that reader:
    # `load_prior` reshapes the loaded matrix onto the axis lengths the sidecar
    # declares, so the shape guards in `__post_init__` can never disagree with
    # it and a sidecar paired with a matrix of another shape is silently
    # accepted -- padded with implicit zeros, which this boundary then reads as
    # the FLOOR, yielding a plausible prior over contexts the data never had.
    # One mutant per axis: a matrix can have the right width and the wrong
    # height, so neither reshape can detect the other's loss.
    (
        "T71",
        "`load_prior` takes the COLUMN count from the SIDECAR's declared cell "
        "axis rather than from the matrix, reshaping the loaded matrix onto it. "
        "A sidecar declaring two cells against a one-cell matrix then loads "
        "without complaint, padded with a whole cell-block of implicit zeros "
        "that reads as the floor -- a fabricated cell answering plausible "
        "probabilities. The shape guard cannot fire because the mutant makes "
        "the matrix agree with the axes by construction",
        "test_a_sidecar_declaring_a_different_axis_LENGTH_than_the_matrix_is_rejected",
        [
            (
                "    matrix = sparse.load_npz(npz).tocsr()",
                "    matrix = sparse.load_npz(npz).tocsr()\n"
                "    matrix.resize(\n"
                "        matrix.get_shape()[0],\n"
                '        len(meta["cells"]) * int(meta["weeks_per_year"]),\n'
                "    )",
            )
        ],
    ),
    (
        "T72",
        "the ROW half of T71: `load_prior` takes the row count from the "
        "SIDECAR's declared species axis and reshapes onto it, so a sidecar "
        "listing a species the matrix has no row for loads silently and every "
        "lookup of that invented species is answered with a floored -- and "
        "entirely fabricated -- prior. Separate mutant because a matrix can "
        "have the right width and the wrong height",
        "test_a_sidecar_declaring_a_different_SPECIES_axis_length_is_rejected",
        [
            (
                "    matrix = sparse.load_npz(npz).tocsr()",
                "    matrix = sparse.load_npz(npz).tocsr()\n"
                "    matrix.resize(\n"
                '        len(meta["species_keys"]),\n'
                "        matrix.get_shape()[1],\n"
                "    )",
            )
        ],
    ),
    # ---- I5: the RELEASED DOCUMENTATION is executed, not trusted ----------
    (
        "T73",
        "a NEW field added to the sidecar that `docs/prior-artifact-format.md` "
        "does not document. Nothing round-trips differently, every existing "
        "sidecar test still passes -- they assert the fields they care about, "
        "never the field SET -- and the released artifact quietly grows a "
        "field its own release documentation never mentions. This is "
        "documentation rot in the direction it actually happens (the code "
        "moves, the prose does not), and only a test that reads the document "
        "against a real artifact can see it",
        "test_the_artifact_documentation_matches_a_freshly_saved_artifact",
        [
            (
                '        "nnz": int(tensor.values.nnz),',
                '        "row_axis": "species_keys",\n'
                '        "nnz": int(tensor.values.nnz),',
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


# A mutant that BREAKS the target module rather than changing its behaviour is
# not a mutant at all. `AttributeError`, `NameError` and `ImportError` mean the
# edit referenced something that does not exist (T28's original
# `pa.compute.utf8_lower` against a module importing only `pyarrow as pa`), and
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


def _crash_in_target(out: str) -> str | None:
    """The crash type, if the output shows one raised FROM THE TARGET MODULE.

    Scoped to the target file on purpose. A mutant is entitled to make a TEST
    raise `AttributeError` -- for instance one that removes a method a test
    calls, where that IS the behaviour under test. What is never legitimate is
    the mutated module failing to load or to resolve its own names, and that is
    what the traceback line `src/birdology/prior/tensor.py:NNN: SomeError`
    reports.
    """
    for crash in _CRASH_TYPES:
        # pytest's one-line traceback summary: "<path>:<line>: <ErrorType>".
        if re.search(rf"^{re.escape(str(TARGET))}:\d+: {crash}$", out, re.MULTILINE):
            return crash
        # A module that does not even import reports the error without a
        # "path:line:" prefix of its own, but names the file in the frame.
        if re.search(
            rf'File "[^"]*{re.escape(TARGET.name)}".*\n(?:.*\n)??\s*{crash}:',
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
    crashed: list[str] = []

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

        crash = _crash_in_target(out)
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
