"""End-to-end: archive in, prior tensors out. NO NETWORK.

NETWORK ISOLATION
-----------------
The download stage (`gbif_download.py`) is the only part of this pipeline
that can reach the internet, and `build_from_archive` does not import it or
call it: its input is a LOCAL zip built here by `zipfile.writestr`. That is
structural, not a convention -- the `--archive` path is a parameter, and no
code path from `build_from_archive` reaches `requests`. It is asserted
directly by `test_the_build_path_never_reaches_the_download_client`, which
poisons `requests` for the duration of a full build, so a future import of
the download client inside the build path fails loudly rather than making a
live call from a test run. The download client's own behaviour is covered by
`test_gbif_download.py`, entirely against mocks.

ARTIFACT PATHS come from the `out_dir` PARAMETER (`tmp_path` here), never
from a hardcoded cluster path.
"""
import builtins
import json
import re
import logging
import shutil
import sys
import zipfile
from pathlib import Path

import h3
import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.backoff import FLOOR, BackoffParams
from birdology.prior.controls import CONTROLS
from birdology.prior.indexing import (
    WEEKS_PER_YEAR,
    aggregate_counts,
    assign_cells,
)
from birdology.prior.leakage import (
    GUARD_MARKER_KEY,
    XENO_CANTO_PUBLISHER_KEYS,
    mark_guards_applied,
)
from birdology.prior.occurrences import read_occurrences
from birdology.prior.smoothing import SmoothingParams
from birdology.prior.tensor import LICENCE, load_prior

import build_prior
from build_prior import (
    MANIFEST_VERSION,
    ZeroMatchGuardError,
    check_manifest,
    build_from_archive,
    estimate_over_full_grid,
    load_test_split,
    recordings_at_resolution,
)

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)

def _measurement_row(n_species: int, n_cells: int, net_peak_bytes: int) -> dict:
    """A SELF-CONSISTENT synthetic row of `measure_peak_results.json`.

    Built from the raw observations -- grid shape and peak RSS -- with the
    stated ratio DERIVED, exactly as `measure_peak.py` writes it. A test that
    needs an inconsistent row makes one by overriding a field on the result,
    so the inconsistency is explicit at the call site rather than an accident
    of the fixture.
    """
    contexts = n_species * n_cells * WEEKS_PER_YEAR
    return {
        "n_species": n_species,
        "n_cells": n_cells,
        "fill": 1.0,
        "contexts": contexts,
        "net_peak_bytes": net_peak_bytes,
        "bytes_per_context_net": net_peak_bytes / contexts,
    }


#: The M3 spellings: a DOI that is CORRECT but carries incidental whitespace
#: or a citation label. Shared by the refusal test and the message test so the
#: two provably speak about the same inputs -- a drift between two literal
#: tuples would let one of them quietly stop covering the trailing-space case.
_WHITESPACE_DOIS = (
    "10.15468/dl.bq3fxc ",
    " 10.15468/dl.bq3fxc",
    "DOI: 10.15468/dl.bq3fxc",
    "10.15468/dl.bq3\tfxc",
)

DOI = "10.15468/dl.test"

#: Artifacts one resolution must produce: the full prior plus ONE PER REGISTRY
#: ENTRY. Derived from `CONTROLS`, never a literal count -- the build loop
#: iterates the registry, and a hardcoded 4, 5 or a fixed name set makes every
#: assertion in this file wrong the day a control is added or removed. The
#: fifth control (`separable`) was added while this file was being written and
#: broke four hardcoded `== 5` assertions, which is the evidence for doing it
#: this way rather than an argument for it.
EXPECTED_VARIANTS = frozenset({"full", *CONTROLS})
EXPECTED_ARTIFACTS = len(EXPECTED_VARIANTS)

#: Four well-separated Danish points. They land in FOUR DISTINCT H3 cells at
#: resolutions 3, 4 and 5 alike (verified), so a multi-resolution sweep really
#: sweeps rather than collapsing to one cell at the coarse end.
POINTS = [
    (55.6867, 12.5700),   # Copenhagen
    (55.9000, 12.5000),   # North Zealand
    (56.1629, 10.2039),   # Aarhus
    (57.0488, 9.9217),    # Aalborg
]

#: Three dates in ISO weeks 2, 24 and 46 (verified). Every OTHER week of the
#: year is therefore unobserved, which is what the full-grid assertions need.
DATES = ("2025-01-07", "2025-06-10", "2025-11-14")
OBSERVED_WEEKS = (2, 24, 46)

SPECIES = (1001, 1002)

#: A THIRD species, present in exactly ONE (cell, week). It exists to make
#: the fixture SPARSE in the species direction, and that is load-bearing
#: rather than decoration.
#:
#: Measured: with every species present in every cell-week, restricting each
#: backoff chunk's ratio table to its own species changes NOTHING -- the
#: parent denominators are sums of `total` over cell-weeks, and every chunk
#: sees the same cell-week key set, so the filtered and unfiltered tables
#: give bit-identical output (max |diff| = 0). A chunk-scoping defect is
#: therefore INVISIBLE on a dense fixture. Add one species that occupies a
#: single cell-week and the same defect moves its value by 0.071: its chunk
#: now sees one cell-week's worth of effort instead of the archive's.
#:
#: Real occurrence data is overwhelmingly of this shape -- most species are
#: in a handful of cells -- so the dense fixture was the unrepresentative one.
SPARSE_SPECIES = 1003
SPARSE_WEEK_INDEX = 0
SPARSE_POINT_INDEX = 0
ALL_SPECIES = (*SPECIES, SPARSE_SPECIES)

#: Records per (species, cell, week), indexed by week position. The two
#: species are deliberately UNBALANCED and the balance CHANGES from week to
#: week. An equal split would make every ratio exactly 0.5 at every level of
#: the hierarchy, so an observed week and an unobserved one would carry the
#: same number and no assertion could tell an estimated context from a
#: copied cell profile. (Measured: with 5 and 5 records the two are
#: bit-identical at 0.5.)
COUNTS_BY_WEEK = {
    0: {1001: 8, 1002: 2},
    1: {1001: 5, 1002: 5},
    2: {1001: 2, 1002: 8},
}


def _row(i, sk, lat, lon, d, pub="pub", who="obs"):
    return "\t".join(
        [str(i), "ds", pub, str(sk), f"sp{sk}", str(lat), str(lon), d, who]
    )


def _zip(tmp_path, rows, name="occ.zip"):
    zip_path = tmp_path / name
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return zip_path


def _archive(tmp_path, extra_rows=()):
    """Two dense species + one SPARSE one, over four cells and three weeks.

    The per-week species balance is what makes the hierarchy observable: with
    a constant balance every level of the backoff collapses to the same
    number and an estimated context is numerically indistinguishable from a
    copied cell profile.

    `SPARSE_SPECIES` occupies exactly one (cell, week). See its comment: a
    fixture in which every species is present in every cell-week makes a
    whole class of chunk-scoping defects numerically invisible, because the
    hierarchy's parent denominators depend on the cell-week key set rather
    than on which species are present.
    """
    rows = []
    i = 0
    for wi, week_date in enumerate(DATES):
        for pi, (lat, lon) in enumerate(POINTS):
            for sk in SPECIES:
                for _ in range(COUNTS_BY_WEEK[wi][sk]):
                    i += 1
                    rows.append(_row(i, sk, lat + i * 1e-7, lon, week_date))
            if wi == SPARSE_WEEK_INDEX and pi == SPARSE_POINT_INDEX:
                i += 1
                rows.append(
                    _row(i, SPARSE_SPECIES, lat + i * 1e-7, lon, week_date)
                )
    rows.extend(extra_rows)
    return _zip(tmp_path, rows)


def _build(tmp_path, **kwargs):
    kwargs.setdefault("resolution", 4)
    kwargs.setdefault("source", "dof")
    kwargs.setdefault("test_recordings", [])
    kwargs.setdefault("out_dir", tmp_path / "priors")
    kwargs.setdefault("gbif_doi", DOI)
    archive = kwargs.pop("archive", None) or _archive(tmp_path)
    return build_from_archive(archive, **kwargs)


# --------------------------------------------------------------------------
# The brief's four acceptance tests.
# --------------------------------------------------------------------------


def test_writes_full_prior_and_every_registered_control(tmp_path):
    """One artifact per registry entry, plus the full prior.

    DERIVED FROM `CONTROLS`, never a hardcoded set of names. The build loop
    iterates the registry, so a control added there must appear on disk with
    no edit to the build script -- and a test naming the four (or five) it
    knows about would either fail spuriously on the addition or, worse,
    silently stop checking the new one. `EXPECTED_VARIANTS` is the one
    derivation every count in this file reads.
    """
    paths = _build(tmp_path)
    variants = {load_prior(p).spec.variant for p in paths}
    assert variants == EXPECTED_VARIANTS
    # ...and the registry is not empty, so the equality above is not vacuous.
    assert len(EXPECTED_VARIANTS) > 1


def test_full_prior_has_probabilities_for_seen_species(tmp_path):
    paths = _build(tmp_path)
    full = next(p for p in paths if load_prior(p).spec.variant == "full")
    t = load_prior(full)
    assert set(t.species_keys.tolist()) == set(ALL_SPECIES)
    assert t.values.max() > 0.0


def test_doi_travels_into_every_artifact(tmp_path):
    """Provenance reaches the CONTROLS too, not just the full prior.

    A control artifact with no DOI is exactly as uncitable as a full one:
    it is released under the same CC BY 4.0 terms, derived from the same
    occurrence records, and reported in the same results table.
    """
    paths = _build(tmp_path)
    assert len(paths) == EXPECTED_ARTIFACTS
    for p in paths:
        assert load_prior(p).gbif_doi == DOI


def test_the_controls_carry_the_doi_without_the_scripts_help(tmp_path):
    """M3: the build script's 'belt and braces' comment, made true.

    `build_from_archive` sets `control.gbif_doi = gbif_doi` on every control
    and calls it belt-and-braces beside `controls._respec`'s own pass-through.
    That was a claim about ANOTHER MODULE that nothing tested from here: the
    harness's B11 deletes the assignment (the belt) and
    `test_doi_travels_into_every_artifact` catches it, but nothing established
    that the braces -- `_respec`'s pass-through -- existed at all. A comment
    claiming redundancy that is not there is worse than no comment, because
    the next person deletes the line it defends.

    This asserts the pass-through DIRECTLY, on the library, with no build
    script involved: a control built from a tensor that carries a DOI carries
    it too. Killer test for the harness's B11b, which breaks `_respec` in
    `controls.py` while leaving the build script's assignment intact -- so the
    build-script tests pass on it and only this one fails.
    """
    from birdology.prior.controls import CONTROLS

    paths = _build(tmp_path)
    full = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))
    assert full.gbif_doi == DOI

    for name, make in CONTROLS.items():
        control = make(full)
        assert control.gbif_doi == DOI, (
            f"`_respec` did not carry the DOI into the {name!r} control, so "
            "the build script's `control.gbif_doi = gbif_doi` is the ONLY "
            "thing providing provenance and its 'belt and braces' comment is "
            "false. Any other caller of the control builders -- a notebook, a "
            "sweep driver -- would write an unattributed artifact"
        )


def test_filenames_encode_source_resolution_and_variant(tmp_path):
    paths = _build(tmp_path, resolution=5, source="eod", gbif_doi="d")
    names = {p.name for p in paths}
    assert "prior_eod_r5_full.npz" in names
    assert "prior_eod_r5_marginal.npz" in names
    assert "prior_eod_r5_uniform.npz" in names
    assert "prior_eod_r5_shuffle_space.npz" in names
    assert "prior_eod_r5_shuffle_time.npz" in names


# --------------------------------------------------------------------------
# T10-R1: the FULL 52-week grid, passed explicitly.
#
# This is the assertion the retired structural-zero check becomes. Under the
# backoff's DEFAULT axes the grid collapses to the OBSERVED weeks -- three of
# them on this fixture -- and the artifact is then well-formed, correct in
# every stored value, and impoverished in its coverage with nothing raised.
# --------------------------------------------------------------------------


def test_the_full_prior_spans_every_week_of_the_year(tmp_path):
    """`n_cells * WEEKS_PER_YEAR` columns EXACTLY, not the observed weeks.

    The precondition is asserted first: the fixture really does observe only
    three of the 52 weeks, so a collapsed grid is visibly different from the
    full one. Without that, a 52-week assertion could pass on a fixture that
    happened to observe every week and would prove nothing.
    """
    paths = _build(tmp_path)
    t = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))

    assert len(OBSERVED_WEEKS) < WEEKS_PER_YEAR, "the fixture must be sparse in time"
    assert t.values.get_shape()[1] == len(t.cells) * WEEKS_PER_YEAR
    # ...and the observed-axes default really would have been smaller, so the
    # assertion above is discriminating rather than trivially true.
    assert len(t.cells) * len(OBSERVED_WEEKS) < len(t.cells) * WEEKS_PER_YEAR


def test_an_unobserved_week_is_ESTIMATED_not_defaulted_to_the_floor(tmp_path):
    """FIXTURE SANITY CHECK, not the proof of T10-R1. Read the whole docstring.

    WHAT THIS DOES NOT PROVE, and why the reviewer's original ruling (which
    asked for exactly this assertion as the proof) was WRONG:

        backoff.py:340   out_p.append(min(max(p, params.floor), 1.0))

    Every emitted `p` is CLAMPED to FLOOR from below. So an estimated context
    and a never-seen one are NUMERICALLY IDENTICAL whenever the estimate falls
    at or below FLOOR -- and on a realistic rare species (1 record against
    100k, 6 cells) all 312 estimated contexts land at or below 10*FLOOR, 261
    of them EXACTLY FLOOR. On such a species `value > FLOOR` would FAIL on a
    CORRECTLY BUILT artifact. The clamp makes "above the floor" a statement
    about the DATA's abundance, not about whether the hierarchy ran.

    On THIS fixture the two dense species are abundant, so the margin is
    enormous (~492000x FLOOR) and the assertion holds comfortably -- which is
    the other half of the problem: a threshold that passes by five orders of
    magnitude discriminates nothing either.

    So it is kept, as a FIXTURE-SPECIFIC sanity check that the fixture's own
    abundant species really do come out above the floor, and it is demoted
    from proof. The proof of T10-R1 is
    `test_every_context_of_the_grid_is_stored_not_merely_reachable`
    (`nnz == dense_capacity`), a REPRESENTATION claim the clamp cannot spoof,
    and
    `test_an_unobserved_week_is_SHRUNK_TOWARD_the_species_global_profile`,
    which asserts the shrinkage itself rather than a magnitude.
    """
    paths = _build(tmp_path)
    t = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))

    unobserved = 30
    assert unobserved not in OBSERVED_WEEKS, "the week must be genuinely unobserved"

    value = t.lookup(SPECIES[0], t.cells[0], unobserved)
    assert value > FLOOR, (
        f"week {unobserved} read exactly the floor. On THIS fixture the dense "
        "species are abundant enough that a real estimate must exceed the "
        "floor; on a rare species it legitimately would not (backoff.py:340 "
        "clamps), which is why this is a fixture check and not the proof"
    )
    # Fixture-specific magnitude, kept for the same reason and with the same
    # caveat. NOT a general property: see the docstring.
    assert value > 10 * FLOOR


def test_an_unobserved_week_is_SHRUNK_TOWARD_the_species_global_profile(tmp_path):
    """THE T10-R1 VALUE-LEVEL PROOF: shrinkage, not a magnitude threshold.

    This is the assertion the reviewer could not construct, and it works
    because it compares an unobserved week against TWO OTHER NUMBERS FROM THE
    SAME ARTIFACT rather than against a constant. `min(max(p, floor), 1.0)`
    can force a value UP to the floor; it cannot manufacture an ORDERING
    between three values it did not compute.

    The claim, straight out of `hierarchical_estimate`: for an unobserved
    (species, cell, week), the cell-week level contributes nothing, so

        p(unobserved week) == _shrink(0, 0, p_cell, w) == p_cell

    -- the CELL's own annual profile for that species. And `p_cell` is itself
    the cell's count shrunk toward the species' GLOBAL share. So the value at
    an unobserved week must sit BETWEEN the species' behaviour in the two
    cells it is compared against, and specifically:

    * it must DIFFER between two cells where the species has different local
      abundance, which a defaulted FLOOR could not do (FLOOR is one constant
      and carries no cell information at all);
    * the species that is LOCALLY COMMONER in a cell must read HIGHER at the
      unobserved week in that same cell.

    Both are properties of the hierarchy having run. The fixture's per-week
    balance is deliberately unbalanced and CHANGES week to week
    (`COUNTS_BY_WEEK`), so the two species' cell profiles genuinely differ.

    NO NEW THRESHOLD CONSTANT is introduced: every number compared is read
    from the artifact.
    """
    paths = _build(tmp_path)
    t = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))

    unobserved = 30
    assert unobserved not in OBSERVED_WEEKS

    cell = t.cells[0]
    dense_a = t.lookup(SPECIES[0], cell, unobserved)
    dense_b = t.lookup(SPECIES[1], cell, unobserved)
    sparse = t.lookup(SPARSE_SPECIES, cell, unobserved)

    # 1. The unobserved week carries SPECIES-SPECIFIC information. A context
    #    defaulted to the clamp would give all three the same FLOOR.
    assert dense_a != sparse, (
        "an abundant species and a one-record species read the SAME value at "
        "an unobserved week, so the value carries no evidence: the week was "
        "defaulted, not estimated"
    )
    assert dense_b != sparse

    # 2. It is ORDERED by the species' actual abundance, which is the
    #    shrinkage doing its work. The sparse species has one record in the
    #    whole archive; the dense ones have dozens.
    assert sparse < dense_a
    assert sparse < dense_b

    # 3. And it is strictly ABOVE the floor for the sparse species too -- not
    #    because of any threshold, but because the floor is what a DEFAULTED
    #    context reads, and a real estimate for a species with a record in
    #    this very cell cannot coincide with "no evidence anywhere".
    assert sparse != FLOOR, (
        "the one-record species read EXACTLY the floor at an unobserved week, "
        "which is what a context absent from the tensor reads: its estimate "
        "was never computed"
    )

    # 4. The same context is STORED, so none of the above came from `lookup`'s
    #    floor substitution. This is the link between the value-level claim and
    #    the representation-level one.
    row = int(np.where(t.species_keys == SPARSE_SPECIES)[0][0])
    col = t.cells.index(cell) * WEEKS_PER_YEAR + (unobserved - 1)
    assert t.values[row, col] != 0.0, (
        "the context is not stored, so every assertion above was reading "
        "`lookup`'s floor substitution rather than an estimate"
    )


def test_every_context_of_the_grid_is_stored_not_merely_reachable(tmp_path):
    """THE STATED PROOF OF T10-R1: `nnz` equals the dense capacity.

    Promoted from a supporting check to THE proof, because it is the one claim
    in this file the FLOOR clamp cannot spoof. `backoff.py:340` clamps every
    emitted `p` to FLOOR from below, so any assertion of the form "this value
    is above FLOOR" is a statement about how abundant the data happened to be,
    not about whether the hierarchy ran -- see
    `test_an_unobserved_week_is_ESTIMATED_not_defaulted_to_the_floor` for the
    measured case where it fails on a CORRECT artifact.

    This is a REPRESENTATION claim instead: every context of the requested
    (species x cells x 52) grid is physically STORED in the matrix. The clamp
    changes values; it cannot conjure stored coordinates. Under the
    observed-axes default the grid would collapse to the three observed weeks
    and `nnz` would be 3/52 of the capacity, whatever any individual value
    read.

    It is also exactly what the sidecar's published `fill` reports to a third
    party, so the proof and the released claim are one number.
    """
    paths = _build(tmp_path)
    t = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))
    assert t.values.nnz == t.dense_capacity
    assert t.dense_capacity == len(ALL_SPECIES) * len(t.cells) * WEEKS_PER_YEAR
    # ...and the fixture really is sparse in time, so a collapsed grid would be
    # visibly smaller rather than coincidentally equal.
    assert len(OBSERVED_WEEKS) < WEEKS_PER_YEAR


def test_an_observed_week_and_an_unobserved_week_carry_DIFFERENT_values(tmp_path):
    """The hierarchy is doing work: the two weeks are not one flat number.

    If every week of a cell came out identical, the 52-week grid would be a
    52-fold copy of the cell profile and passing `weeks=` would buy nothing
    but size. The observed week must differ from the unobserved one.
    """
    paths = _build(tmp_path)
    t = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))
    cell = t.cells[0]
    observed = t.lookup(SPECIES[0], cell, OBSERVED_WEEKS[0])
    unobserved = t.lookup(SPECIES[0], cell, 30)
    assert observed != unobserved
    assert observed > unobserved


def test_the_fixture_is_sparse_in_species_not_just_in_time(tmp_path):
    """PRECONDITION of `test_species_chunking_is_bit_exact`, asserted directly.

    Not a proxy: it reads the built prior and checks that `SPARSE_SPECIES`
    really does occupy ONE (cell, week) in the archive while the dense
    species occupy all of them. If that ever stops holding, chunk-scoping
    mutants go silently equivalent -- measured, not hypothesised -- and the
    bit-exactness test would keep passing while proving nothing.
    """
    paths = _build(tmp_path)
    t = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))
    assert set(t.species_keys.tolist()) == set(ALL_SPECIES)

    # In the RATIO table the sparse species has exactly one row; the dense
    # ones have one per (cell, week) they were observed in.
    table = assign_cells(read_occurrences(_archive(tmp_path)), 4)
    counts = aggregate_counts(mark_guards_applied(table))
    rows = counts.to_pylist()
    occupancy = {}
    for row in rows:
        occupancy.setdefault(row["species_key"], set()).add(
            (row["cell"], row["week"])
        )
    assert len(occupancy[SPARSE_SPECIES]) == 1, (
        "the sparse species must occupy exactly one cell-week, or the "
        "chunk-scoping mutants become equivalent"
    )
    for sk in SPECIES:
        assert len(occupancy[sk]) == len(POINTS) * len(DATES)


def test_species_chunking_is_bit_exact(tmp_path):
    """Chunking bounds memory; it must not change a single value.

    Every level of the hierarchy is derived from the RATIO TABLE, which each
    chunk receives whole, so a chunk's estimates cannot depend on which other
    species share the call. Smoothing is per-chunk for the same reason: the
    kernel never blends across species. Asserted as EXACT equality rather
    than `approx`, because an approximation here would mean the claim is
    false and the artifact depends on an arbitrary batch size.

    Requires a fixture that is SPARSE in the species direction --
    `test_the_fixture_is_sparse_in_species_not_just_in_time` pins that. On a
    dense fixture a chunk that scopes the ratio table to its own species is
    bit-identical to one that does not (measured), so this test would pass on
    the very defect it exists to catch.
    """
    paths_chunked = _build(
        tmp_path, species_chunk=1, out_dir=tmp_path / "chunked"
    )
    paths_whole = _build(
        tmp_path, species_chunk=1000, out_dir=tmp_path / "whole"
    )
    a = load_prior(next(p for p in paths_chunked if "full" in p.name))
    b = load_prior(next(p for p in paths_whole if "full" in p.name))

    assert a.cells == b.cells
    assert a.species_keys.tolist() == b.species_keys.tolist()
    assert np.array_equal(a.dense(), b.dense())


def test_chunk_boundaries_are_deterministic_across_runs(tmp_path):
    """The species axis handed to the backoff is SORTED, not set-ordered.

    `to_tensor` sorts the tensor's own axis, so an unsorted species list
    changes no stored value and no end-to-end comparison can see it. What it
    changes is which species land in which CHUNK -- and a set's iteration
    order depends on the hash seed and on insertion history. The artifact
    would then be reproducible only within a single interpreter run, which is
    not reproducibility at all for a released dataset.

    Asserted on the function that derives the axis rather than on the tensor,
    because the tensor is precisely where the evidence is destroyed.
    """
    table = assign_cells(read_occurrences(_archive(tmp_path)), 4)
    counts = aggregate_counts(mark_guards_applied(table))
    species = build_prior.species_axis(counts)
    assert species == sorted(species)
    assert species == sorted(set(ALL_SPECIES))
    # Repeated derivation is identical, which a set comprehension would not
    # guarantee across processes.
    assert species == build_prior.species_axis(counts)


def test_the_projected_peak_is_reported_before_the_expensive_stage(tmp_path, caplog):
    """The operator learns the cost BEFORE the hours, not after an OOM.

    The projection is from MEASURED scaling (~160 bytes/context, stable
    across a 20x range), and it is logged before `estimate_over_full_grid`
    rather than after, because after is where the OOM happens.

    Deliberately NOT a guard. Refusing to build above some threshold would
    substitute this script's guess for the operator's knowledge of their own
    machine; what the script owes them is the number.
    """
    with caplog.at_level(logging.INFO, logger="birdology.prior.build"):
        _build(tmp_path)
    grid = [r for r in caplog.records if "grid" in r.getMessage()]
    assert grid, "the grid size was never reported"
    assert "projected peak" in grid[0].getMessage()


def test_the_peak_projection_scales_with_the_grid(tmp_path):
    """It is a function of the grid, not a constant dressed up as one."""
    small = build_prior.project_peak_bytes(10, 10)
    big = build_prior.project_peak_bytes(20, 10)
    assert big == 2 * small
    # ...and it follows WEEKS_PER_YEAR, so a documented week window would
    # change the projection rather than leaving it stale.
    assert small == 10 * 10 * WEEKS_PER_YEAR * build_prior.MEASURED_BYTES_PER_CONTEXT


def test_the_memory_constant_is_not_below_the_worst_measurement():
    """I2: the constant must not drift back below what was actually measured.

    The previous 160 B/ctx was fitted to two points at ONE grid shape and
    under-predicted real builds by 26-41% -- in the direction that tells the
    operator a build is CHEAPER than it is, which is precisely the OOM the
    logging exists to prevent. Re-fitted across three scales and two fill
    fractions, the worst observed cost was
    `WORST_MEASURED_BYTES_PER_CONTEXT` B/ctx (net of the ~80 MB interpreter
    baseline).

    Asserted against the RECORDED MEASUREMENT rather than against itself, so a
    future edit that rounds to the mean instead of up has to change the
    measurement too -- which is a visible, arguable act rather than a silent
    one. `>=` rather than `==`: a re-measurement on slower-scaling hardware may
    legitimately raise the constant, and over-estimating costs a bigger node
    while under-estimating costs the whole run.

    M2: THE SECOND CLAUSE USED TO BE `WORST > 160` AND THAT WAS A TRIPWIRE,
    NOT AN ANCHOR. Lowering both numbers to 161/161.0 -- one above it -- left
    this suite green and restored the under-prediction in full. The clause is
    gone, and `WORST_MEASURED_BYTES_PER_CONTEXT` is now READ from
    `scripts/measurements/measure_peak_results.json` rather than typed beside
    the constant, so there is no second number to lower in step. That
    derivation is what
    `test_the_recorded_worst_case_is_READ_FROM_the_measurement_file` pins;
    this test is left to the one comparison it can honestly make.
    """
    assert build_prior.MEASURED_BYTES_PER_CONTEXT >= (
        build_prior.WORST_MEASURED_BYTES_PER_CONTEXT
    ), (
        "MEASURED_BYTES_PER_CONTEXT is below the worst measured cost, so the "
        "operator is told a build is cheaper than the measurements say: this "
        "is the direction that under-warns"
    )


def test_the_projection_states_that_it_excludes_the_interpreter_baseline():
    """The figure is NET, and a caller sizing a node must be told so.

    An operator reading `~0.21 GB` for res 3 and provisioning 0.25 GB would
    OOM on the interpreter alone. The exclusion is a property of the number,
    so it belongs in the docstring of the thing that produces it, and it is
    asserted here so a future rewrite cannot drop it silently.
    """
    assert "baseline" in (build_prior.project_peak_bytes.__doc__ or ""), (
        "`project_peak_bytes` no longer states that its figure excludes the "
        "interpreter baseline"
    )
    assert "EXCLUDES" in (build_prior.project_peak_bytes.__doc__ or "")


def test_the_cell_axis_is_sorted_too(tmp_path):
    """Its own test: two axes, two functions, two independent regressions.

    A sorted species axis and a set-ordered cell axis is still not
    reproducible across processes, and the cell axis is the one that reaches
    thousands of entries at national scale.
    """
    table = assign_cells(read_occurrences(_archive(tmp_path)), 4)
    counts = aggregate_counts(mark_guards_applied(table))
    cells = build_prior.cell_axis(counts)
    assert cells == sorted(cells)
    assert len(cells) == len(POINTS)


def test_a_species_chunk_below_one_is_rejected(tmp_path):
    """A chunk of 0 would emit an EMPTY prior rather than fail."""
    ratios = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["c"], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="species_chunk must be >= 1"):
        estimate_over_full_grid(ratios, [1], ["c"], species_chunk=0)


def test_the_requested_CELL_axis_is_honoured_not_the_observed_one(tmp_path):
    """`cells=` is passed explicitly, and it is the axis that is used.

    HONEST NOTE ON WHAT THIS DOES AND DOES NOT PROVE, because the difference
    matters for how much the `cells=` half of T10-R1 is worth. In the real
    build the requested cell set and the backoff's observed-axes DEFAULT are
    the SAME set: both are derived from the same `counts` table. Measured --
    removing `cells=` from `build_from_archive` leaves the whole end-to-end
    suite green, because the two axes coincide. The `weeks=` half is not like
    that at all: omitting it collapses 52 weeks to 3 and two assertions die.

    So `cells=` is belt-and-braces in the pipeline as it stands, and this
    test pins the property at the level where it IS observable: the function
    honours the cell axis it is GIVEN rather than the one it can infer. That
    is what makes the axis a decision the build made rather than a side
    effect of which rows survived the guards, and it is what a future caller
    building over a fixed national cell set would rely on.
    """
    cph = h3.latlng_to_cell(*POINTS[0], 4)
    unobserved_cell = h3.latlng_to_cell(*POINTS[3], 4)
    assert cph != unobserved_cell

    ratios = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([cph], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
        }
    )
    out = estimate_over_full_grid(ratios, [1], [cph, unobserved_cell])
    emitted = {r["cell"] for r in out.to_pylist()}
    assert emitted == {cph, unobserved_cell}
    assert out.num_rows == 2 * WEEKS_PER_YEAR


def test_the_full_week_grid_is_honoured_even_where_nothing_was_observed():
    """The `weeks=` half, at the function level: 52 rows per cell, always.

    The end-to-end assertions catch this too, but at the level of a value
    read back through `lookup`. Here the shape itself is the claim, which is
    what distinguishes "the grid was requested" from "the grid happened to
    come out right".
    """
    cph = h3.latlng_to_cell(*POINTS[0], 4)
    ratios = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([cph], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
        }
    )
    out = estimate_over_full_grid(ratios, [1], [cph])
    assert sorted(r["week"] for r in out.to_pylist()) == list(
        range(1, WEEKS_PER_YEAR + 1)
    )


# --------------------------------------------------------------------------
# Provenance and licence.
# --------------------------------------------------------------------------


def test_the_week_axis_follows_WEEKS_PER_YEAR_not_a_literal_52(monkeypatch):
    """The week stride has ONE definition, in `indexing`.

    `WEEKS_PER_YEAR` is 52 today, so a hard-coded 52 produces identical
    numbers and NO value-based test can see the difference. The property is
    about provenance rather than value: the grid the build packs and the
    stride the tensor indexes by must be the same symbol, so that changing
    one changes both. A literal here would leave the build packing a 52-week
    grid into a tensor indexed by something else -- columns silently off by
    a whole year's worth of stride, with every value present and plausible.

    Checked by REBINDING the symbol the script resolves at call time. That is
    the only way to make today's coincidence stop being a coincidence.
    """
    monkeypatch.setattr(build_prior, "WEEKS_PER_YEAR", 8)
    cell = "8a1fb46622dffff"
    ratios = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([cell], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
        }
    )
    out = estimate_over_full_grid(ratios, [1], [cell])
    assert sorted(r["week"] for r in out.to_pylist()) == list(range(1, 9)), (
        "the week grid did not follow WEEKS_PER_YEAR, so it is a literal"
    )


def test_an_empty_doi_refuses_to_write_anything(tmp_path):
    """`save_prior` refuses, and the refusal reaches the caller unmodified.

    The build script has no try/except, so the artifact directory must be
    left with nothing in it: a partial release under a normal-looking name is
    the failure mode the refusal exists to prevent.
    """
    out = tmp_path / "priors"
    with pytest.raises(ValueError, match="empty gbif_doi"):
        _build(tmp_path, gbif_doi="", out_dir=out)
    assert not out.exists() or not list(out.glob("*.npz"))


def test_every_sidecar_carries_the_licence_and_an_attribution(tmp_path):
    """CC BY obliges attribution, and a control is released under it too."""
    import json

    paths = _build(tmp_path)
    for p in paths:
        meta = json.loads(p.with_suffix(".json").read_text(encoding="utf-8"))
        assert meta["licence"] == LICENCE
        assert meta["gbif_doi"] == DOI
        assert DOI in meta["attribution"]
        assert LICENCE in meta["attribution"]


def test_artifact_paths_come_from_the_out_dir_parameter(tmp_path):
    """No hardcoded cluster path anywhere on the write path."""
    out = tmp_path / "deeply" / "nested" / "elsewhere"
    paths = _build(tmp_path, out_dir=out)
    assert all(p.parent == out for p in paths)


# --------------------------------------------------------------------------
# T10-R3: the CLI obligations.
# --------------------------------------------------------------------------


def test_the_cli_requires_a_split_decision(capsys):
    """Neither --test-split nor --no-test-split is a hard error.

    `drop_test_contributor_cells` returns early and SILENTLY on an empty
    split, so an un-wired evaluation split produces an unguarded prior that
    is byte-shaped exactly like a guarded one. This argument group is the
    only automated detector of the highest-consequence failure in the plan.
    """
    with pytest.raises(SystemExit):
        build_prior._parse_args(
            ["--archive", "a.zip", "--source", "dof", "--gbif-doi", DOI]
        )
    assert "test-split" in capsys.readouterr().err


def test_the_cli_refuses_both_split_flags_at_once(capsys):
    with pytest.raises(SystemExit):
        build_prior._parse_args(
            [
                "--archive", "a.zip", "--source", "dof", "--gbif-doi", DOI,
                "--test-split", "s.csv", "--no-test-split",
            ]
        )
    assert "not allowed with" in capsys.readouterr().err


def test_the_cli_requires_a_gbif_doi(capsys):
    """Required at the CLI, not merely at the write boundary.

    `save_prior` refuses an empty DOI, so a missing one would fail anyway --
    but only after the whole pipeline has run. Requiring it up front turns a
    multi-hour national build that dies at the write into an instant usage
    error.
    """
    with pytest.raises(SystemExit):
        build_prior._parse_args(
            ["--archive", "a.zip", "--source", "dof", "--no-test-split"]
        )
    assert "gbif-doi" in capsys.readouterr().err


@pytest.mark.parametrize("blank", ["", " ", "   ", "\t", "\n", " \t \n "])
def test_the_cli_refuses_a_BLANK_gbif_doi(capsys, blank):
    """C2: `required=True` requires the FLAG, not a VALUE.

    Reproduced before the fix: `--gbif-doi ''` and `--gbif-doi '   '` were both
    ACCEPTED, and the whitespace one is the dangerous half -- `save_prior`
    refused an EMPTY doi, but `'   '` is not empty, so all five artifacts wrote
    with sidecar `gbif_doi: '   '` and attribution
    `Derived from GBIF occurrence download    .` That is a CC BY 4.0
    attribution violation in a published artifact.

    The operator path is completely ordinary:

        python scripts/build_prior.py --gbif-doi "$DOI" ...   # $DOI unset

    `test_the_cli_requires_a_gbif_doi` only tests flag ABSENCE, which is why it
    caught neither. Parametrised over several whitespace spellings because a
    guard written as `value != " "` would pass on one of them.
    """
    with pytest.raises(SystemExit):
        build_prior._parse_args(
            [
                "--archive", "a.zip", "--source", "dof", "--no-test-split",
                "--gbif-doi", blank,
            ]
        )
    err = capsys.readouterr().err
    assert "blank or whitespace-only" in err, err


def test_the_cli_refuses_a_gbif_doi_that_is_not_a_DOI(capsys):
    """A GBIF download KEY in the DOI field is a mis-attributed release.

    DEVIATION, reported as such: the findings file explicitly did not rule on
    a shape check. Implemented because a GBIF download has BOTH a key
    (`0012345-230530130807322`) and a DOI (`10.15468/dl.abc123`), the download
    page shows both, and only the DOI is citable. A key in this field produces
    a well-formed, permanently mis-attributed release that no reader of the
    artifact can detect -- the same class of failure as the blank, one step
    less obvious. It is a SHAPE check only: nothing here can verify the DOI
    names the download this archive actually came from, so it catches the wrong
    KIND of identifier, not a wrong one.
    """
    for bad in ("0012345-230530130807322", "dof.zip", "GBIF", "11.15468/dl.x"):
        with pytest.raises(SystemExit):
            build_prior._parse_args(
                [
                    "--archive", "a.zip", "--source", "dof", "--no-test-split",
                    "--gbif-doi", bad,
                ]
            )
        assert "is not shaped like a DOI" in capsys.readouterr().err


def test_the_cli_accepts_every_documented_doi_spelling():
    """Guards the two checks above against being too strict to use.

    Without this, a shape check tightened to `value.startswith("10.15468/")`
    would pass both rejection tests while refusing a legitimate DOI copied as
    a URL from the GBIF download page -- and the pressure would be to remove
    the check rather than fix it.
    """
    for good in (
        "10.15468/dl.abc123",
        "doi:10.15468/dl.abc123",
        "https://doi.org/10.15468/dl.abc123",
        "http://dx.doi.org/10.15468/dl.abc123",
    ):
        args = build_prior._parse_args(
            [
                "--archive", "a.zip", "--source", "dof", "--no-test-split",
                "--gbif-doi", good,
            ]
        )
        # Accepted VERBATIM: not stripped, not canonicalised. The sidecar must
        # carry the string the operator attested to.
        assert args.gbif_doi == good


def test_the_cli_REFUSES_a_doi_with_surrounding_whitespace(capsys):
    """M3's no-repair half: the value is NEVER stripped to make it fit.

    SEPARATE from the message test below, and the separation is the point. A
    message improvement is one edit away from being bought by
    `value = value.strip()`, which would satisfy any test that only asked
    "does a whitespace DOI end up working". It must not work: the sidecar has
    to carry the exact string the operator attested to, because silently
    repairing an attribution field means the artifact cites something the
    build was never told. So this pins the REFUSAL and
    `test_the_doi_shape_message_names_whitespace_as_a_probable_cause` pins
    the WORDING, and no single edit can satisfy both wrongly.
    """
    for bad in _WHITESPACE_DOIS:
        with pytest.raises(SystemExit):
            build_prior._parse_args(
                [
                    "--archive", "a.zip", "--source", "dof", "--no-test-split",
                    "--gbif-doi", bad,
                ]
            )
        assert "is not shaped like a DOI" in capsys.readouterr().err


def test_the_doi_shape_message_names_whitespace_as_a_probable_cause(capsys):
    """M3: the three whitespace spellings are refused for a NAMED reason.

    `'10.15468/dl.bq3fxc '`, `' 10.15468/dl.bq3fxc'` and
    `'DOI: 10.15468/dl.bq3fxc'` all land in the shape check, and 'is not
    shaped like a DOI' sends the operator hunting for a malformed identifier
    when the identifier is fine and a space is not. Pasting a DOI off a
    citation line is the most likely operator input after the clean one.

    THE VALUE IS STILL NOT STRIPPED, and that is deliberate: no silent repair
    at a boundary, because the sidecar must carry the exact string the
    operator attested to and quietly repairing an attribution field means the
    artifact cites something the build was never told.
    `test_save_accepts_a_doi_with_INCIDENTAL_surrounding_whitespace` pins the
    byte-for-byte round trip from the other side, so this improvement cannot
    become a provenance change. Only the MESSAGE changes.
    """
    for bad in _WHITESPACE_DOIS:
        with pytest.raises(SystemExit):
            build_prior._parse_args(
                [
                    "--archive", "a.zip", "--source", "dof", "--no-test-split",
                    "--gbif-doi", bad,
                ]
            )
        err = capsys.readouterr().err
        assert "is not shaped like a DOI" in err, err
        assert "WHITESPACE" in err, (
            f"{bad!r} is refused without naming whitespace as a probable "
            f"cause, so the operator is sent hunting for a malformed "
            f"identifier: {err}"
        )
        # And the message shows what was actually received, repr'd, so the
        # invisible character is visible in the terminal.
        assert repr(bad) in err, err


def test_a_measurement_row_inconsistent_with_its_own_numbers_is_REFUSED(tmp_path):
    """M2: committing the file must not just MOVE the retyping into the JSON.

    Committing `measure_peak_results.json` stops a coordinated edit retyping
    two numbers in the source -- but on its own, `bytes_per_context_net:
    161.0` in the JSON would pin the constant at 161 with nothing able to
    object. The stated ratio is redundant with `net_peak_bytes / contexts`,
    and `contexts` is redundant with the grid shape, so the file is checked
    against itself: falsifying the pin now requires fabricating a coherent
    measurement rather than editing one number.

    REFUSED, not recomputed. A row whose stated ratio disagrees with its own
    peak RSS is a row of unknown provenance, and preferring the recomputed
    value would be the silent repair at a boundary the house rules forbid.
    """
    row = _measurement_row(10, 20, 3_000_000_000)

    lied = tmp_path / "lied.json"
    lied.write_text(
        json.dumps([{**row, "bytes_per_context_net": 161.0}]), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="does not follow from"):
        build_prior.validate_measurement_file(lied)

    # ...and inflating `contexts` to make a low ratio arithmetically true is
    # caught by the grid, which is the other redundancy.
    stretched = tmp_path / "stretched.json"
    stretched.write_text(
        json.dumps(
            [
                {
                    **row,
                    "contexts": row["contexts"] * 2,
                    "bytes_per_context_net": row["net_peak_bytes"]
                    / (row["contexts"] * 2),
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="disagrees with its own grid|contains"):
        build_prior.validate_measurement_file(stretched)


def test_the_committed_measurement_is_itself_self_consistent():
    """And the file actually shipped passes that check.

    Without this, the consistency check above could be asserted entirely
    against synthetic fixtures while the committed file quietly violated it --
    a test that cannot fail on the artifact it exists to protect.

    It therefore runs `validate_measurement_file` over the COMMITTED file and
    checks each row's arithmetic here as well. The previous form asserted
    `worst_measured_bytes_per_context() == WORST_MEASURED_BYTES_PER_CONTEXT`,
    which is the function compared against a constant that IS that function's
    return value -- both reading the same file in the same process. It could
    only fail if the file changed mid-session, so it could not fail on the
    thing its own docstring claimed it protected.
    """
    build_prior.validate_measurement_file()

    rows = json.loads(
        build_prior.MEASURE_PEAK_RESULTS.read_text(encoding="utf-8")
    )
    assert rows, "the committed measurement file is empty, so it pins nothing"

    # Independently of the module's own checker: every row's stated ratio must
    # follow from its own raw observations, and its context count from its own
    # grid. Asserted here so a weakening of `_check_measurement_row` cannot
    # take this test with it.
    for i, row in enumerate(rows):
        contexts = row["contexts"]
        assert contexts == (
            row["n_species"] * row["n_cells"] * build_prior.WEEKS_PER_YEAR
        ), f"row {i}: contexts disagrees with its own grid"
        assert row["bytes_per_context_net"] == pytest.approx(
            row["net_peak_bytes"] / contexts
        ), f"row {i}: stated ratio does not follow from peak RSS over contexts"

    # ...and the constant really is the max over those rows.
    assert build_prior.WORST_MEASURED_BYTES_PER_CONTEXT == pytest.approx(
        max(float(r["bytes_per_context_net"]) for r in rows)
    )


def test_a_tampered_measurement_file_fails_a_NAMED_test_not_collection(tmp_path):
    """The row check must be reachable by a test, not raised at import.

    `worst_measured_bytes_per_context` runs at module import. If it also
    validated rows, a mutant that retypes a ratio in the committed JSON would
    raise during import, pytest would report a COLLECTION ERROR naming no
    test, and the mutation harness -- which attributes a kill to the test that
    caught it -- would score the row MISLABELLED. The guard would be working
    and the evidence that it works would be gone.

    So validation lives in `validate_measurement_file`, called from `main`.
    This asserts both halves: importing with a tampered file still SUCCEEDS
    (so collection survives), and the validator REFUSES it.
    """
    tampered = tmp_path / "tampered.json"
    rows = json.loads(
        build_prior.MEASURE_PEAK_RESULTS.read_text(encoding="utf-8")
    )
    rows[0]["bytes_per_context_net"] = 158.0
    tampered.write_text(json.dumps(rows), encoding="utf-8")

    # The import-time derivation tolerates it: it reads the number, it does
    # not audit it. This is what keeps collection alive -- no reload is needed
    # to show it, and reloading the module here would rebind state other tests
    # in this file hold references to.
    assert build_prior.worst_measured_bytes_per_context(tampered) == pytest.approx(
        max(float(r["bytes_per_context_net"]) for r in rows)
    )

    # The validator is where it dies, with a message naming the disagreement.
    with pytest.raises(ValueError, match="does not follow from"):
        build_prior.validate_measurement_file(tampered)


def test_save_prior_refuses_a_WHITESPACE_ONLY_doi_from_any_caller(tmp_path):
    """C2's library half: the CLI guard is not the only thing standing here.

    `non_blank_doi` protects the command line; this protects every other caller
    of the library, including a notebook, a future sweep driver, and this
    plan's own controls. Independently load-bearing: neither is redundant, and
    the harness has one mutant per site for exactly that reason.
    """
    out = tmp_path / "priors"
    with pytest.raises(ValueError, match="empty gbif_doi"):
        _build(tmp_path, gbif_doi="   ", out_dir=out)
    assert not out.exists() or not list(out.glob("*.npz"))


def test_the_cli_accepts_either_split_flag_alone(tmp_path):
    args = build_prior._parse_args(
        ["--archive", "a.zip", "--source", "dof", "--gbif-doi", DOI,
         "--no-test-split"]
    )
    assert args.no_test_split is True and args.test_split is None
    args = build_prior._parse_args(
        ["--archive", "a.zip", "--source", "dof", "--gbif-doi", DOI,
         "--test-split", "s.csv"]
    )
    assert args.no_test_split is False and args.test_split == Path("s.csv")


def test_main_configures_logging_before_any_pipeline_call(tmp_path, monkeypatch):
    """Obligation 4: WARNING must reach the console.

    The zero-match warning is the shared symptom of four silent failure
    modes, and an unconfigured root logger DISCARDS it -- re-silencing all
    four. The assertion is that `basicConfig` has been called by the time the
    first pipeline call happens, not merely somewhere in `main`: configuring
    it afterwards would lose exactly the warnings the build emits.
    """
    seen = []
    real = logging.basicConfig

    def spy(**kwargs):
        seen.append(("basicConfig", kwargs.get("level")))
        real(**kwargs)

    monkeypatch.setattr(logging, "basicConfig", spy)
    monkeypatch.setattr(
        build_prior,
        "build_from_archive",
        lambda *a, **k: seen.append(("build", None)) or [],
    )
    build_prior.main(
        [
            "--archive", str(_archive(tmp_path)), "--source", "dof",
            "--resolutions", "4", "--out", str(tmp_path / "o"),
            "--gbif-doi", DOI, "--no-test-split",
        ]
    )
    assert seen[0] == ("basicConfig", logging.INFO)
    assert ("build", None) in seen


def test_main_sweeps_every_requested_resolution(tmp_path):
    """The sweep is over `--resolutions`, and each is a separate artifact set."""
    out = tmp_path / "priors"
    build_prior.main(
        [
            "--archive", str(_archive(tmp_path)), "--source", "dof",
            "--resolutions", "3", "4", "--out", str(out),
            "--gbif-doi", DOI, "--no-test-split",
        ]
    )
    assert (out / "prior_dof_r3_full.npz").exists()
    assert (out / "prior_dof_r4_full.npz").exists()
    # Distinct grids, not the same artifact written twice.
    assert load_prior(out / "prior_dof_r3_full.npz").cells != load_prior(
        out / "prior_dof_r4_full.npz"
    ).cells


# --------------------------------------------------------------------------
# T10-R3, obligation 1: the split's cells are RE-DERIVED per resolution.
# --------------------------------------------------------------------------


def _split_csv(tmp_path, rows, name="split.csv"):
    path = tmp_path / name
    lines = ["lat,lon,week,recorded_by"]
    lines += [f"{lat},{lon},{week},{who}" for lat, lon, week, who in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_the_split_cell_is_derived_at_each_resolution(tmp_path):
    """A `TestRecording`'s cell is fixed at the resolution it was built at.

    Re-deriving is what makes the sweep possible at all: the SAME split row
    must produce a res-3 cell for the res-3 build and a res-5 cell for the
    res-5 build. One pre-computed cell reused across the sweep matches
    nothing at every other resolution -- and since Task 4 that RAISES.
    """
    split = load_test_split(_split_csv(tmp_path, [(55.6867, 12.5700, 2, "obs")]))
    for resolution in (3, 4, 5):
        recording = recordings_at_resolution(split, resolution)[0]
        assert h3.get_resolution(recording.cell) == resolution
        assert recording.cell == h3.latlng_to_cell(55.6867, 12.5700, resolution)


def test_a_sweep_with_a_split_succeeds_at_every_resolution(tmp_path):
    """The whole point of obligation 1, end to end.

    With cells re-derived per resolution the split guards every artifact of
    the sweep. Before the fix this raised on every resolution except the
    split's own.
    """
    out = tmp_path / "priors"
    build_prior.main(
        [
            "--archive", str(_archive(tmp_path)), "--source", "dof",
            "--resolutions", "3", "4", "5", "--out", str(out),
            "--gbif-doi", DOI,
            "--test-split", str(
                _split_csv(tmp_path, [(55.6867, 12.5700, 2, "obs")])
            ),
        ]
    )
    for resolution in (3, 4, 5):
        assert (out / f"prior_dof_r{resolution}_full.npz").exists()


def test_a_mismatched_resolution_FAILS_the_artifact_rather_than_writing_it(
    tmp_path,
):
    """Obligation 2: a guard ValueError propagates; nothing is written.

    A `TestRecording` built at res 5 against a res-3 occurrence table is the
    exact shape obligation 1 exists to prevent. Catching it to keep a sweep
    going would leave a leak-contaminated artifact on disk under a name
    indistinguishable from a correct one -- worse than the pre-fix silence,
    because it looks deliberate.
    """
    out = tmp_path / "priors"
    mismatched = recordings_at_resolution(
        load_test_split(_split_csv(tmp_path, [(55.6867, 12.5700, 2, "obs")])), 5
    )
    assert h3.get_resolution(mismatched[0].cell) == 5

    with pytest.raises(ValueError, match="H3 resolution mismatch"):
        _build(tmp_path, resolution=3, test_recordings=mismatched, out_dir=out)
    assert not out.exists() or not list(out.glob("*.npz"))


def _build_script_tree():
    import ast

    return ast.parse(Path(build_prior.__file__).read_text(encoding="utf-8"))


def _function(tree, name):
    import ast

    return next(
        n
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
    )


def _reachable_raise(node) -> bool:
    """Whether *node*'s own control flow can reach a `raise`.

    RECURSIVE on purpose, rather than a single `ast.walk` with a filter, so
    both evasions are closed at ANY nesting depth:

    * E1 -- does not descend into a nested `def`/`class`/`lambda`. A `raise`
      inside a function nobody calls is not a re-raise, and `ast.walk`
      descended into it.
    * E4 -- does not descend into the body of a statically-false `if`/`while`.
      `if False: raise` is unreachable code, and a one-level prune would miss
      `if True:\\n    if False:\\n        raise`.

    Only CONSTANT-false conditions are pruned. This is a structural check, not
    a reachability analysis; `if False:` and `if 0:` are the forms a bypass
    actually takes, and anything cleverer would be guessing.
    """
    import ast

    for child in ast.iter_child_nodes(node):
        if isinstance(
            child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
        ):
            continue
        if isinstance(child, ast.Raise):
            return True
        if isinstance(child, (ast.If, ast.While)):
            test = child.test
            if isinstance(test, ast.Constant) and not test.value:
                # Dead body; the `else` branch is the live one.
                if any(_reachable_raise_stmt(s) for s in child.orelse):
                    return True
                continue
        if _reachable_raise(child):
            return True
    return False


def _reachable_raise_stmt(stmt) -> bool:
    """`_reachable_raise` including *stmt* itself, for a statement list."""
    import ast

    if isinstance(stmt, ast.Raise):
        return True
    if isinstance(
        stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
    ):
        return False
    return _reachable_raise(stmt)


def test_no_except_handler_in_the_build_script_swallows_anything():
    """Obligation 2, structurally: every handler RE-RAISES, reachably.

    A blanket "no `except` anywhere" grep is the wrong check here and was
    tried first: `load_test_split` legitimately catches the `TypeError` /
    `ValueError` that `float()` and `int()` raise on a malformed field, in
    order to re-raise a `ValueError` that NAMES the split, the line and what
    the malformed field would have done to the prior. That is the house
    error contract, not a swallow.

    What must never happen is a handler that returns, passes, logs, or
    otherwise lets the build continue past a guard error. Checked with the
    AST rather than by grep, so it sees the handler BODIES rather than the
    keyword.

    TWO EVASIONS ARE CLOSED HERE, both executed against the previous version:

    * E1 -- a handler whose body only DEFINES a nested function that raises.
      `ast.walk` descended into the nested `def`, so unreachable code in an
      uncalled function counted as a re-raise.
    * E4 -- `if False: raise` inside a swallowing handler, at any depth.

    Both through `_reachable_raise`, which is recursive so neither can be
    re-opened by adding a level of nesting.
    """
    import ast

    tree = _build_script_tree()
    handlers = [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]
    assert handlers, "the AST walk found nothing; the check would be vacuous"
    for handler in handlers:
        assert _reachable_raise(handler), (
            f"except handler at line {handler.lineno} does not re-raise from "
            "its own reachable body (a nested function definition or a "
            "statically-dead branch does not count)"
        )


#: Names whose CALL suppresses an exception without producing an
#: `ast.ExceptHandler` node at all. EVASION E2, executed and confirmed to pass
#: the previous check: `with contextlib.suppress(ValueError): <stage>` around
#: any pipeline call swallows a guard error with no `except` keyword anywhere
#: in the file. Matched on the attribute/name spelling rather than by import
#: analysis, because both `contextlib.suppress(...)` and a bare
#: `suppress(...)` from a `from contextlib import suppress` are the same defect.
EXCEPTION_SUPPRESSING_CALLS = ("suppress",)


def test_the_pipeline_call_path_has_no_try_at_all():
    """The stages are not wrapped, not even in a bare `try/finally`.

    `build_from_archive` orchestrates every guard in the plan. A handler
    around any of them -- however well-intentioned -- would convert a guard
    error into something the loop could survive, and the artifact it then
    wrote would be leak-contaminated under a normal-looking name.

    THE OLD CHECK WAS ASYMMETRIC AND THAT WAS THE HOLE. It forbade
    `ExceptHandler` in `build_from_archive` and `Try` in `main`, so three of
    the five executed evasions walked straight through:

    * E3 -- a `return` added to `build_from_archive`'s EXISTING `finally`
      block. A `return` in a `finally` DISCARDS any in-flight exception,
      including a guard `ValueError`, and a finally-only `Try` has no
      `ExceptHandler` node to find. This was the serious one, and it is why
      the finally is gone: the logging handler that needed it is gone too
      (C1), so `ast.Try` is now forbidden OUTRIGHT here rather than
      whitelisted in a finally-only form. A whitelist would have had to
      re-admit the exact node type the evasion used.
    * E2 -- `contextlib.suppress(ValueError)`, which produces no
      `ExceptHandler` at all.
    * E5 -- the loop body moved into a helper that catches `ValueError` and
      raises `SystemExit(0)`: obligation 2's forbidden behaviour (a failed
      artifact skipped, exit 0) dressed in the house error contract. Covered
      by the module-wide `SystemExit` check below.
    """
    import ast

    tree = _build_script_tree()
    build = _function(tree, "build_from_archive")

    # E3: no `Try` of ANY shape, finally-only included.
    tries = [n for n in ast.walk(build) if isinstance(n, ast.Try)]
    assert not tries, (
        f"`build_from_archive` contains {len(tries)} `try` statement(s) at "
        f"line(s) {[n.lineno for n in tries]}. Even a bare `try/finally` is "
        "forbidden: a `return` added to a `finally` silently DISCARDS every "
        "in-flight exception, including the guard ValueError this function "
        "exists to propagate, and it produces no `except` node for a "
        "handler-shaped check to find"
    )

    # ...and `main`'s resolution loop is unwrapped too, so a failed artifact
    # exits non-zero rather than being skipped.
    main = _function(tree, "main")
    assert not [n for n in ast.walk(main) if isinstance(n, ast.Try)]

    # E2: no exception-suppressing context manager anywhere in the module. Not
    # scoped to `build_from_archive`: a `suppress` around the call in `main` is
    # the same defect one frame out, and the build script has no legitimate use
    # for one.
    suppressors = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id in EXCEPTION_SUPPRESSING_CALLS)
            or (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in EXCEPTION_SUPPRESSING_CALLS
            )
        )
    ]
    assert not suppressors, (
        f"the build script calls an exception-suppressing context manager at "
        f"line(s) {[n.lineno for n in suppressors]}; `contextlib.suppress` "
        "swallows a guard error while producing no `except` node at all"
    )


def test_the_build_script_never_converts_a_guard_error_into_a_clean_exit():
    """EVASION E5: `except ValueError: raise SystemExit(0)` is still a swallow.

    Executed against the previous check and it passed BOTH AST tests: move the
    resolution-loop body into a helper, catch `ValueError` there, and
    `raise SystemExit(0)`. The handler re-raises, so the handler check is
    satisfied; the helper is neither `build_from_archive` nor `main`, so the
    node-type check never looks at it. What reaches the operator is exit 0
    with a failed artifact skipped -- obligation 2's forbidden behaviour,
    dressed in this plan's own error contract.

    `SystemExit` and `sys.exit` are therefore forbidden in the build script
    entirely. It has no legitimate use for either: `main` returns `None`, an
    uncaught `ValueError` gives a non-zero exit through the interpreter's
    normal path, and argparse's own `SystemExit` comes from argparse rather
    than from this file.
    """
    import ast

    tree = _build_script_tree()
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Raise):
            exc = node.exc
            target = exc.func if isinstance(exc, ast.Call) else exc
            if isinstance(target, ast.Name) and target.id == "SystemExit":
                offenders.append(node.lineno)
            if isinstance(target, ast.Attribute) and target.attr == "SystemExit":
                offenders.append(node.lineno)
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "exit":
                offenders.append(node.lineno)
            if isinstance(func, ast.Name) and func.id == "exit":
                offenders.append(node.lineno)
    assert not offenders, (
        f"the build script raises SystemExit or calls sys.exit at line(s) "
        f"{sorted(offenders)}. A guard ValueError caught and re-raised as "
        "`SystemExit(0)` satisfies every handler check in this file while "
        "skipping a failed artifact and reporting success"
    )


def test_the_ast_evasion_checks_are_not_vacuous():
    """Each check above must FAIL on the evasion it claims to close.

    Without this, a check that walked the wrong tree, matched the wrong node
    type, or was scoped to a function that no longer exists would pass exactly
    as it does now. Every evasion is constructed as source here and parsed, so
    the assertion is about the CHECK rather than about today's build script.
    """
    import ast

    # E1: handler whose body only defines a nested function that raises.
    e1 = ast.parse(
        "def f():\n"
        "    try:\n"
        "        g()\n"
        "    except ValueError:\n"
        "        def inner():\n"
        "            raise RuntimeError('never called')\n"
        "        return []\n"
    )
    handler = next(n for n in ast.walk(e1) if isinstance(n, ast.ExceptHandler))
    assert any(isinstance(n, ast.Raise) for n in ast.walk(handler)), (
        "precondition: a plain ast.walk DOES find the nested raise, which is "
        "what made E1 an evasion"
    )
    assert not _reachable_raise(
        handler
    ), "E1 is not closed: a nested definition's raise still counts as a re-raise"

    # E4: `if False: raise` inside a swallowing handler.
    e4 = ast.parse(
        "def f():\n"
        "    try:\n"
        "        g()\n"
        "    except ValueError:\n"
        "        if False:\n"
        "            raise\n"
        "        return []\n"
    )
    handler = next(n for n in ast.walk(e4) if isinstance(n, ast.ExceptHandler))
    assert any(isinstance(n, ast.Raise) for n in ast.walk(handler)), (
        "precondition: a plain ast.walk DOES find the dead raise"
    )
    assert not _reachable_raise(
        handler
    ), "E4 is not closed: a statically-dead raise still counts as a re-raise"

    # E4 again, ONE LEVEL DEEPER, because a non-recursive prune would pass the
    # form above and fail this one.
    e4_nested = ast.parse(
        "def f():\n"
        "    try:\n"
        "        g()\n"
        "    except ValueError:\n"
        "        if True:\n"
        "            if False:\n"
        "                raise\n"
        "        return []\n"
    )
    nested = next(n for n in ast.walk(e4_nested) if isinstance(n, ast.ExceptHandler))
    assert not _reachable_raise(
        nested
    ), "E4 is not closed at depth: the check is not recursive"

    # E3: a finally-only Try, which has NO ExceptHandler node.
    e3 = ast.parse(
        "def build_from_archive():\n"
        "    try:\n"
        "        g()\n"
        "    finally:\n"
        "        return []\n"
    )
    build = _function(e3, "build_from_archive")
    assert not [n for n in ast.walk(build) if isinstance(n, ast.ExceptHandler)], (
        "precondition: a finally-only Try really does produce no handler node, "
        "which is what made E3 invisible to the old check"
    )
    assert [n for n in ast.walk(build) if isinstance(n, ast.Try)], (
        "E3 is not closed: the finally-only Try is not detected"
    )

    # E2: contextlib.suppress, in both spellings.
    for source in (
        "with contextlib.suppress(ValueError):\n    g()\n",
        "with suppress(ValueError):\n    g()\n",
    ):
        tree = ast.parse(source)
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)], (
            "precondition: suppress produces no handler node"
        )
        found = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call)
            and (
                (
                    isinstance(n.func, ast.Name)
                    and n.func.id in EXCEPTION_SUPPRESSING_CALLS
                )
                or (
                    isinstance(n.func, ast.Attribute)
                    and n.func.attr in EXCEPTION_SUPPRESSING_CALLS
                )
            )
        ]
        assert found, f"E2 is not closed for {source!r}"

    # E5: a helper that catches ValueError and raises SystemExit(0).
    e5 = ast.parse(
        "def _one(resolution):\n"
        "    try:\n"
        "        build_from_archive(resolution)\n"
        "    except ValueError as exc:\n"
        "        raise SystemExit(0) from exc\n"
    )
    handler = next(n for n in ast.walk(e5) if isinstance(n, ast.ExceptHandler))
    assert any(isinstance(n, ast.Raise) for n in ast.walk(handler)), (
        "precondition: E5's handler DOES re-raise, which is why the handler "
        "check passed on it"
    )
    system_exits = [
        n
        for n in ast.walk(e5)
        if isinstance(n, ast.Raise)
        and isinstance(n.exc, ast.Call)
        and isinstance(n.exc.func, ast.Name)
        and n.exc.func.id == "SystemExit"
    ]
    assert system_exits, "E5 is not closed: raise SystemExit(0) is not detected"


# --------------------------------------------------------------------------
# T10-R3, obligation 3/4: a split that was supplied must BITE.
# --------------------------------------------------------------------------


def test_a_split_that_matches_nothing_fails_the_build(tmp_path):
    """Obligation 4: a supplied split that bit nothing is a BUILD FAILURE.

    A split at the right resolution whose CONTRIBUTOR appears nowhere in the
    archive removes no row and otherwise builds a perfect artifact whose
    leakage guard did nothing. That is the shared symptom of four silent
    failure modes, and since a split WAS supplied it is a failure rather than
    a note.

    The condition is read from `LeakageGuardResult`'s counts, not from a log
    record; `test_the_zero_match_guard_fires_under_every_hostile_logging_config`
    is what proves that.
    """
    out = tmp_path / "priors"
    ghost = recordings_at_resolution(
        load_test_split(
            _split_csv(tmp_path, [(55.6867, 12.5700, 2, "nobody-by-that-name")])
        ),
        4,
    )
    with pytest.raises(ZeroMatchGuardError, match="matched no occurrence"):
        _build(tmp_path, resolution=4, test_recordings=ghost, out_dir=out)
    assert not out.exists() or not list(out.glob("*.npz"))


#: The logging configurations that were REPRODUCED disarming the old
#: handler-and-string-match detector. `basicConfig(level=ERROR)` before `main()`
#: is the one that was executed end to end: 10 artifacts written, exit 0.
#: `logging.disable(CRITICAL)` is strictly stronger; raising the level on
#: leakage's OWN logger is the narrowest. All three must leave the guard armed.
HOSTILE_LOGGING_CONFIGS = ("basicConfig_ERROR", "disable_CRITICAL", "module_logger_off")


@pytest.mark.parametrize("config", HOSTILE_LOGGING_CONFIGS)
def test_the_zero_match_guard_fires_under_every_hostile_logging_config(
    tmp_path, config
):
    """C1, END TO END: no logging configuration can disarm the guard.

    THE REPRODUCED FAILURE. With `logging.basicConfig(level=logging.ERROR)`
    set before `main()` -- one line, exactly what a CI wrapper or a cron
    harness does -- and a test split matching nothing, the previous build
    wrote ALL TEN artifacts (five priors + five sidecars) and returned exit 0.
    `main()`'s own `basicConfig` is a documented no-op once the root logger
    has handlers, so the WARNING the detector scraped never reached it.

    The guard now reads `LeakageGuardResult.zero_match`, two integers returned
    by the function that filtered. These three configurations therefore change
    nothing, and the artifact directory stays EMPTY -- which is the assertion
    that would have caught the original defect, because the old mechanism
    failed by WRITING rather than by raising the wrong error.

    Each config is asserted to have really taken effect (no WARNING is
    delivered), so the test cannot pass vacuously on a run where logging was
    never actually suppressed.
    """
    out = tmp_path / "priors"
    split = _split_csv(tmp_path, [(55.6867, 12.5700, 2, "nobody-by-that-name")])
    argv = [
        "--archive", str(_archive(tmp_path)), "--source", "dof",
        "--resolutions", "4", "--out", str(out),
        "--gbif-doi", DOI, "--test-split", str(split),
    ]

    root = logging.getLogger()
    leak = logging.getLogger("birdology.prior.leakage")
    saved = (root.level, list(root.handlers), leak.level, leak.disabled)
    delivered: list[logging.LogRecord] = []

    class _Spy(logging.Handler):
        def emit(self, record):
            delivered.append(record)

    spy = _Spy(level=logging.WARNING)
    root.addHandler(spy)
    try:
        if config == "basicConfig_ERROR":
            # A handler is already attached (the spy), so `main`'s own
            # `basicConfig` is a no-op -- which is the documented behaviour
            # that made the original evasion work.
            logging.basicConfig(level=logging.ERROR)
            root.setLevel(logging.ERROR)
        elif config == "disable_CRITICAL":
            logging.disable(logging.CRITICAL)
        elif config == "module_logger_off":
            leak.setLevel(logging.CRITICAL)

        with pytest.raises(ZeroMatchGuardError, match="matched no occurrence"):
            build_prior.main(argv)
    finally:
        logging.disable(logging.NOTSET)
        root.removeHandler(spy)
        root.setLevel(saved[0])
        root.handlers[:] = saved[1]
        leak.setLevel(saved[2])
        leak.disabled = saved[3]

    # THE PRECONDITION: the configuration really did suppress the warning the
    # old detector depended on. Without this the test could pass on a run where
    # logging was still working.
    assert not [
        r for r in delivered if "matched zero of" in r.getMessage()
    ], f"{config} did not actually suppress the zero-match WARNING"

    # And NO RELEASE ARTIFACT was written. The original defect's signature
    # was ten files on disk and exit 0, so this is the assertion that
    # discriminates. The PARSE CHECKPOINT is deliberately exempt: it is
    # written BEFORE the guards precisely so a refusal like this one does
    # not cost the multi-hour parse again (job 5345), it is not part of a
    # release (check_manifest never names it), and its survival here is
    # pinned by test_main_writes_the_checkpoint_before_the_guards_run.
    leftovers = (
        [p for p in out.glob("*") if not p.name.startswith("parse_checkpoint_")]
        if out.exists()
        else []
    )
    assert not leftovers, (
        "the build wrote artifacts despite a split that matched nothing -- "
        "this is the exact C1 failure: a complete, publishable release whose "
        "leakage guard removed nothing"
    )


def test_the_build_script_does_not_scrape_logs_for_control_flow():
    """The dead detector is GONE, not kept as a second opinion.

    A dead detector beside a live one is how the next reviewer stops looking
    for a real one, so the handler path is deleted rather than demoted. This
    asserts the deletion structurally: the build script must not define a
    `logging.Handler`, must not attach one, and must not reference the
    leakage module's warning-marker constant at all.
    """
    import ast

    tree = ast.parse(Path(build_prior.__file__).read_text(encoding="utf-8"))

    # Checked on the AST, not by substring: `ZeroMatchGuardError`'s docstring
    # NAMES the deleted mechanism on purpose, so the next reader knows what was
    # tried and why it failed. A grep would force that history out of the file,
    # which is the opposite of what this finding asked for.
    forbidden_attrs = {"addHandler", "removeHandler", "Handler", "LogRecord"}
    offenders = [
        (n.attr, n.lineno)
        for n in ast.walk(tree)
        if isinstance(n, ast.Attribute) and n.attr in forbidden_attrs
    ]
    offenders += [
        (n.id, n.lineno)
        for n in ast.walk(tree)
        if isinstance(n, ast.Name) and n.id in forbidden_attrs
    ]
    assert not offenders, (
        f"the build script still manipulates logging handlers at {offenders}: "
        "the zero-match guard must read returned counts, and a second "
        "log-based opinion beside it is how the dead detector survived three "
        "review passes"
    )

    # ...and it does not import the marker constant the old path scraped.
    imported = {
        alias.name
        for n in ast.walk(tree)
        if isinstance(n, ast.ImportFrom)
        for alias in n.names
    }
    assert "ZERO_MATCH_WARNING_MARKER" not in imported

    handler_classes = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.ClassDef)
        and any(
            (isinstance(b, ast.Attribute) and b.attr == "Handler")
            or (isinstance(b, ast.Name) and b.id == "Handler")
            for b in n.bases
        )
    ]
    assert not handler_classes, "the build script defines a logging.Handler subclass"


def test_the_zero_match_guard_reads_the_guard_results_own_counts():
    """The guard's CONDITION is `LeakageGuardResult.zero_match` and nothing else.

    Pinned at the source level because the alternative -- the build script
    re-deriving the condition from `len(test_recordings)`, as the old
    `and test_recordings` conjunct did -- is behaviourally identical on every
    fixture in this file while being wrong for a split whose rows all
    normalise to one key, or to none.
    """
    source = Path(build_prior.__file__).read_text(encoding="utf-8")
    assert "if guarded.zero_match:" in source, (
        "the escalation is no longer a comparison on the guard's returned "
        "counts"
    )


def test_a_split_that_DOES_match_removes_rows_and_builds(tmp_path):
    """The precondition of the test above: a real match is not a failure.

    Without this, the zero-match check could be firing on everything and the
    test above would still pass. Here the contributor genuinely appears in
    the archive's cell-week, rows are removed, and the build completes.
    """
    archive = _archive(
        tmp_path,
        extra_rows=[
            _row(9001, 1001, 55.6867, 12.5700, "2025-01-07", who="leaky-observer"),
            _row(9002, 1002, 55.6867, 12.5700, "2025-01-07", who="leaky-observer"),
        ],
    )
    leaked = recordings_at_resolution(
        load_test_split(
            _split_csv(tmp_path, [(55.6867, 12.5700, 2, "leaky-observer")])
        ),
        4,
    )
    paths = _build(
        tmp_path, archive=archive, resolution=4, test_recordings=leaked
    )
    assert len(paths) == EXPECTED_ARTIFACTS


def test_a_no_split_build_does_not_fail_on_the_zero_match_check(tmp_path):
    """`[]` is a legitimate mode and must not trip the zero-match guard.

    `drop_test_contributor_cells` returns EARLY on an empty banned set and so
    never warns; the guard is conditioned on a split having been supplied as
    well. A zero-match check that fired here would make `--no-test-split`
    unusable, and the pressure would be to remove the check rather than the
    bug.

    It must not trip the PARTIAL-match clause either. That clause carries its
    own `banned_key_count > 0` conjunct, because `match_rate` is undefined on
    an empty split and RAISES rather than inventing a 0/0 -- so a clause
    missing the conjunct fails here with a `ValueError` about the rate being
    undefined, not with a passing build.
    """
    assert len(_build(tmp_path, test_recordings=[])) == EXPECTED_ARTIFACTS


# --------------------------------------------------------------------------
# C1 (final gate): ONE MATCHING KEY MUST NOT DISARM THE GUARD.
#
# `zero_match` is `banned_key_count > 0 and contributor_rows_removed == 0` --
# a claim about the WHOLE split. Reproduced at HEAD: a 100-key split with one
# real contributor and ninety-nine Sydney coordinates BUILT ALL FIVE
# ARTIFACTS, while the all-hundred-dead control raised. The per-key clause
# below is what closes that, and these tests pin both directions of it.
# --------------------------------------------------------------------------


def _mostly_dead_split(tmp_path, live, dead, name="partial.csv"):
    """A split with *live* keys that bite and *dead* keys that cannot.

    The live rows use the archive's own contributor at a real observed
    (point, week); the dead rows use contributor names that appear nowhere,
    at the SAME coordinates and week -- so the split is at the right H3
    resolution, passes the resolution check, and fails only on the join. That
    is the shape every realistic degradation takes (a spelling convention
    that changed, a mojibaked column), and it is why a total-only condition
    cannot see it.
    """
    rows = [(55.6867, 12.5700, 2, "obs")] * live
    rows += [
        (55.6867, 12.5700, 2, f"nobody-{i:03d}") for i in range(dead)
    ]
    return recordings_at_resolution(
        load_test_split(_split_csv(tmp_path, rows, name=name)), 4
    )


def test_a_split_where_one_key_in_twentyfive_bites_FAILS_the_build(tmp_path):
    """THE C1 DEFECT, end to end: one live key must not release an artifact.

    25 banned keys, ONE of which matches a real contributor. Rows ARE removed,
    so `zero_match` is false and the total check passes -- this build wrote a
    complete artifact set and exited 0 before the per-key clause existed.

    The directory is asserted EMPTY, not merely that an exception was raised:
    the original C1 failure's signature was files on disk plus exit 0, so the
    written state is the assertion that discriminates.
    """
    out = tmp_path / "priors"
    recordings = _mostly_dead_split(tmp_path, live=1, dead=24)
    with pytest.raises(
        build_prior.PartialMatchGuardError, match="matched any occurrence"
    ):
        _build(tmp_path, resolution=4, test_recordings=recordings, out_dir=out)
    assert not out.exists() or not list(out.glob("*")), (
        "the build wrote artifacts despite 24 of 25 banned keys being inert"
    )


def test_the_partial_match_failure_reports_the_rate_and_both_counts(tmp_path):
    """The operator is told WHAT the rate was, not merely that it was low.

    Without the numbers the message is indistinguishable from the zero-match
    one, and the two demand different investigations: a dead join versus a
    subpopulation of the split that is broken while the rest works.
    """
    recordings = _mostly_dead_split(tmp_path, live=1, dead=24)
    with pytest.raises(build_prior.PartialMatchGuardError) as excinfo:
        _build(
            tmp_path,
            resolution=4,
            test_recordings=recordings,
            out_dir=tmp_path / "priors",
        )
    message = str(excinfo.value)
    assert "only 1 of 25" in message, message
    assert "0.0400" in message, message
    assert "--min-split-match-rate" in message, message


def test_a_split_where_EVERY_key_bites_still_builds(tmp_path):
    """The precondition: the per-key clause must not fire on a healthy split.

    Without this the clause could be firing on everything and the test above
    would still pass. Every key here names the archive's own contributor at
    an observed (point, week), so the rate is 1.0.
    """
    recordings = _mostly_dead_split(tmp_path, live=4, dead=0)
    paths = _build(tmp_path, resolution=4, test_recordings=recordings)
    assert len(paths) == EXPECTED_ARTIFACTS


def test_a_split_just_ABOVE_the_threshold_builds(tmp_path):
    """The boundary from the passing side, so the clause is not a blanket ban.

    1 of 10 keys is a 0.10 rate -- sparse, above the 0.05 default, and it
    builds. Paired with the 1-of-25 failure above, the two bracket the
    threshold and prove the comparison is a real one rather than
    "any dead key fails".
    """
    recordings = _mostly_dead_split(tmp_path, live=1, dead=9)
    paths = _build(tmp_path, resolution=4, test_recordings=recordings)
    assert len(paths) == EXPECTED_ARTIFACTS


def test_the_threshold_is_a_PARAMETER_and_an_operator_can_accept_a_sparse_split(
    tmp_path,
):
    """`--min-split-match-rate 0.0` is the explicit, recorded escape.

    A guard with no escape is a guard people delete. The same 1-of-25 split
    that fails by default builds when the operator says so -- and saying so
    is a flag in the command line, not a silent default.
    """
    recordings = _mostly_dead_split(tmp_path, live=1, dead=24)
    paths = _build(
        tmp_path,
        resolution=4,
        test_recordings=recordings,
        min_split_match_rate=0.0,
    )
    assert len(paths) == EXPECTED_ARTIFACTS


def test_the_TOTAL_zero_match_check_cannot_be_disabled_by_the_threshold(tmp_path):
    """`0.0` relaxes the rate clause and leaves the total clause armed.

    The two clauses are not one condition with a knob. A split that removed
    NOTHING is never a release, whatever the operator passes, so the
    zero-match error must still be the one raised here -- and if the rate
    clause had subsumed the total one, `0.0` would have released it.
    """
    out = tmp_path / "priors"
    ghost = _mostly_dead_split(tmp_path, live=0, dead=25)
    with pytest.raises(ZeroMatchGuardError, match="matched no occurrence"):
        _build(
            tmp_path,
            resolution=4,
            test_recordings=ghost,
            out_dir=out,
            min_split_match_rate=0.0,
        )
    assert not out.exists() or not list(out.glob("*.npz"))


def test_the_zero_match_message_names_the_xeno_canto_guard(tmp_path):
    """C1, related (Minor): guard 1 may legitimately have eaten the rows.

    The message told the operator to check H3 resolution and cell formatting
    and never mentioned that the Xeno-Canto guard runs FIRST -- so on a split
    whose recordings are themselves Xeno-Canto occurrences, guard 2 correctly
    finds nothing left. `xeno_canto_rows_removed` was returned and unused.
    """
    archive = _archive(
        tmp_path,
        extra_rows=[
            _row(
                9101,
                1001,
                55.6867,
                12.5700,
                "2025-01-07",
                pub=next(iter(XENO_CANTO_PUBLISHER_KEYS)),
                who="xc-only",
            ),
        ],
    )
    ghost = recordings_at_resolution(
        load_test_split(
            _split_csv(tmp_path, [(55.6867, 12.5700, 2, "xc-only")])
        ),
        4,
    )
    with pytest.raises(ZeroMatchGuardError) as excinfo:
        _build(
            tmp_path,
            archive=archive,
            resolution=4,
            test_recordings=ghost,
            out_dir=tmp_path / "priors",
        )
    message = str(excinfo.value)
    assert "Xeno-Canto" in message, message
    assert "1 row(s)" in message, message


@pytest.mark.parametrize("bad", ["5", "-0.1", "1.5", "percent"])
def test_the_cli_refuses_a_min_split_match_rate_outside_zero_to_one(capsys, bad):
    """A FRACTION, rejected rather than clamped.

    `5` meaning five percent is the obvious units bug, and clamping it to 1.0
    would demand that every banned key bite -- failing every real build --
    while clamping the other way silently disarms the clause. Both are the
    silent repair at a boundary this plan forbids.
    """
    with pytest.raises(SystemExit):
        build_prior._parse_args(
            [
                "--archive", "a.zip", "--source", "dof", "--no-test-split",
                "--gbif-doi", DOI, "--min-split-match-rate", bad,
            ]
        )
    err = capsys.readouterr().err
    assert "min-split-match-rate" in err, err


def test_build_from_archive_refuses_an_out_of_range_threshold(tmp_path):
    """The library half: every other caller is protected too.

    `split_match_rate` guards the command line; this guards a notebook, a
    sweep driver and this plan's own tests. Independently load-bearing, and
    the harness has one mutant per site.
    """
    with pytest.raises(ValueError, match="min_split_match_rate must be"):
        _build(tmp_path, test_recordings=[], min_split_match_rate=5.0)


def test_the_partial_match_guard_reads_the_guard_results_own_counts():
    """The CONDITION is the guard's returned rate, pinned at the source level.

    The same discipline `test_the_zero_match_guard_reads_the_guard_results_own_counts`
    applies to the total clause. A caller re-deriving the rate from
    `len(test_recordings)` would be behaviourally identical on every fixture
    in this file while being wrong for a split whose rows collide under
    normalisation -- and a caller scraping the leakage module's partial-match
    WARNING would reintroduce the C1 logging defect one clause along.
    """
    source = Path(build_prior.__file__).read_text(encoding="utf-8")
    assert "guarded.match_rate < min_split_match_rate" in source, (
        "the partial-match escalation is no longer a comparison on the "
        "guard's returned per-key counts"
    )
    assert "guarded.banned_key_count > 0 and guarded.match_rate" in source, (
        "the partial-match clause lost its `banned_key_count > 0` conjunct; "
        "`--no-test-split` reaches it with no keys, where the rate is "
        "undefined and RAISES"
    )


# --------------------------------------------------------------------------
# I3: a partial artifact set must be distinguishable from a complete one.
# --------------------------------------------------------------------------


def test_the_manifest_names_every_artifact_the_resolution_wrote(tmp_path):
    """One entry per written artifact, npz and sidecar alike.

    DERIVED from `CONTROLS`, so a control added to the registry appears here
    with no edit to the build script or to this test. Asserted against the
    registry rather than against a literal count for exactly that reason.
    """
    import json

    out = tmp_path / "priors"
    paths = _build(tmp_path, out_dir=out)
    manifest = build_prior.manifest_path(out, "dof", 4)
    assert manifest.exists()

    meta = json.loads(manifest.read_text(encoding="utf-8"))
    assert meta["manifest_version"] == build_prior.MANIFEST_VERSION
    assert meta["source"] == "dof" and meta["resolution"] == 4
    assert meta["artifact_count"] == EXPECTED_ARTIFACTS
    assert {e["npz"] for e in meta["artifacts"]} == {p.name for p in paths}
    assert {e["sidecar"] for e in meta["artifacts"]} == {
        p.with_suffix(".json").name for p in paths
    }
    # The variant set comes from the registry, never from four hardcoded names.
    assert {
        e["npz"].removeprefix("prior_dof_r4_").removesuffix(".npz")
        for e in meta["artifacts"]
    } == EXPECTED_VARIANTS


def test_check_manifest_accepts_a_complete_set(tmp_path):
    """The precondition: a real build verifies.

    Without this, `check_manifest` could be refusing everything and the
    truncation tests below would still pass.
    """
    out = tmp_path / "priors"
    paths = _build(tmp_path, out_dir=out)
    verified = build_prior.check_manifest(out, "dof", 4)
    assert sorted(verified) == sorted(paths)


def test_check_manifest_verifies_NAMES_not_a_COUNT(tmp_path):
    """A count is satisfied by the wrong files. The manifest names them.

    ISOLATES the name check from the count. `test_check_manifest_DETECTS_a_
    truncated_set` deletes an artifact, which lowers the count too, so it
    cannot tell a check that verifies names from one that counts files. Here
    the count is RESTORED by a decoy -- the deleted control's name is taken by
    a copy of the full prior -- so the directory holds exactly as many files
    as the manifest names, and every one of them is a valid, correctly
    provenanced artifact. Only the names disagree.

    Realistic rather than contrived: a sweep writes every resolution into one
    `--out`, so a directory routinely holds files from several builds, and a
    count is satisfied by a neighbour's.
    """
    out = tmp_path / "priors"
    paths = _build(tmp_path, out_dir=out)
    full = next(p for p in paths if "_full" in p.name)
    victim = next(p for p in paths if "_full" not in p.name)

    decoy = victim.with_name(victim.name.replace(".npz", "_decoy.npz"))
    shutil.copy(full, decoy)
    shutil.copy(full.with_suffix(".json"), decoy.with_suffix(".json"))
    victim.unlink()
    victim.with_suffix(".json").unlink()

    # The count now matches exactly, and nothing on disk is malformed.
    assert len(list(out.glob("*.npz"))) == len(paths)
    with pytest.raises(ValueError, match="truncated artifact set"):
        build_prior.check_manifest(out, "dof", 4)


def test_check_manifest_DETECTS_a_truncated_set(tmp_path):
    """THE I3 DEFECT: a missing artifact is now nameable.

    Deleting one control leaves every remaining file individually valid and
    correctly provenanced -- which is precisely why a consumer globbing the
    directory could not tell a complete release from a truncated one.
    """
    out = tmp_path / "priors"
    paths = _build(tmp_path, out_dir=out)
    victim = next(p for p in paths if "_full" not in p.name)
    victim.unlink()
    with pytest.raises(ValueError, match="truncated artifact set"):
        build_prior.check_manifest(out, "dof", 4)


def test_check_manifest_detects_a_missing_SIDECAR_too(tmp_path):
    """Its own case: `save_prior` writes two files and `load_prior` needs both.

    A failure between the two leaves an `.npz` whose sidecar never landed --
    a truncated ARTIFACT rather than a truncated SET, produced by the same
    mid-resolution failure, and invisible to a check that looked only at
    `.npz` names.
    """
    out = tmp_path / "priors"
    paths = _build(tmp_path, out_dir=out)
    paths[0].with_suffix(".json").unlink()
    with pytest.raises(ValueError, match="truncated artifact set"):
        build_prior.check_manifest(out, "dof", 4)


def test_check_manifest_FAILS_CLOSED_when_there_is_no_manifest(tmp_path):
    """No manifest means NOT A RELEASE, never "nothing to check".

    This is the state a build that died part-way leaves behind, and the whole
    finding is that such a directory is indistinguishable from a complete one.
    A check that treated an absent manifest as a pass would restore that
    exactly.
    """
    out = tmp_path / "priors"
    _build(tmp_path, out_dir=out)
    build_prior.manifest_path(out, "dof", 4).unlink()
    with pytest.raises(ValueError, match="no artifact manifest"):
        build_prior.check_manifest(out, "dof", 4)


def test_a_build_that_dies_part_way_leaves_NO_manifest(tmp_path):
    """The reproduction, and what the manifest changes about it.

    An exception forced in the third artifact leaves a fully-formed SUBSET on
    disk -- the finding's exact scenario. The partial artifacts are STILL
    THERE: this mechanism does not clean up, and that is the failure mode
    deliberately kept. What it adds is that the truncation is now detectable,
    because the manifest is written LAST and so is absent.
    """
    out = tmp_path / "priors"
    real_save = build_prior.save_prior
    calls = []

    def exploding(tensor, path):
        calls.append(path)
        if len(calls) == 3:
            raise RuntimeError("forced failure part-way through the resolution")
        return real_save(tensor, path)

    build_prior.save_prior = exploding
    try:
        with pytest.raises(RuntimeError, match="forced failure"):
            _build(tmp_path, out_dir=out)
    finally:
        build_prior.save_prior = real_save

    # The partial set IS on disk -- individually valid, correctly provenanced.
    assert len(list(out.glob("*.npz"))) == 2
    for npz in out.glob("*.npz"):
        assert load_prior(npz).gbif_doi == DOI
    # ...and it is now DETECTABLE as incomplete.
    assert not build_prior.manifest_path(out, "dof", 4).exists()
    with pytest.raises(ValueError, match="no artifact manifest"):
        build_prior.check_manifest(out, "dof", 4)


def test_a_failed_REBUILD_does_not_leave_a_stale_manifest_verifying(tmp_path):
    """The manifest is deleted FIRST, and that ordering is the protocol.

    Rebuilding into a directory that already holds a COMPLETE set of the same
    resolution overwrites artifacts one at a time. A manifest left in place
    from the earlier build would still name every file -- all of which exist,
    because they were overwritten in order -- and `check_manifest` would
    verify a directory whose contents are a mixture of two builds.
    """
    out = tmp_path / "priors"
    _build(tmp_path, out_dir=out)
    assert build_prior.check_manifest(out, "dof", 4)

    real_save = build_prior.save_prior
    calls = []

    def exploding(tensor, path):
        calls.append(path)
        if len(calls) == 3:
            raise RuntimeError("forced failure on the rebuild")
        return real_save(tensor, path)

    build_prior.save_prior = exploding
    try:
        with pytest.raises(RuntimeError, match="forced failure"):
            _build(tmp_path, out_dir=out)
    finally:
        build_prior.save_prior = real_save

    assert not build_prior.manifest_path(out, "dof", 4).exists(), (
        "the previous build's manifest survived a failed rebuild and would "
        "verify a directory holding a mixture of two builds"
    )
    with pytest.raises(ValueError, match="no artifact manifest"):
        build_prior.check_manifest(out, "dof", 4)


def test_check_manifest_refuses_a_manifest_for_another_build(tmp_path):
    """A manifest naming a different source or resolution attests to nothing.

    Every resolution of a sweep writes into ONE `--out`, so the wrong
    manifest being consulted is a name collision away rather than a
    hypothetical.
    """
    out = tmp_path / "priors"
    _build(tmp_path, out_dir=out, resolution=4)
    stray = build_prior.manifest_path(out, "dof", 5)
    stray.write_text(
        build_prior.manifest_path(out, "dof", 4).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="cannot attest to this one"):
        build_prior.check_manifest(out, "dof", 5)


def test_check_manifest_refuses_an_unknown_manifest_version(tmp_path):
    """Half-reading a manifest is worse than not reading one.

    A future field carrying part of the artifact list would be skipped, and
    the check would report a complete set from whatever happened to parse.
    """
    import json

    out = tmp_path / "priors"
    _build(tmp_path, out_dir=out)
    path = build_prior.manifest_path(out, "dof", 4)
    meta = json.loads(path.read_text(encoding="utf-8"))
    meta["manifest_version"] = build_prior.MANIFEST_VERSION + 1
    path.write_text(json.dumps(meta), encoding="utf-8")
    with pytest.raises(ValueError, match="manifest_version"):
        build_prior.check_manifest(out, "dof", 4)


def test_check_manifest_refuses_a_manifest_naming_no_artifacts(tmp_path):
    """An empty list would authenticate an empty directory as a release."""
    import json

    out = tmp_path / "priors"
    _build(tmp_path, out_dir=out)
    path = build_prior.manifest_path(out, "dof", 4)
    meta = json.loads(path.read_text(encoding="utf-8"))
    meta["artifacts"] = []
    path.write_text(json.dumps(meta), encoding="utf-8")
    with pytest.raises(ValueError, match="names no artifacts"):
        build_prior.check_manifest(out, "dof", 4)


def test_the_manifest_machinery_hardcodes_no_control_COUNT_or_NAME():
    """I3's forward requirement, checked on the source.

    A fifth control was added to `CONTROLS` while this finding was open. The
    manifest must carry it from the registry alone, so nothing in the
    manifest path may name a control or count them.

    CHECKED ON THE CODE, NOT THE DOCSTRINGS. `write_manifest`'s docstring
    quotes the reproduced failure verbatim -- `prior_dof_r4_{full,uniform,
    marginal}` -- and that is exactly the history the next reader needs. A
    grep over the whole function would force it out of the file, which is the
    opposite of what this finding asked for; the same distinction
    `test_the_build_script_does_not_scrape_logs_for_control_flow` draws.
    """
    import ast

    source = Path(build_prior.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for name in ("write_manifest", "check_manifest", "artifact_stem"):
        function = _function(tree, name)
        body = list(function.body)
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]
        assert body, f"`{name}` is nothing but a docstring"
        text = "\n".join(
            ast.get_source_segment(source, stmt) or "" for stmt in body
        )
        for control in CONTROLS:
            assert control not in text, (
                f"`{name}` names the {control!r} control literally; the "
                "manifest must be derived from the registry so a control "
                "added there is carried with no edit here"
            )
        # ...and it does not count them either. `len(CONTROLS)` is fine; a
        # literal is not, because it is the number that goes stale silently.
        counts = [
            node.value
            for node in ast.walk(ast.parse(text))
            if isinstance(node, ast.Constant) and node.value in (4, 5)
        ]
        assert not counts, (
            f"`{name}` carries the literal artifact count {counts}; the "
            "registry has grown from four controls to five once already"
        )


#: The two USER-FACING files that describe what a build produces. Prose has no
#: equivalent of `test_the_manifest_machinery_hardcodes_no_control_COUNT_or_NAME`,
#: and it is exactly where the count went stale: both said "four controls" for
#: every commit after `0a352e1` added the fifth (final-gate M1).
_PROSE_FILES = (
    Path(__file__).parent.parent.parent / "README.md",
    Path(__file__).parent.parent.parent / "Makefile",
)


def test_the_USER_FACING_PROSE_states_no_control_COUNT_that_can_go_stale():
    """M1. The release must not describe its own contents with a stale number.

    `README.md` and the `Makefile`'s `prior` target both claimed FOUR controls
    from the commit that added the fifth until this one -- the release's own
    description of what it ships, wrong, in the two places a user reads first.

    The fix is NOT to write five. Nothing here asserts a count at all: the
    build loop iterates `CONTROLS`, the manifest is derived from the paths it
    writes, and the prose is now phrased so a sixth control needs no edit.
    This test forbids the count from coming BACK -- in digits or in words --
    rather than checking it is currently right, because "currently right" is
    what the last two years of this file were.

    Checked against `len(CONTROLS)` too: a claim that happens to match today
    is still a claim that will not match tomorrow, so even the CORRECT number
    is refused.
    """
    import re

    words = (
        "one", "two", "three", "four", "five", "six",
        "seven", "eight", "nine", "ten",
    )
    # "<count> controls", "<count> registered controls", "<count> control
    # artifacts". The `(?<!per )` is not a loophole: "one artifact per control"
    # is DISTRIBUTIVE prose -- it says the set is derived per registry entry,
    # which is exactly the phrasing this test wants -- rather than a claim
    # about how many there are.
    pattern = re.compile(
        r"\b(\d+|" + "|".join(words) + r")\s+(?:\w+\s+){0,2}?"
        r"(?<!per )controls?\b",
        re.IGNORECASE,
    )
    for path in _PROSE_FILES:
        text = path.read_text(encoding="utf-8")
        found = [m.group(0) for m in pattern.finditer(text)]
        assert not found, (
            f"{path.name} states a control count -- {found!r}. Even "
            f"{len(CONTROLS)}, which is right today, goes stale the day a "
            "control is added; the build derives the set from `CONTROLS`, so "
            "the prose should describe it by REFERENCE rather than by number"
        )

    # ...and the reference must actually be there, or "no number" is satisfied
    # by prose that says nothing at all.
    readme = _PROSE_FILES[0].read_text(encoding="utf-8")
    assert "CONTROLS" in readme, (
        "README must point at the registry it derives the artifact set from"
    )
    assert "manifest" in readme.lower(), (
        "README must point a consumer at the manifest, which is the only "
        "authoritative list of what a given release contains"
    )


# --------------------------------------------------------------------------
# M2: the memory constant is pinned against a COMMITTED MEASUREMENT.
# --------------------------------------------------------------------------


def test_the_recorded_worst_case_is_READ_FROM_the_measurement_file():
    """M2: the pin is an artifact, not a number typed beside the constant.

    The previous pin asserted `MEASURED >= WORST` and `WORST > 160`. That
    second clause was an arbitrary tripwire, not an anchor: lowering BOTH to
    161/161.0 -- one above it -- left the suite green and restored the
    under-prediction in full. A coordinated two-line edit walked through it.

    `WORST_MEASURED_BYTES_PER_CONTEXT` is now the max over
    `scripts/measurements/measure_peak_results.json`, and this asserts the
    DERIVATION rather than the value: the module-level binding must be a CALL,
    so replacing it with a literal (the first step of the coordinated edit)
    fails here instead of passing silently.
    """
    import ast
    import json

    rows = json.loads(
        build_prior.MEASURE_PEAK_RESULTS.read_text(encoding="utf-8")
    )
    expected = max(float(r["bytes_per_context_net"]) for r in rows)
    assert build_prior.WORST_MEASURED_BYTES_PER_CONTEXT == expected

    tree = ast.parse(Path(build_prior.__file__).read_text(encoding="utf-8"))
    assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "WORST_MEASURED_BYTES_PER_CONTEXT"
            for t in node.targets
        )
    ]
    assert len(assignments) == 1, assignments
    assert isinstance(assignments[0].value, ast.Call), (
        "WORST_MEASURED_BYTES_PER_CONTEXT is assigned a literal again. That "
        "is the edit that defeated the old pin: lower the constant and the "
        "recorded worst case together and every test stays green"
    )


def test_the_worst_case_really_TRACKS_the_measurement_file(tmp_path):
    """...and the derivation is not a constant dressed as a read.

    A `worst_measured_bytes_per_context` that ignored its argument and
    returned 263.0 would pass the assertion above. Two synthetic measurement
    files with different maxima must give two different answers.
    """
    import json

    low = tmp_path / "low.json"
    low.write_text(json.dumps([{"bytes_per_context_net": 11.0}]), encoding="utf-8")
    high = tmp_path / "high.json"
    high.write_text(
        json.dumps(
            [{"bytes_per_context_net": 11.0}, {"bytes_per_context_net": 99.5}]
        ),
        encoding="utf-8",
    )
    assert build_prior.worst_measured_bytes_per_context(low) == 11.0
    assert build_prior.worst_measured_bytes_per_context(high) == 99.5


def test_a_missing_measurement_file_RAISES_rather_than_falling_back(tmp_path):
    """No fallback literal. A default restores the self-referential pin.

    If the constant could quietly fall back to a number in the source when
    the measurement is unreadable, deleting the file would be a third way to
    perform the coordinated edit -- and the test above would still pass.
    """
    with pytest.raises(ValueError, match="is missing"):
        build_prior.worst_measured_bytes_per_context(tmp_path / "absent.json")
    empty = tmp_path / "empty.json"
    empty.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="pins nothing"):
        build_prior.worst_measured_bytes_per_context(empty)


def test_the_measurement_file_is_committed_beside_the_code():
    """The pin is worthless if the artifact is not in the repository.

    The original `measure_peak_results.json` lived under `.superpowers/`,
    which `.gitignore` excludes wholesale, and no test read it -- so it
    anchored nothing. It is committed under `scripts/measurements/` now, and
    this asserts the path is inside the repository tree rather than in a
    process directory that a fresh clone would not have.
    """
    path = build_prior.MEASURE_PEAK_RESULTS
    assert path.exists(), path
    assert ".superpowers" not in path.parts, (
        "the measurement moved back under a gitignored process directory; a "
        "fresh clone would not carry it and the pin would fail at import"
    )
    assert path.parent.name == "measurements"
    assert path.stat().st_size > 0


def test_an_empty_split_file_is_rejected_at_load(tmp_path):
    """Obligation 3: a header-only split is not a silent no-guard build."""
    path = tmp_path / "empty.csv"
    path.write_text("lat,lon,week,recorded_by\n", encoding="utf-8")
    with pytest.raises(ValueError, match="holds no recordings"):
        load_test_split(path)


def test_a_split_missing_a_column_is_rejected(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("lat,lon,week\n55.0,12.0,2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing columns"):
        load_test_split(path)


def test_a_split_with_an_unparseable_week_is_rejected(tmp_path):
    """A week that is not an integer at all."""
    path = _split_csv(tmp_path, [(55.0, 12.0, "week-two", "obs")])
    with pytest.raises(ValueError, match="unparseable week"):
        load_test_split(path)


def test_a_FRACTIONAL_week_is_rejected_rather_than_truncated(tmp_path):
    """Its OWN test, because it is a different disjunct from `week-two`.

    `week-two` fails `int()` AND `int(float())` alike, so it cannot tell a
    strict coercion from a truncating one. `24.7` distinguishes them: `int()`
    raises, `int(float("24.7"))` silently yields 24.

    Truncation is the dangerous branch. A fractional week means the split
    carries a day-of-year, a fractional epoch, or a units bug -- and a
    truncated one bans the WRONG (cell, week): occurrences that never leaked
    are deleted from the prior while the ones that did leak stay in it. Both
    halves silent, and the artifact still builds.
    """
    path = _split_csv(tmp_path, [(55.0, 12.0, "24.7", "obs")])
    with pytest.raises(ValueError, match="unparseable week"):
        load_test_split(path)
    # The truncating alternative WOULD have accepted it, which is what makes
    # this fixture discriminating rather than a duplicate of the one above.
    assert int(float("24.7")) == 24


def test_a_week_outside_1_to_WEEKS_PER_YEAR_is_rejected(tmp_path):
    """Week 0 and week 53 are integers, and both are outside the grid.

    Rejected by `TestRecording`, not by `load_test_split` -- the split loader
    parses, the dataclass validates the range -- so this asserts the
    composition rather than either half, which is what the build actually
    relies on.
    """
    for bad in (0, WEEKS_PER_YEAR + 1):
        split = load_test_split(_split_csv(tmp_path, [(55.0, 12.0, bad, "obs")]))
        with pytest.raises(ValueError, match="week"):
            recordings_at_resolution(split, 4)


def test_a_split_with_unparseable_coordinates_is_rejected(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("lat,lon,week,recorded_by\nnorth,12.0,2,obs\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unparseable"):
        load_test_split(path)


def test_the_split_format_carries_no_cell_column(tmp_path):
    """The format is resolution-FREE by construction.

    A split carrying a cell would let a caller supply one, and a supplied
    cell is fixed at one resolution. The only way to make a sweep safe is for
    the format not to admit one.
    """
    assert "cell" not in build_prior.TEST_SPLIT_COLUMNS
    path = _split_csv(tmp_path, [(55.0, 12.0, 2, "obs")])
    assert "cell" not in path.read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# T10's acceptance criterion, end to end: the GUARDED table is what reaches
# `target_group_ratios`.
# --------------------------------------------------------------------------


def test_the_table_reaching_the_effort_stage_is_the_guarded_one(tmp_path):
    """The acceptance criterion, observed in the real build.

    `target_group_ratios` refuses a CountTable without the leakage-guard
    marker, so the build completing at all proves the guarded table is what
    reached it. Asserted by intercepting the actual call rather than by
    reasoning about the chain: an unguarded denominator under-weights every
    guarded cell-week uniformly and produces a smooth, plausible, mildly-flat
    prior with no error, no nan and no missing row.
    """
    seen = {}
    real = build_prior.target_group_ratios

    def spy(counts):
        seen["metadata"] = counts.schema.metadata
        return real(counts)

    original = build_prior.target_group_ratios
    build_prior.target_group_ratios = spy
    try:
        _build(tmp_path)
    finally:
        build_prior.target_group_ratios = original

    assert seen["metadata"] == {GUARD_MARKER_KEY: b"1"}


def test_a_xeno_canto_record_never_reaches_the_prior(tmp_path):
    """Guard 1, end to end: the publisher filter really runs in the build.

    The Xeno-Canto species is present in the archive and absent from the
    artifact's species axis. Nothing else in the pipeline would drop it.
    """
    xc = next(iter(XENO_CANTO_PUBLISHER_KEYS))
    archive = _archive(
        tmp_path,
        extra_rows=[
            _row(8001 + i, 7777, 55.6867, 12.5700, "2025-01-07", pub=xc)
            for i in range(5)
        ],
    )
    paths = _build(tmp_path, archive=archive)
    t = load_prior(next(p for p in paths if "full" in p.name))
    assert 7777 not in t.species_keys.tolist()
    assert set(t.species_keys.tolist()) == set(ALL_SPECIES)


# --------------------------------------------------------------------------
# NO NETWORK.
# --------------------------------------------------------------------------


def test_the_build_path_never_reaches_the_download_client(tmp_path, monkeypatch):
    """A full build with `requests` POISONED.

    Not a source grep: an import three modules deep would slip past one. Any
    attempt to import `requests` -- which is the only way `gbif_download`'s
    client can be reached -- raises for the duration of the build. Already
    -imported modules are removed from `sys.modules` first so a cached import
    cannot satisfy the lookup.
    """
    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "requests" or name.startswith("requests."):
            raise AssertionError(
                "the build path imported `requests`: the prior build must "
                "never reach the network"
            )
        return real_import(name, *args, **kwargs)

    for module in [m for m in sys.modules if m.split(".")[0] == "requests"]:
        monkeypatch.delitem(sys.modules, module, raising=False)
    monkeypatch.delitem(sys.modules, "birdology.prior.gbif_download", raising=False)
    monkeypatch.setattr(builtins, "__import__", blocked)

    assert len(_build(tmp_path)) == EXPECTED_ARTIFACTS


def test_the_build_script_does_not_import_the_download_client():
    """The structural half of the same claim.

    The runtime check above proves this particular build never reached the
    network; this proves the module does not name the client at all, so a
    code path that only fires on an unusual argument cannot hide one.
    """
    source = Path(build_prior.__file__).read_text(encoding="utf-8")
    body = source.split('"""', 2)[2]
    for forbidden in ("gbif_download", "requests", "urllib", "http"):
        assert forbidden not in body, f"build_prior.py must not reference {forbidden}"

# --------------------------------------------------------------------------
# I2: the build's shrinkage and smoothing constants reach every artifact, and
# the release documentation describes both them and the manifest truthfully.
# --------------------------------------------------------------------------

ARTIFACT_DOC = Path(__file__).parent.parent.parent / "docs" / "prior-artifact-format.md"


def _fenced(doc: str, language: str) -> list[str]:
    return re.findall(rf"```{language}\n(.*?)```", doc, re.DOTALL)


def test_the_build_records_its_params_in_EVERY_artifact_including_controls(tmp_path):
    """I2: the full prior AND every control publish the build's constants.

    `controls._respec` carries `gbif_doi` and does NOT carry the params, so
    `build_from_archive`'s two assignments are the only thing putting them on a
    control. Asserted through `load_prior` on each written `.npz`, so a
    regression in either the assignment or the sidecar write fails here.

    A control's params describe the BUILD it is the null for, not the control's
    own destroyed structure: a control and the prior it is compared against
    must be known to come from the same constants, or the comparison measures
    the constants rather than the context.
    """
    paths = _build(tmp_path)
    assert len(paths) == EXPECTED_ARTIFACTS
    for p in paths:
        t = load_prior(p)
        assert t.backoff_params == BackoffParams(), (p.name, t.backoff_params)
        assert t.smoothing_params == SmoothingParams(), (p.name, t.smoothing_params)


def test_a_NON_DEFAULT_build_records_the_params_it_actually_used(tmp_path):
    """The sidecar records what the build USED, not what the module defaults to.

    Every other test in this file builds at the defaults, so a `save_prior`
    that hardcoded `BackoffParams()` would pass all of them while lying about
    the one artifact whose parameters a reader most needs: a swept one. The
    values are asserted to DIFFER from the defaults, so this cannot be
    satisfied by a constant.
    """
    backoff = BackoffParams(cell_week_weight=1.25, cell_weight=6.0)
    smoothing = SmoothingParams(space_weight=0.7, time_weight=0.0)
    assert backoff != BackoffParams() and smoothing != SmoothingParams()

    paths = _build(
        tmp_path, backoff_params=backoff, smoothing_params=smoothing
    )
    for p in paths:
        t = load_prior(p)
        assert t.backoff_params == backoff, p.name
        assert t.smoothing_params == smoothing, p.name


def test_the_build_params_actually_CHANGE_the_values_not_just_the_sidecar(tmp_path):
    """The recorded params are the ones the pipeline computed with.

    A build that wrote the caller's params into the sidecar while computing
    with the defaults would satisfy every assertion above and publish a
    correct-looking artifact describing a different computation -- the worst
    shape this finding can take, because the record would then be actively
    misleading rather than absent.

    `cell_week_weight` 0.5 against 50.0 is a hundredfold difference in the
    pseudo-count, so the stored values must differ substantially. Asserted on
    the MATRIX, read back from disk.
    """
    archive = _archive(tmp_path)
    light = _build(
        tmp_path,
        archive=archive,
        out_dir=tmp_path / "light",
        backoff_params=BackoffParams(cell_week_weight=0.5),
    )
    heavy = _build(
        tmp_path,
        archive=archive,
        out_dir=tmp_path / "heavy",
        backoff_params=BackoffParams(cell_week_weight=50.0),
    )
    a = load_prior(next(p for p in light if p.name.endswith("_full.npz")))
    b = load_prior(next(p for p in heavy if p.name.endswith("_full.npz")))

    assert a.species_keys.tolist() == b.species_keys.tolist()
    assert a.cells == b.cells
    spread = float(np.abs(a.dense() - b.dense()).max())
    assert spread > 1e-3, (
        f"a 100x change in `cell_week_weight` moved the largest stored value "
        f"by only {spread:.3e}. The params reach the sidecar but apparently "
        "not the computation, so every artifact would publish a faithful "
        "record of constants it was not built with"
    )


def test_the_documented_MANIFEST_shape_matches_a_real_one(tmp_path):
    """Section 2c of the release doc, executed against a written manifest.

    The manifest is what tells a third party their artifact set is complete,
    and the doc is the only description of it they have. Field names, the
    filename pattern and the artifacts[] entry shape are all checked against a
    manifest `build_from_archive` actually wrote -- not a hand-built example --
    because this document has already once described a regime the pipeline
    cannot produce.
    """
    doc = ARTIFACT_DOC.read_text(encoding="utf-8")
    paths = _build(tmp_path, resolution=4, source="dof")
    out_dir = paths[0].parent
    real = json.loads(
        (out_dir / "prior_dof_r4_manifest.json").read_text(encoding="utf-8")
    )

    # The documented FILENAME PATTERN produces the file that exists.
    assert "prior_{source}_r{resolution}_manifest.json" in doc
    assert (out_dir / "prior_dof_r4_manifest.json").exists()

    # The documented EXAMPLE has the same top-level fields as a real manifest.
    example = next(
        json.loads(b) for b in _fenced(doc, "json") if "manifest_version" in b
    )
    assert sorted(example) == sorted(real), (
        f"the documented manifest lists {sorted(example)!r} but a real one "
        f"carries {sorted(real)!r}"
    )
    assert example["manifest_version"] == real["manifest_version"] == MANIFEST_VERSION

    # ...and the same ENTRY shape, which is what a consumer iterates.
    assert {k for e in example["artifacts"] for k in e} == {
        k for e in real["artifacts"] for k in e
    } == {"npz", "sidecar"}

    # The documented entry count is DERIVED, so a control added to the registry
    # cannot leave the doc claiming a smaller set. Checked against the registry
    # rather than against a literal.
    assert real["artifact_count"] == len(real["artifacts"]) == EXPECTED_ARTIFACTS

    # The RULE the whole section exists for must be stated, in the words a
    # reader acts on: absence means incomplete.
    assert "not a complete artifact set" in doc
    assert "verify the names, not this number" in doc


def test_the_documented_manifest_rule_is_the_one_check_manifest_ENFORCES(tmp_path):
    """The doc says "refuse a set with no manifest"; the reader must do that.

    Documentation that describes a stricter rule than the code implements is
    the same defect class as documentation describing a regime the pipeline
    cannot produce -- a third party writes the strict check, ours does not, and
    the disagreement surfaces as their tooling rejecting our releases or ours
    accepting a truncated set. So the rule is asserted against `check_manifest`
    on a real directory, not just found in the prose.
    """
    paths = _build(tmp_path, resolution=4, source="dof")
    out_dir = paths[0].parent

    # A complete set verifies, and names every artifact rather than counting.
    verified = check_manifest(out_dir, "dof", 4)
    assert sorted(p.name for p in verified) == sorted(p.name for p in paths)

    # Remove the manifest: the doc says refuse, so the reader refuses.
    (out_dir / "prior_dof_r4_manifest.json").unlink()
    with pytest.raises(ValueError, match="not a complete release"):
        check_manifest(out_dir, "dof", 4)


# --------------------------------------------------------------------------
# I2/B-I2j: the params reach the EMPTY-SPECIES path of the full grid.
# --------------------------------------------------------------------------


def test_the_params_reach_the_EMPTY_species_grid_not_just_the_populated_one():
    """The no-species branch is a SECOND call site, and it is easy to miss.

    `estimate_over_full_grid` cannot use `pa.concat_tables([])`, so an empty
    species axis is served by a separate `smooth(hierarchical_estimate(...))`
    call. That call is the one a reader edits last and tests least, and a
    version of it that omitted the params would silently compute the empty
    grid at the module defaults while the populated grid used the caller's.

    NOTHING ABOUT THE VALUES CAN PIN IT: the empty grid has zero rows, so
    there is no number to compare. What is observable is that the params
    OBJECT REACHES THE VALIDATORS -- `smooth` and `hierarchical_estimate` both
    validate what they are handed -- so an invalid params object must be
    refused on the empty path exactly as it is on the populated one. A params
    argument dropped from that call makes the refusal disappear, because the
    default the call would fall back to is valid by construction.

    BOTH objects are checked, and asserted separately: a mutant that drops one
    of the two arguments leaves the other still raising, and a single
    assertion could not tell that from both arriving.
    """
    cell = h3.latlng_to_cell(*POINTS[0], 4)
    ratios = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array([cell], pa.string()),
            "week": pa.array([5], pa.int8()),
            "n": pa.array([1], pa.int64()),
            "total": pa.array([1], pa.int64()),
        }
    )

    # THE PRECONDITION: this really is the empty branch. Zero rows, and the
    # schema still the grid's -- so the branch ran rather than short-circuiting.
    empty = estimate_over_full_grid(ratios, [], [cell])
    assert empty.num_rows == 0
    assert set(empty.schema.names) == {"species_key", "cell", "week", "p"}

    # The BACKOFF params reach the empty path.
    with pytest.raises(ValueError, match="is not below 1.0"):
        estimate_over_full_grid(
            ratios, [], [cell], backoff_params=BackoffParams(floor=2.0)
        )
    # ...and so do the SMOOTHING params. Asserted separately: one dropped
    # argument would leave the other's refusal intact.
    with pytest.raises(ValueError, match="space_weight=-1.0 is negative"):
        estimate_over_full_grid(
            ratios, [], [cell], smoothing_params=SmoothingParams(space_weight=-1.0)
        )

    # And the SAME two objects are refused on the POPULATED path, so the test
    # above is about the empty branch rather than about the guards existing.
    with pytest.raises(ValueError, match="is not below 1.0"):
        estimate_over_full_grid(
            ratios, [1], [cell], backoff_params=BackoffParams(floor=2.0)
        )
    with pytest.raises(ValueError, match="space_weight=-1.0 is negative"):
        estimate_over_full_grid(
            ratios, [1], [cell], smoothing_params=SmoothingParams(space_weight=-1.0)
        )


def test_the_SMOOTHING_params_actually_CHANGE_the_values_not_just_the_sidecar():
    """The smoothing half of `..._CHANGE_the_values_not_just_the_sidecar`.

    That test moves `cell_week_weight` and so pins only that
    `backoff_params` reaches the computation. A build that threaded the
    backoff params honestly and passed `SmoothingParams()` to `smooth` while
    RECORDING the caller's would pass it, pass every sidecar assertion, and
    publish a faithful-looking record of a smoothing that did not happen.

    Measured through `build_from_archive` on this file's archive, reading the
    `.npz` back off disk: `time_weight` 0.0 against 2.0 moves the largest
    stored value by 0.130, and `space_weight` 0.0 against 5.0 by 0.061. The
    threshold below is two orders of magnitude under the smaller of those, so
    it is about the params arriving at all rather than about the exact number.
    """
    import tempfile

    tmp = Path(tempfile.mkdtemp())
    archive = _archive(tmp)

    def spread(one, two):
        a = load_prior(
            next(
                p
                for p in _build(
                    tmp,
                    archive=archive,
                    out_dir=tmp / f"sm_{id(one)}",
                    smoothing_params=one,
                )
                if p.name.endswith("_full.npz")
            )
        )
        b = load_prior(
            next(
                p
                for p in _build(
                    tmp,
                    archive=archive,
                    out_dir=tmp / f"sm_{id(two)}",
                    smoothing_params=two,
                )
                if p.name.endswith("_full.npz")
            )
        )
        assert a.species_keys.tolist() == b.species_keys.tolist()
        assert a.cells == b.cells
        return float(np.abs(a.dense() - b.dense()).max())

    time_spread = spread(
        SmoothingParams(space_weight=0.0, time_weight=0.0),
        SmoothingParams(space_weight=0.0, time_weight=2.0),
    )
    assert time_spread > 1e-3, (
        f"moving `time_weight` 0.0 -> 2.0 changed the largest stored value by "
        f"only {time_spread:.3e}; the smoothing params reach the sidecar but "
        "apparently not the smoother"
    )

    space_spread = spread(
        SmoothingParams(space_weight=0.0, time_weight=0.3),
        SmoothingParams(space_weight=5.0, time_weight=0.3),
    )
    assert space_spread > 1e-3, (
        f"moving `space_weight` 0.0 -> 5.0 changed the largest stored value by "
        f"only {space_spread:.3e}; `time_weight` alone reaching the smoother "
        "would satisfy the assertion above"
    )


# --------------------------------------------------------------------------
# RELEASE PACKAGING: the decoding contract ships BESIDE the artifacts, and the
# manifest accounts for it (the obligation carried open since round 5).
# --------------------------------------------------------------------------


def test_a_CONTROL_of_a_params_less_prior_declares_NO_params_rather_than_defaults(
    tmp_path,
):
    """`_respec` does not carry the params, so it must not INVENT them either.

    `controls._respec` constructs a `PriorTensor` without passing
    `backoff_params`, so the DATACLASS FIELD DEFAULT is what decides what a
    control of a params-less prior declares. If that default were
    `BackoffParams()` rather than `None`, every control built by a library
    caller -- and every control of a hand-assembled tensor -- would publish
    today's constants as the ones it was built with, which is a claim about a
    computation that never ran in the one field whose whole purpose is to be
    believed.

    WHY THIS TEST EXISTS AND WHERE IT WAS FOUND. The mutation harness scored
    exactly these two edits (T-I2m, T-I2p) as SURVIVORS: every other
    construction path passes the params explicitly, so the field defaults were
    reachable only through `_respec` and nothing asserted them. The CLI hides
    it -- `build_from_archive` overwrites both fields on every control -- which
    is precisely why the property needs its own test rather than being read off
    an end-to-end build.

    IT LIVES IN THIS FILE rather than `test_controls.py` only because the round
    that found it did not own that file. The contract belongs to `tensor.py`'s
    dataclass and the reachable path belongs to `controls.py`; a later round
    consolidating control tests should move it.

    Asserted through `save_prior`/`load_prior` on a real sidecar, because the
    harm is what the ARTIFACT says, not what the object holds.
    """
    from birdology.prior.controls import CONTROLS as CONTROL_REGISTRY
    from birdology.prior.tensor import save_prior, to_tensor

    prior = to_tensor(
        pa.table(
            {
                # THREE cells, not two: at two cells the seed-0 cell
                # permutation is the IDENTITY, so `shuffle_space` emitted a
                # grid byte-identical to this prior and
                # `controls._refuse_an_unmoved_shuffle` (final-gate I1) now
                # refuses it. The distinct stored week per cell is what makes
                # the cell blocks differ, so the permutation has something to
                # move.
                "species_key": pa.array([1, 2, 1], pa.int64()),
                "cell": pa.array(["cell-a", "cell-b", "cell-c"], pa.string()),
                "week": pa.array([4, 9, 15], pa.int8()),
                "p": pa.array([0.25, 0.5, 0.3], pa.float64()),
            }
        ),
        build_prior.PriorSpec(resolution=4, source="dof", variant="full"),
    )
    prior.gbif_doi = DOI
    # THE PRECONDITION: the prior itself declares nothing, so what the control
    # declares is the control's own doing.
    assert prior.backoff_params is None and prior.smoothing_params is None

    assert CONTROL_REGISTRY, "an empty registry would make this vacuous"
    for name, make in CONTROL_REGISTRY.items():
        control = make(prior)
        npz = save_prior(control, tmp_path / f"c_{name}.npz")
        back = load_prior(npz)
        assert back.backoff_params is None, (
            f"the {name!r} control of a params-less prior claims "
            f"{back.backoff_params!r}; `_respec` fabricated a record of a "
            "shrinkage that never ran"
        )
        assert back.smoothing_params is None, name


def test_the_build_SHIPS_the_format_documentation_beside_the_artifacts(tmp_path):
    """A Zenodo upload of the output directory carries its own decoding contract.

    THE DEFECT: nothing copied `docs/prior-artifact-format.md` out of the
    repository. Every reference to it was a test reading it from the checkout,
    so a published `output/priors/` was a set of `.npz` files whose format was
    documented only somewhere the consumer does not have. The sidecar alone
    does not say that filling with 0.0 vetoes every unobserved context, or
    what the shrinkage constants cost, or what CC BY obliges.

    Asserted on a PIPELINE build, and BYTE-FOR-BYTE against the repository's
    copy -- a rendered, templated or summarised copy would be a second version
    of the contract that could disagree with the one the tests pin.
    """
    paths = _build(tmp_path)
    out_dir = paths[0].parent

    shipped = out_dir / build_prior.FORMAT_DOC_NAME
    assert shipped.exists(), (
        f"the build wrote {len(paths)} artifacts and no format documentation; "
        "an upload of this directory ships matrices a third party cannot "
        "correctly decode"
    )
    assert shipped.read_bytes() == ARTIFACT_DOC.read_bytes()
    # ...and it really is the document the doc tests pin, not an empty file.
    assert b"A missing entry is NOT zero" in shipped.read_bytes()


def test_the_manifest_ACCOUNTS_for_the_shipped_documentation(tmp_path):
    """By name and by content hash, under the manifest's own protocol.

    The manifest's whole protocol is that a missing entry means an incomplete
    release. Copying the document without recording it would leave a release
    whose completeness check passes over a directory the document was deleted
    from -- the round-5 obligation half-discharged a second time.

    The `sha256` is checked to be the hash of the SHIPPED bytes rather than
    merely present: a recorded constant would satisfy a presence assertion.
    """
    import hashlib
    import json

    paths = _build(tmp_path, resolution=4, source="dof")
    out_dir = paths[0].parent
    meta = json.loads(
        build_prior.manifest_path(out_dir, "dof", 4).read_text(encoding="utf-8")
    )

    entry = meta["documentation"]
    assert entry["path"] == build_prior.FORMAT_DOC_NAME
    shipped = (out_dir / entry["path"]).read_bytes()
    assert entry["sha256"] == hashlib.sha256(shipped).hexdigest()
    # DERIVED, not a constant: the same hash must be the repository copy's.
    assert entry["sha256"] == hashlib.sha256(ARTIFACT_DOC.read_bytes()).hexdigest()


def test_check_manifest_REFUSES_a_set_whose_documentation_is_missing(tmp_path):
    """Absence fails closed, exactly like a missing control.

    A release is what a third party downloads. A directory of well-formed,
    correctly provenanced matrices with no decoding contract is incomplete in
    the only sense that matters, and `check_manifest` is the check that has to
    say so.
    """
    paths = _build(tmp_path, resolution=4, source="dof")
    out_dir = paths[0].parent
    assert check_manifest(out_dir, "dof", 4)  # precondition: it verifies first

    (out_dir / build_prior.FORMAT_DOC_NAME).unlink()
    with pytest.raises(ValueError, match="truncated artifact set"):
        check_manifest(out_dir, "dof", 4)


def test_check_manifest_REFUSES_a_manifest_that_names_no_documentation(tmp_path):
    """A manifest silent about the contract would authenticate its absence.

    Distinct from the test above: there the file is gone and the manifest
    still names it. Here the FILE IS PRESENT and the manifest does not account
    for it, which is what a version of `write_manifest` that skipped the entry
    would produce -- and a reader that only looked at entries it found would
    call that directory complete.
    """
    import json

    paths = _build(tmp_path, resolution=4, source="dof")
    out_dir = paths[0].parent
    path = build_prior.manifest_path(out_dir, "dof", 4)
    meta = json.loads(path.read_text(encoding="utf-8"))
    del meta["documentation"]
    path.write_text(json.dumps(meta), encoding="utf-8")

    assert (out_dir / build_prior.FORMAT_DOC_NAME).exists()
    with pytest.raises(ValueError, match="names no format documentation"):
        check_manifest(out_dir, "dof", 4)


def test_check_manifest_REFUSES_documentation_that_DRIFTED_from_the_hash(tmp_path):
    """The failure a NAME cannot see: the shipped copy was edited or truncated.

    The artifacts are originals; this one file is a COPY, and a copy keeps its
    name when someone edits it in place beside the artifacts or a write leaves
    it half-written. A consumer decoding against a drifted contract gets
    plausible wrong numbers rather than an error, which is why the hash is
    recorded and checked rather than the name alone.
    """
    paths = _build(tmp_path, resolution=4, source="dof")
    out_dir = paths[0].parent
    shipped = out_dir / build_prior.FORMAT_DOC_NAME

    # A TRUNCATION, not a random edit: it is the failure that actually happens,
    # and it leaves a file that still parses as markdown and still has the
    # right name.
    shipped.write_bytes(shipped.read_bytes()[: len(shipped.read_bytes()) // 2])
    with pytest.raises(ValueError, match="is not the document this build published"):
        check_manifest(out_dir, "dof", 4)


def test_the_documentation_is_written_BEFORE_the_manifest_which_is_still_LAST(
    tmp_path,
):
    """The ordering protocol survives the addition. Proved by killing the build.

    The manifest is written LAST so that its absence is exactly the state a
    truncated build leaves. Adding a file to the release could have broken
    that two ways: written after the manifest (a verified set could be missing
    its contract) or written so late that a part-way failure leaves neither.

    An exception forced in the third `save_prior` reproduces a part-way build
    and asserts the honest directory: partial artifacts, the DOCUMENT, and NO
    manifest. `check_manifest` then refuses, as it must.
    """
    out = tmp_path / "priors"
    real_save = build_prior.save_prior
    calls = []

    def exploding(tensor, path):
        calls.append(path)
        if len(calls) == 3:
            raise RuntimeError("forced failure part-way through the resolution")
        return real_save(tensor, path)

    build_prior.save_prior = exploding
    try:
        with pytest.raises(RuntimeError, match="forced failure"):
            _build(tmp_path, out_dir=out)
    finally:
        build_prior.save_prior = real_save

    assert len(list(out.glob("*.npz"))) == 2
    # The document IS there: it is written before the first artifact.
    assert (out / build_prior.FORMAT_DOC_NAME).exists()
    # The manifest is NOT: it is still last, so its absence is the signal.
    assert not build_prior.manifest_path(out, "dof", 4).exists()
    with pytest.raises(ValueError, match="no artifact manifest"):
        check_manifest(out, "dof", 4)


def test_write_manifest_REFUSES_to_attest_to_a_set_with_no_documentation(tmp_path):
    """The writer's own guard, independent of the reader's.

    `check_manifest` protects a consumer; this protects the release. A
    manifest written over a directory whose document was removed between the
    copy and the manifest write would be a completion record attesting to a
    file that is not there -- and it would then fail the consumer's check with
    no explanation of where the file went. Both directions are load-bearing
    and neither is redundant.
    """
    out = tmp_path / "priors"
    paths = _build(tmp_path, out_dir=out)
    (out / build_prior.FORMAT_DOC_NAME).unlink()
    with pytest.raises(ValueError, match="is not beside the artifacts"):
        build_prior.write_manifest(out, "dof", 4, paths, DOI)


def test_a_MISSING_repository_document_FAILS_the_build_rather_than_shipping_a_stub(
    tmp_path, monkeypatch
):
    """No silent repair at the packaging boundary either.

    Substituting a placeholder, or skipping the copy, would reinstate the
    defect while the manifest looked discharged -- worse than the original,
    because the release would then carry a document that says nothing under a
    hash that verifies.
    """
    monkeypatch.setattr(
        build_prior, "FORMAT_DOC_SOURCE", tmp_path / "definitely-not-here.md"
    )
    out = tmp_path / "priors"
    with pytest.raises(ValueError, match="format documentation .* is missing"):
        _build(tmp_path, out_dir=out)
    # NOTHING written: the refusal comes before the first artifact.
    assert not out.exists() or not list(out.glob("*.npz"))


def test_a_RESOLUTION_SWEEP_ships_ONE_document_that_every_manifest_agrees_on(
    tmp_path,
):
    """Three resolutions into one `--out` is the normal invocation.

    The document is per-DIRECTORY, not per-resolution, so the copy is written
    three times with the same bytes and every manifest records the same hash.
    A per-resolution name would leave a consumer with three files and nothing
    saying they are identical; a per-resolution hash disagreement would mean
    the copy drifted mid-sweep.
    """
    import json

    out = tmp_path / "priors"
    archive = _archive(tmp_path)
    for resolution in (3, 4, 5):
        _build(tmp_path, archive=archive, out_dir=out, resolution=resolution)

    docs = sorted(p.name for p in out.glob("*.md"))
    assert docs == [build_prior.FORMAT_DOC_NAME]

    hashes = set()
    for resolution in (3, 4, 5):
        assert check_manifest(out, "dof", resolution)
        meta = json.loads(
            build_prior.manifest_path(out, "dof", resolution).read_text(
                encoding="utf-8"
            )
        )
        hashes.add(meta["documentation"]["sha256"])
    assert len(hashes) == 1, (
        f"the three resolutions of one sweep recorded {len(hashes)} different "
        "documentation hashes; the shipped copy changed mid-sweep"
    )


def test_the_documented_manifest_VERSION_and_DOCUMENTATION_entry_match_a_real_one(
    tmp_path,
):
    """Section 2c and 2d, executed against a manifest the build wrote.

    The version BUMP is the load-bearing half: version 1 carried no
    documentation entry, so a reader that skipped an absent key would
    authenticate exactly the release this entry exists to prevent. If the
    document still said 1 a third party would write a version-1 reader and
    accept our undocumented sets.

    The `sha256` is deliberately NOT compared against the document's example:
    the example is inside the document being hashed, so no literal printed
    there can ever be right. That is asserted, so nobody 'fixes' it.
    """
    import json

    doc = ARTIFACT_DOC.read_text(encoding="utf-8")
    paths = _build(tmp_path, resolution=4, source="dof")
    out_dir = paths[0].parent
    real = json.loads(
        (out_dir / "prior_dof_r4_manifest.json").read_text(encoding="utf-8")
    )

    example = next(
        json.loads(b) for b in _fenced(doc, "json") if "manifest_version" in b
    )
    assert sorted(example) == sorted(real)
    assert example["manifest_version"] == real["manifest_version"] == MANIFEST_VERSION
    assert sorted(example["documentation"]) == sorted(real["documentation"])
    assert example["documentation"]["path"] == real["documentation"]["path"]
    # The one field the document cannot state truthfully, and says so.
    assert example["documentation"]["sha256"] != real["documentation"]["sha256"]
    assert "placeholder" in doc

    # The rule a reader acts on, in the words they act on.
    assert "not a complete artifact set" in doc
    assert "prior-artifact-format.md` is in it" in doc
