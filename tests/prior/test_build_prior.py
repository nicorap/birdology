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
import logging
import sys
import zipfile
from pathlib import Path

import h3
import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.backoff import FLOOR
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
from birdology.prior.tensor import LICENCE, load_prior

import build_prior
from build_prior import (
    ZeroMatchGuardError,
    build_from_archive,
    estimate_over_full_grid,
    load_test_split,
    recordings_at_resolution,
)

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)

DOI = "10.15468/dl.test"

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


def test_writes_full_prior_and_four_controls(tmp_path):
    paths = _build(tmp_path)
    variants = {load_prior(p).spec.variant for p in paths}
    assert variants == {
        "full", "uniform", "marginal", "shuffle_space", "shuffle_time"
    }


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
    assert len(paths) == 5
    for p in paths:
        assert load_prior(p).gbif_doi == DOI


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
    """THE T10-R1 assertion: a never-observed week reads STRICTLY ABOVE FLOOR.

    This is what the retired structural-zero check becomes, and it is what
    distinguishes the two artifacts the ruling is about:

    * a week the backoff NEVER SAW is absent from the tensor and reads back
      as exactly `FLOOR` -- "no evidence";
    * a week it DID see, with no occurrence of that species, is STORED with a
      shrunk estimate: the cell's annual profile pulled toward the species'
      global profile.

    Week 30 is observed by NO record in the fixture. Passing the full week
    grid means it is nevertheless estimated from the cell's own profile, so
    its value is strictly greater than the floor. Under the observed-axes
    default it would be absent and read exactly `FLOOR`, and the difference
    between the two artifacts is invisible in every other respect.
    """
    paths = _build(tmp_path)
    t = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))

    unobserved = 30
    assert unobserved not in OBSERVED_WEEKS, "the week must be genuinely unobserved"

    value = t.lookup(SPECIES[0], t.cells[0], unobserved)
    assert value > FLOOR, (
        f"week {unobserved} read exactly the floor, so it was never estimated: "
        "the grid collapsed to the observed weeks"
    )
    # It is a real shrunk estimate, not a hair above the floor by accident.
    assert value > 10 * FLOOR


def test_every_context_of_the_grid_is_stored_not_merely_reachable(tmp_path):
    """`nnz` equals the dense capacity: every context carries a value.

    `lookup` floors an unstored context, so a single-context assertion cannot
    by itself tell "estimated" from "defaulted" without comparing to FLOOR.
    This is the whole-artifact form of the same claim, and it is what the
    sidecar's published fill reports to a third party.
    """
    paths = _build(tmp_path)
    t = load_prior(next(p for p in paths if load_prior(p).spec.variant == "full"))
    assert t.values.nnz == t.dense_capacity
    assert t.dense_capacity == len(ALL_SPECIES) * len(t.cells) * WEEKS_PER_YEAR


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
            ["--archive", "a.zip", "--source", "dof", "--gbif-doi", "d"]
        )
    assert "test-split" in capsys.readouterr().err


def test_the_cli_refuses_both_split_flags_at_once(capsys):
    with pytest.raises(SystemExit):
        build_prior._parse_args(
            [
                "--archive", "a.zip", "--source", "dof", "--gbif-doi", "d",
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


def test_the_cli_accepts_either_split_flag_alone(tmp_path):
    args = build_prior._parse_args(
        ["--archive", "a.zip", "--source", "dof", "--gbif-doi", "d",
         "--no-test-split"]
    )
    assert args.no_test_split is True and args.test_split is None
    args = build_prior._parse_args(
        ["--archive", "a.zip", "--source", "dof", "--gbif-doi", "d",
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


def test_no_except_handler_in_the_build_script_swallows_anything():
    """Obligation 2, structurally: every handler RE-RAISES.

    A blanket "no `except` anywhere" grep is the wrong check here and was
    tried first: `load_test_split` legitimately catches the `TypeError` /
    `ValueError` that `float()` and `int()` raise on a malformed field, in
    order to re-raise a `ValueError` that NAMES the split, the line and what
    the malformed field would have done to the prior. That is the house
    error contract, not a swallow.

    What must never happen is a handler that returns, passes, logs, or
    otherwise lets the build continue past a guard error. Checked with the
    AST rather than by grep, so it sees the handler BODIES rather than the
    keyword: a handler is acceptable only if it raises.

    The `try` in `build_from_archive` has no `except` at all -- its `finally`
    detaches a log handler -- so it contributes no handler here and the
    pipeline path is covered by the absence.
    """
    import ast

    tree = ast.parse(Path(build_prior.__file__).read_text(encoding="utf-8"))
    handlers = [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]
    assert handlers, "the AST walk found nothing; the check would be vacuous"
    for handler in handlers:
        assert any(
            isinstance(node, ast.Raise) for node in ast.walk(handler)
        ), f"except handler at line {handler.lineno} does not re-raise"


def test_the_pipeline_call_path_has_no_except_at_all():
    """The stages themselves are not wrapped, not even to re-raise.

    `build_from_archive` orchestrates every guard in the plan. A handler
    around any of them -- however well-intentioned -- would convert a guard
    error into something the loop could survive, and the artifact it then
    wrote would be leak-contaminated under a normal-looking name.
    """
    import ast

    tree = ast.parse(Path(build_prior.__file__).read_text(encoding="utf-8"))
    build = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "build_from_archive"
    )
    assert not [n for n in ast.walk(build) if isinstance(n, ast.ExceptHandler)]
    # ...and `main`'s resolution loop is unwrapped too, so a failed artifact
    # exits non-zero rather than being skipped.
    main = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    assert not [n for n in ast.walk(main) if isinstance(n, ast.Try)]


# --------------------------------------------------------------------------
# T10-R3, obligation 3/4: a split that was supplied must BITE.
# --------------------------------------------------------------------------


def test_a_split_that_matches_nothing_fails_the_build(tmp_path):
    """Obligation 4: the zero-match warning becomes a build failure.

    A split at the right resolution whose CONTRIBUTOR appears nowhere in the
    archive removes no row, logs one WARNING, and otherwise builds a perfect
    artifact whose leakage guard did nothing. That is the shared symptom of
    four silent failure modes, and since a split WAS supplied it is a failure
    rather than a note.
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
    assert len(paths) == 5


def test_a_no_split_build_does_not_fail_on_the_zero_match_check(tmp_path):
    """`[]` is a legitimate mode and must not trip the zero-match guard.

    `drop_test_contributor_cells` returns EARLY on an empty banned set and so
    never warns; the guard is conditioned on a split having been supplied as
    well. A zero-match check that fired here would make `--no-test-split`
    unusable, and the pressure would be to remove the check rather than the
    bug.
    """
    assert len(_build(tmp_path, test_recordings=[])) == 5


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

    assert len(_build(tmp_path)) == 5


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