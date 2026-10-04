"""Option (b): build the prior ONLY where it is queried -- and prove it exact.

The full build materialises species x observed_cells x 52 contexts, which on
the global EOD archive is ~2 TiB at r3. The restricted build evaluates the
backoff + one smoothing pass on the query cells plus their observed one-step
ring (the HALO), all 52 weeks, and keeps only the queried contexts. These
tests pin that the kept values are BIT-IDENTICAL to the full build's, at the
places an approximation would break first:

* a query cell at the EDGE of the observed axis (some neighbours unobserved);
* query weeks at the year WRAP (1 and 52);
* a query cell with NO observations (off the full build's axis too);

and that the naive restriction (no halo) is NOT exact, so the equality test
can fail.
"""
import json
import sys
import zipfile
from pathlib import Path

import h3
import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.controls import CONTROLS
from birdology.prior.effort import target_group_ratios
from birdology.prior.indexing import WEEKS_PER_YEAR, aggregate_counts, assign_cells, iso_week
from birdology.prior.leakage import apply_leakage_guards
from birdology.prior.occurrences import read_occurrences
from birdology.prior.query_set import (
    QUERY_SET_SCHEMA,
    load_query_set,
    plan_restriction,
    write_query_set,
)
from birdology.prior.tensor import load_prior

import build_prior
from build_prior import (
    build_from_archive,
    cell_axis,
    estimate_over_full_grid,
    estimate_over_query_set,
    species_axis,
)

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)
DOI = "10.15468/dl.test"
RES = 4

# -- geometry ---------------------------------------------------------------
#: C is the centre; A and B are two of its ring-1 neighbours that are also
#: adjacent to each other; E is a ring-2 cell adjacent to A (observed, NOT
#: queried -- it is the halo cell an A-context needs); U is a ring-1
#: neighbour of C that is QUERIED but NEVER OBSERVED; F is a far observed
#: cell no query touches.
C = h3.latlng_to_cell(55.7, 12.5, RES)
_RING1 = sorted(h3.grid_ring(C, 1))
A = _RING1[0]
B = sorted(set(h3.grid_ring(A, 1)) & set(_RING1))[0]
E = sorted(set(h3.grid_ring(A, 1)) & set(h3.grid_ring(C, 2)))[0]
U = sorted(set(_RING1) - {A, B} - set(h3.grid_ring(A, 1)))[0]
F = h3.latlng_to_cell(57.0488, 9.9217, RES)
OBSERVED = (C, A, B, E, F)

#: ISO weeks 1, 2, 10, 24, 30, 52 (asserted in `test_the_fixture_dates_...`).
DATE_OF_WEEK = {
    1: "2025-01-01",
    2: "2025-01-07",
    24: "2025-06-10",
    30: "2025-07-22",
    52: "2025-12-22",
}
SPECIES = (1001, 1002, 1003)

#: The queried contexts. Weeks are SHARED between cells (1, 24, 30) so the
#: restricted shuffle_space control has something to permute, and A carries
#: the year-wrap weeks.
QUERY = {
    (A, 1), (A, 52), (A, 24), (A, 30),
    (C, 1), (C, 2), (C, 24), (C, 30),
    (B, 1), (B, 24),
    (U, 10),
}
ON_GRID_QUERY = sorted((c, w) for c, w in QUERY if c in OBSERVED)


def _row(i, sk, lat, lon, d, pub="pub", who="obs"):
    return "\t".join(
        [str(i), "ds", pub, str(sk), f"sp{sk}", str(lat), str(lon), d, who]
    )


def archive(tmp_path, name="occ.zip"):
    """Varying counts over five observed cells and five weeks, sparse in
    species: a species/cell/week combination is skipped on a fixed pattern
    so the hierarchy's levels genuinely differ."""
    rows = []
    i = 0
    for ci, cell in enumerate(OBSERVED):
        lat, lon = h3.cell_to_latlng(cell)
        for wi, (week, d) in enumerate(sorted(DATE_OF_WEEK.items())):
            for sk in SPECIES:
                if (ci + wi + sk) % 4 == 0:
                    continue
                for _ in range(1 + (ci * 3 + wi * 5 + sk) % 7):
                    i += 1
                    rows.append(_row(i, sk, lat + i * 1e-7, lon, d))
    zip_path = tmp_path / name
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return zip_path


def query_set_file(tmp_path, contexts=QUERY, name="query_set.parquet"):
    lats, lons, weeks = [], [], []
    for cell, week in sorted(contexts):
        lat, lon = h3.cell_to_latlng(cell)
        lats.append(lat)
        lons.append(lon)
        weeks.append(week)
    table = pa.table({"lat": lats, "lon": lons, "week": weeks}, schema=QUERY_SET_SCHEMA)
    path, _ = write_query_set(table, {"sources": [], "distinct_rows": len(lats)},
                              tmp_path / name)
    return path


def build(tmp_path, out, **kwargs):
    kwargs.setdefault("resolution", RES)
    kwargs.setdefault("source", "dof")
    kwargs.setdefault("test_recordings", [])
    kwargs.setdefault("gbif_doi", DOI)
    zip_path = kwargs.pop("archive", None) or archive(tmp_path)
    build_from_archive(zip_path, out_dir=tmp_path / out, **kwargs)
    return tmp_path / out


def ratios_of(zip_path):
    table = assign_cells(read_occurrences(zip_path), RES)
    counts = aggregate_counts(apply_leakage_guards(table, []).table)
    return counts, target_group_ratios(counts)


def _value_index(table):
    return {
        (sk, c, w): p
        for sk, c, w, p in zip(
            table.column("species_key").to_pylist(),
            table.column("cell").to_pylist(),
            table.column("week").to_pylist(),
            table.column("p").to_pylist(),
        )
    }


# --------------------------------------------------------------------------
# The fixture is the fixture it claims to be.
# --------------------------------------------------------------------------


def test_the_fixture_dates_land_in_the_weeks_they_are_named_for():
    from datetime import date

    for week, d in DATE_OF_WEEK.items():
        assert iso_week(date.fromisoformat(d)) == week


def test_the_fixture_geometry_has_an_edge_cell_and_an_unobserved_query_cell():
    assert len(set(OBSERVED)) == 5
    # A is at the EDGE of the observed axis: some of its six neighbours are
    # observed (C, B, E), the rest are not.
    ring_a = set(h3.grid_ring(A, 1))
    assert {C, B, E} <= ring_a
    assert ring_a - set(OBSERVED)
    # E is observed, adjacent to a query cell and NOT itself queried: the
    # halo cell a naive restriction would drop.
    assert E not in {c for c, _ in QUERY}
    # U is queried and never observed.
    assert U not in OBSERVED and (U, 10) in QUERY
    assert F not in set(h3.grid_disk(A, 1)) | set(h3.grid_disk(C, 1))


# --------------------------------------------------------------------------
# (a) EXACTNESS
# --------------------------------------------------------------------------


def test_the_restricted_values_EQUAL_the_full_build_bit_for_bit(tmp_path):
    zip_path = archive(tmp_path)
    qs = load_query_set(query_set_file(tmp_path))
    full = load_prior(build(tmp_path, "full", archive=zip_path) / "prior_dof_r4_full.npz")
    restricted = load_prior(
        build(tmp_path, "restricted", archive=zip_path, query_set=qs)
        / "prior_dof_r4_full.npz"
    )
    assert np.array_equal(full.species_keys, restricted.species_keys)
    # The restricted cell axis is the ON-GRID query cells: observed AND
    # queried. U (queried, unobserved) is off it exactly as it is off the
    # full build's; E (observed, halo only) is not published.
    assert restricted.cells == sorted({A, B, C})
    assert U not in full.cells and U not in restricted.cells
    for cell, week in ON_GRID_QUERY:
        assert np.array_equal(full.vector(cell, week), restricted.vector(cell, week)), (
            cell, week
        )


def test_the_YEAR_WRAP_and_EDGE_contexts_are_among_those_compared():
    """Guards the exactness test against a fixture edit that quietly drops
    the cases it exists for."""
    assert (A, 1) in ON_GRID_QUERY and (A, 52) in ON_GRID_QUERY


def test_species_chunking_of_the_restricted_build_is_bit_exact(tmp_path):
    zip_path = archive(tmp_path)
    qs = load_query_set(query_set_file(tmp_path))
    one = load_prior(
        build(tmp_path, "one", archive=zip_path, query_set=qs, species_chunk=1)
        / "prior_dof_r4_full.npz"
    )
    many = load_prior(
        build(tmp_path, "many", archive=zip_path, query_set=qs, species_chunk=50)
        / "prior_dof_r4_full.npz"
    )
    assert (one.values != many.values).nnz == 0
    assert one.values.nnz == many.values.nnz


def test_an_UNOBSERVED_query_cell_behaves_as_in_the_full_build(tmp_path):
    """Off the axis in both: the Phase B lookup floors it in both."""
    from birdology.phase_b.rescoring import prior_for_rows

    zip_path = archive(tmp_path)
    qs = load_query_set(query_set_file(tmp_path))
    full = load_prior(build(tmp_path, "full", archive=zip_path) / "prior_dof_r4_full.npz")
    restricted = load_prior(
        build(tmp_path, "restricted", archive=zip_path, query_set=qs)
        / "prior_dof_r4_full.npz"
    )
    bridge = {"a": 1001, "b": 1002}
    got_full = prior_for_rows(full, [U], [10], ["a", "b"], bridge)
    got_restricted = prior_for_rows(restricted, [U], [10], ["a", "b"], bridge)
    assert np.array_equal(got_full.probs, got_restricted.probs)
    assert got_full.n_floored == got_restricted.n_floored == 2


# --------------------------------------------------------------------------
# (b) THE HALO IS NECESSARY: the naive restriction is NOT exact.
# --------------------------------------------------------------------------


def test_a_NAIVE_no_halo_restriction_is_NOT_equal_on_edge_contexts(tmp_path):
    counts, ratios = ratios_of(archive(tmp_path))
    species = species_axis(counts)
    observed = cell_axis(counts)
    full = _value_index(estimate_over_full_grid(ratios, species, observed))
    naive_cells = sorted({c for c, _ in QUERY} & set(observed))
    naive = _value_index(estimate_over_full_grid(ratios, species, naive_cells))
    edge = [(sk, A, w) for sk in species for w in (1, 24, 52)]
    assert any(full[k] != naive[k] for k in edge), (
        "dropping the halo changed nothing on A's contexts: this fixture "
        "cannot tell an exact restriction from a naive one"
    )


def test_the_HALO_restriction_IS_equal_on_the_same_contexts(tmp_path):
    counts, ratios = ratios_of(archive(tmp_path))
    species = species_axis(counts)
    observed = cell_axis(counts)
    full = _value_index(estimate_over_full_grid(ratios, species, observed))
    plan = plan_restriction(QUERY, observed)
    assert E in plan.halo_cells and F not in plan.halo_cells
    assert U not in plan.halo_cells
    block = estimate_over_query_set(ratios, species, plan, species_chunk=2)
    assert block.shape == (len(species), len(plan.on_grid_contexts))
    for j, (cell, week) in enumerate(plan.on_grid_contexts):
        for i, sk in enumerate(species):
            assert block[i, j] == full[(sk, cell, week)]


def test_the_plan_separates_on_grid_and_off_grid_query_contexts():
    plan = plan_restriction(QUERY, list(OBSERVED))
    assert plan.query_contexts == sorted(QUERY)
    assert plan.on_grid_contexts == ON_GRID_QUERY
    assert plan.on_grid_cells == sorted({A, B, C})
    assert plan.halo_cells == sorted({A, B, C, E})
    expected = {
        "query_contexts": 11,
        "query_cells": 4,
        "on_grid_contexts": 10,
        "on_grid_cells": 3,
        "off_grid_contexts": 1,
        "halo_cells": 4,
        "observed_cells": 5,
    }
    assert {k: plan.counts[k] for k in expected} == expected
    # The smoothing SUPPORT: every on-grid query context, its two adjacent
    # weeks (year-wrapping) and its observed ring-1 neighbours at its week.
    support = set(plan.support_points)
    assert {(A, 52), (A, 1), (A, 2), (A, 51)} <= support  # the wrap, both ways
    assert (E, 1) in support and (E, 52) in support  # the halo cell, at A's weeks
    assert all(c in OBSERVED for c, _ in support)
    assert plan.counts["support_points"] == len(plan.support_points)
    assert len(support) == len(plan.support_points)


# --------------------------------------------------------------------------
# (c) THE ARTIFACT SAYS IT IS RESTRICTED
# --------------------------------------------------------------------------


def test_the_restricted_sidecar_records_the_restriction_and_the_query_set_hash(tmp_path):
    zip_path = archive(tmp_path)
    qpath = query_set_file(tmp_path)
    qs = load_query_set(qpath)
    out = build(tmp_path, "restricted", archive=zip_path, query_set=qs)
    meta = json.loads((out / "prior_dof_r4_full.json").read_text())
    assert meta["format_version"] == 2
    assert meta["restricted"] is True
    r = meta["restriction"]
    assert r["query_set_sha256"] == qs.sha256
    assert r["query_set_path"] == str(qpath.resolve())
    assert r["domain"] == "query_contexts"
    assert [tuple(x) for x in r["contexts"]] == sorted(QUERY)
    assert r["counts"]["on_grid_contexts"] == 10
    assert r["counts"]["off_grid_contexts"] == 1
    assert r["counts"]["halo_cells"] == 4
    assert r["counts"]["query_set_rows"] == len(qs)
    # Exactly the on-grid domain is stored.
    assert meta["nnz"] == len(SPECIES) * 10


def test_EVERY_artifact_of_a_restricted_build_is_marked_restricted(tmp_path):
    qs = load_query_set(query_set_file(tmp_path))
    out = build(tmp_path, "restricted", query_set=qs)
    shipped = ("full", *(n for n in CONTROLS if n not in build_prior.RESTRICTED_OMITTED_CONTROLS))
    for variant in shipped:
        meta = json.loads((out / f"prior_dof_r4_{variant}.json").read_text())
        assert meta["restricted"] is True, variant
        assert meta["restriction"]["query_set_sha256"] == qs.sha256, variant
        assert load_prior(out / f"prior_dof_r4_{variant}.npz").restriction is not None


def test_an_UNRESTRICTED_build_says_so_and_is_otherwise_unchanged(tmp_path):
    out = build(tmp_path, "full")
    meta = json.loads((out / "prior_dof_r4_full.json").read_text())
    assert meta["restricted"] is False
    assert meta["restriction"] is None
    assert meta["nnz"] == meta["dense_capacity"]
    assert load_prior(out / "prior_dof_r4_full.npz").restriction is None


def test_the_restricted_build_logs_its_own_projected_peak(tmp_path, caplog):
    import logging

    qs = load_query_set(query_set_file(tmp_path))
    with caplog.at_level(logging.INFO, logger="birdology.prior.build"):
        build(tmp_path, "restricted", query_set=qs)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "RESTRICTED" in text
    assert "halo" in text and "projected peak" in text


# --------------------------------------------------------------------------
# (g) CONTROLS RUN ON RESTRICTED ARTIFACTS
# --------------------------------------------------------------------------


def test_the_restricted_controls_are_defined_over_the_DOMAIN(tmp_path):
    qs = load_query_set(query_set_file(tmp_path))
    out = build(tmp_path, "restricted", query_set=qs)
    full = load_prior(out / "prior_dof_r4_full.npz")
    cols, contexts = full.domain_columns()
    assert contexts == ON_GRID_QUERY
    D = full.domain_dense()
    load = lambda v: load_prior(out / f"prior_dof_r4_{v}.npz").domain_dense()  # noqa: E731

    assert np.allclose(load("uniform"), D.mean())
    assert np.allclose(load("marginal"), D.mean(axis=1, keepdims=True))
    # Mass is preserved exactly by uniform, marginal and both shuffles.
    for v in ("uniform", "marginal", "shuffle_space", "shuffle_time"):
        assert np.isclose(load(v).sum(), D.sum()), v

    weeks = np.array([w for _, w in contexts])
    cells = np.array([c for c, _ in contexts])
    space = load("shuffle_space")
    assert not np.array_equal(space, D)
    for w in set(weeks.tolist()):  # values move only among cells of ONE week
        m = weeks == w
        assert np.array_equal(np.sort(space[:, m], axis=1), np.sort(D[:, m], axis=1))
    time = load("shuffle_time")
    assert not np.array_equal(time, D)
    for c in set(cells.tolist()):  # values move only among weeks of ONE cell
        m = cells == c
        assert np.array_equal(np.sort(time[:, m], axis=1), np.sort(D[:, m], axis=1))


def test_a_restricted_release_omits_separable_and_says_so(tmp_path):
    """Job 6787_3: on the global EOD query set the separable surrogate's
    outer product reached 2.67 > 1.0 and the build died AFTER the full prior
    and four controls were written. A query set is not a cell x week
    product, so the product of its marginals is unbounded by construction;
    no consumer schedules it (phase_c_run.PRIOR_VARIANTS, phase_b's four
    controls). A restricted release skips it and the MANIFEST records why;
    an unrestricted release still ships it."""
    qs = load_query_set(query_set_file(tmp_path))
    out = build(tmp_path, "restricted", query_set=qs)
    assert not (out / "prior_dof_r4_separable.npz").exists()
    for name in ("full", *(n for n in CONTROLS if n != "separable")):
        assert (out / f"prior_dof_r4_{name}.npz").exists()
    meta = json.loads((out / "prior_dof_r4_manifest.json").read_text())
    assert "separable" in meta["omitted_controls"]
    assert "restricted" in meta["omitted_controls"]["separable"]
    build_prior.check_manifest(out, "dof", RES)

    unrestricted = build(tmp_path, "full")
    assert (unrestricted / "prior_dof_r4_separable.npz").exists()
    meta = json.loads((unrestricted / "prior_dof_r4_manifest.json").read_text())
    assert "omitted_controls" not in meta


def test_restricted_controls_refuse_out_of_domain_lookups_too(tmp_path):
    from birdology.prior.tensor import OutOfDomainError

    qs = load_query_set(query_set_file(tmp_path))
    out = build(tmp_path, "restricted", query_set=qs)
    for variant in ("full", *(n for n in CONTROLS if n != "separable")):
        t = load_prior(out / f"prior_dof_r4_{variant}.npz")
        with pytest.raises(OutOfDomainError):
            t.vector(A, 5)


# --------------------------------------------------------------------------
# The CLI flag, on both parse paths.
# --------------------------------------------------------------------------


def _argv(zip_path, out, *extra):
    return [
        "--archive", str(zip_path), "--source", "dof", "--resolutions", "4",
        "--out", str(out), "--gbif-doi", DOI, "--no-test-split", *extra,
    ]


@pytest.mark.parametrize("parse_path", [[], ["--sequential"]])
def test_the_cli_query_set_flag_builds_a_restricted_artifact(tmp_path, parse_path):
    zip_path = archive(tmp_path)
    qpath = query_set_file(tmp_path)
    out = tmp_path / "out"
    build_prior.main(_argv(zip_path, out, "--query-set", str(qpath), *parse_path))
    restricted = load_prior(out / "prior_dof_r4_full.npz")
    full = load_prior(build(tmp_path, "ref", archive=zip_path) / "prior_dof_r4_full.npz")
    assert restricted.restriction is not None
    for cell, week in ON_GRID_QUERY:
        assert np.array_equal(full.vector(cell, week), restricted.vector(cell, week))
    build_prior.check_manifest(out, "dof", RES)


def test_the_restricted_projection_is_linear_in_each_measured_term():
    from build_prior import (
        RESTRICTED_BYTES_PER_CHUNK_SUPPORT_POINT,
        RESTRICTED_BYTES_PER_RATIO_ROW,
        RESTRICTED_BYTES_PER_STORED_CONTEXT,
        project_restricted_peak_bytes,
    )

    base = project_restricted_peak_bytes(100, 1000, 5000, 10_000, 50)
    assert project_restricted_peak_bytes(100, 1001, 5000, 10_000, 50) - base == (
        100 * RESTRICTED_BYTES_PER_STORED_CONTEXT
    )
    assert project_restricted_peak_bytes(100, 1000, 5000, 10_001, 50) - base == (
        RESTRICTED_BYTES_PER_RATIO_ROW
    )
    assert project_restricted_peak_bytes(100, 1000, 5001, 10_000, 50) - base == (
        50 * RESTRICTED_BYTES_PER_CHUNK_SUPPORT_POINT
    )
    # The measured slopes, rounded up (see the constants' tables).
    assert RESTRICTED_BYTES_PER_STORED_CONTEXT >= 70
    assert RESTRICTED_BYTES_PER_RATIO_ROW >= 232
