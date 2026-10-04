"""The sweep's RESOLUTION axis: every row's H3 cell is derived at the
SweepCell's resolution from the row's lat/lon (fleet job 7022).

Phase A embedding caches carry ``cell`` at resolution 4 ONLY. ``run_cell``
used to read that column straight into ``prior_for_rows``, so a res-3 or
res-5 cell looked r4 cells up in an r3/r5 prior: every lookup floored and
``full == uniform`` at every resolution (invisible against the Denmark-only
DOF prior), and the restricted EOD prior raised OutOfDomainError on an r4
cell asked of the r3 artifact. The resolution axis never varied.

The fix: per-row lat/lon from the Phase A manifests, joined BY FILEPATH,
cells derived with the SAME ``h3.latlng_to_cell`` call the prior's own
``assign_cells`` uses; refusals on a missing location row, a null lat/lon,
or a derived r4 cell disagreeing with the cache's own ``cell`` column.
"""
from __future__ import annotations

import sys
from pathlib import Path

import h3
import numpy as np
import pyarrow as pa
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import birdology.phase_b.sweep as sweep_mod
from birdology.phase_b.sweep import run_cell
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import PriorSpec, PriorTensor
from tests.phase_b._locations import cells_at, locations_for
from tests.phase_b.test_sweep import (
    _CLASS_IDX,
    _CODE_TO_GBIF,
    _DOI,
    _PROV,
    _cell,
    _eval_fixture,
    _train_cache,
)


def _prior_at(resolution: int) -> PriorTensor:
    """test_sweep's distinct-valued 3-species prior, on the fixture
    points' cells AT *resolution* (what build_prior would emit there)."""
    cells = cells_at(resolution)
    n_species, n_cols = 3, len(cells) * WEEKS_PER_YEAR
    dense = (1.0 + np.arange(n_species * n_cols, dtype=np.float64)).reshape(
        n_species, n_cols
    ) / (n_species * n_cols + 1.0)
    return PriorTensor(
        spec=PriorSpec(resolution=resolution, source="eod", variant="full"),
        species_keys=np.asarray([111, 222, 333], dtype=np.int64),
        cells=list(cells),
        values=sparse.csr_matrix(dense),
        gbif_doi=_DOI,
    )


def _spy_prior_for_rows(monkeypatch) -> list[list[str]]:
    """Record the cells of every prior_for_rows call run_cell makes."""
    seen: list[list[str]] = []
    real = sweep_mod.prior_for_rows

    def spy(prior, cells, weeks, scoreable, code_to_gbif):
        seen.append(list(cells))
        return real(prior, cells, weeks, scoreable, code_to_gbif)

    monkeypatch.setattr(sweep_mod, "prior_for_rows", spy)
    return seen


def _run_at(cell, *, eval_locations=None, train_locations=None, prior=None):
    train = _train_cache()
    eval_cache, metadata = _eval_fixture()
    return run_cell(
        cell,
        train_cache=train,
        eval_cache=eval_cache,
        eval_metadata=metadata,
        eval_locations=(
            locations_for(eval_cache.table)
            if eval_locations is None
            else eval_locations
        ),
        train_locations=(
            locations_for(train.table)
            if train_locations is None
            else train_locations
        ),
        prior=_prior_at(cell.resolution) if prior is None else prior,
        class_idx=_CLASS_IDX,
        code_to_gbif=_CODE_TO_GBIF,
        provenance=_PROV,
        mlp_hidden=4,
        mlp_max_iter=20,
    )


# ---------------------------------------------------------------------------
# (a) r3 / r5 cells hand r3 / r5 cells to the prior — eval AND train side
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("resolution", [3, 5])
@pytest.mark.parametrize("method", ["rescoring", "offset_global", "mlp"])
def test_prior_lookups_receive_cells_at_the_cell_resolution(
    monkeypatch, resolution, method
):
    seen = _spy_prior_for_rows(monkeypatch)
    cell = _cell(
        method=method,
        resolution=resolution,
        tau=1.0 if method == "rescoring" else None,
    )
    row = _run_at(cell)
    assert seen, "run_cell made no prior lookup"
    resolutions = {h3.get_resolution(c) for cells in seen for c in cells}
    assert resolutions == {resolution}, (
        f"res-{resolution} {method} cell asked the prior for cells at "
        f"resolutions {sorted(resolutions)}"
    )
    # And they are the RIGHT cells: the fixture points' cells at that
    # resolution, every one on the prior's axis — nothing floors.
    assert {c for cells in seen for c in cells} == set(cells_at(resolution))
    assert row["prior_coverage_failures_eval"] == 0
    if method != "rescoring":
        assert row["prior_coverage_failures_train"] == 0


# ---------------------------------------------------------------------------
# (b) r4 behaviour unchanged, to the bit, on the existing fixture
# ---------------------------------------------------------------------------

#: Recorded from the pre-fix code (HEAD ec62c96, cells read off the cache's
#: r4 column) on test_sweep's fixture — the r4 path must reproduce them.
_R4_GOLDEN = {
    ("rescoring", "full", 1.0): (
        [0.9106995884773662] * 5, 0.9016909016909016, 0.95,
    ),
    ("rescoring", "shuffle_space", 0.5): (
        [0.9780219780219781, 0.9413179413179412, 0.936841403227958,
         0.9332010582010583, 0.9678004535147392],
        0.9465201465201464, 0.9299999999999999,
    ),
    ("offset_per_species", "full", None): ([1.0] * 5, 1.0, 1.0),
    ("mlp", "full", None): (
        [0.4437605488811937, 0.5529474973102424, 0.5912656420592928,
         0.4948972103734008, 0.5775375530510919],
        0.6247123247123246, 0.54,
    ),
}


@pytest.mark.parametrize("key", list(_R4_GOLDEN), ids=lambda k: f"{k[0]}-{k[1]}")
def test_r4_rows_are_numerically_unchanged(key):
    from tests.phase_b.test_sweep import _run

    method, variant, tau = key
    row = _run(
        _cell(method=method, prior_variant=variant, tau=tau, resolution=4),
        mlp_hidden=8,
        mlp_max_iter=40,
    )
    per_seed, auroc, top1 = _R4_GOLDEN[key]
    assert row["cmap_per_seed"] == per_seed
    assert row["auroc_mean"] == auroc
    assert row["top1_mean"] == top1
    assert row["prior_coverage_failures_eval"] == 0


# ---------------------------------------------------------------------------
# (c) refusals: no location row / null lat-lon — never the cache cell
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("side", ["eval", "train"])
def test_refuses_a_cache_row_with_no_location_row(side):
    train = _train_cache()
    eval_cache, _ = _eval_fixture()
    table = eval_cache.table if side == "eval" else train.table
    locations = locations_for(table)
    dropped = locations.filter(
        pa.compute.invert(
            pa.compute.is_in(
                locations.column("filepath"),
                value_set=pa.array(["r3.ogg", "r7.ogg"]),
            )
        )
    )
    kwargs = {f"{side}_locations": dropped}
    with pytest.raises(ValueError) as exc:
        _run_at(_cell(method="mlp", tau=None, resolution=3), **kwargs)
    msg = str(exc.value)
    assert f"2 of {table.num_rows} {side} cache row(s) have no location row" in msg
    assert "r3.ogg" in msg and "r7.ogg" in msg


def test_refuses_a_null_lat_or_lon():
    eval_cache, _ = _eval_fixture()
    locations = locations_for(eval_cache.table)
    lat = locations.column("lat").to_pylist()
    lon = locations.column("lon").to_pylist()
    lat[4] = None
    lon[9] = None
    nulled = locations.set_column(
        1, "lat", pa.array(lat, pa.float64())
    ).set_column(2, "lon", pa.array(lon, pa.float64()))
    with pytest.raises(ValueError) as exc:
        _run_at(_cell(resolution=5), eval_locations=nulled)
    msg = str(exc.value)
    assert "2 of 20 eval cache row(s) have a null lat/lon" in msg
    assert "r4.ogg" in msg and "r9.ogg" in msg


# ---------------------------------------------------------------------------
# (d) consistency guard: derived r4 cell must equal the cache's r4 cell
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("resolution", [3, 4, 5])
def test_refuses_when_derived_r4_cell_disagrees_with_the_cache(resolution):
    """Two eval rows located at the WRONG fixture point (a mis-joined
    manifest): their derived r4 cell is not the cache's r4 cell. Refused
    at every resolution — the guard proves the join, independent of which
    resolution the cell asks for."""
    eval_cache, _ = _eval_fixture()
    locations = locations_for(eval_cache.table)
    lat = locations.column("lat").to_pylist()
    lon = locations.column("lon").to_pylist()
    for i in (2, 11):  # move to the next fixture point
        j = (i + 1) % 20
        lat[i], lon[i] = lat[j], lon[j]
    swapped = locations.set_column(
        1, "lat", pa.array(lat, pa.float64())
    ).set_column(2, "lon", pa.array(lon, pa.float64()))
    with pytest.raises(ValueError) as exc:
        _run_at(_cell(resolution=resolution), eval_locations=swapped)
    msg = str(exc.value)
    assert "2 of 20 eval cache row(s) carry an r4 cell that disagrees" in msg
    assert "r2.ogg" in msg and "r11.ogg" in msg
    assert eval_cache.table.column("cell")[2].as_py() in msg


def test_refuses_a_recording_located_at_two_points():
    eval_cache, _ = _eval_fixture()
    locations = locations_for(eval_cache.table)
    dup = pa.table(
        {"filepath": ["r6.ogg"], "lat": [0.0], "lon": [0.0]},
        schema=locations.schema,
    )
    with pytest.raises(ValueError, match=r"r6\.ogg.*more than one point|more than one point.*r6\.ogg"):
        _run_at(_cell(resolution=3), eval_locations=pa.concat_tables([locations, dup]))


def test_extra_location_rows_are_harmless():
    """A manifest covers more recordings than a cache (rows dropped at
    load, other splits): unmatched location rows change nothing."""
    eval_cache, _ = _eval_fixture()
    locations = locations_for(eval_cache.table)
    extra = pa.table(
        {"filepath": ["not_in_cache.ogg"], "lat": [0.0], "lon": [0.0]},
        schema=locations.schema,
    )
    a = _run_at(_cell(resolution=3))
    b = _run_at(_cell(resolution=3), eval_locations=pa.concat_tables([extra, locations]))
    assert a["cmap_per_seed"] == b["cmap_per_seed"]


# ---------------------------------------------------------------------------
# (e) fleet regression (job 7022): a RESTRICTED r3 prior is asked r3 cells
# ---------------------------------------------------------------------------


def _restricted_prior_at(resolution: int, weeks: list[int]):
    """A restricted prior whose declared domain is EXACTLY the fixture's
    (cell at *resolution*, week) contexts — what build_prior --query-set
    emits from the Phase A manifests. Any lookup of an r4 cell (or any
    context outside) raises OutOfDomainError, as on the fleet."""
    from birdology.prior.tensor import Restriction, restricted_tensor

    cells = cells_at(resolution)
    contexts = sorted({(c, w) for c in cells for w in weeks})
    restriction = Restriction("/q.parquet", "ab" * 32, tuple(contexts), {})
    block = (1.0 + np.arange(3 * len(contexts), dtype=np.float64)).reshape(
        3, len(contexts)
    ) / (3 * len(contexts) + 1.0)
    tensor = restricted_tensor(
        block,
        [111, 222, 333],
        contexts,
        PriorSpec(resolution=resolution, source="eod", variant="full"),
        restriction,
    )
    tensor.gbif_doi = _DOI
    return tensor


@pytest.mark.parametrize("method", ["rescoring", "offset_global", "mlp"])
def test_restricted_r3_prior_is_asked_r3_cells_not_the_caches_r4(method):
    """The fleet's raise: '12000 context(s) requested in prior_for_rows are
    outside this RESTRICTED prior's declared domain ... ('8429aa7ffffffff',
    28)' — an r4 cell asked of the r3 prior. With cells derived at the
    cell's resolution every lookup is in-domain and nothing floors."""
    prior = _restricted_prior_at(3, weeks=list(range(1, 14)))
    row = _run_at(
        _cell(method=method, resolution=3, tau=1.0 if method == "rescoring" else None),
        prior=prior,
    )
    assert row["prior_coverage_failures_eval"] == 0
    if method != "rescoring":
        assert row["prior_coverage_failures_train"] == 0


def test_learned_arm_without_train_locations_is_refused_by_name():
    """Rescoring performs no training lookup and needs no train
    locations; a learned arm does, and must refuse by name rather than
    crash (or fall back to the cache's r4 cell)."""
    train = _train_cache()
    eval_cache, metadata = _eval_fixture()
    common = dict(
        train_cache=train,
        eval_cache=eval_cache,
        eval_metadata=metadata,
        eval_locations=locations_for(eval_cache.table),
        prior=_prior_at(3),
        class_idx=_CLASS_IDX,
        code_to_gbif=_CODE_TO_GBIF,
        provenance=_PROV,
    )
    row = run_cell(_cell(resolution=3), **common)  # rescoring: fine
    assert row["prior_coverage_failures_eval"] == 0
    with pytest.raises(ValueError, match=r"train_locations"):
        run_cell(_cell(method="offset_global", tau=None, resolution=3), **common)
