"""Commonness tiers for the app from the DOF prior: tiers, names, raster, asset."""
import json
import sys
from pathlib import Path

import h3
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.app_tiers import (  # noqa: E402
    BASE_THRESHOLDS, NO_VALUE, build_raster, load_prior, match_names, p_grid, read_asset,
    synthetic_inputs, tiers_from_p, write_asset,
)


def test_thresholds_are_lower_bounds():
    p = np.array([1e-6, 4.99e-5, 5e-5, 3e-4, 1e-3, 3e-3, 9.99e-3, 1e-2, 0.5])
    assert tiers_from_p(p, BASE_THRESHOLDS).tolist() == [0, 0, 1, 2, 3, 4, 4, 5, 5]


def test_match_names_exact_and_through_synonyms():
    prior = {1: "Turdus merula", 2: "Thinornis dubius", 3: "Astur gentilis", 4: "Struthio camelus"}
    app = ["Turdus merula", "Charadrius dubius", "Accipiter gentilis"]
    assert match_names(prior, app) == {1: "Turdus merula", 2: "Charadrius dubius", 3: "Accipiter gentilis"}


def test_match_names_prefers_the_exact_name_on_a_collision():
    prior = {7: "Charadrius dubius", 8: "Thinornis dubius"}
    assert match_names(prior, ["Charadrius dubius"]) == {7: "Charadrius dubius"}


def _write_prior(tmp_path, cells, keys, dense):
    """dense: [species, cells*52]; written fully materialised like build_prior."""
    rows, cols = np.nonzero(np.ones_like(dense, dtype=bool))
    np.savez(tmp_path / "p.npz", format=np.bytes_(b"coo"), shape=np.array(dense.shape),
             row=rows, col=cols, data=dense[rows, cols])
    meta = {"species_keys": keys, "cells": cells, "weeks_per_year": 52, "missing_value": 1e-6,
            "nnz": int(dense.size), "dense_capacity": int(dense.size)}
    (tmp_path / "p.json").write_text(json.dumps(meta))
    return tmp_path / "p.npz", tmp_path / "p.json"


def test_load_prior_and_p_grid_follow_the_documented_column_layout(tmp_path):
    cells = sorted([h3.latlng_to_cell(55.68, 12.57, 5), h3.latlng_to_cell(56.15, 10.20, 5)])
    dense = np.full((2, 2 * 52), 1e-6)
    dense[0, 1 * 52 + (20 - 1)] = 0.02           # species key 10, cell 1, week 20
    npz, js = _write_prior(tmp_path, cells, [10, 11], dense)
    meta, grid = load_prior(npz, js)
    p = p_grid(meta, grid, {10: "Turdus merula", 11: "Apus apus"}, ["Apus apus", "Turdus merula"])
    assert p.shape == (2, 52, 2)
    assert p[1, 19, 1] == 0.02 and p[0, 19, 1] == 1e-6 and p[1, 19, 0] == 1e-6


def test_raster_pixels_agree_with_h3(tmp_path):
    cells, _, _ = synthetic_inputs()
    meta, raster = build_raster(cells, 5)
    hits = 0
    for i in range(0, meta["nlat"], 7):
        for j in range(0, meta["nlon"], 7):
            lat = meta["lat0"] + (i + 0.5) * meta["step"]
            lon = meta["lon0"] + (j + 0.5) * meta["step"]
            c = h3.latlng_to_cell(lat, lon, 5)
            want = cells.index(c) if c in cells else 0xFFFF
            assert raster[i, j] == want
            hits += want != 0xFFFF
    assert hits > 0


def test_asset_round_trip_and_sidecar(tmp_path):
    cells, species, p = synthetic_inputs()
    tiers = tiers_from_p(p, BASE_THRESHOLDS)
    tiers[0, 0, 0] = NO_VALUE
    rmeta, raster = build_raster(cells, 5)
    side = write_asset(tmp_path, species=species, cells=cells, tiers=tiers, raster_meta=rmeta,
                       raster=raster, thresholds=list(BASE_THRESHOLDS), calibration={"factor": 1.0},
                       gbif_doi="10.15468/dl.3j4czw", licence="CC BY 4.0",
                       attribution="Derived from GBIF occurrence download 10.15468/dl.3j4czw.",
                       export_date="2026-10-16")
    got_side, got_tiers, got_raster = read_asset(tmp_path)
    assert got_side == side and side["version"] == 1 and side["weeks"] == 52
    assert side["tier_names"] == ["rare", "scarce", "uncommon", "frequent", "common", "abundant"]
    assert (got_tiers == tiers).all() and (got_raster == raster).all()
    assert len(side["tiers"]["offsets"]) == len(cells)


def test_write_asset_refuses_an_empty_doi(tmp_path):
    cells, species, p = synthetic_inputs()
    rmeta, raster = build_raster(cells, 5)
    with pytest.raises(ValueError, match="DOI"):
        write_asset(tmp_path, species=species, cells=cells, tiers=tiers_from_p(p, BASE_THRESHOLDS),
                    raster_meta=rmeta, raster=raster, thresholds=list(BASE_THRESHOLDS), calibration={},
                    gbif_doi=" ", licence="CC BY 4.0", attribution="x", export_date="2026-10-16")


def test_synthetic_inputs_expected_tiers():
    cells, species, p = synthetic_inputs()
    t = tiers_from_p(p, BASE_THRESHOLDS)
    cph = cells.index(h3.latlng_to_cell(55.68, 12.57, 5))
    aar = cells.index(h3.latlng_to_cell(56.15, 10.20, 5))
    s = {n: i for i, n in enumerate(species)}
    assert t[cph, 19, s["Turdus merula"]] == 5      # abundant
    assert t[aar, 19, s["Turdus merula"]] == 3      # frequent
    assert t[cph, 19, s["Apus apus"]] == 4          # common in week 20
    assert t[cph, 1, s["Apus apus"]] == 0           # rare in week 2
    assert t[cph, 19, s["Tarsiger cyanurus"]] == 0  # rare
