"""Commonness tiers for the Fugle DK app, from a birdology occurrence prior.

A prior value is the smoothed share of a cell-week's records that are one
species. The app wants six tiers (its ExploreTier vocabulary) per (cell, week,
species), shipped as a small asset: a JSON sidecar, one zlib block of tiers per
cell, and a lat/lon raster mapping a position to its cell so the app needs no
H3 library. Layout: docs/superpowers/specs/2026-10-16-app-commonness-design.md.
"""
from __future__ import annotations

import json
import math
import zlib
from pathlib import Path

import h3
import numpy as np
import pyarrow.parquet as pq

from birdology.phase_d.taxonomy import SYNONYMS

TIER_NAMES = ("rare", "scarce", "uncommon", "frequent", "common", "abundant")
NO_VALUE = 255
WEEKS = 52
OUTSIDE = 0xFFFF
#: Lower bounds on p of scarce, uncommon, frequent, common, abundant.
BASE_THRESHOLDS = (5e-5, 3e-4, 1e-3, 3e-3, 1e-2)


def tiers_from_p(p: np.ndarray, thresholds) -> np.ndarray:
    """Tier index per value: the number of lower bounds p reaches."""
    return np.searchsorted(np.asarray(thresholds, np.float64), np.asarray(p), side="right").astype(np.uint8)


def load_prior(npz: Path, sidecar: Path) -> tuple[dict, np.ndarray]:
    """(meta, dense grid [species, cells*52]) per prior-artifact-format.md section 5."""
    meta = json.loads(Path(sidecar).read_text())
    with np.load(npz) as z:
        if bytes(z["format"]) != b"coo":
            raise ValueError("not a COO prior artifact")
        n_rows, n_cols = (int(x) for x in z["shape"])
        if n_rows != len(meta["species_keys"]) or n_cols != len(meta["cells"]) * meta["weeks_per_year"]:
            raise ValueError("prior matrix and sidecar axes disagree")
        grid = np.full((n_rows, n_cols), float(meta["missing_value"]), np.float64)
        grid[z["row"], z["col"]] = z["data"]
    if meta["weeks_per_year"] != WEEKS:
        raise ValueError(f"expected {WEEKS} weeks, got {meta['weeks_per_year']}")
    return meta, grid


def species_names_from_parquet(parquet_dir: Path) -> dict[int, str]:
    """GBIF species key -> its most frequent `species` name in an archive_parquet copy."""
    counts: dict[tuple[int, str], int] = {}
    for f in sorted(Path(parquet_dir).glob("part-*.parquet")):
        t = pq.read_table(f, columns=["speciesKey", "species"])
        g = t.group_by(["speciesKey", "species"]).aggregate([("speciesKey", "count")])
        for key, name, n in zip(*(g[c].to_pylist() for c in ("speciesKey", "species", "speciesKey_count"))):
            key, name = key.strip(), name.strip()
            if key.isdigit() and name:
                counts[(int(key), name)] = counts.get((int(key), name), 0) + n
    best: dict[int, tuple[int, str]] = {}
    for (key, name), n in counts.items():
        if key not in best or n > best[key][0]:
            best[key] = (n, name)
    return {k: v[1] for k, v in best.items()}


def match_names(prior_names: dict[int, str], app_names: list[str]) -> dict[int, str]:
    """GBIF key -> app scientific name: exact, else through a SYNONYMS group.
    When two keys reach one app name, the exact match wins."""
    app = set(app_names)
    groups = {n: g for g in SYNONYMS.values() for n in g}
    exact, via = {}, {}
    for key, name in sorted(prior_names.items()):
        if name in app:
            exact[key] = name
        else:
            for cand in groups.get(name, ()):
                if cand in app:
                    via[key] = cand
                    break
    taken = set(exact.values())
    out = dict(exact)
    for key, name in via.items():
        if name not in taken:
            out[key] = name
            taken.add(name)
    return out


def p_grid(meta: dict, grid: np.ndarray, matched: dict[int, str], species: list[str]) -> np.ndarray:
    """[cells, 52, len(species)] of p, species in the given order."""
    row_of = {k: i for i, k in enumerate(meta["species_keys"])}
    key_of = {name: key for key, name in matched.items()}
    rows = [row_of[key_of[s]] for s in species]
    n_cells = len(meta["cells"])
    return grid[rows].reshape(len(species), n_cells, WEEKS).transpose(1, 2, 0).copy()


def build_raster(cells: list[str], resolution: int, step: float = 0.02) -> tuple[dict, np.ndarray]:
    """uint16 [nlat, nlon]: index of the cell holding each pixel centre, else OUTSIDE."""
    if len(cells) >= OUTSIDE:
        raise ValueError("too many cells for a uint16 raster")
    pts = [p for c in cells for p in h3.cell_to_boundary(c)]
    lat_min = math.floor(min(p[0] for p in pts) / step) * step
    lon_min = math.floor(min(p[1] for p in pts) / step) * step
    nlat = math.ceil((max(p[0] for p in pts) - lat_min) / step) + 1
    nlon = math.ceil((max(p[1] for p in pts) - lon_min) / step) + 1
    index = {c: i for i, c in enumerate(cells)}
    raster = np.full((nlat, nlon), OUTSIDE, np.uint16)
    for i in range(nlat):
        lat = lat_min + (i + 0.5) * step
        for j in range(nlon):
            c = h3.latlng_to_cell(lat, lon_min + (j + 0.5) * step, resolution)
            if c in index:
                raster[i, j] = index[c]
    meta = {"lat0": round(lat_min, 6), "lon0": round(lon_min, 6), "step": step, "nlat": nlat, "nlon": nlon}
    return meta, raster


def write_asset(out: Path, *, species, cells, tiers, raster_meta, raster, thresholds, calibration,
                gbif_doi, licence, attribution, export_date) -> dict:
    """dk_tiers.json + dk_tiers.bin + dk_raster.bin into `out`; returns the sidecar."""
    if not str(gbif_doi).strip():
        raise ValueError("a GBIF download DOI is required: the CC BY 4.0 data must be attributed")
    tiers = np.asarray(tiers, np.uint8)
    if tiers.shape != (len(cells), WEEKS, len(species)):
        raise ValueError(f"tiers shape {tiers.shape} != ({len(cells)}, {WEEKS}, {len(species)})")
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    blocks, offsets, lengths, pos = [], [], [], 0
    for c in range(len(cells)):
        b = zlib.compress(tiers[c].tobytes(), 9)
        blocks.append(b)
        offsets.append(pos)
        lengths.append(len(b))
        pos += len(b)
    (out / "dk_tiers.bin").write_bytes(b"".join(blocks))
    (out / "dk_raster.bin").write_bytes(np.asarray(raster, "<u2").tobytes())
    side = {
        "version": 1, "source": "dof", "resolution": h3.get_resolution(cells[0]), "weeks": WEEKS,
        "species": list(species), "cells": list(cells), "tier_names": list(TIER_NAMES),
        "no_value": NO_VALUE, "thresholds": [float(t) for t in thresholds], "calibration": calibration,
        "gbif_doi": gbif_doi, "licence": licence, "attribution": attribution, "export_date": export_date,
        "tiers": {"file": "dk_tiers.bin", "layout": "per cell: zlib([week][species] uint8)",
                  "offsets": offsets, "lengths": lengths},
        "raster": {"file": "dk_raster.bin", "dtype": "uint16le", "outside": OUTSIDE, **raster_meta},
    }
    (out / "dk_tiers.json").write_text(json.dumps(side, indent=1) + "\n")
    return side


def read_asset(out: Path) -> tuple[dict, np.ndarray, np.ndarray]:
    out = Path(out)
    side = json.loads((out / "dk_tiers.json").read_text())
    blob = (out / "dk_tiers.bin").read_bytes()
    S, n = len(side["species"]), len(side["cells"])
    tiers = np.stack([
        np.frombuffer(zlib.decompress(blob[o:o + ln]), np.uint8).reshape(WEEKS, S)
        for o, ln in zip(side["tiers"]["offsets"], side["tiers"]["lengths"])
    ]) if n else np.zeros((0, WEEKS, S), np.uint8)
    r = side["raster"]
    raster = np.frombuffer((out / "dk_raster.bin").read_bytes(), "<u2").reshape(r["nlat"], r["nlon"])
    return side, tiers, raster


def synthetic_inputs() -> tuple[list[str], list[str], np.ndarray]:
    """A two-cell, three-species prior with known tiers; also the app's test fixture.

    Copenhagen: Turdus merula 0.02 (abundant); Apus apus 0.005 in weeks 18-33
    (common), 1e-6 otherwise (rare); Tarsiger cyanurus 1e-5 (rare).
    Aarhus: the same, but Turdus merula 0.002 (frequent).
    """
    cph = h3.latlng_to_cell(55.68, 12.57, 5)
    aar = h3.latlng_to_cell(56.15, 10.20, 5)
    cells = sorted([cph, aar])
    species = ["Apus apus", "Tarsiger cyanurus", "Turdus merula"]
    p = np.full((2, WEEKS, 3), 1e-6)
    for c, merula in ((cells.index(cph), 0.02), (cells.index(aar), 0.002)):
        p[c, :, 2] = merula
        p[c, 17:33, 0] = 0.005
        p[c, :, 1] = 1e-5
    return cells, species, p


BREEDERS = ("Turdus merula", "Parus major", "Fringilla coelebs")
SWIFT = "Apus apus"
_SUMMER = np.arange(13, 30)                                   # weeks 14-30, 0-based
_WINTER = np.r_[np.arange(0, 10), np.arange(44, 52)]          # weeks 1-10 and 45-52
MIN_SU_SPECIES = 10


class CalibrationError(Exception):
    pass


def check_tiers(tiers: np.ndarray, species: list[str], su: set[str]) -> dict[str, dict]:
    """The spec's three checks on tiers [cells, 52, S]."""
    col = {s: i for i, s in enumerate(species)}
    missing = [s for s in (*BREEDERS, SWIFT) if s not in col]
    if missing:
        raise CalibrationError(f"check species not exported: {missing}")
    su_cols = [col[s] for s in sorted(su) if s in col]
    if len(su_cols) < MIN_SU_SPECIES:
        raise CalibrationError(f"only {len(su_cols)} SU species exported; need {MIN_SU_SPECIES}")
    rare, common = 0, 4
    su_share = float((tiers[:, :, su_cols] == rare).mean())
    breed_share = float((tiers[:, _SUMMER][:, :, [col[s] for s in BREEDERS]] >= common).mean())
    swift_share = float((tiers[:, _WINTER, col[SWIFT]] == rare).mean())
    return {
        "su_rare": {"value": su_share, "need": 0.95, "passed": su_share >= 0.95},
        "breeders_common": {"value": breed_share, "need": 0.90, "passed": breed_share >= 0.90},
        "swift_winter_rare": {"value": swift_share, "need": 0.90, "passed": swift_share >= 0.90},
    }


def calibrate(p: np.ndarray, species: list[str], su: set[str], base=BASE_THRESHOLDS, factors=None):
    """(thresholds, factor, checks): the base thresholds scaled by the factor
    closest to 1 (in log terms) for which every check passes."""
    if factors is None:
        factors = [2.0 ** (k / 8) for k in range(-16, 17)]   # 0.25..4, exactly 1.0 and 2.0
    best = None
    for f in sorted(factors, key=lambda f: abs(math.log(f))):
        thresholds = [float(t * f) for t in base]
        checks = check_tiers(tiers_from_p(p, thresholds), species, su)
        if all(c["passed"] for c in checks.values()):
            return thresholds, float(f), checks
        if best is None:
            best = checks
    failed = ", ".join(k for k, c in best.items() if not c["passed"])
    raise CalibrationError(f"no threshold factor passes; at factor 1 failing: {failed} ({best})")
