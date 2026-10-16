#!/usr/bin/env python3
"""Export the app's commonness asset from the DOF prior.

Real export (on the fleet):
    python scripts/prior_app_export.py \\
        --prior data/prior/dof/prior_dof_r5_full \\
        --names data/prior/parquet/dof \\
        --labels apps/models/birdnet_v3/BirdNET+_V3.0-preview3.1_Global_10K-pruned_Labels.csv \\
        --su src/birdology/prior/data/su_species.csv \\
        --out data/phase_d/app_commonness

Test fixture for the app (synthetic prior, base thresholds, no calibration):
    python scripts/prior_app_export.py --fixture ../fugle-dk/test/fixtures/dk_tiers
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

from birdology.phase_d.birdnet3 import load_labels
from birdology.prior.app_tiers import (
    BASE_THRESHOLDS, CalibrationError, build_raster, calibrate, load_prior, match_names, p_grid,
    species_names_from_parquet, synthetic_inputs, tiers_from_p, write_asset,
)
from birdology.prior.su_list import load_su_species


def fixture(out: Path) -> int:
    cells, species, p = synthetic_inputs()
    rmeta, raster = build_raster(cells, 5)
    write_asset(out, species=species, cells=cells, tiers=tiers_from_p(p, BASE_THRESHOLDS),
                raster_meta=rmeta, raster=raster, thresholds=list(BASE_THRESHOLDS),
                calibration={"factor": 1.0, "fixture": True}, gbif_doi="10.15468/dl.3j4czw",
                licence="CC BY 4.0", attribution="Synthetic test fixture.", export_date="2026-10-16")
    print(f"fixture -> {out}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fixture", type=Path)
    ap.add_argument("--prior", type=Path, help="prior path without .npz/.json")
    ap.add_argument("--names", type=Path, help="archive_parquet copy of the prior's source archive")
    ap.add_argument("--labels", type=Path, help="the app's audio labels CSV")
    ap.add_argument("--su", type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args(argv)
    if args.fixture:
        return fixture(args.fixture)
    for name in ("prior", "names", "labels", "su", "out"):
        if getattr(args, name) is None:
            ap.error(f"--{name} is required for a real export")

    meta, grid = load_prior(args.prior.with_suffix(".npz"), args.prior.with_suffix(".json"))
    names = species_names_from_parquet(args.names)
    prior_names = {k: names[k] for k in meta["species_keys"] if k in names}
    app_species = load_labels(args.labels)
    matched = match_names(prior_names, app_species)
    species = sorted(matched.values())
    unmatched = sorted(set(prior_names.values()) - set(matched.values()))
    print(f"{len(meta['species_keys'])} prior species, {len(prior_names)} named, {len(species)} matched "
          f"to the app; {len(unmatched)} unmatched: {unmatched[:30]}{' ...' if len(unmatched) > 30 else ''}")
    p = p_grid(meta, grid, matched, species)
    su = load_su_species(args.su)
    try:
        thresholds, factor, checks = calibrate(p, species, su)
    except CalibrationError as e:
        print(f"FAIL: {e}")
        return 1
    for k, c in checks.items():
        print(f"check {k}: {c['value']:.3f} (need {c['need']}) {'ok' if c['passed'] else 'FAIL'}")
    print(f"thresholds {thresholds} (factor {factor:.3f})")
    rmeta, raster = build_raster(meta["cells"], meta["spec"]["resolution"] if "spec" in meta else 5)
    side = write_asset(args.out, species=species, cells=meta["cells"], tiers=tiers_from_p(p, thresholds),
                       raster_meta=rmeta, raster=raster, thresholds=thresholds,
                       calibration={"factor": factor, "checks": checks},
                       gbif_doi=meta["gbif_doi"], licence=meta["licence"], attribution=meta["attribution"],
                       export_date=dt.date.today().isoformat())
    sizes = {f.name: f.stat().st_size for f in args.out.iterdir()}
    print(json.dumps({"cells": len(side["cells"]), "species": len(side["species"]), "bytes": sizes}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
