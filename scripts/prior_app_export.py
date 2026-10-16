#!/usr/bin/env python3
"""Export the app's commonness asset from the DOF prior.

Real export (on the fleet):
    python scripts/prior_app_export.py \\
        --counts data/prior/occurrences/dof/counts_r5.parquet \\
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

import h3
import pyarrow.parquet as pq

from birdology.phase_d.birdnet3 import load_labels
from birdology.prior.app_tiers import (
    BASE_THRESHOLDS, CalibrationError, build_raster, calibrate, check_su_per_species, force_su_rare,
    match_names, shares_from_counts,
    species_names_from_parquet, synthetic_inputs, tiers_from_p, unmatched_names, write_asset,
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
    ap.add_argument("--counts", type=Path, help="DOF counts parquet (species_key, cell, week, n)")
    ap.add_argument("--gbif-doi", default="10.15468/dl.3j4czw", help="GBIF download DOI for attribution")
    ap.add_argument("--window", type=int, default=2, help="weeks either side pooled into a share")
    ap.add_argument("--min-records", type=int, default=500,
                    help="pooled cell-week records below which the national share is used")
    ap.add_argument("--names", type=Path, help="archive_parquet copy of the prior's source archive")
    ap.add_argument("--labels", type=Path, help="the app's audio labels CSV")
    ap.add_argument("--su", type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args(argv)
    if args.fixture:
        return fixture(args.fixture)
    for name in ("counts", "names", "labels", "su", "out"):
        if getattr(args, name) is None:
            ap.error(f"--{name} is required for a real export")

    counts = pq.read_table(args.counts, columns=["species_key", "cell", "week", "n"])
    names = species_names_from_parquet(args.names)
    keys = sorted(set(counts.column("species_key").to_pylist()) & set(names))
    count_names = {k: names[k] for k in keys}
    app_species = load_labels(args.labels)
    matched = match_names(count_names, app_species)
    species = sorted(matched.values())
    unmatched = unmatched_names(count_names, matched)
    print(f"{len(keys)} species in the counts, {len(species)} matched to the app; "
          f"{len(unmatched)} unmatched: {unmatched}")
    key_of = {name: key for key, name in matched.items()}
    cells, p = shares_from_counts(counts, [key_of[s] for s in species], args.window, args.min_records)
    su = load_su_species(args.su)
    try:
        thresholds, factor, checks = calibrate(p, species, su)
        tiers, forced = force_su_rare(tiers_from_p(p, thresholds), species, su)
        checks = {"su_rare": check_su_per_species(tiers, species, su), **checks}
    except CalibrationError as e:
        print(f"FAIL: {e}")
        return 1
    for k, c in checks.items():
        print(f"check {k}: {c['value']:.3f} (need {c['need']}) {'ok' if c['passed'] else 'FAIL'}")
    if not all(c["passed"] for c in checks.values()):
        print("FAIL: a check did not pass")
        return 1
    print(f"thresholds {thresholds} (factor {factor:.3f}); {len(forced)} SU species forced rare")
    rmeta, raster = build_raster(cells, h3.get_resolution(cells[0]))
    side = write_asset(args.out, species=species, cells=cells, tiers=tiers,
                       raster_meta=rmeta, raster=raster, thresholds=thresholds,
                       calibration={"factor": factor, "checks": checks, "window": args.window,
                                    "min_records": args.min_records, "source": "DOF counts",
                                    "su_forced_rare": forced},
                       gbif_doi=args.gbif_doi, licence="CC BY 4.0",
                       attribution=f"Derived from DOFbasen records (Dansk Ornitologisk Forening), GBIF occurrence "
                                   f"download https://doi.org/{args.gbif_doi}. Released under CC BY 4.0 "
                                   "(https://creativecommons.org/licenses/by/4.0/).",
                       export_date=dt.date.today().isoformat())
    sizes = {f.name: f.stat().st_size for f in args.out.iterdir()}
    print(json.dumps({"cells": len(side["cells"]), "species": len(side["species"]), "bytes": sizes}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
