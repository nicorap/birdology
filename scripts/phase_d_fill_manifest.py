#!/usr/bin/env python3
"""Manifest of the European XCL recordings the r4 manifest dropped, for BirdNET embedding.

XCL_r4_manifest keeps only recordings the Xeno-canto date lookup could date.
That lookup queries XC by the eBird code's CURRENT scientific name, which XC
does not use for renamed taxa (brnowl -> Tyto furcata, lirplo -> Thinornis
dubius, hergul -> no name at all), so those species come back "undated" and are
dropped wholesale -- measured: 4,380 European recordings, almost all of them
Barn Owl, Whimbrel, Little Ringed Plover, Goshawk, Herring Gull, Little
Bittern, redpolls, House Martin, Nutcracker and Dotterel.

The head needs no dates, so this writes them to a fill manifest with week null
and cell from coordinates. ebird_code stays XCL's own (unremapped).

    python scripts/phase_d_fill_manifest.py
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from birdology.phase_d.data import in_box
from birdology.prior.indexing import latlng_cells


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)
    R = args.root
    out = args.out or R / "data/phase_a/XCL_eu_undated_fill_manifest.parquet"
    kept = set(pq.read_table(R / "data/phase_a/XCL_r4_manifest.parquet", columns=["filepath"]).column("filepath").to_pylist())
    xm = pq.read_table(R / "data/birdset/XCL/XCL/XCL_metadata.parquet",
                       columns=["filepath", "lat", "long", "ebird_code"]).to_pandas().reset_index(drop=True)
    eu = xm[in_box(xm.lat, xm.long) & ~xm.filepath.isin(kept)].drop_duplicates("filepath")
    n = len(eu)
    table = pa.table({
        "filepath": pa.array(eu.filepath.tolist(), pa.string()),
        "cell": pa.array(list(latlng_cells(eu.lat.to_numpy(), eu.long.to_numpy(), 4)), pa.string()),
        "week": pa.nulls(n, pa.int8()),
        "ebird_code": pa.array(eu.ebird_code.tolist(), pa.string()),
        "split": pa.array(["train"] * n, pa.string()),
        "lat": pa.array(eu.lat.to_numpy(), pa.float64()),
        "lon": pa.array(eu.long.to_numpy(), pa.float64()),
    }).replace_schema_metadata({"purpose": "European XCL rows the r4 manifest dropped as undated (phase_d)"})
    pq.write_table(table, out)
    print(f"{n} rows -> {out}")
    print(eu.ebird_code.value_counts().head(15).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
