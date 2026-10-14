#!/usr/bin/env python3
"""Pack outdoor/urban ESC-50 clips into a tar shard + manifest for BirdNET screening.

Freesound recordings of wind, rain or traffic often carry birdsong in the
background; mixing those into a bird recording would add a second, unlabelled
bird. BirdNET scores the packed clips (cluster/phase_d_embed.sbatch), and
phase_d_mix_noise.py drops every clip whose top bird score exceeds a threshold.

ESC-50 (https://github.com/karolpiczak/ESC-50) is CC BY-NC 3.0: fine for this
research, not for a shipped app.

    python scripts/phase_d_noise_pack.py --esc data/noise/ESC-50-master
"""
from __future__ import annotations

import argparse
import tarfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

#: Outdoor and urban backgrounds a Danish recorder meets; no animal classes.
CLASSES = ("engine", "car_horn", "train", "siren", "airplane", "helicopter",
           "church_bells", "footsteps", "wind", "rain")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--esc", type=Path, default=Path("data/noise/ESC-50-master"))
    ap.add_argument("--classes", nargs="+", default=list(CLASSES))
    args = ap.parse_args(argv)
    R = args.root
    esc = args.esc if args.esc.is_absolute() else R / args.esc
    meta = pd.read_csv(esc / "meta/esc50.csv")
    meta = meta[meta.category.isin(args.classes)].reset_index(drop=True)
    shard_dir = R / "data/noise/ESCN/ESCN"
    shard_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(shard_dir / "ESCN_shard_0001.tar.gz", "w:gz") as tar:
        for fn in meta.filename:
            tar.add(esc / "audio" / fn, arcname=fn)
    n = len(meta)
    pq.write_table(pa.table({
        "filepath": meta.filename.tolist(), "cell": [""] * n, "week": pa.nulls(n, pa.int8()),
        "ebird_code": meta.category.tolist(), "split": [f"fold{f}" for f in meta.fold],
        "lat": pa.nulls(n, pa.float64()), "lon": pa.nulls(n, pa.float64()),
    }), R / "data/phase_a/ESCN_manifest.parquet")
    meta.to_csv(R / "data/noise/ESCN/escn_meta.csv", index=False)
    print(n, "clips;", meta.groupby("fold").size().to_dict())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
