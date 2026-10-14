#!/usr/bin/env python3
"""Mix bird-free ESC-50 noise into a test set's tar shards at fixed SNRs.

Writes <out>/snrXX/<NAME>_snrXX/<shard> with the SAME member names, so the
original manifest and the BirdNET embed pipeline apply unchanged. Every SNR of
one recording gets the same noise draw (seeded by the member name).

Use ESC-50 fold 5 for test sets and folds 1-4 for training audio, so no noise
clip is shared between them. One shard per call: the cluster runs one array
task per shard (writing four noisy copies of a 1 GB shard serially took about
1.5 h on the home NFS).

    python scripts/phase_d_mix_noise.py --src data/birdset/DKX/DKX --name DKXN \\
        --shard DKX_focal_shard_0004.tar.gz --folds 5 --snrs 20 10 5 0
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from birdology.phase_d.noise import NoisePool, is_bird_label, mix_shard


def bird_free_clips(root: Path, folds: list[int], bird_max: float) -> tuple[list[str], pd.DataFrame]:
    """ESC clips in ``folds`` whose top BirdNET bird score is at most ``bird_max``."""
    meta = pd.read_csv(root / "data/noise/ESCN/escn_meta.csv")
    caches = sorted((root / "data/phase_a/embeddings/ESCN_birdnet").glob("*.parquet"))
    if not caches:
        raise SystemExit("no BirdNET scores for the noise clips: run phase_d_noise_pack.py and embed ESCN first")
    scored = pq.read_table(caches[0], columns=["filepath", "logits"]).to_pandas()
    sci = [l.strip() for l in open(root / "apps/models/birdnet_labels_raw.txt")]
    bird = np.array([is_bird_label(s) for s in sci])
    scored["bird_max"] = [float(np.asarray(v)[bird].max()) for v in scored.logits]
    meta = meta.merge(scored[["filepath", "bird_max"]], left_on="filename", right_on="filepath")
    pool = meta[meta.fold.isin(folds)]
    return pool[pool.bird_max <= bird_max].filename.tolist(), pool[pool.bird_max > bird_max]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--src", type=Path, required=True, help="directory of the clean tar shards")
    ap.add_argument("--name", required=True, help="output set name, e.g. DKXN")
    ap.add_argument("--shard", required=True, help="basename of the one shard to mix")
    ap.add_argument("--folds", type=int, nargs="+", default=[5])
    ap.add_argument("--snrs", type=int, nargs="+", default=[20, 10, 5, 0])
    ap.add_argument("--bird-max", type=float, default=0.25)
    args = ap.parse_args(argv)
    R = args.root
    src = (args.src if args.src.is_absolute() else R / args.src) / args.shard
    keep, dropped = bird_free_clips(R, args.folds, args.bird_max)
    print(f"noise folds {args.folds}: {len(keep)} clips kept, {len(dropped)} dropped for birds "
          f"{dropped.category.value_counts().to_dict()}")
    pool = NoisePool.from_files([R / "data/noise/ESC-50-master/audio" / f for f in keep])
    outs = {float(s): R / f"data/birdset/{args.name}/snr{s:02d}/{args.name}_snr{s:02d}/{args.shard}" for s in args.snrs}
    written, skipped = mix_shard(src, outs, pool)
    print(f"{args.shard}: {written} recordings x {len(outs)} SNRs; skipped {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
