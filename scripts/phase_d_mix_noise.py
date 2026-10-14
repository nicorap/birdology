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

from birdology.phase_d.noise import NoisePool, mix_shard, screened_clips


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
    scores = sorted((R / "data/phase_a/embeddings/ESCN_birdnet").glob("*.parquet"))
    if not scores:
        raise SystemExit("no BirdNET scores for the noise clips: run phase_d_noise_pack.py and embed ESCN first")
    sci = [l.strip() for l in open(R / "apps/models/birdnet_labels_raw.txt")]
    keep, dropped = screened_clips(R / "data/noise/ESCN/escn_meta.csv", scores[0], sci, args.folds, args.bird_max)
    print(f"noise folds {args.folds}: {len(keep)} clips kept, {len(dropped)} dropped for birds "
          f"{sorted({d[1] for d in dropped})}")
    pool = NoisePool.from_files([R / "data/noise/ESC-50-master/audio" / f for f in keep])
    outs = {float(s): R / f"data/birdset/{args.name}/snr{s:02d}/{args.name}_snr{s:02d}/{args.shard}" for s in args.snrs}
    written, skipped = mix_shard(src, outs, pool)
    print(f"{args.shard}: {written} recordings x {len(outs)} SNRs; skipped {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
