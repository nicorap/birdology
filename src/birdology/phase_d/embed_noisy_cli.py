"""Embed one tar shard with noise mixed into every recording before encoding.

The noise-augmented twin of ``birdology.phase_a.embed_cli``: same manifest
slicing, completion check and output schema, but the encoder is wrapped in
``NoisyEncoder`` -- bird-free ESC-50 clips (screened with BirdNET) at a
per-recording SNR drawn from [--snr-min, --snr-max]. Mixing happens in
memory, so no noisy audio is ever written to storage.

Use ESC-50 folds 1-4 here; fold 5 is reserved for the noisy test sets.

    python3 -m birdology.phase_d.embed_noisy_cli --shard XCL_shard_0001.tar.gz \\
        --manifest XCL_eu_all_manifest.parquet --out XCL_eu_noisy_birdnet \\
        --esc-root /srv/home/rapnic/birdology --folds 1 2 3 4
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pyarrow.parquet as pq

from birdology.phase_a.embed import _audio_member_names, embed_shard, shard_is_complete
from birdology.phase_a.embed_cli import slice_manifest_to_shard
from birdology.phase_a.encoders import load_encoder_with_logits
from birdology.phase_d.noise import NoisePool, NoisyEncoder, screened_clips

log = logging.getLogger("phase_d.embed_noisy")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--shard", required=True, type=Path)
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"),
                    help="birdology root holding data/noise and the ESC BirdNET scores")
    ap.add_argument("--encoder", default="birdnet")
    ap.add_argument("--folds", type=int, nargs="+", default=[1, 2, 3, 4])
    ap.add_argument("--snr-min", type=float, default=0.0)
    ap.add_argument("--snr-max", type=float, default=20.0)
    ap.add_argument("--bird-max", type=float, default=0.25)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    R = args.root

    manifest = pq.read_table(args.manifest)
    members = set(_audio_member_names(args.shard))
    shard_manifest = slice_manifest_to_shard(manifest, members)
    if shard_manifest.num_rows == 0:
        log.info("%s: no manifest rows in this shard, nothing to do", args.shard.name)
        return 0
    if not args.force and shard_is_complete(args.shard, shard_manifest, args.out):
        log.error("%s: output under %s already complete; refusing without --force", args.shard.name, args.out)
        return 2

    sci = [l.strip() for l in open(R / "apps/models/birdnet_labels_raw.txt")]
    scores = sorted((R / "data/phase_a/embeddings/ESCN_birdnet").glob("*.parquet"))
    if not scores:
        log.error("no BirdNET scores for the ESC noise clips under %s", R / "data/phase_a/embeddings/ESCN_birdnet")
        return 2
    keep, dropped = screened_clips(R / "data/noise/ESCN/escn_meta.csv", scores[0], sci, args.folds, args.bird_max)
    pool = NoisePool.from_files([R / "data/noise/ESC-50-master/audio" / f for f in keep])
    log.info("noise pool: folds %s, %d clips (%d dropped for birds), SNR %.0f-%.0f dB",
             args.folds, len(pool.names), len(dropped), args.snr_min, args.snr_max)

    encoder = NoisyEncoder(load_encoder_with_logits(args.encoder), pool, (args.snr_min, args.snr_max))
    try:
        result = embed_shard(args.shard, shard_manifest, encoder, out_dir=args.out, with_logits=True)
    finally:
        encoder.close()
    log.info("%s: embedded=%d/expected=%d failed=%d schema=%s", result.shard, result.embedded,
             result.expected, len(result.failed), result.schema_variant)
    if result.embedded == 0 and result.expected > 0:
        log.error("%s: every expected file failed", result.shard)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
