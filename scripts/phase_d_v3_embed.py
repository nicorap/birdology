#!/usr/bin/env python3
"""Per-window BirdNET+ V3 embeddings of one tar shard (birdology.phase_d.birdnet3).

Writes <out>/<shard stem>.parquet with one row per 3 s window: filepath,
window (0-based), embedding (1280 float32). All-zero windows (digital
silence, which some Xeno-canto files are padded with) are skipped: the model
returns NaN for them. Species scores are not stored;
birdnet3.scores rebuilds them from the embeddings. Files that fail to decode
go to <shard stem>.failures.json. With --manifest only its filepaths are
embedded (tar members are matched exactly, then by basename).

Needs onnxruntime (apps/venvs/onnx on the fleet):

    python scripts/phase_d_v3_embed.py --shard data/birdset/DKX/DKX/DKX_focal_shard_0001.tar.gz \\
        --out data/phase_d/v3/DKX
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import soundfile as sf

from birdology.phase_a.embed import _audio_member_names, _resolve_members, iter_shard_audio
from birdology.phase_d.birdnet3 import Embedder, resample, windows

log = logging.getLogger("phase_d_v3_embed")
MODEL = "apps/models/birdnet_v3/BirdNET+_V3.0-preview3.1_Global_10K-pruned_FP16.onnx"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--shard", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, help="parquet with a filepath column; default: every audio member")
    ap.add_argument("--model", type=Path, default=Path(MODEL))
    ap.add_argument("--max-windows", type=int, default=0,
                    help="keep at most this many windows per recording (the first ones); 0 keeps all")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    rel = lambda p: p if p.is_absolute() else args.root / p  # noqa: E731
    shard, out = rel(args.shard), rel(args.out)
    stem = shard.name.split(".")[0]
    table_path, fail_path = out / f"{stem}.parquet", out / f"{stem}.failures.json"
    if table_path.exists() and not args.force:
        log.info("%s exists; skipping (use --force to redo)", table_path)
        return 0
    out.mkdir(parents=True, exist_ok=True)

    members = _audio_member_names(shard)
    if args.manifest:
        wanted = set(pq.read_table(rel(args.manifest), columns=["filepath"]).column("filepath").to_pylist())
        resolved = _resolve_members(members, wanted)
        if members and not resolved:
            raise SystemExit(f"{shard.name}: zero overlap with the manifest (e.g. {members[:2]})")
    else:
        resolved = {m: m for m in members}

    embed = Embedder(rel(args.model))
    log.info("%s: %d members to embed; providers %s", shard.name, len(resolved), embed.session.get_providers())
    F, I, E, failed = [], [], [], {}
    t0 = time.time()
    for name, payload in iter_shard_audio(shard):
        canonical = resolved.get(name)
        if canonical is None:
            continue
        try:
            x, sr = sf.read(io.BytesIO(payload), dtype="float32", always_2d=False)
            if x.size == 0:
                raise ValueError("decoded to 0 samples")
            w = windows(resample(x, sr))
            w = w[: args.max_windows] if args.max_windows else w
            idx = np.flatnonzero(np.abs(w).max(1) > 0)     # V3 returns NaN for an all-zero window
            if idx.size == 0:
                raise ValueError("digital silence: every window is all zeros")
            _, e = embed(w[idx])
        except Exception as exc:  # one bad file must not lose the shard
            failed[canonical] = f"{type(exc).__name__}: {exc}"
            continue
        F += [canonical] * len(e)
        I += idx.tolist()
        E.append(e.astype(np.float32))
    emb = np.concatenate(E) if E else np.zeros((0, 1280), np.float32)
    table = pa.table({
        "filepath": pa.array(F, pa.string()),
        "window": pa.array(I, pa.int32()),
        "embedding": pa.FixedSizeListArray.from_arrays(pa.array(emb.ravel(), pa.float32()), emb.shape[1]),
    })
    tmp = table_path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp)
    tmp.rename(table_path)
    fail_path.write_text(json.dumps(failed, indent=1))
    log.info("%s: %d recordings, %d windows, %d failed in %.0f s -> %s", shard.name, len(set(F)), len(F),
             len(failed), time.time() - t0, table_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
