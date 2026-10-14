#!/usr/bin/env python3
"""DKX under noise: raw BirdNET, BirdNET + DOF filter and each head, per SNR.

Reads the clean DKX cache and every noisy copy (DKXN_snrXX_birdnet, built by
phase_d_mix_noise.py + cluster/phase_d_embed.sbatch), restricted to the
recordings present in all of them. CIs are recordist-clustered and paired with
raw BirdNET at the SAME SNR.

    python scripts/phase_d_noise_eval.py --heads data/phase_d/dk_head_v3/head.pt \\
        --out data/phase_d/noise_curve.csv
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from birdology.phase_b.metrics import cmap
from birdology.phase_d.data import load_cache
from birdology.phase_d.evaluate import RecordistBootstrap, finite, label_matrix, log_scores, top1_hits
from birdology.phase_d.head import DanishHead
from birdology.phase_d.taxonomy import remap_code, resolve_remap, species_mask
from birdology.prior.tensor import load_prior


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--heads", type=Path, nargs="*", default=[])
    ap.add_argument("--snrs", type=int, nargs="+", default=[20, 10, 5, 0])
    ap.add_argument("--prior", default="data/prior/dof_pre2025/prior_dof_r4_full.npz")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)
    R = args.root
    codes = [l.strip() for l in open(R / "apps/models/birdnet_labels.txt")]
    sci = [l.strip() for l in open(R / "apps/models/birdnet_labels_raw.txt")]
    col = {c: j for j, c in enumerate(codes)}
    remap = resolve_remap(codes, sci)
    bridge = json.load(open(R / "data/phase_a/code_to_gbif.json"))
    prior = Path(args.prior) if Path(args.prior).is_absolute() else R / args.prior
    danish = species_mask(codes, remap, bridge, {int(k) for k in load_prior(prior).species_keys})
    heads = [(p.parent.name, DanishHead.load(p)) for p in args.heads]
    dkm = pq.read_table(R / "data/birdset/DKX/DKX/DKX_metadata_focal.parquet").to_pandas().set_index("filepath")

    emb = R / "data/phase_a/embeddings"
    sets = {"clean": emb / "DKX_focal_birdnet"} | {f"{s} dB": emb / f"DKXN_snr{s:02d}_birdnet" for s in args.snrs}
    caches = {k: load_cache([d]) for k, d in sets.items()}
    common = sorted(set.intersection(*[set(c.filepaths) for c in caches.values()]))
    common = [f for f in common if remap_code(dkm.loc[f, "ebird_code"], remap) in col]
    true = np.array([col[remap_code(dkm.loc[f, "ebird_code"], remap)] for f in common])
    Y = label_matrix([[remap_code(c, remap) for c in dkm.loc[f, "ebird_code_multilabel"]] for f in common], col, len(codes))
    bs = RecordistBootstrap(dkm.loc[common, "recordist"].to_numpy())
    filt = np.where(danish, 0.0, -np.inf)[None, :]
    print(f"recordings in every set: {len(common)}, recordists {len(bs.recordists)}")

    rows = []
    for cond, c in caches.items():
        pos = {f: i for i, f in enumerate(c.filepaths)}
        ix = np.array([pos[f] for f in common])
        S, E = c.scores[ix].astype(np.float64), c.embeddings[ix]
        scorers = {"raw BirdNET": log_scores(S), "BirdNET + DOF filter": log_scores(S) + filt}
        for name, h in heads:
            fused = h.fuse(S, E)
            scorers[f"{name} fusion"] = fused
            scorers[f"{name} fusion + filter"] = fused + filt
        base = top1_hits(scorers["raw BirdNET"], true)
        base_f = top1_hits(finite(scorers["BirdNET + DOF filter"]), true)
        for k, X in scorers.items():
            Xf = finite(X)
            t1 = top1_hits(Xf, true)
            (lo, hi), (lo2, hi2) = bs.ci(t1 - base), bs.ci(t1 - base_f)
            rows.append(dict(condition=cond, scorer=k, top1=t1.mean(), cmap=cmap(Xf, Y)[0],
                             d_raw=(t1 - base).mean(), d_raw_lo=lo, d_raw_hi=hi,
                             d_filter=(t1 - base_f).mean(), d_filter_lo=lo2, d_filter_hi=hi2))
    df = pd.DataFrame(rows)
    pd.set_option("display.width", 220)
    print(df.round(3).to_string(index=False))
    print("\ntop-1 by condition:")
    print(df.pivot(index="scorer", columns="condition", values="top1")[list(caches)].round(3).to_string())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print("wrote", args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
