#!/usr/bin/env python3
"""Where does BirdNET fail on Danish birds? Per-recording error analysis on DKX.

An error is top-1 != primary label. It is "soft" when the predicted species is
one of the recording's background species (BirdNET heard a bird that really
is there) and "hard" otherwise. Reported for raw BirdNET and for BirdNET
restricted to species DOF has recorded in Denmark, plus the species the Danish
filter removes outright (rarities DOF had not recorded before 2025).

    python scripts/phase_d_dkx_errors.py --out data/phase_d/dkx_hard_errors.csv
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from birdology.phase_d.data import load_cache
from birdology.phase_d.evaluate import finite, log_scores
from birdology.phase_d.taxonomy import remap_code, resolve_remap, species_mask
from birdology.prior.tensor import load_prior


def analyse(X, true, codes, sci, multi, danish, recordist, quality, top_score, tag):
    genus = np.array([s.split()[0] for s in sci])
    pred = X.argmax(1)
    rank = (X > X[np.arange(len(X)), true][:, None]).sum(1) + 1
    bg = np.array([codes[p] in set(m) - {codes[t]} for p, m, t in zip(pred, multi, true)])
    wrong = pred != true
    hard = wrong & ~bg
    print(f"\n===== {tag}: top-1 {1 - wrong.mean():.3f}  top-3 {(rank <= 3).mean():.3f}  top-5 {(rank <= 5).mean():.3f}")
    print(f"errors {wrong.sum()}: background species {(wrong & bg).sum()} | non-Danish species "
          f"{(wrong & ~danish[pred]).sum()} | same genus {(wrong & (genus[pred] == genus[true])).sum()} | "
          f"true species outside top-5 {(wrong & (rank > 5)).sum()}; hard errors {hard.sum()}")
    print("most common hard confusions (true -> predicted):")
    for (t, p), n in Counter((sci[t], sci[p]) for t, p in zip(true[hard], pred[hard])).most_common(20):
        print(f"  {n:2d}  {t:28s} -> {p}")
    per = pd.DataFrame({"species": [sci[t] for t in true], "ok": ~hard, "rec": recordist}).groupby("species").agg(
        n=("ok", "size"), accuracy=("ok", "mean"), recordists=("rec", "nunique"))
    print("species with >= 3 recordings and accuracy < 0.7:")
    print(per[(per.n >= 3) & (per.accuracy < 0.7)].sort_values("accuracy").to_string())
    print("hard-error rate by XC quality:", pd.Series(hard).groupby(quality).mean().round(3).to_dict())
    bins = [(0, .25), (.25, .5), (.5, .8), (.8, 1.01)]
    print("hard-error rate by BirdNET's top score:",
          {f"{lo}-{hi}": (round(float(hard[(top_score >= lo) & (top_score < hi)].mean()), 3),
                          int(((top_score >= lo) & (top_score < hi)).sum())) for lo, hi in bins})
    return hard, pred


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--prior", default="data/prior/dof_pre2025/prior_dof_r4_full.npz")
    ap.add_argument("--out", type=Path, default=None, help="CSV of hard errors under the Danish filter")
    args = ap.parse_args(argv)
    R = args.root
    codes = [l.strip() for l in open(R / "apps/models/birdnet_labels.txt")]
    sci = [l.strip() for l in open(R / "apps/models/birdnet_labels_raw.txt")]
    col = {c: j for j, c in enumerate(codes)}
    remap = resolve_remap(codes, sci)
    bridge = json.load(open(R / "data/phase_a/code_to_gbif.json"))
    prior = Path(args.prior) if Path(args.prior).is_absolute() else R / args.prior
    danish = species_mask(codes, remap, bridge, {int(k) for k in load_prior(prior).species_keys})
    dkm = pq.read_table(R / "data/birdset/DKX/DKX/DKX_metadata_focal.parquet").to_pandas().set_index("filepath")
    c = load_cache([R / "data/phase_a/embeddings/DKX_focal_birdnet"])
    meta = dkm.loc[c.filepaths]
    true = np.array([col.get(remap_code(x, remap), -1) for x in meta.ebird_code])
    ok = true >= 0
    print(f"recordings {len(true)}, scoreable {ok.sum()}")
    meta, true, S = meta[ok], true[ok], c.scores[ok].astype(np.float64)
    multi = [[remap_code(x, remap) for x in m] for m in meta.ebird_code_multilabel]
    common = dict(true=true, codes=codes, sci=sci, multi=multi, danish=danish, recordist=meta.recordist.to_numpy(),
                  quality=meta.quality.to_numpy(), top_score=S.max(1))
    analyse(log_scores(S), tag="raw BirdNET", **common)
    hard, pred = analyse(finite(log_scores(S) + np.where(danish, 0.0, -np.inf)[None, :]),
                         tag="BirdNET + DOF species filter", **common)
    removed = Counter(sci[t] for t in true if not danish[t])
    print("\nDKX species the Danish filter removes (not recorded by DOF before 2025):", dict(removed))
    if args.out:
        out = meta.loc[hard, ["id", "ebird_code", "recordist", "quality"]].assign(predicted=[codes[p] for p in pred[hard]])
        out.to_csv(args.out)
        print(f"wrote {len(out)} hard errors to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
