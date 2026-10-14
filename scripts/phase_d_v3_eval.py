#!/usr/bin/env python3
"""DKX (clean and noisy) scored the way the BirdNET Live app scores a file.

BirdNET+ V3 per-window embeddings (scripts/phase_d_v3_embed.py) are turned back
into species scores, max-pooled over a recording's 3 s windows, and compared
with BirdNET 2.4 (raw, + DOF filter, + a Danish head) on the SAME recordings:

  V3 raw            the app with its species filter off
  V3 + geo filter   the app's default: geoExclude, geo-model score >= 0.03 at the
                    recording's lat/lon/week
  V3 + DOF filter   species recorded in Denmark (our filter, mapped by name)
  V3 + <head>       with --v3-head: log s + alpha log q per window, then max
                    (scripts/phase_d_v3_train_head.py), alone and with the geo filter

CIs resample recordists, paired against the app default.

    python scripts/phase_d_v3_eval.py --head data/phase_d/dk_head_v5/head.pt --out data/phase_d/v3_dkx.csv
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from birdology.phase_b.metrics import cmap
from birdology.phase_d.birdnet3 import app_week, classifier_from_onnx, iter_window_cache, load_labels, max_by_recording
from birdology.phase_d.data import load_cache
from birdology.phase_d.evaluate import RecordistBootstrap, finite, label_matrix, log_scores, top1_hits
from birdology.phase_d.taxonomy import remap_code, resolve_remap, species_mask
from birdology.prior.tensor import load_prior

V3 = "apps/models/birdnet_v3"
AUDIO = f"{V3}/BirdNET+_V3.0-preview3.1_Global_10K-pruned_FP16.onnx"
LABELS = f"{V3}/BirdNET+_V3.0-preview3.1_Global_10K-pruned_Labels.csv"
GEO = f"{V3}/BirdNET+_Geomodel_V3.0.4_Global_10K-pruned_FP16.onnx"
GEO_LABELS = f"{V3}/BirdNET+_Geomodel_V3.0.4_Global_10K-pruned_Labels.txt"
GEO_THRESHOLD = 0.03                     # the app's default (settings_providers.dart)


def pooled_v3(d: Path, clf, head=None) -> dict[str, np.ndarray]:
    """filepath -> max over windows of log V3 score (+ alpha log q of ``head``)."""
    out = {}
    for fp, _, E in iter_window_cache(d):
        X = log_scores(1.0 / (1.0 + np.exp(-clf.logits(E))))
        if head is not None:
            X = X + head.alpha * head.log_q(E)
        names, M = max_by_recording(fp, X.astype(np.float32))
        out.update(zip(names, M))
    return out


def geo_scores(path: Path, rows: list[tuple[float, float, int]]) -> np.ndarray:
    import onnxruntime as ort

    s = ort.InferenceSession(str(path), providers=["CPUExecutionProvider"])
    name = s.get_inputs()[0].name
    return np.concatenate([s.run(None, {name: np.array([r], np.float32)})[0] for r in rows])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--v3-dir", type=Path, default=Path("data/phase_d/v3"))
    ap.add_argument("--snrs", type=int, nargs="+", default=[20, 10, 5, 0])
    ap.add_argument("--head", type=Path, help="a BirdNET 2.4 Danish head (head.pt) to compare with")
    ap.add_argument("--v3-head", type=Path, help="a Danish head on V3 windows (phase_d_v3_train_head.py)")
    ap.add_argument("--prior", default="data/prior/dof_pre2025/prior_dof_r4_full.npz")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)
    R = args.root
    rel = lambda p: Path(p) if Path(p).is_absolute() else R / p  # noqa: E731

    codes = [l.strip() for l in open(R / "apps/models/birdnet_labels.txt")]
    sci = [l.strip() for l in open(R / "apps/models/birdnet_labels_raw.txt")]
    col = {c: j for j, c in enumerate(codes)}
    remap = resolve_remap(codes, sci)
    bridge = json.load(open(R / "data/phase_a/code_to_gbif.json"))
    danish24 = species_mask(codes, remap, bridge, {int(k) for k in load_prior(rel(args.prior)).species_keys})

    v3_sci = load_labels(rel(LABELS))
    v3_idx = {s.lower(): j for j, s in enumerate(v3_sci)}
    to_v3 = np.array([v3_idx.get(s.lower(), -1) for s in sci])          # 2.4 axis -> V3 axis
    danish3 = np.zeros(len(v3_sci), bool)
    danish3[to_v3[(to_v3 >= 0) & danish24]] = True
    clf = classifier_from_onnx(rel(AUDIO))
    print(f"V3 axis {len(v3_sci)} species; {np.sum(to_v3 >= 0)} of BirdNET 2.4's {len(sci)} matched by name; "
          f"Danish species on V3 {danish3.sum()} (2.4: {danish24.sum()})")

    dkm = pq.read_table(R / "data/birdset/DKX/DKX/DKX_metadata_focal.parquet").to_pandas().set_index("filepath")
    emb = R / "data/phase_a/embeddings"
    conds = {"clean": (rel(args.v3_dir) / "DKX", emb / "DKX_focal_birdnet")}
    conds |= {f"{s} dB": (rel(args.v3_dir) / f"DKXN_snr{s:02d}", emb / f"DKXN_snr{s:02d}_birdnet") for s in args.snrs}
    v3 = {k: pooled_v3(d3, clf) for k, (d3, _) in conds.items()}
    v3h, v3h_name = None, None
    if args.v3_head:
        from birdology.phase_d.head import DanishHead
        h3 = DanishHead.load(rel(args.v3_head))
        v3h_name = f"V3 + {args.v3_head.parent.name}"
        v3h = {k: pooled_v3(d3, clf, h3) for k, (d3, _) in conds.items()}
    b24 = {k: load_cache([d]) for k, (_, d) in conds.items()}

    common = set.intersection(*[set(x) for x in v3.values()], *[set(c.filepaths) for c in b24.values()])
    t24 = {f: col.get(remap_code(dkm.loc[f, "ebird_code"], remap), -1) for f in common}
    lost = sorted({dkm.loc[f, "ebird_code"] for f in common if t24[f] < 0 or to_v3[t24[f]] < 0})
    common = sorted(f for f in common if t24[f] >= 0 and to_v3[t24[f]] >= 0)
    true24 = np.array([t24[f] for f in common])
    true3 = to_v3[true24]
    print(f"recordings scored: {len(common)}; true species missing from an axis: {lost}")
    bs = RecordistBootstrap(dkm.loc[common, "recordist"].to_numpy())

    geo_sci = [l.rstrip("\n").split("\t")[1] for l in open(rel(GEO_LABELS))]
    geo_to_v3 = np.array([v3_idx.get(s.lower(), -1) for s in geo_sci])
    keys = [(float(dkm.loc[f, "lat"]), float(dkm.loc[f, "long"]),
             app_week(int(dkm.loc[f, "date"][5:7]), int(dkm.loc[f, "date"][8:10]))) for f in common]
    uniq = sorted(set(keys))
    G = geo_scores(rel(GEO), uniq)
    geo_mask = np.zeros((len(uniq), len(v3_sci)), bool)
    for i in range(len(uniq)):
        hit = geo_to_v3[(G[i] >= GEO_THRESHOLD) & (geo_to_v3 >= 0)]
        geo_mask[i, hit] = True
    geo_rows = geo_mask[[uniq.index(k) for k in keys]]
    print(f"geo filter keeps {geo_rows.sum(1).mean():.0f} species per recording on average; "
          f"true species kept in {geo_rows[np.arange(len(common)), true3].mean():.3f} of recordings")

    head = None
    if args.head:
        from birdology.phase_d.head import DanishHead
        head = DanishHead.load(rel(args.head))

    multi = [[remap_code(c, remap) for c in dkm.loc[f, "ebird_code_multilabel"]] for f in common]
    Y24 = label_matrix(multi, col, len(codes))
    Y3 = np.zeros((len(common), len(v3_sci)), np.float32)
    r, c = np.nonzero(Y24)
    ok = to_v3[c] >= 0
    Y3[r[ok], to_v3[c[ok]]] = 1
    filt3 = np.where(danish3, 0.0, -np.inf)[None, :]
    filt24 = np.where(danish24, 0.0, -np.inf)[None, :]

    rows = []
    for cond in conds:
        S3 = np.stack([v3[cond][f] for f in common]).astype(np.float64)
        pos = {f: i for i, f in enumerate(b24[cond].filepaths)}
        ix = np.array([pos[f] for f in common])
        S24, E24 = b24[cond].scores[ix].astype(np.float64), b24[cond].embeddings[ix]
        sc = {"V3 raw (app, filter off)": (S3, true3, Y3),
              "V3 + geo filter (app default)": (np.where(geo_rows, S3, -np.inf), true3, Y3),
              "V3 + DOF filter": (S3 + filt3, true3, Y3),
              "BirdNET 2.4 raw": (log_scores(S24), true24, Y24),
              "BirdNET 2.4 + DOF filter": (log_scores(S24) + filt24, true24, Y24)}
        if head is not None:
            sc[f"BirdNET 2.4 + {args.head.parent.name}"] = (head.fuse(S24, E24), true24, Y24)
        if v3h is not None:
            H3 = np.stack([v3h[cond][f] for f in common]).astype(np.float64)
            sc[v3h_name] = (H3, true3, Y3)
            sc[f"{v3h_name} + geo filter"] = (np.where(geo_rows, H3, -np.inf), true3, Y3)
        ref = top1_hits(finite(sc["V3 + geo filter (app default)"][0]), true3)
        for name, (X, t, Y) in sc.items():
            Xf = finite(X)
            h = top1_hits(Xf, t)
            lo, hi = bs.ci(h - ref)
            rows.append(dict(condition=cond, scorer=name, top1=h.mean(), cmap=cmap(Xf, Y)[0],
                             d_app=(h - ref).mean(), d_app_lo=lo, d_app_hi=hi))
    df = pd.DataFrame(rows)
    pd.set_option("display.width", 200)
    print(df.round(3).to_string(index=False))
    print("\ntop-1 by condition:")
    print(df.pivot(index="scorer", columns="condition", values="top1")[list(conds)].round(3).to_string())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(rel(args.out), index=False)
    print("wrote", rel(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
