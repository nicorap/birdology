#!/usr/bin/env python3
"""Train the Danish head on European XCL embeddings and evaluate it once on DKX.

Splits
  train  XCL recordings inside the European box, XC id below the 90th percentile
  val    the same box, XC id at or above it -- the newest uploads, the ones
         BirdNET 2.4 is least likely to have trained on. Picks the head (kind,
         weight decay, learning rate) and the fusion weight alpha.
  test   DKX: Danish Xeno-canto uploads since 2025-01-01, disjoint from XCL.
Recordists who appear in DKX are removed from train and val.

XCL labels use an older eBird taxonomy than BirdNET's axis; labels are remapped
(birdology.phase_d.taxonomy) before anything else. The European rows XCL's r4
manifest dropped for lacking a date are mostly those renamed taxa, so their
embeddings live in a separate fill cache (--extra-cache), built with
scripts/phase_d_fill_manifest.py and scripts/cluster/phase_d_embed.sbatch.

    python scripts/phase_d_train_head.py --out data/phase_d/dk_head_v3 \\
        --extra-cache data/phase_a/embeddings/XCL_eu_undated_fill_birdnet
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch

from birdology.phase_b.metrics import cmap
from birdology.phase_d.data import in_box, load_cache, xc_id
from birdology.phase_d.evaluate import RecordistBootstrap, finite, label_matrix, log_scores, soft_hits, top1_hits
from birdology.phase_d.head import DanishHead, TrainConfig, train_head
from birdology.phase_d.taxonomy import remap_code, resolve_remap, species_mask
from birdology.prior.tensor import load_prior

log = logging.getLogger("phase_d_train_head")

GRID = [TrainConfig(kind=k, weight_decay=wd, lr=lr)
        for k in ("linear", "mlp") for wd, lr in ((1e-4, 1e-3), (1e-2, 1e-3), (1e-2, 1e-4), (1e-1, 1e-4))]
ALPHAS = (0.25, 0.5, 1.0, 2.0, 4.0)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--extra-cache", type=Path, nargs="*", default=[], help="more XCL BirdNET cache dirs (relative to --root or absolute)")
    ap.add_argument("--noisy-cache", type=Path, nargs="*", default=[],
                    help="BirdNET caches of the SAME recordings with noise mixed in (phase_d.embed_noisy_cli); "
                         "added to train as augmentation and to val, so selection sees both conditions")
    ap.add_argument("--prior", default="data/prior/dof_pre2025/prior_dof_r4_full.npz", help="DOF prior whose species define 'recorded in Denmark'")
    ap.add_argument("--min-train", type=int, default=5, help="recordings a species needs to get a head output")
    ap.add_argument("--val-quantile", type=float, default=0.9)
    ap.add_argument("--floor-q", type=float, default=1e-4)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    torch.set_num_threads(int(os.environ.get("SLURM_CPUS_PER_TASK", "8")))
    R = args.root
    rel = lambda p: p if Path(p).is_absolute() else R / p  # noqa: E731
    args.out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    codes = [l.strip() for l in open(R / "apps/models/birdnet_labels.txt")]
    sci = [l.strip() for l in open(R / "apps/models/birdnet_labels_raw.txt")]
    col = {c: j for j, c in enumerate(codes)}
    remap = resolve_remap(codes, sci)
    rm = lambda c: remap_code(c, remap)  # noqa: E731
    bridge = json.load(open(R / "data/phase_a/code_to_gbif.json"))
    dof_keys = {int(k) for k in load_prior(rel(args.prior)).species_keys}
    danish = species_mask(codes, remap, bridge, dof_keys)
    log.info("remapped %d old codes onto BirdNET's axis; %d axis species recorded in Denmark", len(remap), danish.sum())

    # ---- test set first: its recordists are excluded from training
    dkm = pq.read_table(R / "data/birdset/DKX/DKX/DKX_metadata_focal.parquet").to_pandas().set_index("filepath")
    dk = load_cache([R / "data/phase_a/embeddings/DKX_focal_birdnet"])
    dk_recordists = set(dkm.loc[dk.filepaths, "recordist"])
    dk_ids = {xc_id(f) for f in dkm.index}

    # ---- European XCL pool
    xm = pq.read_table(R / "data/birdset/XCL/XCL/XCL_metadata.parquet",
                       columns=["filepath", "lat", "long", "recordist", "ebird_code"]).to_pandas().reset_index(drop=True)
    xm = xm[in_box(xm.lat, xm.long) & ~xm.recordist.isin(dk_recordists)].drop_duplicates("filepath").set_index("filepath")
    xm["ebird_code"] = xm.ebird_code.map(rm)
    leak = {xc_id(f) for f in xm.index} & dk_ids
    if leak:
        raise SystemExit(f"{len(leak)} DKX recordings are in the XCL training pool, e.g. {sorted(leak)[:3]}")
    xc = load_cache([R / "data/phase_a/embeddings/XCL_birdnet"] + [rel(p) for p in args.extra_cache], keep=list(xm.index))
    y = np.array([col.get(c, -1) for c in xm.loc[xc.filepaths, "ebird_code"]])
    xc = xc.take(np.flatnonzero(y >= 0)); y = y[y >= 0]
    ids = np.array([xc_id(f) for f in xc.filepaths])
    val_id = int(np.quantile(ids, args.val_quantile))
    is_val = ids >= val_id
    counts = np.bincount(y[~is_val], minlength=len(codes))
    classes = np.flatnonzero(counts >= args.min_train)
    head_idx = {c: i for i, c in enumerate(classes)}
    tr, va = ~is_val & np.isin(y, classes), is_val & np.isin(y, classes)
    log.info("European XCL: %d labelled recordings (%d val, XC id >= %d); head classes %d (%d recorded in Denmark)",
             len(y), is_val.sum(), val_id, len(classes), danish[classes].sum())

    E_tr, y_tr = xc.embeddings[tr], y[tr]
    E_va, S_va, y_va_axis = xc.embeddings[va], xc.scores[va], y[va]
    if args.noisy_cache:
        noisy = load_cache([rel(p) for p in args.noisy_cache], keep=xc.filepaths)
        pos = {f: i for i, f in enumerate(xc.filepaths)}
        ni = np.array([pos[f] for f in noisy.filepaths])
        ntr, nva = tr[ni], va[ni]
        E_tr, y_tr = np.concatenate([E_tr, noisy.embeddings[ntr]]), np.concatenate([y_tr, y[ni][ntr]])
        E_va = np.concatenate([E_va, noisy.embeddings[nva]])
        S_va = np.concatenate([S_va, noisy.scores[nva]])
        y_va_axis = np.concatenate([y_va_axis, y[ni][nva]])
        log.info("noisy augmentation: +%d train, +%d val rows", ntr.sum(), nva.sum())
    mu, sd = E_tr.mean(0), E_tr.std(0) + 1e-6
    z = lambda a: torch.from_numpy(((a - mu) / sd).astype(np.float32))  # noqa: E731
    Xtr, ytr = z(E_tr), torch.tensor([head_idx[c] for c in y_tr])
    Xva, yva = z(E_va), torch.tensor([head_idx[c] for c in y_va_axis])
    Sva = S_va.astype(np.float64)
    log.info("raw BirdNET top-1 on val: %.3f", (Sva.argmax(1) == y_va_axis).mean())

    rows, heads = [], []
    for cfg in GRID:
        res = train_head(Xtr, ytr, Xva, yva, len(classes), cfg)
        head = DanishHead(model=res.model, kind=cfg.kind, classes=classes, n_axis=len(codes), mu=mu, sd=sd,
                          floor_q=args.floor_q, remap=remap)
        lq = head.log_q(E_va)
        log.info("%s wd=%g lr=%g: val loss %.3f (epoch %d), head val top-1 %.3f",
                 cfg.kind, cfg.weight_decay, cfg.lr, res.val_loss, res.best_epoch, res.val_top1)
        for a in ALPHAS:
            rows.append(dict(kind=cfg.kind, wd=cfg.weight_decay, lr=cfg.lr, alpha=a, val_loss=res.val_loss,
                             head_val_top1=res.val_top1,
                             fusion_val_top1=float(((log_scores(Sva) + a * lq).argmax(1) == y_va_axis).mean()),
                             model=len(heads)))
        heads.append(head)
    grid = pd.DataFrame(rows)
    grid.drop(columns="model").to_csv(args.out / "val_grid.csv", index=False)
    best_head = heads[int(grid.loc[grid.val_loss.idxmin(), "model"])]
    pick = grid.loc[grid.fusion_val_top1.idxmax()]
    fused = heads[int(pick.model)]
    fused.alpha = float(pick.alpha)
    fused.meta = {"val_xc_id_min": val_id, "val_fusion_top1": float(pick.fusion_val_top1), "kind": pick.kind,
                  "weight_decay": float(pick.wd), "lr": float(pick.lr), "min_train": args.min_train,
                  "n_train": int(len(y_tr)), "n_val": int(len(y_va_axis)),
                  "noisy_cache": [str(p) for p in args.noisy_cache]}
    fused.save(args.out / "head.pt")
    log.info("chosen on val: fusion %s wd=%g lr=%g alpha=%g (val top-1 %.3f)",
             pick.kind, pick.wd, pick.lr, pick.alpha, pick.fusion_val_top1)

    # ---- DKX, once
    true = np.array([col.get(rm(c), -1) for c in dkm.loc[dk.filepaths, "ebird_code"]])
    ok = np.flatnonzero(true >= 0)
    dk, true = dk.take(ok), true[ok]
    multi = [[rm(c) for c in m] for m in dkm.loc[dk.filepaths, "ebird_code_multilabel"]]
    Y = label_matrix(multi, col, len(codes))
    S = dk.scores.astype(np.float64)
    filt = np.where(danish, 0.0, -np.inf)[None, :]
    fusion = fused.fuse(S, dk.embeddings)
    scorers = {"raw BirdNET": log_scores(S), "BirdNET + DOF filter": log_scores(S) + filt,
               "head alone": best_head.head_only(dk.embeddings), "fusion": fusion, "fusion + DOF filter": fusion + filt}
    bs = RecordistBootstrap(dkm.loc[dk.filepaths, "recordist"].to_numpy())
    base, base_f = top1_hits(scorers["raw BirdNET"], true), top1_hits(finite(scorers["BirdNET + DOF filter"]), true)
    out = []
    for name, X in scorers.items():
        Xf = finite(X)
        t1 = top1_hits(Xf, true)
        (lo, hi), (lo2, hi2) = bs.ci(t1 - base), bs.ci(t1 - base_f)
        out.append(dict(scorer=name, cmap=cmap(Xf, Y)[0], top1=t1.mean(), soft_top1=soft_hits(Xf, multi, codes).mean(),
                        d_raw=(t1 - base).mean(), d_raw_lo=lo, d_raw_hi=hi,
                        d_filter=(t1 - base_f).mean(), d_filter_lo=lo2, d_filter_hi=hi2))
    res = pd.DataFrame(out)
    res.to_csv(args.out / "dkx_scores.csv", index=False)
    log.info("DKX: %d recordings, %d recordists, head covers %.1f%% of true species\n%s", len(true),
             len(bs.recordists), 100 * np.isin(true, classes).mean(), res.round(3).to_string(index=False))
    log.info("done in %.0f s -> %s", time.time() - t0, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
