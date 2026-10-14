#!/usr/bin/env python3
"""Train a Danish head on BirdNET+ V3 (the Live app's model) per-window embeddings.

Same splits and model search as scripts/phase_d_train_head.py (BirdNET 2.4):
European XCL, DKX recordists removed, the newest 10% of Xeno-canto IDs held
out for validation. The difference is the unit. The app scores 3 s windows and
then takes the best window, so the head is trained on windows, not on whole
recordings: each training recording contributes its --top-k windows where V3
gives the labelled species the highest score (where the bird most likely
calls; Xeno-canto labels say only that it is somewhere in the file).

Selection is done the way the app is scored: per window
    fused = log s_V3 + alpha * log q_head
then max over the recording's windows, top-1 against the label.

The app shows a species when its score passes a threshold (0.35). The head's
``meta["app_offset"]`` is the offset b in sigmoid(fused + b) (the ``cal`` app
export, birdology.phase_d.export_onnx) at which as many validation
recordings pass the threshold as with V3 alone.

    python scripts/phase_d_v3_train_head.py --out data/phase_d/v3_head_v1
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

from birdology.phase_d.birdnet3 import classifier_from_onnx, iter_window_cache, load_labels, max_by_recording, top_windows
from birdology.phase_d.data import in_box, xc_id
from birdology.phase_d.evaluate import log_scores
from birdology.phase_d.head import DanishHead, TrainConfig, train_head
from birdology.phase_d.taxonomy import remap_code, resolve_remap

log = logging.getLogger("phase_d_v3_train_head")

V3 = "apps/models/birdnet_v3"
AUDIO = f"{V3}/BirdNET+_V3.0-preview3.1_Global_10K-pruned_FP16.onnx"
LABELS = f"{V3}/BirdNET+_V3.0-preview3.1_Global_10K-pruned_Labels.csv"
GRID = [TrainConfig(kind=k, weight_decay=wd, lr=lr)
        for k in ("linear", "mlp") for wd, lr in ((1e-2, 1e-3), (1e-2, 1e-4))]
ALPHAS = (0.25, 0.5, 1.0, 2.0)
CHUNK = 20000          # windows scored at once on validation (x 9789 species)
APP_THRESHOLD = 0.35   # the app's default confidence threshold


def _chunks(fp: np.ndarray):
    """(lo, hi) bounds of about CHUNK windows that never split a recording."""
    starts = np.flatnonzero(np.r_[True, fp[1:] != fp[:-1]])
    bounds = [0]
    for s in starts[1:]:
        if s - bounds[-1] >= CHUNK:
            bounds.append(s)
    bounds.append(len(fp))
    return list(zip(bounds[:-1], bounds[1:]))


def app_offset(clf, E, fp, head, threshold: float = APP_THRESHOLD) -> tuple[float, float]:
    """(offset, pass rate): sigmoid(max fused + offset) >= threshold for the same
    share of recordings as V3's own top score >= threshold."""
    top_raw, top_fused = [], []
    for lo, hi in _chunks(fp):
        lS = log_scores(1.0 / (1.0 + np.exp(-clf.logits(E[lo:hi]))))
        f = fp[lo:hi]
        top_raw.append(max_by_recording(f, lS)[1].max(1))
        top_fused.append(max_by_recording(f, lS + head.alpha * head.log_q(E[lo:hi]))[1].max(1))
    top_raw, top_fused = np.concatenate(top_raw), np.concatenate(top_fused)
    rate = float(np.mean(np.exp(top_raw) >= threshold))
    logit_t = float(np.log(threshold / (1 - threshold)))
    return logit_t - float(np.quantile(top_fused, 1 - rate)), rate


def recording_top1(clf, E, fp, true_by_fp, heads, alphas):
    """Recording-level top-1 of raw V3 and of every (head, alpha) fusion, max over windows."""
    hits = {"raw": []} | {(h, a): [] for h in range(len(heads)) for a in alphas}
    for lo, hi in _chunks(fp):
        f = fp[lo:hi]
        t = np.array([true_by_fp[x] for x in max_by_recording(f, np.zeros((hi - lo, 1)))[0]])
        lS = log_scores(1.0 / (1.0 + np.exp(-clf.logits(E[lo:hi]))))
        hits["raw"].append(max_by_recording(f, lS)[1].argmax(1) == t)
        for h, head in enumerate(heads):
            lq = head.log_q(E[lo:hi])
            for a in alphas:
                hits[(h, a)].append(max_by_recording(f, lS + a * lq)[1].argmax(1) == t)
    return {k: float(np.concatenate(v).mean()) for k, v in hits.items()}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--cache", type=Path, default=Path("data/phase_d/v3/XCL_eu"))
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--top-k", type=int, default=3, help="training windows per recording")
    ap.add_argument("--min-train", type=int, default=5)
    ap.add_argument("--val-quantile", type=float, default=0.9)
    ap.add_argument("--floor-q", type=float, default=1e-4)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    torch.set_num_threads(int(os.environ.get("SLURM_CPUS_PER_TASK", "8")))
    R = args.root
    rel = lambda p: p if Path(p).is_absolute() else R / p  # noqa: E731
    out = rel(args.out)
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    codes = [l.strip() for l in open(R / "apps/models/birdnet_labels.txt")]
    sci = [l.strip() for l in open(R / "apps/models/birdnet_labels_raw.txt")]
    col = {c: j for j, c in enumerate(codes)}
    remap = resolve_remap(codes, sci)
    v3_sci = load_labels(rel(LABELS))
    v3_idx = {s.lower(): j for j, s in enumerate(v3_sci)}
    code_to_v3 = {c: v3_idx.get(sci[j].lower(), -1) for c, j in col.items()}
    clf = classifier_from_onnx(rel(AUDIO))

    dkm = pq.read_table(R / "data/birdset/DKX/DKX/DKX_metadata_focal.parquet").to_pandas()
    dk_recordists, dk_ids = set(dkm.recordist), {xc_id(f) for f in dkm.filepath}
    xm = pq.read_table(R / "data/birdset/XCL/XCL/XCL_metadata.parquet",
                       columns=["filepath", "lat", "long", "recordist", "ebird_code"]).to_pandas()
    xm = xm[in_box(xm.lat, xm.long) & ~xm.recordist.isin(dk_recordists)].drop_duplicates("filepath")
    xm["y"] = [code_to_v3.get(remap_code(c, remap), -1) for c in xm.ebird_code]
    xm = xm[xm.y >= 0].set_index("filepath")
    if {xc_id(f) for f in xm.index} & dk_ids:
        raise SystemExit("DKX recordings are in the training pool")
    ids = np.array([xc_id(f) for f in xm.index])
    val_id = int(np.quantile(ids, args.val_quantile))
    xm["val"] = ids >= val_id
    y_of, val_of = xm.y.to_dict(), xm.val.to_dict()

    # top-k windows of every cached pool recording; every window of the val ones
    Ek, yk, fk, Eva, fva = [], [], [], [], []
    for fp, _, E in iter_window_cache(rel(args.cache), keep=set(xm.index)):
        y = np.array([y_of[f] for f in fp])
        label_logit = np.einsum("ij,ij->i", E, clf.W[y]) + clf.b[y]
        k = top_windows(fp, label_logit, args.top_k)
        Ek.append(E[k]); yk.append(y[k]); fk.append(fp[k])
        is_val = np.array([val_of[f] for f in fp])
        Eva.append(E[is_val]); fva.append(fp[is_val])
    Ek, yk, fk = np.concatenate(Ek), np.concatenate(yk), np.concatenate(fk)
    Eva, fva = np.concatenate(Eva), np.concatenate(fva)
    k_val = np.array([val_of[f] for f in fk])

    # head classes: species with >= min_train cached TRAINING recordings
    tr_rec = pd.Series(yk[~k_val], index=fk[~k_val]).groupby(level=0).first()
    counts = np.bincount(tr_rec.to_numpy(), minlength=len(v3_sci))
    classes = np.flatnonzero(counts >= args.min_train)
    head_idx = np.full(len(v3_sci), -1)
    head_idx[classes] = np.arange(len(classes))
    tr, va = ~k_val & (head_idx[yk] >= 0), k_val & (head_idx[yk] >= 0)
    Etr, ytr, Eva_k, yva_k = Ek[tr], yk[tr], Ek[va], yk[va]
    keep_val = head_idx[np.array([y_of[f] for f in fva])] >= 0
    Eva, fva = Eva[keep_val], fva[keep_val]
    n_tr_rec = int(len(set(fk[tr])))
    log.info("cached pool: %d train recordings (%d windows, top-%d), %d val recordings (%d windows; XC id >= %d); "
             "%d head classes", n_tr_rec, len(ytr), args.top_k, len(set(fva)), len(Eva), val_id, len(classes))

    mu, sd = Etr.mean(0), Etr.std(0) + 1e-6
    z = lambda a: torch.from_numpy(((a - mu) / sd).astype(np.float32))  # noqa: E731
    Xtr, Ytr = z(Etr), torch.from_numpy(head_idx[ytr])
    Xva, Yva = z(Eva_k), torch.from_numpy(head_idx[yva_k])
    heads, rows = [], []
    for cfg in GRID:
        res = train_head(Xtr, Ytr, Xva, Yva, len(classes), cfg)
        heads.append(DanishHead(model=res.model, kind=cfg.kind, classes=classes, n_axis=len(v3_sci), mu=mu, sd=sd,
                                floor_q=args.floor_q, remap=remap))
        log.info("%s wd=%g lr=%g: val loss %.3f (epoch %d), head window top-1 %.3f",
                 cfg.kind, cfg.weight_decay, cfg.lr, res.val_loss, res.best_epoch, res.val_top1)
        rows.append(dict(model=len(heads) - 1, kind=cfg.kind, wd=cfg.weight_decay, lr=cfg.lr,
                         val_loss=res.val_loss, head_window_top1=res.val_top1))
    acc = recording_top1(clf, Eva, fva, y_of, heads, ALPHAS)
    log.info("raw V3 recording-level val top-1 (max over windows): %.3f", acc["raw"])
    grid = pd.DataFrame([r | dict(alpha=a, fusion_val_top1=acc[(r["model"], a)]) for r in rows for a in ALPHAS])
    grid.to_csv(out / "val_grid.csv", index=False)
    pick = grid.loc[grid.fusion_val_top1.idxmax()]
    head = heads[int(pick.model)]
    head.alpha = float(pick.alpha)
    offset, pass_rate = app_offset(clf, Eva, fva, head)
    head.meta = {"backbone": "BirdNET+ V3.0-preview3.1 Global 10K-pruned FP16", "unit": "3 s window",
                 "top_k": args.top_k, "val_xc_id_min": val_id, "val_raw_top1": acc["raw"],
                 "val_fusion_top1": float(pick.fusion_val_top1), "kind": pick.kind, "weight_decay": float(pick.wd),
                 "lr": float(pick.lr), "n_train_windows": int(len(ytr)), "n_train_recordings": n_tr_rec,
                 "app_offset": offset, "app_threshold": APP_THRESHOLD, "val_threshold_pass_rate": pass_rate}
    head.save(out / "head.pt")
    log.info("val grid:\n%s", grid.round(3).to_string(index=False))
    log.info("chosen: %s wd=%g lr=%g alpha=%g, val top-1 %.3f (raw V3 %.3f); app offset %.3f "
             "(%.3f of val recordings pass %.2f); done in %.0f s -> %s", pick.kind, pick.wd, pick.lr, pick.alpha,
             pick.fusion_val_top1, acc["raw"], offset, pass_rate, APP_THRESHOLD, time.time() - t0, out)
    (out / "summary.json").write_text(json.dumps(head.meta, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
