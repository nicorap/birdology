#!/usr/bin/env python3
"""Export the Danish head fused with BirdNET 2.4, for BirdNET-Analyzer and TensorFlow.

Three steps, two environments (torch is not in the TF container):

  arrays  (phase_c venv)  head.pt -> head_arrays.npz
  build   (tf.sif)        head_arrays.npz + BirdNET SavedModel -> export dir:
                          savedmodel/, DanishBirdNET.tflite, DanishBirdNET_Labels.txt
                          (English) and DanishBirdNET_Labels_da.txt (Danish)
  check   (tf.sif, GPU)   runs the export on DKX audio the way BirdNET-Analyzer does
                          (3 s chunks, max over chunks) and reports: parity of the
                          backbone with the cached BirdNET scores, top-1 of raw
                          BirdNET and of the fusion, and TFLite-vs-SavedModel agreement

Use with BirdNET-Analyzer:
    birdnet_analyzer.analyze recordings/ --classifier export/DanishBirdNET.tflite
(the labels file must sit next to the model, named <model>_Labels.txt).
"""
from __future__ import annotations

import argparse
import io
import json
import shutil
import tarfile
from pathlib import Path

import numpy as np

B = Path("/srv/home/rapnic/birdology")
BIRDNET = B / "apps/models/birdnet/acoustic-models/v2.4/pb"


def cmd_arrays(args) -> int:
    from birdology.phase_d.head import DanishHead, head_arrays

    head = DanishHead.load(args.head)
    np.savez(args.out, **head_arrays(head))
    print(f"{args.head} ({head.kind}, {len(head.classes)} classes, alpha {head.alpha}) -> {args.out}")
    return 0


def _scores_are_logits(model_dir: Path) -> bool:
    import tensorflow as tf
    bn = tf.saved_model.load(str(model_dir))
    x = np.random.default_rng(0).normal(0, 0.1, (2, 144000)).astype(np.float32)
    s = bn.signatures["basic"](inputs=tf.constant(x))["scores"].numpy()
    return bool(s.min() < 0 or s.max() > 1)


def cmd_build(args) -> int:
    import tensorflow as tf
    from birdology.phase_d.export_tf import DanishBirdNET, to_tflite

    arrays = dict(np.load(args.arrays))
    logits = _scores_are_logits(BIRDNET / "model-fp32")
    print(f"BirdNET 'basic' signature returns {'logits' if logits else 'sigmoid scores'}")
    m = DanishBirdNET(BIRDNET / "model-fp32", arrays, scores_are_logits=logits)
    args.out.mkdir(parents=True, exist_ok=True)
    tf.saved_model.save(m, str(args.out / "savedmodel"), signatures={"serving_default": m.__call__})
    to_tflite(m, args.out / "DanishBirdNET.tflite")
    shutil.copy(BIRDNET / "labels/en_uk.txt", args.out / "DanishBirdNET_Labels.txt")
    shutil.copy(BIRDNET / "labels/da.txt", args.out / "DanishBirdNET_Labels_da.txt")
    (args.out / "export.json").write_text(json.dumps({
        "kind": str(arrays["kind"]), "head_classes": int(len(arrays["classes"])), "alpha": float(arrays["alpha"]),
        "floor_q": float(arrays["floor_q"]), "birdnet_scores_are_logits": logits,
        "output": "logits; sigmoid(logits) = BirdNET score * head probability**alpha"}, indent=2))
    size = (args.out / "DanishBirdNET.tflite").stat().st_size / 1e6
    print(f"wrote {args.out}/savedmodel and DanishBirdNET.tflite ({size:.1f} MB)")
    return 0


def cmd_check(args) -> int:
    import pyarrow.parquet as pq
    import soundfile as sf
    import tensorflow as tf
    from scipy.signal import resample_poly

    from birdology.phase_d.data import load_cache
    from birdology.phase_d.evaluate import RecordistBootstrap, top1_hits
    from birdology.phase_d.export_tf import SAMPLE_RATE, chunks
    from birdology.phase_d.taxonomy import remap_code, resolve_remap

    codes = [l.strip() for l in open(B / "apps/models/birdnet_labels.txt")]
    sci = [l.strip() for l in open(B / "apps/models/birdnet_labels_raw.txt")]
    col = {c: j for j, c in enumerate(codes)}
    remap = resolve_remap(codes, sci)
    meta = pq.read_table(B / "data/birdset/DKX/DKX/DKX_metadata_focal.parquet").to_pandas().set_index("filepath")
    cache = load_cache([B / "data/phase_a/embeddings/DKX_focal_birdnet"])
    cached = dict(zip(cache.filepaths, cache.scores))
    m = tf.saved_model.load(str(args.export / "savedmodel"))
    alpha = json.loads((args.export / "export.json").read_text())["alpha"]

    names, true, raw_max, fused_max, parity = [], [], [], [], []
    for shard in sorted((B / "data/birdset/DKX/DKX").glob("*.tar.gz")):
        with tarfile.open(shard) as tar:
            for mem in tar:
                if not mem.isfile() or mem.name not in meta.index:
                    continue
                t = col.get(remap_code(meta.loc[mem.name, "ebird_code"], remap), -1)
                try:
                    x, sr = sf.read(io.BytesIO(tar.extractfile(mem).read()), dtype="float32", always_2d=False)
                except Exception:
                    continue
                if t < 0 or len(x) == 0:
                    continue
                if x.ndim > 1:
                    x = x.mean(1)
                if sr != SAMPLE_RATE:
                    g = np.gcd(int(sr), SAMPLE_RATE)
                    x = resample_poly(x, SAMPLE_RATE // g, int(sr) // g).astype(np.float32)
                parts = {k: v.numpy() for k, v in m.parts(tf.constant(chunks(x))).items()}
                s_max = np.exp(parts["log_s"]).max(0)
                names.append(mem.name); true.append(t)
                raw_max.append(s_max)
                fused_max.append((parts["log_s"] + alpha * parts["log_q"]).max(0))
                if mem.name in cached:
                    c = cached[mem.name]
                    parity.append((float(np.abs(s_max - c).max()), int(s_max.argmax() == c.argmax())))
    true = np.array(true)
    raw_max, fused_max = np.stack(raw_max), np.stack(fused_max)
    bs = RecordistBootstrap(meta.loc[names, "recordist"].to_numpy())
    raw_t1, fused_t1 = top1_hits(raw_max, true), top1_hits(fused_max, true)
    lo, hi = bs.ci(fused_t1 - raw_t1)
    par = np.array(parity)
    print(f"DKX recordings scored: {len(true)}")
    print(f"backbone parity vs cached BirdNET scores: top-1 agreement {par[:, 1].mean():.3f}, "
          f"median max|diff| {np.median(par[:, 0]):.4f}")
    print(f"chunk-level (3 s, max over chunks): raw BirdNET top-1 {raw_t1.mean():.3f}; "
          f"fused top-1 {fused_t1.mean():.3f}; diff {(fused_t1 - raw_t1).mean():+.3f} [{lo:+.3f}, {hi:+.3f}]")

    interp = tf.lite.Interpreter(model_path=str(args.export / "DanishBirdNET.tflite"))
    interp.allocate_tensors()
    inp, out = interp.get_input_details()[0], interp.get_output_details()[0]
    rng = np.random.default_rng(0)
    diffs = []
    for _ in range(8):
        x = rng.normal(0, 0.05, (1, 144000)).astype(np.float32)
        interp.set_tensor(inp["index"], x)
        interp.invoke()
        lite = interp.get_tensor(out["index"])
        ref = m.signatures["serving_default"](inputs=tf.constant(x))["logits"].numpy()
        diffs.append((float(np.abs(lite - ref).max()), int(lite.argmax() == ref.argmax())))
    d = np.array(diffs)
    print(f"TFLite vs SavedModel on 8 noise chunks: max|diff| {d[:, 0].max():.2e}, arg-max agreement {d[:, 1].mean():.2f}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("arrays"); a.add_argument("--head", type=Path, required=True); a.add_argument("--out", type=Path, required=True)
    b = sub.add_parser("build"); b.add_argument("--arrays", type=Path, required=True); b.add_argument("--out", type=Path, required=True)
    c = sub.add_parser("check"); c.add_argument("--export", type=Path, required=True)
    args = ap.parse_args(argv)
    return {"arrays": cmd_arrays, "build": cmd_build, "check": cmd_check}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
