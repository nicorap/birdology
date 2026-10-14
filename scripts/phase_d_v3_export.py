#!/usr/bin/env python3
"""Export a V3 Danish head into the BirdNET Live app's model files.

Writes <out>/ ready to replace the app's assets/models/ files:
  <name>.onnx            the app's model with the head appended (export_onnx)
  <labels>.csv, <blacklist>.json, geo-model files   copied unchanged
  model_config.json      the app's config pointing at the new model
and checks it on real DKX audio: the new model's ``birdnet_predictions`` and
``embeddings_out`` must equal the original model's, and ``predictions`` must
equal the NumPy reference of the fusion.

Run in the phase_c venv (torch for head.pt, onnx + onnxruntime):

    python scripts/phase_d_v3_export.py --head data/phase_d/v3_head_v1/head.pt --out data/phase_d/app_export_v1
"""
from __future__ import annotations

import argparse
import io
import json
import shutil
import tarfile
from pathlib import Path

import numpy as np
import soundfile as sf

from birdology.phase_d.birdnet3 import resample, windows
from birdology.phase_d.export_onnx import FUSIONS, append_head, reference
from birdology.phase_d.head import DanishHead, head_arrays

V3 = Path("apps/models/birdnet_v3")
MODEL = "BirdNET+_V3.0-preview3.1_Global_10K-pruned_FP16.onnx"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/srv/home/rapnic/birdology"))
    ap.add_argument("--head", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--fusion", choices=FUSIONS, default="cal")
    ap.add_argument("--name", default="BirdNET+_V3.0-preview3.1_Global_10K-pruned_DK_FP16")
    ap.add_argument("--check-recordings", type=int, default=20)
    args = ap.parse_args(argv)
    import onnxruntime as ort

    R = args.root
    rel = lambda p: p if Path(p).is_absolute() else R / p  # noqa: E731
    src_dir, out = R / V3, rel(args.out)
    out.mkdir(parents=True, exist_ok=True)
    head = DanishHead.load(rel(args.head))
    arrays = head_arrays(head)
    model_file = f"{args.name}.onnx"
    append_head(src_dir / MODEL, arrays, out / model_file, fusion=args.fusion)

    cfg = json.loads((src_dir / "model_config.json").read_text())
    for f in [cfg["audioModel"]["labels"]["file"], cfg["audioModel"]["scoreBlacklistFile"],
              cfg["geoModel"]["modelFile"], cfg["geoModel"]["labelsFile"]]:
        shutil.copy(src_dir / f, out / f)
    am = cfg["audioModel"]
    am["name"] = f"{am['name']} + Danish head"
    # the app caches the extracted model as <file>_v<version>: a new head under
    # the same file name needs a new version, or phones keep the old one
    am["version"] = f"{am['version']}-dk.{rel(args.head).parent.name}"
    am["description"] = (f"{am['description']} With a Danish classifier head (birdology Phase D; "
                         f"{len(head.classes)} species, alpha {head.alpha}, fusion {args.fusion}, "
                         f"offset {float(arrays['offset']):.3f}).")
    am["modelFile"] = am["onnx"]["modelFile"] = model_file
    (out / "model_config.json").write_text(json.dumps(cfg, indent=2) + "\n")

    orig = ort.InferenceSession(str(src_dir / MODEL), providers=["CPUExecutionProvider"])
    new = ort.InferenceSession(str(out / model_file), providers=["CPUExecutionProvider"])
    shard = sorted((R / "data/birdset/DKX/DKX").glob("*.tar.gz"))[0]
    worst = {"birdnet_predictions": 0.0, "embeddings_out": 0.0, "predictions": 0.0}
    agree, n = 0, 0
    with tarfile.open(shard) as tar:
        for mem in tar:
            if not mem.isfile() or n >= args.check_recordings:
                continue
            x, sr = sf.read(io.BytesIO(tar.extractfile(mem).read()), dtype="float32", always_2d=False)
            W = windows(resample(x, sr))[:8]
            s, e = orig.run(["predictions", "embeddings_out"], {"input": W})
            p, s2, e2 = new.run(["predictions", "birdnet_predictions", "embeddings_out"], {"input": W})
            ref = reference(s, e, arrays, args.fusion)
            worst["birdnet_predictions"] = max(worst["birdnet_predictions"], float(np.abs(s2 - s).max()))
            worst["embeddings_out"] = max(worst["embeddings_out"], float(np.abs(e2 - e).max()))
            worst["predictions"] = max(worst["predictions"], float(np.abs(p - ref).max()))
            agree += int((p.argmax(1) == ref.argmax(1)).all())
            n += 1
    size = (out / model_file).stat().st_size / 1e6
    print(f"wrote {out}/{model_file} ({size:.1f} MB) and model_config.json")
    print(f"parity on {n} DKX recordings (<= 8 windows each): max|diff| " +
          ", ".join(f"{k} {v:.2e}" for k, v in worst.items()) + f"; arg-max agrees in {agree}/{n}")
    return 0 if worst["birdnet_predictions"] < 1e-4 and worst["predictions"] < 1e-3 and agree == n else 1


if __name__ == "__main__":
    raise SystemExit(main())
