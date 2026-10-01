"""Dev-time generator for the AST-frontend golden fixtures.

Run ONCE, by hand, with network access (committed tests stay offline):

    uv run python tests/phase_c/make_ast_goldens.py --birdset-src .dev_ast_golden

``--birdset-src`` points at a directory holding the REAL BirdSet sources,
downloaded verbatim from github.com/DBD-research-group/BirdSet @ main
(86a0777cae76f26d278391e63a6ae4da1bdec772, fetched 2026-10-01):

* ``birdset_resize.py``          <- birdset/datamodule/components/resize.py
* ``birdset_augmentations.py``   <- birdset/datamodule/components/augmentations.py
* ``birdset_transforms.py``      <- birdset/datamodule/components/transforms.py
  (reference only — the composition below mirrors its ``_preprocess``)

THE MEASURED CONTRACT THIS SCRIPT EXECUTES (ground truth, not our code)
-----------------------------------------------------------------------
BirdSet's AST path is the VISION pipeline at 32 kHz — it does NOT use HF's
ASTFeatureExtractor (16 kHz Kaldi fbank) at all. Per
``transforms.py`` ``PreprocessingConfig`` defaults (lines 45-73) and
``BirdSetTransformsWrapper._preprocess`` (lines 291-327), applied in order:

1. ``torchaudio.transforms.Spectrogram(n_fft=1024, hop_length=320, power=2.0)``
2. ``torchaudio.transforms.MelScale(n_mels=128, sample_rate=32000, n_stft=513)``
3. ``PowerToDB(ref=1.0, amin=1e-10, top_db=80.0)``  (augmentations.py — the
   librosa formula; the top_db floor references the max of the whole tensor,
   so goldens are generated with batch size 1 to match per-item semantics)
4. ``Resizer(db_scale=True, target_width=1024)`` (resize.py) — NOT an
   interpolation: pad right with CONSTANT -80.0 dB, or truncate, to width 1024
5. normalize ``(x - mean) / std`` with mean=-4.268, std=-4.569 — the std is
   NEGATIVE in BirdSet's own source (transforms.py line 64, "calculated on
   AudioSet"); the sign flip is part of what the checkpoint saw. Reproduced
   verbatim, never "fixed".
6. the model adapter transpose (birdset/modules/models/ast.py forward:
   ``squeeze(1)``, ``transpose(1, 2)``) → (time=1024, mel=128), the shape
   ``ASTForAudioClassification`` consumes as ``input_values``.

The XCL AST experiment config (configs/experiment/birdset_neurips24/XCL/
ast.yaml) overrides only ``resizer.target_width: 1024`` — i.e. the defaults
above, unchanged.

Output: ``tests/phase_c/ast_goldens.npz`` (compressed) with one (1024, 128)
float32 array per wave in :func:`golden_waves`.
"""
from __future__ import annotations

import numpy as np

SR = 32000


def golden_waves() -> dict[str, np.ndarray]:
    """Deterministic synthetic waveforms @32 kHz — shared verbatim by the
    committed golden test, so the test exercises the exact inputs the
    goldens were generated from."""
    rng = np.random.default_rng(42)
    noise = (rng.standard_normal(SR) * 0.1).astype(np.float32)  # 1 s
    t = np.arange(2 * SR, dtype=np.float64) / SR
    chirp = (0.5 * np.sin(2 * np.pi * (200.0 * t + 950.0 * t * t))).astype(
        np.float32
    )  # 2 s, 200 Hz sweeping up
    silence = np.zeros(SR, dtype=np.float32)  # 1 s
    return {"noise_1s": noise, "chirp_2s": chirp, "silence_1s": silence}


def _load_birdset(src_dir):
    """Import the REAL BirdSet Resizer and PowerToDB from the downloaded
    sources. resize.py imports only torch and loads directly;
    augmentations.py pulls the whole BirdSet dependency tree, so the
    PowerToDB class source is extracted verbatim and exec'd with its own
    imports satisfied — the executed code is still BirdSet's, untouched."""
    import importlib.util
    import re
    import warnings
    from pathlib import Path

    import torch
    from torch import nn

    src_dir = Path(src_dir)
    spec = importlib.util.spec_from_file_location(
        "birdset_resize", src_dir / "birdset_resize.py"
    )
    resize_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(resize_mod)

    aug_src = (src_dir / "birdset_augmentations.py").read_text()
    match = re.search(r"class PowerToDB.*?(?=\nclass |\Z)", aug_src, re.S)
    if match is None:
        raise ValueError("PowerToDB class not found in birdset_augmentations.py")
    namespace = {"torch": torch, "nn": nn, "warnings": warnings}
    exec(compile(match.group(0), "birdset_augmentations.py", "exec"), namespace)
    return resize_mod.Resizer, namespace["PowerToDB"]


def main(birdset_src: str, out_path: str) -> None:
    import torch
    import torchaudio

    Resizer, PowerToDB = _load_birdset(birdset_src)

    # PreprocessingConfig defaults, verbatim (transforms.py:45-73).
    spectrogram = torchaudio.transforms.Spectrogram(
        n_fft=1024, hop_length=320, power=2.0
    )
    melscale = torchaudio.transforms.MelScale(
        n_mels=128, sample_rate=32000, n_stft=513
    )
    power_to_db = PowerToDB()  # ref=1.0, amin=1e-10, top_db=80.0
    resizer = Resizer(db_scale=True, target_width=1024)
    mean, std = -4.268, -4.569  # std NEGATIVE — BirdSet's own constant.

    goldens: dict[str, np.ndarray] = {}
    for name, wave in golden_waves().items():
        x = torch.from_numpy(wave)[None, None, :]  # (B=1, 1, T)
        # _preprocess order (transforms.py:291-327): spec→mel→dB→resize→norm.
        spec = spectrogram(x)
        mel = melscale(spec)
        db = power_to_db(mel)
        resized = resizer.resize_spectrogram_batch(db)
        normed = (resized - mean) / std
        # Model adapter (modules/models/ast.py): squeeze channel, transpose
        # to (time, mel) — the tensor ASTForAudioClassification consumes.
        out = normed.squeeze(1).transpose(1, 2)[0]
        goldens[name] = out.numpy().astype(np.float32)
        print(f"{name}: shape={tuple(out.shape)} "
              f"min={out.min():.4f} max={out.max():.4f}")

    np.savez_compressed(out_path, **goldens)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--birdset-src", required=True)
    parser.add_argument(
        "--out",
        default=str(Path(__file__).parent / "ast_goldens.npz"),
    )
    args = parser.parse_args()
    main(args.birdset_src, args.out)
