"""Training data pipeline — XCM crops with context.

THE FRONTEND IS THE CARD'S RECIPE — AND THE MEASURED OUTPUT SHAPE
-----------------------------------------------------------------
``convnext_frontend`` reproduces the BirdSet ConvNext card's recipe with
the card's own numbers, spelled once as module constants: sr 32000,
n_fft 1024, hop 320, n_mels 128, power 2.0, PowerToDB top_db 80,
normalize mean -4.268 std 4.569 — output (1, 128, 501) for a 5 s clip.

THE 501-FRAME RULING (controller, binding): the card's (1, 128, 334)
example shape is internally inconsistent and FALSIFIED by the measured
authority — BirdSet's own training transform
(birdset/datamodule/components/transforms.py in
DBD-research-group/BirdSet) applies torchaudio
``Spectrogram(n_fft=1024, hop_length=320)`` to full 5 s clips, which
yields ``160000 // 320 + 1 = 501`` centered frames. The checkpoint was
trained on (1, 128, 501) inputs, so the frontend emits the NATURAL
frame count of whatever wave it receives — no trimming, and no padding
either: padding in normalized-dB space would inject fake energy floors.
ALL padding lives in ``CropDataset``, in the waveform domain, where a
zero sample is honestly silence.

CROP OFFSETS ARE A PURE FUNCTION OF (seed, epoch, index)
--------------------------------------------------------
``np.random.default_rng((seed, epoch, idx))`` — a fresh hash-seeded RNG
per item, never a shared stateful one. Task 5's bit-exact resume replays
(seed, epoch) after preemption and must land on the SAME offsets; a
shared RNG's output would depend on access order and worker count.
``epoch`` is a plain mutable attribute for exactly that reason: the
trainer bumps it between epochs and the resume path sets it back.

REVIEW FOCUS 1 — LABEL GAPS ARE LOUD, AND EARLY
-----------------------------------------------
The XCL checkpoint's axis (9,736 codes) and the XCM manifest's species
set overlap but neither contains the other. A row whose ``ebird_code``
is not in ``label2id`` is EXCLUDED at construction: the dataset shrinks,
``skipped_codes`` (dict code -> row count) names every gap, a warning is
logged once — and iteration can never KeyError mid-epoch.

SHORT RECORDINGS (<5 s) ZERO-PAD RIGHT — matching BirdSet's 5 s clip
handling: the event sits at the clip start, silence trails. Null
(cell, week) rows pass through to ``ContextProvider.vector(None, None)``
— zeros + present 0.0, the provider's own absent-context answer.
"""
from __future__ import annotations

import logging
from collections import Counter
from typing import Callable

import numpy as np
import pyarrow as pa
import torch
import torchaudio

from birdology.phase_c.context import ContextProvider

logger = logging.getLogger(__name__)

# --- The ConvNext model card's values, verbatim (Global Constraints). ---
SAMPLE_RATE = 32000
N_FFT = 1024
HOP_LENGTH = 320
N_MELS = 128
POWER = 2.0
TOP_DB = 80.0
NORM_MEAN = -4.268
NORM_STD = 4.569
#: Natural frame count for a 5 s clip: 160000 // 320 + 1 (centered STFT).
#: Measured from BirdSet's own transforms.py — the checkpoint's true input
#: time axis (the card's 334 example is falsified; see module docstring).
N_FRAMES = 501

#: 5 s @ 32 kHz.
_CROP_SECONDS_DEFAULT = 5.0


class PowerToDB(torch.nn.Module):
    """librosa-style power→dB, per the BirdSet card's snippet.

    ``10*log10(max(S, amin)) - 10*log10(max(ref, amin))``, then floored
    at ``(max - top_db)`` — the floor is taken over the whole input, as
    librosa does. NOTE: the top_db floor references the global max of
    THIS tensor — correct for the per-item call path used here; calling
    it on a BATCH would couple items through the shared max.
    """

    def __init__(
        self, ref: float = 1.0, amin: float = 1e-10, top_db: float = TOP_DB
    ) -> None:
        super().__init__()
        if amin <= 0:
            raise ValueError("amin must be strictly positive")
        if top_db is not None and top_db < 0:
            raise ValueError("top_db must be non-negative")
        self.ref = ref
        self.amin = amin
        self.top_db = top_db

    def forward(self, spec: torch.Tensor) -> torch.Tensor:
        amin = torch.tensor(self.amin, dtype=spec.dtype, device=spec.device)
        ref = torch.tensor(abs(self.ref), dtype=spec.dtype, device=spec.device)
        log_spec = 10.0 * torch.log10(torch.clamp(spec, min=self.amin))
        log_spec = log_spec - 10.0 * torch.log10(torch.maximum(ref, amin))
        if self.top_db is not None:
            log_spec = torch.clamp(log_spec, min=log_spec.max() - self.top_db)
        return log_spec


# Built once at module level: the transforms hold no trainable state and
# rebuilding mel filterbanks per item would dominate __getitem__.
_SPECTROGRAM = torchaudio.transforms.Spectrogram(
    n_fft=N_FFT, hop_length=HOP_LENGTH, power=POWER
)
_MEL_SCALE = torchaudio.transforms.MelScale(
    n_mels=N_MELS, sample_rate=SAMPLE_RATE, n_stft=N_FFT // 2 + 1
)
_POWER_TO_DB = PowerToDB(ref=1.0, amin=1e-10, top_db=TOP_DB)

# --- The AST path's MEASURED constants (BirdSet sources, not the card). ---
# Source of truth: DBD-research-group/BirdSet @ main
# (86a0777cae76f26d278391e63a6ae4da1bdec772, read 2026-10-01):
#   birdset/datamodule/components/transforms.py:45-73
#     (PreprocessingConfig defaults — Spectrogram(1024, 320, 2.0),
#      MelScale(128, 32000, 513), PowerToDB(), Resizer(db_scale=True),
#      mean=-4.268, std=-4.569) and :291-327 (_preprocess order).
#   birdset/datamodule/components/resize.py
#     (Resizer: pad right with -80.0 when db_scale, or truncate, to
#      target_width=1024 — pad/truncate, NEVER interpolation).
#   configs/experiment/birdset_neurips24/XCL/ast.yaml
#     (overrides ONLY resizer.target_width: 1024 — the defaults stand).
#   birdset/modules/models/ast.py forward: squeeze(1), transpose(1, 2)
#     → (time=1024, mel=128), matching the checkpoint's config.json
#     (DBD-research-group/AST-BirdSet-XCL @ 0424378a: max_length=1024,
#      num_mel_bins=128; the repo ships NO preprocessor_config.json —
#      BirdSet's own vision pipeline above IS the preprocessing).
#: AST input time axis — Resizer target_width / config max_length.
AST_N_FRAMES = 1024
#: Resizer's dB-scale constant pad value (resize.py: db_scale=True).
AST_PAD_DB = -80.0
#: Spectrogram-normalization constants, verbatim from BirdSet
#: transforms.py:63-64 ("calculated on AudioSet"). The std is NEGATIVE
#: in their source — (x - mean) / std therefore flips the sign of every
#: normalized value relative to a positive-std normalization. That
#: arithmetic is what the AST-BirdSet-XCL checkpoint was trained on, so
#: it is reproduced EXACTLY, never "fixed" to +4.569.
AST_NORM_MEAN = -4.268
AST_NORM_STD = -4.569


def ast_frontend(wave: torch.Tensor) -> torch.Tensor:
    """Mono waveform @32 kHz → (1024, 128) time×mel for BirdSet AST.

    BirdSet's AST path is the VISION pipeline at 32 kHz — it never uses
    HF's ASTFeatureExtractor (16 kHz Kaldi fbank). Golden-gated
    bit-exact against BirdSet's own Resizer/PowerToDB sources
    (tests/phase_c/make_ast_goldens.py; constants' provenance above).
    Steps, in BirdSet's _preprocess order (transforms.py:291-327):

    1. Spectrogram/MelScale/PowerToDB — the SAME module-level transforms
       as ``convnext_frontend`` (identical constants, measured).
    2. Resize to 1024 frames: pad right with -80.0 dB or truncate
       (resize.py) — pad/truncate, not interpolation; short clips carry
       an honest -80 dB floor, never fake energy.
    3. Normalize ``(x - (-4.268)) / (-4.569)`` — the NEGATIVE std is
       BirdSet's own constant (transforms.py:64) and part of the
       checkpoint's contract (see AST_NORM_STD).
    4. Transpose to (time, mel) — the model adapter's squeeze+transpose
       (birdset/modules/models/ast.py), so this tensor is exactly
       ``ASTForAudioClassification``'s ``input_values``.

    Unlike ``convnext_frontend`` the output time axis is FIXED at 1024:
    the resize-to-target is BirdSet's own training-time behavior, so
    padding here is reproducing the checkpoint's contract, not inventing
    one (the refuse-don't-pad doctrine governs OUR choices; this one is
    theirs, measured).
    """
    if wave.dim() != 1:
        raise ValueError(f"expected mono 1-D waveform, got shape {tuple(wave.shape)}")
    spec = _SPECTROGRAM(wave.to(torch.float32))
    mel = _MEL_SCALE(spec)
    db = _POWER_TO_DB(mel)  # (128, n_frames)
    n_frames = db.shape[-1]
    if n_frames < AST_N_FRAMES:
        db = torch.nn.functional.pad(
            db, (0, AST_N_FRAMES - n_frames), value=AST_PAD_DB
        )
    elif n_frames > AST_N_FRAMES:
        db = db[:, :AST_N_FRAMES]
    normed = (db - AST_NORM_MEAN) / AST_NORM_STD
    return normed.transpose(0, 1)  # (1024, 128) time×mel


def convnext_frontend(wave: torch.Tensor) -> torch.Tensor:
    """Mono waveform @32 kHz → (1, 128, n_frames) normalized mel.

    Emits the NATURAL frame count: ``len(wave) // 320 + 1`` centered
    STFT frames — (1, 128, 501) for a 5 s / 160,000-sample crop.
    Controller ruling: the card's (1, 128, 334) example shape is
    falsified by the measured BirdSet training transform
    (transforms.py: Spectrogram(n_fft=1024, hop_length=320) on full 5 s
    clips → 501 frames); the checkpoint saw 501-frame inputs. The
    frontend never trims and never pads — padding in normalized-dB
    space would fake energy floors; CropDataset owns all padding, in
    the waveform domain.
    """
    if wave.dim() != 1:
        raise ValueError(f"expected mono 1-D waveform, got shape {tuple(wave.shape)}")
    spec = _SPECTROGRAM(wave.to(torch.float32))
    mel = _MEL_SCALE(spec)
    db = _POWER_TO_DB(mel)
    normed = (db - NORM_MEAN) / NORM_STD
    return normed.unsqueeze(0)


def make_label_map(id2label: dict) -> dict[str, int]:
    """Invert HF's ``id2label`` → ``label2id``.

    Keys arrive as ints from live configs and as STRINGS from
    json-round-tripped ones; both normalize to int here.
    """
    return {label: int(idx) for idx, label in id2label.items()}


class CropDataset(torch.utils.data.Dataset):
    """XCM manifest rows → 5 s crops with context, labels on the model axis.

    Yields ``{"inputs": (1,128,501) f32, "context": (n_classes,) f32,
    "present": float, "label": int}``.

    ``skipped_codes`` names every manifest ebird_code absent from
    ``label2id`` with its row count (Review Focus 1) — those rows are
    gone from the dataset before the first ``__getitem__``.
    """

    def __init__(
        self,
        manifest: pa.Table,
        load_audio: Callable[[str], np.ndarray],
        context_provider: ContextProvider,
        label2id: dict[str, int],
        crop_s: float = _CROP_SECONDS_DEFAULT,
        seed: int = 0,
        epoch: int = 0,
        frontend: Callable[[torch.Tensor], torch.Tensor] = convnext_frontend,
    ) -> None:
        self.load_audio = load_audio
        self.context_provider = context_provider
        self.label2id = dict(label2id)
        self.crop_samples = int(round(crop_s * SAMPLE_RATE))
        self.seed = seed
        self.epoch = epoch
        self.frontend = frontend

        rows = manifest.to_pylist()
        kept: list[dict] = []
        skipped: Counter[str] = Counter()
        for row in rows:
            code = row["ebird_code"]
            if code in self.label2id:
                kept.append(row)
            else:
                skipped[code] += 1
        self.rows = kept
        self.skipped_codes: dict[str, int] = dict(skipped)
        if skipped:
            logger.warning(
                "CropDataset: skipped %d manifest rows whose ebird_code is "
                "not in label2id: %s",
                sum(skipped.values()),
                dict(skipped),
            )

    def __len__(self) -> int:
        return len(self.rows)

    def _crop(self, wave: np.ndarray, idx: int) -> np.ndarray:
        n = len(wave)
        if n < self.crop_samples:
            # Shorter than the crop: zero-pad right (BirdSet's 5 s clip
            # handling — event at clip start, silence trails).
            return np.pad(wave, (0, self.crop_samples - n))
        if n == self.crop_samples:
            return wave
        # The load-bearing determinism: a fresh hash-seeded RNG per
        # (seed, epoch, idx) — offsets never depend on access order.
        rng = np.random.default_rng((self.seed, self.epoch, idx))
        start = int(rng.integers(0, n - self.crop_samples + 1))
        return wave[start : start + self.crop_samples]

    def __getitem__(self, idx: int) -> dict:
        row = self.rows[idx]
        wave = np.ascontiguousarray(
            self.load_audio(row["filepath"]), dtype=np.float32
        )
        crop = self._crop(wave, idx)
        inputs = self.frontend(torch.from_numpy(crop))

        vector, present = self.context_provider.vector(row["cell"], row["week"])
        return {
            "inputs": inputs,
            "context": torch.from_numpy(vector),
            "present": float(present),
            "label": self.label2id[row["ebird_code"]],
        }
