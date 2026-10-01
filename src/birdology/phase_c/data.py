"""Training data pipeline — XCM crops with context.

THE FRONTEND IS THE MODEL CARD, VERBATIM
----------------------------------------
``convnext_frontend`` reproduces the BirdSet ConvNext card's recipe with
the card's own numbers, spelled once as module constants: sr 32000,
n_fft 1024, hop 320, n_mels 128, power 2.0, PowerToDB top_db 80,
normalize mean -4.268 std 4.569, output (1, 128, 334) for a 5 s clip.

THE 334-FRAME RECONCILIATION (measured, then pinned): torchaudio's
centered ``Spectrogram`` yields ``160000 // 320 + 1 = 501`` frames for a
5 s crop — more than the card's 334. The card's (B, 1, 128, 334) input
shape is the BINDING contract (the checkpoint's embedding was trained on
it), so the frontend RIGHT-TRIMS the time axis to 334 frames and
zero-pads right when a shorter wave yields fewer. Right-trim, not
center-crop: it keeps frame t aligned to sample t*hop from the clip
start, so the retained window is a deterministic prefix of the crop the
dataset already randomized — no second randomness source.

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
#: The card's binding time-axis length for a 5 s clip: (B, 1, 128, 334).
N_FRAMES = 334

#: 5 s @ 32 kHz.
_CROP_SECONDS_DEFAULT = 5.0


class PowerToDB(torch.nn.Module):
    """librosa-style power→dB, per the BirdSet card's snippet.

    ``10*log10(max(S, amin)) - 10*log10(max(ref, amin))``, then floored
    at ``(max - top_db)`` — the floor is taken over the whole input, as
    librosa does.
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


def convnext_frontend(wave: torch.Tensor) -> torch.Tensor:
    """Mono waveform @32 kHz → the card's (1, 128, 334) normalized mel.

    Time axis reconciliation (measured): 160,000 samples yield 501
    centered STFT frames; the card's 334 is binding, so the output is
    right-trimmed to 334 frames (zero-padded right when shorter).
    """
    if wave.dim() != 1:
        raise ValueError(f"expected mono 1-D waveform, got shape {tuple(wave.shape)}")
    spec = _SPECTROGRAM(wave.to(torch.float32))
    mel = _MEL_SCALE(spec)
    db = _POWER_TO_DB(mel)
    normed = (db - NORM_MEAN) / NORM_STD

    n_frames = normed.shape[-1]
    if n_frames >= N_FRAMES:
        normed = normed[..., :N_FRAMES]
    else:
        normed = torch.nn.functional.pad(normed, (0, N_FRAMES - n_frames))
    return normed.unsqueeze(0)


def make_label_map(id2label: dict) -> dict[str, int]:
    """Invert HF's ``id2label`` → ``label2id``.

    Keys arrive as ints from live configs and as STRINGS from
    json-round-tripped ones; both normalize to int here.
    """
    return {label: int(idx) for idx, label in id2label.items()}


class CropDataset(torch.utils.data.Dataset):
    """XCM manifest rows → 5 s crops with context, labels on the model axis.

    Yields ``{"inputs": (1,128,334) f32, "context": (n_classes,) f32,
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
    ) -> None:
        self.load_audio = load_audio
        self.context_provider = context_provider
        self.label2id = dict(label2id)
        self.crop_samples = int(round(crop_s * SAMPLE_RATE))
        self.seed = seed
        self.epoch = epoch

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
        inputs = convnext_frontend(torch.from_numpy(crop))

        vector, present = self.context_provider.vector(row["cell"], row["week"])
        return {
            "inputs": inputs,
            "context": torch.from_numpy(vector),
            "present": float(present),
            "label": self.label2id[row["ebird_code"]],
        }
