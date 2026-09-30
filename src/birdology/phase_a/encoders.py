"""Frozen encoders: melsummary (checkpoint-free), Perch 2.0, BirdNET V2.4.

Perch and BirdNET wrap heavyweight TF runtimes that do NOT exist on the dev
Mac or in CI, so this module keeps zero module-level TF/birdnet imports: the
runtimes are imported lazily inside the loader functions and raise ImportError
with install instructions when absent. The framing, normalization and pooling
logic — the part a subtle bug would silently poison 528k cached embeddings
with — lives here as pure-numpy functions so contract tests can pin it down
without TensorFlow.

Cited facts (2026-09-30 reconnaissance, docs/superpowers/plans/
2026-09-30-task6-real-encoders.md):
- Perch 2.0: mono float32 32 kHz, 5 s windows (160 000 samples), hop 5 s;
  recordings < 5 s are zero-CENTER-padded; per-window normalization is
  mean-subtract then peak-normalize to 0.25 (perch-hoplite's normalize_audio);
  serving returns 1536-d per window; recording-level = MEAN over windows
  (the paper's own recipe, §2.5.1).
- BirdNET V2.4: the `birdnet` pip package resamples internally (scipy FFT),
  so we pass OUR sr through; `encode_arrays` returns embeddings
  (1, n_segments, 1024) plus a boolean mask of the same shape; a segment is
  VALID when mask.any(axis=-1); recording-level = MEAN over valid segments.
  n_workers MUST be 1 under Slurm (the package spawns per-core workers
  otherwise).
"""
from __future__ import annotations

from collections.abc import Callable, Mapping

import numpy as np

Encoder = Callable[[np.ndarray, int], np.ndarray]

PERCH_SR = 32_000
PERCH_WINDOW_SAMPLES = 160_000  # 5 s at 32 kHz
PERCH_HOP_SAMPLES = 160_000  # hop 5 s: non-overlapping windows
PERCH_TARGET_PEAK = 0.25


def _mean_log_mel(samples: np.ndarray, sr: int, n_mels: int = 64) -> np.ndarray:
    """A real, deterministic, checkpoint-free embedding.

    Deliberately NOT a random or constant vector: a stand-in that ignores its
    input would let every downstream test pass on data that carries no signal,
    which is the failure this project keeps finding. This is a genuine (if
    weak) acoustic summary, so a fusion test that improves on it is measuring
    something.
    """
    if samples.size == 0:
        return np.zeros(n_mels, dtype=np.float32)
    spectrum = np.abs(np.fft.rfft(samples.astype(np.float64)))
    edges = np.linspace(0, spectrum.size, n_mels + 1).astype(int)
    bands = [spectrum[a:b].mean() if b > a else 0.0 for a, b in zip(edges, edges[1:])]
    return np.log1p(np.asarray(bands, dtype=np.float64)).astype(np.float32)


# ---------------------------------------------------------------------------
# Perch 2.0 — pure-numpy framing/normalization + lazy TF wrapper
# ---------------------------------------------------------------------------


def _frame_for_perch(samples: np.ndarray, sr: int) -> np.ndarray:
    """Frame a mono recording into normalized (n_windows, 160000) float32.

    Mirrors perch-hoplite's semantics exactly (frame_audio + normalize_audio):
    - sr must be 32 kHz. Our corpus is native 32 kHz; silently accepting
      another rate would embed garbage that no downstream check could catch.
    - Recordings shorter than one window are zero-CENTER-padded to 5 s.
    - Longer recordings are framed at hop 5 s; a trailing partial window is
      zero-padded at the end so no audio is dropped.
    - Each window is then mean-subtracted and peak-normalized to 0.25
      (all-zero windows are left as zeros — no division by zero).
    """
    if sr != PERCH_SR:
        raise ValueError(
            f"perch requires sr == {PERCH_SR} (got {sr}); the corpus is native "
            "32 kHz and resampling here would silently change the embeddings"
        )
    x = np.asarray(samples, dtype=np.float32).reshape(-1)
    frame, hop = PERCH_WINDOW_SAMPLES, PERCH_HOP_SAMPLES
    if x.size < frame:
        pad = frame - x.size
        left = pad // 2
        x = np.pad(x, (left, pad - left))
        n_windows = 1
    else:
        n_windows = int(np.ceil((x.size - frame) / hop)) + 1
        needed = (n_windows - 1) * hop + frame
        x = np.pad(x, (0, needed - x.size))
    windows = np.stack([x[i * hop : i * hop + frame] for i in range(n_windows)])
    # Per-window normalization (perch-hoplite normalize_audio semantics).
    windows = windows - windows.mean(axis=1, keepdims=True)
    peaks = np.max(np.abs(windows), axis=1, keepdims=True)
    safe_peaks = np.where(peaks > 0, peaks, 1.0)
    windows = windows * (PERCH_TARGET_PEAK / safe_peaks)
    return windows.astype(np.float32)


def _load_perch_model() -> Callable[[np.ndarray], Mapping[str, np.ndarray]]:
    """Lazy-load Perch 2.0 via perch-hoplite; adapt to windows -> {'embedding'}.

    Raises ImportError with install instructions when the runtime is absent —
    never a silent fallback: an embedding cache built with the wrong encoder
    is indistinguishable from a correct one.
    """
    try:
        from perch_hoplite.zoo import model_configs
    except ImportError as exc:
        raise ImportError(
            "the perch encoder requires perch-hoplite and TensorFlow >= 2.20: "
            "pip install perch-hoplite  (on the fleet, use the tf.sif "
            "container from Task 6.1). Refusing to fall back to a stand-in."
        ) from exc
    loaded = model_configs.load_model_by_name("perch_v2")

    def infer(windows: np.ndarray) -> Mapping[str, np.ndarray]:
        outputs = loaded.batch_embed(windows)
        emb = np.asarray(outputs.embeddings, dtype=np.float32)
        # hoplite returns [batch, time, channels]; each input here is exactly
        # one 5 s window, so time == 1 — flatten to (batch, 1536).
        return {"embedding": emb.reshape(emb.shape[0], emb.shape[-1])}

    return infer


def make_perch_encoder(
    model: Callable[[np.ndarray], Mapping[str, np.ndarray]] | None = None,
) -> Encoder:
    """Build the Perch 2.0 encoder.

    `model` is injectable for contract tests: a callable taking a
    (n_windows, 160000) float32 batch and returning a mapping with
    'embedding' of shape (n_windows, 1536). Default None lazy-loads the real
    SavedModel (ImportError when the runtime is absent).
    """
    if model is None:
        model = _load_perch_model()

    def encode(samples: np.ndarray, sr: int) -> np.ndarray:
        windows = _frame_for_perch(samples, sr)
        emb = np.asarray(model(windows)["embedding"], dtype=np.float32)
        # Recording-level embedding: MEAN over windows (Perch paper §2.5.1).
        return emb.mean(axis=0).astype(np.float32)

    return encode


# ---------------------------------------------------------------------------
# BirdNET V2.4 — pure-numpy masked pooling + lazy wrapper
# ---------------------------------------------------------------------------


def _masked_mean(embeddings: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Mean over VALID segments of (n_segments, dim) embeddings.

    `mask` is birdnet's boolean embeddings_masked of the same shape; a segment
    is valid when mask.any(axis=-1). Invalid segments are excluded — averaging
    them in would dilute the recording embedding with padding garbage.
    """
    emb = np.asarray(embeddings, dtype=np.float32)
    valid = np.asarray(mask, dtype=bool).any(axis=-1)
    if not valid.any():
        raise ValueError("birdnet returned no valid segments for this recording")
    return emb[valid].mean(axis=0).astype(np.float32)


def _load_birdnet_model():
    """Lazy-load BirdNET V2.4 (ProtoBuf backend — the GPU path)."""
    try:
        import birdnet
    except ImportError as exc:
        raise ImportError(
            "the birdnet encoder requires the birdnet package: "
            "pip install birdnet  (on the fleet, use the tf.sif container "
            "from Task 6.1). Refusing to fall back to a stand-in."
        ) from exc
    return birdnet.load("acoustic", "2.4", "pb")


def make_birdnet_encoder(model=None) -> Encoder:
    """Build the BirdNET V2.4 encoder.

    `model` is injectable for contract tests: an object whose
    `encode_arrays([(samples, sr)], n_workers=1)` returns a result with
    `.embeddings` (1, n_segments, 1024) and boolean `.embeddings_masked` of
    the same shape. Default None lazy-loads the real model.

    We pass OUR sr straight through — the package resamples internally
    (scipy FFT). n_workers is pinned to 1: under Slurm the package would
    otherwise spawn one worker per core of the whole node.
    """
    if model is None:
        model = _load_birdnet_model()

    def encode(samples: np.ndarray, sr: int) -> np.ndarray:
        x = np.asarray(samples, dtype=np.float32).reshape(-1)
        result = model.encode_arrays([(x, sr)], n_workers=1)
        emb = np.asarray(result.embeddings)[0]
        mask = np.asarray(result.embeddings_masked)[0]
        return _masked_mean(emb, mask)

    return encode


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_ENCODER_FACTORIES: dict[str, Callable[[], Encoder]] = {
    "melsummary": lambda: _mean_log_mel,
    "perch": make_perch_encoder,
    "birdnet": make_birdnet_encoder,
}


def load_encoder(name: str) -> Encoder:
    """Return the named frozen encoder.

    `perch` and `birdnet` lazy-import their runtimes here and raise
    ImportError with install instructions when absent — failing at load time,
    before any audio is read, rather than silently substituting a stand-in:
    an embedding cache built with the wrong encoder is indistinguishable from
    a correct one.
    """
    if name not in _ENCODER_FACTORIES:
        raise ValueError(f"unknown encoder {name!r}; have {sorted(_ENCODER_FACTORIES)}")
    return _ENCODER_FACTORIES[name]()
