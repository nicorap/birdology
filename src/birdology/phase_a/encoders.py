"""Frozen encoders. Task 6 replaces the stand-in with Perch 2.0 and BirdNET."""
from __future__ import annotations

from collections.abc import Callable

import numpy as np

Encoder = Callable[[np.ndarray, int], np.ndarray]


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


_ENCODERS: dict[str, Encoder] = {"melsummary": _mean_log_mel}


def load_encoder(name: str) -> Encoder:
    """Return the named frozen encoder.

    `perch` and `birdnet` raise until Task 6 wires their checkpoints — an
    explicit NotImplementedError rather than a silent fallback to the stand-in,
    because an embedding cache built with the wrong encoder is indistinguishable
    from a correct one.
    """
    if name in ("perch", "birdnet"):
        raise NotImplementedError(
            f"{name} is wired in Task 6; pass --encoder melsummary to run the "
            "pipeline with the checkpoint-free stand-in"
        )
    if name not in _ENCODERS:
        raise ValueError(f"unknown encoder {name!r}; have {sorted(_ENCODERS)}")
    return _ENCODERS[name]
