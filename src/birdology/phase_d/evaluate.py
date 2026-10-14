"""Per-recording scoring and recordist-clustered bootstrap CIs.

Recordings from one recordist share equipment, site and habits, so they are
not independent: resampling recordings would understate the uncertainty.
Every CI here resamples RECORDISTS with replacement and pools their recordings.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np


def log_scores(S: np.ndarray) -> np.ndarray:
    """log of BirdNET's sigmoid scores, floored so log(0) cannot appear."""
    return np.log(np.clip(S, 1e-12, 1.0))


def finite(X: np.ndarray) -> np.ndarray:
    """Replace -inf (masked-out classes) by a very low finite score, for metrics."""
    return np.where(np.isfinite(X), X, -1e9)


def top1_hits(X: np.ndarray, true: np.ndarray) -> np.ndarray:
    """1.0 where the arg-max class is the primary label, else 0.0."""
    return (np.asarray(X).argmax(1) == np.asarray(true)).astype(float)


def soft_hits(X: np.ndarray, multilabels: Sequence[Sequence[str]], codes: Sequence[str]) -> np.ndarray:
    """1.0 where the arg-max class is ANY labelled species (primary or background)."""
    pred = np.asarray(X).argmax(1)
    return np.array([codes[p] in set(m) for p, m in zip(pred, multilabels)], dtype=float)


def label_matrix(multilabels: Sequence[Sequence[str]], col: Mapping[str, int], n_classes: int) -> np.ndarray:
    """Multi-hot int8 matrix; labels absent from ``col`` are ignored."""
    Y = np.zeros((len(multilabels), n_classes), dtype=np.int8)
    for i, labs in enumerate(multilabels):
        for c in labs:
            j = col.get(c)
            if j is not None:
                Y[i, j] = 1
    return Y


class RecordistBootstrap:
    """Percentile CIs for the mean of a per-recording quantity, clustered by recordist.

    The resampling plan is fixed at construction (seeded), so every quantity
    evaluated with one instance uses the SAME resamples -- differences between
    scorers are then paired.
    """

    def __init__(self, recordists: Sequence[str], n_boot: int = 2000, seed: int = 0):
        rec = np.asarray(recordists)
        self.recordists = np.array(sorted(set(rec.tolist())))
        if len(self.recordists) < 2:
            raise ValueError("a recordist bootstrap needs at least two recordists")
        self._groups = [np.flatnonzero(rec == r) for r in self.recordists]
        rng = np.random.default_rng(seed)
        self._plans = [rng.integers(0, len(self._groups), len(self._groups)) for _ in range(n_boot)]

    def ci(self, values: np.ndarray, level: float = 0.95) -> tuple[float, float]:
        values = np.asarray(values, dtype=float)
        means = [values[np.concatenate([self._groups[g] for g in plan])].mean() for plan in self._plans]
        tail = (1 - level) / 2 * 100
        lo, hi = np.percentile(means, [tail, 100 - tail])
        return float(lo), float(hi)
