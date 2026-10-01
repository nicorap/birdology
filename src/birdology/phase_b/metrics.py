"""Evaluation metrics for Phase B fusion — plain numpy, no sklearn.

cmAP (class-mean average precision) is the primary number of the paper
(BirdSet's headline metric). AP is rank-based: the mean over positives of
precision at each positive's rank, under a stable descending score sort.
Implementations here are pure numpy because Task 6's sweep calls them
millions of times — and because our primary metric must be understood
exactly, not imported. sklearn is used only in the tests, as an oracle.
"""
from __future__ import annotations

from typing import Callable

import numpy as np


def gather_scores(
    logits: np.ndarray,
    class_idx: dict[str, int],
    scoreable: list[str],
) -> np.ndarray:
    """Select logits columns BY CODE via *class_idx*, in *scoreable* order.

    ``(n_rows, n_classes)`` full encoder output -> ``(n_rows,
    len(scoreable))`` float32, column j holding ``scoreable[j]``'s scores.
    The gather is never positional: reordering the label file (and hence
    the logits axis) changes nothing as long as class_idx matches it.
    """
    cols = [class_idx[code] for code in scoreable]
    return np.asarray(logits)[:, cols].astype(np.float32, copy=False)


def _average_precision(scores: np.ndarray, y: np.ndarray) -> float:
    """Rank-based AP for one class (sklearn-equivalent definition).

    Mean over positives of precision at each positive's rank, with ties
    broken by stable ordering of the descending sort.
    """
    order = np.argsort(-scores, kind="stable")
    y_sorted = y[order].astype(np.float64)
    cum_pos = np.cumsum(y_sorted)
    ranks = np.arange(1, y_sorted.size + 1, dtype=np.float64)
    precision_at = cum_pos / ranks
    n_pos = cum_pos[-1]
    return float(precision_at[y_sorted == 1].sum() / n_pos)


def _auroc(scores: np.ndarray, y: np.ndarray) -> float:
    """One-vs-rest AUROC for one class via the Mann-Whitney U statistic
    with tie-averaged ranks (sklearn-equivalent trapezoidal ROC area).
    """
    scores = scores.astype(np.float64)
    order = np.argsort(scores, kind="stable")
    ranks = np.empty(scores.size, dtype=np.float64)
    ranks[order] = np.arange(1, scores.size + 1, dtype=np.float64)
    # Average ranks within tied score groups.
    sorted_scores = scores[order]
    tie_starts = np.flatnonzero(
        np.r_[True, sorted_scores[1:] != sorted_scores[:-1]]
    )
    tie_ends = np.r_[tie_starts[1:], scores.size]
    for start, end in zip(tie_starts, tie_ends):
        if end - start > 1:
            ranks[order[start:end]] = (start + 1 + end) / 2.0
    pos = y == 1
    n_pos = int(pos.sum())
    n_neg = scores.size - n_pos
    u = ranks[pos].sum() - n_pos * (n_pos + 1) / 2.0
    return float(u / (n_pos * n_neg))


def auroc_macro(scores: np.ndarray, y: np.ndarray) -> tuple[float, int]:
    """Macro one-vs-rest AUROC over classes with >=1 positive AND >=1
    negative; same ``(value, n_scored)`` contract as :func:`cmap` —
    degenerate classes are excluded and counted, ``(nan, 0)`` when none
    qualify.
    """
    scores = np.asarray(scores)
    y = np.asarray(y)
    aurocs = [
        _auroc(scores[:, j], y[:, j])
        for j in range(y.shape[1])
        if y[:, j].any() and not y[:, j].all()
    ]
    if not aurocs:
        return float("nan"), 0
    return float(np.mean(aurocs)), len(aurocs)


def cmap(scores: np.ndarray, y: np.ndarray) -> tuple[float, int]:
    """Class-mean average precision over classes WITH >=1 positive.

    Zero-positive classes are EXCLUDED from the mean and the exclusion is
    COUNTED: returns ``(mean_AP, n_scored)`` where n_scored is the number
    of classes that entered the mean. ``n_scored == 0`` -> ``(nan, 0)``,
    never a ZeroDivisionError.
    """
    scores = np.asarray(scores)
    y = np.asarray(y)
    aps = [
        _average_precision(scores[:, j], y[:, j])
        for j in range(y.shape[1])
        if y[:, j].any()
    ]
    if not aps:
        return float("nan"), 0
    return float(np.mean(aps)), len(aps)


def top1(scores: np.ndarray, y: np.ndarray) -> float:
    """Fraction of rows whose argmax column is a positive in *y*.

    Computed over rows with >=1 positive label only: a row with no
    positives (a no-bird segment) has no notion of top-1 correctness and
    is excluded from the denominator. All rows positive-free -> nan.
    """
    scores = np.asarray(scores)
    y = np.asarray(y)
    has_pos = y.any(axis=1)
    if not has_pos.any():
        return float("nan")
    hits = y[np.arange(y.shape[0]), scores.argmax(axis=1)] == 1
    return float(hits[has_pos].mean())


def stratified(
    scores: np.ndarray,
    y: np.ndarray,
    strata: np.ndarray,
    metric: Callable[[np.ndarray, np.ndarray], tuple[float, int]],
) -> dict[object, tuple[float, int]]:
    """Re-compute *metric* on each stratum's row subset.

    *strata* is a ``(n_rows,)`` array of arbitrary hashable labels
    (frequency decile ints, bool expected-under-prior, test-set strings);
    keys are preserved as-is in the returned dict. A stratum where the
    metric can score nothing returns the metric's own degenerate value
    (``(nan, 0)``), never an exception.
    """
    scores = np.asarray(scores)
    y = np.asarray(y)
    strata = np.asarray(strata)
    result: dict[object, tuple[float, int]] = {}
    for key in dict.fromkeys(strata.tolist()):  # first-appearance order
        mask = strata == key
        result[key] = metric(scores[mask], y[mask])
    return result
