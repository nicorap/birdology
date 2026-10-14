"""Per-recording metrics and the recordist-clustered bootstrap."""
import numpy as np
import pytest

from birdology.phase_d.evaluate import (
    RecordistBootstrap, finite, label_matrix, log_scores, soft_hits, top1_hits,
)


def test_log_scores_never_returns_minus_inf():
    assert np.isfinite(log_scores(np.array([[0.0, 1.0, 0.5]]))).all()


def test_top1_and_soft_hits():
    X = np.array([[0.1, 0.9, 0.0], [0.8, 0.1, 0.1]])
    assert top1_hits(X, np.array([1, 2])).tolist() == [1.0, 0.0]
    # second recording predicts "a", which is a background species there
    assert soft_hits(X, [["b"], ["c", "a"]], ["a", "b", "c"]).tolist() == [1.0, 1.0]


def test_finite_replaces_masked_classes():
    X = finite(np.array([[-np.inf, 0.0]]))
    assert np.isfinite(X).all() and X.argmax() == 1


def test_label_matrix_ignores_unknown_codes():
    Y = label_matrix([["a", "zz"], ["b"]], {"a": 0, "b": 1}, 3)
    assert Y.tolist() == [[1, 0, 0], [0, 1, 0]]


def test_bootstrap_ci_brackets_the_mean_and_is_zero_for_constants():
    rec = np.repeat(["r1", "r2", "r3", "r4"], 5)
    vals = np.r_[np.ones(10), np.zeros(10)]
    bs = RecordistBootstrap(rec, n_boot=500)
    lo, hi = bs.ci(vals)
    assert lo <= vals.mean() <= hi
    assert bs.ci(np.full(20, 0.3)) == pytest.approx((0.3, 0.3))


def test_bootstrap_resamples_recordists_not_recordings():
    # one recordist holds every success: clustering must make the CI wide
    rec = np.r_[np.repeat("big", 50), [f"r{i}" for i in range(5)]]
    vals = np.r_[np.ones(50), np.zeros(5)]
    lo, hi = RecordistBootstrap(rec, n_boot=500).ci(vals)
    assert lo < 0.5 and hi > 0.95


def test_bootstrap_needs_two_recordists():
    with pytest.raises(ValueError):
        RecordistBootstrap(["only"] * 4)
