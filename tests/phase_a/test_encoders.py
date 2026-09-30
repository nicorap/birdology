"""Contract tests for the real encoder wrappers (mocked models, no TF).

These pin the framing/normalization/pooling math that would otherwise
silently poison 528k cached embeddings: shapes, content POSITION (center-pad
vs right-pad), per-window normalization, masked pooling, the sr guard, and
the loud ImportError paths. The golden-reference tests against the official
tooling are cluster-marked elsewhere; nothing here imports tensorflow or
birdnet.
"""
from __future__ import annotations

import sys
from types import SimpleNamespace

import numpy as np
import pytest

from birdology.phase_a.encoders import (
    PERCH_SR,
    PERCH_WINDOW_SAMPLES,
    _frame_for_perch,
    _masked_mean,
    load_encoder,
    make_birdnet_encoder,
    make_perch_encoder,
)

# ---------------------------------------------------------------------------
# _frame_for_perch: shapes and content position
# ---------------------------------------------------------------------------


def test_exactly_five_seconds_is_one_window():
    rng = np.random.default_rng(0)
    samples = rng.standard_normal(PERCH_WINDOW_SAMPLES).astype(np.float32)
    windows = _frame_for_perch(samples, PERCH_SR)
    assert windows.shape == (1, PERCH_WINDOW_SAMPLES)
    assert windows.dtype == np.float32


def test_seven_seconds_is_two_windows_with_correct_content():
    """7 s -> 2 windows at hop 5 s; window 1 holds samples[160000:] + trailing pad.

    Spikes on a zero background make the window boundary observable: a spike
    at absolute index 160005 must land at index 5 of window 1. A 2.5 s hop
    (M1) also yields 2 windows for 7 s, so the CONTENT check is what kills it.
    """
    samples = np.zeros(7 * PERCH_SR, dtype=np.float32)
    samples[10] = 1.0
    samples[PERCH_WINDOW_SAMPLES + 5] = -1.0
    windows = _frame_for_perch(samples, PERCH_SR)
    assert windows.shape == (2, PERCH_WINDOW_SAMPLES)
    assert int(np.argmax(np.abs(windows[0]))) == 10
    assert int(np.argmax(np.abs(windows[1]))) == 5
    assert windows[1][5] < 0


def test_ten_seconds_is_exactly_two_windows():
    """10 s at hop 5 s -> exactly 2 windows (a 2.5 s hop would give 3)."""
    rng = np.random.default_rng(1)
    samples = rng.standard_normal(10 * PERCH_SR).astype(np.float32)
    windows = _frame_for_perch(samples, PERCH_SR)
    assert windows.shape == (2, PERCH_WINDOW_SAMPLES)


def test_short_recording_is_center_padded():
    """3 s -> one window; content sits in the MIDDLE, zeros on both sides.

    A spike at signal index 0 must land at index 32000 (left pad =
    (160000 - 96000) // 2). Right-padding (M4) would leave it at index 0.
    """
    samples = np.zeros(3 * PERCH_SR, dtype=np.float32)
    samples[0] = 1.0
    windows = _frame_for_perch(samples, PERCH_SR)
    assert windows.shape == (1, PERCH_WINDOW_SAMPLES)
    left_pad = (PERCH_WINDOW_SAMPLES - 3 * PERCH_SR) // 2
    assert int(np.argmax(np.abs(windows[0]))) == left_pad


# ---------------------------------------------------------------------------
# _frame_for_perch: per-window normalization
# ---------------------------------------------------------------------------


def test_window_is_mean_subtracted():
    """A strong DC offset must be gone after normalization."""
    t = np.arange(PERCH_WINDOW_SAMPLES, dtype=np.float32)
    samples = 0.5 + 0.1 * np.sin(2 * np.pi * 440 * t / PERCH_SR).astype(np.float32)
    windows = _frame_for_perch(samples, PERCH_SR)
    assert abs(float(windows[0].mean())) < 1e-6


def test_window_peak_is_quarter():
    rng = np.random.default_rng(2)
    samples = (5.0 * rng.standard_normal(PERCH_WINDOW_SAMPLES)).astype(np.float32)
    windows = _frame_for_perch(samples, PERCH_SR)
    assert float(np.max(np.abs(windows[0]))) == pytest.approx(0.25, abs=1e-6)


def test_all_zero_window_stays_zero_without_nan():
    samples = np.zeros(PERCH_WINDOW_SAMPLES, dtype=np.float32)
    windows = _frame_for_perch(samples, PERCH_SR)
    assert np.all(windows == 0.0)
    assert not np.any(np.isnan(windows))


# ---------------------------------------------------------------------------
# sr guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_sr", [44100, 48000, 16000])
def test_perch_rejects_non_32khz(bad_sr):
    samples = np.zeros(bad_sr, dtype=np.float32)
    with pytest.raises(ValueError, match="32"):
        _frame_for_perch(samples, bad_sr)


def test_perch_encoder_rejects_non_32khz_end_to_end():
    encoder = make_perch_encoder(model=lambda w: {"embedding": np.zeros((len(w), 4))})
    with pytest.raises(ValueError, match="32"):
        encoder(np.zeros(1000, dtype=np.float32), 44100)


# ---------------------------------------------------------------------------
# perch pooling (mocked model)
# ---------------------------------------------------------------------------


def test_perch_mean_pools_over_windows():
    def fake_model(windows: np.ndarray):
        assert windows.shape[1] == PERCH_WINDOW_SAMPLES
        # Distinct per-window embeddings so the mean is observable.
        emb = np.stack(
            [np.full(1536, float(i + 1), dtype=np.float32) for i in range(len(windows))]
        )
        return {"embedding": emb}

    encoder = make_perch_encoder(model=fake_model)
    samples = np.zeros(10 * PERCH_SR, dtype=np.float32)
    samples[::PERCH_SR] = 1.0  # non-degenerate content
    vec = encoder(samples, PERCH_SR)
    assert vec.shape == (1536,)
    assert vec.dtype == np.float32
    # Two windows with embeddings 1.0 and 2.0 -> mean 1.5.
    np.testing.assert_allclose(vec, np.full(1536, 1.5, dtype=np.float32))


# ---------------------------------------------------------------------------
# birdnet masked pooling
# ---------------------------------------------------------------------------


def test_masked_mean_excludes_invalid_segments():
    """Including the invalid segment would change the answer (999s)."""
    emb = np.array(
        [[1.0, 3.0], [3.0, 5.0], [999.0, 999.0]],
        dtype=np.float32,
    )
    mask = np.array([[True, True], [True, False], [False, False]])
    pooled = _masked_mean(emb, mask)
    np.testing.assert_allclose(pooled, np.array([2.0, 4.0], dtype=np.float32))


def test_masked_mean_all_invalid_raises():
    emb = np.ones((2, 4), dtype=np.float32)
    mask = np.zeros((2, 4), dtype=bool)
    with pytest.raises(ValueError, match="valid segments"):
        _masked_mean(emb, mask)


def test_birdnet_encoder_passes_sr_through_and_pins_workers():
    calls = {}

    class FakeModel:
        def encode_arrays(self, arrays, n_workers):
            calls["arrays"] = arrays
            calls["n_workers"] = n_workers
            emb = np.array([[[1.0, 3.0], [5.0, 7.0], [999.0, 999.0]]], dtype=np.float32)
            mask = np.array([[[True, True], [True, True], [False, False]]])
            return SimpleNamespace(embeddings=emb, embeddings_masked=mask)

    encoder = make_birdnet_encoder(model=FakeModel())
    samples = np.ones(32000, dtype=np.float32)
    vec = encoder(samples, 32000)
    assert calls["n_workers"] == 1
    (passed_samples, passed_sr) = calls["arrays"][0]
    assert passed_sr == 32000  # OUR sr goes through; the package resamples
    assert passed_samples.dtype == np.float32
    np.testing.assert_allclose(vec, np.array([3.0, 5.0], dtype=np.float32))
    assert vec.dtype == np.float32


# ---------------------------------------------------------------------------
# ImportError paths (simulate absent runtimes; loud failure, no fallback)
# ---------------------------------------------------------------------------


def test_perch_raises_importerror_with_install_instructions(monkeypatch):
    monkeypatch.setitem(sys.modules, "perch_hoplite", None)
    monkeypatch.setitem(sys.modules, "perch_hoplite.zoo", None)
    with pytest.raises(ImportError, match="pip install perch-hoplite"):
        load_encoder("perch")


def test_birdnet_raises_importerror_with_install_instructions(monkeypatch):
    monkeypatch.setitem(sys.modules, "birdnet", None)
    with pytest.raises(ImportError, match="pip install birdnet"):
        load_encoder("birdnet")


# ---------------------------------------------------------------------------
# registry
# ---------------------------------------------------------------------------


def test_melsummary_unchanged():
    encoder = load_encoder("melsummary")
    vec = encoder(np.ones(32000, dtype=np.float32), 32000)
    assert vec.shape == (64,)
    assert vec.dtype == np.float32


def test_unknown_encoder_raises_valueerror():
    with pytest.raises(ValueError, match="unknown encoder"):
        load_encoder("clap")


def test_factories_return_callables_with_mocked_runtime():
    perch = make_perch_encoder(
        model=lambda w: {"embedding": np.ones((len(w), 1536), dtype=np.float32)}
    )
    assert callable(perch)

    class FakeModel:
        def encode_arrays(self, arrays, n_workers):
            return SimpleNamespace(
                embeddings=np.ones((1, 1, 1024), dtype=np.float32),
                embeddings_masked=np.ones((1, 1, 1024), dtype=bool),
            )

    birdnet = make_birdnet_encoder(model=FakeModel())
    assert callable(birdnet)
    assert birdnet(np.zeros(48000, dtype=np.float32), 32000).shape == (1024,)


def test_module_import_pulls_no_tensorflow():
    """encoders.py must stay importable on TF-less machines (melsummary users)."""
    assert "tensorflow" not in sys.modules
