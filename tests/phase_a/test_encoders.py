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
    PERCH_HOP_SAMPLES,
    PERCH_SR,
    PERCH_TARGET_PEAK,
    PERCH_WINDOW_SAMPLES,
    _frame_for_perch,
    _masked_mean,
    _pool_perch_outputs,
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


def test_seven_seconds_is_one_window_remainder_dropped():
    """7 s -> ONE window (the first 5 s); the 2 s remainder is DROPPED.

    perch-hoplite frames with librosa.util.frame(window=hop=160000), which
    keeps only FULL windows. Zero-padding the remainder instead would, after
    mean-subtract + peak-normalize, present a ~synthetic DC plateau to the
    model as real audio (the review's oracle-verified DC-plateau failure).
    The spike in the remainder must leave NO trace: the single window must
    be byte-identical to framing just the first 5 s.
    """
    samples = np.zeros(7 * PERCH_SR, dtype=np.float32)
    samples[10] = 1.0
    samples[PERCH_WINDOW_SAMPLES + 5] = -1.0  # lives in the dropped remainder
    windows = _frame_for_perch(samples, PERCH_SR)
    assert windows.shape == (1, PERCH_WINDOW_SAMPLES)
    assert int(np.argmax(np.abs(windows[0]))) == 10
    expected = _frame_for_perch(samples[:PERCH_WINDOW_SAMPLES], PERCH_SR)
    np.testing.assert_array_equal(windows, expected)


def test_five_point_one_seconds_is_one_window_remainder_dropped():
    """5.1 s -> ONE window equal to framing the first 5 s alone.

    This is the review's measured worst case: with pad-the-remainder, the
    second window was ~97% synthetic plateau at peak 0.25, averaged into
    the recording embedding at weight 1/2.
    """
    rng = np.random.default_rng(42)
    samples = rng.standard_normal(int(5.1 * PERCH_SR)).astype(np.float32)
    windows = _frame_for_perch(samples, PERCH_SR)
    assert windows.shape == (1, PERCH_WINDOW_SAMPLES)
    np.testing.assert_array_equal(
        windows, _frame_for_perch(samples[: PERCH_WINDOW_SAMPLES], PERCH_SR)
    )


def test_nine_point_nine_seconds_is_one_window_remainder_dropped():
    """9.9 s -> ONE window (oracle-pinned): 4.9 s of remainder is dropped."""
    rng = np.random.default_rng(42)
    samples = rng.standard_normal(int(9.9 * PERCH_SR)).astype(np.float32)
    windows = _frame_for_perch(samples, PERCH_SR)
    assert windows.shape == (1, PERCH_WINDOW_SAMPLES)
    np.testing.assert_array_equal(
        windows, _frame_for_perch(samples[: PERCH_WINDOW_SAMPLES], PERCH_SR)
    )


def test_ten_seconds_is_exactly_two_windows():
    """10 s at hop 5 s -> exactly 2 windows (a 2.5 s hop would give 3)."""
    rng = np.random.default_rng(1)
    samples = rng.standard_normal(10 * PERCH_SR).astype(np.float32)
    windows = _frame_for_perch(samples, PERCH_SR)
    assert windows.shape == (2, PERCH_WINDOW_SAMPLES)


# ---------------------------------------------------------------------------
# _frame_for_perch: librosa oracle (executable fidelity claim)
# ---------------------------------------------------------------------------

try:  # oracle-only dependency; its absence must skip ONLY the oracle tests
    import librosa
except ImportError:  # pragma: no cover - exercised only on librosa-less envs
    librosa = None

requires_librosa = pytest.mark.skipif(
    librosa is None,
    reason="librosa oracle: install the 'bench' extra (uv sync --extra bench)",
)


def _librosa_reference(samples: np.ndarray) -> np.ndarray:
    """hoplite's framing via librosa itself, + the same normalization.

    <1 window: librosa.util.pad_center; >=1 window: librosa.util.frame
    (window == hop == 160000, drops the trailing remainder). Then per-window
    mean-subtract and peak-normalize to 0.25 — the composed pipeline the
    review's oracle compared byte-for-byte.
    """
    x = np.asarray(samples, dtype=np.float32).reshape(-1)
    if x.size < PERCH_WINDOW_SAMPLES:
        windows = librosa.util.pad_center(x, size=PERCH_WINDOW_SAMPLES)[np.newaxis, :]
    else:
        windows = librosa.util.frame(
            x, frame_length=PERCH_WINDOW_SAMPLES, hop_length=PERCH_HOP_SAMPLES, axis=0
        )
    windows = windows - windows.mean(axis=1, keepdims=True)
    peaks = np.max(np.abs(windows), axis=1, keepdims=True)
    safe = np.where(peaks > 0, peaks, 1.0)
    return (windows * (PERCH_TARGET_PEAK / safe)).astype(np.float32)


@requires_librosa
@pytest.mark.parametrize(
    "seconds", [3.0, 5.0, 5.1, 7.0, 9.9, 10.0], ids=lambda s: f"{s}s"
)
def test_framing_matches_librosa_oracle(seconds):
    """Byte-identity with librosa's own framing at every oracle length.

    These are the review's exact oracle cases: 3 s (center-pad), 5/10 s
    (exact multiples), 5.1/7/9.9 s (remainder must be dropped).
    """
    rng = np.random.default_rng(42)
    samples = rng.standard_normal(int(seconds * PERCH_SR)).astype(np.float32)
    ours = _frame_for_perch(samples, PERCH_SR)
    ref = _librosa_reference(samples)
    assert ours.shape == ref.shape
    np.testing.assert_array_equal(ours, ref)


@requires_librosa
@pytest.mark.parametrize("n", [96000, 96001, 159999])
def test_center_pad_matches_librosa_pad_center(n):
    """Odd/edge pad amounts: the pad//2 split must match librosa.util.pad_center."""
    rng = np.random.default_rng(7)
    samples = rng.standard_normal(n).astype(np.float32)
    np.testing.assert_array_equal(
        _frame_for_perch(samples, PERCH_SR), _librosa_reference(samples)
    )


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
# perch adapter pooling: _pool_perch_outputs (pure, no TF)
# ---------------------------------------------------------------------------


def _asymmetric_fixture(shape: tuple[int, ...]) -> np.ndarray:
    """Values unique per position so ANY reshuffling changes the answer."""
    return np.arange(np.prod(shape), dtype=np.float32).reshape(shape) + 1.0


def test_pool_perch_outputs_passes_flat_batch_through():
    emb = _asymmetric_fixture((3, 1536))
    out = _pool_perch_outputs(emb)
    assert out.shape == (3, 1536)
    assert out.dtype == np.float32
    np.testing.assert_array_equal(out, emb)


def test_pool_perch_outputs_squeezes_time_axis():
    """(n, 1, 1536) -> (n, 1536) preserving every value at its position.

    Asymmetric values make a scrambled reshape (e.g. transpose-equivalent)
    change the result — this is the reviewer's surviving R3 made killable.
    """
    emb3 = _asymmetric_fixture((4, 1, 1536))
    out = _pool_perch_outputs(emb3)
    assert out.shape == (4, 1536)
    np.testing.assert_array_equal(out, emb3[:, 0, :])
    # Spot-pin positions a transpose/reorder would move:
    assert out[0, 0] == 1.0
    assert out[1, 0] == 1537.0
    assert out[3, 1535] == 4 * 1536.0


def test_pool_perch_outputs_rejects_multi_time_axis():
    """time > 1 must raise, never be silently reshaped across windows."""
    with pytest.raises(ValueError, match="time axis"):
        _pool_perch_outputs(_asymmetric_fixture((2, 3, 1536)))


def test_pool_perch_outputs_rejects_wrong_rank():
    with pytest.raises(ValueError, match="shape"):
        _pool_perch_outputs(np.ones(1536, dtype=np.float32))


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
        def encode_arrays(self, arrays, n_workers, device):
            calls["arrays"] = arrays
            calls["n_workers"] = n_workers
            calls["device"] = device
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


def test_birdnet_encoder_forwards_device_explicitly():
    """device is chosen at ENCODE time; the wrapper must forward it every call.

    Default 'GPU' (the fleet runs GPU nodes); an explicit 'CPU' must also
    reach encode_arrays. A keyword-only fake makes a positional or dropped
    forward fail loudly.
    """
    seen = []

    class FakeModel:
        def encode_arrays(self, arrays, *, n_workers, device):
            seen.append(device)
            emb = np.ones((1, 1, 4), dtype=np.float32)
            mask = np.ones((1, 1, 4), dtype=bool)
            return SimpleNamespace(embeddings=emb, embeddings_masked=mask)

    fake = FakeModel()
    make_birdnet_encoder(model=fake)(np.zeros(32000, dtype=np.float32), 32000)
    make_birdnet_encoder(model=fake, device="CPU")(
        np.zeros(32000, dtype=np.float32), 32000
    )
    assert seen == ["GPU", "CPU"]


def test_birdnet_gpu_failure_reraises_with_both_known_causes():
    """A GPU-path backend failure must name the two fleet failure modes.

    birdnet's GPU backend asserts exactly ONE GPU visible to TF in its
    worker process; on the fleet that assertion fires either because TF
    sees zero GPUs (tf.sif missing nvidia wheels / no --nv) or more than
    one (CUDA_VISIBLE_DEVICES unrestricted). The bare worker assertion is
    undebuggable from a Slurm log; the wrapper must re-raise with both
    causes and their remedies.
    """

    class ExplodingModel:
        def encode_arrays(self, arrays, n_workers, device):
            raise AssertionError("len(gpus) == 1")

    encoder = make_birdnet_encoder(model=ExplodingModel())
    with pytest.raises(RuntimeError) as excinfo:
        encoder(np.zeros(32000, dtype=np.float32), 32000)
    msg = str(excinfo.value)
    assert "ZERO GPUs" in msg and "--nv" in msg and "nvidia" in msg
    assert "MORE THAN ONE GPU" in msg and "CUDA_VISIBLE_DEVICES" in msg
    assert "--gres=gpu:1" in msg
    assert isinstance(excinfo.value.__cause__, AssertionError)


def test_birdnet_cpu_failure_is_not_rewritten_as_gpu_diagnostic():
    """device='CPU' failures re-raise untouched — the GPU causes don't apply."""

    class ExplodingModel:
        def encode_arrays(self, arrays, n_workers, device):
            raise ValueError("bad audio")

    encoder = make_birdnet_encoder(model=ExplodingModel(), device="CPU")
    with pytest.raises(ValueError, match="bad audio"):
        encoder(np.zeros(32000, dtype=np.float32), 32000)


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
        def encode_arrays(self, arrays, n_workers, device):
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


def test_pool_perch_outputs_accepts_the_MEASURED_4d_shape():
    """batch_embed on the real perch_v2 returns (n, 1, 1, 1536) — measured on
    the fleet 2026-09-30. The first golden-reference run failed here."""
    n = 3
    x = np.arange(n * 1536, dtype=np.float32).reshape(n, 1, 1, 1536)
    out = _pool_perch_outputs(x)
    assert out.shape == (n, 1536)
    # Asymmetric content check: row i must be row i's values, untangled.
    assert (out == x.reshape(n, 1536)).all()


def test_pool_perch_outputs_refuses_a_nonsingleton_middle_axis():
    """time=2 means the model saw >5 s inputs; squeezing would interleave."""
    x = np.zeros((2, 2, 1, 1536), dtype=np.float32)
    with pytest.raises(ValueError, match="non-singleton"):
        _pool_perch_outputs(x)
