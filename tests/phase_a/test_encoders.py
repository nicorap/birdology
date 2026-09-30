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
    _batch_for_perch,
    _masked_mean,
    _pool_perch_outputs,
    load_encoder,
    make_birdnet_encoder,
    make_perch_encoder,
)

# ---------------------------------------------------------------------------
# _batch_for_perch: validation and shaping (framing/normalization now live
# INSIDE hoplite's batch_embed -- measured by reading its source on the fleet
# 2026-09-30: frame_audio + normalize_audio are its first two lines. Our first
# implementation reproduced both stages faithfully -- librosa-oracle
# byte-identical -- and STILL failed the golden gate at ~1.2e-4, because
# hoplite re-normalized our already-normalized windows and float32 drift got
# amplified by the network. Equality comes from SHARING the reference code
# path, not imitating it. The oracle tests died with the imitation; the
# golden-reference gate on the fleet is now the sole fidelity proof.)


def test_batch_for_perch_shapes_one_recording_as_a_batch():
    samples = np.arange(7 * PERCH_SR, dtype=np.float32)
    batch = _batch_for_perch(samples, PERCH_SR)
    assert batch.shape == (1, 7 * PERCH_SR)
    assert (batch[0] == samples).all()


def test_batch_for_perch_flattens_and_casts():
    samples = np.ones((2, PERCH_SR), dtype=np.float64)
    batch = _batch_for_perch(samples, PERCH_SR)
    assert batch.dtype == np.float32
    assert batch.shape == (1, 2 * PERCH_SR)


def test_batch_for_perch_refuses_empty_audio():
    with pytest.raises(ValueError, match="empty"):
        _batch_for_perch(np.array([], dtype=np.float32), PERCH_SR)


# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_sr", [44100, 48000, 16000])
def test_perch_rejects_non_32khz(bad_sr):
    samples = np.zeros(bad_sr, dtype=np.float32)
    with pytest.raises(ValueError, match="32"):
        _batch_for_perch(samples, bad_sr)


def test_perch_encoder_rejects_non_32khz_end_to_end():
    encoder = make_perch_encoder(model=lambda w: {"embedding": np.zeros((len(w), 4))})
    with pytest.raises(ValueError, match="32"):
        encoder(np.zeros(1000, dtype=np.float32), 44100)


# ---------------------------------------------------------------------------
# perch pooling (mocked model)
# ---------------------------------------------------------------------------


def test_perch_mean_pools_a_2d_mock_return():
    """The mock receives the (1, n_samples) RAW batch (framing is hoplite's
    job now) and returns per-window embeddings; the encoder mean-pools."""
    def fake_model(batch: np.ndarray):
        assert batch.shape == (1, 10 * PERCH_SR)  # raw audio, unframed
        emb = np.stack(
            [np.full(1536, 1.0, dtype=np.float32), np.full(1536, 2.0, dtype=np.float32)]
        )
        return {"embedding": emb}

    encoder = make_perch_encoder(model=fake_model)
    samples = np.zeros(10 * PERCH_SR, dtype=np.float32)
    samples[::PERCH_SR] = 1.0  # non-degenerate content
    vec = encoder(samples, PERCH_SR)
    assert vec.shape == (1536,)
    assert vec.dtype == np.float32
    np.testing.assert_allclose(vec, np.full(1536, 1.5, dtype=np.float32))


def test_perch_passes_through_an_already_pooled_mock_return():
    def fake_model(batch: np.ndarray):
        return {"embedding": np.full(1536, 3.0, dtype=np.float32)}

    encoder = make_perch_encoder(model=fake_model)
    vec = encoder(np.zeros(5 * PERCH_SR, dtype=np.float32), PERCH_SR)
    np.testing.assert_allclose(vec, np.full(1536, 3.0, dtype=np.float32))


# ---------------------------------------------------------------------------
# perch adapter pooling: _pool_perch_outputs (pure, no TF)
# ---------------------------------------------------------------------------


def _asymmetric_fixture(shape: tuple[int, ...]) -> np.ndarray:
    """Values unique per position so ANY reshuffling changes the answer."""
    return np.arange(np.prod(shape), dtype=np.float32).reshape(shape) + 1.0


def test_pool_perch_outputs_mean_pools_a_2d_window_matrix():
    x = np.stack([np.full(1536, 1.0), np.full(1536, 3.0)]).astype(np.float32)
    out = _pool_perch_outputs(x)
    assert out.shape == (1536,)
    np.testing.assert_allclose(out, np.full(1536, 2.0, dtype=np.float32))


def test_pool_perch_outputs_pools_the_MEASURED_4d_shape():
    """batch_embed on the real perch_v2 returns (1, n_windows, 1, 1536) for
    one recording -- measured on the fleet 2026-09-30. Asymmetric values so
    a scrambled axis order changes the answer."""
    n = 3
    x = np.arange(n * 1536, dtype=np.float32).reshape(1, n, 1, 1536)
    out = _pool_perch_outputs(x)
    assert out.shape == (1536,)
    np.testing.assert_allclose(out, x[0, :, 0, :].mean(axis=0))


def test_pool_perch_outputs_pools_the_embed_style_3d_shape():
    n = 2
    x = np.arange(n * 1536, dtype=np.float32).reshape(n, 1, 1536)
    out = _pool_perch_outputs(x)
    np.testing.assert_allclose(out, x[:, 0, :].mean(axis=0))


def test_pool_perch_outputs_refuses_a_multi_recording_batch():
    """batch > 1 means several recordings; pooling across them would mix
    embeddings from different files into one row."""
    x = np.zeros((2, 3, 1, 1536), dtype=np.float32)
    with pytest.raises(ValueError, match="ONE recording"):
        _pool_perch_outputs(x)


def test_pool_perch_outputs_refuses_unknown_rank():
    with pytest.raises(ValueError, match="unexpected"):
        _pool_perch_outputs(np.zeros((1, 2, 3, 4, 1536), dtype=np.float32))


# ---------------------------------------------------------------------------


def test_masked_mean_excludes_PADDING_segments():
    """True marks PADDING (numpy MaskedArray convention — MEASURED on the
    fleet: a mixed-length batch pads the short clip with True-masked NaN
    segments). Including the padding segment would change the answer (999s);
    with NaN padding it would poison the whole mean."""
    emb = np.array(
        [[1.0, 3.0], [3.0, 5.0], [999.0, 999.0]],
        dtype=np.float32,
    )
    # Segments 0,1 valid (False); segment 2 is padding (True on any feature).
    mask = np.array([[False, False], [False, True], [True, True]])
    pooled = _masked_mean(emb, mask)
    np.testing.assert_allclose(pooled, np.array([1.0, 3.0], dtype=np.float32))


def test_masked_mean_with_nan_padding_matches_the_measured_failure():
    """The real shape of the bug: padding embeddings are NaN. Under the
    inverted convention the mean was NaN or the call raised no-valid."""
    emb = np.array([[2.0, 6.0], [np.nan, np.nan]], dtype=np.float32)
    mask = np.array([[False, False], [True, True]])
    pooled = _masked_mean(emb, mask)
    np.testing.assert_allclose(pooled, np.array([2.0, 6.0], dtype=np.float32))


def test_masked_mean_all_padding_raises():
    emb = np.ones((2, 4), dtype=np.float32)
    mask = np.ones((2, 4), dtype=bool)  # every segment marked padding
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
            mask = np.array([[[False, False], [False, False], [True, True]]])
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
            mask = np.zeros((1, 1, 4), dtype=bool)  # False = valid (measured)
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
                embeddings_masked=np.zeros((1, 1, 1024), dtype=bool),
            )

    birdnet = make_birdnet_encoder(model=FakeModel())
    assert callable(birdnet)
    assert birdnet(np.zeros(48000, dtype=np.float32), 32000).shape == (1024,)


def test_module_import_pulls_no_tensorflow():
    """encoders.py must stay importable on TF-less machines (melsummary users)."""
    assert "tensorflow" not in sys.modules
