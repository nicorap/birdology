"""The logits column: pooled species posteriors riding beside embeddings.

Per-recording logits are cached at embed time because recovering them later
means a second 581 GB pass over the corpus; the column costs ~31 GB for XCL.
Pooling direction is encoder-specific AND DELIBERATELY ASYMMETRIC:
Perch = MEAN over windows (hoplite's own recipe, matching the embedding),
BirdNET = MAX over segments (BirdSet's convention for recording-level
scores). Every fixture here is asymmetric — max != mean per class — so a
wrong pooling direction changes the answer instead of hiding.

Nothing here imports tensorflow or birdnet.
"""
from __future__ import annotations

import io
import logging
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import soundfile as sf

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.embed import (
    EMBEDDING_SCHEMA,
    LOGITS_SCHEMA,
    SCHEMA_VARIANT_KEY,
    embed_shard,
)
from birdology.phase_a.encoders import (
    PERCH_SR,
    _dense_birdnet_scores,
    _max_pool_birdnet_logits,
    _pool_perch_logits,
    load_encoder_with_logits,
    make_birdnet_encoder_with_logits,
    make_perch_encoder_with_logits,
)

# ---------------------------------------------------------------------------
# pure pooling functions
# ---------------------------------------------------------------------------

# Asymmetric across BOTH axes: per class, max != mean != value at any fixed
# window, so mean-vs-max confusion AND axis confusion each change the answer.
_PERCH_LOGITS_2D = np.array(
    [
        [0.0, 10.0, -4.0],
        [6.0, 2.0, -8.0],
        [3.0, 3.0, 12.0],
    ],
    dtype=np.float32,
)  # per-class mean: [3, 5, 0]; per-class max: [6, 10, 12] — never equal


def test_perch_logits_mean_pool_over_time_2d():
    out = _pool_perch_logits(_PERCH_LOGITS_2D)
    assert out.shape == (3,)
    assert out.dtype == np.float32
    np.testing.assert_allclose(out, np.array([3.0, 5.0, 0.0], dtype=np.float32))
    # The asymmetry guarantee: a max-pool mutation cannot sneak past.
    assert not np.allclose(out, _PERCH_LOGITS_2D.max(axis=0))


def test_perch_logits_mean_pool_over_time_MEASURED_3d_shape():
    """batch_embed reshapes logits['label'] to framed_audio.shape[:2] +
    (n_classes,) == (1, n_windows, n_classes) — measured from its source.
    Time is axis 1; pooling the wrong axis yields a wrong length or wrong
    values here."""
    x = _PERCH_LOGITS_2D[None, :, :]  # (1, 3 windows, 3 classes)
    out = _pool_perch_logits(x)
    assert out.shape == (3,)
    np.testing.assert_allclose(out, np.array([3.0, 5.0, 0.0], dtype=np.float32))


def test_perch_logits_refuses_a_multi_recording_batch():
    x = np.zeros((2, 3, 14795), dtype=np.float32)
    with pytest.raises(ValueError, match="ONE recording"):
        _pool_perch_logits(x)


def test_perch_logits_refuses_surprise_ranks():
    """Never squeeze/reshape blind: a cited API claim is a hypothesis until
    measured, and a surprise shape must fail loudly."""
    for bad in (np.zeros(5, dtype=np.float32), np.zeros((1, 2, 3, 4), dtype=np.float32)):
        with pytest.raises(ValueError, match="unexpected perch logits shape"):
            _pool_perch_logits(bad)


_BIRDNET_PREDICTIONS = np.array(
    [
        [0.9, 0.1, 0.0],
        [0.1, 0.5, 0.2],
        [0.2, 0.3, 0.8],
    ],
    dtype=np.float32,
)  # per-class max: [0.9, 0.5, 0.8]; per-class mean: [0.4, 0.3, 1/3] — never equal


def test_birdnet_logits_MAX_pool_per_birdset_convention():
    out = _max_pool_birdnet_logits(_BIRDNET_PREDICTIONS)
    assert out.shape == (3,)
    assert out.dtype == np.float32
    np.testing.assert_allclose(out, np.array([0.9, 0.5, 0.8], dtype=np.float32))
    # The deliberate asymmetry vs Perch: a mean-pool mutation must fail here.
    assert not np.allclose(out, _BIRDNET_PREDICTIONS.mean(axis=0))


def test_birdnet_logits_refuses_non_2d():
    for bad in (
        np.zeros(6, dtype=np.float32),
        np.zeros((1, 2, 3), dtype=np.float32),
    ):
        with pytest.raises(ValueError, match="unexpected birdnet predictions shape"):
            _max_pool_birdnet_logits(bad)


def test_birdnet_logits_refuses_zero_segments():
    with pytest.raises(ValueError, match="zero prediction segments"):
        _max_pool_birdnet_logits(np.zeros((0, 3), dtype=np.float32))


# ---------------------------------------------------------------------------
# _dense_birdnet_scores: identity verification, scatter, truncation guard
# ---------------------------------------------------------------------------

_BN_N_SPECIES = 7  # stand-in for 6522; what's pinned is k == n_species

# Dense (2 segments, 7 species), asymmetric per class: max != mean, and no
# two columns share values — a positional read of permuted columns changes
# the answer instead of hiding.
_BN_DENSE_PROBS = np.array(
    [
        [0.9, 0.1, 0.0, 0.4, 0.2, 0.7, 0.3],
        [0.1, 0.5, 0.2, 0.6, 0.8, 0.3, 0.05],
    ],
    dtype=np.float32,
)
_BN_DENSE_MAX = np.array([0.9, 0.5, 0.2, 0.6, 0.8, 0.7, 0.3], dtype=np.float32)

# Two DIFFERENT per-segment permutations (the confidence-sorted case).
_BN_PERMUTED_IDS = np.array(
    [
        [3, 0, 6, 1, 5, 2, 4],
        [6, 5, 4, 3, 2, 1, 0],
    ],
    dtype=np.uint16,
)
# probs[seg, j] carries the score OF species _BN_PERMUTED_IDS[seg, j].
_BN_PERMUTED_PROBS = np.take_along_axis(
    _BN_DENSE_PROBS, _BN_PERMUTED_IDS.astype(np.int64), axis=1
)

_BN_IDENTITY_IDS = np.broadcast_to(
    np.arange(_BN_N_SPECIES, dtype=np.uint16), _BN_DENSE_PROBS.shape
)


def test_dense_scores_identity_ids_pass_through():
    out = _dense_birdnet_scores(_BN_DENSE_PROBS, _BN_IDENTITY_IDS, _BN_N_SPECIES)
    np.testing.assert_array_equal(out, _BN_DENSE_PROBS)


def test_dense_scores_scatter_by_permuted_ids():
    """When ids are NOT the identity, every score must land at its own class
    index — reading the permuted matrix positionally gives different numbers
    (the fixture guarantees it), so a skipped scatter cannot hide."""
    out = _dense_birdnet_scores(_BN_PERMUTED_PROBS, _BN_PERMUTED_IDS, _BN_N_SPECIES)
    np.testing.assert_array_equal(out, _BN_DENSE_PROBS)
    assert not np.array_equal(_BN_PERMUTED_PROBS, _BN_DENSE_PROBS)


def test_dense_scores_refuse_truncated_k_below_n_species():
    """The silently-wrong-cache case: top_k left at the package default (5)
    yields k < n_species, and pooling it would write scores that are wrong
    for every class outside the top k. REFUSED, never pooled."""
    with pytest.raises(ValueError, match="top_k"):
        _dense_birdnet_scores(
            _BN_PERMUTED_PROBS[:, :5], _BN_PERMUTED_IDS[:, :5], _BN_N_SPECIES
        )


def test_dense_scores_refuse_non_permutation_ids():
    """Duplicate ids would silently drop a score in the scatter."""
    bad_ids = _BN_PERMUTED_IDS.copy()
    bad_ids[0, 1] = bad_ids[0, 0]  # duplicate -> not a permutation
    with pytest.raises(ValueError, match="permutation"):
        _dense_birdnet_scores(_BN_PERMUTED_PROBS, bad_ids, _BN_N_SPECIES)


def test_dense_scores_refuse_zero_segments_and_shape_mismatch():
    with pytest.raises(ValueError, match="zero prediction segments"):
        _dense_birdnet_scores(
            np.zeros((0, _BN_N_SPECIES), np.float32),
            np.zeros((0, _BN_N_SPECIES), np.uint16),
            _BN_N_SPECIES,
        )
    with pytest.raises(ValueError, match="species_probs/species_ids"):
        _dense_birdnet_scores(
            _BN_DENSE_PROBS, _BN_IDENTITY_IDS[:, :5], _BN_N_SPECIES
        )


# ---------------------------------------------------------------------------
# encoder2 wrappers (mocked models)
# ---------------------------------------------------------------------------


def test_perch_encoder_with_logits_returns_both_pooled():
    def fake_model(batch):
        assert batch.shape == (1, 10 * PERCH_SR)
        return {
            "embedding": np.stack(
                [np.full(1536, 1.0), np.full(1536, 3.0)]
            ).astype(np.float32),
            "logits": _PERCH_LOGITS_2D,  # (3 windows, 3 classes) mock
        }

    encoder2 = make_perch_encoder_with_logits(model=fake_model)
    emb, logits = encoder2(np.zeros(10 * PERCH_SR, dtype=np.float32), PERCH_SR)
    assert emb.shape == (1536,)
    np.testing.assert_allclose(emb, np.full(1536, 2.0, dtype=np.float32))
    np.testing.assert_allclose(logits, np.array([3.0, 5.0, 0.0], dtype=np.float32))
    assert logits.dtype == np.float32


def test_perch_encoder_with_logits_rejects_non_32khz():
    encoder2 = make_perch_encoder_with_logits(
        model=lambda b: {"embedding": np.zeros(4), "logits": np.zeros((1, 3))}
    )
    with pytest.raises(ValueError, match="32"):
        encoder2(np.zeros(1000, dtype=np.float32), 44100)


class _FakeSession:
    """Mock of a birdnet session: run_arrays + end, result from a callable."""

    def __init__(self, result_fn):
        self._result_fn = result_fn
        self.run_inputs: list = []
        self.ended = 0
        self.entered = 0

    def __enter__(self):
        # The REAL birdnet session starts its pipeline processes here.
        # A run_arrays before __enter__ raises exactly like birdnet 1.1.1:
        # this is the bug that failed all 12000 pilot recordings.
        self.entered += 1
        return self

    def __exit__(self, *exc):
        self.end()
        return False

    def run_arrays(self, inputs):
        if self.entered == 0:
            raise RuntimeError("Pipeline is not initialized.")
        self.run_inputs.append(inputs)
        return self._result_fn(inputs)

    def end(self):
        self.ended += 1


class _MeasuredBirdnetModel:
    """Mock pinned to birdnet 1.1.1's MEASURED session API (fleet,
    2026-09-30): `encode_session`/`predict_session` take KEYWORD-ONLY knobs
    and return sessions whose `run_arrays` carries the SAME result shapes
    as the retired one-shots (verified live). The predict session HONOURS
    the top_k captured at session open — a wrapper that drops top_k=None
    gets the default-5 confidence-sorted TRUNCATED result, exactly like the
    real package, and the k < n_species guard must refuse it. The old
    one-shot encode_arrays/predict_arrays methods are deliberately ABSENT:
    they cost a measured 12.14 s of process spawn per call and the wrapper
    must not fall back to them.
    """

    def __init__(self, ids=None, probs=None):
        # ids/probs default to the dense identity case.
        self._ids = _BN_IDENTITY_IDS if ids is None else ids
        self._probs = _BN_DENSE_PROBS if probs is None else probs
        self.calls: dict = {}
        self.encode_sessions: list[_FakeSession] = []
        self.predict_sessions: list[_FakeSession] = []

    def encode_session(self, *, n_workers, batch_size, device):
        def result_fn(inputs):
            emb = np.array([[[1.0, 3.0], [5.0, 7.0]]], dtype=np.float32)
            mask = np.zeros((1, 2, 2), dtype=bool)
            return SimpleNamespace(embeddings=emb, embeddings_masked=mask)

        session = _FakeSession(result_fn)
        self.encode_sessions.append(session)
        return session

    def _predict_result(self, inputs, top_k):
        probs, ids = self._probs, self._ids
        if top_k is not None:
            order = np.argsort(-probs, axis=1)[:, :top_k]
            probs = np.take_along_axis(probs, order, axis=1)
            ids = np.take_along_axis(ids.astype(np.int64), order, axis=1)
        return SimpleNamespace(
            species_probs=np.asarray(probs, dtype=np.float32)[None],
            species_ids=np.asarray(ids, dtype=np.uint16)[None],
            species_list=[f"sp{i}" for i in range(_BN_N_SPECIES)],
            n_species=_BN_N_SPECIES,
        )

    def predict_session(
        self,
        *,
        top_k=5,
        n_workers=None,
        batch_size=1,
        device="CPU",
        apply_sigmoid=True,
        sigmoid_sensitivity=1.0,
        default_confidence_threshold=0.1,
        apply_softmax=False,
    ):
        self.calls.update(
            top_k=top_k,
            n_workers=n_workers,
            batch_size=batch_size,
            device=device,
            apply_sigmoid=apply_sigmoid,
            default_confidence_threshold=default_confidence_threshold,
        )

        def result_fn(inputs):
            self.calls["sr"] = inputs[0][1]
            return self._predict_result(inputs, top_k)

        session = _FakeSession(result_fn)
        self.predict_sessions.append(session)
        return session


def test_birdnet_encoder_with_logits_dense_identity_ids():
    """The MEASURED happy path: a top_k=None session yields the full dense
    matrix with identity ids; recording-level scores are the per-class MAX.
    A wrapper that leaves top_k at the default 5 gets a truncated result
    from this honouring mock and the guard fails the test."""
    model = _MeasuredBirdnetModel()
    encoder2 = make_birdnet_encoder_with_logits(model=model)
    emb, scores = encoder2(np.ones(32000, dtype=np.float32), 32000)
    np.testing.assert_allclose(emb, np.array([3.0, 5.0], dtype=np.float32))
    np.testing.assert_allclose(scores, _BN_DENSE_MAX)
    assert scores.dtype == np.float32
    # The measured session-open contract, knob by knob:
    assert model.calls["top_k"] is None  # full 6522-wide vector, no truncation
    assert model.calls["apply_sigmoid"] is True  # probabilities, like the tooling
    assert model.calls["default_confidence_threshold"] is None  # no zeroing
    assert model.calls["n_workers"] == 1  # Slurm: never one worker per core
    assert model.calls["batch_size"] == 1  # embed.py feeds one recording/call
    assert model.calls["device"] == "GPU"
    assert model.calls["sr"] == 32000  # OUR sr; the package resamples internally
    # Max, not mean — BirdSet's convention; the fixture makes them differ.
    assert not np.allclose(scores, _BN_DENSE_PROBS.mean(axis=0))


def test_birdnet_encoder_with_logits_opens_each_session_ONCE():
    """The per-call spawn bug this task kills (measured 12.14 s/call — 81 h
    projected for the HSN pilot): across TWO encoder calls, exactly ONE
    encode session and ONE predict session are opened, lazily (nothing
    before the first call)."""
    model = _MeasuredBirdnetModel()
    encoder2 = make_birdnet_encoder_with_logits(model=model)
    assert model.encode_sessions == [] and model.predict_sessions == []
    encoder2(np.ones(32000, dtype=np.float32), 32000)
    encoder2(np.ones(32000, dtype=np.float32), 32000)
    assert len(model.encode_sessions) == 1
    assert len(model.predict_sessions) == 1
    assert len(model.encode_sessions[0].run_inputs) == 2
    assert len(model.predict_sessions[0].run_inputs) == 2


def test_birdnet_encoder_with_logits_close_ends_BOTH_sessions():
    """close() must end BOTH the encode and the predict session — ending
    only one leaks the other's worker processes for the life of the Slurm
    task. Idempotent: a second close must not double-end."""
    model = _MeasuredBirdnetModel()
    encoder2 = make_birdnet_encoder_with_logits(model=model)
    encoder2(np.ones(32000, dtype=np.float32), 32000)
    encoder2.close()
    assert model.encode_sessions[0].ended == 1
    assert model.predict_sessions[0].ended == 1
    encoder2.close()
    assert model.encode_sessions[0].ended == 1
    assert model.predict_sessions[0].ended == 1


def test_birdnet_encoder_with_logits_retries_predict_once_then_raises():
    """Retry-once on the predict session too: a transient failure re-opens
    the session and retries the recording; persistent failure raises after
    the SECOND attempt (the mock would succeed on the third, so a
    retry-forever mutation returns instead of raising and fails here)."""
    attempts = []

    class FlakyPredictModel(_MeasuredBirdnetModel):
        def _predict_result(self, inputs, top_k):
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("worker died")
            return super()._predict_result(inputs, top_k)

    model = FlakyPredictModel()
    encoder2 = make_birdnet_encoder_with_logits(model=model, device="CPU")
    _, scores = encoder2(np.ones(32000, dtype=np.float32), 32000)
    np.testing.assert_allclose(scores, _BN_DENSE_MAX)
    assert len(model.predict_sessions) == 2  # re-opened exactly once
    assert model.predict_sessions[0].ended == 1

    attempts2 = []

    class BrokenPredictModel(_MeasuredBirdnetModel):
        def _predict_result(self, inputs, top_k):
            attempts2.append(1)
            if len(attempts2) <= 3:
                raise RuntimeError("systematically broken")
            return super()._predict_result(inputs, top_k)

    encoder2b = make_birdnet_encoder_with_logits(
        model=BrokenPredictModel(), device="CPU"
    )
    with pytest.raises(RuntimeError, match="systematically broken"):
        encoder2b(np.ones(32000, dtype=np.float32), 32000)
    assert len(attempts2) == 2  # original + ONE retry, never a third


def test_birdnet_encoder_with_logits_scatters_permuted_ids():
    """If species_ids come back permuted (confidence-sorted), scores must be
    scattered to their class indices before pooling; trusting column order
    gives different numbers (fixture-guaranteed) and fails here."""
    model = _MeasuredBirdnetModel(ids=_BN_PERMUTED_IDS, probs=_BN_PERMUTED_PROBS)
    encoder2 = make_birdnet_encoder_with_logits(model=model)
    _, scores = encoder2(np.ones(32000, dtype=np.float32), 32000)
    np.testing.assert_allclose(scores, _BN_DENSE_MAX)
    assert not np.allclose(scores, _BN_PERMUTED_PROBS.max(axis=0))


def test_birdnet_encoder_refuses_a_stuck_truncated_result():
    """A model whose predict session IGNORES top_k and serves the default-5
    truncation no matter what (k=5 < n_species=7) must be refused — pooling
    it would write the silently-wrong cache this guard exists for."""

    class StuckTopKModel(_MeasuredBirdnetModel):
        def predict_session(self, **kwargs):
            kwargs["top_k"] = 5  # the package default, immovably
            return super().predict_session(**kwargs)

    encoder2 = make_birdnet_encoder_with_logits(model=StuckTopKModel())
    with pytest.raises(ValueError, match="top_k"):
        encoder2(np.ones(32000, dtype=np.float32), 32000)


def test_birdnet_with_logits_without_predict_api_raises_naming_it():
    """A birdnet build without predict_session must fail NAMING the API it
    needs — never guess a call signature."""

    class EncodeOnlyModel:
        def encode_session(self, *, n_workers, batch_size, device):
            def result_fn(inputs):
                return SimpleNamespace(
                    embeddings=np.ones((1, 1, 4), dtype=np.float32),
                    embeddings_masked=np.zeros((1, 1, 4), dtype=bool),
                )

            return _FakeSession(result_fn)

    encoder2 = make_birdnet_encoder_with_logits(model=EncodeOnlyModel())
    with pytest.raises(NotImplementedError, match="predict_session"):
        encoder2(np.zeros(32000, dtype=np.float32), 32000)


def test_birdnet_with_logits_result_without_species_probs_raises():
    """The OLD mock-pinned `.predictions` contract does not exist on the
    real package; a result carrying it (and not .species_probs) must be
    refused loudly, not silently adapted."""

    class OldHypothesisModel(_MeasuredBirdnetModel):
        def _predict_result(self, inputs, top_k):
            return SimpleNamespace(predictions=np.ones((1, 1, 3)))

    encoder2 = make_birdnet_encoder_with_logits(model=OldHypothesisModel())
    with pytest.raises(NotImplementedError, match="species_probs"):
        encoder2(np.zeros(32000, dtype=np.float32), 32000)


def test_birdnet_with_logits_surprise_shapes_raise():
    """A missing batch axis, a batch of 2, or ids/probs disagreeing must be
    refused, never squeezed blind."""

    class BadShapeModel(_MeasuredBirdnetModel):
        def __init__(self, probs_shape, ids_shape):
            super().__init__()
            self.probs_shape = probs_shape
            self.ids_shape = ids_shape

        def _predict_result(self, inputs, top_k):
            return SimpleNamespace(
                species_probs=np.ones(self.probs_shape, dtype=np.float32),
                species_ids=np.zeros(self.ids_shape, dtype=np.uint16),
                n_species=_BN_N_SPECIES,
            )

    for probs_shape, ids_shape in (
        ((3, 4), (3, 4)),  # no batch axis
        ((2, 3, 4), (2, 3, 4)),  # two recordings
        ((1, 3, 4), (1, 3, 5)),  # ids/probs disagree
    ):
        encoder2 = make_birdnet_encoder_with_logits(
            model=BadShapeModel(probs_shape, ids_shape)
        )
        with pytest.raises(ValueError, match="species_probs/species_ids"):
            encoder2(np.zeros(32000, dtype=np.float32), 32000)


def test_melsummary_with_logits_returns_none_logits():
    encoder2 = load_encoder_with_logits("melsummary")
    emb, logits = encoder2(np.ones(32000, dtype=np.float32), 32000)
    assert emb.shape == (64,)
    assert logits is None


def test_load_encoder_with_logits_unknown_name_raises():
    with pytest.raises(ValueError, match="unknown encoder"):
        load_encoder_with_logits("clap")


def test_load_encoder_with_logits_import_errors_stay_loud(monkeypatch):
    monkeypatch.setitem(sys.modules, "perch_hoplite", None)
    monkeypatch.setitem(sys.modules, "perch_hoplite.zoo", None)
    with pytest.raises(ImportError, match="pip install perch-hoplite"):
        load_encoder_with_logits("perch")
    monkeypatch.setitem(sys.modules, "birdnet", None)
    with pytest.raises(ImportError, match="pip install birdnet"):
        load_encoder_with_logits("birdnet")


# ---------------------------------------------------------------------------
# embed_shard with_logits: schema variants through a REAL parquet roundtrip
# ---------------------------------------------------------------------------

_LOGITS_WIDTH = 200  # small stand-in for 14795; the LENGTH is still pinned


def _ogg_bytes(seconds=1.0, sr=32000):
    buf = io.BytesIO()
    x = np.sin(2 * np.pi * 440 * np.arange(int(sr * seconds)) / sr) * 0.1
    sf.write(buf, x, sr, format="OGG", subtype="VORBIS")
    return buf.getvalue()


def _shard(tmp_path, members):
    p = tmp_path / "shard_0001.tar.gz"
    with tarfile.open(p, "w:gz") as tf:
        for name, payload in members:
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            tf.addfile(info, io.BytesIO(payload))
    return p


def _manifest(paths):
    return pa.table(
        {
            "filepath": pa.array(paths, pa.string()),
            "cell": pa.array(["c"] * len(paths), pa.string()),
            "week": pa.array([1] * len(paths), pa.int8()),
            "ebird_code": pa.array(["gretit"] * len(paths), pa.string()),
            "split": pa.array(["train"] * len(paths), pa.string()),
        }
    )


def _encoder2(n_dims=8, logits_width=_LOGITS_WIDTH):
    """(embedding, logits) stand-in; logits values depend on the input so a
    scrambled write is detectable."""

    def run(samples, sr):
        emb = np.full(n_dims, float(len(samples)), dtype=np.float32)
        logits = np.arange(logits_width, dtype=np.float32) + float(len(samples))
        return emb, logits

    return run


def test_with_logits_roundtrips_the_logits_schema(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    res = embed_shard(
        p,
        _manifest(["a/XC1.ogg", "a/XC2.ogg"]),
        _encoder2(),
        out_dir=out,
        with_logits=True,
    )
    assert res.embedded == 2
    assert res.schema_variant == "embedding+logits"
    t = pq.read_table(out / "shard_0001.parquet")
    assert "logits" in t.schema.names
    assert t.schema.field("logits").type == pa.list_(pa.float32())
    assert t.schema.metadata[SCHEMA_VARIANT_KEY] == b"embedding+logits"
    # M5's target: the FULL logits vector lands in the table — length pinned.
    for row in t.to_pylist():
        assert len(row["logits"]) == _LOGITS_WIDTH
        assert len(row["embedding"]) == 8
    # Values, not just lengths: a truncation or reorder changes these.
    row0 = t.to_pylist()[0]
    n_samples = row0["embedding"][0]
    np.testing.assert_allclose(
        row0["logits"], np.arange(_LOGITS_WIDTH, dtype=np.float32) + n_samples
    )


def test_perch_raw_uncalibrated_flag_lands_in_the_parquet_metadata(tmp_path):
    """The falsified reconnaissance claim ('hoplite applies the perch_v2
    calibration') means the cached logits are RAW; that fact must travel in
    the parquet schema metadata so no consumer can assume calibrated
    values. Built with the REAL perch wrapper (mocked model) through a REAL
    parquet roundtrip."""

    def fake_model(batch):
        return {
            "embedding": np.zeros(8, dtype=np.float32),
            "logits": _PERCH_LOGITS_2D,
        }

    encoder2 = make_perch_encoder_with_logits(model=fake_model)
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes(sr=PERCH_SR))])
    out = tmp_path / "emb"
    res = embed_shard(
        p, _manifest(["a/XC1.ogg"]), encoder2, out_dir=out, with_logits=True
    )
    assert res.schema_variant == "embedding+logits"
    t = pq.read_table(out / "shard_0001.parquet")
    assert t.schema.metadata[b"perch_logits_are_raw_uncalibrated"] == b"true"
    # And the logits in the file are the RAW pooled values — no affine map.
    np.testing.assert_allclose(
        t.to_pylist()[0]["logits"], np.array([3.0, 5.0, 0.0], dtype=np.float32)
    )


def test_non_perch_encoders_write_no_raw_uncalibrated_flag(tmp_path):
    """The flag is a perch-specific measured fact, not boilerplate: a plain
    encoder's parquet must NOT carry it."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder2(), out_dir=out, with_logits=True)
    t = pq.read_table(out / "shard_0001.parquet")
    assert b"perch_logits_are_raw_uncalibrated" not in (t.schema.metadata or {})


def test_without_logits_stays_on_the_default_schema(tmp_path):
    """with_logits=False is the DEFAULT contract: unchanged schema, variant
    declared as plain 'embedding'."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"

    def encoder(samples, sr):
        return np.zeros(8, dtype=np.float32)

    res = embed_shard(p, _manifest(["a/XC1.ogg"]), encoder, out_dir=out)
    assert res.schema_variant == "embedding"
    t = pq.read_table(out / "shard_0001.parquet")
    assert "logits" not in t.schema.names
    assert t.schema.names == EMBEDDING_SCHEMA.names
    assert t.schema.metadata[SCHEMA_VARIANT_KEY] == b"embedding"


def test_melsummary_style_all_none_logits_writes_the_default_schema(tmp_path):
    """An encoder with no classifier head yields logits=None for every file;
    the shard must fall back to the default schema, not write a column of
    nulls Phase B would have to special-case."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", _ogg_bytes())])
    out = tmp_path / "emb"

    def encoder2(samples, sr):
        return np.zeros(8, dtype=np.float32), None

    res = embed_shard(
        p,
        _manifest(["a/XC1.ogg", "a/XC2.ogg"]),
        encoder2,
        out_dir=out,
        with_logits=True,
    )
    assert res.embedded == 2
    assert res.schema_variant == "embedding"
    t = pq.read_table(out / "shard_0001.parquet")
    assert "logits" not in t.schema.names
    assert t.schema.metadata[SCHEMA_VARIANT_KEY] == b"embedding"


def test_all_failed_shard_under_with_logits_writes_a_readable_logits_schema(
    tmp_path,
):
    """Zero embedded rows must still leave a parquet with the REQUESTED
    variant's full schema — Phase B reads every parquet in a logits out_dir
    with one expectation."""
    p = _shard(tmp_path, [("a/XC1.ogg", b"bad"), ("a/XC2.ogg", b"also bad")])
    out = tmp_path / "emb"
    res = embed_shard(
        p,
        _manifest(["a/XC1.ogg", "a/XC2.ogg"]),
        _encoder2(),
        out_dir=out,
        with_logits=True,
    )
    assert res.embedded == 0
    assert len(res.failed) == 2
    t = pq.read_table(out / "shard_0001.parquet")
    assert t.num_rows == 0
    assert t.schema.names == LOGITS_SCHEMA.names
    assert t.schema.field("logits").type == pa.list_(pa.float32())
    assert t.schema.metadata[SCHEMA_VARIANT_KEY] == b"embedding+logits"


def test_inconsistent_logits_lengths_within_a_shard_raise(tmp_path):
    """A wrong-length posterior misaligns every class index downstream; a
    within-shard width change is a systematic encoder bug, raised loudly,
    never recorded as a per-file failure."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", _ogg_bytes())])
    widths = iter([200, 100])

    def drifting_encoder(samples, sr):
        return np.zeros(8, dtype=np.float32), np.zeros(next(widths), dtype=np.float32)

    with pytest.raises(ValueError, match="logits"):
        embed_shard(
            p,
            _manifest(["a/XC1.ogg", "a/XC2.ogg"]),
            drifting_encoder,
            out_dir=tmp_path / "emb",
            with_logits=True,
        )


def test_mixed_some_none_some_logits_raises(tmp_path):
    """Half a posterior is worse than none: an encoder that yields logits
    for some files and None for others is inconsistent, not a variant."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", _ogg_bytes())])
    flags = iter([True, False])

    def flaky_encoder(samples, sr):
        emb = np.zeros(8, dtype=np.float32)
        return (emb, np.zeros(10, dtype=np.float32)) if next(flags) else (emb, None)

    with pytest.raises(ValueError, match="inconsistent"):
        embed_shard(
            p,
            _manifest(["a/XC1.ogg", "a/XC2.ogg"]),
            flaky_encoder,
            out_dir=tmp_path / "emb",
            with_logits=True,
        )


# ---------------------------------------------------------------------------
# embed_cli --with-logits threading
# ---------------------------------------------------------------------------


def _wav_bytes():
    sr = 16000
    t = np.linspace(0, 0.1, 1600, endpoint=False)
    buf = io.BytesIO()
    sf.write(buf, np.sin(2 * np.pi * 440 * t).astype("float32"), sr, format="WAV")
    return buf.getvalue()


def _cli_fixture(tmp_path):
    shard = tmp_path / "XCM_shard_0001.tar.gz"
    payload = _wav_bytes()
    with tarfile.open(shard, "w:gz") as tf:
        info = tarfile.TarInfo("a.wav")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    manifest = tmp_path / "manifest.parquet"
    pq.write_table(_manifest(["a.wav"]), manifest)
    return shard, manifest


def test_embed_cli_threads_with_logits_to_embed_shard_and_loader(tmp_path):
    from birdology.phase_a.embed import ShardResult
    from birdology.phase_a.embed_cli import main as embed_main

    shard, manifest = _cli_fixture(tmp_path)
    seen = {}

    def fake_embed(shard_path, manifest_table, encoder, *, out_dir, with_logits=False):
        seen["with_logits"] = with_logits
        seen["encoder"] = encoder
        return ShardResult(shard=shard_path.name, embedded=1, expected=1)

    sentinel2 = object()
    with patch("birdology.phase_a.embed_cli.embed_shard", side_effect=fake_embed), \
         patch(
             "birdology.phase_a.embed_cli.load_encoder_with_logits",
             return_value=sentinel2,
         ) as loader2:
        rc = embed_main(
            ["--shard", str(shard), "--manifest", str(manifest),
             "--out", str(tmp_path / "out"), "--encoder", "melsummary",
             "--with-logits"]
        )
    assert rc == 0
    assert seen["with_logits"] is True
    assert seen["encoder"] is sentinel2
    loader2.assert_called_once_with("melsummary")


def test_embed_cli_without_the_flag_uses_the_plain_encoder(tmp_path):
    from birdology.phase_a.embed import ShardResult
    from birdology.phase_a.embed_cli import main as embed_main

    shard, manifest = _cli_fixture(tmp_path)
    seen = {}

    def fake_embed(shard_path, manifest_table, encoder, *, out_dir, with_logits=False):
        seen["with_logits"] = with_logits
        return ShardResult(shard=shard_path.name, embedded=1, expected=1)

    with patch("birdology.phase_a.embed_cli.embed_shard", side_effect=fake_embed), \
         patch(
             "birdology.phase_a.embed_cli.load_encoder_with_logits"
         ) as loader2:
        rc = embed_main(
            ["--shard", str(shard), "--manifest", str(manifest),
             "--out", str(tmp_path / "out"), "--encoder", "melsummary"]
        )
    assert rc == 0
    assert seen["with_logits"] is False
    loader2.assert_not_called()


def test_embed_cli_logs_which_schema_variant_it_wrote(tmp_path, caplog):
    from birdology.phase_a.embed_cli import main as embed_main

    shard, manifest = _cli_fixture(tmp_path)
    with caplog.at_level(logging.INFO, logger="phase_a.embed"):
        rc = embed_main(
            ["--shard", str(shard), "--manifest", str(manifest),
             "--out", str(tmp_path / "out"), "--encoder", "melsummary",
             "--with-logits"]
        )
    assert rc == 0
    # melsummary has no head: the honest variant is plain 'embedding',
    # and the operator can see it in the log.
    assert any("schema=embedding" in r.getMessage() for r in caplog.records)


def test_logits_tests_import_no_tensorflow_or_birdnet():
    assert "tensorflow" not in sys.modules
    assert "birdnet" not in sys.modules
