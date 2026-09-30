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


def test_birdnet_encoder_with_logits_max_pools_predictions():
    calls = {}

    class FakeModel:
        def encode_arrays(self, arrays, n_workers, device):
            emb = np.array([[[1.0, 3.0], [5.0, 7.0]]], dtype=np.float32)
            mask = np.zeros((1, 2, 2), dtype=bool)
            return SimpleNamespace(embeddings=emb, embeddings_masked=mask)

        def predict_arrays(self, arrays, n_workers, device):
            calls["n_workers"] = n_workers
            calls["device"] = device
            calls["sr"] = arrays[0][1]
            return SimpleNamespace(predictions=_BIRDNET_PREDICTIONS[None, :, :])

    encoder2 = make_birdnet_encoder_with_logits(model=FakeModel())
    emb, logits = encoder2(np.ones(32000, dtype=np.float32), 32000)
    np.testing.assert_allclose(emb, np.array([3.0, 5.0], dtype=np.float32))
    np.testing.assert_allclose(logits, np.array([0.9, 0.5, 0.8], dtype=np.float32))
    assert calls["n_workers"] == 1  # Slurm: never one worker per core
    assert calls["device"] == "GPU"
    assert calls["sr"] == 32000  # OUR sr; the package resamples internally


def test_birdnet_with_logits_without_predict_api_raises_naming_it():
    """birdnet 1.1.1's predict API was NOT measurable locally; if the real
    package differs, the wrapper must fail naming what it needs — the Task
    6.4 pilot is the measurement — never guess a call signature."""

    class EncodeOnlyModel:
        def encode_arrays(self, arrays, n_workers, device):
            return SimpleNamespace(
                embeddings=np.ones((1, 1, 4), dtype=np.float32),
                embeddings_masked=np.zeros((1, 1, 4), dtype=bool),
            )

    encoder2 = make_birdnet_encoder_with_logits(model=EncodeOnlyModel())
    with pytest.raises(NotImplementedError, match="predict_arrays"):
        encoder2(np.zeros(32000, dtype=np.float32), 32000)


def test_birdnet_with_logits_result_without_predictions_raises():
    class WeirdModel:
        def encode_arrays(self, arrays, n_workers, device):
            return SimpleNamespace(
                embeddings=np.ones((1, 1, 4), dtype=np.float32),
                embeddings_masked=np.zeros((1, 1, 4), dtype=bool),
            )

        def predict_arrays(self, arrays, n_workers, device):
            return SimpleNamespace(scores=np.ones((1, 1, 3)))

    encoder2 = make_birdnet_encoder_with_logits(model=WeirdModel())
    with pytest.raises(NotImplementedError, match="predictions"):
        encoder2(np.zeros(32000, dtype=np.float32), 32000)


def test_birdnet_with_logits_surprise_prediction_shape_raises():
    """A (n_segments, n_classes) return without the batch axis, or a batch
    of 2, must be refused, never squeezed blind."""

    class BadShapeModel:
        def __init__(self, shape):
            self.shape = shape

        def encode_arrays(self, arrays, n_workers, device):
            return SimpleNamespace(
                embeddings=np.ones((1, 1, 4), dtype=np.float32),
                embeddings_masked=np.zeros((1, 1, 4), dtype=bool),
            )

        def predict_arrays(self, arrays, n_workers, device):
            return SimpleNamespace(predictions=np.ones(self.shape, dtype=np.float32))

    for shape in ((3, 4), (2, 3, 4)):
        encoder2 = make_birdnet_encoder_with_logits(model=BadShapeModel(shape))
        with pytest.raises(ValueError, match="unexpected birdnet predictions shape"):
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
