"""Shard-level embedding extraction. No GPU, no network."""
import io
import sys
import tarfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import soundfile as sf

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.embed import (
    ShardResult,
    embed_shard,
    iter_shard_audio,
    shard_is_complete,
)


def _ogg_bytes(seconds=1.0, sr=32000):
    buf = io.BytesIO()
    x = np.sin(2 * np.pi * 440 * np.arange(int(sr * seconds)) / sr) * 0.1
    sf.write(buf, x, sr, format="OGG", subtype="VORBIS")
    return buf.getvalue()


def _stereo_ogg_bytes(seconds=1.0, sr=32000):
    buf = io.BytesIO()
    t = np.arange(int(sr * seconds)) / sr
    left = np.sin(2 * np.pi * 440 * t) * 0.1
    right = np.sin(2 * np.pi * 660 * t) * 0.1
    sf.write(buf, np.stack([left, right], axis=1), sr, format="OGG", subtype="VORBIS")
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
            "lat": pa.array([55.0] * len(paths), pa.float64()),
            "lon": pa.array([12.0] * len(paths), pa.float64()),
        }
    )


def _encoder(n_dims=8):
    """Stand-in for a frozen encoder: deterministic, no torch needed."""
    def run(samples, sr):
        return np.full(n_dims, float(len(samples)), dtype=np.float32)
    return run


def test_iter_shard_audio_reads_without_extracting(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    got = list(iter_shard_audio(p))
    assert len(got) == 1
    name, payload = got[0]
    assert name == "a/XC1.ogg"
    assert sf.info(io.BytesIO(payload)).samplerate == 32000


def test_iter_shard_audio_skips_non_audio_members(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/notes.txt", b"hi")])
    assert [n for n, _ in iter_shard_audio(p)] == ["a/XC1.ogg"]


def test_embed_shard_writes_one_row_per_manifest_file(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    res = embed_shard(p, _manifest(["a/XC1.ogg", "a/XC2.ogg"]), _encoder(), out_dir=out)
    assert isinstance(res, ShardResult)
    assert res.embedded == 2
    assert not res.failed
    written = pq.read_table(out / "shard_0001.parquet")
    assert written.num_rows == 2
    assert "embedding" in written.schema.names


def test_a_file_absent_from_the_manifest_is_not_embedded(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC9.ogg", _ogg_bytes())])
    res = embed_shard(
        p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=tmp_path / "emb"
    )
    assert res.embedded == 1


def test_ONE_BAD_FILE_does_not_abandon_the_shard(tmp_path):
    """Review Focus 4: a truncated member must not lose the other files."""
    p = _shard(
        tmp_path,
        [
            ("a/XC1.ogg", _ogg_bytes()),
            ("a/XC2.ogg", b"this is not an ogg file"),
            ("a/XC3.ogg", _ogg_bytes()),
        ],
    )
    res = embed_shard(
        p, _manifest(["a/XC1.ogg", "a/XC2.ogg", "a/XC3.ogg"]),
        _encoder(), out_dir=tmp_path / "emb",
    )
    assert res.embedded == 2
    assert "a/XC2.ogg" in res.failed


def test_failures_are_RECORDED_PER_FILE_not_only_logged(tmp_path):
    """182 array tasks make a log line invisible."""
    p = _shard(tmp_path, [("a/XC1.ogg", b"bad")])
    out = tmp_path / "emb"
    res = embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    assert res.failed["a/XC1.ogg"]
    assert (out / "shard_0001.failures.json").exists()


def test_failures_file_is_written_even_when_nothing_failed(tmp_path):
    """An absent failures file and an empty one are different statements."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    res = embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    assert not res.failed
    assert (out / "shard_0001.failures.json").exists()


def test_shard_is_complete_requires_a_MATCHING_ROW_COUNT(tmp_path):
    """Review Focus 5: a preempted task must not look finished."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    assert not shard_is_complete(p, out)

    # A partial write: one row where the shard holds two audio members.
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    assert not shard_is_complete(p, out)

    embed_shard(p, _manifest(["a/XC1.ogg", "a/XC2.ogg"]), _encoder(), out_dir=out)
    assert shard_is_complete(p, out)


def test_embedding_width_is_preserved(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(n_dims=16), out_dir=out)
    t = pq.read_table(out / "shard_0001.parquet")
    assert len(t.column("embedding")[0].as_py()) == 16


def test_context_columns_travel_with_the_embedding(tmp_path):
    """Phase B joins on these; an embedding with no context is useless."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    row = pq.read_table(out / "shard_0001.parquet").to_pylist()[0]
    for col in ("filepath", "cell", "week", "ebird_code", "split"):
        assert col in row


def test_an_ALL_FAILED_shard_still_writes_a_readable_schema(tmp_path):
    """pa.Table.from_pylist([]) is a ZERO-COLUMN table; Phase B could not
    read the resulting parquet. The writer must pin an explicit schema."""
    p = _shard(tmp_path, [("a/XC1.ogg", b"bad"), ("a/XC2.ogg", b"also bad")])
    out = tmp_path / "emb"
    res = embed_shard(p, _manifest(["a/XC1.ogg", "a/XC2.ogg"]), _encoder(), out_dir=out)
    assert res.embedded == 0
    assert len(res.failed) == 2
    table_path = out / "shard_0001.parquet"
    assert table_path.exists()
    t = pq.read_table(table_path)
    assert t.num_rows == 0
    for col in ("filepath", "cell", "week", "ebird_code", "split", "embedding"):
        assert col in t.schema.names
    assert t.schema.field("week").type == pa.int8()
    assert t.schema.field("embedding").type == pa.list_(pa.float32())


def test_stereo_audio_is_mixed_down_to_mono_before_the_encoder(tmp_path):
    """The encoder contract is 1-D float32; stereo must be mixed down."""
    p = _shard(tmp_path, [("a/XC1.ogg", _stereo_ogg_bytes())])
    seen = []

    def spy_encoder(samples, sr):
        seen.append(samples)
        return np.zeros(4, dtype=np.float32)

    res = embed_shard(p, _manifest(["a/XC1.ogg"]), spy_encoder, out_dir=tmp_path / "emb")
    assert res.embedded == 1
    assert len(seen) == 1
    assert seen[0].ndim == 1
