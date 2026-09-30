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
    EMBEDDING_SCHEMA,
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
    full = _manifest(["a/XC1.ogg", "a/XC2.ogg"])
    assert not shard_is_complete(p, full, out)

    # A partial write: one row where the manifest expects two.
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    assert not shard_is_complete(p, full, out)

    embed_shard(p, full, _encoder(), out_dir=out)
    assert shard_is_complete(p, full, out)


def test_a_manifest_EXCLUDED_member_does_not_block_completion(tmp_path):
    """Task 3's manifest drops ~2% of recordings by design; a shard whose
    manifest-listed members are all done must be COMPLETE even though the
    tar holds members the manifest never asked for."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC9.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    m = _manifest(["a/XC1.ogg"])
    res = embed_shard(p, m, _encoder(), out_dir=out)
    assert res.embedded == 1
    assert res.expected == 1
    assert shard_is_complete(p, m, out)


def test_completion_is_SET_BASED_not_count_arithmetic(tmp_path):
    """F2, measured: a crash between the two writes leaves a parquet holding
    {XC1, XC2} and a STALE failures json holding {XC2} -- 3 accountings for
    2 names, with XC3 lost. Count arithmetic (2 + 1 >= 3) blesses it."""
    p = _shard(
        tmp_path,
        [
            ("a/XC1.ogg", _ogg_bytes()),
            ("a/XC2.ogg", _ogg_bytes()),
            ("a/XC3.ogg", _ogg_bytes()),
        ],
    )
    m = _manifest(["a/XC1.ogg", "a/XC2.ogg", "a/XC3.ogg"])
    out = tmp_path / "emb"
    out.mkdir()
    rows = [
        {
            "filepath": n,
            "cell": "c",
            "week": 1,
            "ebird_code": "gretit",
            "split": "train",
            "embedding": [0.0] * 8,
        }
        for n in ("a/XC1.ogg", "a/XC2.ogg")
    ]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=EMBEDDING_SCHEMA),
        out / "shard_0001.parquet",
    )
    (out / "shard_0001.failures.json").write_text('{"a/XC2.ogg": "stale"}')
    assert not shard_is_complete(p, m, out)


def test_a_CORRUPT_parquet_means_incomplete_not_complete_or_crash(tmp_path):
    """F5/M9: a torn parquet (preemption mid-write) must read as incomplete."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    m = _manifest(["a/XC1.ogg"])
    embed_shard(p, m, _encoder(), out_dir=out)
    assert shard_is_complete(p, m, out)
    (out / "shard_0001.parquet").write_bytes(b"\x00garbage not a parquet\x00")
    assert shard_is_complete(p, m, out) is False


def test_a_CORRUPT_failures_json_means_incomplete_not_complete_or_crash(tmp_path):
    """F5/M8: an unreadable failures sidecar makes the output pair
    untrustworthy, even when the parquet alone covers everything."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    m = _manifest(["a/XC1.ogg"])
    embed_shard(p, m, _encoder(), out_dir=out)
    assert shard_is_complete(p, m, out)
    (out / "shard_0001.failures.json").write_bytes(b"{not json![")
    assert shard_is_complete(p, m, out) is False


def test_ZERO_manifest_shard_overlap_raises_instead_of_blessing_no_op(tmp_path):
    """F3: zero overlap must be LOUD. (A bare `./` prefix difference is now
    RESOLVED by the basename fallback -- measured real in XCM -- so this
    fixture uses names that differ even by basename.)"""
    p = _shard(tmp_path, [("./a/XC7.ogg", _ogg_bytes()), ("./a/XC8.ogg", _ogg_bytes())])
    with pytest.raises(ValueError, match="ZERO overlap"):
        embed_shard(
            p, _manifest(["a/XC1.ogg", "a/XC2.ogg"]), _encoder(),
            out_dir=tmp_path / "emb",
        )
    # And no output was blessed.
    assert not (tmp_path / "emb" / "shard_0001.parquet").exists()


def test_a_TRUNCATED_ogg_that_decodes_to_zero_samples_is_a_failure(tmp_path):
    """F1, measured: a valid-header OGG cut at 60% returns 0 samples from
    sf.read WITHOUT raising; embedding it writes a garbage row."""
    whole = _ogg_bytes(seconds=2.0)
    truncated = whole[: int(len(whole) * 0.6)]
    # Precondition of the probe: the truncation is the silent kind.
    s, _ = sf.read(io.BytesIO(truncated), dtype="float32", always_2d=False)
    assert s.size == 0
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", truncated)])
    out = tmp_path / "emb"
    res = embed_shard(
        p, _manifest(["a/XC1.ogg", "a/XC2.ogg"]), _encoder(), out_dir=out
    )
    assert res.embedded == 1
    assert "a/XC2.ogg" in res.failed
    assert "0 samples" in res.failed["a/XC2.ogg"]
    written = pq.read_table(out / "shard_0001.parquet")
    assert written.column("filepath").to_pylist() == ["a/XC1.ogg"]


def test_a_crash_during_the_parquet_write_leaves_NO_torn_final_file(
    tmp_path, monkeypatch
):
    """F2: writes must be atomic. Simulate preemption mid-`write_table`:
    with tmp+os.replace the destination path never exists; a non-atomic
    write leaves a torn file at the final path that a reader half-trusts."""
    import birdology.phase_a.embed as embed_mod

    def torn_write(table, where, *args, **kwargs):
        Path(where).write_bytes(b"half a parquet")
        raise RuntimeError("preempted mid-write")

    monkeypatch.setattr(embed_mod.pq, "write_table", torn_write)
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    with pytest.raises(RuntimeError, match="preempted"):
        embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    assert not (out / "shard_0001.parquet").exists()
    # Failures json is written FIRST, so it survived the crash -- and the
    # missing parquet (the completion witness) makes the shard re-run.
    assert (out / "shard_0001.failures.json").exists()


def test_no_tmp_files_survive_a_successful_run(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    assert not list(out.glob("*.tmp"))


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


def test_a_BARE_BASENAME_shard_is_resolved_against_full_manifest_paths(tmp_path):
    """Measured on WEKA 2026-09-30: XCM shards 0127 and 0158-0182 store bare
    basenames (XC111167.ogg) while the manifest carries full paths
    (data/xeno-canto/.../XC111167.ogg). BirdSet's packaging is inconsistent
    ACROSS SHARDS OF THE SAME CONFIG; 26 real shards failed on it."""
    p = _shard(tmp_path, [("XC1.ogg", _ogg_bytes()), ("XC2.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    manifest = _manifest(["data/xc/parus_major/XC1.ogg", "data/xc/parus_major/XC2.ogg"])
    res = embed_shard(p, manifest, _encoder(), out_dir=out)
    assert res.embedded == 2
    # The parquet must store the CANONICAL manifest path -- Phase B's join key.
    written = pq.read_table(out / "shard_0001.parquet")
    assert sorted(written.column("filepath").to_pylist()) == [
        "data/xc/parus_major/XC1.ogg",
        "data/xc/parus_major/XC2.ogg",
    ]
    assert shard_is_complete(p, manifest, out)


def test_an_AMBIGUOUS_basename_is_left_unresolved_not_guessed(tmp_path):
    """Two manifest rows sharing one basename: a wrong join is worse than a
    dropped file, so the bare member matches NEITHER -- and with nothing
    resolvable in the whole shard, the zero-overlap guard fires loudly."""
    p = _shard(tmp_path, [("XC1.ogg", _ogg_bytes())])
    manifest = _manifest(["site_a/XC1.ogg", "site_b/XC1.ogg"])
    with pytest.raises(ValueError, match="ZERO overlap"):
        embed_shard(p, manifest, _encoder(), out_dir=tmp_path / "emb")

    # With a resolvable sibling present, the ambiguous member is dropped
    # SILENTLY-BY-DESIGN (not guessed, not fatal) and the rest proceeds.
    second = tmp_path / "second"
    second.mkdir()
    p2 = _shard(
        second,
        [("XC1.ogg", _ogg_bytes()), ("data/xc/parus_major/XC2.ogg", _ogg_bytes())],
    )
    manifest2 = _manifest(
        ["site_a/XC1.ogg", "site_b/XC1.ogg", "data/xc/parus_major/XC2.ogg"]
    )
    res = embed_shard(p2, manifest2, _encoder(), out_dir=tmp_path / "emb2")
    assert res.embedded == 1
    assert res.expected == 1


def test_a_FAILED_bare_member_is_recorded_under_the_canonical_path(tmp_path):
    p = _shard(tmp_path, [("XC1.ogg", b"not audio")])
    out = tmp_path / "emb"
    manifest = _manifest(["data/xc/parus_major/XC1.ogg"])
    res = embed_shard(p, manifest, _encoder(), out_dir=out)
    assert "data/xc/parus_major/XC1.ogg" in res.failed
    # Completion accounting is in manifest-path space, so the failure must be too.
    assert shard_is_complete(p, manifest, out)


def test_zero_overlap_still_raises_when_even_basenames_differ(tmp_path):
    """The mismatch guard survives the resolver: members that match nothing
    (not even by basename) are still a systematic key mismatch."""
    p = _shard(tmp_path, [("XC999.ogg", _ogg_bytes())])
    manifest = _manifest(["data/xc/parus_major/XC1.ogg"])
    with pytest.raises(ValueError, match="ZERO overlap"):
        embed_shard(p, manifest, _encoder(), out_dir=tmp_path / "emb")
