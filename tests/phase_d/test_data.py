"""Reading Phase A caches into arrays."""
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from birdology.phase_d.data import in_box, load_cache, xc_id


def _write(path, names, dim=4, n_axis=3, offset=0.0):
    n = len(names)
    pq.write_table(pa.table({
        "filepath": names,
        "embedding": [list(np.full(dim, i + offset, np.float32)) for i in range(n)],
        "logits": [list(np.full(n_axis, 0.1 * i, np.float32)) for i in range(n)],
    }), path)


def test_load_concatenates_filters_and_dedups(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir(); b.mkdir()
    _write(a / "s1.parquet", ["XC1.ogg", "XC2.ogg"])
    _write(b / "s1.parquet", ["XC2.ogg", "XC3.ogg"], offset=100.0)    # XC2 again: first row wins
    c = load_cache([a, b])
    assert c.filepaths == ["XC1.ogg", "XC2.ogg", "XC3.ogg"]
    assert c.embeddings.shape == (3, 4) and c.scores.shape == (3, 3)
    assert c.embeddings[1, 0] == 1.0
    only = load_cache([a, b], keep=["XC3.ogg"], scores=False)
    assert only.filepaths == ["XC3.ogg"] and only.scores is None
    assert only.take(np.array([0])).filepaths == ["XC3.ogg"]


def test_load_refuses_an_empty_result(tmp_path):
    _write(tmp_path / "s.parquet", ["XC1.ogg"])
    with pytest.raises(ValueError):
        load_cache([tmp_path], keep=["XC9.ogg"])


def test_xc_id_and_box():
    assert xc_id("XC1051471.flac") == 1051471
    with pytest.raises(ValueError):
        xc_id("recording.flac")
    assert in_box([55.7, 10.0], [12.6, -60.0]).tolist() == [True, False]
