"""Phase B's single gateway to Phase A embedding caches.

Every later fusion task reads cached posteriors through `load_cache` and
scores classes through `class_index`/`align_labels`. The tests here pin the
two failure classes that would silently poison a sweep: a heterogeneous
cache (mixed raw/calibrated logits in one dir) and POSITIONAL class
mapping (scores attributed to the wrong species when a label file is
reordered).

Nothing here touches the network or WEKA: shards are written with pyarrow
into tmp_path.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_b.cache import (
    CacheView,
    align_labels,
    class_index,
    load_cache,
)

_RAW_KEY = b"perch_logits_are_raw_uncalibrated"


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


def _write_shard(
    path,
    filepaths,
    *,
    logits=None,
    raw_flag=None,
    multilabel=None,
):
    """One Phase A shard parquet, matching embed.py's writer contract."""
    n = len(filepaths)
    cols = {
        "filepath": pa.array(filepaths, pa.string()),
        "cell": pa.array(["c"] * n, pa.string()),
        "week": pa.array([1] * n, pa.int8()),
        "ebird_code": pa.array([None] * n, pa.string()),
        "split": pa.array(["test"] * n, pa.string()),
        "embedding": pa.array([[0.0, 1.0]] * n, pa.list_(pa.float32())),
    }
    variant = b"embedding"
    if logits is not None:
        cols["logits"] = pa.array(logits, pa.list_(pa.float32()))
        variant = b"embedding+logits"
    if multilabel is not None:
        cols["ebird_code_multilabel"] = pa.array(multilabel, pa.list_(pa.string()))
    metadata = {b"schema_variant": variant}
    if raw_flag is not None:
        metadata[_RAW_KEY] = raw_flag
    table = pa.table(cols).replace_schema_metadata(metadata)
    pq.write_table(table, path)


# ---------------------------------------------------------------------------
# load_cache
# ---------------------------------------------------------------------------


def test_load_cache_concatenates_shards_and_reads_raw_flag_true(tmp_path):
    _write_shard(
        tmp_path / "shard_0001.parquet",
        ["a.ogg", "b.ogg"],
        logits=[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
        raw_flag=b"true",
    )
    _write_shard(
        tmp_path / "shard_0002.parquet",
        ["c.ogg"],
        logits=[[7.0, 8.0, 9.0]],
        raw_flag=b"true",
    )
    view = load_cache(tmp_path)
    assert isinstance(view, CacheView)
    assert view.table.num_rows == 3
    assert set(view.table.column("filepath").to_pylist()) == {
        "a.ogg",
        "b.ogg",
        "c.ogg",
    }
    # Original columns survive concatenation.
    assert "embedding" in view.table.column_names
    assert "logits" in view.table.column_names
    assert view.logits_are_raw is True
    assert view.n_classes == 3


def test_load_cache_without_the_flag_reads_false(tmp_path):
    _write_shard(
        tmp_path / "shard_0001.parquet",
        ["a.ogg"],
        logits=[[0.1, 0.9]],
    )
    view = load_cache(tmp_path)
    assert view.logits_are_raw is False
    assert view.n_classes == 2


def test_load_cache_without_logits_column_has_zero_classes(tmp_path):
    _write_shard(tmp_path / "shard_0001.parquet", ["a.ogg"])
    view = load_cache(tmp_path)
    assert view.logits_are_raw is False
    assert view.n_classes == 0
    assert "logits" not in view.table.column_names


def test_load_cache_mixed_raw_flags_across_shards_raise(tmp_path):
    """One shard flagged raw, one not: the cache-poisoning class. Refuse
    loudly — concatenating would mix raw and calibrated numbers on one
    class axis."""
    _write_shard(
        tmp_path / "shard_0001.parquet",
        ["a.ogg"],
        logits=[[1.0, 2.0]],
        raw_flag=b"true",
    )
    _write_shard(
        tmp_path / "shard_0002.parquet",
        ["b.ogg"],
        logits=[[0.1, 0.2]],
    )
    with pytest.raises(ValueError, match="perch_logits_are_raw_uncalibrated"):
        load_cache(tmp_path)


def test_load_cache_empty_dir_raises(tmp_path):
    """Zero shards is a wrong path or an unfinished fleet, never a valid
    empty cache."""
    with pytest.raises(ValueError, match="parquet"):
        load_cache(tmp_path)


# ---------------------------------------------------------------------------
# class_index
# ---------------------------------------------------------------------------


def test_class_index_maps_codes_to_line_numbers(tmp_path):
    labels = tmp_path / "labels.txt"
    labels.write_text("gretit\neurrob\ncomcha\n", encoding="utf-8")
    assert class_index(labels) == {"gretit": 0, "eurrob": 1, "comcha": 2}


def test_class_index_skips_blank_lines_without_shifting_indices(tmp_path):
    """A blank line is noise, not a class: real indices follow the
    NON-BLANK line order (the model head has no blank row)."""
    labels = tmp_path / "labels.txt"
    labels.write_text("gretit\n\neurrob\n\n", encoding="utf-8")
    assert class_index(labels) == {"gretit": 0, "eurrob": 1}


def test_class_index_duplicate_code_raises(tmp_path):
    """A duplicate silently shadows a class — one of the two rows becomes
    unreachable. Refused."""
    labels = tmp_path / "labels.txt"
    labels.write_text("gretit\neurrob\ngretit\n", encoding="utf-8")
    with pytest.raises(ValueError, match="gretit"):
        class_index(labels)


# ---------------------------------------------------------------------------
# align_labels
# ---------------------------------------------------------------------------


def _soundscape_table(multilabel):
    """A soundscape cache slice: plain ebird_code all-None, labels in the
    multilabel column."""
    n = len(multilabel)
    return pa.table(
        {
            "filepath": pa.array([f"s{i}.ogg" for i in range(n)], pa.string()),
            "ebird_code": pa.array([None] * n, pa.string()),
            "ebird_code_multilabel": pa.array(multilabel, pa.list_(pa.string())),
        }
    )


def test_KILLER_scoring_by_code_is_identical_across_label_file_orders(tmp_path):
    """THE KILLER TEST: the class mapping is by CODE, never positional.

    Two label files carry the same codes in opposite orders; each cache's
    logits axis follows its own file. 'eurrob' always scores 0.9 and
    'gretit' always 0.2 — distinct values, so a positional gather (zipping
    sorted scoreable species against logits columns in file order) visibly
    SWAPS the two species' scores on file B. Scoring through class_idx
    must yield identical per-species results for both files.
    """
    truth = {"eurrob": np.float32(0.9), "gretit": np.float32(0.2)}
    table = _soundscape_table([["eurrob"], ["gretit"], ["eurrob", "gretit"]])

    per_file: dict[str, dict[str, np.ndarray]] = {}
    logits_by_file: dict[str, np.ndarray] = {}
    for name, order in (("A", ["eurrob", "gretit"]), ("B", ["gretit", "eurrob"])):
        labels_path = tmp_path / f"labels_{name}.txt"
        labels_path.write_text("\n".join(order) + "\n", encoding="utf-8")
        idx = class_index(labels_path)
        # This cache's logits columns follow THIS file's class order.
        row_logits = np.zeros(len(order), dtype=np.float32)
        for code, p in truth.items():
            row_logits[idx[code]] = p
        logits = np.tile(row_logits, (table.num_rows, 1))
        logits_by_file[name] = logits

        Y, scoreable, unscoreable = align_labels(table, idx)
        assert scoreable == ["eurrob", "gretit"]  # sorted, file-order-free
        assert unscoreable == []
        assert Y.shape == (3, 2)
        # The scoring path every later task uses: gather the logits column
        # OF EACH CODE via class_idx, in scoreable order.
        gathered = logits[:, [idx[code] for code in scoreable]]
        per_file[name] = {
            code: gathered[:, j] for j, code in enumerate(scoreable)
        }

    for code, p in truth.items():
        np.testing.assert_allclose(per_file["A"][code], p)
        np.testing.assert_allclose(per_file["B"][code], p)
    # The fixture's teeth: the two files' raw logits columns really are
    # swapped, so a positional gather would have swapped the species.
    assert not np.allclose(logits_by_file["A"], logits_by_file["B"])


def test_unscoreable_species_are_reported_and_jointly_cover_the_union(tmp_path):
    """A label code absent from the class list cannot be scored — it goes
    in the third slot and gets NO Y column; scoreable + unscoreable must
    jointly equal the label union (nothing silently dropped)."""
    labels_path = tmp_path / "labels.txt"
    labels_path.write_text("eurrob\ngretit\n", encoding="utf-8")
    idx = class_index(labels_path)
    table = _soundscape_table(
        [["eurrob", "mystery1"], ["gretit"], ["mystery2"]]
    )
    Y, scoreable, unscoreable = align_labels(table, idx)
    assert scoreable == ["eurrob", "gretit"]
    assert unscoreable == ["mystery1", "mystery2"]
    assert Y.shape == (3, 2)
    # mystery2's row has no scoreable label -> all-zero, still present.
    np.testing.assert_array_equal(
        Y, np.array([[1, 0], [0, 1], [0, 0]], dtype=np.uint8)
    )
    label_union = {"eurrob", "gretit", "mystery1", "mystery2"}
    assert set(scoreable) | set(unscoreable) == label_union
    assert set(scoreable) & set(unscoreable) == set()


def test_empty_multilabel_rows_stay_as_all_zero_negative_rows(tmp_path):
    """Soundscape windows with no bird are NEGATIVES: all-zero Y rows,
    row count unchanged — dropping them would inflate every precision."""
    labels_path = tmp_path / "labels.txt"
    labels_path.write_text("eurrob\n", encoding="utf-8")
    idx = class_index(labels_path)
    table = _soundscape_table([[], ["eurrob"], []])
    Y, scoreable, unscoreable = align_labels(table, idx)
    assert Y.shape[0] == table.num_rows == 3
    np.testing.assert_array_equal(
        Y, np.array([[0], [1], [0]], dtype=np.uint8)
    )
    assert Y.dtype == np.uint8


def test_missing_label_column_raises_naming_available_columns(tmp_path):
    labels_path = tmp_path / "labels.txt"
    labels_path.write_text("eurrob\n", encoding="utf-8")
    idx = class_index(labels_path)
    table = pa.table({"filepath": pa.array(["a.ogg"], pa.string())})
    with pytest.raises(ValueError, match="filepath"):
        align_labels(table, idx)
    with pytest.raises(ValueError, match="ebird_code_multilabel"):
        align_labels(table, idx)
