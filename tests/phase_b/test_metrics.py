"""Phase B metrics — cmAP (the paper's primary number), AUROC, top-1,
stratified re-scoring, and the by-code logits gather.

cmAP here is BirdSet's headline metric: class-mean average precision over
classes with at least one positive, zero-positive classes EXCLUDED from the
mean and COUNTED in n_scored. AP/AUROC are plain-numpy implementations
(Task 6's sweep calls them millions of times); sklearn appears only as the
cross-check oracle. Nothing here touches the network or WEKA.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_b.metrics import (
    auroc_macro,
    cmap,
    gather_scores,
    stratified,
    top1,
)


# ---------------------------------------------------------------------------
# cmap
# ---------------------------------------------------------------------------


def test_cmap_matches_hand_computed_fixture():
    """4 rows x 3 classes, every AP worked by hand.

    Class 0: scores [0.9, 0.2, 0.8, 0.1], y [1, 0, 1, 0]
      desc order: 0.9(+) 0.8(+) 0.2(-) 0.1(-)
      precision at each positive's rank: 1/1, 2/2 -> AP = 1.
    Class 1: scores [0.6, 0.9, 0.1, 0.4], y [0, 1, 0, 1]
      desc order: 0.9(+) 0.6(-) 0.4(+) 0.1(-)
      precisions: 1/1 at rank 1, 2/3 at rank 3 -> AP = (1 + 2/3)/2 = 5/6.
    Class 2: scores [0.3, 0.5, 0.7, 0.2], y [1, 0, 0, 0]
      desc order: 0.7(-) 0.5(-) 0.3(+) 0.2(-)
      precision: 1/3 at rank 3 -> AP = 1/3.
    cmAP = (1 + 5/6 + 1/3)/3 = 13/18; all 3 classes have a positive.
    """
    scores = np.array(
        [
            [0.9, 0.6, 0.3],
            [0.2, 0.9, 0.5],
            [0.8, 0.1, 0.7],
            [0.1, 0.4, 0.2],
        ]
    )
    y = np.array(
        [
            [1, 0, 1],
            [0, 1, 0],
            [1, 0, 0],
            [0, 1, 0],
        ],
        dtype=np.uint8,
    )
    value, n_scored = cmap(scores, y)
    assert n_scored == 3
    assert value == pytest.approx(13.0 / 18.0, rel=1e-12)


def test_cmap_excludes_and_counts_zero_positive_classes():
    """3 classes, one all-negative: that class must not enter the mean
    (AP is undefined without a positive) and the exclusion must be
    visible in n_scored == 2, never silently absorbed.

    Class 0: scores [0.9, 0.1], y [1, 0] -> AP = 1.
    Class 1: scores [0.2, 0.8], y [1, 0] -> positive ranks 2nd -> AP = 1/2.
    Class 2: all-negative -> excluded.
    cmAP = (1 + 1/2)/2 = 3/4 over n_scored = 2.
    """
    scores = np.array([[0.9, 0.2, 0.5], [0.1, 0.8, 0.6]])
    y = np.array([[1, 1, 0], [0, 0, 0]], dtype=np.uint8)
    value, n_scored = cmap(scores, y)
    assert n_scored == 2
    assert value == pytest.approx(0.75, rel=1e-12)


def test_cmap_all_zero_positive_returns_nan_zero_not_error():
    """Everything all-negative: (nan, 0), never a ZeroDivisionError."""
    scores = np.array([[0.9, 0.2], [0.1, 0.8]])
    y = np.zeros((2, 2), dtype=np.uint8)
    value, n_scored = cmap(scores, y)
    assert n_scored == 0
    assert np.isnan(value)


# ---------------------------------------------------------------------------
# sklearn cross-check (oracle only — implementation stays plain numpy)
# ---------------------------------------------------------------------------


def _random_fixture(seed=20261001, n_rows=60, n_classes=7):
    """Seeded random scores + labels; distinct scores so no tie ambiguity
    clouds the oracle comparison, every class guaranteed >=1 positive and
    >=1 negative (so both AP and AUROC are defined for every class)."""
    rng = np.random.default_rng(seed)
    scores = rng.permutation(n_rows * n_classes).astype(np.float64)
    scores = scores.reshape(n_rows, n_classes) / (n_rows * n_classes)
    y = (rng.random((n_rows, n_classes)) < 0.3).astype(np.uint8)
    y[0, :] = 1  # >=1 positive per class
    y[1, :] = 0  # >=1 negative per class
    return scores, y


def test_cmap_per_class_ap_matches_sklearn():
    """Our rank-based AP == sklearn.average_precision_score, per class.

    Checked through the public API: cmap on a single-class slice returns
    (AP, 1), so the mean IS that class's AP.
    """
    from sklearn.metrics import average_precision_score

    scores, y = _random_fixture()
    for j in range(y.shape[1]):
        ours, n_scored = cmap(scores[:, [j]], y[:, [j]])
        assert n_scored == 1
        theirs = average_precision_score(y[:, j], scores[:, j])
        np.testing.assert_allclose(ours, theirs, atol=1e-12)


def test_auroc_macro_per_class_matches_sklearn():
    """Our AUROC == sklearn.roc_auc_score per class AND as the macro mean."""
    from sklearn.metrics import roc_auc_score

    scores, y = _random_fixture()
    per_class = []
    for j in range(y.shape[1]):
        ours, n_scored = auroc_macro(scores[:, [j]], y[:, [j]])
        assert n_scored == 1
        theirs = roc_auc_score(y[:, j], scores[:, j])
        np.testing.assert_allclose(ours, theirs, atol=1e-12)
        per_class.append(theirs)
    macro, n_scored = auroc_macro(scores, y)
    assert n_scored == y.shape[1]
    np.testing.assert_allclose(macro, np.mean(per_class), atol=1e-12)


# ---------------------------------------------------------------------------
# auroc_macro
# ---------------------------------------------------------------------------


def test_auroc_macro_skips_single_sign_classes_and_counts_them():
    """A class needs >=1 positive AND >=1 negative for AUROC. Class 0 is
    separable (AUROC 1), class 1 is all-positive, class 2 all-negative:
    only class 0 scores -> (1.0, 1). All-degenerate -> (nan, 0).
    """
    scores = np.array([[0.9, 0.5, 0.5], [0.1, 0.6, 0.4]])
    y = np.array([[1, 1, 0], [0, 1, 0]], dtype=np.uint8)
    value, n_scored = auroc_macro(scores, y)
    assert n_scored == 1
    assert value == pytest.approx(1.0)

    y_degenerate = np.array([[1, 0], [1, 0]], dtype=np.uint8)
    value, n_scored = auroc_macro(scores[:, :2], y_degenerate)
    assert n_scored == 0
    assert np.isnan(value)


# ---------------------------------------------------------------------------
# top1
# ---------------------------------------------------------------------------


def test_top1_counts_argmax_hits_over_rows_with_positives():
    """Row 0: argmax col 0, positive there -> 1. Row 1: argmax col 1,
    negative there (positive sits in col 0) -> 0. Row 2: NO positives ->
    excluded from the denominator (a no-bird segment has no notion of
    top-1 correctness). top1 = 1/2.
    """
    scores = np.array(
        [
            [0.9, 0.1, 0.2],
            [0.1, 0.8, 0.3],
            [0.5, 0.2, 0.9],
        ]
    )
    y = np.array(
        [
            [1, 0, 0],
            [1, 0, 0],
            [0, 0, 0],
        ],
        dtype=np.uint8,
    )
    assert top1(scores, y) == pytest.approx(0.5)


def test_top1_all_rows_without_positives_returns_nan():
    scores = np.array([[0.9, 0.1], [0.2, 0.8]])
    y = np.zeros((2, 2), dtype=np.uint8)
    assert np.isnan(top1(scores, y))


# ---------------------------------------------------------------------------
# gather_scores (the by-code gather carried over from Task 1's review)
# ---------------------------------------------------------------------------


def test_KILLER_gather_scores_selects_columns_by_code_never_positionally():
    """THE KILLER TEST: two class_idx dicts map the same codes to
    DIFFERENT column positions; the logits matrices follow each dict's
    own class order. 'eurrob' always scores 0.9 and 'gretit' always 0.2
    — distinct values, so a positional gather (taking columns in file
    order) would visibly SWAP the two species' scores on mapping B.
    gather_scores must yield identical per-species columns through both.
    """
    truth = {"eurrob": np.float32(0.9), "gretit": np.float32(0.2)}
    scoreable = sorted(truth)  # ["eurrob", "gretit"] — align_labels order
    n_rows = 3

    per_mapping: dict[str, np.ndarray] = {}
    logits_by_mapping: dict[str, np.ndarray] = {}
    for name, idx in (
        ("A", {"eurrob": 0, "gretit": 1}),
        ("B", {"gretit": 0, "eurrob": 1}),
    ):
        row = np.zeros(len(idx), dtype=np.float32)
        for code, p in truth.items():
            row[idx[code]] = p
        logits = np.tile(row, (n_rows, 1))
        logits_by_mapping[name] = logits
        gathered = gather_scores(logits, idx, scoreable)
        assert gathered.shape == (n_rows, len(scoreable))
        assert gathered.dtype == np.float32
        per_mapping[name] = gathered

    for j, code in enumerate(scoreable):
        np.testing.assert_allclose(per_mapping["A"][:, j], truth[code])
        np.testing.assert_allclose(per_mapping["B"][:, j], truth[code])
    np.testing.assert_array_equal(per_mapping["A"], per_mapping["B"])
    # The fixture's teeth: the two raw logits matrices really differ, so
    # a positional gather would have swapped the species.
    assert not np.allclose(logits_by_mapping["A"], logits_by_mapping["B"])


# ---------------------------------------------------------------------------
# stratified
# ---------------------------------------------------------------------------


def test_stratified_recomputes_metric_per_stratum_with_keys_preserved():
    """Two strata with hand-computably different cmAP.

    Stratum 'good' (rows 0-1), class 0 only positive class there:
      scores [0.9, 0.1], y [1, 0] -> AP = 1; class 1 all-negative in the
      stratum -> excluded -> ('good': (1.0, 1)).
    Stratum 7 (rows 2-3), class 1 only positive class there:
      scores [0.2, 0.8], y [1, 0] -> the positive ranks 2nd -> AP = 1/2
      -> (7: (0.5, 1)). Keys keep their original (hashable) identities.
    """
    scores = np.array(
        [
            [0.9, 0.3],
            [0.1, 0.4],
            [0.5, 0.2],
            [0.6, 0.8],
        ]
    )
    y = np.array(
        [
            [1, 0],
            [0, 0],
            [0, 1],
            [0, 0],
        ],
        dtype=np.uint8,
    )
    strata = np.array(["good", "good", 7, 7], dtype=object)
    result = stratified(scores, y, strata, cmap)
    assert set(result.keys()) == {"good", 7}
    assert result["good"][1] == 1
    assert result["good"][0] == pytest.approx(1.0)
    assert result[7][1] == 1
    assert result[7][0] == pytest.approx(0.5)


def test_stratified_stratum_without_scoreable_classes_yields_nan_zero():
    """A stratum whose rows are all-negative has no scoreable class:
    (nan, 0), never an exception."""
    scores = np.array([[0.9, 0.1], [0.2, 0.8]])
    y = np.array([[1, 0], [0, 0]], dtype=np.uint8)
    strata = np.array(["has_pos", "empty"])
    result = stratified(scores, y, strata, cmap)
    assert result["has_pos"] == (pytest.approx(1.0), 1)
    value, n_scored = result["empty"]
    assert n_scored == 0
    assert np.isnan(value)


# ---------------------------------------------------------------------------
# integration with Task 1's align_labels Y
# ---------------------------------------------------------------------------


def test_empty_label_rows_are_ap_auroc_negatives_but_not_top1_rows(tmp_path):
    """Empty-multilabel rows (no-bird soundscape windows) flow from
    cache.align_labels as all-zero Y rows and must count as NEGATIVES in
    AP/AUROC — dropping them would inflate every precision — while top1
    excludes them from its denominator.

    One class 'eurrob', 4 rows: [eurrob], [], [eurrob], [].
    scores: 0.9, 0.8, 0.4, 0.1 -> desc order 0.9(+) 0.8(-) 0.4(+) 0.1(-)
      AP with the empty rows = (1/1 + 2/3)/2 = 5/6  (without them it
      would be a trivial 1.0 — the fixture's teeth).
      AUROC: positive pairs beaten = (0.9>0.8, 0.9>0.1, 0.4>0.1) = 3 of
      4 -> 3/4.
    top1 (single class, argmax always col 0): scored only on the 2
    labelled rows, both hits -> 1.0.
    """
    from birdology.phase_b.cache import align_labels, class_index

    labels_path = tmp_path / "labels.txt"
    labels_path.write_text("eurrob\n", encoding="utf-8")
    idx = class_index(labels_path)
    multilabel = [["eurrob"], [], ["eurrob"], []]
    table = pa.table(
        {
            "filepath": pa.array(
                [f"s{i}.ogg" for i in range(4)], pa.string()
            ),
            "ebird_code_multilabel": pa.array(
                multilabel, pa.list_(pa.string())
            ),
        }
    )
    Y, scoreable, unscoreable = align_labels(table, idx)
    assert scoreable == ["eurrob"]
    assert unscoreable == []
    assert Y.shape == (4, 1)  # empty rows present as all-zero negatives

    scores = np.array([[0.9], [0.8], [0.4], [0.1]])
    ap, n_scored = cmap(scores, Y)
    assert n_scored == 1
    assert ap == pytest.approx(5.0 / 6.0, rel=1e-12)
    auroc, n_scored = auroc_macro(scores, Y)
    assert n_scored == 1
    assert auroc == pytest.approx(0.75, rel=1e-12)
    assert top1(scores, Y) == pytest.approx(1.0)
