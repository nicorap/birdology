"""Reproduction-gate comparison logic — pure, no WEKA, no network.

The gate (scripts/cluster/repro_gate.py) is Phase B's hard gate: our
pipeline must land within ±0.05 absolute cmAP of BirdSet's published
Perch row (Table 5, arXiv:2403.10380v3) before any prior-conditioned
number is reported. These tests pin the comparison logic, the output
line contract, the sigmoid-if-raw branch, and the BirdNET report-only
policy; the controller runs the fleet side.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_REPO = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_REPO / "src"))
sys.path.insert(0, str(_REPO / "scripts" / "cluster"))

import repro_gate
from birdology.phase_b.metrics import cmap


# ---------------------------------------------------------------------------
# compare_to_published
# ---------------------------------------------------------------------------


def test_compare_within_tolerance_passes():
    """Published Perch NBP cmAP is 0.63; 0.60 is within ±0.05."""
    ok, published, delta = repro_gate.compare_to_published("NBP", 0.60)
    assert ok is True
    assert published == pytest.approx(0.63)
    assert delta == pytest.approx(-0.03)


def test_compare_out_of_tolerance_fails():
    """0.50 vs published 0.63 is a -0.13 delta -- stack-level breakage."""
    ok, published, delta = repro_gate.compare_to_published("NBP", 0.50)
    assert ok is False
    assert published == pytest.approx(0.63)
    assert delta == pytest.approx(-0.13)


def test_compare_exactly_at_tolerance_boundary_passes():
    """±0.05 is INclusive: the tolerance text says 'within ±0.05'."""
    ok, _, _ = repro_gate.compare_to_published("PER", 0.18 + 0.05)
    assert ok is True


def test_compare_unknown_test_set_raises_naming_it():
    with pytest.raises(ValueError, match="XYZ"):
        repro_gate.compare_to_published("XYZ", 0.5)


# ---------------------------------------------------------------------------
# per-pair line format + gate verdict
# ---------------------------------------------------------------------------

#: The contract from the brief:
#: `ok|FAIL <encoder>/<testset>: cmap=X.XXX published=Y.YY delta=+Z.ZZZ`
LINE_RE = re.compile(
    r"^(ok|FAIL) (\w+)/(\w+): cmap=\d\.\d{3} "
    r"published=\d\.\d{2} delta=[+-]\d\.\d{3}$"
)


def test_pair_line_is_parseable_ok_case():
    line = repro_gate.format_pair_line("perch", "NBP", 0.60)
    m = LINE_RE.match(line)
    assert m, f"unparseable line: {line!r}"
    assert m.group(1) == "ok"
    assert m.group(2) == "perch"
    assert m.group(3) == "NBP"


def test_pair_line_is_parseable_fail_case_with_signed_delta():
    line = repro_gate.format_pair_line("perch", "NBP", 0.50)
    m = LINE_RE.match(line)
    assert m, f"unparseable line: {line!r}"
    assert m.group(1) == "FAIL"
    assert "delta=-0.130" in line


def test_verdict_ok_iff_all_pairs_pass():
    all_pass = [("perch", "NBP", 0.63), ("perch", "HSN", 0.44)]
    one_fail = [("perch", "NBP", 0.63), ("perch", "HSN", 0.20)]
    text_ok, ok = repro_gate.render_report(all_pass)
    text_fail, fail_ok = repro_gate.render_report(one_fail)
    assert ok is True and "REPRO_GATE_OK" in text_ok
    assert "REPRO_GATE_FAIL" not in text_ok
    assert fail_ok is False and "REPRO_GATE_FAIL" in text_fail
    assert "REPRO_GATE_OK" not in text_fail


def test_report_has_one_parseable_line_per_pair_plus_verdict():
    pairs = [("perch", "NBP", 0.63), ("perch", "SSW", 0.28)]
    text, _ = repro_gate.render_report(pairs)
    lines = text.splitlines()
    assert len(lines) == 3
    assert all(LINE_RE.match(line) for line in lines[:2])


# ---------------------------------------------------------------------------
# sigmoid-if-raw branch
# ---------------------------------------------------------------------------


def _fixture_scores_labels():
    """6 rows x 3 classes of raw logits with a mixed label matrix."""
    rng = np.random.default_rng(20261001)
    raw = rng.normal(scale=3.0, size=(6, 3)).astype(np.float32)
    y = np.array(
        [[1, 0, 0], [0, 1, 0], [1, 1, 0], [0, 0, 1], [0, 1, 1], [0, 0, 0]],
        dtype=np.uint8,
    )
    return raw, y


def test_raw_cache_gets_sigmoid_applied():
    raw, _ = _fixture_scores_labels()
    out = repro_gate.calibrate_scores(raw, logits_are_raw=True)
    np.testing.assert_allclose(out, 1.0 / (1.0 + np.exp(-raw)), rtol=1e-6)
    assert out.min() >= 0.0 and out.max() <= 1.0


def test_sigmoid_cache_passes_through_untouched():
    raw, _ = _fixture_scores_labels()
    probs = (1.0 / (1.0 + np.exp(-raw))).astype(np.float32)
    out = repro_gate.calibrate_scores(probs, logits_are_raw=False)
    assert out is probs  # untouched, not even copied


def test_sigmoid_is_monotone_invariant_for_cmap():
    """Rank metrics can't change under sigmoid — the branch exists only
    for threshold comparability across encoders."""
    raw, y = _fixture_scores_labels()
    cmap_raw, n_raw = cmap(raw, y)
    cmap_sig, n_sig = cmap(
        repro_gate.calibrate_scores(raw, logits_are_raw=True), y
    )
    assert n_raw == n_sig
    assert cmap_raw == pytest.approx(cmap_sig, abs=1e-12)


# ---------------------------------------------------------------------------
# BirdNET is report-only
# ---------------------------------------------------------------------------


def test_failing_birdnet_delta_does_not_flip_gate_verdict():
    """BirdNET has no published BirdSet row (test-data leakage, §4.1):
    its pairs are printed for the record but NEVER gate pass/fail."""
    pairs = [
        ("perch", "NBP", 0.63),  # passes
        ("birdnet", "NBP", 0.10),  # delta -0.53, way out of tolerance
    ]
    text, ok = repro_gate.render_report(pairs)
    assert ok is True
    assert text.splitlines()[-1] == "REPRO_GATE_OK"
    # BirdNET's numbers still appear, for the record.
    assert "birdnet/NBP" in text and "cmap=0.100" in text


def test_failing_perch_delta_still_fails_with_birdnet_present():
    pairs = [
        ("perch", "NBP", 0.10),
        ("birdnet", "NBP", 0.63),
    ]
    _, ok = repro_gate.render_report(pairs)
    assert ok is False


# ---------------------------------------------------------------------------
# score_pair — the full cache->cmap pipeline on tmpdir fixtures (no WEKA)
# ---------------------------------------------------------------------------


def _write_fixture_pair(tmp_path, *, raw_flag):
    """One cache shard + metadata parquet + labels file, 4 recordings x
    3 classes (aaa/bbb/ccc), labels chosen so class order matters."""
    logits = [
        [5.0, -2.0, 0.5],
        [-1.0, 3.0, -0.5],
        [0.2, 0.1, 4.0],
        [-3.0, -2.5, -1.0],
    ]
    cols = {
        "filepath": pa.array(
            ["r1.ogg", "r2.ogg", "r3.ogg", "r4.ogg"], pa.string()
        ),
        "cell": pa.array(["c"] * 4, pa.string()),
        "week": pa.array([1] * 4, pa.int8()),
        "ebird_code": pa.array([None] * 4, pa.string()),
        "split": pa.array(["test"] * 4, pa.string()),
        "embedding": pa.array([[0.0, 1.0]] * 4, pa.list_(pa.float32())),
        "logits": pa.array(logits, pa.list_(pa.float32())),
    }
    metadata = {b"schema_variant": b"embedding+logits"}
    if raw_flag:
        metadata[b"perch_logits_are_raw_uncalibrated"] = b"true"
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    pq.write_table(
        pa.table(cols).replace_schema_metadata(metadata),
        cache_dir / "shard_0001.parquet",
    )

    meta_path = tmp_path / "meta.parquet"
    pq.write_table(
        pa.table(
            {
                # Deliberately NOT in cache row order: the join is by
                # filepath, never positional.
                "filepath": pa.array(
                    ["r3.ogg", "r1.ogg", "r4.ogg", "r2.ogg"], pa.string()
                ),
                "ebird_code_multilabel": pa.array(
                    [["ccc"], ["aaa"], [], ["bbb", "ccc"]],
                    pa.list_(pa.string()),
                ),
            }
        ),
        meta_path,
    )

    labels_path = tmp_path / "labels.txt"
    labels_path.write_text("aaa\nbbb\nccc\n", encoding="utf-8")
    return cache_dir, meta_path, labels_path, np.asarray(logits)


def test_score_pair_computes_cmap_and_auroc_on_fixture(tmp_path):
    cache_dir, meta_path, labels_path, logits = _write_fixture_pair(
        tmp_path, raw_flag=True
    )
    result = repro_gate.score_pair(cache_dir, meta_path, labels_path)
    # Expected, computed independently through the Task 1/2 modules with
    # the label matrix in sorted-scoreable order (aaa, bbb, ccc).
    y = np.array(
        [[1, 0, 0], [0, 1, 1], [0, 0, 1], [0, 0, 0]], dtype=np.uint8
    )
    exp_cmap, exp_n = cmap(logits, y)
    assert result.cmap == pytest.approx(exp_cmap)
    assert result.n_scored == exp_n
    assert 0.0 <= result.auroc <= 1.0
    # Raw cache -> scores are sigmoid probabilities.
    assert result.scores.min() >= 0.0 and result.scores.max() <= 1.0


def test_score_pair_refuses_cache_row_missing_from_metadata(tmp_path):
    cache_dir, meta_path, labels_path, _ = _write_fixture_pair(
        tmp_path, raw_flag=True
    )
    meta = pq.read_table(meta_path)
    pq.write_table(meta.slice(0, 3), meta_path)  # drop one recording
    with pytest.raises(ValueError, match="r2.ogg"):
        repro_gate.score_pair(cache_dir, meta_path, labels_path)



