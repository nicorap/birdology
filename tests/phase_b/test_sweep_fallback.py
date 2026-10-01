"""Encoder-score fallback for eval species absent from the training
class axis (ruling (b), fleet failure 2026-10).

On the fleet, ~60 cells across 5 shards died because `_map_columns`
refuses whole cells when ANY eval-scoreable species is missing from the
fit group's training rows (e.g. NES eval has 'warvir'/'yelwar' never
present in training; PER has 'goeant1'/'scbwoo5'). The ruling: the
learned arms fall back PER SPECIES to the encoder's raw eval score (the
deployed-system behavior) for unfitted species, and every row reports a
`species_coverage` fraction. The strict `_map_columns` refusal remains
for any caller not opting into the fallback.

Fixtures are tiny in-memory CacheViews — no network, no WEKA.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import birdology.phase_b.sweep as sweep_mod
from birdology.phase_b.cache import CacheView
from birdology.phase_b.learned import (
    OffsetModel,
    fit_logit_offset,
    fit_mlp_head,
    prepare_training,
)
from birdology.phase_b.rescoring import prior_for_rows
from birdology.phase_b.sweep import (
    Provenance,
    SweepCell,
    _eval_probabilities,
    _map_columns,
    _map_columns_with_fallback,
    run_cell,
    write_rows,
)
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import FLOOR, PriorSpec, PriorTensor

_CELLS = ["cella", "cellb", "cellc", "celld"]
_DOI = "10.15468/dl.test00"

_PROV = Provenance(
    git_sha="deadbeefcafe",
    train_cache_dir="/caches/train",
    eval_cache_dir="/caches/eval/HSN",
    prior_path="/priors/prior_res4.npz",
    prior_doi=_DOI,
)


def _prior(codes_gbif: list[int]) -> PriorTensor:
    n_species, n_cols = len(codes_gbif), len(_CELLS) * WEEKS_PER_YEAR
    dense = (1.0 + np.arange(n_species * n_cols, dtype=np.float64)).reshape(
        n_species, n_cols
    ) / (n_species * n_cols + 1.0)
    return PriorTensor(
        spec=PriorSpec(resolution=4, source="eod", variant="full"),
        species_keys=np.asarray(codes_gbif, dtype=np.int64),
        cells=list(_CELLS),
        values=sparse.csr_matrix(dense),
        gbif_doi=_DOI,
    )


def _cache_table(
    *,
    codes: list[str],
    class_idx: dict[str, int],
    n: int,
    labels: list[str],
    splits: list[str],
    logits: np.ndarray,
    seed: int,
    d: int = 6,
) -> pa.Table:
    rng = np.random.default_rng(seed)
    embeddings = rng.normal(size=(n, d))
    for i in range(n):
        embeddings[i, class_idx[labels[i]] % d] += 1.5
    return pa.table(
        {
            "filepath": pa.array([f"r{i}.ogg" for i in range(n)], pa.string()),
            "cell": pa.array(
                [_CELLS[i % len(_CELLS)] for i in range(n)], pa.string()
            ),
            "week": pa.array([1 + (i % 13) for i in range(n)], pa.int8()),
            "split": pa.array(splits, pa.string()),
            "ebird_code": pa.array(labels, pa.string()),
            "embedding": pa.array(embeddings.tolist(), pa.list_(pa.float64())),
            "logits": pa.array(logits.tolist(), pa.list_(pa.float64())),
        }
    )


def _correlated_logits(
    labels: list[str], class_idx: dict[str, int], seed: int
) -> np.ndarray:
    rng = np.random.default_rng(seed)
    n = len(labels)
    logits = rng.uniform(0.15, 0.45, size=(n, len(class_idx)))
    for i, code in enumerate(labels):
        logits[i, class_idx[code]] = rng.uniform(0.55, 0.9)
    return logits


def _fixture(train_codes: list[str], eval_codes: list[str]):
    """Caches whose TRAINING rows carry only *train_codes* while the
    eval multilabels carry *eval_codes* — the fleet's failure shape when
    eval_codes has species training never saw."""
    all_codes = sorted(set(train_codes) | set(eval_codes))
    class_idx = {code: i for i, code in enumerate(all_codes)}
    code_to_gbif = {code: 111 * (i + 1) for i, code in enumerate(all_codes)}
    prior = _prior([code_to_gbif[c] for c in all_codes])

    n_train = 24
    train_labels = [train_codes[i % len(train_codes)] for i in range(n_train)]
    train = CacheView(
        table=_cache_table(
            codes=all_codes,
            class_idx=class_idx,
            n=n_train,
            labels=train_labels,
            splits=["train_xc"] * n_train,
            logits=_correlated_logits(train_labels, class_idx, seed=11),
            seed=1,
        ),
        logits_are_raw=False,
        n_classes=len(all_codes),
    )

    n_eval = 20
    eval_labels = [eval_codes[i % len(eval_codes)] for i in range(n_eval)]
    eval_table = _cache_table(
        codes=all_codes,
        class_idx=class_idx,
        n=n_eval,
        labels=eval_labels,
        splits=["test_hsn"] * n_eval,
        logits=_correlated_logits(eval_labels, class_idx, seed=23),
        seed=2,
    )
    eval_view = CacheView(
        table=eval_table, logits_are_raw=False, n_classes=len(all_codes)
    )
    metadata = pa.table(
        {
            "filepath": eval_table.column("filepath"),
            "ebird_code_multilabel": pa.array(
                [[c] for c in eval_labels], pa.list_(pa.string())
            ),
        }
    )
    return train, eval_view, metadata, prior, class_idx, code_to_gbif


_THREE = ["amecro", "blujay", "carwre"]
_FOUR = ["amecro", "blujay", "carwre", "dowwoo"]


def _cell(**overrides) -> SweepCell:
    base = dict(
        encoder="perch",
        method="mlp",
        prior_variant="full",
        resolution=4,
        tau=None,
        test_set="HSN",
        seeds=(0, 1, 2, 3, 4),
    )
    base.update(overrides)
    return SweepCell(**base)


def _run(cell, fixture, **kwargs) -> dict:
    train, eval_view, metadata, prior, class_idx, code_to_gbif = fixture
    return run_cell(
        cell,
        train_cache=train,
        eval_cache=eval_view,
        eval_metadata=metadata,
        prior=prior,
        class_idx=class_idx,
        code_to_gbif=code_to_gbif,
        provenance=_PROV,
        **kwargs,
    )


def _capture_scored(monkeypatch) -> list[np.ndarray]:
    """Capture every `scored` matrix run_cell hands to top1 — one call
    per executed seed, in seed order."""
    captured: list[np.ndarray] = []
    real_top1 = sweep_mod.top1

    def capturing(scored, y):
        captured.append(np.array(scored, copy=True))
        return real_top1(scored, y)

    monkeypatch.setattr(sweep_mod, "top1", capturing)
    return captured


# ---------------------------------------------------------------------------
# (i) the mask helper — and the strict helper's refusal is UNCHANGED
# ---------------------------------------------------------------------------


def test_map_columns_with_fallback_mask_and_columns():
    cols, fitted = _map_columns_with_fallback(
        ["amecro", "blujay", "carwre"], ["amecro", "carwre", "dowwoo"]
    )
    assert cols.tolist() == [0, 2, -1]
    assert fitted.tolist() == [True, True, False]
    assert fitted.dtype == np.bool_

    cols_full, fitted_full = _map_columns_with_fallback(
        ["amecro", "blujay"], ["amecro", "blujay"]
    )
    assert cols_full.tolist() == [0, 1]
    assert fitted_full.all()


def test_strict_map_columns_still_refuses_missing_species():
    """The fallback is OPT-IN at the two run_cell sites; any other
    caller keeps the hard refusal."""
    with pytest.raises(ValueError, match="dowwoo"):
        _map_columns(["amecro", "blujay"], ["amecro", "dowwoo"])


# ---------------------------------------------------------------------------
# (ii) mlp: unfitted species column IS the encoder's eval score
# ---------------------------------------------------------------------------


def test_mlp_unfitted_species_column_equals_encoder_eval_score(monkeypatch):
    fixture = _fixture(_THREE, _FOUR)
    _train, eval_view, _meta, _prior_t, class_idx, _bridge = fixture
    captured = _capture_scored(monkeypatch)
    _run(_cell(method="mlp"), fixture, mlp_hidden=8, mlp_max_iter=40)

    scoreable = sorted(_FOUR)
    eval_scores = _eval_probabilities(eval_view, class_idx, scoreable)
    j_unfitted = scoreable.index("dowwoo")
    assert len(captured) == 5  # one scored matrix per seed
    for scored in captured:
        assert np.array_equal(scored[:, j_unfitted], eval_scores[:, j_unfitted])
        for j, code in enumerate(scoreable):
            if code == "dowwoo":
                continue
            assert not np.array_equal(scored[:, j], eval_scores[:, j]), (
                f"fitted column {code} should carry MLP probabilities, "
                "not the raw encoder score"
            )


# ---------------------------------------------------------------------------
# (iii) offset per_species: unfitted species get lam=0 — the encoder
#        score in the arm's log output space
# ---------------------------------------------------------------------------


def test_offset_per_species_unfitted_column_is_encoder_score_identity(
    monkeypatch,
):
    fixture = _fixture(_THREE, _FOUR)
    _train, eval_view, _meta, _prior_t, class_idx, _bridge = fixture
    captured = _capture_scored(monkeypatch)
    _run(_cell(method="offset_per_species"), fixture)

    scoreable = sorted(_FOUR)
    eval_scores = _eval_probabilities(eval_view, class_idx, scoreable)
    j_unfitted = scoreable.index("dowwoo")
    # OffsetModel.predict(s, p) = log(max(s, FLOOR)) + lam * log(max(p,
    # FLOOR)); lam=0 makes the prior term vanish EXACTLY, so the
    # unfitted column is the encoder score passed through the arm's
    # fixed log transform — rank-identical to the raw encoder score and
    # scale-consistent with the fitted (log-space) columns.
    expected = np.log(np.maximum(eval_scores[:, j_unfitted], FLOOR))
    assert len(captured) >= 1  # variant 'full' offset rows are degenerate
    for scored in captured:
        assert np.array_equal(scored[:, j_unfitted], expected)
        for j, code in enumerate(scoreable):
            if code == "dowwoo":
                continue
            fitted_expected = np.log(np.maximum(eval_scores[:, j], FLOOR))
            assert not np.array_equal(scored[:, j], fitted_expected), (
                f"fitted column {code} should carry a nonzero lam offset"
            )


# ---------------------------------------------------------------------------
# (iv) species_coverage — fraction fitted, 1.0 for non-per-species arms
# ---------------------------------------------------------------------------


def test_species_coverage_is_fraction_of_fitted_species(tmp_path):
    fixture = _fixture(_THREE, _FOUR)
    row = _run(_cell(method="mlp"), fixture, mlp_hidden=8, mlp_max_iter=40)
    assert row["species_coverage"] == pytest.approx(0.75)  # 3 of 4 fitted

    # The column lands in the parquet schema.
    out = tmp_path / "results"
    write_rows([row], out)
    on_disk = pa.concat_tables(
        [pq.read_table(p) for p in sorted(out.glob("*.parquet"))]
    )
    assert "species_coverage" in on_disk.column_names
    assert on_disk.column("species_coverage").to_pylist() == [
        pytest.approx(0.75)
    ]


def test_species_coverage_is_one_for_rescoring_and_offset_global():
    fixture = _fixture(_THREE, _FOUR)
    rescoring = _run(_cell(method="rescoring", tau=1.0), fixture)
    assert rescoring["species_coverage"] == 1.0
    # offset_global's scalar lam applies to every column — no per-column
    # mapping, full coverage by construction.
    offset_global = _run(_cell(method="offset_global"), fixture)
    assert offset_global["species_coverage"] == 1.0


def test_species_coverage_is_one_for_full_coverage_learned_cell():
    fixture = _fixture(_THREE, _THREE)
    row = _run(_cell(method="mlp"), fixture, mlp_hidden=8, mlp_max_iter=40)
    assert row["species_coverage"] == 1.0


# ---------------------------------------------------------------------------
# (v) regression guard: full coverage is BIT-IDENTICAL to the old path
# ---------------------------------------------------------------------------


def test_mlp_full_coverage_scores_bit_identical_to_pre_fallback_path(
    monkeypatch,
):
    fixture = _fixture(_THREE, _THREE)
    train, eval_view, metadata, prior, class_idx, code_to_gbif = fixture
    captured = _capture_scored(monkeypatch)
    _run(_cell(method="mlp"), fixture, mlp_hidden=8, mlp_max_iter=40)

    # The pre-fallback path, reimplemented verbatim for seed 0:
    # scored = model.predict(...)[:, _map_columns(train_scoreable,
    # scoreable)] (sweep.py as of the strict refusal).
    embeddings, _scores, y_train, train_scoreable, _stats = prepare_training(
        train, class_idx
    )
    train_cells = train.table.column("cell").to_pylist()
    train_weeks = train.table.column("week").to_pylist()
    train_lookup = prior_for_rows(
        prior, train_cells, train_weeks, train_scoreable, code_to_gbif
    )
    model = fit_mlp_head(
        embeddings, train_lookup.probs, y_train, seed=0, hidden=8, max_iter=40
    )
    scoreable = sorted(_THREE)
    eval_cells = eval_view.table.column("cell").to_pylist()
    eval_weeks = eval_view.table.column("week").to_pylist()
    eval_lookup = prior_for_rows(
        prior, eval_cells, eval_weeks, train_scoreable, code_to_gbif
    )
    eval_embeddings = np.asarray(
        eval_view.table.column("embedding").to_pylist(), dtype=np.float64
    )
    proba = model.predict(eval_embeddings, eval_lookup.probs)
    old_scored = proba[:, _map_columns(train_scoreable, scoreable)]
    assert np.array_equal(captured[0], old_scored)


def test_offset_per_species_full_coverage_bit_identical_to_old_path(
    monkeypatch,
):
    fixture = _fixture(_THREE, _THREE)
    train, eval_view, metadata, prior, class_idx, code_to_gbif = fixture
    captured = _capture_scored(monkeypatch)
    _run(_cell(method="offset_per_species"), fixture)

    embeddings, train_scores, y_train, train_scoreable, _stats = (
        prepare_training(train, class_idx)
    )
    train_cells = train.table.column("cell").to_pylist()
    train_weeks = train.table.column("week").to_pylist()
    train_lookup = prior_for_rows(
        prior, train_cells, train_weeks, train_scoreable, code_to_gbif
    )
    model = fit_logit_offset(
        train_scores, train_lookup.probs, y_train, per_species=True, seed=0
    )
    scoreable = sorted(_THREE)
    eval_cells = eval_view.table.column("cell").to_pylist()
    eval_weeks = eval_view.table.column("week").to_pylist()
    eval_lookup = prior_for_rows(
        prior, eval_cells, eval_weeks, scoreable, code_to_gbif
    )
    lam_eval = np.asarray(model.lam, dtype=np.float64)[
        _map_columns(train_scoreable, scoreable)
    ]
    eval_scores = _eval_probabilities(eval_view, class_idx, scoreable)
    old_scored = OffsetModel(lam=lam_eval, per_species=True).predict(
        eval_scores, eval_lookup.probs
    )
    assert np.array_equal(captured[0], old_scored)


# ---------------------------------------------------------------------------
# the fallback logs ONE warning naming the cell and the missing codes
# ---------------------------------------------------------------------------


def test_fallback_logs_one_warning_naming_cell_and_missing_species(caplog):
    fixture = _fixture(_THREE, _FOUR)
    with caplog.at_level(logging.WARNING, logger="birdology.phase_b.sweep"):
        _run(_cell(method="mlp"), fixture, mlp_hidden=8, mlp_max_iter=40)
    warnings = [
        r
        for r in caplog.records
        if "training class axis" in r.getMessage()
    ]
    assert len(warnings) == 1, (
        "exactly ONE warning per cell (not per species, not per seed)"
    )
    message = warnings[0].getMessage()
    assert "dowwoo" in message
    assert "mlp" in message and "HSN" in message


def test_full_coverage_cell_logs_no_fallback_warning(caplog):
    fixture = _fixture(_THREE, _THREE)
    with caplog.at_level(logging.WARNING, logger="birdology.phase_b.sweep"):
        _run(_cell(method="mlp"), fixture, mlp_hidden=8, mlp_max_iter=40)
    assert not [
        r
        for r in caplog.records
        if "training class axis" in r.getMessage()
    ]
