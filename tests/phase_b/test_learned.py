"""The two learned cheap arms (Task 5): logit offset + MLP head.

Both arms train on CACHED arrays only — no audio, no encoder weights, no
network. Fixtures are in-memory numpy arrays and pyarrow tables.

The offset-arm fixtures use a residual-balanced design: class sizes are
chosen so that the BCE gradient w.r.t. lam at lam=0 is ~0 when the prior
is noise (sum of residuals ~0), which makes "noise prior => lam near 0"
a sharp statement rather than an artifact of log-prior acting as an
uncontrolled intercept (log p < 0 always, so any residual imbalance
would drag lam away from 0 even for a useless prior).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_b.learned import (
    MLPModel,
    OffsetModel,
    fit_logit_offset,
    fit_mlp_head,
    guard_training_split,
    prepare_training,
)
from birdology.phase_b.cache import CacheView


# ---------------------------------------------------------------------------
# fixtures — residual-balanced offset designs
# ---------------------------------------------------------------------------

#: With scores 0.9 (pos) / 0.1 (neg), sigmoid(log s) residuals are
#: -0.5263 per positive and +0.0909 per negative; 19 positives against
#: 110 negatives sums the residuals to ~0, killing the intercept bias.
_N_POS, _N_NEG = 19, 110
_S_POS, _S_NEG = 0.9, 0.1
_P_HIGH, _P_LOW = 0.7, 0.1


def _balanced_y_and_scores() -> tuple[np.ndarray, np.ndarray]:
    y = np.concatenate([np.ones(_N_POS), np.zeros(_N_NEG)])
    scores = np.where(y == 1, _S_POS, _S_NEG)
    return y[:, None], scores[:, None]


def _informative_prior(y: np.ndarray) -> np.ndarray:
    """High prior exactly where y is positive — the prior 'knows' y."""
    return np.where(y == 1, _P_HIGH, _P_LOW)


def _noise_prior(y: np.ndarray) -> np.ndarray:
    """{high, low} split evenly WITHIN each label class: zero
    correlation with y by construction (stratified, not sampled)."""
    p = np.empty_like(y, dtype=np.float64)
    for value in (0, 1):
        idx = np.flatnonzero(y[:, 0] == value)
        half = len(idx) // 2
        p[idx[:half], 0] = _P_HIGH
        p[idx[half:], 0] = _P_LOW
    return p


# ---------------------------------------------------------------------------
# 1. lam == 0 recovers the baseline exactly
# ---------------------------------------------------------------------------


def test_offset_lam_zero_returns_log_scores_and_preserves_ranking():
    """With lam forced to 0 the offset arm IS the raw arm: predicted
    values equal log(scores) exactly and the per-row ranking (argsort)
    equals the raw ranking — the baseline is recovered, not approximated.
    """
    rng = np.random.default_rng(20261001)
    scores = rng.uniform(0.05, 0.95, size=(7, 5))
    prior_probs = rng.uniform(0.01, 0.9, size=(7, 5))
    model = OffsetModel(lam=np.float64(0.0), per_species=False)
    out = model.predict(scores, prior_probs)
    assert np.array_equal(out, np.log(scores))
    assert np.array_equal(
        np.argsort(-out, axis=1), np.argsort(-scores, axis=1)
    )


# ---------------------------------------------------------------------------
# 2. determinism + shapes
# ---------------------------------------------------------------------------


def test_fit_logit_offset_deterministic_and_shaped():
    """Same data + same seed -> bit-identical lam. Scalar mode returns a
    shape-() array; per-species mode returns (k,). Both float64."""
    rng = np.random.default_rng(7)
    n, k = 60, 3
    scores = rng.uniform(0.05, 0.95, size=(n, k))
    prior_probs = rng.uniform(0.01, 0.9, size=(n, k))
    y = (rng.random((n, k)) < 0.3).astype(np.uint8)

    a = fit_logit_offset(scores, prior_probs, y, per_species=False, seed=11)
    b = fit_logit_offset(scores, prior_probs, y, per_species=False, seed=11)
    assert np.asarray(a.lam).shape == ()
    assert np.asarray(a.lam).dtype == np.float64
    assert np.array_equal(np.asarray(a.lam), np.asarray(b.lam))
    assert a.per_species is False

    c = fit_logit_offset(scores, prior_probs, y, per_species=True, seed=11)
    d = fit_logit_offset(scores, prior_probs, y, per_species=True, seed=11)
    assert np.asarray(c.lam).shape == (k,)
    assert np.asarray(c.lam).dtype == np.float64
    assert np.array_equal(np.asarray(c.lam), np.asarray(d.lam))
    assert c.per_species is True


# ---------------------------------------------------------------------------
# 3. convexity sanity — informative prior earns lam > 0, noise does not
# ---------------------------------------------------------------------------


def test_informative_prior_fits_positive_lam_noise_prior_fits_near_zero():
    """On the residual-balanced fixture an informative prior (high where
    y=1) must earn a clearly positive scalar lam, while a stratified
    noise prior must fit |lam| < 0.1 — the tolerance is loose against
    second-order curvature effects but an order of magnitude below the
    informative lam, so the two cannot be confused.
    """
    y, scores = _balanced_y_and_scores()

    informative = fit_logit_offset(
        scores, _informative_prior(y), y, per_species=False, seed=0
    )
    noise = fit_logit_offset(
        scores, _noise_prior(y), y, per_species=False, seed=0
    )
    lam_inf = float(np.asarray(informative.lam))
    lam_noise = float(np.asarray(noise.lam))
    assert lam_inf > 0.2
    assert abs(lam_noise) < 0.1
    assert lam_inf > lam_noise + 0.2


# ---------------------------------------------------------------------------
# 4. per-species mode separates a helpful prior from a useless one
# ---------------------------------------------------------------------------


def test_per_species_fit_separates_helpful_and_noise_species():
    """Species A gets the informative prior, species B the stratified
    noise prior; the per-species fit must give lam_A > lam_B — the whole
    point of per-species mode is that one shared lam cannot serve both.
    """
    y, scores = _balanced_y_and_scores()
    y2 = np.hstack([y, y])
    scores2 = np.hstack([scores, scores])
    prior2 = np.hstack([_informative_prior(y), _noise_prior(y)])

    model = fit_logit_offset(scores2, prior2, y2, per_species=True, seed=3)
    lam = np.asarray(model.lam)
    assert lam.shape == (2,)
    assert lam[0] > lam[1]
    assert lam[0] > 0.2
    assert abs(lam[1]) < 0.1


# ---------------------------------------------------------------------------
# lam at the search bound must raise, never truncate silently
# ---------------------------------------------------------------------------


def test_fit_logit_offset_raises_when_lam_hits_search_bound():
    """A fixture engineered to want a huge |lam| — near-constant scores
    with an extremely informative (anti-correlated, near-unit) prior —
    drives the bounded minimizer into the ±10 bound. Returning the
    truncated lam silently would poison every cache built from it, so
    the fit must refuse loudly instead."""
    n = 60
    y = np.concatenate([np.ones(30), np.zeros(30)])[:, None]
    scores = np.full((n, 1), 0.5)
    # Prior is LOWER on positives by a factor e^0.7 and near 1 on
    # negatives: log p is tiny in magnitude, so BCE keeps improving as
    # lam -> -inf and the minimizer pins at the -10 bound.
    prior = np.where(y == 1, np.exp(-0.7), 0.999)
    with pytest.raises(ValueError, match="bound"):
        fit_logit_offset(scores, prior, y, per_species=False, seed=0)
    # Per-species mode guards each column's fit the same way.
    with pytest.raises(ValueError, match="bound"):
        fit_logit_offset(scores, prior, y, per_species=True, seed=0)


# ---------------------------------------------------------------------------
# raw-logits guard — scores entering log must be probabilities
# ---------------------------------------------------------------------------


def test_fit_logit_offset_rejects_raw_logits():
    """A score outside [0, 1] is a raw uncalibrated logit; logging it
    would violate the carried obligation that only un-thresholded
    sigmoid/softmax outputs may enter a log. Refused loudly."""
    y, scores = _balanced_y_and_scores()
    raw = scores.copy()
    raw[0, 0] = -2.5
    with pytest.raises(ValueError, match="raw"):
        fit_logit_offset(
            raw, _informative_prior(y), y, per_species=False, seed=0
        )


# ---------------------------------------------------------------------------
# 8 (offset part). float discipline — float64 out, hard zeros floored
# ---------------------------------------------------------------------------


def test_offset_outputs_are_float64_and_finite_under_hard_zero_prior():
    """predict output is float64 (never float32) and a hard-zero prior
    probability is floored with the imported FLOOR before the log — no
    -inf can veto a detection."""
    y, scores = _balanced_y_and_scores()
    prior = _informative_prior(y)
    model = fit_logit_offset(
        scores.astype(np.float32),
        prior.astype(np.float32),
        y,
        per_species=False,
        seed=0,
    )
    assert np.asarray(model.lam).dtype == np.float64
    zero_prior = prior.copy()
    zero_prior[0, 0] = 0.0
    out = model.predict(scores, zero_prior)
    assert out.dtype == np.float64
    assert np.all(np.isfinite(out))


# ---------------------------------------------------------------------------
# 5. guard_training_split + prepare_training
# ---------------------------------------------------------------------------


def _cache_table(
    splits: list[str],
    codes: list[str | None],
    *,
    n_classes: int = 3,
    d: int = 4,
    seed: int = 0,
) -> pa.Table:
    """An in-memory table with the cache's column contract (filepath /
    cell / week / split / ebird_code / embedding / logits)."""
    rng = np.random.default_rng(seed)
    n = len(splits)
    logits = rng.uniform(0.05, 0.95, size=(n, n_classes))
    embeddings = rng.normal(size=(n, d))
    return pa.table(
        {
            "filepath": pa.array([f"r{i}.ogg" for i in range(n)], pa.string()),
            "cell": pa.array(["c"] * n, pa.string()),
            "week": pa.array([1] * n, pa.int8()),
            "split": pa.array(splits, pa.string()),
            "ebird_code": pa.array(codes, pa.string()),
            "embedding": pa.array(
                embeddings.astype(np.float32).tolist(), pa.list_(pa.float32())
            ),
            "logits": pa.array(
                logits.astype(np.float32).tolist(), pa.list_(pa.float32())
            ),
        }
    )


def test_guard_training_split_raises_naming_offending_splits():
    """Any row whose split does not start with 'train' (here a test_5s
    soundscape row) must raise, and the error must NAME the offending
    split values — the train/test shift is the spec's mechanism and a
    silent soundscape row in training would break it invisibly."""
    table = _cache_table(
        ["train", "test_5s", "train_xc"], ["sp1", None, "sp2"]
    )
    with pytest.raises(ValueError, match="test_5s"):
        guard_training_split(table)


def test_guard_training_split_passes_all_train_rows():
    table = _cache_table(["train", "train_xc"], ["sp1", "sp2"])
    guard_training_split(table)  # must not raise


def test_prepare_training_end_to_end_on_train_cache_view():
    """prepare_training runs the guard, builds Y from the SINGLE-label
    ebird_code column (focal caches carry one label per recording, not
    ebird_code_multilabel), gathers scores by code, and returns
    parallel (embeddings, scores, y, rows, stats) arrays."""
    table = _cache_table(
        ["train", "train", "train"],
        ["spB", "spA", "spB"],
        n_classes=3,
        d=4,
        seed=5,
    )
    view = CacheView(table=table, logits_are_raw=False, n_classes=3)
    class_idx = {"spA": 0, "spB": 1, "spC": 2}

    embeddings, scores, y, rows, stats = prepare_training(view, class_idx)

    assert embeddings.shape == (3, 4)
    assert embeddings.dtype == np.float64
    # scoreable = sorted present codes = [spA, spB] -> k == 2
    assert scores.shape == (3, 2)
    assert scores.dtype == np.float64
    assert y.shape == (3, 2)
    # Row labels: spB, spA, spB against columns [spA, spB].
    assert y.tolist() == [[0, 1], [1, 0], [0, 1]]
    assert rows == ["spA", "spB"]
    # Fully labeled, fully scoreable: stats report zeros.
    assert stats.n_unlabeled == 0
    assert stats.unscoreable == []
    # Gather is by code: scores column j is logits column class_idx[code].
    logits = np.asarray(table.column("logits").to_pylist())
    np.testing.assert_allclose(scores[:, 0], logits[:, 0], rtol=1e-6)
    np.testing.assert_allclose(scores[:, 1], logits[:, 1], rtol=1e-6)


def test_prepare_training_reports_unlabeled_and_unscoreable_rows():
    """A None in ebird_code becomes an ALL-NEGATIVE y row (a negative,
    never a dropped row) and is COUNTED in stats.n_unlabeled; a labeled
    code absent from class_idx is reported in stats.unscoreable instead
    of being silently discarded."""
    table = _cache_table(
        ["train", "train", "train"],
        ["spA", None, "spZ"],  # spZ not in class_idx
        n_classes=3,
    )
    view = CacheView(table=table, logits_are_raw=False, n_classes=3)
    class_idx = {"spA": 0, "spB": 1}

    embeddings, scores, y, rows, stats = prepare_training(view, class_idx)

    assert rows == ["spA"]
    # Row 1 (None label) and row 2 (unscoreable spZ) are all-negative,
    # present, not dropped: n rows preserved.
    assert y.shape == (3, 1)
    assert y.tolist() == [[1], [0], [0]]
    assert stats.n_unlabeled == 1
    assert stats.unscoreable == ["spZ"]


def test_prepare_training_rejects_non_train_cache_view():
    table = _cache_table(["train", "test_5s"], ["spA", None])
    view = CacheView(table=table, logits_are_raw=False, n_classes=3)
    with pytest.raises(ValueError, match="test_5s"):
        prepare_training(view, {"spA": 0})


# ---------------------------------------------------------------------------
# 7. the sklearn-multilabel claim, MEASURED and pinned
# ---------------------------------------------------------------------------


def test_sklearn_mlpclassifier_supports_multilabel_indicator_y():
    """The brief's measure-don't-trust pin: MLPClassifier fit on a
    3-sample binary indicator matrix must accept it as multilabel and
    predict_proba must return ONE (n, k) ndarray of per-class logistic
    probabilities (NOT a k-long list of (n, 2) arrays, which is what the
    one-vs-rest wrappers return). Verified against the installed sklearn
    (1.9.1); if this test ever fails after an sklearn upgrade,
    fit_mlp_head needs the one-vs-rest fallback the brief names.
    """
    from sklearn.neural_network import MLPClassifier

    X = np.random.default_rng(0).normal(size=(3, 4))
    Y = np.array([[1, 0], [0, 1], [1, 1]])
    clf = MLPClassifier(hidden_layer_sizes=(8,), max_iter=5, random_state=0)
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # convergence noise at max_iter=5
        clf.fit(X, Y)
    proba = clf.predict_proba(X)
    assert isinstance(proba, np.ndarray)
    assert proba.shape == (3, 2)
    assert proba.dtype == np.float64
    assert clf.n_outputs_ == 2
    # Multilabel semantics: per-class independent sigmoids, so rows need
    # NOT sum to 1 (that would be the multiclass softmax head).
    assert clf.out_activation_ == "logistic"
    assert np.all((proba >= 0.0) & (proba <= 1.0))


def test_sklearn_mlpclassifier_proba_columns_track_their_own_y_columns():
    """Column-correspondence pin: fit on 2 label columns with DISJOINT
    signal (y column 0 depends only on features 0-1, column 1 only on
    features 2-3) and assert each predict_proba column separates ITS OWN
    y column far better than the other. A swapped column order in
    predict_proba collapses both own-column margins and fails loudly.
    (Simulated swap check: running these asserts on proba[:, ::-1]
    fails — the swapped own-column margins collapse to ~0.11, far
    below the 0.5 threshold.)"""
    from sklearn.neural_network import MLPClassifier

    rng = np.random.default_rng(42)
    n = 80
    X = rng.normal(size=(n, 4))
    Y = np.stack(
        [(X[:, 0] + X[:, 1] > 0), (X[:, 2] + X[:, 3] > 0)], axis=1
    ).astype(np.uint8)
    clf = MLPClassifier(
        hidden_layer_sizes=(16,), max_iter=400, random_state=0
    )
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        clf.fit(X, Y)
    proba = clf.predict_proba(X)

    def margin(col: np.ndarray, y: np.ndarray) -> float:
        """Mean proba on positives minus mean on negatives."""
        return float(col[y == 1].mean() - col[y == 0].mean())

    # Each proba column must track its own label column...
    assert margin(proba[:, 0], Y[:, 0]) > 0.5
    assert margin(proba[:, 1], Y[:, 1]) > 0.5
    # ...and carry (near) no signal for the OTHER column — the strict
    # inequality pair is what a column swap cannot satisfy.
    assert margin(proba[:, 0], Y[:, 0]) > margin(proba[:, 1], Y[:, 0]) + 0.3
    assert margin(proba[:, 1], Y[:, 1]) > margin(proba[:, 0], Y[:, 1]) + 0.3


# ---------------------------------------------------------------------------
# 6. MLP head — separable fixture, shapes, determinism
# ---------------------------------------------------------------------------


def _mlp_fixture(seed: int = 20261001):
    """n=40, d=8, k=2, linearly separable per class by construction: each
    class's positives get +2.0 on its own pair of embedding dims."""
    rng = np.random.default_rng(seed)
    n, d, k = 40, 8, 2
    y = (rng.random((n, k)) < 0.4).astype(np.uint8)
    embeddings = rng.normal(size=(n, d))
    embeddings[:, 0:2] += 2.0 * y[:, [0]]
    embeddings[:, 2:4] += 2.0 * y[:, [1]]
    prior_probs = rng.uniform(0.05, 0.9, size=(n, k))
    return embeddings, prior_probs, y


def test_mlp_fit_beats_uniform_guess_on_its_own_training_data():
    """Smoke-level sanity, not a benchmark: on the separable fixture the
    fitted head's train-set cmAP must beat the positive-rate baseline
    (the AP of a constant/uniform scorer is the prevalence)."""
    from birdology.phase_b.metrics import cmap

    embeddings, prior_probs, y = _mlp_fixture()
    model = fit_mlp_head(embeddings, prior_probs, y, seed=0, max_iter=300)
    pred = model.predict(embeddings, prior_probs)
    trained_cmap, n_scored = cmap(pred, y)
    assert n_scored == 2
    prevalence_ap = float(y.mean())  # constant scores -> AP == prevalence
    assert trained_cmap > prevalence_ap + 0.1


def test_mlp_predict_shape_dtype_and_seed_determinism():
    """predict returns (n, k) float64; same seed -> identical
    predictions; a different seed -> different predictions (the seed
    really reaches the weight init)."""
    embeddings, prior_probs, y = _mlp_fixture()
    a = fit_mlp_head(embeddings, prior_probs, y, seed=1, max_iter=50)
    b = fit_mlp_head(embeddings, prior_probs, y, seed=1, max_iter=50)
    c = fit_mlp_head(embeddings, prior_probs, y, seed=2, max_iter=50)
    assert isinstance(a, MLPModel)
    pa_ = a.predict(embeddings, prior_probs)
    pb = b.predict(embeddings, prior_probs)
    pc = c.predict(embeddings, prior_probs)
    assert pa_.shape == (40, 2)
    assert pa_.dtype == np.float64
    assert np.array_equal(pa_, pb)
    assert not np.array_equal(pa_, pc)


def test_mlp_refuses_nan_prior_with_named_error_not_sklearn_crash():
    """A NaN prior probability reaching the learned arms' feature
    assembly must raise OUR error naming the count — never sklearn's
    'Input X contains NaN' from deep inside MLPClassifier.fit (the
    fleet's r5+marginal crash signature). The floor-then-log path
    cannot repair it: np.maximum(nan, FLOOR) is nan (measured), so
    without this refusal the NaN rides into sklearn silently."""
    embeddings, prior_probs, y = _mlp_fixture()
    prior_probs = prior_probs.copy()
    prior_probs[3, 1] = np.nan
    with pytest.raises(ValueError, match=r"NaN"):
        try:
            fit_mlp_head(embeddings, prior_probs, y, seed=0, max_iter=20)
        except ValueError as exc:
            assert "Input X contains NaN" not in str(exc), (
                "the NaN reached sklearn instead of being refused at "
                "the feature-assembly boundary"
            )
            assert "prior" in str(exc)
            raise


def test_offset_fit_refuses_nan_prior_with_named_error():
    """The offset arm shares the same floored-log feature path; a NaN
    prior must be refused there too, not silently fitted on."""
    y, scores = _balanced_y_and_scores()
    prior = _informative_prior(y).copy()
    prior[0, 0] = np.nan
    with pytest.raises(ValueError, match=r"NaN.*prior|prior.*NaN"):
        fit_logit_offset(scores, prior, y, per_species=False, seed=0)


def test_mlp_fit_early_stops_on_a_held_out_validation_fraction():
    """TIMEOUT fix: fit_mlp_head must enable sklearn early stopping —
    a held-out validation_fraction scored each epoch, halting after
    n_iter_no_change epochs without improvement. Pinned behaviorally:
    on the separable fixture with a huge max_iter the fit stops FAR
    before the cap and carries validation_scores_ (the attribute only
    an early-stopping fit has). Science caveat pinned in the next
    tests: the held-out fraction slightly changes the fit vs training
    on all rows, and determinism is preserved because sklearn seeds
    the validation split from random_state (= our seed).

    NOTE: sklearn sets validation_scores_ = None when early stopping is
    OFF (measured, 1.9.1), so the assertion is on the scores LIST being
    populated — hasattr alone passes either way and pins nothing."""
    embeddings, prior_probs, y = _mlp_fixture()
    model = fit_mlp_head(embeddings, prior_probs, y, seed=0, max_iter=5000)
    scores = getattr(model.clf, "validation_scores_", None)
    assert scores is not None and len(scores) > 0, (
        "validation_scores_ is empty/None: early stopping is not enabled, "
        "so every fleet fit runs to tol/max_iter on the full training "
        "loss and 12/48 shards hit the 12h wall"
    )
    assert model.clf.n_iter_ < 5000


def test_mlp_fit_with_early_stopping_stays_deterministic_per_seed():
    """The validation split is drawn with random_state=seed inside
    sklearn, so early stopping must NOT break the same-machine
    determinism contract: same data + same seed -> bit-identical
    predictions."""
    embeddings, prior_probs, y = _mlp_fixture()
    a = fit_mlp_head(embeddings, prior_probs, y, seed=3, max_iter=200)
    b = fit_mlp_head(embeddings, prior_probs, y, seed=3, max_iter=200)
    assert np.array_equal(
        a.predict(embeddings, prior_probs), b.predict(embeddings, prior_probs)
    )


def test_mlp_fit_logs_wall_clock_duration(caplog):
    """Each MLP fit logs its wall-clock duration — the fleet's only way
    to see WHICH fit groups eat the 12h budget before the scheduler
    kills the shard with no per-fit evidence."""
    import logging

    embeddings, prior_probs, y = _mlp_fixture()
    with caplog.at_level(logging.INFO, logger="birdology.phase_b.learned"):
        fit_mlp_head(embeddings, prior_probs, y, seed=0, max_iter=20)
    timed = [r for r in caplog.records if "fit" in r.message and "s" in r.message]
    assert timed, "no per-fit duration log record was emitted"


def test_mlp_handles_hard_zero_prior_finitely():
    """A hard-zero prior probability must be floored before the log that
    builds the [embeddings ‖ log p] feature block — fit and predict both
    stay finite."""
    embeddings, prior_probs, y = _mlp_fixture()
    prior_probs = prior_probs.copy()
    prior_probs[0, 0] = 0.0
    model = fit_mlp_head(embeddings, prior_probs, y, seed=0, max_iter=20)
    out = model.predict(embeddings, prior_probs)
    assert np.all(np.isfinite(out))
    assert out.dtype == np.float64
