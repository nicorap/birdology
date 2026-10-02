"""The two learned cheap arms of Phase B: logit offset and MLP head.

Both arms train on CACHED arrays (CPU, seconds-to-minutes) — no audio, no
encoder weights, no network. They consume the same gathered scores /
prior probabilities / label matrices the rescoring arm consumes, and obey
the same carried obligations:

- Scores entering any ``log()`` are un-thresholded sigmoid/softmax
  outputs — never clipped or thresholded first. Hard zeros are floored
  with ``FLOOR`` IMPORTED from ``birdology.prior.tensor`` (via the
  rescoring module's re-export), never a re-defined constant.
- Weeks, where consumed, arrive pre-folded through
  ``birdology.prior.indexing.iso_week`` (the cache's ``week`` column is
  already folded by the Phase A writer).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import pyarrow as pa
from scipy.optimize import minimize_scalar

from birdology.phase_b.cache import CacheView, align_labels
from birdology.phase_b.metrics import gather_scores
from birdology.phase_b.rescoring import FLOOR

logger = logging.getLogger(__name__)

#: lam search interval for the 1-D BCE minimization. The objective is
#: convex in lam (it is a logistic regression in the single feature
#: log p with fixed offset log s), so any bracket containing the minimum
#: finds it; +/-10 is far beyond any plausible prior exponent. A fit
#: that PINS at a bound is refused loudly (see _fit_one): silently
#: returning the truncated lam would poison every downstream cache.
_LAM_BOUNDS = (-10.0, 10.0)

#: |fitted lam - bound| below this means the minimizer pinned at the
#: bound rather than finding an interior minimum. The minimizer runs
#: with xatol well below this tolerance so a genuine interior optimum
#: near (but not at) the bound is not misflagged.
_LAM_BOUND_TOL = 1e-6


def _checked_log_scores(scores: np.ndarray) -> np.ndarray:
    """log(scores) after verifying scores are probabilities.

    A value outside [0, 1] is a raw uncalibrated logit — logging it
    would violate the carried obligation that only un-thresholded
    sigmoid/softmax outputs enter a log. Hard zeros (which a saturated
    sigmoid can emit) are floored with the imported FLOOR, never
    clipped away.
    """
    s = np.asarray(scores, dtype=np.float64)
    if s.size and (s.min() < 0.0 or s.max() > 1.0):
        raise ValueError(
            f"scores outside [0, 1] (min={s.min()}, max={s.max()}) look "
            "like raw uncalibrated logits; the learned arms require "
            "sigmoid/softmax probabilities before any log()"
        )
    return np.log(np.maximum(s, FLOOR))


def _log_floored(probs: np.ndarray) -> np.ndarray:
    """log of probabilities floored with the imported FLOOR — a hard 0.0
    can never produce a -inf that vetoes a detection.

    A NaN is REFUSED, not floored: ``np.maximum(nan, FLOOR)`` is nan
    (NaN loses every comparison), so without this guard a NaN prior
    rides through the floor, through the log, and into sklearn, which
    crashes a whole fleet cell with its own message ('Input X contains
    NaN') naming nothing about the prior. In this pipeline a NaN prior
    probability has NO defined meaning — ``tensor._validate_p`` forbids
    non-finite values at write time and FLOOR is the one documented
    'no evidence' value — so a NaN here is a corrupt artifact or a
    control built from one, and repairing it to FLOOR would silently
    convert corruption into 'no evidence'. The error counts and
    locates the NaNs so the operator can find the artifact.
    """
    p = np.asarray(probs, dtype=np.float64)
    nan_mask = np.isnan(p)
    if nan_mask.any():
        n_nan = int(nan_mask.sum())
        rows, cols = (
            np.nonzero(nan_mask) if p.ndim == 2 else (np.nonzero(nan_mask)[0], None)
        )
        where = (
            f"first at (row={int(rows[0])}, col={int(cols[0])})"
            if cols is not None and len(rows)
            else f"first at index {int(rows[0])}" if len(rows) else ""
        )
        raise ValueError(
            f"{n_nan} NaN prior probability value(s) in the learned arms' "
            f"feature path ({where} of shape {p.shape}); NaN has no "
            "defined meaning for this prior — the artifact forbids "
            "non-finite p at write time and FLOOR is the documented "
            "'no evidence' value — so this is a corrupt prior artifact "
            "or a control built from one. Refusing rather than flooring: "
            "a repaired NaN would be indistinguishable from real "
            "'no evidence' for every row downstream"
        )
    return np.log(np.maximum(p, FLOOR))


@dataclass(frozen=True)
class OffsetModel:
    """The fitted logit-offset arm: ``log s + lam * log(floored p)``.

    ``lam`` is a shape-() float64 array in scalar mode or
    ``(n_scoreable,)`` in per-species mode (broadcast over columns).
    """

    lam: np.ndarray
    per_species: bool

    def predict(
        self, scores: np.ndarray, prior_probs: np.ndarray
    ) -> np.ndarray:
        """``log scores + lam * log(floored prior)`` — float64 out, same
        float discipline as :func:`birdology.phase_b.rescoring.rescore`:
        prior floored with the imported FLOOR before the log."""
        return _checked_log_scores(scores) + np.asarray(
            self.lam, dtype=np.float64
        ) * _log_floored(prior_probs)


def _bce(log_s: np.ndarray, log_p: np.ndarray, lam: float, y: np.ndarray) -> float:
    """Mean binary cross-entropy of ``sigmoid(log_s + lam * log_p)``
    against *y*, computed via the numerically stable logits form
    ``softplus(z) - y*z``."""
    z = log_s + lam * log_p
    return float(np.mean(np.logaddexp(0.0, z) - y * z))


def fit_logit_offset(
    scores: np.ndarray,
    prior_probs: np.ndarray,
    y: np.ndarray,
    per_species: bool,
    seed: int,
) -> OffsetModel:
    """Fit lam by minimizing BCE of ``sigmoid(log s + lam * log p)``.

    Scalar mode: one bounded 1-D minimization over all (row, species)
    cells. Per-species mode: an independent 1-D fit per column.

    DETERMINISM: the fit is deterministic given the data alone. The 1-D
    objective is convex in lam (a logistic regression in the single
    feature log p with fixed offset log s), so bounded scalar
    minimization needs no random initialization — *seed* is accepted for
    interface uniformity with :func:`fit_mlp_head` and enters nothing.

    TRUST BOUNDARY: array inputs are trusted and unguarded — derive
    training arrays via :func:`prepare_training`, which enforces
    :func:`guard_training_split`; a direct-array call bypasses that
    guard by design.

    A fitted lam within ``_LAM_BOUND_TOL`` of the ``_LAM_BOUNDS`` edge
    raises ValueError: the true optimum lies at or beyond the bracket,
    and silently returning the truncated value would poison every cache
    built from it.
    """
    del seed  # convex 1-D objective: no stochastic init to seed.
    log_s = _checked_log_scores(scores)
    log_p = _log_floored(prior_probs)
    y_f = np.asarray(y, dtype=np.float64)

    def _fit_one(ls: np.ndarray, lp: np.ndarray, yy: np.ndarray) -> float:
        result = minimize_scalar(
            lambda lam: _bce(ls, lp, lam, yy),
            bounds=_LAM_BOUNDS,
            method="bounded",
            # xatol far below _LAM_BOUND_TOL: a pinned fit lands within
            # the tolerance of the bound instead of stopping ~1e-5 away
            # (scipy's default xatol) and slipping past the check.
            options={"xatol": 1e-8},
        )
        lam = float(result.x)
        if min(abs(lam - b) for b in _LAM_BOUNDS) < _LAM_BOUND_TOL:
            raise ValueError(
                f"fitted lam {lam!r} is pinned at the search bound "
                f"{_LAM_BOUNDS}; the optimum lies outside the bracket, "
                "and silently truncating it would poison every cache "
                "built from this fit — widen _LAM_BOUNDS or inspect the "
                "degenerate scores/prior that produced it"
            )
        return lam

    if per_species:
        lam = np.array(
            [
                _fit_one(log_s[:, j], log_p[:, j], y_f[:, j])
                for j in range(log_s.shape[1])
            ],
            dtype=np.float64,
        )
    else:
        lam = np.float64(_fit_one(log_s, log_p, y_f))
    return OffsetModel(lam=np.asarray(lam, dtype=np.float64), per_species=per_species)


@dataclass(frozen=True)
class MLPModel:
    """The fitted MLP arm: an sklearn MLPClassifier over the feature
    block ``[embeddings ‖ log(floored prior_probs)]``.

    MEASURED FACT (sklearn 1.9.1, pinned by
    ``test_sklearn_mlpclassifier_supports_multilabel_indicator_y``):
    MLPClassifier accepts a binary indicator matrix as multilabel y and
    its ``predict_proba`` returns ONE ``(n, k)`` float64 ndarray of
    independent per-class logistic probabilities (``out_activation_ ==
    "logistic"``) — no one-vs-rest column loop needed.
    """

    clf: Any  # sklearn MLPClassifier; untyped to keep sklearn optional
    n_classes: int

    def predict(
        self, embeddings: np.ndarray, prior_probs: np.ndarray
    ) -> np.ndarray:
        """Per-class probabilities ``(n, k)`` float64 for new rows,
        built from the same feature block as training."""
        features = _mlp_features(embeddings, prior_probs)
        proba = self.clf.predict_proba(features)
        out = np.asarray(proba, dtype=np.float64)
        if out.shape != (features.shape[0], self.n_classes):
            raise ValueError(
                f"predict_proba returned shape {out.shape}, expected "
                f"({features.shape[0]}, {self.n_classes}) — the sklearn "
                "multilabel contract this model was built on has changed"
            )
        return out


def _mlp_features(
    embeddings: np.ndarray, prior_probs: np.ndarray
) -> np.ndarray:
    """``[embeddings ‖ log(floored prior_probs)]`` as float64 — the
    prior enters as log probabilities, floored with the imported FLOOR
    so a hard 0.0 cannot inject a -inf feature."""
    emb = np.asarray(embeddings, dtype=np.float64)
    return np.hstack([emb, _log_floored(prior_probs)])


def fit_mlp_head(
    embeddings: np.ndarray,
    prior_probs: np.ndarray,
    y: np.ndarray,
    seed: int,
    hidden: int = 256,
    max_iter: int = 200,
    validation_fraction: float = 0.1,
    n_iter_no_change: int = 10,
) -> MLPModel:
    """Fit the MLP arm on cached arrays (CPU, sklearn).

    sklearn's MLPClassifier handles the multilabel indicator *y*
    natively (measured fact — see :class:`MLPModel`). ``random_state=
    seed`` makes the fit deterministic given (data, seed): the seed
    enters the weight initialization, the minibatch shuffling AND the
    early-stopping validation split.

    EARLY STOPPING (fleet TIMEOUT fix, documented science): training
    holds out ``validation_fraction`` of the rows, scores them each
    epoch, and stops after ``n_iter_no_change`` epochs without
    improvement — instead of grinding the full training loss to ``tol``
    or ``max_iter``, which cost 12 of 48 fleet shards their 12h wall.
    This CHANGES THE FIT slightly relative to no-early-stopping: the
    model trains on ~90% of rows and halts at the best validation
    score (sklearn restores the best-scoring weights). That is a
    regularization choice, not a silent repair — it is pinned by
    ``test_mlp_fit_early_stops_on_a_held_out_validation_fraction``,
    and determinism per (data, seed) is preserved because the split is
    drawn from ``random_state`` (pinned by
    ``test_mlp_fit_with_early_stopping_stays_deterministic_per_seed``).
    Cross-seed spread already absorbs the split's randomness: the seed
    axis exists exactly to measure fit variance.

    Each fit logs its wall-clock duration and iteration count at INFO —
    the fleet's per-fit evidence of where a 12h budget goes.

    TRUST BOUNDARY: array inputs are trusted and unguarded — derive
    training arrays via :func:`prepare_training`, which enforces
    :func:`guard_training_split`; a direct-array call bypasses that
    guard by design.
    """
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.neural_network import MLPClassifier

    features = _mlp_features(embeddings, prior_probs)
    y_arr = np.asarray(y)
    clf = MLPClassifier(
        hidden_layer_sizes=(hidden,),
        max_iter=max_iter,
        random_state=seed,
        early_stopping=True,
        validation_fraction=validation_fraction,
        n_iter_no_change=n_iter_no_change,
    )
    import time
    import warnings

    t0 = time.perf_counter()
    with warnings.catch_warnings():
        # A small max_iter stopping before convergence is a caller
        # choice, not a defect worth polluting sweep logs over. ONLY
        # ConvergenceWarning is suppressed — anything else sklearn
        # emits must stay visible.
        warnings.simplefilter("ignore", category=ConvergenceWarning)
        clf.fit(features, y_arr)
    logger.info(
        "mlp fit: %.1fs, %d/%d iters (n=%d, d=%d, k=%d, hidden=%d, seed=%d)",
        time.perf_counter() - t0,
        clf.n_iter_,
        max_iter,
        features.shape[0],
        features.shape[1],
        y_arr.shape[1],
        hidden,
        seed,
    )
    return MLPModel(clf=clf, n_classes=y_arr.shape[1])


def guard_training_split(table: pa.Table) -> None:
    """Refuse any table carrying non-training rows.

    Raises ValueError unless EVERY row's ``split`` value starts with
    ``"train"`` — the train/test shift is the spec's mechanism, so a
    soundscape (test) cache must never be fit on. The error NAMES the
    offending split values so the wrong cache dir is identifiable from
    the message alone.
    """
    if "split" not in table.column_names:
        raise ValueError(
            "table has no 'split' column; cannot prove these rows are "
            f"training rows (columns: {table.column_names})"
        )
    splits = table.column("split").to_pylist()
    offending = sorted(
        {s if s is not None else "<null>" for s in splits}
        - {s for s in splits if s is not None and s.startswith("train")}
    )
    if offending:
        raise ValueError(
            f"non-training split values {offending} in table; learned "
            "arms fit on train-split focal caches only — a test/"
            "soundscape row in training would break the train/test shift"
        )


@dataclass(frozen=True)
class TrainingStats:
    """What prepare_training coerced or set aside, reported instead of
    silently absorbed.

    - ``n_unlabeled``: rows whose *label_col* was None. Each becomes an
      ALL-NEGATIVE y row (a negative example for every scoreable
      species), NOT a dropped row — the row count is preserved.
    - ``unscoreable``: sorted label codes present in the cache but
      absent from *class_idx*; their rows also end up all-negative for
      the scoreable columns.
    """

    n_unlabeled: int
    unscoreable: list[str]


def prepare_training(
    cache_view: CacheView,
    class_idx: dict[str, int],
    label_col: str = "ebird_code",
) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[str], TrainingStats]:
    """Guard + gather in one place: training arrays from a CacheView.

    Returns ``(embeddings, scores, y, scoreable, stats)``:

    - ``embeddings``: ``(n, d)`` float64 from the cache embedding column.
    - ``scores``: ``(n, k)`` float64 probabilities gathered BY CODE in
      scoreable order (sigmoid applied iff the cache declares raw
      logits, so downstream logs always see probabilities).
    - ``y``: ``(n, k)`` uint8 built from the SINGLE-label *label_col*
      (focal caches carry one label per recording in ``ebird_code``, NOT
      ``ebird_code_multilabel``) via the same by-code alignment as
      :func:`birdology.phase_b.cache.align_labels`. A None label makes
      an ALL-NEGATIVE row (see :class:`TrainingStats`), never a dropped
      row.
    - ``scoreable``: the sorted species codes forming the column axis.
    - ``stats``: a :class:`TrainingStats` counting None-label rows and
      naming unscoreable codes, so callers can log what was coerced.

    ROW ORDER: all row-indexed outputs (``embeddings``, ``scores``,
    ``y``) are row-parallel to ``cache_view.table`` order — row i of
    each array is row i of the table. Task 6 relies on this to zip the
    table's ``cell`` / ``week`` columns against these arrays for
    ``prior_for_rows``.

    Runs :func:`guard_training_split` first — a fit prepared THROUGH
    THIS FUNCTION can never see a non-train row. Direct-array calls to
    ``fit_logit_offset`` / ``fit_mlp_head`` bypass the guard by design
    (arrays carry no split column to check).
    """
    table = cache_view.table
    guard_training_split(table)
    if label_col not in table.column_names:
        raise ValueError(
            f"label column {label_col!r} not in table; available "
            f"columns: {table.column_names}"
        )
    # Wrap the single-label column as one-element lists and reuse
    # align_labels' by-code contract (empty label -> all-zero row).
    singles = table.column(label_col).to_pylist()
    as_lists = pa.array(
        [[code] if code is not None else [] for code in singles],
        pa.list_(pa.string()),
    )
    labeled = table.append_column("_single_label_as_list", as_lists)
    y, scoreable, unscoreable = align_labels(
        labeled, class_idx, label_col="_single_label_as_list"
    )
    stats = TrainingStats(
        n_unlabeled=sum(1 for code in singles if code is None),
        unscoreable=list(unscoreable),
    )

    logits = np.asarray(
        table.column("logits").to_pylist(), dtype=np.float64
    )
    gathered = gather_scores(logits, class_idx, scoreable).astype(np.float64)
    if cache_view.logits_are_raw:
        # Raw uncalibrated logits -> un-thresholded sigmoid probabilities
        # (the only form allowed to enter a log downstream).
        gathered = 1.0 / (1.0 + np.exp(-gathered))
    embeddings = np.asarray(
        table.column("embedding").to_pylist(), dtype=np.float64
    )
    return embeddings, gathered, y, scoreable, stats
