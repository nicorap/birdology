"""The Phase B sweep driver and its results contract.

Every number the paper reports flows through `run_cell`'s row dict and
`write_rows`' parquet schema. The driver composes Tasks 1-5 — cache,
metrics, rescoring, learned arms — into the factorial grid, under the
carried obligations:

* RESUME GRANULARITY IS THE FULL 5-SEED CELL. `run_cell` executes every
  seed and returns ONE row; a partial seed set is never materialised as a
  row, so resume can never manufacture a 4-seed mean. The refusals on a
  non-5-distinct seed tuple enforce the same ruling at the front door.
* The control-variant vocabulary is IMPORTED from
  `rescoring.CONTROL_VARIANTS`, never re-spelled: a hand-typed
  'shuffled_space' would silently match nothing.
* Scores entering `rescore` / `OffsetModel.predict` are un-thresholded
  sigmoid/softmax probabilities, with the sigmoid applied EXACTLY ONCE
  iff the cache declares raw logits — decided by the cache's metadata
  flag, never by inspecting values. Hard zeros are floored upstream (the
  imported FLOOR); nothing here clips.
* Weeks come ONLY off the cache's `week` column (already iso_week-folded
  by the manifest); they are never recomputed from dates.
* `prepare_training`'s arrays are row-parallel to the cache table, so
  the table's `cell`/`week` columns zip directly into `prior_for_rows`.
* Nothing gates on cross-machine bit-identity of MLP outputs; the
  determinism contract is same-machine only.
* `prior_coverage_failures_train` / `prior_coverage_failures_eval`
  count COVERAGE failures (cells/species the prior could not answer,
  i.e. `PriorLookup.n_floored`) on the training and eval lookups
  SEPARATELY — rescoring performs no training lookup, so its train
  column is null, and the two columns are never summed into one number
  that would compare different quantities across methods.
* EVAL LABELS NEVER COME FROM THE CACHE TABLE. A real Phase A cache
  carries `filepath/cell/week/ebird_code/split/embedding[/logits]` and
  NO `ebird_code_multilabel` column; the multilabel ground truth lives
  in the test set's metadata parquet and is joined to cache rows BY
  FILEPATH (the exact contract of `repro_gate.score_pair`), refusing —
  with named examples — any cache recording the metadata does not
  cover.
* `rescore` at tau=0 ALIASES its input; score arrays are treated as
  read-only everywhere in this module.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy import stats

from birdology.phase_b.cache import CacheView, align_labels
from birdology.phase_b.learned import (
    OffsetModel,
    fit_logit_offset,
    fit_mlp_head,
    prepare_training,
)
from birdology.phase_b.metrics import (
    auroc_macro,
    cmap,
    gather_scores,
    stratified,
    top1,
)
from birdology.phase_b.rescoring import (
    CONTROL_VARIANTS,
    control_prior,
    prior_for_rows,
    rescore,
    train_marginal_from_prior,
)
from birdology.prior.tensor import FLOOR, PriorTensor

logger = logging.getLogger(__name__)

#: Reverse of CONTROL_VARIANTS: spec.variant value -> control kind. Built
#: from the imported mapping so the vocabulary has exactly one spelling.
_KIND_OF_VARIANT = {variant: kind for kind, variant in CONTROL_VARIANTS.items()}

#: Variant vocabulary a cell may carry: the full prior plus the four
#: control variants, in CONTROL_VARIANTS' own spelling.
_VARIANT_VOCAB = ("full",) + tuple(CONTROL_VARIANTS.values())

#: Variants whose prior tensor does not depend on the seed. For these a
#: rescoring or offset cell is IDENTICAL across seeds by construction:
#: the row is computed once, replicated, and flagged `seeds_degenerate`
#: with std 0.0 — never a fake spread.
_SEED_FREE_VARIANTS = frozenset(
    ("full", CONTROL_VARIANTS["uniform"], CONTROL_VARIANTS["marginal"])
)

_METHODS = ("rescoring", "offset_global", "offset_per_species", "mlp")
_LEARNED_METHODS = ("offset_global", "offset_per_species", "mlp")

#: Parquet schema of the results dataset, fixed and versioned. A row dict
#: missing a column, or carrying an extra one, fails the table build
#: loudly rather than writing a file other readers cannot join.
RESULTS_SCHEMA_VERSION = "2"
_RESULTS_SCHEMA = pa.schema(
    [
        pa.field("config_hash", pa.string()),
        pa.field("encoder", pa.string()),
        pa.field("method", pa.string()),
        pa.field("prior_variant", pa.string()),
        pa.field("resolution", pa.int64()),
        pa.field("tau", pa.float64()),  # null for learned arms
        pa.field("test_set", pa.string()),
        pa.field("seeds", pa.list_(pa.int64())),
        pa.field("cmap_mean", pa.float64()),
        pa.field("cmap_std", pa.float64()),
        pa.field("auroc_mean", pa.float64()),
        pa.field("auroc_std", pa.float64()),
        pa.field("top1_mean", pa.float64()),
        pa.field("top1_std", pa.float64()),
        pa.field("n_scored", pa.int64()),
        # I2: coverage failures split by lookup — train is NULL for
        # rescoring (which performs no training lookup); summing them
        # into one column would compare different quantities across
        # methods.
        pa.field("prior_coverage_failures_train", pa.int64()),
        pa.field("prior_coverage_failures_eval", pa.int64()),
        # Ruling (b), schema v2: fraction of eval-scoreable species the
        # learned arm actually FIT (1.0 for rescoring/offset_global and
        # for learned cells with full coverage). Below 1.0, the missing
        # species' scores are the ENCODER's raw eval scores (deployed-
        # system fallback), never another species' column.
        pa.field("species_coverage", pa.float64()),
        # I1 (spec §4 stratification): JSON-encoded
        # ``{stratum: [cmap_or_null, n_scored]}`` dicts. Deciles key the
        # frequency-decile stratification of the eval label prevalence
        # (the rarity curve's data path); expectation keys are
        # 'expected'/'unexpected' under the FULL prior at each row's
        # cell-week (prior prob > FLOOR for >=1 positive label).
        pa.field("cmap_by_freq_decile", pa.string()),
        pa.field("cmap_by_prior_expectation", pa.string()),
        pa.field("cmap_per_seed", pa.list_(pa.float64())),
        pa.field("seeds_degenerate", pa.bool_()),
        pa.field("git_sha", pa.string()),
        pa.field("train_cache_dir", pa.string()),
        pa.field("eval_cache_dir", pa.string()),
        pa.field("prior_path", pa.string()),
        pa.field("prior_doi", pa.string()),
        pa.field("wall_s", pa.float64()),
    ],
    metadata={b"results_schema_version": RESULTS_SCHEMA_VERSION.encode()},
)


@dataclass(frozen=True)
class SweepCell:
    """One configuration of the factorial grid — the unit of resume."""

    encoder: str  # 'perch' | 'birdnet'
    method: str  # 'rescoring' | 'offset_global' | 'offset_per_species' | 'mlp'
    prior_variant: str  # 'full' | values of CONTROL_VARIANTS
    resolution: int  # 3 | 4 | 5
    tau: float | None  # rescoring only; None for learned arms
    test_set: str  # 'HSN' | 'NES' | 'PER' | 'SSW' | 'SNE' | 'UHH'
    seeds: tuple[int, ...] = (0, 1, 2, 3, 4)


@dataclass(frozen=True)
class Provenance:
    """What a results row names as its inputs. Every artifact names its
    inputs; an empty `prior_doi` is refused by `run_cell` because a row
    that cannot cite its prior cannot be reproduced or attributed."""

    git_sha: str
    train_cache_dir: str
    eval_cache_dir: str
    prior_path: str
    prior_doi: str


def config_hash(cell: SweepCell) -> str:
    """Stable hex digest over ALL of a cell's fields.

    The hash keys the resume logic: a field it ignored would let resume
    skip a cell that was never run. JSON with sorted keys over the
    dataclass dict, so two processes (or two days) agree byte for byte.
    """
    payload = dataclasses.asdict(cell)
    payload["seeds"] = list(payload["seeds"])
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def plan_grid(
    *,
    encoders: tuple[str, ...] = ("perch", "birdnet"),
    methods: tuple[str, ...] = _METHODS,
    prior_variants: tuple[str, ...] = _VARIANT_VOCAB,
    resolutions: tuple[int, ...] = (3, 4, 5),
    taus: tuple[float, ...] = (0.25, 0.5, 1.0, 2.0),
    test_sets: tuple[str, ...] = ("HSN", "NES", "PER", "SSW", "SNE", "UHH"),
    seeds: tuple[int, ...] = (0, 1, 2, 3, 4),
) -> list[SweepCell]:
    """The cheap-arm grid, deduplicated, in deterministic order.

    The tau axis applies to rescoring only; learned arms carry
    ``tau=None`` (a learned arm has no prior temperature — hashing a
    phantom tau would split one real configuration into four hashes).
    """
    cells: list[SweepCell] = []
    for encoder in encoders:
        for test_set in test_sets:
            for resolution in resolutions:
                for variant in prior_variants:
                    for method in methods:
                        method_taus = taus if method == "rescoring" else (None,)
                        for tau in method_taus:
                            cells.append(
                                SweepCell(
                                    encoder=encoder,
                                    method=method,
                                    prior_variant=variant,
                                    resolution=resolution,
                                    tau=tau,
                                    test_set=test_set,
                                    seeds=tuple(seeds),
                                )
                            )
    return list(dict.fromkeys(cells))


def shard_cells(cells: list[SweepCell], shard: int, n_shards: int) -> list[SweepCell]:
    """A stable partition of the grid: shard k of n is ``cells[k::n]``.

    Every cell lands in exactly one shard and the assignment depends only
    on the grid order (itself deterministic), so two machines given the
    same arguments never collide and never both skip a cell.
    """
    if n_shards < 1:
        raise ValueError(f"n_shards must be >= 1, got {n_shards}")
    if not 0 <= shard < n_shards:
        raise ValueError(
            f"shard {shard} out of range for n_shards={n_shards}; shards "
            "are numbered 0..n_shards-1"
        )
    return cells[shard::n_shards]


def fit_group_key(cell: SweepCell) -> tuple:
    """The axes a learned-arm FIT depends on — everything except test_set
    and tau. Cells sharing this key refit the identical model, so the
    sweep's dominant cost (150 distinct MLP fits vs 6x that when every
    test set refits) is shared through run_cell's fit_cache. Rescoring
    cells fit nothing; their key still groups them harmlessly."""
    return (
        cell.encoder,
        cell.method,
        cell.prior_variant,
        cell.resolution,
        cell.seeds,
    )


def shard_cells_grouped(
    cells: list[SweepCell], shard: int, n_shards: int
) -> list[SweepCell]:
    """Partition the grid by FIT GROUP, not by cell: all cells sharing a
    fit_group_key land in the same shard (groups assigned k::n in first-
    appearance order). Plain per-cell sharding would scatter one fit's six
    test sets across shards and refit the same model on each — 6x the
    sweep's dominant cost. Same disjoint-cover guarantee as shard_cells.
    """
    if n_shards < 1:
        raise ValueError(f"n_shards must be >= 1, got {n_shards}")
    if not 0 <= shard < n_shards:
        raise ValueError(
            f"shard {shard} out of range for n_shards={n_shards}; shards "
            "are numbered 0..n_shards-1"
        )
    groups: dict[tuple, list[SweepCell]] = {}
    order: list[tuple] = []
    for cell in cells:
        key = fit_group_key(cell)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(cell)
    mine: list[SweepCell] = []
    for key in order[shard::n_shards]:
        mine.extend(groups[key])
    return mine


def _validate_cell(cell: SweepCell, provenance: Provenance) -> None:
    """The row contract's refusals, checked before any work."""
    if not provenance.prior_doi or not provenance.prior_doi.strip():
        raise ValueError(
            "refusing to run a cell with an empty prior_doi (got "
            f"{provenance.prior_doi!r}); every results artifact names its "
            "inputs, and a row that cannot cite the prior it was rescored "
            "by cannot be reproduced or attributed"
        )
    if len(cell.seeds) != 5 or len(set(cell.seeds)) != 5:
        raise ValueError(
            f"cell carries seeds {cell.seeds!r}; the resume granularity is "
            "the FULL cell of 5 distinct seeds — a 4-seed or duplicated "
            "seed tuple would persist a mean the ruling forbids"
        )
    if cell.method not in _METHODS:
        raise ValueError(
            f"unknown method {cell.method!r}; expected one of {_METHODS}"
        )
    if cell.prior_variant not in _VARIANT_VOCAB:
        raise ValueError(
            f"unknown prior_variant {cell.prior_variant!r}; expected one "
            f"of {_VARIANT_VOCAB} (the vocabulary is "
            "rescoring.CONTROL_VARIANTS plus 'full', imported — never "
            "re-spelled)"
        )
    if cell.method == "rescoring" and cell.tau is None:
        raise ValueError(
            "rescoring cells need a tau; None is reserved for the learned "
            "arms, which have no prior temperature"
        )


def _variant_tensor(
    variant: str, prior: PriorTensor, seed: int
) -> PriorTensor:
    """The prior tensor a seed sees: the full prior, or a control built
    from it. Shuffle controls draw from an rng seeded with *seed* — the
    ONLY place a seed enters a rescoring cell."""
    if variant == "full":
        return prior
    kind = _KIND_OF_VARIANT[variant]
    return control_prior(kind, prior, np.random.default_rng(seed))


def _eval_probabilities(
    eval_cache: CacheView, class_idx: dict[str, int], scoreable: list[str]
) -> np.ndarray:
    """Eval scores as probabilities, sigmoid applied EXACTLY ONCE iff the
    cache's metadata declares raw logits — never inferred from values."""
    logits = np.asarray(
        eval_cache.table.column("logits").to_pylist(), dtype=np.float64
    )
    scores = gather_scores(logits, class_idx, scoreable).astype(np.float64)
    if eval_cache.logits_are_raw:
        scores = 1.0 / (1.0 + np.exp(-scores))
    return scores


def _map_columns(
    train_scoreable: list[str], eval_scoreable: list[str]
) -> list[int]:
    """Train-axis column index per eval-scoreable code; refuses a code
    the model never saw — predicting it would silently hand back another
    species' column or an invented value. This STRICT form stays the
    default for any caller not explicitly opting into the encoder-score
    fallback (:func:`_map_columns_with_fallback`)."""
    col_of = {code: j for j, code in enumerate(train_scoreable)}
    missing = [code for code in eval_scoreable if code not in col_of]
    if missing:
        raise ValueError(
            f"eval scoreable species {missing} absent from the training "
            "class axis; a learned arm cannot predict a species it never "
            "fit, and silently reusing another column would misattribute "
            "every one of its scores"
        )
    return [col_of[code] for code in eval_scoreable]


def _map_columns_with_fallback(
    train_scoreable: list[str], eval_scoreable: list[str]
) -> tuple[np.ndarray, np.ndarray]:
    """Ruling (b): per-eval-species train column with ``-1`` for a
    species the model never fit, plus the boolean fitted mask.

    Returns ``(cols, fitted)`` — ``cols`` int64 ``(n_eval_scoreable,)``,
    ``fitted = cols >= 0``. Callers keep the model's column for fitted
    species and fall back to the ENCODER's raw eval score for the rest
    (the deployed-system behavior); nothing here ever substitutes
    another species' column. The strict refusal (:func:`_map_columns`)
    remains the default elsewhere.
    """
    col_of = {code: j for j, code in enumerate(train_scoreable)}
    cols = np.asarray(
        [col_of.get(code, -1) for code in eval_scoreable], dtype=np.int64
    )
    return cols, cols >= 0


def _guard_eval_split(table: pa.Table) -> None:
    """Refuse an eval table carrying training rows — the symmetric twin
    of :func:`birdology.phase_b.learned.guard_training_split`.

    Raises ValueError when any ``split`` value starts with ``"train"``
    (or when the column is absent and the rows cannot be proven eval
    rows): a focal/training cache handed to the eval slot would
    'evaluate' on training data and silently void the train/test shift.
    The error NAMES the offending split values.
    """
    if "split" not in table.column_names:
        raise ValueError(
            "eval table has no 'split' column; cannot prove these rows "
            f"are eval rows (columns: {table.column_names})"
        )
    splits = table.column("split").to_pylist()
    offending = sorted(
        {s for s in splits if s is not None and s.startswith("train")}
    )
    if offending:
        raise ValueError(
            f"eval cache carries training split values {offending}; "
            "run_cell evaluates on test/soundscape rows only — a train-"
            "split cache in the eval slot would score the training data "
            "and silently void the train/test shift"
        )


def _align_eval_labels(
    eval_table: pa.Table,
    eval_metadata: pa.Table,
    class_idx: dict[str, int],
) -> tuple[np.ndarray, list[str], list[str]]:
    """Eval label matrix from the SEPARATE metadata table, joined to the
    cache rows BY FILEPATH — the exact contract of
    ``repro_gate.score_pair``.

    Real Phase A caches carry NO ``ebird_code_multilabel`` column (see
    ``phase_a/embed.py``'s EMBEDDING_SCHEMA); the multilabel ground
    truth lives only in the test set's metadata parquet. The join is
    never positional, and a cache recording whose filepath is absent
    from the metadata raises NAMING examples — a silent inner join would
    quietly shrink the test set.
    """
    cache_files: list[str] = eval_table.column("filepath").to_pylist()
    labels_by_file = dict(
        zip(
            eval_metadata.column("filepath").to_pylist(),
            eval_metadata.column("ebird_code_multilabel").to_pylist(),
        )
    )
    missing = [f for f in cache_files if f not in labels_by_file]
    if missing:
        raise ValueError(
            f"{len(missing)} eval cache recording(s) missing from the "
            f"eval-labels metadata: {missing[:5]} -- a silent inner join "
            "would shrink the test set; refusing (the same contract as "
            "repro_gate.score_pair)"
        )
    # Re-emit the metadata rows IN CACHE ROW ORDER so Y rows align with
    # the logits rows, then let align_labels build the matrix by code.
    aligned = pa.table(
        {
            "ebird_code_multilabel": pa.array(
                [labels_by_file[f] for f in cache_files],
                pa.list_(pa.string()),
            )
        }
    )
    return align_labels(aligned, class_idx)


def _merge_strata(dicts: list[dict]) -> dict[str, list]:
    """Mean the per-seed stratified cmap tuples into one JSON-ready
    dict ``{stratum: [cmap_or_null, n_scored]}``.

    ``n_scored`` per stratum is seed-invariant (it depends only on the
    labels and the stratum mask), so the first seed's count is THE
    count; a stratum every seed scored as NaN becomes null, never a
    fabricated number."""
    merged: dict[str, list] = {}
    for key in dicts[0]:
        values = [d[key][0] for d in dicts]
        n_scored = int(dicts[0][key][1])
        finite = [float(v) for v in values if np.isfinite(v)]
        merged[str(key)] = [
            float(np.mean(finite)) if finite else None,
            n_scored,
        ]
    return merged


def run_cell(
    cell: SweepCell,
    *,
    train_cache: CacheView,
    eval_cache: CacheView,
    eval_metadata: pa.Table,
    prior: PriorTensor,
    class_idx: dict[str, int],
    code_to_gbif: dict[str, int],
    provenance: Provenance,
    mlp_hidden: int = 256,
    mlp_max_iter: int = 200,
    fit_cache: dict | None = None,
) -> dict:
    """Execute *cell* for ALL its seeds and return ONE results row.

    *eval_metadata* is the test set's metadata table (``filepath`` +
    ``ebird_code_multilabel``): real Phase A caches carry no label
    column, so the eval labels are joined to the cache rows BY FILEPATH
    (see :func:`_align_eval_labels`), refusing any cache recording the
    metadata does not cover. The eval table itself is refused when it
    carries training-split rows (:func:`_guard_eval_split`).

    Learned arms fit on *train_cache* per seed and evaluate on
    *eval_cache*; rescoring fits nothing — seeds enter only the control
    shuffles. For seed-free configurations (variant in 'full' / 'uniform'
    / 'marginal' with a deterministic method) the five seeds are
    IDENTICAL by construction: the metrics are computed once, replicated
    per seed, std set to 0.0 and the row flagged ``seeds_degenerate`` —
    a fake spread is never reported.

    MARGINAL-CONTROL CANCELLATION (documented, not a defect): under the
    default marginal ``m = train_marginal_from_prior(tensor, ...)`` the
    rescoring arm's correction ``tau * (log p - log m)`` cancels EXACTLY
    for the 'marginal' control — that control's prior IS the global
    marginal, so ``log p == log m`` cell-wise and the scores pass
    through unchanged at every tau. The marginal row therefore cannot
    carry the frequency-exploitation check for rescoring; that check is
    carried by the LEARNED arms (whose fits can exploit the marginal as
    a feature). The control row stays in the grid on purpose: it is the
    documented tau-invariant baseline, equal to the raw-encoder row by
    construction.
    """
    _validate_cell(cell, provenance)
    t0 = time.perf_counter()

    eval_table = eval_cache.table
    _guard_eval_split(eval_table)
    Y, scoreable, _unscoreable = _align_eval_labels(
        eval_table, eval_metadata, class_idx
    )
    eval_scores = _eval_probabilities(eval_cache, class_idx, scoreable)
    # Obligation: weeks come ONLY off the cache's `week` column.
    eval_cells = eval_table.column("cell").to_pylist()
    eval_weeks = eval_table.column("week").to_pylist()

    # I1 — class-level strata, fixed across seeds (they derive from the
    # labels and the FULL prior, never from the per-seed scores).
    # Frequency deciles: rank classes by eval label prevalence
    # (ties broken by code — deterministic), decile k = rank*10//n.
    prevalence = Y.mean(axis=0) if Y.size else np.zeros(len(scoreable))
    rank_order = sorted(
        range(len(scoreable)),
        key=lambda j: (float(prevalence[j]), scoreable[j]),
    )
    freq_decile = np.zeros(len(scoreable), dtype=np.int64)
    for pos, j in enumerate(rank_order):
        freq_decile[j] = pos * 10 // max(len(scoreable), 1)
    # Expected-under-prior: a class is 'expected' iff the FULL prior
    # answers above FLOOR for it at >=1 eval row's cell-week.
    full_lookup = prior_for_rows(
        prior, eval_cells, eval_weeks, scoreable, code_to_gbif
    )
    prior_expectation = np.where(
        (full_lookup.probs > FLOOR).any(axis=0), "expected", "unexpected"
    )

    def _class_strata(scored: np.ndarray, strata: np.ndarray) -> dict:
        """cmap per CLASS stratum via metrics.stratified — arrays are
        transposed so the stratum mask selects classes, and the metric
        transposes back before scoring."""
        return stratified(
            scored.T, Y.T, strata, lambda s, yy: cmap(s.T, yy.T)
        )

    # Ruling (b): the per-species learned arms (mlp, offset_per_species)
    # map eval columns onto the TRAIN class axis; an eval species the fit
    # group's training rows never carried keeps the ENCODER's raw eval
    # score instead of refusing the whole cell (the fleet lost ~60 cells
    # across 5 shards to the strict refusal — e.g. NES's warvir/yelwar,
    # PER's goeant1/scbwoo5). species_coverage reports the fitted
    # fraction; 1.0 for rescoring/offset_global (no per-column mapping).
    species_coverage = 1.0
    fallback_cols = np.empty(0, dtype=np.int64)
    fitted_mask = np.empty(0, dtype=np.bool_)
    if cell.method in _LEARNED_METHODS:
        # prepare_training's arrays are row-parallel to the train table,
        # so the table's cell/week columns zip against them directly.
        embeddings, train_scores, y_train, train_scoreable, _stats = (
            prepare_training(train_cache, class_idx)
        )
        train_cells = train_cache.table.column("cell").to_pylist()
        train_weeks = train_cache.table.column("week").to_pylist()
        eval_embeddings = (
            np.asarray(
                eval_table.column("embedding").to_pylist(), dtype=np.float64
            )
            if cell.method == "mlp"
            else None
        )
        if cell.method in ("mlp", "offset_per_species"):
            fallback_cols, fitted_mask = _map_columns_with_fallback(
                train_scoreable, scoreable
            )
            if scoreable:
                species_coverage = float(fitted_mask.mean())
            unfitted = [
                code for code, ok in zip(scoreable, fitted_mask) if not ok
            ]
            if unfitted:
                # ONE warning per cell, naming it and every missing code
                # — not one line per species, not one per seed.
                logger.warning(
                    "cell %s (%s/%s/%s res%d %s): %d eval species absent "
                    "from the training class axis %s; falling back to raw "
                    "encoder scores for them (species_coverage=%.4f)",
                    config_hash(cell)[:12],
                    cell.encoder,
                    cell.method,
                    cell.prior_variant,
                    cell.resolution,
                    cell.test_set,
                    len(unfitted),
                    unfitted,
                    species_coverage,
                )

    def _seed_metrics(seed: int) -> dict:
        """One seed's metrics: cmap/n_scored/auroc/top1, the SPLIT
        coverage counts (train None for rescoring — no training
        lookup), and the two class-stratified cmap dicts."""
        tensor = _variant_tensor(cell.prior_variant, prior, seed)
        if cell.method == "rescoring":
            lookup = prior_for_rows(
                tensor, eval_cells, eval_weeks, scoreable, code_to_gbif
            )
            # M2: for the 'marginal' control this marginal equals the
            # control's own prior, so log p - log m == 0 and `scored`
            # is the raw encoder ranking at every tau (see the run_cell
            # docstring) — correct, documented, kept as the
            # tau-invariant baseline row.
            marginal = train_marginal_from_prior(
                tensor, scoreable, code_to_gbif
            )
            scored = rescore(eval_scores, lookup.probs, cell.tau, marginal)
            coverage_train: int | None = None
            coverage_eval = lookup.n_floored
        else:
            train_lookup = prior_for_rows(
                tensor, train_cells, train_weeks, train_scoreable, code_to_gbif
            )
            if cell.method == "mlp":
                # Fit shared across test sets: the fit depends only on
                # fit_group_key(cell) + seed (train side), never on the
                # eval cache. With shard_cells_grouped, one shard owns a
                # group's six test sets and pays for ONE fit.
                _fk = (fit_group_key(cell), seed)
                model = fit_cache.get(_fk) if fit_cache is not None else None
                if model is None:
                    model = fit_mlp_head(
                        embeddings,
                        train_lookup.probs,
                        y_train,
                        seed=seed,
                        hidden=mlp_hidden,
                        max_iter=mlp_max_iter,
                    )
                    if fit_cache is not None:
                        fit_cache[_fk] = model
                # MLP features need the prior on the TRAIN class axis
                # (the feature width is fixed by the fit).
                eval_lookup = prior_for_rows(
                    tensor, eval_cells, eval_weeks, train_scoreable, code_to_gbif
                )
                proba = model.predict(eval_embeddings, eval_lookup.probs)
                # Ruling (b): fitted species carry the model's column;
                # unfitted ones KEEP the encoder's raw eval score (the
                # deployed-system fallback) — never another column.
                scored = eval_scores.copy()
                scored[:, fitted_mask] = proba[
                    :, fallback_cols[fitted_mask]
                ]
            else:
                per_species = cell.method == "offset_per_species"
                _fk = (fit_group_key(cell), seed)
                model = fit_cache.get(_fk) if fit_cache is not None else None
                if model is None:
                    model = fit_logit_offset(
                        train_scores,
                        train_lookup.probs,
                        y_train,
                        per_species=per_species,
                        seed=seed,
                    )
                    if fit_cache is not None:
                        fit_cache[_fk] = model
                eval_lookup = prior_for_rows(
                    tensor, eval_cells, eval_weeks, scoreable, code_to_gbif
                )
                if per_species:
                    # Ruling (b): unfitted species get lam=0 — the
                    # prior term vanishes EXACTLY (0 * finite log of
                    # the FLOORed prior), so predict returns
                    # log(max(encoder score, FLOOR)) for them: the
                    # encoder score passed through the arm's fixed log
                    # transform, scale-consistent with fitted columns.
                    lam_full = np.asarray(model.lam, dtype=np.float64)
                    lam_eval = np.zeros(len(scoreable), dtype=np.float64)
                    lam_eval[fitted_mask] = lam_full[
                        fallback_cols[fitted_mask]
                    ]
                    model = OffsetModel(lam=lam_eval, per_species=True)
                scored = model.predict(eval_scores, eval_lookup.probs)
            coverage_train = train_lookup.n_floored
            coverage_eval = eval_lookup.n_floored
        cmap_v, n_scored = cmap(scored, Y)
        auroc_v, _n_auroc = auroc_macro(scored, Y)
        top1_v = top1(scored, Y)
        return {
            "cmap": cmap_v,
            "n_scored": n_scored,
            "auroc": auroc_v,
            "top1": top1_v,
            "coverage_train": coverage_train,
            "coverage_eval": coverage_eval,
            "by_decile": _class_strata(scored, freq_decile),
            "by_expectation": _class_strata(scored, prior_expectation),
        }

    degenerate = (
        cell.method != "mlp" and cell.prior_variant in _SEED_FREE_VARIANTS
    )
    if degenerate:
        # Identical by construction: compute once, replicate, std 0.0.
        m = _seed_metrics(cell.seeds[0])
        per_seed = [m] * len(cell.seeds)
        cmap_std = auroc_std = top1_std = 0.0
    else:
        per_seed = [_seed_metrics(seed) for seed in cell.seeds]
        cmap_std = float(np.std([m["cmap"] for m in per_seed]))
        auroc_std = float(np.std([m["auroc"] for m in per_seed]))
        top1_std = float(np.std([m["top1"] for m in per_seed]))
    cmaps = [m["cmap"] for m in per_seed]
    aurocs = [m["auroc"] for m in per_seed]
    top1s = [m["top1"] for m in per_seed]
    last = per_seed[-1]

    return {
        "config_hash": config_hash(cell),
        "encoder": cell.encoder,
        "method": cell.method,
        "prior_variant": cell.prior_variant,
        "resolution": cell.resolution,
        "tau": cell.tau,
        "test_set": cell.test_set,
        "seeds": list(cell.seeds),
        "cmap_mean": float(np.mean(cmaps)),
        "cmap_std": cmap_std,
        "auroc_mean": float(np.mean(aurocs)),
        "auroc_std": auroc_std,
        "top1_mean": float(np.mean(top1s)),
        "top1_std": top1_std,
        "n_scored": int(last["n_scored"]),
        "prior_coverage_failures_train": (
            None
            if last["coverage_train"] is None
            else int(last["coverage_train"])
        ),
        "prior_coverage_failures_eval": int(last["coverage_eval"]),
        "species_coverage": species_coverage,
        "cmap_by_freq_decile": json.dumps(
            _merge_strata([m["by_decile"] for m in per_seed]),
            sort_keys=True,
        ),
        "cmap_by_prior_expectation": json.dumps(
            _merge_strata([m["by_expectation"] for m in per_seed]),
            sort_keys=True,
        ),
        "cmap_per_seed": [float(v) for v in cmaps],
        "seeds_degenerate": degenerate,
        "git_sha": provenance.git_sha,
        "train_cache_dir": provenance.train_cache_dir,
        "eval_cache_dir": provenance.eval_cache_dir,
        "prior_path": provenance.prior_path,
        "prior_doi": provenance.prior_doi,
        "wall_s": time.perf_counter() - t0,
    }


def load_existing_hashes(out_dir: Path) -> set[str]:
    """Every config_hash already on disk under *out_dir*. An absent dir
    is an empty result set, not an error — a first run has nothing."""
    out_dir = Path(out_dir)
    if not out_dir.is_dir():
        return set()
    hashes: set[str] = set()
    for path in sorted(out_dir.glob("*.parquet")):
        hashes.update(
            pq.read_table(path, columns=["config_hash"])
            .column("config_hash")
            .to_pylist()
        )
    return hashes


def write_rows(rows: list[dict], out_dir: Path) -> Path:
    """Append *rows* to the parquet dataset under *out_dir*.

    A row whose config_hash already exists — on disk or earlier in this
    same batch — is SKIPPED LOUDLY: logged by hash and counted in the
    sidecar, never silently duplicated. Each call writes one parquet
    file in the fixed versioned schema plus a JSON sidecar
    ``{git_sha, written_at, n_rows, n_skipped_existing}``; the sidecar
    path is returned.

    CONCURRENCY CONTRACT (honest statement): the dedupe above is
    PER-PROCESS ONLY. ``load_existing_hashes`` is read once at entry and
    the check-then-write is not atomic, so two writers appending the
    same hash concurrently can BOTH write it — nothing here locks the
    directory. Cross-writer uniqueness is guaranteed solely by
    ``shard_cells``' disjoint partition of the grid: two correctly
    sharded processes never carry the same config_hash, so the race is
    unreachable under the intended deployment. Running two writers on
    OVERLAPPING cell sets voids the no-duplicates property; readers who
    cannot assume disjoint shards must dedupe on config_hash.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    existing = load_existing_hashes(out_dir)

    kept: list[dict] = []
    n_skipped = 0
    for row in rows:
        h = row["config_hash"]
        if h in existing:
            logger.warning(
                "skipping row with existing config_hash %s (%s/%s/%s)",
                h,
                row.get("encoder"),
                row.get("method"),
                row.get("test_set"),
            )
            n_skipped += 1
            continue
        existing.add(h)
        kept.append(row)

    stem = f"rows-{uuid.uuid4().hex}"
    if kept:
        columns = {
            field.name: [row[field.name] for row in kept]
            for field in _RESULTS_SCHEMA
        }
        table = pa.table(columns, schema=_RESULTS_SCHEMA)
        pq.write_table(table, out_dir / f"{stem}.parquet")

    sidecar = out_dir / f"{stem}.json"
    sidecar.write_text(
        json.dumps(
            {
                "git_sha": rows[0]["git_sha"] if rows else "",
                "written_at": datetime.now(timezone.utc).isoformat(),
                "n_rows": len(kept),
                "n_skipped_existing": n_skipped,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return sidecar


#: The config axes two compared rows may differ on — at most ONE of them,
#: the axis under comparison. (seeds is deliberately absent: it has its
#: own stricter refusal — the tuples must be identical.)
_PAIRING_AXES = ("encoder", "method", "prior_variant", "resolution", "tau", "test_set")

#: Two-sided 95% confidence level for the paired CI.
_CI_LEVEL = 0.975


def paired_delta(
    rows_a: list[dict], rows_b: list[dict]
) -> tuple[float, tuple[float, float]]:
    """Mean paired cmAP delta (a - b) across shared seeds, with a 95% CI
    from the t-distribution on the per-seed deltas (n=5 pairs per row
    -> t_{0.975,4} = 2.776).

    Rows are paired positionally. A pair differing on MORE than one
    config axis is not a paired comparison and is refused; so is a pair
    with non-identical seed tuples (the deltas would subtract different
    draws).

    MULTI-ROW POOLING CAVEAT: when more than one row pair is passed,
    every pair's per-seed deltas are POOLED into one sample — the CI is
    computed on n_pairs x 5 deltas with df = n-1 AS IF they were
    independent draws. Deltas from different test sets (or other config
    axes) are not exchangeable: pooling them inflates df and narrows
    the CI relative to a per-row (or per-test-set) analysis. Headline
    claims should therefore be made PER ROW PAIR (one test set, one
    axis contrast, df=4); the multi-row form is a convenience summary,
    not a substitute for per-row inference.
    """
    if not rows_a or len(rows_a) != len(rows_b):
        raise ValueError(
            f"paired_delta needs equal-length non-empty row lists, got "
            f"{len(rows_a)} and {len(rows_b)}"
        )
    deltas: list[float] = []
    for a, b in zip(rows_a, rows_b):
        if list(a["seeds"]) != list(b["seeds"]):
            raise ValueError(
                f"seed tuples differ ({a['seeds']!r} vs {b['seeds']!r}); "
                "a paired delta subtracts the SAME seed's draw on both "
                "sides, so non-identical tuples pair unrelated runs"
            )
        differing = [ax for ax in _PAIRING_AXES if a[ax] != b[ax]]
        if len(differing) > 1:
            raise ValueError(
                f"rows differ on config axes {differing}; a paired "
                "comparison varies exactly the one axis under study — "
                "anything else confounds the delta"
            )
        per_a, per_b = a["cmap_per_seed"], b["cmap_per_seed"]
        if len(per_a) != len(per_b):
            raise ValueError(
                f"per-seed cmap lists differ in length ({len(per_a)} vs "
                f"{len(per_b)}); the rows do not share a seed set"
            )
        deltas.extend(float(x) - float(y) for x, y in zip(per_a, per_b))

    arr = np.asarray(deltas, dtype=np.float64)
    mean = float(arr.mean())
    sd = float(arr.std(ddof=1))
    half = float(stats.t.ppf(_CI_LEVEL, arr.size - 1)) * sd / np.sqrt(arr.size)
    return mean, (mean - half, mean + half)
