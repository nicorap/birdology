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
* `prior_coverage_failures` counts COVERAGE failures (cells/species the
  prior could not answer, i.e. `PriorLookup.n_floored`), not values that
  happen to equal FLOOR — hence the column name.
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
from birdology.phase_b.metrics import auroc_macro, cmap, gather_scores, top1
from birdology.phase_b.rescoring import (
    CONTROL_VARIANTS,
    control_prior,
    prior_for_rows,
    rescore,
    train_marginal_from_prior,
)
from birdology.prior.tensor import PriorTensor

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
RESULTS_SCHEMA_VERSION = "1"
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
        pa.field("prior_coverage_failures", pa.int64()),
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
    species' column or an invented value."""
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


def run_cell(
    cell: SweepCell,
    *,
    train_cache: CacheView,
    eval_cache: CacheView,
    prior: PriorTensor,
    class_idx: dict[str, int],
    code_to_gbif: dict[str, int],
    provenance: Provenance,
    mlp_hidden: int = 256,
    mlp_max_iter: int = 200,
) -> dict:
    """Execute *cell* for ALL its seeds and return ONE results row.

    Learned arms fit on *train_cache* per seed and evaluate on
    *eval_cache*; rescoring fits nothing — seeds enter only the control
    shuffles. For seed-free configurations (variant in 'full' / 'uniform'
    / 'marginal' with a deterministic method) the five seeds are
    IDENTICAL by construction: the metrics are computed once, replicated
    per seed, std set to 0.0 and the row flagged ``seeds_degenerate`` —
    a fake spread is never reported.
    """
    _validate_cell(cell, provenance)
    t0 = time.perf_counter()

    eval_table = eval_cache.table
    Y, scoreable, _unscoreable = align_labels(eval_table, class_idx)
    eval_scores = _eval_probabilities(eval_cache, class_idx, scoreable)
    # Obligation: weeks come ONLY off the cache's `week` column.
    eval_cells = eval_table.column("cell").to_pylist()
    eval_weeks = eval_table.column("week").to_pylist()

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

    def _seed_metrics(seed: int) -> tuple[float, int, float, float, int]:
        """(cmap, n_scored, auroc, top1, coverage_failures) for one seed."""
        tensor = _variant_tensor(cell.prior_variant, prior, seed)
        if cell.method == "rescoring":
            lookup = prior_for_rows(
                tensor, eval_cells, eval_weeks, scoreable, code_to_gbif
            )
            marginal = train_marginal_from_prior(
                tensor, scoreable, code_to_gbif
            )
            scored = rescore(eval_scores, lookup.probs, cell.tau, marginal)
            coverage = lookup.n_floored
        else:
            train_lookup = prior_for_rows(
                tensor, train_cells, train_weeks, train_scoreable, code_to_gbif
            )
            if cell.method == "mlp":
                model = fit_mlp_head(
                    embeddings,
                    train_lookup.probs,
                    y_train,
                    seed=seed,
                    hidden=mlp_hidden,
                    max_iter=mlp_max_iter,
                )
                # MLP features need the prior on the TRAIN class axis
                # (the feature width is fixed by the fit).
                eval_lookup = prior_for_rows(
                    tensor, eval_cells, eval_weeks, train_scoreable, code_to_gbif
                )
                proba = model.predict(eval_embeddings, eval_lookup.probs)
                scored = proba[:, _map_columns(train_scoreable, scoreable)]
            else:
                per_species = cell.method == "offset_per_species"
                model = fit_logit_offset(
                    train_scores,
                    train_lookup.probs,
                    y_train,
                    per_species=per_species,
                    seed=seed,
                )
                eval_lookup = prior_for_rows(
                    tensor, eval_cells, eval_weeks, scoreable, code_to_gbif
                )
                if per_species:
                    lam_eval = np.asarray(model.lam, dtype=np.float64)[
                        _map_columns(train_scoreable, scoreable)
                    ]
                    model = OffsetModel(lam=lam_eval, per_species=True)
                scored = model.predict(eval_scores, eval_lookup.probs)
            coverage = train_lookup.n_floored + eval_lookup.n_floored
        cmap_v, n_scored = cmap(scored, Y)
        auroc_v, _n_auroc = auroc_macro(scored, Y)
        top1_v = top1(scored, Y)
        return cmap_v, n_scored, auroc_v, top1_v, coverage

    degenerate = (
        cell.method != "mlp" and cell.prior_variant in _SEED_FREE_VARIANTS
    )
    if degenerate:
        # Identical by construction: compute once, replicate, std 0.0.
        cmap_v, n_scored, auroc_v, top1_v, coverage = _seed_metrics(
            cell.seeds[0]
        )
        cmaps = [cmap_v] * len(cell.seeds)
        aurocs = [auroc_v] * len(cell.seeds)
        top1s = [top1_v] * len(cell.seeds)
        cmap_std = auroc_std = top1_std = 0.0
    else:
        cmaps, aurocs, top1s = [], [], []
        for seed in cell.seeds:
            cmap_v, n_scored, auroc_v, top1_v, coverage = _seed_metrics(seed)
            cmaps.append(cmap_v)
            aurocs.append(auroc_v)
            top1s.append(top1_v)
        cmap_std = float(np.std(cmaps))
        auroc_std = float(np.std(aurocs))
        top1_std = float(np.std(top1s))

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
        "n_scored": int(n_scored),
        "prior_coverage_failures": int(coverage),
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
