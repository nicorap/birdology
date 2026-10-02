"""The sweep driver + results contract (Task 6).

Every number the paper reports flows through `run_cell`'s row contract, so
these tests pin the contract rather than the arithmetic (the arithmetic is
Tasks 2-5's, already pinned there). Fixtures are tiny in-memory CacheViews
and PriorTensors — no file caches, no network, no WEKA.

Carried obligations exercised here:

* resume granularity is the FULL 5-seed cell (refusals on 4/duplicate seeds;
  rows only ever carry complete 5-seed metrics);
* variant vocabulary is IMPORTED from `rescoring.CONTROL_VARIANTS`;
* weeks come off the cache's `week` column, never recomputed;
* `prior_coverage_failures` names a coverage-failure count;
* degenerate seed sets report std 0.0 + `seeds_degenerate=True`, never a
  fake spread.
"""
from __future__ import annotations

import dataclasses
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_b.rescoring import CONTROL_VARIANTS
from birdology.phase_b.cache import CacheView, load_cache
from birdology.phase_b.sweep import (
    Provenance,
    SweepCell,
    config_hash,
    load_existing_hashes,
    paired_delta,
    fit_group_key,
    plan_grid,
    run_cell,
    shard_cells,
    shard_cells_grouped,
    write_rows,
)
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import PriorSpec, PriorTensor


# ---------------------------------------------------------------------------
# fixtures — tiny in-memory caches, prior, bridge, provenance
# ---------------------------------------------------------------------------

_CODES = ["amecro", "blujay", "carwre"]
_CLASS_IDX = {code: i for i, code in enumerate(_CODES)}
_CODE_TO_GBIF = {"amecro": 111, "blujay": 222, "carwre": 333}
_CELLS = ["cella", "cellb", "cellc", "celld"]
_DOI = "10.15468/dl.test00"

_PROV = Provenance(
    git_sha="deadbeefcafe",
    train_cache_dir="/caches/train",
    eval_cache_dir="/caches/eval/HSN",
    prior_path="/priors/prior_res4.npz",
    prior_doi=_DOI,
)


def _prior() -> PriorTensor:
    """3 species x 4 cells, every (species, cell, week) value DISTINCT —
    the asymmetric design from the rescoring tests: shuffling cells or
    weeks cannot silently reproduce the original."""
    n_species, n_cols = 3, len(_CELLS) * WEEKS_PER_YEAR
    dense = (1.0 + np.arange(n_species * n_cols, dtype=np.float64)).reshape(
        n_species, n_cols
    ) / (n_species * n_cols + 1.0)
    return PriorTensor(
        spec=PriorSpec(resolution=4, source="eod", variant="full"),
        species_keys=np.asarray([111, 222, 333], dtype=np.int64),
        cells=list(_CELLS),
        values=sparse.csr_matrix(dense),
        gbif_doi=_DOI,
    )


def _cache_table(
    *,
    n: int,
    label_col: str,
    labels: list,
    splits: list[str],
    logits: np.ndarray,
    seed: int = 0,
    d: int = 6,
) -> pa.Table:
    """A table with the cache column contract (cell / week / split /
    labels / embedding / logits). Weeks cycle 1..13 — already folded, as
    the manifest guarantees; the driver must read them off this column."""
    rng = np.random.default_rng(seed)
    embeddings = rng.normal(size=(n, d))
    # A little class signal in the embeddings so the MLP has something
    # to learn without being able to saturate.
    for i in range(n):
        first = labels[i][0] if isinstance(labels[i], list) and labels[i] else labels[i]
        if first is not None:
            embeddings[i, _CLASS_IDX[first] % d] += 1.5
    if label_col == "ebird_code":
        label_arr = pa.array(labels, pa.string())
    else:
        label_arr = pa.array(labels, pa.list_(pa.string()))
    return pa.table(
        {
            "filepath": pa.array([f"r{i}.ogg" for i in range(n)], pa.string()),
            "cell": pa.array([_CELLS[i % len(_CELLS)] for i in range(n)], pa.string()),
            "week": pa.array([1 + (i % 13) for i in range(n)], pa.int8()),
            "split": pa.array(splits, pa.string()),
            label_col: label_arr,
            "embedding": pa.array(embeddings.tolist(), pa.list_(pa.float64())),
            "logits": pa.array(logits.tolist(), pa.list_(pa.float64())),
        }
    )


def _correlated_logits(labels: list, n_classes: int, seed: int) -> np.ndarray:
    """Probabilities correlated with the labels but noisy enough that a
    prior can move the ranking."""
    rng = np.random.default_rng(seed)
    n = len(labels)
    logits = rng.uniform(0.15, 0.45, size=(n, n_classes))
    for i, item in enumerate(labels):
        members = item if isinstance(item, list) else [item]
        for code in members:
            if code is not None:
                logits[i, _CLASS_IDX[code]] = rng.uniform(0.55, 0.9)
    return logits


def _train_cache(n: int = 24) -> CacheView:
    labels = [_CODES[i % 3] for i in range(n)]
    table = _cache_table(
        n=n,
        label_col="ebird_code",
        labels=labels,
        splits=["train_xc"] * n,
        logits=_correlated_logits(labels, 3, seed=11),
        seed=1,
    )
    return CacheView(table=table, logits_are_raw=False, n_classes=3)


def _metadata_for(table: pa.Table, multilabels: list[list[str]]) -> pa.Table:
    """The SEPARATE eval-label metadata table (filepath +
    ebird_code_multilabel) — real Phase A caches carry NO label column,
    so eval labels can only arrive through a table like this one."""
    return pa.table(
        {
            "filepath": table.column("filepath"),
            "ebird_code_multilabel": pa.array(
                multilabels, pa.list_(pa.string())
            ),
        }
    )


def _eval_multilabels(n: int = 20) -> list[list[str]]:
    labels: list[list[str]] = [[_CODES[i % 3]] for i in range(n)]
    labels[0] = ["amecro", "blujay"]
    labels[5] = ["blujay", "carwre"]
    return labels


def _eval_fixture(n: int = 20) -> tuple[CacheView, pa.Table]:
    """A REAL-schema eval cache (filepath/cell/week/ebird_code/split/
    embedding/logits — NO multilabel column) plus its label metadata."""
    multilabels = _eval_multilabels(n)
    table = _cache_table(
        n=n,
        label_col="ebird_code",
        labels=[item[0] for item in multilabels],
        splits=["test_hsn"] * n,
        logits=_correlated_logits(multilabels, 3, seed=23),
        seed=2,
    )
    view = CacheView(table=table, logits_are_raw=False, n_classes=3)
    return view, _metadata_for(table, multilabels)


def _flat_eval_fixture(n: int = 20) -> tuple[CacheView, pa.Table]:
    """Every score identical (0.5): the ranking is decided ENTIRELY by
    the prior. Labels cycle i%3 against the i%4 cell cycle, so labels
    VARY within each cell block — a week shuffle (which reorders rows
    only within a cell) must therefore move the metrics."""
    multilabels: list[list[str]] = [[_CODES[i % 3]] for i in range(n)]
    table = _cache_table(
        n=n,
        label_col="ebird_code",
        labels=[item[0] for item in multilabels],
        splits=["test_hsn"] * n,
        logits=np.full((n, 3), 0.5),
        seed=3,
    )
    view = CacheView(table=table, logits_are_raw=False, n_classes=3)
    return view, _metadata_for(table, multilabels)


def _cell(**overrides) -> SweepCell:
    base = dict(
        encoder="perch",
        method="rescoring",
        prior_variant="full",
        resolution=4,
        tau=1.0,
        test_set="HSN",
        seeds=(0, 1, 2, 3, 4),
    )
    base.update(overrides)
    return SweepCell(**base)


def _run(
    cell: SweepCell,
    *,
    eval_cache: CacheView | None = None,
    eval_metadata: pa.Table | None = None,
    **kwargs,
) -> dict:
    if eval_cache is None and eval_metadata is None:
        eval_cache, eval_metadata = _eval_fixture()
    return run_cell(
        cell,
        train_cache=_train_cache(),
        eval_cache=eval_cache,
        eval_metadata=eval_metadata,
        prior=_prior(),
        class_idx=_CLASS_IDX,
        code_to_gbif=_CODE_TO_GBIF,
        provenance=_PROV,
        **kwargs,
    )


_ROW_FLOATS = [
    "cmap_mean",
    "cmap_std",
    "auroc_mean",
    "auroc_std",
    "top1_mean",
    "top1_std",
    "wall_s",
]


def _fake_row(cell: SweepCell, cmap_per_seed: list[float]) -> dict:
    """A schema-complete row for write/paired_delta tests that do not
    need run_cell's arithmetic."""
    row = {
        "config_hash": config_hash(cell),
        "encoder": cell.encoder,
        "method": cell.method,
        "prior_variant": cell.prior_variant,
        "resolution": cell.resolution,
        "tau": cell.tau,
        "test_set": cell.test_set,
        "seeds": list(cell.seeds),
        "cmap_mean": float(np.mean(cmap_per_seed)),
        "cmap_std": float(np.std(cmap_per_seed)),
        "auroc_mean": 0.8,
        "auroc_std": 0.0,
        "top1_mean": 0.7,
        "top1_std": 0.0,
        "n_scored": 3,
        "prior_coverage_failures_train": None,
        "prior_coverage_failures_eval": 0,
        "species_coverage": 1.0,
        "cmap_by_freq_decile": "{}",
        "cmap_by_prior_expectation": "{}",
        "cmap_per_seed": [float(v) for v in cmap_per_seed],
        "seeds_degenerate": False,
        "git_sha": _PROV.git_sha,
        "train_cache_dir": _PROV.train_cache_dir,
        "eval_cache_dir": _PROV.eval_cache_dir,
        "prior_path": _PROV.prior_path,
        "prior_doi": _PROV.prior_doi,
        "wall_s": 1.0,
    }
    return row


# ---------------------------------------------------------------------------
# 1. config_hash — stable across runs, sensitive to EVERY field
# ---------------------------------------------------------------------------


def test_config_hash_is_stable_and_sensitive_to_every_field():
    """The hash keys the resume logic: two calls on the same cell must
    agree, and perturbing ANY single field must change it — a field the
    hash ignores would let resume skip a cell that was never run."""
    cell = _cell()
    assert config_hash(cell) == config_hash(_cell())

    perturbations = {
        "encoder": "birdnet",
        "method": "mlp",
        "prior_variant": CONTROL_VARIANTS["uniform"],
        "resolution": 5,
        "tau": 2.0,
        "test_set": "NES",
        "seeds": (5, 6, 7, 8, 9),
    }
    # Every declared field must be perturbed — a new SweepCell field
    # added without a perturbation here fails loudly.
    assert set(perturbations) == {f.name for f in dataclasses.fields(SweepCell)}
    base_hash = config_hash(cell)
    for field, value in perturbations.items():
        changed = dataclasses.replace(cell, **{field: value})
        assert config_hash(changed) != base_hash, field


# ---------------------------------------------------------------------------
# 2. run_cell refusals — provenance and the 5-seed contract
# ---------------------------------------------------------------------------


def test_run_cell_refuses_empty_prior_doi():
    """Every results artifact names its inputs; an empty (or whitespace)
    DOI is an uncitable row and is refused before any work."""
    for bad in ("", "   "):
        prov = dataclasses.replace(_PROV, prior_doi=bad)
        eval_cache, eval_metadata = _eval_fixture()
        with pytest.raises(ValueError, match="doi|DOI"):
            run_cell(
                _cell(),
                train_cache=_train_cache(),
                eval_cache=eval_cache,
                eval_metadata=eval_metadata,
                prior=_prior(),
                class_idx=_CLASS_IDX,
                code_to_gbif=_CODE_TO_GBIF,
                provenance=prov,
            )


def test_run_cell_refuses_four_seeds_and_duplicate_seeds():
    """Resume granularity is the FULL 5-seed cell: a 4-seed or
    duplicated-seed cell could manufacture a 4-seed mean, which the
    pre-flight ruling forbids persisting."""
    with pytest.raises(ValueError, match="seed"):
        _run(_cell(seeds=(0, 1, 2, 3)))
    with pytest.raises(ValueError, match="seed"):
        _run(_cell(seeds=(0, 1, 2, 3, 3)))


# ---------------------------------------------------------------------------
# 3. degenerate seeds — std 0.0 + flag; shuffles genuinely vary
# ---------------------------------------------------------------------------


def test_full_variant_rescoring_row_is_degenerate_with_zero_std():
    """Seeds only enter the control shuffles; for variant 'full' the five
    seeds are IDENTICAL by construction, so the row must say so (std 0.0,
    seeds_degenerate=True) rather than report a fake spread."""
    row = _run(_cell(prior_variant="full"))
    assert row["seeds_degenerate"] is True
    assert row["cmap_std"] == 0.0
    assert row["auroc_std"] == 0.0
    assert row["top1_std"] == 0.0
    assert len(set(row["cmap_per_seed"])) == 1
    assert len(row["cmap_per_seed"]) == 5


def test_uniform_variant_rescoring_row_is_degenerate_too():
    row = _run(_cell(prior_variant=CONTROL_VARIANTS["uniform"]))
    assert row["seeds_degenerate"] is True
    assert row["cmap_std"] == 0.0


def test_shuffle_variant_seeds_produce_genuinely_differing_cmaps():
    """On the flat-score fixture the ranking is decided entirely by the
    prior, so different shuffle draws (rng seeded per seed) must land
    different per-seed cmaps — a constant list would mean the seeds never
    reached the shuffle."""
    flat_cache, flat_meta = _flat_eval_fixture()
    row = _run(
        _cell(prior_variant=CONTROL_VARIANTS["time_shuffled"]),
        eval_cache=flat_cache,
        eval_metadata=flat_meta,
    )
    assert row["seeds_degenerate"] is False
    assert len(set(row["cmap_per_seed"])) > 1
    assert row["cmap_std"] > 0.0


# ---------------------------------------------------------------------------
# 4. write_rows — loud duplicate skip, schema version, sidecar
# ---------------------------------------------------------------------------


def test_write_rows_skips_duplicate_hash_loudly_and_versions_schema(tmp_path):
    """A row whose config_hash already exists is SKIPPED (counted in the
    returned sidecar, logged) and never duplicated on disk; every parquet
    file carries results_schema_version=2 in its metadata (bumped from 1
    when species_coverage was added for the encoder-score fallback)."""
    out = tmp_path / "results"
    r1 = _fake_row(_cell(tau=0.25), [0.5] * 5)
    r2 = _fake_row(_cell(tau=0.5), [0.6] * 5)

    sidecar1 = write_rows([r1], out)
    meta1 = json.loads(sidecar1.read_text(encoding="utf-8"))
    assert meta1["n_rows"] == 1
    assert meta1["n_skipped_existing"] == 0
    assert meta1["git_sha"] == _PROV.git_sha
    assert "written_at" in meta1

    sidecar2 = write_rows([r1, r2], out)
    meta2 = json.loads(sidecar2.read_text(encoding="utf-8"))
    assert meta2["n_rows"] == 1
    assert meta2["n_skipped_existing"] == 1

    files = sorted(out.glob("*.parquet"))
    tables = [pq.read_table(p) for p in files]
    hashes = [h for t in tables for h in t.column("config_hash").to_pylist()]
    assert sorted(hashes) == sorted([r1["config_hash"], r2["config_hash"]])
    for path in files:
        meta = pq.read_schema(path).metadata or {}
        assert meta[b"results_schema_version"] == b"2"


# ---------------------------------------------------------------------------
# 5. resume — only missing hashes are attempted
# ---------------------------------------------------------------------------


def test_resume_attempts_only_missing_hashes(tmp_path):
    """Write a grid subset, then re-plan the full grid: the cells left to
    attempt are exactly the ones whose hashes are not on disk."""
    out = tmp_path / "results"
    grid = plan_grid(
        encoders=("perch",),
        methods=("rescoring",),
        prior_variants=("full",),
        resolutions=(4,),
        taus=(0.25, 0.5, 1.0, 2.0),
        test_sets=("HSN",),
    )
    assert len(grid) == 4
    done, missing = grid[:2], grid[2:]
    write_rows([_fake_row(c, [0.5] * 5) for c in done], out)

    existing = load_existing_hashes(out)
    attempted = [c for c in plan_grid(
        encoders=("perch",),
        methods=("rescoring",),
        prior_variants=("full",),
        resolutions=(4,),
        taus=(0.25, 0.5, 1.0, 2.0),
        test_sets=("HSN",),
    ) if config_hash(c) not in existing]
    assert attempted == missing


def test_load_existing_hashes_of_absent_dir_is_empty(tmp_path):
    assert load_existing_hashes(tmp_path / "nope") == set()


# ---------------------------------------------------------------------------
# 6. paired_delta — hand-computed fixture + refusals
# ---------------------------------------------------------------------------


def test_paired_delta_matches_hand_computed_ci():
    """Hand-computed: a=[.50,.60,.55,.45,.65], b=[.40,.50,.50,.40,.55]
    -> deltas [.10,.10,.05,.05,.10], mean .08,
    sd(ddof=1) = sqrt(0.003/4) = 0.0273861,
    half-width = t_{0.975,4} * sd/sqrt(5) = 2.7764 * 0.0122474 = 0.0340050,
    CI = (0.0459950, 0.1140050)."""
    a = _fake_row(_cell(tau=1.0), [0.50, 0.60, 0.55, 0.45, 0.65])
    b = _fake_row(_cell(tau=0.5), [0.40, 0.50, 0.50, 0.40, 0.55])
    mean, (lo, hi) = paired_delta([a], [b])
    assert mean == pytest.approx(0.08)
    assert lo == pytest.approx(0.0459950, abs=1e-4)
    assert hi == pytest.approx(0.1140050, abs=1e-4)


def test_paired_delta_refuses_mismatched_axes():
    """Rows differing on MORE than the one axis under comparison are not
    a paired comparison at all."""
    a = _fake_row(_cell(tau=1.0, test_set="HSN"), [0.5] * 5)
    b = _fake_row(_cell(tau=0.5, test_set="NES"), [0.4] * 5)
    with pytest.raises(ValueError, match="ax"):
        paired_delta([a], [b])


def test_paired_delta_refuses_non_identical_seed_tuples():
    a = _fake_row(_cell(tau=1.0), [0.5] * 5)
    b = _fake_row(_cell(tau=0.5, seeds=(1, 2, 3, 4, 5)), [0.4] * 5)
    with pytest.raises(ValueError, match="seed"):
        paired_delta([a], [b])


# ---------------------------------------------------------------------------
# 7. end-to-end micro-sweep — 4 cells through the real run_cell
# ---------------------------------------------------------------------------


def test_micro_sweep_four_cells_end_to_end(tmp_path):
    """Tiny caches + tiny prior -> four real cells (rescoring tau=1
    full + uniform, offset_global full, mlp full), rows land on disk,
    every metric finite, provenance populated. This is the test that
    catches interface drift between Tasks 1-5 and the driver."""
    cells = [
        _cell(method="rescoring", prior_variant="full", tau=1.0),
        _cell(
            method="rescoring",
            prior_variant=CONTROL_VARIANTS["uniform"],
            tau=1.0,
        ),
        _cell(method="offset_global", prior_variant="full", tau=None),
        _cell(method="mlp", prior_variant="full", tau=None),
    ]
    rows = [_run(c, mlp_hidden=8, mlp_max_iter=40) for c in cells]

    out = tmp_path / "results"
    write_rows(rows, out)
    on_disk = pa.concat_tables(
        [pq.read_table(p) for p in sorted(out.glob("*.parquet"))]
    )
    assert on_disk.num_rows == 4
    assert set(on_disk.column("config_hash").to_pylist()) == {
        config_hash(c) for c in cells
    }
    # I2: the parquet schema carries the SPLIT coverage columns (train
    # nullable — null for rescoring) and never the old merged column.
    assert "prior_coverage_failures_train" in on_disk.column_names
    assert "prior_coverage_failures_eval" in on_disk.column_names
    assert "prior_coverage_failures" not in on_disk.column_names
    # Ruling (b): every row reports the fitted-species fraction; the
    # full-coverage fixtures here are all 1.0.
    assert "species_coverage" in on_disk.column_names
    assert on_disk.column("species_coverage").to_pylist() == [1.0] * 4
    # I1: the strata columns land on disk too.
    assert "cmap_by_freq_decile" in on_disk.column_names
    assert "cmap_by_prior_expectation" in on_disk.column_names

    for cell, row in zip(cells, rows):
        assert row["config_hash"] == config_hash(cell)
        for key in _ROW_FLOATS:
            assert np.isfinite(row[key]), (cell.method, key)
        assert len(row["cmap_per_seed"]) == 5
        assert all(np.isfinite(v) for v in row["cmap_per_seed"])
        assert row["n_scored"] > 0
        # I2: coverage split — train is None (no training lookup) for
        # rescoring, an int for learned arms; eval is always counted.
        if cell.method == "rescoring":
            assert row["prior_coverage_failures_train"] is None
        else:
            assert row["prior_coverage_failures_train"] == 0
        assert row["prior_coverage_failures_eval"] == 0
        # I1: every row carries both strata, JSON-encoded, with finite
        # cmap values wherever a stratum could be scored.
        for col in ("cmap_by_freq_decile", "cmap_by_prior_expectation"):
            strata = json.loads(row[col])
            assert strata, (cell.method, col)
            for key, (value, n_sc) in strata.items():
                assert n_sc >= 0
                if value is not None:
                    assert np.isfinite(value), (cell.method, col, key)
        assert row["seeds"] == list(cell.seeds)
        assert row["wall_s"] > 0.0
        assert row["git_sha"] == _PROV.git_sha
        assert row["train_cache_dir"] == _PROV.train_cache_dir
        assert row["eval_cache_dir"] == _PROV.eval_cache_dir
        assert row["prior_path"] == _PROV.prior_path
        assert row["prior_doi"] == _DOI


def test_sigmoid_applied_exactly_once_iff_cache_declares_raw_logits():
    """A raw-logits eval cache and a pre-sigmoided one describing the
    same scores must produce the SAME row metrics: sigmoid exactly once,
    decided by the cache's declared flag, never by value inspection."""
    calibrated, metadata = _eval_fixture()
    probs = np.asarray(calibrated.table.column("logits").to_pylist())
    raw_logits = np.log(probs / (1.0 - probs))
    raw_table = calibrated.table.set_column(
        calibrated.table.column_names.index("logits"),
        "logits",
        pa.array(raw_logits.tolist(), pa.list_(pa.float64())),
    )
    raw = CacheView(table=raw_table, logits_are_raw=True, n_classes=3)
    row_cal = _run(_cell(), eval_cache=calibrated, eval_metadata=metadata)
    row_raw = _run(_cell(), eval_cache=raw, eval_metadata=metadata)
    assert row_raw["cmap_mean"] == pytest.approx(row_cal["cmap_mean"])
    assert row_raw["auroc_mean"] == pytest.approx(row_cal["auroc_mean"])


# ---------------------------------------------------------------------------
# 8. learned-arm seeding — MLP varies by seed, offset does not
# ---------------------------------------------------------------------------


def test_mlp_rows_differ_across_seeds_same_machine():
    """The seed enters the MLP's weight init and batch shuffling, so two
    seeds must produce different per-seed metrics on the same machine —
    identical values would mean the seed never reached the fit. (The
    determinism contract is same-machine only; nothing here compares
    across platforms.)"""
    row = _run(
        _cell(method="mlp", prior_variant="full", tau=None),
        mlp_hidden=8,
        mlp_max_iter=15,
    )
    assert row["seeds_degenerate"] is False
    assert len(set(row["cmap_per_seed"])) > 1
    assert row["cmap_std"] > 0.0


def test_mlp_marginal_cell_with_nan_prior_raises_named_error_not_sklearn(
):
    """The fleet's r5+marginal+UHH crash, reproduced in miniature: ONE
    NaN stored in the prior artifact (load_prior never re-validates p)
    is spread across that species' ENTIRE row by the marginal control
    (a mean over any axis containing a NaN is NaN), survives the
    floor (np.maximum(nan, FLOOR) is nan) and used to reach
    MLPClassifier as 'Input X contains NaN'. It must now be refused by
    OUR feature-path guard, naming the NaN count — not sklearn's
    message, which names nothing an operator can act on."""
    good = _prior()
    d = good.values.toarray()
    d[0, 5] = np.nan  # one corrupt stored entry, species 0
    bad = PriorTensor(
        spec=good.spec,
        species_keys=good.species_keys,
        cells=list(good.cells),
        values=sparse.csr_matrix(d),
        gbif_doi=good.gbif_doi,
    )
    eval_cache, metadata = _eval_fixture()
    with pytest.raises(ValueError, match=r"NaN prior probability"):
        run_cell(
            _cell(
                method="mlp",
                prior_variant=CONTROL_VARIANTS["marginal"],
                tau=None,
            ),
            train_cache=_train_cache(),
            eval_cache=eval_cache,
            eval_metadata=metadata,
            prior=bad,
            class_idx=_CLASS_IDX,
            code_to_gbif=_CODE_TO_GBIF,
            provenance=_PROV,
            mlp_hidden=4,
            mlp_max_iter=20,
        )


def test_offset_rows_identical_across_seeds_degenerate_flagged():
    """The offset fit is a convex 1-D minimization with no randomness:
    on variant 'full' every seed is the same fit, so the row must report
    std 0.0 + seeds_degenerate=True, never a fake spread."""
    row = _run(_cell(method="offset_global", prior_variant="full", tau=None))
    assert row["seeds_degenerate"] is True
    assert row["cmap_std"] == 0.0
    assert len(set(row["cmap_per_seed"])) == 1


# ---------------------------------------------------------------------------
# plan_grid + sharding — deterministic, deduplicated, stable partition
# ---------------------------------------------------------------------------


def test_plan_grid_default_shape_vocabulary_and_determinism():
    """2 encoders x [rescoring x 5 variants x 3 res x 4 taus + 3 learned
    methods x 5 variants x 3 res] x 6 test sets = 1260 cells, no
    duplicates, same order on every call, variant vocabulary IMPORTED
    from CONTROL_VARIANTS."""
    grid = plan_grid()
    assert len(grid) == 1260
    assert len(set(grid)) == len(grid)
    assert grid == plan_grid()

    variants = {c.prior_variant for c in grid}
    assert variants == {"full"} | set(CONTROL_VARIANTS.values())
    for cell in grid:
        if cell.method == "rescoring":
            assert cell.tau in (0.25, 0.5, 1.0, 2.0)
        else:
            assert cell.tau is None
        assert cell.seeds == (0, 1, 2, 3, 4)


def test_shard_cells_is_a_stable_partition():
    grid = plan_grid(
        encoders=("perch",),
        methods=("rescoring",),
        resolutions=(4,),
        test_sets=("HSN", "NES"),
    )
    shards = [shard_cells(grid, k, 3) for k in range(3)]
    combined = [c for shard in shards for c in shard]
    assert sorted(combined, key=config_hash) == sorted(grid, key=config_hash)
    assert len(combined) == len(grid)
    assert shards[0] == shard_cells(grid, 0, 3)
    with pytest.raises(ValueError):
        shard_cells(grid, 3, 3)
    with pytest.raises(ValueError):
        shard_cells(grid, -1, 3)


# ---------------------------------------------------------------------------
# 9. C1 — the eval-label seam: labels come from SEPARATE metadata, joined
#    by filepath, with repro_gate's missing-recording refusal
# ---------------------------------------------------------------------------


def test_run_cell_works_on_real_schema_cache_without_label_column():
    """Real Phase A caches carry filepath/cell/week/ebird_code/split/
    embedding[/logits] and NO ebird_code_multilabel column; the eval
    labels arrive in a separate metadata table joined BY FILEPATH. A
    run_cell that reads labels off the cache table itself fails on every
    real cell — this fixture has no label column to read."""
    eval_cache, metadata = _eval_fixture()
    assert (
        "ebird_code_multilabel" not in eval_cache.table.column_names
    ), "fixture must mirror the REAL Phase A schema (labels only in metadata)"
    row = _run(_cell(), eval_cache=eval_cache, eval_metadata=metadata)
    assert np.isfinite(row["cmap_mean"])
    assert row["n_scored"] > 0


def test_run_cell_joins_labels_by_filepath_not_position():
    """Shuffling the metadata table's row order must not change the row
    metrics: the join is BY FILEPATH, never positional."""
    eval_cache, metadata = _eval_fixture()
    perm = np.random.default_rng(7).permutation(metadata.num_rows)
    shuffled = metadata.take(pa.array(perm))
    row_a = _run(_cell(), eval_cache=eval_cache, eval_metadata=metadata)
    row_b = _run(_cell(), eval_cache=eval_cache, eval_metadata=shuffled)
    assert row_a["cmap_mean"] == pytest.approx(row_b["cmap_mean"])
    assert row_a["auroc_mean"] == pytest.approx(row_b["auroc_mean"])


def test_run_cell_refuses_cache_recording_missing_from_metadata():
    """repro_gate.score_pair's refusal, carried over: a cache row whose
    filepath is absent from the metadata raises NAMING examples — a
    silent inner join would quietly shrink the test set."""
    eval_cache, metadata = _eval_fixture()
    truncated = metadata.slice(0, metadata.num_rows - 2)
    with pytest.raises(ValueError, match=r"missing from.*metadata|metadata"):
        _run(_cell(), eval_cache=eval_cache, eval_metadata=truncated)
    # The error names at least one offending filepath.
    try:
        _run(_cell(), eval_cache=eval_cache, eval_metadata=truncated)
    except ValueError as exc:
        assert "r18.ogg" in str(exc) or "r19.ogg" in str(exc)


# ---------------------------------------------------------------------------
# 10. M1 — symmetric eval-side split guard
# ---------------------------------------------------------------------------


def test_run_cell_refuses_train_split_rows_in_eval_cache():
    """Symmetric to guard_training_split: an eval table whose split
    values start with 'train' is a focal/training cache handed to the
    eval slot — scoring it would 'evaluate' on training data."""
    multilabels = _eval_multilabels()
    n = len(multilabels)
    table = _cache_table(
        n=n,
        label_col="ebird_code",
        labels=[item[0] for item in multilabels],
        splits=["train_xc"] * n,
        logits=_correlated_logits(multilabels, 3, seed=23),
        seed=2,
    )
    bad_eval = CacheView(table=table, logits_are_raw=False, n_classes=3)
    metadata = _metadata_for(table, multilabels)
    with pytest.raises(ValueError, match="train"):
        _run(_cell(), eval_cache=bad_eval, eval_metadata=metadata)


# ---------------------------------------------------------------------------
# 11. I1 — stratified columns: frequency deciles + expected-under-prior
# ---------------------------------------------------------------------------


def test_strata_columns_are_deterministic_and_well_formed():
    """The decile boundaries derive from eval label prevalence alone, so
    two identical runs must produce byte-identical strata JSON; the
    expected-under-prior stratum keys are exactly the two booleans (or a
    subset when one side is empty)."""
    eval_cache, metadata = _eval_fixture()
    row_a = _run(_cell(), eval_cache=eval_cache, eval_metadata=metadata)
    row_b = _run(_cell(), eval_cache=eval_cache, eval_metadata=metadata)
    assert row_a["cmap_by_freq_decile"] == row_b["cmap_by_freq_decile"]
    assert (
        row_a["cmap_by_prior_expectation"]
        == row_b["cmap_by_prior_expectation"]
    )

    deciles = json.loads(row_a["cmap_by_freq_decile"])
    assert deciles  # at least one populated decile
    for key, (value, n_sc) in deciles.items():
        assert int(key) in range(10)
        if value is not None:
            assert np.isfinite(value)
            assert n_sc > 0

    expectation = json.loads(row_a["cmap_by_prior_expectation"])
    assert set(expectation) <= {"expected", "unexpected"}
    assert expectation  # never empty: every class lands in one stratum
    total = sum(n_sc for _v, n_sc in expectation.values())
    assert total == row_a["n_scored"]


def test_fit_group_key_excludes_test_set_and_tau():
    """Cells differing ONLY in test_set/tau share a fit group — one fit
    serves six test sets; a resolution change does not."""
    assert fit_group_key(_cell(method="mlp", test_set="HSN", tau=None)) == \
        fit_group_key(_cell(method="mlp", test_set="NES", tau=None))
    assert fit_group_key(_cell(tau=0.5)) == fit_group_key(_cell(tau=2.0))
    assert fit_group_key(_cell(method="mlp")) != \
        fit_group_key(_cell(method="mlp", resolution=5))


def test_shard_cells_grouped_disjoint_cover_and_group_integrity():
    """Every cell lands in exactly one shard AND every fit group lives in
    exactly one shard — a split group would refit the same model twice."""
    cells = []
    for ts in ("HSN", "NES", "PER"):
        for res in (3, 4, 5):
            cells.append(_cell(method="mlp", test_set=ts, resolution=res,
                               tau=None))
            cells.append(_cell(method="rescoring", test_set=ts,
                               resolution=res, tau=1.0))
    n_shards = 4
    shards = [shard_cells_grouped(cells, k, n_shards) for k in range(n_shards)]
    seen = [config_hash(c) for sh in shards for c in sh]
    assert sorted(seen) == sorted(config_hash(c) for c in cells)
    assert len(seen) == len(set(seen))
    for key in {fit_group_key(c) for c in cells}:
        owners = [
            k for k, sh in enumerate(shards)
            if any(fit_group_key(c) == key for c in sh)
        ]
        assert len(owners) == 1, f"group split across shards {owners}"


def test_run_cell_fit_cache_shares_mlp_fit_across_test_sets(monkeypatch):
    """Two cells differing only in test_set fit ONCE per seed through a
    shared fit_cache — pinned by counting fit_mlp_head calls."""
    import birdology.phase_b.sweep as sweep_mod

    calls = {"n": 0}
    real_fit = sweep_mod.fit_mlp_head

    def counting_fit(*a, **k):
        calls["n"] += 1
        return real_fit(*a, **k)

    monkeypatch.setattr(sweep_mod, "fit_mlp_head", counting_fit)
    eval_cache, metadata = _eval_fixture()
    cache: dict = {}
    for ts in ("HSN", "NES"):
        _run(
            _cell(method="mlp", test_set=ts, tau=None),
            eval_cache=eval_cache,
            eval_metadata=metadata,
            mlp_hidden=4,
            mlp_max_iter=20,
            fit_cache=cache,
        )
    n_seeds = len(_cell().seeds)
    assert calls["n"] == n_seeds, (
        f"expected {n_seeds} fits (one per seed, shared across test sets), "
        f"got {calls['n']}"
    )


# ---------------------------------------------------------------------------
# 12. non-finite embeddings — dropped at CACHE LOAD, so 5 bad Perch rows
#     (the measured XCL fact) cannot kill a cell the way a NaN prior is
#     refused. db0264b's repro, re-armed with NaN EMBEDDINGS.
# ---------------------------------------------------------------------------


def _with_nan_embeddings(table: pa.Table, rows: list[int]) -> pa.Table:
    """The fixture table with the given rows' embeddings made all-NaN —
    exactly the measured XCL_perch shape (all 1536 dims NaN, here all d)."""
    emb = table.column("embedding").to_pylist()
    for i in rows:
        emb[i] = [float("nan")] * len(emb[i])
    return table.set_column(
        table.column_names.index("embedding"),
        "embedding",
        pa.array(emb, pa.list_(pa.float64())),
    )


def _view_via_load_cache(table: pa.Table, tmp_path: Path, name: str) -> CacheView:
    """Round-trip an in-memory fixture through a real on-disk shard and
    load_cache — the ONLY path real fleet caches take, and the layer
    that owns the non-finite-embedding drop."""
    d = tmp_path / name
    d.mkdir()
    pq.write_table(
        table.replace_schema_metadata(
            {b"schema_variant": b"embedding+logits"}
        ),
        d / "shard_0000.parquet",
    )
    return load_cache(d)


def test_mlp_cell_with_nan_train_embeddings_completes_after_counted_drop(
    tmp_path, caplog
):
    """Train side: 2 all-NaN embedding rows of 240 (well under the
    broken-cache threshold) are dropped at load with ONE warning
    counting 2 and naming both filepaths; the fit proceeds on the
    remaining rows and the cell completes with finite metrics —
    instead of sklearn killing the whole cell with 'Input X contains
    NaN'."""
    train_table = _with_nan_embeddings(_train_cache(240).table, [5, 17])
    with caplog.at_level("WARNING", logger="birdology.phase_b.cache"):
        train = _view_via_load_cache(train_table, tmp_path, "train")
    assert train.table.num_rows == 238
    eval_cache, metadata = _eval_fixture()
    row = run_cell(
        _cell(method="mlp", prior_variant="full", tau=None),
        train_cache=train,
        eval_cache=eval_cache,
        eval_metadata=metadata,
        prior=_prior(),
        class_idx=_CLASS_IDX,
        code_to_gbif=_CODE_TO_GBIF,
        provenance=_PROV,
        mlp_hidden=4,
        mlp_max_iter=20,
    )
    assert np.isfinite(row["cmap_mean"])
    assert row["n_scored"] > 0
    records = [r for r in caplog.records if "non-finite" in r.getMessage()]
    assert len(records) == 1, "ONE counted warning per cache, not per row"
    msg = records[0].getMessage()
    assert "2 of 240" in msg
    assert "r5.ogg" in msg and "r17.ogg" in msg


def test_eval_nan_embedding_row_dropped_metrics_over_remaining_rows(
    tmp_path, caplog
):
    """Eval side: a NaN-embedding eval row cannot be scored by ANY arm,
    so it is dropped THE SAME WAY (shrinking the metric denominator
    honestly, never zero-filled) and counted in its own per-cache
    warning — separate from any train-side count because each cache
    dir's load names itself. The metrics must equal a clean run over
    exactly the surviving rows."""
    n = 200
    multilabels: list[list[str]] = [[_CODES[i % 3]] for i in range(n)]
    table = _cache_table(
        n=n,
        label_col="ebird_code",
        labels=[item[0] for item in multilabels],
        splits=["test_hsn"] * n,
        logits=_correlated_logits(multilabels, 3, seed=23),
        seed=2,
    )
    metadata = _metadata_for(table, multilabels)
    poisoned = _with_nan_embeddings(table, [7])
    with caplog.at_level("WARNING", logger="birdology.phase_b.cache"):
        loaded = _view_via_load_cache(poisoned, tmp_path, "eval")
    assert loaded.table.num_rows == 199
    assert "r7.ogg" not in loaded.table.column("filepath").to_pylist()

    row = _run(_cell(), eval_cache=loaded, eval_metadata=metadata)
    # Oracle: the SAME eval set minus the bad row, handed in clean —
    # the dropped-row run must be indistinguishable from it.
    keep = [i for i in range(n) if i != 7]
    clean = CacheView(
        table=table.take(pa.array(keep)), logits_are_raw=False, n_classes=3
    )
    row_clean = _run(_cell(), eval_cache=clean, eval_metadata=metadata)
    assert row["cmap_mean"] == pytest.approx(row_clean["cmap_mean"])
    assert row["auroc_mean"] == pytest.approx(row_clean["auroc_mean"])
    assert row["n_scored"] == row_clean["n_scored"]

    records = [r for r in caplog.records if "non-finite" in r.getMessage()]
    assert len(records) == 1
    msg = records[0].getMessage()
    assert "1 of 200" in msg
    assert "r7.ogg" in msg
    assert "eval" in msg  # the warning names WHICH cache dropped rows


def test_mlp_marginal_cell_with_nan_embeddings_completes_with_drop_warning(
    tmp_path, caplog
):
    """db0264b's fleet repro, re-armed: NaN EMBEDDINGS instead of a NaN
    prior. The prior is clean, so _log_floored's refusal stays silent;
    the NaN would ride the embedding block of the MLP's X straight into
    sklearn. The load-side drop must let the cell COMPLETE — with the
    counted per-cache warnings (train and eval counted separately) —
    rather than refuse: a handful of degenerate recordings must not
    kill a whole cell."""
    n = 200
    multilabels: list[list[str]] = [[_CODES[i % 3]] for i in range(n)]
    eval_table = _cache_table(
        n=n,
        label_col="ebird_code",
        labels=[item[0] for item in multilabels],
        splits=["test_hsn"] * n,
        logits=_correlated_logits(multilabels, 3, seed=23),
        seed=2,
    )
    metadata = _metadata_for(eval_table, multilabels)
    with caplog.at_level("WARNING", logger="birdology.phase_b.cache"):
        train = _view_via_load_cache(
            _with_nan_embeddings(_train_cache(240).table, [5, 17]),
            tmp_path,
            "train2",
        )
        eval_cache = _view_via_load_cache(
            _with_nan_embeddings(eval_table, [3]), tmp_path, "eval2"
        )
    row = run_cell(
        _cell(
            method="mlp",
            prior_variant=CONTROL_VARIANTS["marginal"],
            tau=None,
        ),
        train_cache=train,
        eval_cache=eval_cache,
        eval_metadata=metadata,
        prior=_prior(),
        class_idx=_CLASS_IDX,
        code_to_gbif=_CODE_TO_GBIF,
        provenance=_PROV,
        mlp_hidden=4,
        mlp_max_iter=20,
    )
    assert np.isfinite(row["cmap_mean"])
    assert np.isfinite(row["auroc_mean"])
    records = [r for r in caplog.records if "non-finite" in r.getMessage()]
    assert len(records) == 2, "one counted warning per cache (train + eval)"
    by_msg = {("train2" if "train2" in r.getMessage() else "eval2"): r.getMessage()
              for r in records}
    assert "2 of 240" in by_msg["train2"]
    assert "1 of 200" in by_msg["eval2"]
