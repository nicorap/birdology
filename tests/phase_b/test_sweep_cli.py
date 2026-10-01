"""main()-level tests for scripts/phase_b_sweep.py (C1 regression).

These run the REAL CLI end to end on tiny on-disk fixtures whose cache
parquets carry the REAL Phase A schema — ``filepath / cell / week /
ebird_code / split / embedding / logits`` and NO ``ebird_code_multilabel``
column (see phase_a/embed.py's EMBEDDING_SCHEMA). The multilabel ground
truth lives ONLY in a separate per-test-set metadata parquet, wired in
through ``--eval-labels``: exactly the seam the whole-branch review found
broken (Task 6 assumed a label column real caches never carry).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

import phase_b_sweep as cli
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import PriorSpec, PriorTensor, save_prior

_CODES = ["amecro", "blujay", "carwre"]
_CELLS = ["cella", "cellb", "cellc", "celld"]
_DOI = "10.15468/dl.test00"


def _phase_a_table(*, n: int, split: str, seed: int) -> pa.Table:
    """A shard in the REAL Phase A cache schema: filepath / cell / week /
    ebird_code / split / embedding / logits — no label-matrix column."""
    rng = np.random.default_rng(seed)
    codes = [_CODES[i % 3] for i in range(n)]
    logits = rng.uniform(0.15, 0.45, size=(n, 3))
    for i, code in enumerate(codes):
        logits[i, _CODES.index(code)] = rng.uniform(0.55, 0.9)
    embeddings = rng.normal(size=(n, 6))
    return pa.table(
        {
            "filepath": pa.array(
                [f"{split}_r{i}.ogg" for i in range(n)], pa.string()
            ),
            "cell": pa.array(
                [_CELLS[i % len(_CELLS)] for i in range(n)], pa.string()
            ),
            "week": pa.array([1 + (i % 13) for i in range(n)], pa.int8()),
            "ebird_code": pa.array(codes, pa.string()),
            "split": pa.array([split] * n, pa.string()),
            "embedding": pa.array(
                embeddings.tolist(), pa.list_(pa.float32())
            ),
            "logits": pa.array(logits.tolist(), pa.list_(pa.float32())),
        }
    )


def _metadata_table(cache_table: pa.Table) -> pa.Table:
    """The per-test-set metadata parquet: filepath + multilabel labels
    (here: the single focal code wrapped in a list)."""
    return pa.table(
        {
            "filepath": cache_table.column("filepath"),
            "ebird_code_multilabel": pa.array(
                [[c] for c in cache_table.column("ebird_code").to_pylist()],
                pa.list_(pa.string()),
            ),
        }
    )


def _write_prior(path: Path) -> None:
    n_species, n_cols = 3, len(_CELLS) * WEEKS_PER_YEAR
    dense = (1.0 + np.arange(n_species * n_cols, dtype=np.float64)).reshape(
        n_species, n_cols
    ) / (n_species * n_cols + 1.0)
    tensor = PriorTensor(
        spec=PriorSpec(resolution=4, source="eod", variant="full"),
        species_keys=np.asarray([111, 222, 333], dtype=np.int64),
        cells=list(_CELLS),
        values=sparse.csr_matrix(dense),
        gbif_doi=_DOI,
    )
    save_prior(tensor, path)


@pytest.fixture()
def cli_workspace(tmp_path: Path) -> dict:
    """Real-schema on-disk fixtures for one (perch, HSN) sweep cell."""
    train_dir = tmp_path / "caches" / "train" / "perch"
    eval_dir = tmp_path / "caches" / "eval" / "perch" / "HSN"
    train_dir.mkdir(parents=True)
    eval_dir.mkdir(parents=True)

    train_table = _phase_a_table(n=24, split="train_xc", seed=11)
    eval_table = _phase_a_table(n=20, split="test_hsn", seed=23)
    pq.write_table(train_table, train_dir / "shard0.parquet")
    pq.write_table(eval_table, eval_dir / "shard0.parquet")

    meta_dir = tmp_path / "metadata"
    meta_dir.mkdir()
    metadata_path = meta_dir / "HSN_metadata.parquet"
    pq.write_table(_metadata_table(eval_table), metadata_path)

    _write_prior(tmp_path / "prior_res4.npz")
    labels_path = tmp_path / "labels.txt"
    labels_path.write_text("\n".join(_CODES) + "\n", encoding="utf-8")
    bridge_path = tmp_path / "bridge.json"
    bridge_path.write_text(
        json.dumps({"amecro": 111, "blujay": 222, "carwre": 333}),
        encoding="utf-8",
    )
    return {
        "tmp": tmp_path,
        "metadata_path": metadata_path,
        "eval_table": eval_table,
        "argv": [
            "--train-cache", str(tmp_path / "caches" / "train" / "{encoder}"),
            "--eval-cache",
            str(tmp_path / "caches" / "eval" / "{encoder}" / "{test_set}"),
            "--eval-labels", str(meta_dir / "{test_set}_metadata.parquet"),
            "--prior", str(tmp_path / "prior_res{resolution}.npz"),
            "--labels", str(labels_path),
            "--bridge", str(bridge_path),
            "--out", str(tmp_path / "results"),
            "--git-sha", "deadbeefcafe",
            "--encoders", "perch",
            "--methods", "rescoring",
            "--prior-variants", "full",
            "--resolutions", "4",
            "--taus", "1.0",
            "--test-sets", "HSN",
        ],
    }


def test_main_runs_one_cell_on_real_schema_caches(cli_workspace):
    """The CLI completes a real-schema cell end to end: exit 0, one row
    on disk with the strata + split-coverage columns, provenance DOI
    from the prior sidecar."""
    assert cli.main(cli_workspace["argv"]) == 0
    out = cli_workspace["tmp"] / "results"
    table = pa.concat_tables(
        [pq.read_table(p) for p in sorted(out.glob("*.parquet"))]
    )
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert np.isfinite(row["cmap_mean"])
    assert row["prior_doi"] == _DOI
    assert row["prior_coverage_failures_train"] is None  # rescoring
    assert row["prior_coverage_failures_eval"] == 0
    assert json.loads(row["cmap_by_freq_decile"])
    assert json.loads(row["cmap_by_prior_expectation"])
    assert row["species_coverage"] == 1.0
    # Per-shard failures files (shard 0 here): no failures, no file —
    # and never the old shared failures.json that last-shard-wins
    # clobbered on the fleet.
    assert not (out / "failures_shard00.json").exists()
    assert not (out / "failures.json").exists()


def test_main_records_failure_when_metadata_misses_a_recording(cli_workspace):
    """Truncate the metadata so one cache filepath has no labels: the
    cell FAILS (C1's refusal, surfaced through the CLI), the PER-SHARD
    failures file names it, exit code 1 — never a silently shrunken test
    set, and never a shared failures.json two shards overwrite."""
    meta = pq.read_table(cli_workspace["metadata_path"])
    pq.write_table(
        meta.slice(0, meta.num_rows - 1), cli_workspace["metadata_path"]
    )
    assert cli.main(cli_workspace["argv"]) == 1
    out = cli_workspace["tmp"] / "results"
    assert not (out / "failures.json").exists()
    failures = json.loads(
        (out / "failures_shard00.json").read_text(encoding="utf-8")
    )
    assert len(failures) == 1
    assert "missing from" in failures[0]["error"]
    # No results row was written for the failed cell.
    assert not list((cli_workspace["tmp"] / "results").glob("*.parquet"))


def test_failures_filename_carries_the_grid_shard_index(cli_workspace):
    """Shard 3 of 4 writes failures_shard03.json: each shard owns its
    own failures file, so a 5-shard fleet keeps 5 failure reports
    instead of whichever shard finished last."""
    meta = pq.read_table(cli_workspace["metadata_path"])
    pq.write_table(
        meta.slice(0, meta.num_rows - 1), cli_workspace["metadata_path"]
    )
    argv_base = cli_workspace["argv"]
    # One cell in the restricted grid: it lands in exactly one shard.
    # Run all four shards; only the owning shard fails and writes.
    codes = [
        cli.main(argv_base + ["--grid-shard", str(k), "--n-shards", "4"])
        for k in range(4)
    ]
    assert codes.count(1) == 1
    owner = codes.index(1)
    out = cli_workspace["tmp"] / "results"
    shard_files = sorted(out.glob("failures_shard*.json"))
    assert [p.name for p in shard_files] == [f"failures_shard{owner:02d}.json"]
    assert not (out / "failures.json").exists()
