#!/usr/bin/env python3
"""Run the Phase B factorial sweep — a thin CLI over `birdology.phase_b.sweep`.

    python scripts/phase_b_sweep.py \
        --train-cache 'caches/train/{encoder}' \
        --eval-cache 'caches/eval/{encoder}/{test_set}' \
        --eval-labels 'metadata/{test_set}_metadata.parquet' \
        --train-locations manifests/XCL_r4_manifest.parquet \
        --eval-locations 'manifests/{test_set}_test_5s_r4_manifest.parquet' \
        --prior 'output/priors/prior_{source}_res{resolution}.npz' \
        --labels labels/perch_labels.txt \
        --bridge bridges/ebird_to_gbif.json \
        --out output/sweep_results \
        --grid-shard 0 --n-shards 4

The `{encoder}` / `{test_set}` / `{resolution}` placeholders are filled per
cell; inputs are loaded ONCE per shard (memoised by resolved path), then
every cell in the shard runs, skipping hashes already on disk.

EVAL LABELS: real Phase A caches carry NO ebird_code_multilabel column —
the ground truth lives in each test set's metadata parquet (`filepath` +
`ebird_code_multilabel`). `--eval-labels` is a per-test-set path template
({test_set} placeholder); `run_cell` joins it to the cache BY FILEPATH
and refuses any cache recording the metadata does not cover.

EXIT CODE: 0 iff every attempted cell either wrote its row or was
skipped-existing. A FAILED CELL NEVER KILLS THE SHARD: its error is
recorded in `failures_shard{NN}.json` under `--out` (one file PER
GRID SHARD — a single shared failures.json would be overwritten by
whichever shard finished last; the fleet lost 4 of 5 shards' reports
that way), one entry per failed cell naming the cell and the error, a
per-cell summary is printed, and the process exits 1 — so one bad cache
path costs one cell, not a night of compute, while the failure stays
loud enough to block a merge.

LOCATIONS: the caches' own `cell` column is H3 resolution 4 ONLY, so it
cannot answer an r3/r5 prior (fleet job 7022 — the resolution axis never
varied). `--train-locations` / `--eval-locations` point at the Phase A
manifests (`filepath, lat, lon`, ...; {test_set} placeholder on the eval
side); `run_cell` joins them to the cache rows BY FILEPATH and derives
each row's cell at the cell's resolution with the prior's own
`latlng_to_cell` call, refusing a missing/null location or any row whose
derived r4 cell disagrees with the cache's r4 `cell`.

The results rows carry full provenance (git SHA, cache dirs, prior path,
prior DOI). The DOI comes FROM THE PRIOR SIDECAR — `run_cell` refuses an
empty one, so an uncitable artifact can never produce a results row.
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.phase_b.cache import CacheView, class_index, load_cache
from birdology.phase_b.sweep import (
    Provenance,
    SweepCell,
    config_hash,
    load_existing_hashes,
    plan_grid,
    run_cell,
    shard_cells_grouped,
    write_rows,
)
from birdology.prior.tensor import PriorTensor, load_prior

logger = logging.getLogger("phase_b_sweep")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--train-cache",
        required=True,
        help="train cache dir template; may contain {encoder}",
    )
    parser.add_argument(
        "--eval-cache",
        required=True,
        help="eval cache dir template; may contain {encoder} and {test_set}",
    )
    parser.add_argument(
        "--eval-labels",
        required=True,
        help=(
            "eval-label metadata parquet path template (filepath + "
            "ebird_code_multilabel), one per test set; may contain "
            "{test_set}. Real Phase A caches carry no label column — "
            "labels are joined to cache rows by filepath."
        ),
    )
    parser.add_argument(
        "--train-locations",
        required=True,
        help=(
            "train-side Phase A manifest parquet (filepath, lat, lon); may "
            "contain {encoder}. Each training row's H3 cell is derived from "
            "it at the cell's resolution — the cache's own cell is r4 only."
        ),
    )
    parser.add_argument(
        "--eval-locations",
        required=True,
        help=(
            "eval-side Phase A manifest parquet path template (filepath, "
            "lat, lon), one per test set; may contain {test_set} and "
            "{encoder}."
        ),
    )
    parser.add_argument(
        "--prior",
        required=True,
        help="prior .npz path template; may contain {resolution}",
    )
    parser.add_argument(
        "--labels",
        required=True,
        help=(
            "one-eBird-code-per-line label file template; may contain "
            "{encoder}. Perch and BirdNET have DIFFERENT class lists "
            "(14795 vs 6522) — a single file across encoders would gather "
            "wrong columns for one of them."
        ),
    )
    parser.add_argument(
        "--bridge",
        required=True,
        help="JSON file mapping eBird code -> GBIF species key (int)",
    )
    parser.add_argument("--out", required=True, help="results dataset dir")
    parser.add_argument("--grid-shard", type=int, default=0)
    parser.add_argument("--n-shards", type=int, default=1)
    # Grid-restriction axes (defaults: the full factorial grid). Useful
    # for pilot runs and exercised by the main()-level tests.
    parser.add_argument("--encoders", nargs="+", default=None)
    parser.add_argument("--methods", nargs="+", default=None)
    parser.add_argument("--prior-variants", nargs="+", default=None)
    parser.add_argument("--resolutions", nargs="+", type=int, default=None)
    parser.add_argument("--taus", nargs="+", type=float, default=None)
    parser.add_argument("--test-sets", nargs="+", default=None)
    parser.add_argument(
        "--git-sha",
        default=None,
        help="override the git SHA recorded in provenance (default: HEAD)",
    )
    return parser.parse_args(argv)


def _git_sha(override: str | None) -> str:
    if override:
        return override
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _cell_key(cell: SweepCell) -> dict:
    """The cell's config fields, for logs and the per-shard failures file."""
    return {
        "config_hash": config_hash(cell),
        "encoder": cell.encoder,
        "method": cell.method,
        "prior_variant": cell.prior_variant,
        "resolution": cell.resolution,
        "tau": cell.tau,
        "test_set": cell.test_set,
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    out_dir = Path(args.out)

    _class_idx_cache: dict[str, dict[str, int]] = {}

    def _class_idx(encoder: str) -> dict[str, int]:
        path = args.labels.format(encoder=encoder)
        if path not in _class_idx_cache:
            _class_idx_cache[path] = class_index(Path(path))
        return _class_idx_cache[path]
    code_to_gbif = {
        code: int(key)
        for code, key in json.loads(
            Path(args.bridge).read_text(encoding="utf-8")
        ).items()
    }
    git_sha = _git_sha(args.git_sha)

    grid_kwargs = {
        key: tuple(value)
        for key, value in (
            ("encoders", args.encoders),
            ("methods", args.methods),
            ("prior_variants", args.prior_variants),
            ("resolutions", args.resolutions),
            ("taus", args.taus),
            ("test_sets", args.test_sets),
        )
        if value is not None
    }
    # Grouped sharding: one shard owns ALL test sets of a fit group, so the
    # per-shard fit_cache turns 6 refits into 1 (the sweep's dominant cost).
    cells = shard_cells_grouped(
        plan_grid(**grid_kwargs), args.grid_shard, args.n_shards
    )
    fit_cache: dict = {}
    existing = load_existing_hashes(out_dir)

    # Inputs are loaded ONCE per shard, memoised by resolved path.
    caches: dict[str, CacheView] = {}
    priors: dict[str, PriorTensor] = {}
    metadata_tables: dict[str, pa.Table] = {}
    location_tables: dict[str, pa.Table] = {}

    def _cache(template: str, **fields) -> CacheView:
        path = template.format(**fields)
        if path not in caches:
            caches[path] = load_cache(Path(path))
        return caches[path]

    def _eval_metadata(test_set: str) -> pa.Table:
        path = args.eval_labels.format(test_set=test_set)
        if path not in metadata_tables:
            metadata_tables[path] = pq.read_table(
                path, columns=["filepath", "ebird_code_multilabel"]
            )
        return metadata_tables[path]

    def _locations(template: str, **fields) -> pa.Table:
        """(filepath, lat, lon) of a Phase A manifest, read once per path."""
        path = template.format(**fields)
        if path not in location_tables:
            location_tables[path] = pq.read_table(
                path, columns=["filepath", "lat", "lon"]
            )
        return location_tables[path]

    def _prior(resolution: int) -> tuple[str, PriorTensor]:
        path = args.prior.format(resolution=resolution)
        if path not in priors:
            priors[path] = load_prior(Path(path))
        return path, priors[path]

    n_written = n_skipped = 0
    failures: list[dict] = []
    for cell in cells:
        h = config_hash(cell)
        if h in existing:
            logger.info("skip existing %s (%s)", h[:12], _cell_key(cell))
            n_skipped += 1
            continue
        try:
            train_path = args.train_cache.format(encoder=cell.encoder)
            eval_path = args.eval_cache.format(
                encoder=cell.encoder, test_set=cell.test_set
            )
            prior_path, prior = _prior(cell.resolution)
            row = run_cell(
                cell,
                train_cache=_cache(args.train_cache, encoder=cell.encoder),
                eval_cache=_cache(
                    args.eval_cache,
                    encoder=cell.encoder,
                    test_set=cell.test_set,
                ),
                eval_metadata=_eval_metadata(cell.test_set),
                eval_locations=_locations(
                    args.eval_locations,
                    encoder=cell.encoder,
                    test_set=cell.test_set,
                ),
                train_locations=_locations(
                    args.train_locations, encoder=cell.encoder
                ),
                prior=prior,
                class_idx=_class_idx(cell.encoder),
                code_to_gbif=code_to_gbif,
                provenance=Provenance(
                    git_sha=git_sha,
                    train_cache_dir=train_path,
                    eval_cache_dir=eval_path,
                    prior_path=prior_path,
                    prior_doi=prior.gbif_doi,
                ),
                fit_cache=fit_cache,
            )
            write_rows([row], out_dir)
            existing.add(h)
            n_written += 1
            logger.info(
                "wrote %s cmap=%.4f (%s)", h[:12], row["cmap_mean"], _cell_key(cell)
            )
        except Exception as exc:  # a failed cell never kills the shard
            logger.error("cell failed %s: %s", _cell_key(cell), exc)
            failures.append({"cell": _cell_key(cell), "error": str(exc)})

    if failures:
        out_dir.mkdir(parents=True, exist_ok=True)
        # Per-shard filename: shards share --out, and a single
        # failures.json is last-writer-wins — the fleet kept only one
        # of five shards' failure reports before this was per-shard.
        failures_path = out_dir / f"failures_shard{args.grid_shard:02d}.json"
        failures_path.write_text(
            json.dumps(failures, indent=2), encoding="utf-8"
        )
        logger.error(
            "%d/%d cells failed (wrote %d, skipped %d); see %s",
            len(failures),
            len(cells),
            n_written,
            n_skipped,
            failures_path,
        )
        for failure in failures:
            logger.error("  %s: %s", failure["cell"]["config_hash"][:12], failure["error"])
        return 1
    logger.info(
        "shard %d/%d complete: wrote %d, skipped %d existing",
        args.grid_shard,
        args.n_shards,
        n_written,
        n_skipped,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
