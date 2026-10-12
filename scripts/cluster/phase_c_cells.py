#!/usr/bin/env python3
"""Write the Phase C CELLS_TSV that phase_c_train.sbatch indexes.

    python scripts/cluster/phase_c_cells.py \
        --skip-done /srv/home/rapnic/birdology/data/phase_c/results \
        --out /srv/home/rapnic/birdology/data/phase_c/cells/sweep.tsv
    sbatch --array=0-$((N-1))%48 scripts/cluster/phase_c_train.sbatch sweep.tsv

THE GRID (spec 2026-10-01 §4)
-----------------------------
- conditioned: 3 combos (film×convnext, film×ast, token×ast) × 3
  resolutions × 5 seeds, prior_variant ``full``                     =  45
- controls:    the same 45 cells × (marginal, shuffle_space,
  shuffle_time) — uniform ≡ twin by construction (decision 4)       = 135
- twins:       2 models × 5 seeds                                   =  10

The spec counts 15 twins (one per combo × seed); film×ast and token×ast
share ONE AST twin — a twin has no conditioning module, so "the film
twin" and "the token twin" are the same training run. Twins are
resolution-independent and run at r4 (the manifests cover the same
recordings at every resolution; only the context columns differ).

RESUME: --skip-done drops every cell whose results rows already cover
ALL its test sets, keyed (arm, model_id, resolution, seed,
prior_variant). A partially-evaluated cell is kept: re-running it is
cheaper than reasoning about which test sets its checkpoint missed.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

DATA = "/srv/home/rapnic/birdology/data"

#: model → (HF checkpoint, revision) — the ids every fleet row so far used.
CHECKPOINTS = {
    "convnext": ("DBD-research-group/ConvNeXT-Base-BirdSet-XCL", "main"),
    "ast": ("DBD-research-group/AST-BirdSet-XCL", "main"),
}
COMBOS = (("film", "convnext"), ("film", "ast"), ("token", "ast"))
TWIN_MODELS = ("convnext", "ast")
TWIN_RESOLUTION = 4
RESOLUTIONS = (3, 4, 5)
SEEDS = (0, 1, 2, 3, 4)
#: phase_c_run.PRIOR_VARIANTS, conditioned first so the headline lands early.
VARIANTS = ("full", "marginal", "shuffle_space", "shuffle_time")
TEST_SETS = ("HSN", "NES", "PER", "SSW", "SNE", "UHH")
SHARDS_GLOB = f"{DATA}/birdset/XCM/XCM/*.tar.gz"


def grid() -> list[tuple]:
    """Every cell as the sbatch's 9 fields, in launch order."""
    tests = ",".join(TEST_SETS)
    cells = []
    for variant in VARIANTS:
        for res in RESOLUTIONS:
            for arm, model in COMBOS:
                ckpt, rev = CHECKPOINTS[model]
                for seed in SEEDS:
                    cells.append((arm, model, ckpt, rev, res, seed, variant, tests, SHARDS_GLOB))
    for model in TWIN_MODELS:
        ckpt, rev = CHECKPOINTS[model]
        for seed in SEEDS:
            cells.append(("twin", model, ckpt, rev, TWIN_RESOLUTION, seed, "full", tests, SHARDS_GLOB))
    return cells


def done_keys(results_dir: Path) -> dict[tuple, set[str]]:
    """(arm, model_id, resolution, seed, prior_variant) → test sets with a row."""
    import pyarrow.parquet as pq

    done: dict[tuple, set[str]] = {}
    for path in sorted(Path(results_dir).glob("*.parquet")):
        cols = ["arm", "model_id", "resolution", "seed", "prior_variant", "test_set"]
        for row in pq.read_table(path, columns=cols).to_pylist():
            key = (row["arm"], row["model_id"], int(row["resolution"]),
                   int(row["seed"]), row["prior_variant"])
            done.setdefault(key, set()).add(row["test_set"])
    return done


def pending(cells: list[tuple], done: dict[tuple, set[str]]) -> list[tuple]:
    out = []
    for cell in cells:
        arm, _model, ckpt, _rev, res, seed, variant, tests, _glob = cell
        have = done.get((arm, ckpt, res, seed, variant), set())
        if not set(tests.split(",")) <= have:
            out.append(cell)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--skip-done", type=Path, default=None, metavar="RESULTS_DIR")
    args = ap.parse_args(argv)

    cells = grid()
    if args.skip_done is not None:
        if not args.skip_done.is_dir():
            ap.error(f"--skip-done {args.skip_done} is not a directory")
        cells = pending(cells, done_keys(args.skip_done))
    if args.out.exists():
        ap.error(f"{args.out} exists; a changed grid goes to a fresh file (the sbatch exclusivity ruling)")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("".join("\t".join(map(str, c)) + "\n" for c in cells))
    print(f"wrote {len(cells)} cells to {args.out}; "
          f"sbatch --array=0-{len(cells) - 1} phase_c_train.sbatch {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
