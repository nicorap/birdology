#!/usr/bin/env python3
"""Typed occurrence table + guarded counts from an archive_parquet directory.

    python scripts/prior_occurrence_counts.py --parquet data/prior/parquet/eod \\
        --occ data/prior/occurrences/eod --resolutions 3 4 5 \\
        --test-split data/prior/splits_eval.csv --split-contributor-mode placeholder \\
        --compare data/prior/eod_q --source eod

Builds the occurrence table (skipped when --occ already holds one), then for
each resolution the guarded (species, cell, week) counts and their guard
record -- what build_prior.py checkpoints as counts_checkpoint. With
--compare, each resolution is checked against the counts checkpoint in that
directory (an earlier build from the zip): the same rows and n, the same
guard record. Exit 1 on any difference.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_prior import load_test_split, recordings_at_resolution  # noqa: E402

from birdology.prior.counts_checkpoint import GUARD_FIELDS, METADATA_KEY, counts_checkpoint_path  # noqa: E402
from birdology.prior.guarded_counts import KEYS, guarded_counts  # noqa: E402
from birdology.prior.occurrence_table import MANIFEST, build_occurrence_table  # noqa: E402


def _canonical(t: pa.Table) -> pa.Table:
    """Sorted, with the cell as its 64-bit H3 index: sorting a string column
    gathers it into one chunk, and r5's 157M cells exceed 2 GiB (job 12605)."""
    distinct = pc.unique(t["cell"])
    as_int = pa.array([int(c, 16) for c in distinct.to_pylist()], pa.uint64())
    cell = pa.chunked_array([as_int.take(pc.index_in(c, value_set=distinct)) for c in t["cell"].chunks],
                            pa.uint64())
    t = pa.table({"species_key": pc.cast(t["species_key"], pa.int64()), "cell": cell,
                  "week": pc.cast(t["week"], pa.int8()), "n": pc.cast(t["n"], pa.int64())})
    return t.sort_by([(k, "ascending") for k in KEYS])


def compare(counts: pa.Table, guard: dict, checkpoint: Path) -> bool:
    ref = pq.read_table(checkpoint)
    ref_guard = json.loads(ref.schema.metadata[METADATA_KEY])["guard"]
    ok = True
    for f in GUARD_FIELDS:
        if guard[f] != ref_guard[f]:
            print(f"  guard {f}: {guard[f]} != checkpoint {ref_guard[f]}", flush=True)
            ok = False
    if counts.num_rows != ref.num_rows:
        print(f"  rows: {counts.num_rows} != checkpoint {ref.num_rows}", flush=True)
        return False
    a, b = _canonical(counts), _canonical(ref)
    for c in a.column_names:
        if not a[c].equals(b[c]):
            diff = pc.sum(pc.invert(pc.equal(a[c], b[c]))).as_py()
            print(f"  column {c}: {diff} rows differ", flush=True)
            ok = False
    return ok


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--parquet", type=Path, required=True, help="archive_parquet directory")
    ap.add_argument("--occ", type=Path, required=True, help="occurrence table directory (built if empty)")
    ap.add_argument("--resolutions", type=int, nargs="+", default=[3, 4, 5])
    split = ap.add_mutually_exclusive_group(required=True)
    split.add_argument("--test-split", type=Path)
    split.add_argument("--no-test-split", action="store_true")
    ap.add_argument("--split-contributor-mode", choices=("match", "placeholder"), default="match")
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--buckets", type=int, default=64)
    ap.add_argument("--reducers", type=int, default=16)
    ap.add_argument("--out", type=Path, default=None, help="write counts_r<R>.parquet here (default: --occ)")
    ap.add_argument("--compare", type=Path, default=None, help="directory holding counts checkpoints")
    ap.add_argument("--source", default="eod", help="checkpoint source name, for --compare")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if (args.occ / MANIFEST).exists():
        m = json.loads((args.occ / MANIFEST).read_text())
        print(f"occurrence table exists: {m['kept']} rows at {m['resolutions']}", flush=True)
    else:
        m = build_occurrence_table(args.parquet, args.occ, args.resolutions, workers=args.workers)
        print(f"occurrence table: {m['total']} rows read, {m['kept']} kept, dropped {m['dropped']}, "
              f"{m['seconds'] / 60:.1f} min", flush=True)

    rows = [] if args.no_test_split else load_test_split(args.test_split)
    out = args.out or args.occ
    ok = True
    for r in args.resolutions:
        t0 = time.time()
        counts, guard, timings = guarded_counts(args.occ, r, recordings_at_resolution(rows, r),
                                                args.split_contributor_mode, workers=args.workers,
                                                buckets=args.buckets, reducers=args.reducers)
        pq.write_table(counts, out / f"counts_r{r}.parquet", compression="zstd")
        print(f"r{r}: {counts.num_rows} counts rows, guard {guard}, {timings}, {time.time() - t0:.0f} s",
              flush=True)
        if args.compare:
            same = compare(counts, guard, counts_checkpoint_path(args.compare, args.source, r))
            print(f"r{r}: {'IDENTICAL to' if same else 'DIFFERS from'} the checkpoint", flush=True)
            ok &= same
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
