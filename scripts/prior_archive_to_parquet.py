#!/usr/bin/env python3
"""Convert a GBIF SIMPLE_CSV download (e.g. eod.zip) to Parquet, once.

    python scripts/prior_archive_to_parquet.py --archive data/prior/archives/eod.zip \\
        --out data/prior/parquet/eod

--verify re-reads the zip with the csv.DictReader path of read_occurrences and
compares every kept field of every row, in order, with the Parquet (rejected
lines are matched against the rejects file). It is as slow as the old parse
(~120 k rows/s), so it is meant for the 100M-row slice, not the full archive.
"""
from __future__ import annotations

import argparse
import itertools
import logging
import sys
import time
import zipfile
from pathlib import Path

import pyarrow.parquet as pq

from birdology.prior import occurrences as occ
from birdology.prior.archive_parquet import REJECTS, convert_archive


def verify(zip_path: Path, out: Path, manifest: dict) -> int:
    cols = manifest["columns"]
    parts = (pq.ParquetFile(out / p) for p in manifest["parts"])
    rows = (r for f in parts for b in f.iter_batches(batch_size=200_000, columns=cols) for r in b.to_pylist())
    rejects = iter(pq.read_table(out / REJECTS).column("text").to_pylist())
    n = bad = 0
    t = time.time()
    with zipfile.ZipFile(zip_path) as zf, zf.open(manifest["member"]) as raw:
        for src in occ._reader(occ._text(raw)):
            n += 1
            if None in src or None in src.values():
                next(rejects)            # a line Arrow could not hold: it went to the rejects
                continue
            got = next(rows)
            want = {c: src[c] for c in cols}
            if got != want:
                bad += 1
                if bad <= 5:
                    print(f"row {n}: parquet {got} != csv {want}", flush=True)
            if n % 10_000_000 == 0:
                print(f"verified {n} rows, {bad} mismatches, {n / (time.time() - t) / 1e3:.0f} k rows/s", flush=True)
    leftover = sum(1 for _ in itertools.chain(rows, rejects))
    print(f"VERIFY: {n} csv rows, {bad} mismatches, {leftover} parquet rows left over", flush=True)
    return 0 if bad == 0 and leftover == 0 and n == manifest["source_rows"] else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--archive", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--rows-per-file", type=int, default=100_000_000)
    ap.add_argument("--expect-rows", type=int, default=0,
                    help="fail unless rows + rejected lines equals this (e.g. read_occurrences' logged total)")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    m = convert_archive(args.archive, args.out, rows_per_file=args.rows_per_file)
    print(f"{m['source_rows']} source rows = {m['rows']} parquet + {m['rejected_lines']} rejected, "
          f"{len(m['parts'])} parts, {m['seconds'] / 60:.1f} min", flush=True)
    if args.expect_rows and m["source_rows"] != args.expect_rows:
        print(f"FAIL: expected {args.expect_rows} source rows", flush=True)
        return 1
    return verify(args.archive, args.out, m) if args.verify else 0


if __name__ == "__main__":
    sys.exit(main())
