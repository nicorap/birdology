#!/usr/bin/env python3
"""Measure the parallel parse against the sequential path, honestly.

Builds a synthetic GBIF SIMPLE_CSV archive (default 2M rows, GBIF-shaped
with a realistic drop fraction), then times:

* raw DEFLATE decompression alone (the ceiling a single zip stream allows),
* `read_occurrences` + `assign_cells` (the sequential path job 5345 ran),
* `parse_and_aggregate` at workers = 1, 4, 8, cpu_count.

Writes JSON next to itself (`measure_parallel_parse_results.json`) so the
numbers quoted in commit messages and docstrings are a recorded measurement
rather than a claim.

Run:  uv run python scripts/measurements/measure_parallel_parse.py
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)

NAMES = ["obsr123", "obsr9", "Søren Å", "Maria García", "", "obsrX" * 3]
DATES = ["2025-01-07", "2025-06-10T09:30:00", "2025-11-14/2025-11-15",
         "2025-03-03", "2025-11"]  # last one is coarse -> dropped


def build_archive(path: Path, n_rows: int, seed: int = 7) -> Path:
    """A checklist-shaped archive: the shape EOD actually has.

    eBird data arrives as CHECKLISTS: one observer, one site, one date,
    ~10-30 species rows. The first version of this generator drew every
    row's coordinates independently, which produced ~1 aggregation group
    PER ROW (3.8M groups from 4M rows at r5) -- a worst case no real
    archive resembles, and one that made the benchmark measure partial-
    table shipping rather than parsing. Sites, dates and observers are
    therefore drawn from realistic pools and rows arrive checklist-grouped.
    """
    rng = random.Random(seed)
    sites = [
        (54.5 + rng.random() * 3, 8.0 + rng.random() * 7) for _ in range(3000)
    ]
    observers = [f"obsr{rng.randrange(200000)}" for _ in range(5000)]
    dates = [f"2025-{m:02d}-{d:02d}" for m in range(1, 13) for d in (3, 11, 19, 27)]
    lines = [HEADER]
    i = 0
    while i < n_rows:
        lat, lon = sites[rng.randrange(len(sites))]
        who = observers[rng.randrange(len(observers))]
        d = dates[rng.randrange(len(dates))]
        pub = "pub-" + str(rng.randrange(40))
        for _ in range(min(rng.randrange(8, 30), n_rows - i)):
            bad = rng.random()
            sk = "" if bad < 0.01 else str(2480000 + rng.randrange(600))
            row_lat = "91.5" if 0.01 <= bad < 0.02 else f"{lat:.5f}"
            row_d = "2025-11" if 0.02 <= bad < 0.03 else d
            lines.append("\t".join(
                [str(i), "ds", pub, sk, "Turdus merula",
                 row_lat, f"{lon:.5f}", row_d, who]
            ))
            i += 1
    payload = "\n".join(lines) + "\n"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("occurrence.txt", payload)
    return path


def time_decompress(path: Path) -> tuple[float, int]:
    with zipfile.ZipFile(path) as zf:
        member = zf.namelist()[0]
        start = time.perf_counter()
        n = 0
        with zf.open(member) as raw:
            while True:
                buf = raw.read(1 << 22)
                if not buf:
                    break
                n += len(buf)
        return time.perf_counter() - start, n


def main() -> None:
    from birdology.prior.indexing import assign_cells
    from birdology.prior.occurrences import read_occurrences
    from birdology.prior.parallel_parse import parse_and_aggregate

    n_rows = int(sys.argv[1]) if len(sys.argv) > 1 else 2_000_000
    resolutions = [3, 4, 5]
    out = Path(os.environ.get("TMPDIR", "/tmp")) / "parse_bench.zip"
    print(f"building {n_rows} row archive at {out} ...", flush=True)
    build_archive(out, n_rows)
    size = out.stat().st_size

    results: dict = {
        "n_rows": n_rows,
        "resolutions": resolutions,
        "archive_bytes": size,
        "cpu_count": os.cpu_count(),
    }

    dt, decompressed = time_decompress(out)
    results["decompress_only_s"] = round(dt, 3)
    results["decompressed_bytes"] = decompressed
    print(f"decompress only: {dt:.2f}s "
          f"({decompressed / dt / 1e6:.0f} MB/s)", flush=True)

    start = time.perf_counter()
    table = read_occurrences(out)
    for r in resolutions:
        assign_cells(table, r)
    seq = time.perf_counter() - start
    results["sequential_read_plus_assign_s"] = round(seq, 3)
    print(f"sequential read_occurrences + assign_cells x3: {seq:.2f}s",
          flush=True)

    reference = None
    for workers in (1, 4, 8, os.cpu_count()):
        start = time.perf_counter()
        # chunk_bytes 4 MiB so the benchmark archive yields enough chunks
        # (~40 at 2M rows) that the worker count, not the chunk count, is
        # the binding constraint -- as it is on a real multi-hundred-GB
        # archive with tens of thousands of default-size chunks.
        parsed = parse_and_aggregate(
            out, resolutions, workers=workers, chunk_bytes=4 << 20
        )
        wall = time.perf_counter() - start
        results[f"parallel_workers_{workers}_s"] = round(wall, 3)
        print(f"parse_and_aggregate workers={workers}: {wall:.2f}s "
              f"(speedup vs sequential: {seq / wall:.2f}x)", flush=True)
        agg = {
            r: sorted(map(tuple, zip(
                parsed.tables[r].column("species_key").to_pylist(),
                parsed.tables[r].column("cell").to_pylist(),
                parsed.tables[r].column("n").to_pylist(),
            )))
            for r in resolutions
        }
        snapshot = (parsed.dropped, parsed.total, agg)
        if reference is None:
            reference = snapshot
        else:
            assert snapshot == reference, (
                f"workers={workers} diverged from workers=1 -- the benchmark "
                "is void and the parser is broken"
            )

    out_path = Path(__file__).parent / "measure_parallel_parse_results.json"
    out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"wrote {out_path}")
    out.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
