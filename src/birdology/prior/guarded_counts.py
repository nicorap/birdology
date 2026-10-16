"""Leakage guards + (species, cell, week) counts over the typed occurrence table.

The result is what ``build_from_archive`` checkpoints (``counts_checkpoint``):
``aggregate_counts`` of the guarded occurrences, plus the guard record. It is
built without ever holding the occurrences in one process:

* map, one worker per ``occurrence_table`` file: drop Xeno-Canto rows
  (vectorised, the same ``is_in`` the guard uses), set aside the rows whose
  cell is a banned test cell, count the rest by (species, cell, week), and
  write the counts in ``buckets`` files split by species;
* the set-aside rows -- the only ones a banned key can match, since every key
  starts with the cell -- go through ``leakage._drop_test_contributor_cells``
  ITSELF, so the contributor guard, its normalisation and its banned / matched
  key counts are the original code's, not a re-implementation;
* reduce, one bucket per worker, at most ``reducers`` at once: sum the
  counts. Buckets hold disjoint species, so the buckets' results concatenate without another merge.

The reduce is a plain group-by over integer keys, the stage where a GPU
(cuDF) can be compared with the CPU on equal terms.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path

import h3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .leakage import (
    XENO_CANTO_PUBLISHER_KEYS,
    _drop_test_contributor_cells,
    _validate_contributor_mode,
    banned_cell_weeks,
    banned_keys,
)
from .occurrence_table import MANIFEST

KEYS = ["species_key", "cell", "week"]


def _cell_strings(cells: pa.Array) -> pa.Array:
    """64-bit H3 indexes -> their canonical strings, once per distinct cell."""
    if isinstance(cells, pa.ChunkedArray):
        cells = cells.combine_chunks()
    d = cells.dictionary_encode()
    return pa.array([h3.int_to_str(c) for c in d.dictionary.to_pylist()], pa.string()).take(d.indices)


def _count(t: pa.Table) -> pa.Table:
    g = t.group_by(KEYS).aggregate([("species_key", "count")])
    return g.rename_columns(KEYS + ["n"]).cast(pa.schema(
        [("species_key", pa.int64()), ("cell", pa.uint64()), ("week", pa.int8()), ("n", pa.int64())]))


def _write_buckets(counts: pa.Table, work: Path, stem: str, buckets: int) -> None:
    """Split counts by species into the bucket directories (disjoint species per bucket)."""
    b = counts["species_key"].to_numpy() % buckets
    for k in range(buckets):
        pq.write_table(counts.filter(pa.array(b == k)), work / f"b{k:03d}" / f"{stem}.parquet")


def _map(payload) -> dict:
    """One occurrence file: XC guard, set aside banned-cell rows, bucketed counts."""
    path, resolution, banned_cells, buckets, work = payload
    t = pq.read_table(path, columns=["species_key", f"cell_r{resolution}", "week", "publisher_key", "recorded_by"])
    t = t.rename_columns(["species_key", "cell", "week", "publisher_key", "recorded_by"])
    pub = pc.cast(t["publisher_key"], pa.string())
    xc = pc.fill_null(pc.is_in(pub, value_set=pa.array(sorted(XENO_CANTO_PUBLISHER_KEYS), pa.string())), False)
    t = t.filter(pc.invert(xc))
    rows_after_xc = t.num_rows
    hit = pc.is_in(t["cell"], value_set=pa.array(banned_cells, pa.uint64()))
    aside = t.filter(hit)
    counts = _count(t.filter(pc.invert(hit)).select(KEYS))
    _write_buckets(counts, Path(work), Path(path).stem, buckets)
    return {"xc_removed": int(pc.sum(xc).as_py() or 0), "rows_after_xc": rows_after_xc, "aside": aside}


def _reduce(payload) -> pa.Table:
    """One species bucket: sum the counts of every map output."""
    (bucket_dir,) = payload
    files = sorted(Path(bucket_dir).glob("*.parquet"))
    if not files:
        return _count(pa.table({"species_key": pa.array([], pa.int64()), "cell": pa.array([], pa.uint64()),
                                "week": pa.array([], pa.int8())}))
    t = pa.concat_tables(pq.read_table(f) for f in files)
    g = t.group_by(KEYS).aggregate([("n", "sum")])
    return g.rename_columns(KEYS + ["n"])


def guarded_counts(
    occ_dir: Path,
    resolution: int,
    test_recordings,
    contributor_mode: str = "match",
    *,
    workers: int | None = None,
    buckets: int = 64,
    reducers: int = 16,
    work_dir: Path | None = None,
) -> tuple[pa.Table, dict, dict]:
    """(counts, guard record, timings): the counts checkpoint's table and guard, from occurrences."""
    occ_dir = Path(occ_dir)
    manifest = json.loads((occ_dir / MANIFEST).read_text())
    if resolution not in manifest["resolutions"]:
        raise ValueError(f"{occ_dir} has cells at {manifest['resolutions']}, not resolution {resolution}")
    _validate_contributor_mode(contributor_mode)
    recordings = list(test_recordings)
    banned = banned_cell_weeks(recordings) if contributor_mode == "placeholder" else banned_keys(recordings)
    banned_cells = sorted({key[0] for key in banned})
    for cell in banned_cells:   # leakage._assert_one_resolution's check; every table cell is at `resolution`
        try:
            r = h3.get_resolution(cell)
        except Exception as exc:  # noqa: BLE001 - h3 raises several types
            raise ValueError(f"test recording cell {cell!r} is not a valid H3 index: {exc}") from exc
        if r != resolution:
            raise ValueError(f"H3 resolution mismatch: test recording cell {cell!r} is resolution {r}, "
                             f"the occurrence table's cells are resolution {resolution}")
    workers = workers or os.cpu_count() or 1
    timings = {}
    own_work = work_dir is None
    work = Path(tempfile.mkdtemp(prefix="guarded_counts_")) if own_work else Path(work_dir)
    try:
        for k in range(buckets):
            (work / f"b{k:03d}").mkdir(parents=True, exist_ok=False)
        tasks = [(str(occ_dir / p), resolution, [h3.str_to_int(c) for c in banned_cells], buckets, str(work))
                 for p in manifest["parts"]]
        ctx = get_context("spawn")
        t0 = time.time()
        with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
            mapped = list(pool.map(_map, tasks))
        timings["map_s"] = round(time.time() - t0, 1)

        t0 = time.time()
        aside = pa.concat_tables([m["aside"] for m in mapped]) if mapped else None
        rows_before_contributor_guard = sum(m["rows_after_xc"] for m in mapped)
        if aside is not None and aside.num_rows:
            table = aside.set_column(aside.schema.get_field_index("cell"), "cell", _cell_strings(aside["cell"]))
            table = table.set_column(table.schema.get_field_index("publisher_key"), "publisher_key",
                                     pc.cast(table["publisher_key"], pa.string()))
            table = table.set_column(table.schema.get_field_index("recorded_by"), "recorded_by",
                                     pc.cast(table["recorded_by"], pa.string()))
            kept, removed, banned_count, matched_count = _drop_test_contributor_cells(
                table, recordings, contributor_mode)
            kept = kept.set_column(kept.schema.get_field_index("cell"), "cell",
                                   pa.array([h3.str_to_int(c) for c in kept["cell"].to_pylist()], pa.uint64()))
            _write_buckets(_count(kept.select(KEYS)), work, "contributor-guard-survivors", buckets)
        else:
            removed, banned_count, matched_count = 0, len(banned), 0
        timings["contributor_guard_s"] = round(time.time() - t0, 1)

        t0 = time.time()
        # Reducer memory is the bucket's share of the partial counts, all held at
        # once: at r4, 16 buckets x 16 reducers ran out of 256 GB (job 12556).
        with ProcessPoolExecutor(max_workers=min(workers, buckets, reducers), mp_context=ctx) as pool:
            reduced = list(pool.map(_reduce, [(str(work / f"b{k:03d}"),) for k in range(buckets)]))
        counts = pa.concat_tables(reduced)
        timings["reduce_s"] = round(time.time() - t0, 1)
    finally:
        if own_work:
            shutil.rmtree(work, ignore_errors=True)

    counts = pa.table({
        "species_key": counts["species_key"],
        "cell": _cell_strings(counts["cell"].combine_chunks()),
        "week": counts["week"],
        "n": counts["n"],
    })
    guard = {
        "banned_key_count": int(banned_count),
        "matched_key_count": int(matched_count),
        "xeno_canto_rows_removed": sum(m["xc_removed"] for m in mapped),
        "contributor_rows_removed": int(removed),
        "rows_before_contributor_guard": rows_before_contributor_guard,
        "rows_after_guards": rows_before_contributor_guard - int(removed),
    }
    return counts, guard, timings
