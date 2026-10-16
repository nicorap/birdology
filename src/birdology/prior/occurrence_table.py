"""A typed, per-occurrence table built from the Parquet copy of a GBIF archive.

``archive_parquet`` stores the archive as raw text. This module applies the
row rules of :func:`occurrences.read_occurrences` -- the same rows dropped for
the same reasons, counted the same way -- and writes what survives as typed
columns: species, coordinates, date, week, the H3 cell at each resolution (as
its 64-bit integer), provenance, and the columns rarity flagging needs.

WHY NOT THE PRE-AGGREGATED PARSE
--------------------------------
``parallel_parse`` aggregates on (species, cell, week, publisher,
contributor). Measured on 10M EOD rows (job 12550), 96-98% of those keys are
distinct: an eBird observer rarely logs one species twice in one cell-week.
The "aggregate" was the occurrence table again, three times over, merged by
one parent thread -- why jobs 5846/6237 never finished. Here every worker
writes its own file and nothing is merged; the reduction happens later, on
(species, cell, week), where keys DO collapse.

EXACT BY CONSTRUCTION
---------------------
Every rule runs the same Python function ``read_occurrences`` runs, once per
DISTINCT value instead of once per row, and the result is mapped back:
``int(s.strip())`` for species keys, ``_parse_date`` + ``iso_week`` for dates,
``_optional`` for text, ``h3.latlng_to_cell`` per distinct (lat, lon) pair.
Coordinates are nearly all distinct, so a plain decimal numeral
(``-?[0-9]+(.[0-9]+)?``) is cast by Arrow -- correctly rounded, as Python's
``float`` is -- and anything else goes through ``float(s.strip())`` one by
one. Lines the Arrow CSV reader could not hold (``rejects.parquet``) are split
as ``csv.DictReader`` splits them and run through the same rules.
"""
from __future__ import annotations

import json
import os
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path

import h3
import h3.api.basic_int as h3i
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .archive_parquet import MANIFEST as SOURCE_MANIFEST
from .archive_parquet import REJECTS
from .indexing import iso_week
from .occurrences import _occurrence_member, _optional, _parse_date

MANIFEST = "manifest.json"
DROP_REASONS = ("no_species_key", "bad_coordinates", "unparseable_date")
_PLAIN_DECIMAL = r"^-?[0-9]+(\.[0-9]+)?$"
_TEXT = pa.dictionary(pa.int32(), pa.string())

#: Source column -> (output column, parse) for the columns carried along for
#: rarity flagging and provenance. None of them can drop a row.
CARRIED = {
    "gbifID": ("gbif_id", "int"),
    "datasetKey": ("dataset_key", "text"),
    "publishingOrgKey": ("publisher_key", "text"),
    "recordedBy": ("recorded_by", "text"),
    "countryCode": ("country_code", "text"),
    "stateProvince": ("state_province", "text"),
    "individualCount": ("individual_count", "int"),
    "coordinateUncertaintyInMeters": ("coordinate_uncertainty_m", "float"),
    "occurrenceStatus": ("occurrence_status", "text"),
    "basisOfRecord": ("basis_of_record", "text"),
    "issue": ("issue", "text"),
}


def schema(resolutions: list[int]) -> pa.Schema:
    fields = [
        ("species_key", pa.int64()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("event_date", pa.date32()),
        ("week", pa.int8()),
        *[(f"cell_r{r}", pa.uint64()) for r in resolutions],
    ]
    types = {"int": pa.int64(), "float": pa.float64(), "text": _TEXT}
    fields += [(name, types[kind]) for name, kind in CARRIED.values()]
    return pa.schema(fields)


def _int(s: str) -> int | None:
    try:
        return int(s.strip())
    except ValueError:
        return None


def _float(s: str) -> float | None:
    try:
        return float(s.strip())
    except ValueError:
        return None


def _date(s: str):
    return _parse_date(s.strip())


def _week(s: str) -> int | None:
    d = _date(s)
    return iso_week(d) if d is not None else None


def _map_distinct(col, fn, type_: pa.DataType) -> pa.Array:
    """``fn`` applied once per distinct string, mapped back to every row."""
    d = (col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col).dictionary_encode()
    values = pa.array([fn(s) for s in d.dictionary.to_pylist()], type_)
    return values.take(d.indices)


def _coordinates(col) -> tuple[np.ndarray, np.ndarray]:
    """(values, parsed) with ``float(s.strip())`` semantics, vectorised where exact."""
    arr = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
    out = np.full(len(arr), np.nan)
    ok = np.zeros(len(arr), dtype=bool)
    fast = pc.match_substring_regex(arr, _PLAIN_DECIMAL).to_numpy(zero_copy_only=False)
    fi = np.flatnonzero(fast)
    out[fi] = pc.cast(arr.take(pa.array(fi)), pa.float64()).to_numpy(zero_copy_only=False)
    ok[fi] = True
    si = np.flatnonzero(~fast)
    for i, s in zip(si, arr.take(pa.array(si)).to_pylist()):
        v = _float(s)
        if v is not None:
            out[i], ok[i] = v, True
    return out, ok


def _usable(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    """``occurrences._coordinates_usable``, vectorised (NaN compares False)."""
    with np.errstate(invalid="ignore"):
        return np.isfinite(lat) & np.isfinite(lon) & (np.abs(lat) <= 90.0) & (np.abs(lon) <= 180.0)


def _cells(lat: np.ndarray, lon: np.ndarray, resolution: int) -> pa.Array:
    """``h3.latlng_to_cell`` (as the 64-bit index) once per distinct (lat, lon)."""
    if len(lat) == 0:
        return pa.array([], pa.uint64())
    pairs = np.ascontiguousarray(np.stack([lat, lon], 1)).view(np.dtype((np.void, 16))).ravel()
    _, first, inverse = np.unique(pairs, return_index=True, return_inverse=True)
    cells = np.fromiter((h3i.latlng_to_cell(lat[i], lon[i], resolution) for i in first), np.uint64, len(first))
    return pa.array(cells[inverse.ravel()], pa.uint64())


def typed_rows(t: pa.Table, resolutions: list[int]) -> tuple[pa.Table, dict[str, int]]:
    """Raw-text rows -> (kept typed rows, {total, <drop reason>...}), the read_occurrences rules."""
    names = set(t.column_names)
    total = t.num_rows

    def raw(name: str) -> pa.Array:
        return t[name].combine_chunks() if name in names else pa.array([""] * total, pa.string())

    species = _map_distinct(raw("speciesKey"), _int, pa.int64())
    sk_ok = species.is_valid().to_numpy(zero_copy_only=False)
    lat, lat_ok = _coordinates(raw("decimalLatitude"))
    lon, lon_ok = _coordinates(raw("decimalLongitude"))
    coord_ok = lat_ok & lon_ok & _usable(lat, lon)
    date = _map_distinct(raw("eventDate"), _date, pa.date32())
    date_ok = date.is_valid().to_numpy(zero_copy_only=False)
    counts = {
        "total": total,
        "no_species_key": int((~sk_ok).sum()),
        "bad_coordinates": int((sk_ok & ~coord_ok).sum()),
        "unparseable_date": int((sk_ok & coord_ok & ~date_ok).sum()),
    }
    keep_np = np.flatnonzero(sk_ok & coord_ok & date_ok)
    keep = pa.array(keep_np)
    kdate = date.take(keep)
    cols = {
        "species_key": species.take(keep),
        "lat": pa.array(lat[keep_np]),
        "lon": pa.array(lon[keep_np]),
        "event_date": kdate,
        "week": _map_distinct(raw("eventDate"), _week, pa.int8()).take(keep),
    }
    for r in resolutions:
        cols[f"cell_r{r}"] = _cells(lat[keep_np], lon[keep_np], r)
    parse = {"int": (_int, pa.int64()), "float": (_float, pa.float64()), "text": (_optional, pa.string())}
    for src, (name, kind) in CARRIED.items():
        fn, type_ = parse[kind]
        if src in names:
            values = _map_distinct(raw(src).take(keep), fn, type_)
        else:
            values = pa.nulls(len(keep_np), type_)
        cols[name] = values.dictionary_encode() if kind == "text" else values
    return pa.table(cols, schema=schema(resolutions)), counts


def _rejected_rows(source: Path, manifest: dict) -> pa.Table:
    """Rejected lines split as csv.DictReader splits them, as a raw-text table."""
    lines = pq.read_table(source / REJECTS).column("text").to_pylist()
    header = manifest.get("header")
    if header is None:          # manifests written before the header was recorded
        with zipfile.ZipFile(manifest["source"]) as zf, zf.open(_occurrence_member(zf)) as f:
            header = f.readline().decode("utf-8", "replace").rstrip("\r\n").split("\t")
    width = len(header)
    # DictReader: a short line's missing fields are None (``or ""`` -> ""), a
    # long line's extra fields go to restkey and are never read.
    rows = [(line.split("\t") + [""] * width)[:width] for line in lines]
    cols = manifest["columns"]
    idx = {name: i for i, name in enumerate(header)}
    return pa.table({c: pa.array([r[idx[c]] for r in rows], pa.string()) for c in cols})


def _task(payload: tuple[str, list[int], str, list[int]]) -> dict[str, int]:
    """Row groups of one source part -> one output file. Module-level for spawn."""
    path, row_groups, out, resolutions = payload
    f = pq.ParquetFile(path)
    totals = dict.fromkeys(("total", "kept", *DROP_REASONS), 0)
    with pq.ParquetWriter(out, schema(resolutions), compression="zstd") as w:
        for rg in row_groups:
            rows, counts = typed_rows(f.read_row_group(rg), resolutions)
            w.write_table(rows)
            for k, v in counts.items():
                totals[k] += v
            totals["kept"] += rows.num_rows
    return totals


def build_occurrence_table(
    source: Path,
    out_dir: Path,
    resolutions: list[int],
    *,
    workers: int | None = None,
    row_groups_per_file: int = 10,
) -> dict:
    """Write ``out_dir/occ-NNNNN.parquet`` + manifest from an archive_parquet directory."""
    source, out_dir = Path(source), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if any(out_dir.glob("occ-*.parquet")):
        raise FileExistsError(f"{out_dir} already holds occ files; build into an empty directory")
    workers = workers or os.cpu_count() or 1
    resolutions = sorted(resolutions)
    t0 = time.time()
    src_manifest = json.loads((source / SOURCE_MANIFEST).read_text())
    tasks = []
    for p in src_manifest["parts"]:
        n = pq.ParquetFile(source / p).num_row_groups
        for start in range(0, n, row_groups_per_file):
            out = out_dir / f"occ-{len(tasks):05d}.parquet"
            tasks.append((str(source / p), list(range(start, min(n, start + row_groups_per_file))), str(out), resolutions))

    totals = dict.fromkeys(("total", "kept", *DROP_REASONS), 0)
    rejected = _rejected_rows(source, src_manifest)
    parts = [Path(t[2]).name for t in tasks]
    if rejected.num_rows:
        rows, counts = typed_rows(rejected, resolutions)
        pq.write_table(rows, out_dir / "occ-rejects.parquet", compression="zstd")
        parts.append("occ-rejects.parquet")
        for k, v in counts.items():
            totals[k] += v
        totals["kept"] += rows.num_rows
    if workers == 1 or len(tasks) < 2:
        results = map(_task, tasks)
    else:
        pool = ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn"))
        results = pool.map(_task, tasks)
    for counts in results:
        for k, v in counts.items():
            totals[k] += v
    if workers != 1 and len(tasks) >= 2:
        pool.shutdown()

    if totals["total"] != src_manifest["source_rows"]:
        raise RuntimeError(f"read {totals['total']} rows, the source holds {src_manifest['source_rows']}")
    manifest = {
        "source": str(source),
        "source_rows": src_manifest["source_rows"],
        "resolutions": resolutions,
        "total": totals["total"],
        "kept": totals["kept"],
        "dropped": {k: totals[k] for k in DROP_REASONS},
        "parts": parts,
        "h3": h3.__version__,
        "pyarrow": pa.__version__,
        "seconds": round(time.time() - t0, 1),
    }
    (out_dir / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest
