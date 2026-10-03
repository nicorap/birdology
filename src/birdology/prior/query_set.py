"""The QUERY SET: every (lat, lon, week) any consumer of the prior looks up.

WHY THIS EXISTS (option (b) of the EOD sizing ruling)
-----------------------------------------------------
The full build materialises ``species x observed_cells x 52`` contexts. On
the global EOD archive that is 8.3e9 contexts at H3 r3 (~2 TiB projected)
and 3.6e10 at r4; it was designed for Denmark. Every consumer of the prior
(Phase B train/eval, Phase C train/eval) only ever reads the contexts its
manifests carry, so the build is restricted to those -- EXACTLY, via the
one-step smoothing halo (see :func:`halo_cells` and
``scripts/build_prior.py::estimate_over_query_set``).

RESOLUTION-FREE, ON PURPOSE
---------------------------
The file holds distinct ``(lat, lon, week)`` rows, never a cell, for the
same reason the leakage test split does (``build_prior.TEST_SPLIT_COLUMNS``):
a pre-computed cell is fixed at one H3 resolution, and a ``--resolutions 3 4
5`` sweep needs each resolution's own cells. :meth:`QuerySet.contexts_at`
derives them with ``h3.latlng_to_cell(lat, lon, resolution)`` -- the SAME
call ``indexing.assign_cells`` makes for the occurrences and
``phase_a.manifest`` makes for the recordings, so a query cell and an
occurrence cell for one point cannot disagree.

Every contract violation raises ``ValueError`` naming the check.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import h3
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .indexing import WEEKS_PER_YEAR
from .smoothing import SmoothingParams, _ADJACENT, _validate_cells
from .smoothing import _validate_params as _validate_smoothing_params

#: The query set's columns. No cell: see the module docstring.
QUERY_SET_COLUMNS = ("lat", "lon", "week")

QUERY_SET_SCHEMA = pa.schema(
    [("lat", pa.float64()), ("lon", pa.float64()), ("week", pa.int8())]
)


def _provenance_path(path: Path) -> Path:
    return Path(path).with_suffix(".provenance.json")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_weeks(table: pa.Table, where: str) -> None:
    weeks = table.column("week")
    if table.num_rows == 0:
        return
    lo, hi = pc.min(weeks).as_py(), pc.max(weeks).as_py()
    if lo < 1 or hi > WEEKS_PER_YEAR:
        raise ValueError(
            f"{where} carries week values {lo}..{hi}, outside 1..{WEEKS_PER_YEAR}; "
            "manifest weeks come from `indexing.iso_week`, which folds ISO week "
            "53 into 52, so a week outside the range means a different "
            "derivation leaked in. Refused rather than folded: the prior has "
            "no column for it and a silently folded week is a different context"
        )


def build_query_set(manifests: Sequence[Path]) -> tuple[pa.Table, dict]:
    """Distinct ``(lat, lon, week)`` over *manifests*, plus provenance.

    A row contributes only when lat, lon AND week are all non-null: a
    manifest row with no week is a null-context recording (POW), which no
    consumer looks the prior up for. Such a manifest is still RECORDED in
    the provenance with ``rows_with_context == 0``, so the operator can see
    it was read rather than forgotten.

    The output is sorted (lat, lon, week) so the written bytes -- and hence
    the sha256 the prior's sidecar pins -- do not depend on manifest order.
    """
    pieces = []
    sources = []
    for path in manifests:
        path = Path(path)
        table = pq.read_table(path)
        missing = [c for c in QUERY_SET_COLUMNS if c not in table.column_names]
        if missing:
            raise ValueError(
                f"manifest {path} is missing column(s) {missing}; a query set "
                f"is built from {list(QUERY_SET_COLUMNS)} and a manifest "
                "without them cannot say which contexts it will look up"
            )
        selected = table.select(list(QUERY_SET_COLUMNS))
        mask = pc.and_(
            pc.and_(pc.is_valid(selected.column("lat")), pc.is_valid(selected.column("lon"))),
            pc.is_valid(selected.column("week")),
        )
        kept = selected.filter(mask)
        kept = pa.table(
            {
                "lat": kept.column("lat").cast(pa.float64()),
                "lon": kept.column("lon").cast(pa.float64()),
                "week": kept.column("week").cast(pa.int8()),
            },
            schema=QUERY_SET_SCHEMA,
        )
        _validate_weeks(kept, f"manifest {path}")
        pieces.append(kept)
        sources.append(
            {
                "path": str(path.resolve()),
                "rows": table.num_rows,
                "rows_with_context": kept.num_rows,
            }
        )
    stacked = pa.concat_tables(pieces) if pieces else QUERY_SET_SCHEMA.empty_table()
    distinct = stacked.group_by(list(QUERY_SET_COLUMNS)).aggregate([])
    distinct = distinct.select(list(QUERY_SET_COLUMNS)).sort_by(
        [(c, "ascending") for c in QUERY_SET_COLUMNS]
    )
    distinct = distinct.cast(QUERY_SET_SCHEMA)
    provenance = {
        "columns": list(QUERY_SET_COLUMNS),
        "sources": sources,
        "distinct_rows": distinct.num_rows,
    }
    return distinct, provenance


def write_query_set(
    table: pa.Table, provenance: dict, out: Path
) -> tuple[Path, Path]:
    """Write the parquet, then its provenance JSON carrying the parquet's sha256.

    The sha256 is of the BYTES WRITTEN, read back off disk, so the hash a
    prior's sidecar later pins describes the file that exists, not the
    table that was meant to be written.
    """
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table.cast(QUERY_SET_SCHEMA), out)
    record = dict(provenance)
    record["path"] = str(out.resolve())
    record["sha256"] = _sha256(out)
    prov = _provenance_path(out)
    prov.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return out, prov


@dataclass(frozen=True)
class QuerySet:
    """A loaded query set: its rows, where it came from and its content hash."""

    path: str
    sha256: str
    table: pa.Table

    def __len__(self) -> int:
        return self.table.num_rows

    def contexts_at(self, resolution: int) -> set[tuple[str, int]]:
        """The distinct ``(cell, week)`` contexts at *resolution*.

        ``h3.latlng_to_cell(lat, lon, resolution)`` -- the call
        ``indexing.assign_cells`` makes -- so a query cell is the cell an
        occurrence at the same point lands in. Computed per distinct
        (lat, lon) so a point queried in many weeks costs one h3 call.
        """
        lats = self.table.column("lat").to_pylist()
        lons = self.table.column("lon").to_pylist()
        weeks = self.table.column("week").to_pylist()
        cell_of: dict[tuple[float, float], str] = {}
        out: set[tuple[str, int]] = set()
        for lat, lon, week in zip(lats, lons, weeks):
            key = (lat, lon)
            cell = cell_of.get(key)
            if cell is None:
                cell = h3.latlng_to_cell(lat, lon, resolution)
                cell_of[key] = cell
            out.add((cell, int(week)))
        return out


def load_query_set(path: Path) -> QuerySet:
    """Read a query set, refusing a file that is not one."""
    path = Path(path)
    table = pq.read_table(path)
    missing = [c for c in QUERY_SET_COLUMNS if c not in table.column_names]
    if missing:
        raise ValueError(
            f"{path} is not a query set: it lacks column(s) {missing} "
            f"(a query set is distinct {list(QUERY_SET_COLUMNS)} rows, written "
            "by scripts/build_query_set.py)"
        )
    table = table.select(list(QUERY_SET_COLUMNS))
    for name in QUERY_SET_COLUMNS:
        if table.column(name).null_count:
            raise ValueError(
                f"query set {path} has nulls in {name!r}; every row must be a "
                "located, dated context"
            )
    table = pa.table(
        {
            "lat": table.column("lat").cast(pa.float64()),
            "lon": table.column("lon").cast(pa.float64()),
            "week": table.column("week").cast(pa.int8()),
        },
        schema=QUERY_SET_SCHEMA,
    )
    _validate_weeks(table, f"query set {path}")
    return QuerySet(path=str(path.resolve()), sha256=_sha256(path), table=table)


def halo_cells(query_cells: Iterable[str], observed_cells: Iterable[str]) -> list[str]:
    """``(Q_cells u ring1(Q_cells)) n observed``, sorted.

    THE EXACTNESS ARGUMENT, which this function is half of. ``smoothing.smooth``
    is ONE pass: context (c, w) blends its own backoff value with those of
    ``h3.grid_disk(c, 1)`` at week w, and of (c, w-1) and (c, w+1) with the
    52<->1 wrap, and a neighbour contributes ONLY if its cell is on the cell
    list the grid was built over (``smoothing._neighbour_codes``: ``c in
    code_of``). In the full build that list is the observed-cell axis. So a
    query context's smoothed value depends on exactly: its own cell, its
    observed ring-1 neighbours, and all 52 weeks of those (the temporal
    neighbours are in the same cell). Building over this set with all 52
    weeks reproduces it bit for bit; ``hierarchical_estimate``'s values do
    not depend on which grid points are requested at all (every level is a
    ratio of sums over the whole RatioTable).

    The intersection with *observed_cells* is load-bearing in BOTH
    directions: an unobserved neighbour must stay off the grid (the full
    build never had it, and adding it would blend in a value the full build
    never saw), and an unobserved QUERY cell must stay off too -- it is not
    on the full build's axis, and lookups there behave as they do against
    the full build.
    """
    observed = set(observed_cells)
    wanted: set[str] = set()
    for cell in set(query_cells):
        wanted.add(cell)
        wanted.update(h3.grid_disk(cell, 1))
    return sorted(wanted & observed)


@dataclass(frozen=True)
class RestrictionPlan:
    """Which contexts a restricted build evaluates, keeps and publishes.

    * ``query_contexts`` -- every queried (cell, week) at this resolution,
      sorted. This is the artifact's DECLARED DOMAIN.
    * ``on_grid_contexts`` -- the query contexts whose cell is OBSERVED, i.e.
      on the full build's cell axis. Only these carry values; the rest are
      in the domain but off the grid exactly as in the full build.
    * ``support_points`` -- every (cell, week) whose BACKOFF value the one
      smoothing pass over ``on_grid_contexts`` reads: each context itself,
      its two adjacent weeks (52 <-> 1 wrapping, ``smoothing._ADJACENT``) and
      its observed ring-1 neighbours at the same week. Sorted, distinct.
    * ``self_index`` / ``ring_index`` / ``prev_index`` / ``next_index`` --
      positions in ``support_points``, row-aligned with ``on_grid_contexts``.
      ``ring_index`` is padded with -1 and lists neighbours in
      ``h3.grid_disk`` order filtered to observed cells: the order
      ``smoothing._neighbour_codes`` adds them in, which is what makes the
      float sum bit-identical rather than merely close.
    """

    query_contexts: list
    on_grid_contexts: list
    on_grid_cells: list
    halo_cells: list
    support_points: list
    self_index: np.ndarray
    ring_index: np.ndarray
    prev_index: np.ndarray
    next_index: np.ndarray
    counts: dict


def plan_restriction(
    query_contexts: Iterable[tuple[str, int]], observed_cells: Sequence[str]
) -> RestrictionPlan:
    """The exact restriction of the full build to *query_contexts*.

    *observed_cells* is the full build's cell axis (``build_prior.cell_axis``
    of the guarded counts) -- the cell list `smooth` would have been handed.
    It is validated the way `smooth` validates it (canonical H3, one
    resolution), so a table the full build would refuse is refused here too.
    """
    observed_list = list(observed_cells)
    _validate_cells(observed_list)
    observed = set(observed_list)
    query = sorted({(str(c), int(w)) for c, w in query_contexts})
    for _cell, week in query:
        if not 1 <= week <= WEEKS_PER_YEAR:
            raise ValueError(
                f"query context week {week!r} is outside 1..{WEEKS_PER_YEAR}"
            )
    query_cells = {c for c, _ in query}
    on_grid = [(c, w) for c, w in query if c in observed]
    on_grid_cells = sorted({c for c, _ in on_grid})
    halo = halo_cells(query_cells, observed_list)

    ring_of = {
        cell: [n for n in h3.grid_disk(cell, 1) if n != cell and n in observed]
        for cell in on_grid_cells
    }
    wanted: set[tuple[str, int]] = set()
    for cell, week in on_grid:
        wanted.add((cell, week))
        prev, nxt = (int(x) for x in _ADJACENT[week])
        wanted.add((cell, prev))
        wanted.add((cell, nxt))
        for n in ring_of[cell]:
            wanted.add((n, week))
    support = sorted(wanted)
    position = {pt: i for i, pt in enumerate(support)}
    width = max((len(r) for r in ring_of.values()), default=0)
    n_ctx = len(on_grid)
    ring_index = np.full((n_ctx, width), -1, dtype=np.int64)
    self_index = np.empty(n_ctx, dtype=np.int64)
    prev_index = np.empty(n_ctx, dtype=np.int64)
    next_index = np.empty(n_ctx, dtype=np.int64)
    for j, (cell, week) in enumerate(on_grid):
        self_index[j] = position[(cell, week)]
        prev, nxt = (int(x) for x in _ADJACENT[week])
        prev_index[j] = position[(cell, prev)]
        next_index[j] = position[(cell, nxt)]
        for slot, n in enumerate(ring_of[cell]):
            ring_index[j, slot] = position[(n, week)]

    counts = {
        "query_contexts": len(query),
        "query_cells": len(query_cells),
        "on_grid_contexts": n_ctx,
        "on_grid_cells": len(on_grid_cells),
        "off_grid_contexts": len(query) - n_ctx,
        "halo_cells": len(halo),
        "observed_cells": len(observed),
        "support_points": len(support),
    }
    return RestrictionPlan(
        query_contexts=query,
        on_grid_contexts=on_grid,
        on_grid_cells=on_grid_cells,
        halo_cells=halo,
        support_points=support,
        self_index=self_index,
        ring_index=ring_index,
        prev_index=prev_index,
        next_index=next_index,
        counts=counts,
    )


def smooth_on_plan(
    support_values: np.ndarray,
    plan: RestrictionPlan,
    params: SmoothingParams = SmoothingParams(),
) -> np.ndarray:
    """`smoothing.smooth`'s one pass, evaluated only at ``plan.on_grid_contexts``.

    *support_values* is ``(n_species, len(plan.support_points))``: the
    backoff at every support point. Returns ``(n_species, n_on_grid)``.

    THE SAME FLOAT OPERATIONS IN THE SAME ORDER as `smooth`, per element:
    ``total = p``; ``weight = 1.0``; for each ring slot in `grid_disk` order
    (absent neighbours skipped) ``total += space_weight * p_n``, ``weight +=
    space_weight``; then the previous week, then the next week, each with
    ``time_weight``; the result is ``total / weight``. In the full build
    every observed cell carries all 52 weeks, so a temporal neighbour is
    always present and a spatial one is present iff its cell is observed --
    which is exactly what ``plan`` encodes. The zero-weight short-circuits
    are kept for the same reason `smooth` keeps them (they skip the term).
    """
    _validate_smoothing_params(params)
    p = np.asarray(support_values, dtype=np.float64)
    total = p[:, plan.self_index].copy()
    weight = np.ones(len(plan.self_index), dtype=np.float64)
    if params.space_weight:
        for slot in range(plan.ring_index.shape[1]):
            found = plan.ring_index[:, slot] >= 0
            total[:, found] += params.space_weight * p[:, plan.ring_index[found, slot]]
            weight[found] += params.space_weight
    if params.time_weight:
        for index in (plan.prev_index, plan.next_index):
            total += params.time_weight * p[:, index]
            weight += params.time_weight
    return total / weight
