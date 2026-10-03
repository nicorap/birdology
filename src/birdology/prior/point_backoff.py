"""The backoff at an explicit set of (cell, week) POINTS, vectorised.

WHY THIS MODULE EXISTS
----------------------
`backoff.hierarchical_estimate` is the reference implementation and stays
the only one the FULL build uses. It walks the RatioTable with `to_pylist()`
on EVERY call and evaluates a dense ``cells x weeks`` grid. The RESTRICTED
build (option (b): build the prior only where it is queried) needs the
backoff at a sparse POINT set -- the query contexts plus their one-step
smoothing neighbours -- over a RatioTable of up to ~1e8 rows, in hundreds of
species chunks. Re-walking 1e8 Python dicts per chunk is the cost this
module removes: the table is validated and sorted ONCE, with numpy, into a
species-major layout, and each species reads only its own contiguous slice
of rows. The whole build therefore touches every ratio row once, plus
``n_species x n_points`` elementwise arithmetic.

WHY IT IS EXACT, NOT CLOSE
--------------------------
`hierarchical_estimate` computes, for a grid point (s, c, w):

    p_global = max(global_species[s] / global_n, floor)        (0 if global_n == 0)
    p_cell   = _shrink(cell_species[s, c], cell_n[c], p_global, cell_weight)
    p        = _shrink(cell_week_species[s, c, w], cell_week_total[c, w],
                       p_cell, cell_week_weight)
    out      = min(max(p, floor), 1.0)

Every one of those sums is a sum of INTEGER counts. The reference
accumulates them into Python floats starting from 0.0; this module
accumulates them as int64 (or as float64 `bincount` weights) and converts
once. Both are exact while every partial sum is below 2**53 (~9e15 records;
the global archive holds 1.76e9), so they give the same float64 for every
sum regardless of summation order. The arithmetic after that is the same
IEEE-754 float64 `+`, `*`, `/`, `max` and `min`, elementwise, in the same
order, including `_shrink`'s "denominator == 0.0 returns the coarser value"
branch. So every value is bit-identical to the reference's, which
`tests/prior/test_point_backoff.py` pins against `hierarchical_estimate`
over random tables and every parameter regime, the zero-weight edge
included.

The RatioTable contract is the reference's, refused with the same named
messages: week outside 1..52, total < 1, n < 1, n > total, two totals for one
cell-week, a duplicate (species, cell, week), and species counts summing
above their cell-week total.
"""
from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pyarrow as pa

from .arrow_strings import dictionary_codes
from .backoff import (
    BackoffParams,
    _validate_grid_axis,
    _validate_params,
    _validate_table,
    _validate_week,
)
from .indexing import WEEKS_PER_YEAR

#: Week digit stride: weeks are 1..52, so 53 keeps every packed key distinct.
_WEEK_STRIDE = WEEKS_PER_YEAR + 1

def _numpy(column, dtype) -> np.ndarray:
    if isinstance(column, pa.ChunkedArray):
        if column.num_chunks == 0:
            return np.zeros(0, dtype=dtype)
        column = column.combine_chunks()
    return np.asarray(column.to_numpy(zero_copy_only=False), dtype=dtype)


class PointStatistics:
    """A whole RatioTable, validated, summed and sorted species-major ONCE."""

    def __init__(self, ratios: pa.Table) -> None:
        _validate_table(ratios)
        self.n_ratio_rows = ratios.num_rows
        species = _numpy(ratios.column("species_key"), np.int64)
        weeks = _numpy(ratios.column("week"), np.int64)
        n = _numpy(ratios.column("n"), np.int64)
        total = _numpy(ratios.column("total"), np.int64)
        codes, distinct = dictionary_codes(ratios.column("cell"))
        cells = distinct.to_pylist()

        def name(i):
            return (int(species[i]), cells[codes[i]], int(weeks[i]))

        if weeks.size:
            bad = np.flatnonzero((weeks < 1) | (weeks > WEEKS_PER_YEAR))
            if bad.size:
                _validate_week(int(weeks[bad[0]]), "the RatioTable")
        i = np.flatnonzero(total < 1)
        if i.size:
            raise ValueError(
                f"RatioTable row {name(i[0])} has total={int(total[i[0]])}; the "
                "cell-week effort is a count of records and must be >= 1, and "
                "dividing by it would give inf or nan rather than an error"
            )
        i = np.flatnonzero(n < 1)
        if i.size:
            raise ValueError(
                f"RatioTable row {name(i[0])} has n={int(n[i[0]])}; absence is "
                "ROW ABSENCE in this table, so a non-positive count means a "
                "zero-fill was mistaken for an observation"
            )
        i = np.flatnonzero(n > total)
        if i.size:
            raise ValueError(
                f"RatioTable row {name(i[0])} has n={int(n[i[0]])} which exceeds "
                f"its own total={int(total[i[0]])}; the numerator and the "
                "denominator are no longer paired row-for-row"
            )

        n_cells = len(cells)
        # cell_week_total as a dense (cell, week) table. 0 == "no row".
        cw_total = np.zeros((max(n_cells, 1), _WEEK_STRIDE), dtype=np.int64)
        cw_total[codes, weeks] = total
        disagree = np.flatnonzero(cw_total[codes, weeks] != total)
        if disagree.size:
            k = disagree[0]
            raise ValueError(
                f"RatioTable cell-week {(cells[codes[k]], int(weeks[k]))!r} "
                "carries two different totals; `total` is a property of the "
                "(cell, week), so picking one would make every parent "
                "denominator depend on row order"
            )

        # Species-major sort: one contiguous slice of rows per species.
        self._species, sp_index = np.unique(species, return_inverse=True)
        sp_index = sp_index.ravel()
        row_key = (sp_index * max(n_cells, 1) + codes) * _WEEK_STRIDE + weeks
        order = np.argsort(row_key, kind="stable")
        sorted_key = row_key[order]
        dup = np.flatnonzero(np.diff(sorted_key) == 0)
        if dup.size:
            k = order[dup[0]]
            raise ValueError(
                f"RatioTable has a duplicate row for {name(k)!r}; a RatioTable "
                "holds one row per (species, cell, week). Duplicates arrive by "
                "concatenating two independently-computed tables, where each "
                "copy's partial `n` is paired with a `total` that already "
                "covers the whole cell-week"
            )
        del row_key, sorted_key

        # Cell-week count coherence.
        n_sum = np.zeros_like(cw_total)
        np.add.at(n_sum, (codes, weeks), n)
        over = np.argwhere(n_sum > cw_total)
        if over.size:
            c, w = (int(x) for x in over[0])
            raise ValueError(
                f"RatioTable cell-week {(cells[c], w)!r} has species counts "
                f"summing to {int(n_sum[c, w])}, which exceeds its total="
                f"{int(cw_total[c, w])}; `total` counts ALL records in the "
                "cell-week, so the rows present cannot out-count it -- this is "
                "two partial tables of DISJOINT species concatenated"
            )
        del n_sum

        self._cell_code = {cell: i for i, cell in enumerate(cells)}
        self._cw_total = cw_total.astype(np.float64)
        self._cell_n = cw_total.sum(axis=1).astype(np.float64)
        self._global_n = float(int(cw_total.sum()))
        global_species = np.zeros(self._species.size, dtype=np.int64)
        np.add.at(global_species, sp_index, n)
        self._global_species = global_species.astype(np.float64)
        # Rows in species-major order, and each species' slice boundaries.
        self._codes = codes[order]
        self._weeks = weeks[order]
        self._n = n[order].astype(np.float64)
        self._offsets = np.searchsorted(
            sp_index[order], np.arange(self._species.size + 1)
        )

    def _prepare(self, points: Sequence[tuple[str, int]]) -> dict:
        """Validate and index a point list ONCE per distinct list object.

        The restricted build evaluates the SAME support-point list for every
        species chunk -- ~200 chunks over ~1e6 points at r5 -- so the
        validation and the cell lookups are cached against the identity of
        the list object (`is`, not equality: a caller mutating a list in
        place between calls gets a fresh preparation only if it passes a new
        list, which is what `build_prior` does).
        """
        cached = getattr(self, "_prepared", None)
        if cached is not None and cached[0] is points:
            return cached[1]
        pts = [(str(c), int(w)) for c, w in points]
        _validate_grid_axis(pts, "point")
        for _cell, week in pts:
            _validate_week(week, "the requested points")
        n_pts = len(pts)
        code = np.array([self._cell_code.get(c, -1) for c, _ in pts], dtype=np.int64)
        week = np.array([w for _, w in pts], dtype=np.int64)
        known = code >= 0
        point_cells, point_cell = np.unique(code[known], return_inverse=True)
        n_pc = point_cells.size
        pc_of_point = np.full(n_pts, -1, dtype=np.int64)
        pc_of_point[known] = point_cell.ravel()
        # table cell code -> compact point-cell index, -1 when no point uses it
        pc_of_code = np.full(len(self._cell_code) + 1, -1, dtype=np.int64)
        pc_of_code[point_cells] = np.arange(n_pc)
        # (compact point-cell, week) -> point index
        point_at = np.full((max(n_pc, 1), _WEEK_STRIDE), -1, dtype=np.int64)
        point_at[pc_of_point[known], week[known]] = np.flatnonzero(known)
        cw_total = np.zeros(n_pts, dtype=np.float64)
        cw_total[known] = self._cw_total[code[known], week[known]]
        prepared = {
            "n_pts": n_pts,
            "known": known,
            "n_pc": n_pc,
            "pc_of_point": pc_of_point,
            "pc_of_code": pc_of_code,
            "point_at": point_at,
            "cw_total": cw_total,
            "cell_n_pc": self._cell_n[point_cells],
        }
        self._prepared = (points, prepared)
        return prepared

    def estimate(
        self,
        species_keys: Sequence[int],
        points: Sequence[tuple[str, int]],
        params: BackoffParams = BackoffParams(),
    ) -> np.ndarray:
        """``(len(species_keys), len(points))`` float64: the backoff at each point.

        Element ``[i, j]`` equals `hierarchical_estimate`'s value for
        ``(species_keys[i], *points[j])`` bit for bit. A species or cell
        absent from the RatioTable takes the reference's no-evidence path.
        """
        _validate_params(params)
        _validate_grid_axis(list(species_keys), "species_key")
        keys = np.asarray(list(species_keys), dtype=np.int64)
        prepared = self._prepare(points)
        n_pts = prepared["n_pts"]
        out = np.empty((keys.size, n_pts), dtype=np.float64)
        if keys.size == 0 or n_pts == 0:
            return out
        known = prepared["known"]
        n_pc = prepared["n_pc"]
        pc_of_point = prepared["pc_of_point"]
        pc_of_code = prepared["pc_of_code"]
        point_at = prepared["point_at"]
        cw_total = prepared["cw_total"]
        cell_n_pc = prepared["cell_n_pc"]

        if self._species.size:
            sp_pos = np.minimum(
                np.searchsorted(self._species, keys), self._species.size - 1
            )
            sp_known = self._species[sp_pos] == keys
        else:
            sp_pos = np.zeros(keys.size, dtype=np.int64)
            sp_known = np.zeros(keys.size, dtype=bool)

        for i in range(keys.size):
            cell_species = np.zeros(n_pc, dtype=np.float64)
            cell_week_species = np.zeros(n_pts, dtype=np.float64)
            if sp_known[i]:
                gs = self._global_species[sp_pos[i]]
                lo, hi = self._offsets[sp_pos[i]], self._offsets[sp_pos[i] + 1]
                pc = pc_of_code[self._codes[lo:hi]]
                hit = pc >= 0
                pc_hit = pc[hit]
                n_hit = self._n[lo:hi][hit]
                cell_species = np.bincount(pc_hit, weights=n_hit, minlength=n_pc)
                at = point_at[pc_hit, self._weeks[lo:hi][hit]]
                on = at >= 0
                cell_week_species[at[on]] = n_hit[on]
            else:
                gs = 0.0
            p_global = (gs / self._global_n) if self._global_n else 0.0
            p_global = max(p_global, params.floor)
            p_cell_pc = _shrink(cell_species, cell_n_pc, p_global, params.cell_weight)
            p_cell = np.full(n_pts, _shrink_scalar(0.0, 0.0, p_global, params.cell_weight))
            p_cell[known] = p_cell_pc[pc_of_point[known]]
            p = _shrink(cell_week_species, cw_total, p_cell, params.cell_week_weight)
            out[i] = np.minimum(np.maximum(p, params.floor), 1.0)
        return out


def _shrink_scalar(local_num, local_den, coarse_p, weight):
    """`backoff._shrink` itself, for the one scalar case (an unknown cell)."""
    denominator = local_den + weight
    if denominator == 0.0:
        return coarse_p
    return (local_num + weight * coarse_p) / denominator


def _shrink(local_num, local_den, coarse_p, weight):
    """`backoff._shrink`, elementwise, with its zero-denominator branch."""
    denominator = local_den + weight
    with np.errstate(divide="ignore", invalid="ignore"):
        shrunk = (local_num + weight * coarse_p) / denominator
    return np.where(denominator == 0.0, coarse_p, shrunk)
