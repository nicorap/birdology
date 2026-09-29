"""Three-level hierarchical backoff.

Most (cell, week, species) combinations are empty, and empty almost always
means "nobody looked" rather than "absent". A raw ratio would therefore report
0.0 for unobserved-but-present species and 1.0 for a cell with one record.

Shrink each estimate toward the next coarser level, with weight set by how much
data stands behind it:

    cell-week  ->  cell's annual profile  ->  species' global profile

A species with no occurrences anywhere still gets `floor`, never zero: the
label set can legitimately contain species the occurrence data misses.

WHAT THE OUTPUT `p` MEANS
-------------------------
`p` is a shrunk estimate of P(species | a bird was recorded here this week) --
the same quantity the upstream `ratio` measures, pulled toward the coarser
levels where the local data is thin. It is NOT P(species present), and it is
NOT renormalised over `species_keys`: a species absent from the occurrence data
contributes `floor` on top, so the values over a requested species list can sum
to slightly more or (for a subset) much less than 1.0.

THE DENOMINATOR IS `total`, NEVER A RE-SUM OF `n`
-------------------------------------------------
`total` is the effort proxy: the count of ALL bird records in the cell-week,
computed once upstream over the complete table. This module READS it. It does
not re-derive it by summing the `n` values it was handed, and the distinction
is not academic:

* the two agree only while the table still holds every species;
* filtering to a label set is an ordinary pipeline step, and after it the rows
  of a cell-week sum to far less than `total`;
* a re-summed denominator then reports a share of 1.0 where the real share is
  0.01 -- a hundredfold error, in the direction of confident presence, with
  nothing raised anywhere.

This is the same rule the upstream effort module states: a value derived once
must not be derived a second time locally.

PARENT LEVELS COME FROM PARENT COUNTS
-------------------------------------
Both coarser levels are ratios of sums, never means of child ratios. The
GLOBAL level is that raw ratio, floored; the CELL level is that raw ratio
SHRUNK toward the global one, which is what the code returns and what the
levels below consume:

    p_global = max(sum(n everywhere) / sum(total over all cell-weeks), floor)
    p_cell   = _shrink(sum_n_cell, sum_total_cell, p_global, cell_weight)

where `sum_n_cell` sums `n` over the cell and `sum_total_cell` sums `total`
over the cell's distinct cell-weeks. Writing `p_cell` as the raw ratio alone
would understate it wherever the cell is thin, which is most of the grid.

Averaging child ratios instead would weight a one-record cell-week exactly as
heavily as a thousand-record one, which silently reintroduces the observer
effort bias the target-group correction removes. The numbers are far apart: a
cell with weeks of 1 and 101 records gives 0.02 from counts and 0.51 from
averaged ratios.

Each cell-week's `total` is counted ONCE per (cell, week) when summing a parent
denominator, not once per row -- otherwise a cell-week with ten species present
would contribute ten times its own effort.

STATED LIMITATION -- THE FLOOR ERASES THE ABSENCE DISTINCTION ABOVE 1/floor
---------------------------------------------------------------------------
The floor keeps an unobserved cell-week off a hard zero, so "nobody looked"
stays distinguishable from "looked and did not find". That distinction holds
only while the GLOBAL DENOMINATOR stays below `1/floor` -- with the default
`floor = 1e-6`, below 1e6 total bird records.

Above that threshold -- the normal GBIF regime -- both quantities are clamped
to `floor` and a species SURVEYED AND NOT FOUND becomes bit-identical to a
species NEVER RECORDED ANYWHERE. Verified: with 1e6 records in a cell, a
species holding one record elsewhere scores exactly 1e-6 in a surveyed week
where it was absent, and an unrecorded species also scores exactly 1e-6.

This is a property of the method, not a bug to route around. The floor is
still necessary (a hard zero would let the prior veto a correct detection
outright), so the boundary is documented and TESTED in both regimes rather
than papered over -- see `test_the_absence_distinction_is_STRICT_below_the_
floor_threshold` and `test_the_absence_distinction_COLLAPSES_above_1_over_
floor`. The same limitation is recorded in the design spec, because the paper
must state the threshold rather than claim the distinction holds everywhere.

WEEKS
-----
`week` is 1..52 on input and on output, validated at both ends. Circular
52 <-> 1 adjacency is deliberately NOT handled here: this module only shrinks
toward coarser levels, and blending neighbouring weeks belongs to the smoothing
stage. A week outside 1..52 is rejected rather than silently dropped from the
emitted grid, which would delete its records from the cell profile with no
error.

The DEFAULT axes are the OBSERVED axes: `cells=None` means the cells present
in the table and `weeks=None` means the weeks present in it. An empty table
therefore yields an EMPTY grid under default axes, even when `cells=` is given
explicitly. There is no invented fallback week: a caller who wants a grid over
a table with no rows must name the `weeks=` it wants, because any week this
module chose for it would be arbitrary and silent.

CELLS
-----
`cell` is an opaque string, already normalised upstream, at ONE H3 resolution.
It is passed through byte-for-byte: no local casefolding, no parent-cell
derivation. Two differently-cased spellings are two different cells here, which
is what makes a redundant second normalisation visible instead of a no-op.

ERROR CONTRACT
--------------
Every contract violation raises `ValueError`. There is no try/except: an
upstream shape change must stop the pipeline, not be warned about and carried,
because a warn-and-continue path here produces a prior that is confidently
wrong exactly where the data is thinnest.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from .indexing import WEEKS_PER_YEAR

FLOOR = 1e-6

_REQUIRED_TYPES = {
    "species_key": pa.int64(),
    "cell": pa.string(),
    "week": pa.int8(),
    "n": pa.int64(),
    "total": pa.int64(),
}


@dataclass(frozen=True)
class BackoffParams:
    """Shrinkage strengths. Higher = pull harder toward the coarser level."""

    cell_week_weight: float = 10.0
    cell_weight: float = 50.0
    floor: float = FLOOR


def _shrink(local_num: float, local_den: float, coarse_p: float, weight: float) -> float:
    """Posterior mean with a `weight`-strength pseudo-count at `coarse_p`."""
    return (local_num + weight * coarse_p) / (local_den + weight)


def _validate_table(ratios: pa.Table) -> None:
    """Reject anything that is not a RatioTable. Never repairs, never casts."""
    for name, expected in _REQUIRED_TYPES.items():
        if name not in ratios.column_names:
            raise ValueError(
                f"RatioTable is missing the {name!r} column; got "
                f"{ratios.column_names!r}. This module needs both {'n'!r} and "
                f"{'total'!r}: reading {'ratio'!r} alone would discard the "
                "effort weight that decides how far to shrink"
            )
        actual = ratios.schema.field(name).type
        if actual != expected:
            # Deliberately not cast: `week` and `cell` are derived exactly
            # once, upstream. A cast here would be a second derivation.
            raise ValueError(
                f"RatioTable column {name!r} must be {expected}, got {actual}"
            )
        if ratios.column(name).null_count:
            raise ValueError(
                f"RatioTable column {name!r} contains null values; a null key "
                "vanishes from the emitted grid and a null count poisons a "
                "denominator, neither with an error"
            )


def _validate_grid_axis(values: Sequence[object], name: str) -> None:
    """Reject duplicates on an output axis.

    A repeated value emits duplicate grid rows, and a downstream tensor would
    silently keep whichever one it indexed last.
    """
    seen: set[object] = set()
    for value in values:
        if value in seen:
            raise ValueError(
                f"duplicate {name} {value!r} in the requested grid; the output "
                "would carry two rows for one (species, cell, week)"
            )
        seen.add(value)


def _validate_week(week: int, where: str) -> None:
    if not 1 <= week <= WEEKS_PER_YEAR:
        raise ValueError(
            f"week {week!r} in {where} is outside 1..{WEEKS_PER_YEAR}; weeks "
            "are folded into that range upstream, so a value outside it means "
            "a different derivation leaked in"
        )


def hierarchical_estimate(
    ratios: pa.Table,
    species_keys: Sequence[int],
    params: BackoffParams = BackoffParams(),
    *,
    cells: Sequence[str] | None = None,
    weeks: Sequence[int] | None = None,
) -> pa.Table:
    """Estimate P(species | cell, week) over the full requested grid."""
    _validate_table(ratios)
    _validate_grid_axis(list(species_keys), "species_key")
    if cells is not None:
        _validate_grid_axis(list(cells), "cell")
    if weeks is not None:
        _validate_grid_axis([int(w) for w in weeks], "week")
        for week in weeks:
            _validate_week(int(week), "the requested weeks")

    rows = ratios.to_pylist()

    # Effort per cell-week, read from `total` and recorded ONCE per (cell,
    # week). Counting it per row would multiply a cell-week's effort by the
    # number of species present in it.
    cell_week_total: dict[tuple[str, int], int] = {}
    cell_week_n_sum: dict[tuple[str, int], int] = {}
    cell_week_species: dict[tuple[int, str, int], float] = {}
    cell_species: dict[tuple[int, str], float] = {}
    global_species: dict[int, float] = {}

    for row in rows:
        sk = row["species_key"]
        cell = row["cell"]
        week = int(row["week"])
        n = row["n"]
        total = row["total"]

        _validate_week(week, "the RatioTable")
        if total < 1:
            raise ValueError(
                f"RatioTable row ({sk}, {cell!r}, {week}) has total={total}; "
                "the cell-week effort is a count of records and must be >= 1, "
                "and dividing by it would give inf or nan rather than an error"
            )
        if n < 1:
            raise ValueError(
                f"RatioTable row ({sk}, {cell!r}, {week}) has n={n}; absence "
                "is ROW ABSENCE in this table, so a non-positive count means a "
                "zero-fill was mistaken for an observation"
            )
        if n > total:
            raise ValueError(
                f"RatioTable row ({sk}, {cell!r}, {week}) has n={n} which "
                f"exceeds its own total={total}; the numerator and the "
                "denominator are no longer paired row-for-row"
            )

        key = (cell, week)
        if key in cell_week_total and cell_week_total[key] != total:
            raise ValueError(
                f"RatioTable cell-week {key!r} carries two different totals "
                f"({cell_week_total[key]} and {total}); `total` is a property "
                "of the (cell, week), so picking one would make every parent "
                "denominator depend on row order"
            )
        cell_week_total[key] = total
        cell_week_n_sum[key] = cell_week_n_sum.get(key, 0) + n

        row_key = (sk, cell, week)
        if row_key in cell_week_species:
            raise ValueError(
                f"RatioTable has a duplicate row for {row_key!r}; a RatioTable "
                "holds one row per (species, cell, week). Duplicates arrive by "
                "concatenating two independently-computed tables, where each "
                "copy's partial `n` is paired with a `total` that already "
                "covers the whole cell-week -- summing them double-counts the "
                "numerator and keeping one discards records"
            )
        cell_week_species[row_key] = float(n)
        cell_species[(sk, cell)] = cell_species.get((sk, cell), 0.0) + n
        global_species[sk] = global_species.get(sk, 0.0) + n

    # Cell-week COUNT COHERENCE. `n <= total` per row is not enough: two
    # partial RatioTables covering DISJOINT species of the same cell-week
    # produce no duplicate key, agreeing totals and a legal `n` on every row,
    # yet their shares sum above 1.0 (60 + 80 against a total of 100 emits
    # p = [0.6, 0.8]). `total` covers ALL records in the cell-week, so the
    # species present in it can never out-count it.
    for (cell, week), n_sum in cell_week_n_sum.items():
        if n_sum > cell_week_total[(cell, week)]:
            raise ValueError(
                f"RatioTable cell-week {(cell, week)!r} has species counts "
                f"summing to {n_sum}, which exceeds its total="
                f"{cell_week_total[(cell, week)]}; `total` counts ALL records "
                "in the cell-week, so the rows present cannot out-count it -- "
                "this is two partial tables of DISJOINT species concatenated, "
                "each `n` paired with a `total` for the whole cell-week"
            )

    # Parent denominators: sums of COUNTS, over distinct cell-weeks.
    cell_n: dict[str, float] = {}
    global_n = 0.0
    for (cell, _week), total in cell_week_total.items():
        cell_n[cell] = cell_n.get(cell, 0.0) + total
        global_n += total

    # DEFAULT AXES ARE THE OBSERVED AXES. An empty table yields an empty grid;
    # no fallback week is invented, because any week chosen here would be an
    # arbitrary, silent shape decision made on the caller's behalf.
    all_cells = list(cells) if cells is not None else sorted(cell_n)
    all_weeks = (
        [int(w) for w in weeks]
        if weeks is not None
        else sorted({w for _, w in cell_week_total})
    )

    out_species: list[int] = []
    out_cells: list[str] = []
    out_weeks: list[int] = []
    out_p: list[float] = []

    for sk in species_keys:
        p_global = (global_species.get(sk, 0.0) / global_n) if global_n else 0.0
        p_global = max(p_global, params.floor)
        for cell in all_cells:
            p_cell = _shrink(
                cell_species.get((sk, cell), 0.0),
                cell_n.get(cell, 0.0),
                p_global,
                params.cell_weight,
            )
            for week in all_weeks:
                p = _shrink(
                    cell_week_species.get((sk, cell, week), 0.0),
                    cell_week_total.get((cell, week), 0.0),
                    p_cell,
                    params.cell_week_weight,
                )
                out_species.append(sk)
                out_cells.append(cell)
                out_weeks.append(week)
                out_p.append(min(max(p, params.floor), 1.0))

    return pa.table(
        {
            "species_key": pa.array(out_species, pa.int64()),
            "cell": pa.array(out_cells, pa.string()),
            "week": pa.array(out_weeks, pa.int8()),
            "p": pa.array(out_p, pa.float64()),
        }
    )
