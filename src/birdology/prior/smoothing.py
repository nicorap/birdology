"""Smooth the prior across neighbouring hexes and adjacent weeks.

Neighbours come from `h3.grid_disk`, never from lat/lon arithmetic: a hex at
the antimeridian or near a pole has six neighbours like any other, which a
naive +/- degree lookup would get wrong. (The twelve pentagon cells per
resolution have five; the code reads the ring rather than assuming a count.)

Weeks are circular — week 52 is adjacent to week 1 — because a migration
that peaks in late December does not restart in January. THIS MODULE OWNS
THAT SEAM. The backoff stage deliberately does not blend weeks at all and
asserts that it does not, so nothing upstream has pre-joined 52 to 1: if the
wrap were dropped here the seam would simply be open, every value either side
of New Year would be smoothed from one neighbour instead of two, and no other
stage would notice.

WHAT THE OUTPUT `p` MEANS -- AND WHAT IS NOT DONE TO IT
-------------------------------------------------------
Each output value is a weighted average of a cell-week's own `p` and the `p`
of its neighbours that are present in the grid, with the cell's own value
weighted 1.0. That is a convex combination, so `p` keeps the meaning it had
upstream: a shrunk estimate of P(species | a bird was recorded here this
week).

`p` is NOT renormalised over species, here or anywhere. Dividing by `weight`
normalises the KERNEL -- the neighbour weights sum to one -- which is a
different operation from scaling the species axis to sum to 1.0. The values
over a requested species list may legitimately sum above 1.0 (a species
absent from the occurrence data contributes a floor on top) or far below it (a
subset of species was requested), and forcing them to 1.0 would inflate the
survivors of a filtered label set to fill mass belonging to the species that
were dropped.

For the same reason there is no clamp on the output: with non-negative
weights a convex combination of values already in [0, 1] cannot leave [0, 1],
so a clamp could only mask an upstream violation instead of letting it
surface. The INPUT `p` is therefore validated to be finite and inside
[0, 1] -- the premise of that argument is checked where it can be observed,
rather than assumed and then hidden by a clamp. Consumers of this stage may
rely on the output `p` being finite and in [0, 1]: it is enforced here, not
merely inherited.

ISOLATED CELLS AND ABSENT NEIGHBOURS
------------------------------------
A cell may have NO neighbour in the grid (a boundary cell, or a single-cell
fixture), and a requested week subset need not contain week 1 at all. Both
are ordinary. The accumulator therefore starts at the cell's own value with
`weight = 1.0`, which is load-bearing: starting at 0.0 would divide by zero on
an isolated cell and emit nan instead of the value the cell already had.

A neighbour that is not in the grid is SKIPPED, never imputed as zero: "not
in this grid" is not "absent here", and imputing zero would drag every
boundary cell and every end-of-window week toward absence.

SCALE
-----
The input is the DENSE grid from the backoff stage -- species x cells x 52
rows, which is millions of rows for a national run. This module therefore
does not build a Python dict keyed by every (species, cell, week) tuple; keys
are packed into one int64 array and neighbours are resolved with
`np.searchsorted`. The packing is mixed-radix and so injective, and an absent
neighbour is encoded with a cell digit no real row can carry, so a missing
neighbour can never alias a different row's key.

CELLS
-----
`cell` is opaque and case-sensitive, and it is passed through byte-for-byte:
no casefolding, no re-derivation. But this stage is the first that has to ASK
H3 about a cell, and H3 answers with lower-case canonical strings, so a row
whose spelling is not canonical would match no neighbour at all -- a spatial
term that silently does nothing. Non-canonical spellings and mixed
resolutions are therefore rejected rather than normalised.

ERROR CONTRACT
--------------
Every contract violation raises `ValueError`. There is no try/except and no
repair: the defects this stage can introduce (an open seam, a neighbour
unmatched because two resolutions were mixed, a duplicate grid point resolved
by row order) all produce a plausible-looking prior with no error, no nan and
no missing rows, so they must stop the pipeline instead of being carried.
"""
from __future__ import annotations

from dataclasses import dataclass

import h3
import numpy as np
import pyarrow as pa

from .arrow_strings import dictionary_codes
from .indexing import WEEKS_PER_YEAR

_REQUIRED_TYPES = {
    "species_key": pa.int64(),
    "cell": pa.string(),
    "week": pa.int8(),
    "p": pa.float64(),
}


@dataclass(frozen=True)
class SmoothingParams:
    """Neighbour weights, relative to the cell's own value of 1.0.

    UNTUNED, LIKE THE BACKOFF'S -- BUT MEASURABLY THE SMALLER EFFECT (I2)
    ---------------------------------------------------------------------
    `space_weight = 0.3` and `time_weight = 0.3` were chosen by judgement and
    never swept. Read `BackoffParams`' docstring first: it carries the full
    retention table and the argument that the paper must sweep these
    constants. This one records what THESE two are worth, which is much less
    than the shrinkage -- and that is a measurement, not a defence.

    A weight is relative to the cell's own value of 1.0, so `0.3` means each of
    the six hex neighbours (and each of the two adjacent weeks) counts for
    three tenths of a vote against the cell's own one. At `space_weight = 0.3`
    a cell with six present neighbours has 6 * 0.3 = 1.8 of foreign weight
    against its own 1.0, so a majority of the output value is already its
    neighbours'.

    MEASURED, at H3 resolution 5 on a 167 879-record `build_from_archive` build
    with a known signal (fraction of the true weekly / across-cell CV that
    reaches the `.npz`; the other three constants at their defaults):

        space_weight   0.0     0.3(default)   1.0
        seasonal       0.506   0.426          0.423
        spatial        0.917   0.887          0.881

        time_weight    0.0     0.3(default)   1.0
        seasonal       0.466   0.426          0.413
        spatial        0.892   0.887          0.898

    Three things follow, all of which contradict what one would guess:

    1. `space_weight` COSTS SEASONAL AMPLITUDE (0.506 -> 0.426) more than it
       costs spatial contrast (0.917 -> 0.887). Blurring a thin cell-week
       across its neighbours is another way of averaging away a weekly peak.
    2. Neither weight is where the amplitude goes. `cell_week_weight` moves
       seasonal retention from 1.094 to 0.134 over its swept range; these two
       move it by ~0.08 and ~0.05.
    3. Both response curves are FLAT above the default -- 0.3 and 1.0 are
       nearly the same artifact -- because a cell's six neighbours in a clumped
       observer field mostly resemble it. Almost all of the smoothing these
       constants do is done by the first 0.3.

    `0.0` is a legitimate value for either and is not a special case: the
    short-circuits in `smooth` make it skip the term, which is a cost guard
    rather than a semantic one. `save_prior` writes this object into every
    sidecar, so an artifact says which row of the table above applies to it.
    """

    space_weight: float = 0.3
    time_weight: float = 0.3


def _adjacent_weeks(week: int) -> tuple[int, int]:
    """The two weeks either side of *week*, wrapping 52 <-> 1.

    The single definition of the seam: the vectorised path reads a lookup
    table built from this function, so the wrap cannot be true in one path and
    false in the other.
    """
    prev = week - 1 or WEEKS_PER_YEAR
    nxt = week + 1 if week < WEEKS_PER_YEAR else 1
    return prev, nxt


# Row w holds the (prev, next) of week w. Row 0 is an unreachable placeholder:
# weeks are validated to 1..52 before this table is indexed, and leaving it out
# would make a raw `weeks` index off-by-one.
_ADJACENT = np.array(
    [(0, 0), *(_adjacent_weeks(w) for w in range(1, WEEKS_PER_YEAR + 1))],
    dtype=np.int64,
)


def _validate_table(estimates: pa.Table) -> None:
    """Reject anything that is not an estimate table. Never repairs, casts."""
    for name, expected in _REQUIRED_TYPES.items():
        if name not in estimates.column_names:
            raise ValueError(
                f"estimate table is missing the {name!r} column; got "
                f"{estimates.column_names!r}"
            )
        actual = estimates.schema.field(name).type
        if actual != expected:
            # Deliberately not cast: `cell` and `week` are derived exactly
            # once, upstream, and a cast here would be a second derivation. A
            # float32 `p` would also lose the floor the backoff guarantees.
            raise ValueError(
                f"estimate table column {name!r} must be {expected}, got {actual}"
            )
        if estimates.column(name).null_count:
            raise ValueError(
                f"estimate table column {name!r} contains null values; a null "
                "key matches no neighbour and a null `p` poisons every "
                "neighbour that reads it, neither with an error"
            )

    # `p` must be a probability, not merely a float64 without nulls. The null
    # guard above rejects the NULL form of exactly this defect; nan and inf are
    # the same defect in a form Arrow's null_count cannot see. A single nan at
    # the centre of a ring turns every cell that reads it into nan, and an
    # out-of-range value is carried straight through the convex combination
    # into the output, because this module deliberately does not clamp (a
    # clamp would mask the violation instead of surfacing it). The two clauses
    # are checked SEPARATELY: a column of nan alongside finite values violates
    # only the first, and a column of -5.0 only the second.
    values = _numeric(estimates.column("p"), np.float64)
    if not np.all(np.isfinite(values)):
        raise ValueError(
            "estimate table column 'p' contains non-finite values; a nan or "
            "inf `p` is averaged into every neighbour that reads it and turns "
            "a whole neighbourhood non-finite with no error"
        )
    if values.size and (values.min() < 0.0 or values.max() > 1.0):
        raise ValueError(
            "estimate table column 'p' has values outside [0.0, 1.0] "
            f"({values.min()}..{values.max()}); `p` is a probability, and "
            "this module emits a convex combination without clamping, so an "
            "out-of-range input is carried straight into the output"
        )


def _validate_params(params: SmoothingParams) -> None:
    """Weights must be finite and non-negative.

    A negative weight is not a smoother: it subtracts its neighbour's
    evidence, can drive the total weight to zero (a division by zero) and can
    push `p` outside [0, 1]. A nan weight passes the `if params.space_weight`
    truth test and silently turns the whole column into nan.
    """
    for name, value in (
        ("space_weight", params.space_weight),
        ("time_weight", params.time_weight),
    ):
        if not np.isfinite(value):
            raise ValueError(
                f"{name}={value!r} is not finite; a nan weight is truthy and "
                "would turn every smoothed value into nan"
            )
        if value < 0.0:
            raise ValueError(
                f"{name}={value!r} is negative; a neighbour weight below zero "
                "subtracts its neighbour's evidence and can leave `p` outside "
                "[0, 1] or divide by a zero total weight"
            )


def _validate_weeks(weeks: np.ndarray) -> None:
    if weeks.size and (weeks.min() < 1 or weeks.max() > WEEKS_PER_YEAR):
        raise ValueError(
            f"estimate table has week values outside 1..{WEEKS_PER_YEAR} "
            f"({weeks.min()}..{weeks.max()}); weeks are folded into that range "
            "upstream, and a week outside it has no defined neighbours"
        )


def _validate_cells(cells: list[str]) -> None:
    """Real H3 cells, canonically spelled, all at ONE resolution.

    Both failure modes are silent without this guard. A non-canonical
    spelling (upper case, say) is still a valid cell to `h3.is_valid_cell`,
    but `grid_disk` emits lower case, so the row matches no neighbour. Mixed
    resolutions are worse: `grid_disk` answers for each cell, yet a res-4 and
    a res-5 hex are never neighbours, so the spatial term evaluates to a
    no-op over the whole table.
    """
    resolutions = set()
    for cell in cells:
        # Raises H3CellInvalidError (a ValueError) for a synthetic name such
        # as the 'c' that upstream fixtures use.
        resolutions.add(h3.get_resolution(cell))
        if h3.int_to_str(h3.str_to_int(cell)) != cell:
            raise ValueError(
                f"cell {cell!r} is not H3's canonical spelling of itself; "
                "`h3.grid_disk` emits canonical lower-case strings, so this "
                "row would match no neighbour and be silently left unsmoothed"
            )
    if len(resolutions) > 1:
        raise ValueError(
            f"estimate table mixes H3 resolutions {sorted(resolutions)!r}; "
            "cells at different resolutions are never neighbours, so the "
            "spatial term would silently do nothing"
        )


def _single_array(column: pa.ChunkedArray | pa.Array) -> pa.Array:
    """One contiguous Arrow array, whatever the input chunking."""
    if isinstance(column, pa.ChunkedArray):
        return column.combine_chunks()
    return column


def _codes(values: pa.Array | pa.ChunkedArray) -> tuple[np.ndarray, list]:
    """Dense 0..k-1 codes plus the distinct values they index.

    Takes the column CHUNKED and never concatenates it (job 5722): a
    species chunk of the full grid is 50 x cells x 52 rows, whose 15-byte
    cell text crosses the 2 GiB int32-offset limit at ~55k cells, and
    ``combine_chunks`` of such a ``pa.string`` column raises "offset
    overflow". ``dictionary_codes`` yields the same codes in the same
    first-occurrence order.
    """
    codes, distinct = dictionary_codes(values)
    return codes, distinct.to_pylist()


def _neighbour_codes(cells: list[str]) -> np.ndarray:
    """(n_cells, ring_width) matrix of neighbour codes; `len(cells)` = absent.

    `h3.grid_disk` is the only source of adjacency: it is right at the
    antimeridian, at the poles and at the pentagons, where lat/lon arithmetic
    is wrong. The origin cell is dropped from its own ring — `grid_disk`
    includes it, and keeping it would double the cell's own weight.
    """
    absent = len(cells)
    code_of = {cell: i for i, cell in enumerate(cells)}
    rings = [
        [code_of[c] for c in h3.grid_disk(cell, 1) if c != cell and c in code_of]
        for cell in cells
    ]
    width = max((len(ring) for ring in rings), default=0)
    matrix = np.full((len(cells), width), absent, dtype=np.int64)
    for i, ring in enumerate(rings):
        matrix[i, : len(ring)] = ring
    return matrix


def _numeric(column: pa.ChunkedArray | pa.Array, dtype: type) -> np.ndarray:
    return np.asarray(
        _single_array(column).to_numpy(zero_copy_only=False), dtype=dtype
    )


def smooth(
    estimates: pa.Table, params: SmoothingParams = SmoothingParams()
) -> pa.Table:
    """Weighted average of each point with its space and time neighbours."""
    _validate_table(estimates)
    _validate_params(params)

    weeks = _numeric(estimates.column("week"), np.int64)
    _validate_weeks(weeks)

    species, _ = _codes(_single_array(estimates.column("species_key")))
    cell_code, cells = _codes(estimates.column("cell"))
    _validate_cells(cells)

    # Mixed-radix packing, hence injective. The cell digit is bounded by
    # len(cells) + 1 and the week digit by WEEKS_PER_YEAR + 1; `len(cells)` is
    # reserved for "no such neighbour", so an absent neighbour's key cannot
    # collide with any real row's key.
    #
    # The two `+ 1`s are NOT equally load-bearing, despite looking symmetric.
    # `cell_stride`'s is REAL headroom: `_neighbour_codes` pads an absent
    # neighbour with the code `len(cells)`, which is one past the last real
    # cell code, so the stride must be `len(cells) + 1` for that sentinel to
    # occupy a digit of its own. Drop it and the sentinel wraps into the next
    # species' digit and aliases a real row -- a silently different number,
    # not an error (mutant S43).
    #
    # `week_stride`'s `+ 1` is INERT BUT SYMMETRIC: week digits are validated
    # to 1..52 before this point and there is no week sentinel, so digit 0 is
    # unreachable and `WEEKS_PER_YEAR` would be an equally injective radix.
    # It is kept so the two strides read the same way and so a future week
    # sentinel would have somewhere to go. A mutant of it is EQUIVALENT and is
    # deliberately not registered; see the harness's note on it.
    cell_stride = len(cells) + 1
    week_stride = WEEKS_PER_YEAR + 1

    def pack(cell_digit: np.ndarray, week_digit: np.ndarray) -> np.ndarray:
        return (species * cell_stride + cell_digit) * week_stride + week_digit

    keys = pack(cell_code, weeks)
    order = np.argsort(keys, kind="stable")
    sorted_keys = keys[order]
    # No size guard: `np.diff` of a 0- or 1-element array is empty and
    # `np.any` of an empty array is False, so the fewer-than-two-rows case is
    # already handled by the arithmetic. The `sorted_keys.size > 1 and` that
    # used to sit here was unreachable as a guard, and it contradicted
    # `tensor._reject_duplicates`, which states this same fact in its
    # docstring and carries no such conjunct. Two modules disagreeing about
    # one arithmetic fact is how the next reader concludes one of them knows
    # something the other does not.
    if np.any(np.diff(sorted_keys) == 0):
        raise ValueError(
            "estimate table has duplicate (species_key, cell, week) rows; one "
            "grid point with two values means a neighbour reads whichever copy "
            "it happens to find, so the output depends on row order"
        )

    def neighbour_values(wanted: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """(row index, found mask) for each wanted packed key."""
        position = np.searchsorted(sorted_keys, wanted)
        position = np.minimum(position, len(sorted_keys) - 1)
        return order[position], sorted_keys[position] == wanted

    p = _numeric(estimates.column("p"), np.float64)
    total = p.copy()
    # LOAD-BEARING: the cell's own value carries weight 1.0. A cell with no
    # neighbour in the grid divides by this and nothing else.
    weight = np.ones(p.shape, dtype=np.float64)

    # These two short-circuits are a COST guard, not a semantic one: a weight
    # of 0.0 would add 0.0-weighted neighbours and change nothing, so removing
    # them is numerically equivalent (see the mutation harness's note on S13).
    # They matter because the bandwidth sweep does pass 0.0, and the ring
    # lookup over a national cell set is not free.
    if params.space_weight:
        ring = _neighbour_codes(cells)
        for slot in range(ring.shape[1]):
            index, found = neighbour_values(pack(ring[cell_code, slot], weeks))
            total[found] += params.space_weight * p[index[found]]
            weight[found] += params.space_weight

    if params.time_weight:
        for side in range(_ADJACENT.shape[1]):
            index, found = neighbour_values(pack(cell_code, _ADJACENT[weeks, side]))
            total[found] += params.time_weight * p[index[found]]
            weight[found] += params.time_weight

    return pa.table(
        {
            "species_key": estimates.column("species_key"),
            "cell": estimates.column("cell"),
            "week": estimates.column("week"),
            "p": pa.array(total / weight, pa.float64()),
        }
    )
