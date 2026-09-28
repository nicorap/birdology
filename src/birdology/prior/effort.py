"""Target-group background correction.

Raw occurrence counts measure observers, not birds: Copenhagen looks
species-rich and west Jutland looks empty, and both are artifacts of where
birders live. Using each species' *share* of all bird records in the same
cell-week puts observer effort in both numerator and denominator, where it
cancels to first order.

Limitation, stated in the paper: this corrects for effort density, not
observer preference. Rarities are reported disproportionately.

Input contract (from `indexing.aggregate_counts`, AFTER the leakage guards):
`species_key` int64, `cell` non-null normalised lowercase H3 string, `week`
int8 in 1..52, `n` int64 >= 1. This module VALIDATES that contract and raises;
it never re-derives `week` or `cell`, and it never repairs a violation. Two
defects earlier in this plan came from a second, local derivation of a value
that already had one.

The denominator is the sum of the very `n` values that form the numerator, so
it is >= 1 whenever the contract holds and the row is present. It can only
misbehave two ways, and both are checked up front rather than allowed to
surface as `inf`/`nan` or as a vanished row:

* `n <= 0` -- a cell-week can then total 0 (`0/0` -> nan) or a negative
  number, and nothing downstream distinguishes that from a genuine ratio;
* a null in `cell`, `week` or `n` -- a null join key matches nothing in the
  inner join, so the row is SILENTLY DROPPED and the prior is simply missing a
  cell-week with no error anywhere.

This validation is deliberately never caught-and-continued. A contract
violation here means the upstream pipeline changed shape, and a warn-and-carry
path would reproduce the silent-failure class the leakage guards exist to kill.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

_REQUIRED_TYPES = {
    "species_key": pa.int64(),
    "cell": pa.string(),
    "week": pa.int8(),
    "n": pa.int64(),
}


def _validate(counts: pa.Table) -> None:
    """Reject anything that violates the CountTable contract. Never repairs."""
    for name, expected in _REQUIRED_TYPES.items():
        if name not in counts.column_names:
            raise ValueError(
                f"CountTable is missing the {name!r} column; got "
                f"{counts.column_names!r}"
            )
        actual = counts.schema.field(name).type
        if actual != expected:
            # Deliberately not cast: `week` and `cell` are derived exactly once,
            # upstream. A cast here would be a second derivation.
            raise ValueError(
                f"CountTable column {name!r} must be {expected}, got {actual}"
            )

    for name in ("cell", "week", "n"):
        if counts.column(name).null_count:
            raise ValueError(
                f"CountTable column {name!r} contains null values; a null "
                "(cell, week) key joins to nothing and would silently drop the "
                "row from the prior"
            )

    if counts.num_rows and pc.min(counts.column("n")).as_py() < 1:
        raise ValueError(
            "CountTable column n must be >= 1 (it is a count of records); "
            "a non-positive count can drive a cell-week total to zero, and "
            "0/0 is nan rather than an error"
        )


def target_group_ratios(counts: pa.Table) -> pa.Table:
    """Add per-(cell, week) totals and each species' share of them."""
    _validate(counts)

    if counts.num_rows == 0:
        return counts.append_column(
            "total", pa.array([], pa.int64())
        ).append_column("ratio", pa.array([], pa.float64()))

    totals = (
        counts.select(["cell", "week", "n"])
        .group_by(["cell", "week"])
        .aggregate([("n", "sum")])
        .rename_columns(["cell", "week", "total"])
    )
    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")
    if joined.num_rows != counts.num_rows:
        # Unreachable while `_validate` holds: every (cell, week) in `counts`
        # is by construction a key in `totals`. Kept as defence in depth,
        # because the failure it catches -- a row silently absent from the
        # prior -- is invisible downstream. It reads as a mild prior, not a bug.
        raise ValueError(
            f"join changed the row count ({counts.num_rows} -> {joined.num_rows}); "
            "a (cell, week) key failed to match its own total"
        )
    ratio = pc.divide(
        pc.cast(joined.column("n"), pa.float64()),
        pc.cast(joined.column("total"), pa.float64()),
    )
    return joined.append_column("ratio", ratio).select(
        ["species_key", "cell", "week", "n", "total", "ratio"]
    )
