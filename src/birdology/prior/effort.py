"""Target-group background correction.

Raw occurrence counts measure observers, not birds: Copenhagen looks
species-rich and west Jutland looks empty, and both are artifacts of where
birders live. Using each species' *share* of all bird records in the same
cell-week puts observer effort in both numerator and denominator, where it
cancels to first order.

Limitation, stated in the paper: this corrects for effort density, not
observer preference. Rarities are reported disproportionately.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def target_group_ratios(counts: pa.Table) -> pa.Table:
    """Add per-(cell, week) totals and each species' share of them."""
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
    ratio = pc.divide(
        pc.cast(joined.column("n"), pa.float64()),
        pc.cast(joined.column("total"), pa.float64()),
    )
    return joined.append_column("ratio", ratio).select(
        ["species_key", "cell", "week", "n", "total", "ratio"]
    )
