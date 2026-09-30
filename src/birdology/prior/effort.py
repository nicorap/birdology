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
int8 in 1..52, `n` int64 >= 1, AND the Arrow schema-metadata marker
`leakage.GUARD_MARKER_KEY`.

THE GUARD MARKER IS PART OF THE CONTRACT, NOT A CONVENTION
-----------------------------------------------------------
"AFTER the leakage guards" used to be prose. It is now checked: a CountTable
without the marker is REFUSED. A guarded and an unguarded CountTable are
structurally identical, so nothing else in this module -- or anywhere
downstream -- can tell them apart, and the consequence of getting it wrong is
invisible rather than loud: the leaked rows stay in the cell-week denominator
and under-weight every guarded cell-week by the same factor, producing a
plausible, mildly-flat prior. See `_require_guard_marker`.

What the marker does NOT assert: that a non-empty evaluation split was wired
in. `apply_leakage_guards(table, [])` stamps too, because `[]` is legitimate.
Requiring a split belongs to the build script.

What this module actually checks, stated precisely because a later task will
otherwise lean on a guarantee that is not here: it validates the four column
DTYPES, the PRESENCE of each column, NULLITY of `cell`/`week`/`n`, and the
`n >= 1` floor. It does NOT check the VALUE RANGE of `week` (1..52), and it
does NOT check that `cell` is lowercase or a well-formed H3 index -- those
belong to `indexing.py`, upstream, and are deliberately not re-derived here.
A `week` of 0 or 99 passes through this module untouched. (Not hypothetical:
Task 6 found exactly such a week being accepted here and then silently
dropped one module downstream.)

Whatever it does check, it raises on; it never re-derives `week` or `cell`,
and it never repairs a violation. Two defects earlier in this plan came from a
second, local derivation of a value that already had one.

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

Input validation is not enough on its own. It catches rows that arrive broken;
it cannot catch rows that leave broken. The output is therefore re-checked
before it is returned: `total >= 1` on every row, `ratio` finite and inside
[0, 1]. The post-join row-count invariant only sees rows that VANISH -- a row
that is kept but poisoned (null `total`, `inf`/`nan` `ratio`) passes it
untouched, because the count still matches.

WHAT `ratio` MEANS -- read this before consuming it
---------------------------------------------------
`ratio` is P(species | a bird was recorded in this cell-week). It is a SHARE
of the target-group background, NOT P(species is present). The two are easy to
confuse because `ratio` looks like a probability: it lies in [0, 1] and sums to
1.0 across the species of a cell-week.

Two consequences, both of which silently corrupt a prior rather than raise:

* A cell-week absent from this table means NOBODY LOOKED, not that no bird was
  there. Absence is ROW ABSENCE. Zero-filling missing cell-weeks with
  `ratio = 0` turns "unsurveyed" into "certainly absent" -- the classic
  target-group-background pitfall.
* `ratio == 0` never occurs on contract-conforming input (`n >= 1` forces
  `ratio > 0`), so a zero in this column is a bug, not an observation.

Error contract
--------------
Every contract violation raises `ValueError`, with one documented escape: a
cell-week total above 2**53 is not exactly representable in float64, so
`pc.cast` raises `pyarrow.lib.ArrowInvalid` instead. That is unreachable at
GBIF scale (order 1e9 bird records globally, spread over millions of
cell-weeks), and it is recorded here only so the uniform-`ValueError` claim is
honest.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from .leakage import GUARD_MARKER_KEY, guards_applied

_REQUIRED_TYPES = {
    "species_key": pa.int64(),
    "cell": pa.string(),
    "week": pa.int8(),
    "n": pa.int64(),
}


def _require_guard_marker(counts: pa.Table) -> None:
    """The CountTable must come from a table the leakage guards stamped.

    A guarded and an unguarded CountTable are structurally identical -- the
    same four columns, the same dtypes, the same nullability -- so no check
    over the DATA can tell them apart. This one check is therefore the only
    place in the codebase where the distinction is mechanically enforceable,
    and the failure it closes is the quietest in the plan: an unguarded table
    leaves the leaked rows in the per-cell-week DENOMINATOR, which
    under-weights every guarded cell-week UNIFORMLY. The result is a smooth,
    plausible, mildly-flat prior -- no error, no nan, no missing row, nothing
    a reviewer or a downstream stage could see.

    It raises rather than warns, and it is checked BEFORE the column contract:
    a table that never saw a guard is not a CountTable with a fixable problem,
    it is the wrong table.

    What this does NOT establish: that the guards were given the RIGHT test
    split. The marker says both guards ran, not that a non-empty split was
    supplied -- `[]` is a legitimate mode and stamps too. Requiring a split to
    have been wired in is the build script's job, because it is the only layer
    that knows one was meant to be.
    """
    if not guards_applied(counts):
        raise ValueError(
            "CountTable does not carry the leakage-guard marker "
            f"({GUARD_MARKER_KEY!r}); the effort denominator must be computed "
            "over the table `leakage.apply_leakage_guards` returned. A "
            "guarded and an unguarded CountTable are structurally identical, "
            "so this marker is the only thing that can tell them apart: an "
            "unguarded denominator keeps every leaked record in the "
            "cell-week total and under-weights every guarded cell-week "
            "uniformly, producing a plausible, mildly-flat prior with no "
            "error and no missing row"
        )


def _validate(counts: pa.Table) -> None:
    """Reject anything that violates the CountTable contract. Never repairs."""
    for name in ("total", "ratio"):
        if name in counts.column_names:
            raise ValueError(
                f"CountTable already carries a {name!r} column; this looks "
                "like a RatioTable fed back in. Appending would produce a "
                "duplicate field name, and the join would pick one at random"
            )

    # AFTER the re-run check, BEFORE the column contract. A RatioTable fed
    # back in has ALSO lost the marker (the inner join drops schema metadata),
    # so marker-first would answer every re-run with the guard message and
    # send the caller to look for a missing guard that ran perfectly well.
    # The more specific diagnosis wins.
    _require_guard_marker(counts)

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


def _check_output(out: pa.Table) -> None:
    """Re-check the emitted RatioTable.

    The input guard sees rows that arrive broken; this sees rows that leave
    broken. The post-join row-count invariant cannot substitute for it: a row
    that is KEPT but poisoned (null `total`, `inf`/`nan` `ratio`) leaves the
    count intact and sails straight into the prior looking like a real share.
    """
    total = out.column("total")
    if total.null_count or pc.min(total).as_py() < 1:
        raise ValueError(
            "RatioTable has a row whose cell-week total is null or < 1; the "
            "denominator is a sum of counts that are each >= 1, so this means "
            "the totals no longer line up with the rows they divide"
        )

    ratio = out.column("ratio")
    if ratio.null_count or not pc.all(pc.is_finite(ratio)).as_py():
        raise ValueError(
            "RatioTable has a null or non-finite ratio (inf/nan); a zero or "
            "missing denominator reached the division instead of being "
            "rejected, and nothing downstream distinguishes inf from a share"
        )
    if pc.min(ratio).as_py() < 0.0 or pc.max(ratio).as_py() > 1.0:
        raise ValueError(
            "RatioTable has a ratio outside [0, 1]; a share of a cell-week "
            "total cannot exceed the total or go negative, so the numerator "
            "and the denominator are no longer paired row-for-row"
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
    out = joined.append_column("ratio", ratio).select(
        ["species_key", "cell", "week", "n", "total", "ratio"]
    )
    _check_output(out)
    return out
