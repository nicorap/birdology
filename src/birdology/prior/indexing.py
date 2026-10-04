"""Map occurrences onto an (H3 cell, ISO week) grid and aggregate to counts.

H3 is used rather than a lat/lon grid because the data spans 0-60+ degrees of
latitude, where lat/lon cells differ in area by a factor of two. Hexagons are
equal-area and have six equidistant neighbours, which the smoothing kernel in
`smoothing.py` relies on.
"""
from __future__ import annotations

from datetime import date

import h3
import pyarrow as pa

from .arrow_strings import TEXT_TYPE, canonical_dictionary, decode_strings
from .leakage import guards_applied, mark_guards_applied

WEEKS_PER_YEAR = 52


def iso_week(d: date) -> int:
    """ISO week number, with week 53 folded into 52.

    71 of every 400 years have an ISO week 53. Keeping it would create a 53rd
    index that the circular smoothing kernel would treat as a real week with a
    seventh of the usual data behind it.
    """
    week = d.isocalendar().week
    return WEEKS_PER_YEAR if week > WEEKS_PER_YEAR else week


def latlng_cells(lats, lons, resolution: int) -> list[str]:
    """H3 cells at *resolution* for parallel lat/lon sequences.

    THE one derivation of a cell from a point: the prior's cell axis is
    built through it (`assign_cells`) and Phase B derives every recording's
    cell through it too, so a prior and its queries can never disagree on
    how a point becomes a cell.
    """
    return [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in zip(lats, lons)]


def assign_cells(table: pa.Table, resolution: int) -> pa.Table:
    """Add `cell` (H3 index at *resolution*) and `week` columns."""
    lats = table.column("lat").to_pylist()
    lons = table.column("lon").to_pylist()
    dates = table.column("event_date").to_pylist()

    cells = latlng_cells(lats, lons, resolution)
    weeks = [iso_week(d) for d in dates]

    return table.append_column(
        "cell", pa.array(cells, pa.string())
    ).append_column("week", pa.array(weeks, pa.int8()))


def aggregate_counts(table: pa.Table) -> pa.Table:
    """Reduce to (species_key, cell, week) -> n.

    Provenance (publisher_key, recorded_by) is dropped — run leakage guards on
    the assign_cells output first.

    THE GUARD MARKER IS RE-STAMPED, CONDITIONALLY (T10-R2)
    ------------------------------------------------------
    `leakage.apply_leakage_guards` stamps its output with Arrow schema
    metadata, and `effort.target_group_ratios` refuses a table without it.
    That marker does NOT survive this function on its own: measured,
    `group_by(...).aggregate(...)` returns a schema whose metadata is `None`,
    and neither `rename_columns` nor `select` restores it. (`filter` DOES
    preserve it, which is why the guards themselves need no re-stamp.) So the
    chain guards -> aggregate -> ratios would lose the marker in the middle
    and every guarded build would be refused.

    The re-stamp is CONDITIONAL on the input carrying the marker, and that is
    the whole of its value. An unconditional stamp would make this function
    authenticate ANY table that passed through it, including one that never
    saw a guard — the check would still be present, still raise on a table
    handed straight to `target_group_ratios`, and yet be decorative for every
    realistic pipeline, which routes through here. Propagating a marker is
    not the same as minting one.

    AN EXISTING ``n`` COLUMN IS A WEIGHT, AND IT IS SUMMED (job 5345)
    -----------------------------------------------------------------
    The parallel parse (`parallel_parse.parse_and_aggregate`) hands over
    PRE-AGGREGATED rows carrying an ``n`` of how many occurrences each
    (species, cell, week, publisher, contributor) group collapsed. Appending
    a fresh ``n = 1`` to such a table would silently count GROUPS instead of
    occurrences — every count >1 shrunk to 1, no error, a plausible flatter
    prior. Measured on the synthetic fixture: 84 of 84 groups undercounted.
    A per-occurrence table (the sequential path) carries no ``n``, so its
    behaviour is unchanged; the two paths produce identical counts, which
    `test_the_aggregates_feed_the_existing_pipeline` pins on a fixture whose
    groups genuinely exceed 1.

    THE pa.string() BOUNDARY (job 5722)
    -----------------------------------
    The parallel parse hands over ``cell`` as
    ``dictionary<int32, large_string>`` -- no 32-bit-offset column survives
    anywhere upstream, because a fleet-sized one overflowed. Everything
    downstream (`effort`, `backoff`, `smoothing`, `tensor`) demands
    ``cell: pa.string()`` and never casts. So THIS is where the narrowing
    happens: after the reduction to (species, cell, week), when the table
    is as small as it will ever be. The aggregation groups on the canonical
    dictionary INDICES (integers; sorted dictionary, so no information is
    lost) and the output cell column is decoded by
    ``arrow_strings.decode_strings`` in chunks bounded far below 2 GiB --
    an unbounded ``.cast(pa.string())`` would be the same overflow again
    at fleet scale. A plain ``pa.string()`` input (the sequential path)
    takes the SAME route since job 6787_5: grouping on the raw strings
    emitted a >2 GiB key chunk at r5 and negative offsets.
    """
    keyed = table
    vocabulary = None
    cell_type = table.schema.field("cell").type
    # ALL text layouts take the integer-code route (job 6787_5). The plain
    # pa.string() input of the sequential path used to go straight into
    # group_by, whose string key output is one unbounded chunk: at r5 over
    # 1.76B occurrences it crossed 2 GiB and wrote negative offsets
    # ("Negative offsets in binary array" at the counts-checkpoint write).
    if (
        pa.types.is_dictionary(cell_type)
        or pa.types.is_large_string(cell_type)
        or pa.types.is_string(cell_type)
    ):
        canonical = canonical_dictionary(table.column("cell"))
        vocabulary = (
            canonical.chunk(0).dictionary
            if canonical.num_chunks
            else pa.array([], pa.large_string())
        )
        keyed = table.set_column(
            table.schema.get_field_index("cell"),
            "cell",
            pa.chunked_array(
                [chunk.indices for chunk in canonical.chunks], type=pa.int32()
            ),
        )
    if "n" in keyed.column_names:
        weighted = keyed.select(["species_key", "cell", "week", "n"])
    else:
        weighted = keyed.select(["species_key", "cell", "week"]).append_column(
            "n", pa.array([1] * keyed.num_rows, pa.int64())
        )
    grouped = weighted.group_by(["species_key", "cell", "week"]).aggregate(
        [("n", "sum")]
    )
    counts = grouped.rename_columns({"n_sum": "n"}).select(
        ["species_key", "cell", "week", "n"]
    )
    if vocabulary is not None:
        counts = counts.set_column(
            1,
            pa.field("cell", pa.string()),
            decode_strings(
                pa.chunked_array(
                    [
                        pa.DictionaryArray.from_arrays(chunk, vocabulary)
                        for chunk in counts.column("cell").chunks
                    ],
                    type=TEXT_TYPE,
                )
            ),
        )
    if guards_applied(table):
        counts = mark_guards_applied(counts)
    return counts
