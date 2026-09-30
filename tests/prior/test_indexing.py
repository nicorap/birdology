"""H3 cell + ISO week assignment."""
import sys
from datetime import date
from pathlib import Path

import h3
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.indexing import (
    WEEKS_PER_YEAR,
    aggregate_counts,
    assign_cells,
    iso_week,
)
from birdology.prior.occurrences import OCCURRENCE_COLUMNS


def _table(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "scientific_name": pa.array([r[1] for r in rows], pa.string()),
            "lat": pa.array([r[2] for r in rows], pa.float64()),
            "lon": pa.array([r[3] for r in rows], pa.float64()),
            "event_date": pa.array([r[4] for r in rows], pa.date32()),
            "publisher_key": pa.array([r[5] for r in rows], pa.string()),
            "recorded_by": pa.array([r[6] for r in rows], pa.string()),
        }
    )


def test_iso_week_ordinary_date():
    assert iso_week(date(2025, 11, 14)) == 46


def test_iso_week_53_folds_into_52():
    # 2020 has an ISO week 53.
    assert date(2020, 12, 31).isocalendar().week == 53
    assert iso_week(date(2020, 12, 31)) == WEEKS_PER_YEAR


def test_iso_week_always_in_range():
    for year in (2015, 2016, 2020, 2021, 2026):
        for month, day in ((1, 1), (6, 15), (12, 28), (12, 31)):
            assert 1 <= iso_week(date(year, month, day)) <= WEEKS_PER_YEAR


def test_assign_cells_adds_cell_and_week():
    t = _table([(1, "Turdus merula", 55.6867, 12.5700, date(2025, 11, 14), "p", "o")])
    out = assign_cells(t, resolution=4)
    assert out.column("week")[0].as_py() == 46
    assert isinstance(out.column("cell")[0].as_py(), str)


def test_same_location_same_cell():
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2025, 1, 5), "p", "o"),
            (1, "A", 55.6868, 12.5701, date(2025, 1, 5), "p", "o"),
        ]
    )
    out = assign_cells(t, resolution=4)
    assert out.column("cell")[0].as_py() == out.column("cell")[1].as_py()


def test_finer_resolution_splits_cells():
    t = _table(
        [
            (1, "A", 55.6000, 12.5000, date(2025, 1, 5), "p", "o"),
            (1, "A", 55.7000, 12.6000, date(2025, 1, 5), "p", "o"),
        ]
    )
    coarse = assign_cells(t, resolution=3)
    fine = assign_cells(t, resolution=7)
    assert fine.column("cell")[0].as_py() != fine.column("cell")[1].as_py()
    assert len(set(coarse.column("cell").to_pylist())) <= len(
        set(fine.column("cell").to_pylist())
    )


def test_aggregate_counts_sums_duplicates():
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2025, 11, 14), "p", "o"),
            (1, "A", 55.6867, 12.5700, date(2025, 11, 14), "p", "o"),
            (2, "B", 55.6867, 12.5700, date(2025, 11, 14), "p", "o"),
        ]
    )
    counts = aggregate_counts(assign_cells(t, resolution=4))
    by_species = {
        row["species_key"]: row["n"] for row in counts.to_pylist()
    }
    assert by_species[1] == 2
    assert by_species[2] == 1


def test_aggregate_counts_schema():
    t = _table([(1, "A", 55.6, 12.5, date(2025, 3, 3), "p", "o")])
    counts = aggregate_counts(assign_cells(t, resolution=4))
    assert counts.schema.names == ["species_key", "cell", "week", "n"]


# --- Additional coverage beyond the brief -----------------------------------
# The brief locks the happy path. These lock the contract Task 4 and the
# smoothing kernel depend on: the exact week-53 convention at both ends of the
# year, resolution really being a parameter, and null provenance surviving.


def test_iso_week_53_folds_into_52_in_2015_too():
    """A second real week-53 year, so the fold is not a 2020 coincidence."""
    assert date(2015, 12, 31).isocalendar().week == 53
    assert iso_week(date(2015, 12, 31)) == WEEKS_PER_YEAR


def test_iso_week_53_in_january_folds_into_52():
    """2021-01-01 is ISO week 53 *of 2020* — the January face of week 53."""
    year, week, _ = date(2021, 1, 1).isocalendar()
    assert (year, week) == (2020, 53)
    assert iso_week(date(2021, 1, 1)) == WEEKS_PER_YEAR


def test_iso_week_covers_every_day_of_a_53_week_year():
    """Every day of a 53-week year lands in 1..52, and week 52 is the only
    week that gains days (it absorbs week 53)."""
    from collections import Counter

    counts = Counter()
    d = date(2020, 1, 1)
    while d <= date(2020, 12, 31):
        w = iso_week(d)
        assert 1 <= w <= WEEKS_PER_YEAR
        counts[w] += 1
        d = date.fromordinal(d.toordinal() + 1)
    # 2020-01-01 is a Wednesday, so ISO week 1 of 2020 starts in Dec 2019 and
    # only 5 of its days fall in the calendar year; every other week is 7 days
    # except 52, which swallows week 53's days.
    assert counts[1] == 5
    assert all(counts[w] == 7 for w in range(2, 52))
    assert counts[WEEKS_PER_YEAR] > 7


def test_week_column_is_int8():
    t = _table([(1, "A", 55.6, 12.5, date(2025, 3, 3), "p", "o")])
    out = assign_cells(t, resolution=4)
    assert out.schema.field("week").type == pa.int8()
    assert out.schema.field("cell").type == pa.string()
    # leakage.keep_mask documents a CONTRACT that week is non-null and integral,
    # and raises rather than coercing. Pin the guarantee side here, so the
    # promise assign_cells makes is an assertion and not just prose.
    assert out.column("week").null_count == 0


def test_cell_is_at_the_requested_resolution():
    """Resolution is a parameter, never hardcoded: resolutions 3, 4 and 5 are
    all swept by the experiment grid."""
    t = _table([(1, "A", 55.6867, 12.5700, date(2025, 3, 3), "p", "o")])
    for resolution in (3, 4, 5):
        cell = assign_cells(t, resolution=resolution).column("cell")[0].as_py()
        assert h3.get_resolution(cell) == resolution


def test_cell_uses_lat_lon_order_not_lon_lat():
    """h3 v4 takes (lat, lon); it does NOT validate the latitude range, so a
    swapped call returns a plausible-looking cell in the wrong hemisphere
    instead of raising. Copenhagen (55.69N, 12.63E) swapped lands in the
    Arabian Sea (12.40N, 55.70E), which would silently poison every prior.
    Asymmetric points only — 0/0 and any lat==lon point survive the swap.
    """
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2025, 3, 3), "p", "o"),  # Copenhagen
            (2, "B", -33.8688, 151.2093, date(2025, 3, 3), "p", "o"),  # Sydney
        ]
    )
    out = assign_cells(t, resolution=4)
    cph, syd = out.column("cell")[0].as_py(), out.column("cell")[1].as_py()

    # Round-trip form: the cell centre must be back where we put the record.
    lat, lon = h3.cell_to_latlng(cph)
    assert lat == pytest.approx(55.69, abs=0.1)
    assert lon == pytest.approx(12.63, abs=0.1)
    lat, lon = h3.cell_to_latlng(syd)
    assert lat == pytest.approx(-33.87, abs=0.2)
    assert lon == pytest.approx(151.21, abs=0.2)

    # Exact form: identical to a direct lat-first call, and different from the
    # swapped call (which h3 accepts without error).
    assert cph == h3.latlng_to_cell(55.6867, 12.5700, 4)
    assert cph != h3.latlng_to_cell(12.5700, 55.6867, 4)
    assert syd == h3.latlng_to_cell(-33.8688, 151.2093, 4)
    assert syd != h3.latlng_to_cell(151.2093, -33.8688, 4)


def test_assign_cells_preserves_occurrence_columns_and_nulls():
    """Task 4's leakage guard reads publisher_key / recorded_by and relies on
    them being NULL (not "") when absent — indexing must not disturb that."""
    t = _table([(1, None, 55.6, 12.5, date(2025, 3, 3), None, None)])
    out = assign_cells(t, resolution=4)
    assert out.schema.names == list(OCCURRENCE_COLUMNS) + ["cell", "week"]
    assert out.column("publisher_key")[0].as_py() is None
    assert out.column("recorded_by")[0].as_py() is None
    assert out.column("scientific_name")[0].as_py() is None
    assert out.column("publisher_key").null_count == 1


def test_assign_cells_keeps_null_island():
    """0.0/0.0 is deliberately kept upstream, so it must get a real cell."""
    t = _table([(1, "A", 0.0, 0.0, date(2025, 3, 3), "p", "o")])
    out = assign_cells(t, resolution=4)
    assert out.column("cell")[0].as_py() == h3.latlng_to_cell(0.0, 0.0, 4)


def test_assign_cells_handles_an_empty_table():
    out = assign_cells(_table([]), resolution=4)
    assert out.num_rows == 0
    assert out.schema.field("week").type == pa.int8()
    assert aggregate_counts(out).num_rows == 0


def test_aggregate_counts_keeps_distinct_weeks_apart():
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2025, 11, 14), "p", "o"),
            (1, "A", 55.6867, 12.5700, date(2025, 3, 3), "p", "o"),
        ]
    )
    counts = aggregate_counts(assign_cells(t, resolution=4)).to_pylist()
    assert sorted(row["week"] for row in counts) == [10, 46]
    assert all(row["n"] == 1 for row in counts)


def test_aggregate_counts_keeps_distinct_cells_apart():
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2025, 11, 14), "p", "o"),
            (1, "A", -33.8688, 151.2093, date(2025, 11, 14), "p", "o"),
        ]
    )
    counts = aggregate_counts(assign_cells(t, resolution=4)).to_pylist()
    assert len(counts) == 2
    assert {row["n"] for row in counts} == {1}


def test_aggregate_counts_merges_week_53_into_week_52():
    """End-to-end week-53 fold: a week-53 record and a week-52 record at the
    same cell collapse into ONE week-52 row with n=2. Without the fold this
    yields two rows (weeks 52 and 53)."""
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2020, 12, 31), "p", "o"),  # ISO week 53
            (1, "A", 55.6867, 12.5700, date(2020, 12, 22), "p", "o"),  # ISO week 52
        ]
    )
    counts = aggregate_counts(assign_cells(t, resolution=4)).to_pylist()
    assert len(counts) == 1
    assert counts[0]["week"] == WEEKS_PER_YEAR
    assert counts[0]["n"] == 2


def test_aggregate_counts_n_is_int64():
    t = _table([(1, "A", 55.6, 12.5, date(2025, 3, 3), "p", "o")])
    counts = aggregate_counts(assign_cells(t, resolution=4))
    assert counts.schema.field("n").type == pa.int64()
    assert counts.schema.field("species_key").type == pa.int64()
    assert counts.schema.field("week").type == pa.int8()


# --- Cross-module contracts this module OWNS (I1) ----------------------------
# `indexing.py` had no mutation harness until finding I1, and the three
# contracts below are the ones other modules depend on without being able to
# check. They are pinned HERE, at the producer, so the harness can name a
# killer inside this file rather than relying on a downstream file happening
# to notice.


def test_cell_column_is_string_not_large_string():
    """`tensor.py` and `leakage.py` key on `cell` by pyarrow type.

    `pa.large_string()` holds the same bytes and compares equal element-wise,
    so every value-based assertion in the repository passes under it while
    `table.schema == other.schema` comparisons and `pc.is_in` value sets
    silently stop matching. Nothing downstream re-derives the type, so this is
    the only place it can be pinned.
    """
    t = _table([(1, "A", 55.6, 12.5, date(2025, 3, 3), "p", "o")])
    out = assign_cells(t, resolution=4)
    assert out.schema.field("cell").type == pa.string()
    assert out.schema.field("cell").type != pa.large_string()
    assert aggregate_counts(out).schema.field("cell").type == pa.string()


def test_aggregate_counts_emits_ONE_row_per_species_cell_week():
    """Row uniqueness is a contract `backoff.py` REFUSES to re-derive.

    `backoff.hierarchical_estimate` rejects a duplicate (species, cell, week)
    row rather than accumulating it (mutants B27/B27b), and its docstring says
    the `aggregate_counts` group_by guarantees uniqueness upstream. Two records
    of the same species, cell and week differing ONLY in provenance must
    therefore collapse to one row with n=2 — provenance is projected away
    BEFORE the group_by, and if it were not, the leaf grouping would be finer
    than the key backoff validates against and every duplicate would arrive at
    backoff as a rejected table.
    """
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2025, 11, 14), "pub-a", "Ole"),
            (1, "A", 55.6867, 12.5700, date(2025, 11, 14), "pub-b", "Jens"),
        ]
    )
    counts = aggregate_counts(assign_cells(t, resolution=4)).to_pylist()
    assert len(counts) == 1
    assert counts[0]["n"] == 2
    keys = [(r["species_key"], r["cell"], r["week"]) for r in counts]
    assert len(set(keys)) == len(keys)


def test_week_and_cell_are_aligned_to_their_OWN_row():
    """The two derived columns are built as list comprehensions over separate
    column reads, so a reversal, an off-by-one or a re-sort in either one
    misaligns every record without changing the SET of cells or weeks — and a
    test that only checks sets, or that uses one row, cannot see it.

    Both pairings are asserted against a fixture where the two rows differ in
    species, in place and in week, so no permutation of either list is the
    identity.
    """
    cph, syd = (55.6867, 12.5700), (-33.8688, 151.2093)
    t = _table(
        [
            (1, "A", cph[0], cph[1], date(2025, 11, 14), "p", "o"),  # week 46
            (2, "B", syd[0], syd[1], date(2025, 3, 3), "p", "o"),  # week 10
        ]
    )
    out = assign_cells(t, resolution=4).to_pylist()
    by_species = {row["species_key"]: row for row in out}
    assert by_species[1]["week"] == 46
    assert by_species[2]["week"] == 10
    assert by_species[1]["cell"] == h3.latlng_to_cell(*cph, 4)
    assert by_species[2]["cell"] == h3.latlng_to_cell(*syd, 4)

