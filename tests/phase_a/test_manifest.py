"""The manifest is Phase B's only contract with the corpus."""
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.manifest import (
    MANIFEST_COLUMNS,
    DropReason,
    build_manifest,
)


def _focal(rows):
    """(filepath, lat, long, ebird_code, id)"""
    return pa.table(
        {
            "filepath": pa.array([r[0] for r in rows], pa.string()),
            "lat": pa.array([r[1] for r in rows], pa.float64()),
            "long": pa.array([r[2] for r in rows], pa.float64()),
            "ebird_code": pa.array([r[3] for r in rows], pa.string()),
            "id": pa.array([r[4] for r in rows], pa.int64()),
        }
    )


def test_focal_row_gets_cell_and_week():
    t = _focal([("data/xc/parus_major/XC1.ogg", 55.6867, 12.57, "gretit", 1)])
    out, drops = build_manifest(
        t, dates={1: date(2021, 10, 15)}, resolution=4, split="train"
    )
    assert out.num_rows == 1
    row = out.to_pylist()[0]
    assert row["week"] == 41           # 2021-10-15 is ISO week 41
    assert isinstance(row["cell"], str)
    assert row["split"] == "train"
    assert not drops


def test_schema_is_exact():
    t = _focal([("a.ogg", 55.0, 12.0, "gretit", 1)])
    out, _ = build_manifest(t, dates={1: date(2021, 1, 4)}, resolution=4, split="train")
    assert tuple(out.schema.names) == MANIFEST_COLUMNS


def test_a_recording_with_no_date_is_DROPPED_and_COUNTED():
    """Review Focus 2: never default the week."""
    t = _focal([("a.ogg", 55.0, 12.0, "gretit", 1), ("b.ogg", 55.0, 12.0, "gretit", 2)])
    out, drops = build_manifest(
        t, dates={1: date(2021, 1, 4)}, resolution=4, split="train"
    )
    assert out.num_rows == 1
    assert drops[DropReason.NO_DATE] == 1


def test_a_recording_with_no_coordinates_is_DROPPED_and_COUNTED():
    """Review Focus 3: 1.7% of XCM has no lat/long."""
    t = pa.table(
        {
            "filepath": pa.array(["a.ogg", "b.ogg"], pa.string()),
            "lat": pa.array([55.0, None], pa.float64()),
            "long": pa.array([12.0, None], pa.float64()),
            "ebird_code": pa.array(["gretit", "gretit"], pa.string()),
            "id": pa.array([1, 2], pa.int64()),
        }
    )
    out, drops = build_manifest(
        t, dates={1: date(2021, 1, 4), 2: date(2021, 1, 4)},
        resolution=4, split="train",
    )
    assert out.num_rows == 1
    assert drops[DropReason.NO_COORDINATES] == 1


def test_drop_counts_and_coverage_are_IN_THE_TABLE_METADATA():
    """A consumer must see the coverage they actually got, not assume 100%."""
    t = _focal([("a.ogg", 55.0, 12.0, "gretit", 1), ("b.ogg", 55.0, 12.0, "gretit", 2)])
    out, _ = build_manifest(
        t, dates={1: date(2021, 1, 4)}, resolution=4, split="train"
    )
    meta = {k.decode(): v.decode() for k, v in (out.schema.metadata or {}).items()}
    assert meta["input_rows"] == "2"
    assert meta["kept_rows"] == "1"
    assert meta["coverage"].startswith("0.5")
    assert meta[f"dropped_{DropReason.NO_DATE.value}"] == "1"


def test_soundscape_rows_are_dated_from_the_filename():
    t = pa.table(
        {
            "filepath": pa.array(["HSN_001_20150708_061805.ogg"], pa.string()),
            "lat": pa.array([37.0], pa.float64()),
            "long": pa.array([-118.5], pa.float64()),
            "ebird_code": pa.array(["whcspa"], pa.string()),
        }
    )
    out, drops = build_manifest(t, dates=None, resolution=4, split="test_5s")
    assert out.num_rows == 1
    assert out.to_pylist()[0]["week"] == 28    # 2015-07-08 is ISO week 28
    assert not drops


def test_an_undatable_soundscape_filename_is_DROPPED_and_COUNTED():
    t = pa.table(
        {
            "filepath": pa.array(["no_date_here.ogg"], pa.string()),
            "lat": pa.array([37.0], pa.float64()),
            "long": pa.array([-118.5], pa.float64()),
            "ebird_code": pa.array(["whcspa"], pa.string()),
        }
    )
    out, drops = build_manifest(t, dates=None, resolution=4, split="test_5s")
    assert out.num_rows == 0
    assert drops[DropReason.UNDATABLE_FILENAME] == 1


def test_week_uses_THE_PRIORS_convention_including_the_53_fold():
    """A second derivation would disagree in 17.75% of years."""
    from birdology.prior.indexing import WEEKS_PER_YEAR

    t = _focal([("a.ogg", 55.0, 12.0, "gretit", 1)])
    out, _ = build_manifest(
        t, dates={1: date(2020, 12, 31)}, resolution=4, split="train"
    )
    # 2020-12-31 is ISO week 53, which folds into 52.
    assert out.to_pylist()[0]["week"] == WEEKS_PER_YEAR


def test_every_week_is_in_range():
    rows, dates = [], {}
    for i, d in enumerate(
        [date(2021, 1, 1), date(2020, 12, 31), date(2021, 6, 15), date(2016, 1, 1)]
    ):
        rows.append((f"{i}.ogg", 55.0, 12.0, "gretit", i))
        dates[i] = d
    out, _ = build_manifest(_focal(rows), dates=dates, resolution=4, split="train")
    for row in out.to_pylist():
        assert 1 <= row["week"] <= 52


def test_finer_resolution_splits_cells():
    rows = [("a.ogg", 55.0, 12.0, "gretit", 1), ("b.ogg", 55.9, 12.9, "gretit", 2)]
    dates = {1: date(2021, 1, 4), 2: date(2021, 1, 4)}
    coarse, _ = build_manifest(_focal(rows), dates=dates, resolution=3, split="train")
    fine, _ = build_manifest(_focal(rows), dates=dates, resolution=7, split="train")
    assert len(set(fine.column("cell").to_pylist())) >= len(
        set(coarse.column("cell").to_pylist())
    )


def test_cell_is_the_cell_of_lat_long_IN_THAT_ORDER():
    """Transposed coordinates produce a VALID cell for the wrong spot on Earth.

    Copenhagen (55.6867 N, 12.57 E) transposed is (12.57 N, 55.69 E) — the
    Arabian Sea. Both are valid h3 inputs, so only comparing against the cell
    of the correctly-ordered point catches a swap.
    """
    import h3

    t = _focal([("a.ogg", 55.6867, 12.57, "gretit", 1)])
    out, _ = build_manifest(t, dates={1: date(2021, 1, 4)}, resolution=4, split="train")
    assert out.to_pylist()[0]["cell"] == h3.latlng_to_cell(55.6867, 12.57, 4)


def test_distinct_weeks_travels_in_the_metadata():
    """A station id that parses as one plausible date collapses every row onto
    one week; the manifest carries the spread so the driver CAN refuse."""
    rows, dates = [], {}
    spread = [
        date(2021, 1, 4),    # week 1
        date(2021, 1, 5),    # week 1 (same week — must not double-count)
        date(2021, 6, 15),   # week 24
        date(2021, 10, 15),  # week 41
    ]
    for i, d in enumerate(spread):
        rows.append((f"{i}.ogg", 55.0, 12.0, "gretit", i))
        dates[i] = d
    # A fifth row with no date is DROPPED: dropped rows must not count.
    rows.append(("nodate.ogg", 55.0, 12.0, "gretit", 99))
    out, _ = build_manifest(_focal(rows), dates=dates, resolution=4, split="train")
    meta = {k.decode(): v.decode() for k, v in (out.schema.metadata or {}).items()}
    assert meta["distinct_weeks"] == "3"


def test_the_split_column_carries_the_GIVEN_split_not_a_default():
    """Kills review mutant M7 (split hardcoded to 'train').

    Phase B SELECTS rows by split; a manifest whose every row says 'train'
    would silently feed eval recordings into training-side lookups.
    """
    t = pa.table(
        {
            "filepath": pa.array(["HSN_001_20150708_061805.ogg"], pa.string()),
            "lat": pa.array([37.0], pa.float64()),
            "long": pa.array([-118.5], pa.float64()),
            "ebird_code": pa.array(["whcspa"], pa.string()),
        }
    )
    out, _ = build_manifest(t, dates=None, resolution=4, split="test_5s")
    assert out.to_pylist()[0]["split"] == "test_5s"


def test_resolution_is_HONOURED_exact_cell_at_a_nondefault_resolution():
    """Kills review mutant M8 (resolution hardcoded to 4).

    test_finer_resolution_splits_cells uses >=, which equal cell-sets satisfy,
    and the exact-cell test runs at the default resolution -- so a mutant
    ignoring the parameter passed every test. Assert the exact r6 cell.
    """
    import h3

    t = _focal([("a.ogg", 55.6867, 12.57, "gretit", 1)])
    out, _ = build_manifest(t, dates={1: date(2021, 1, 4)}, resolution=6, split="train")
    assert out.to_pylist()[0]["cell"] == h3.latlng_to_cell(55.6867, 12.57, 6)
    assert out.to_pylist()[0]["cell"] != h3.latlng_to_cell(55.6867, 12.57, 4)


def test_a_SINGLE_null_coordinate_is_counted_as_NO_COORDINATES_not_BAD_CELL():
    """Pins the drop ATTRIBUTION for a half-null row (review mutant M6).

    lat present, lon null must count as NO_COORDINATES. Under the or->and
    mutant the row still dropped -- but via float(None) TypeError into
    BAD_CELL, so the coverage report would blame the wrong cause.
    """
    t = pa.table(
        {
            "filepath": pa.array(["a.ogg"], pa.string()),
            "lat": pa.array([55.0], pa.float64()),
            "long": pa.array([None], pa.float64()),
            "ebird_code": pa.array(["gretit"], pa.string()),
            "id": pa.array([1], pa.int64()),
        }
    )
    out, drops = build_manifest(
        t, dates={1: date(2021, 1, 4)}, resolution=4, split="train"
    )
    assert out.num_rows == 0
    assert drops[DropReason.NO_COORDINATES] == 1
    assert DropReason.BAD_CELL not in drops
