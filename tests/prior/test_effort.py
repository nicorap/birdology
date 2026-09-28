"""Target-group background: correct for observer effort, not bird abundance."""
import sys
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.effort import target_group_ratios


def _counts(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "n": pa.array([r[3] for r in rows], pa.int64()),
        }
    )


def test_ratio_is_share_of_cell_week_total():
    t = _counts([(1, "c", 5, 3), (2, "c", 5, 1)])
    out = {r["species_key"]: r for r in target_group_ratios(t).to_pylist()}
    assert out[1]["total"] == 4
    assert out[1]["ratio"] == pytest.approx(0.75)
    assert out[2]["ratio"] == pytest.approx(0.25)


def test_ratios_in_a_cell_week_sum_to_one():
    t = _counts([(1, "c", 5, 3), (2, "c", 5, 1), (3, "c", 5, 4)])
    total = sum(r["ratio"] for r in target_group_ratios(t).to_pylist())
    assert total == pytest.approx(1.0)


def test_effort_cancels_between_busy_and_quiet_cells():
    """A city cell with 100x the records gives the same ratio as a rural one."""
    t = _counts(
        [
            (1, "city", 5, 750), (2, "city", 5, 250),
            (1, "rural", 5, 3), (2, "rural", 5, 1),
        ]
    )
    rows = {(r["species_key"], r["cell"]): r["ratio"] for r in target_group_ratios(t).to_pylist()}
    assert rows[(1, "city")] == pytest.approx(rows[(1, "rural")])


def test_cells_are_independent():
    t = _counts([(1, "a", 5, 1), (2, "b", 5, 1)])
    for row in target_group_ratios(t).to_pylist():
        assert row["ratio"] == pytest.approx(1.0)


def test_weeks_are_independent():
    t = _counts([(1, "c", 5, 1), (2, "c", 40, 1)])
    for row in target_group_ratios(t).to_pylist():
        assert row["ratio"] == pytest.approx(1.0)


def test_empty_table_returns_empty():
    assert target_group_ratios(_counts([])).num_rows == 0
