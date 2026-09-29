"""Neighbour smoothing over H3 rings and adjacent weeks."""
import sys
from pathlib import Path

import h3
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.smoothing import SmoothingParams, smooth


def _est(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "p": pa.array([r[3] for r in rows], pa.float64()),
        }
    )


def _lookup(t):
    return {(r["species_key"], r["cell"], r["week"]): r["p"] for r in t.to_pylist()}


CPH = h3.latlng_to_cell(55.6867, 12.5700, 4)
NEIGHBOUR = [c for c in h3.grid_disk(CPH, 1) if c != CPH][0]


def test_neighbouring_cell_pulls_value_up():
    t = _est([(1, CPH, 20, 1.0), (1, NEIGHBOUR, 20, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, NEIGHBOUR, 20)] > 0.0


def test_adjacent_week_pulls_value_up():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 21)] > 0.0


def test_week_52_and_week_1_are_adjacent():
    """The year is circular: late December informs early January."""
    t = _est([(1, CPH, 52, 1.0), (1, CPH, 1, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 1)] > 0.0


def test_distant_week_is_not_pulled():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 40, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 40)] == pytest.approx(0.0, abs=1e-9)


def test_antimeridian_cell_has_six_neighbours():
    """Review Focus 4: H3 handles the seam; a lat/lon grid would not."""
    seam = h3.latlng_to_cell(0.0, 179.9, 4)
    assert len([c for c in h3.grid_disk(seam, 1) if c != seam]) == 6


def test_antimeridian_cell_smooths_without_error():
    seam = h3.latlng_to_cell(0.0, 179.9, 4)
    other = [c for c in h3.grid_disk(seam, 1) if c != seam][0]
    out = _lookup(smooth(_est([(1, seam, 20, 1.0), (1, other, 20, 0.0)])))
    assert out[(1, other, 20)] > 0.0


def test_zero_weights_are_a_noop():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.0, time_weight=0.0)))
    assert out[(1, CPH, 20)] == pytest.approx(1.0)
    assert out[(1, CPH, 21)] == pytest.approx(0.0)


def test_schema_is_preserved():
    t = _est([(1, CPH, 20, 0.5)])
    assert smooth(t).schema.names == ["species_key", "cell", "week", "p"]
