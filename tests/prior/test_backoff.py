"""Three-level hierarchical backoff for a very sparse grid."""
import sys
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.backoff import (
    FLOOR,
    BackoffParams,
    hierarchical_estimate,
)
from birdology.prior.effort import target_group_ratios


def _counts(rows):
    return target_group_ratios(
        pa.table(
            {
                "species_key": pa.array([r[0] for r in rows], pa.int64()),
                "cell": pa.array([r[1] for r in rows], pa.string()),
                "week": pa.array([r[2] for r in rows], pa.int8()),
                "n": pa.array([r[3] for r in rows], pa.int64()),
            }
        )
    )


def _lookup(table):
    return {(r["species_key"], r["cell"], r["week"]): r["p"] for r in table.to_pylist()}


def test_dense_cell_week_dominated_by_local_estimate():
    """With 1000 local records, the local ratio should win."""
    rows = [(1, "c", 5, 1000)]
    rows += [(2, "other", 5, 1000)]
    out = _lookup(hierarchical_estimate(_counts(rows), [1, 2]))
    assert out[(1, "c", 5)] > 0.9


def test_sparse_cell_week_shrinks_toward_coarser_level():
    """One local record must not produce a probability of 1.0."""
    rows = [(1, "c", 5, 1), (2, "c", 40, 100), (1, "c", 40, 1)]
    out = _lookup(hierarchical_estimate(_counts(rows), [1, 2]))
    assert out[(1, "c", 5)] < 1.0


def test_species_absent_everywhere_gets_floor_not_zero():
    """Review Focus 1: a label-set species with no occurrence data."""
    out = _lookup(hierarchical_estimate(_counts([(1, "c", 5, 10)]), [1, 999]))
    assert out[(999, "c", 5)] >= FLOOR
    assert out[(999, "c", 5)] > 0.0


def test_species_absent_everywhere_does_not_raise():
    hierarchical_estimate(_counts([(1, "c", 5, 10)]), [1, 999, 1000])


def test_empty_cell_week_backs_off_without_dividing_by_zero():
    """Review Focus 2: a (cell, week) nobody visited."""
    ratios = _counts([(1, "a", 5, 10)])
    out = hierarchical_estimate(ratios, [1], cells=["a", "empty"], weeks=[5])
    values = _lookup(out)
    assert values[(1, "empty", 5)] >= FLOOR
    assert all(v == v for v in values.values())  # no NaN


def test_probabilities_are_bounded():
    rows = [(1, "c", 5, 7), (2, "c", 5, 3)]
    for p in _lookup(hierarchical_estimate(_counts(rows), [1, 2])).values():
        assert 0.0 <= p <= 1.0


def test_stronger_shrinkage_pulls_toward_global():
    rows = [(1, "c", 5, 1), (2, "c", 40, 100)]
    weak = _lookup(
        hierarchical_estimate(_counts(rows), [1, 2], params=BackoffParams(cell_week_weight=0.1))
    )
    strong = _lookup(
        hierarchical_estimate(_counts(rows), [1, 2], params=BackoffParams(cell_week_weight=1000.0))
    )
    assert strong[(1, "c", 5)] < weak[(1, "c", 5)]
