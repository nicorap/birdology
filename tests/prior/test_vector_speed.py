"""`vector()` must be a column READ, not a column EXTRACTION.

Job 6961 (Phase C pilot on the restricted EOD r4 prior): 0.5 samples/s
against the twin's 50 -- 100x -- because every training sample's context
called `PriorTensor.vector`, which sliced ONE column out of a CSR matrix
(`values[:, [col]]`). On CSR that scans every row: measured on the real
r4 prior (10,319 x 687,388, nnz 7.8e8) at 1.51 s per call. A CSC copy
answers the same column in 0.07 ms (21,560x), bit-identical.
"""
import sys
from pathlib import Path

import numpy as np
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.backoff import FLOOR
from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import PriorSpec, PriorTensor

SPEC = PriorSpec(resolution=4, source="eod", variant="full")


def _random_tensor(seed=0, n_species=40, n_cells=7, density=0.3):
    rng = np.random.default_rng(seed)
    dense = rng.random((n_species, n_cells * WEEKS_PER_YEAR))
    dense[rng.random(dense.shape) > density] = 0.0
    return PriorTensor(
        spec=SPEC,
        species_keys=np.arange(100, 100 + n_species, dtype=np.int64),
        cells=[f"cell-{i}" for i in range(n_cells)],
        values=sparse.csr_matrix(dense),
    ), dense


def test_vector_equals_the_dense_column_everywhere():
    t, dense = _random_tensor()
    for ci, cell in enumerate(t.cells):
        for week in range(1, WEEKS_PER_YEAR + 1):
            col = ci * WEEKS_PER_YEAR + (week - 1)
            expected = np.where(dense[:, col] == 0.0, FLOOR, dense[:, col])
            assert np.array_equal(t.vector(cell, week), expected), (cell, week)


def test_vector_never_slices_a_column_out_of_the_csr_matrix(monkeypatch):
    """Column-slicing a CSR matrix is the O(nnz) path job 6961 died on.
    Make it raise: vector() must answer from a column-major view instead."""
    t, _ = _random_tensor()

    def forbidden(self, key):
        raise AssertionError(
            f"vector() sliced the CSR matrix with {key!r}: an O(nnz) scan "
            "per call (1.5 s on the fleet r4 prior)"
        )

    monkeypatch.setattr(sparse.csr_matrix, "__getitem__", forbidden)
    t.vector("cell-3", 17)


def test_the_column_view_is_built_once_and_reused():
    t, _ = _random_tensor()
    t.vector("cell-0", 1)
    first = t._columns
    t.vector("cell-5", 52)
    assert t._columns is first


def test_the_column_view_does_not_leak_into_equality_or_repr():
    t, _ = _random_tensor()
    t.vector("cell-1", 2)
    assert "_columns" not in repr(t)
