"""Task 1: the context provider — prior vectors on the MODEL's class axis.

The provider's one job is the axis bridge: the prior speaks GBIF
species_key (int) in `species_keys` order, the model speaks eBird code
(str) in `id2label` order. Every test here pins that the join is BY CODE
(never positional), that "absent context" and "all-FLOOR context" are
DIFFERENT answers (Review Focus 2), and that ISO week 53 folds to 52
BEFORE the tensor lookup (the phase_b ruling).

No network, no WEKA, no downloads: the prior is a tiny real `PriorTensor`
built in memory from arrays.
"""
from __future__ import annotations

import numpy as np
import pytest
import torch
from scipy import sparse

from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import FLOOR, PriorSpec, PriorTensor

from birdology.phase_c.context import (
    ContextEncoder,
    ContextProvider,
    build_context_matrix,
)

# ---------------------------------------------------------------------------
# Fixture: a tiny REAL PriorTensor, two cells, three species.
#
#   species 111 (codeA): stored p=0.5 in cellA week 5
#   species 222 (codeB): stored p=0.25 in cellA week 5
#   species 333 (codeC): nothing stored anywhere (reads FLOOR)
#   cellB: NOTHING stored at all -> every vector from it is all-FLOOR,
#          which is Review Focus 2's "present but no eBird data" cell.
# ---------------------------------------------------------------------------

SPECIES_KEYS = np.array([111, 222, 333], dtype=np.int64)
CELLS = ["cellA", "cellB"]
BRIDGE = {"codeA": 111, "codeB": 222, "codeC": 333}


def _tiny_prior() -> PriorTensor:
    n_cols = len(CELLS) * WEEKS_PER_YEAR
    col_a_week5 = 0 * WEEKS_PER_YEAR + (5 - 1)
    col_a_week52 = 0 * WEEKS_PER_YEAR + (52 - 1)
    rows = [0, 1, 0]
    cols = [col_a_week5, col_a_week5, col_a_week52]
    vals = [0.5, 0.25, 0.125]
    matrix = sparse.csr_matrix(
        (vals, (rows, cols)), shape=(len(SPECIES_KEYS), n_cols), dtype=np.float64
    )
    return PriorTensor(
        spec=PriorSpec(resolution=4, source="eod", variant="full"),
        species_keys=SPECIES_KEYS,
        cells=CELLS,
        values=matrix,
    )


@pytest.fixture()
def prior() -> PriorTensor:
    return _tiny_prior()


def _expected_normed(probs: np.ndarray) -> np.ndarray:
    """The brief's layer-norm, computed independently of the module."""
    logp = np.log(probs.astype(np.float64))
    return ((logp - logp.mean()) / (logp.std() + 1e-6)).astype(np.float32)


# ---------------------------------------------------------------------------
# The join is by CODE, never positional.
# ---------------------------------------------------------------------------


class TestClassAxisAlignment:
    def test_vector_values_follow_codes_not_positions(self, prior):
        """Shuffling class_codes must shuffle the output IDENTICALLY.

        A positional join would hand codeA's prior to whatever code sits
        at index 0 — plausible numbers, silently wrong species.
        """
        ordered = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        shuffled = build_context_matrix(prior, ["codeC", "codeA", "codeB"], BRIDGE)

        v_ordered, _ = ordered.vector("cellA", 5)
        v_shuffled, _ = shuffled.vector("cellA", 5)

        # codeA's value must travel with codeA wherever it sits.
        assert v_ordered[0] == pytest.approx(v_shuffled[1])
        assert v_ordered[1] == pytest.approx(v_shuffled[2])
        assert v_ordered[2] == pytest.approx(v_shuffled[0])
        # And the two vectors genuinely differ positionally (the shuffle
        # is not a no-op on this fixture).
        assert not np.allclose(v_ordered, v_shuffled)

    def test_vector_matches_hand_computed_layer_normed_log_prior(self, prior):
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        vec, flag = provider.vector("cellA", 5)
        expected = _expected_normed(np.array([0.5, 0.25, FLOOR]))
        np.testing.assert_allclose(vec, expected, rtol=1e-6)
        assert flag == 1.0

    def test_vector_shape_and_dtype(self, prior):
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        vec, flag = provider.vector("cellA", 5)
        assert vec.shape == (3,)
        assert vec.dtype == np.float32
        assert isinstance(flag, float)

    def test_duplicate_class_code_is_refused(self, prior):
        """A duplicate code would silently shadow a class column."""
        with pytest.raises(ValueError, match="duplicate"):
            build_context_matrix(prior, ["codeA", "codeA", "codeC"], BRIDGE)


# ---------------------------------------------------------------------------
# Absent context vs all-FLOOR context (Review Focus 2).
# ---------------------------------------------------------------------------


class TestAbsentVersusFloor:
    def test_all_floor_cell_is_present_with_finite_values(self, prior):
        """cellB has NO stored data: that is a real context, flag 1.0.

        Conflating it with 'missing' would erase the sparse-region
        signal: a cell with no eBird data is exactly where the prior
        says 'rare everywhere', and FiLM may learn from that.
        """
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        vec, flag = provider.vector("cellB", 10)
        assert flag == 1.0
        assert np.all(np.isfinite(vec))

    def test_missing_cell_gives_zeros_and_flag_zero(self, prior):
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        vec, flag = provider.vector("not-a-cell-on-the-axis", 10)
        assert flag == 0.0
        np.testing.assert_array_equal(vec, np.zeros(3, dtype=np.float32))
        assert vec.dtype == np.float32

    def test_none_cell_and_none_week_give_zeros_and_flag_zero(self, prior):
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        for cell, week in [(None, 10), ("cellA", None), (None, None)]:
            vec, flag = provider.vector(cell, week)
            assert flag == 0.0
            np.testing.assert_array_equal(vec, np.zeros(3, dtype=np.float32))

    def test_absent_and_all_floor_are_distinguishable_by_flag(self, prior):
        """The pair (zeros, 0.0) and (all-floor-normed, 1.0) must differ
        in the FLAG even when the vectors coincide (an all-FLOOR log
        vector layer-norms to zeros too)."""
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        _, flag_absent = provider.vector(None, None)
        _, flag_floor = provider.vector("cellB", 10)
        assert flag_absent == 0.0
        assert flag_floor == 1.0


# ---------------------------------------------------------------------------
# Week 53 folds to 52 BEFORE the lookup (phase_b ruling).
# ---------------------------------------------------------------------------


class TestWeekFold:
    def test_week_53_reads_week_52(self, prior):
        """PriorTensor.vector raises for week 53; the provider must fold
        FIRST so a late-December recording cannot sink a batch."""
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        v53, flag53 = provider.vector("cellA", 53)
        v52, flag52 = provider.vector("cellA", 52)
        assert flag53 == 1.0 == flag52
        np.testing.assert_array_equal(v53, v52)

    def test_out_of_range_week_stays_loud(self, prior):
        """54 is not a fold candidate — it is a caller error and the
        tensor's own ValueError must surface, not be swallowed."""
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        with pytest.raises(ValueError):
            provider.vector("cellA", 54)


# ---------------------------------------------------------------------------
# Partial bridge: unknown codes get the floor, loudly counted — no KeyError.
# ---------------------------------------------------------------------------


class TestPartialBridge:
    def test_code_absent_from_bridge_gets_floor_column(self, prior):
        provider = build_context_matrix(
            prior, ["codeA", "codeB", "not-in-bridge"], BRIDGE
        )
        vec, flag = provider.vector("cellA", 5)
        assert flag == 1.0
        expected = _expected_normed(np.array([0.5, 0.25, FLOOR]))
        np.testing.assert_allclose(vec, expected, rtol=1e-6)

    def test_code_mapped_to_off_axis_key_gets_floor_column(self, prior):
        bridge = dict(BRIDGE, codeC=999_999)  # key not on species_keys
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], bridge)
        vec, _ = provider.vector("cellA", 5)
        expected = _expected_normed(np.array([0.5, 0.25, FLOOR]))
        np.testing.assert_allclose(vec, expected, rtol=1e-6)

    def test_unmapped_codes_are_named_not_silently_floored(self, prior):
        provider = build_context_matrix(
            prior, ["codeA", "ghost1", "ghost2"], BRIDGE
        )
        assert provider.missing_codes == ["ghost1", "ghost2"]


# ---------------------------------------------------------------------------
# ContextEncoder: shape, dtype, seed determinism.
# ---------------------------------------------------------------------------


class TestContextEncoder:
    def test_output_shape_and_dtype(self):
        enc = ContextEncoder(n_classes=7, d_ctx=16)
        vec = torch.zeros(4, 7)
        flag = torch.ones(4, 1)
        out = enc(vec, flag)
        assert out.shape == (4, 16)
        assert out.dtype == torch.float32

    def test_default_d_ctx_is_256(self):
        enc = ContextEncoder(n_classes=7)
        out = enc(torch.zeros(1, 7), torch.zeros(1, 1))
        assert out.shape == (1, 256)

    def test_mlp_widths_match_the_brief(self):
        """Linear(n_classes+1, 512) -> GELU -> Linear(512, d_ctx)."""
        enc = ContextEncoder(n_classes=9, d_ctx=32)
        linears = [m for m in enc.modules() if isinstance(m, torch.nn.Linear)]
        assert [(m.in_features, m.out_features) for m in linears] == [
            (10, 512),
            (512, 32),
        ]
        assert any(isinstance(m, torch.nn.GELU) for m in enc.modules())

    def test_deterministic_under_fixed_seed(self):
        vec = torch.randn(3, 5, generator=torch.Generator().manual_seed(7))
        flag = torch.ones(3, 1)

        torch.manual_seed(0)
        out1 = ContextEncoder(n_classes=5, d_ctx=8)(vec, flag)
        torch.manual_seed(0)
        out2 = ContextEncoder(n_classes=5, d_ctx=8)(vec, flag)
        assert torch.equal(out1, out2)

    def test_encoder_consumes_provider_output(self, prior):
        """End-to-end: the provider's (vector, flag) feeds the encoder."""
        provider = build_context_matrix(prior, ["codeA", "codeB", "codeC"], BRIDGE)
        vec, flag = provider.vector("cellA", 5)
        enc = ContextEncoder(n_classes=3, d_ctx=8)
        out = enc(
            torch.from_numpy(vec).unsqueeze(0),
            torch.tensor([[flag]], dtype=torch.float32),
        )
        assert out.shape == (1, 8)
        assert torch.all(torch.isfinite(out))
