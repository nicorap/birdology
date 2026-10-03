"""Consumers REFUSE lookups outside a restricted prior's declared domain.

Against a full prior an unknown cell floors (Phase B) or reads as absent
context (Phase C), which is correct: the cell was never observed. Against a
RESTRICTED prior, an observed context outside the query set would floor too
-- while the full build holds a real value there. That silent wrong number
is what these tests forbid. In-domain contexts whose cell was never observed
keep today's behaviour exactly, and unrestricted priors are untouched.
"""
import sys
from pathlib import Path

import numpy as np
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_b.rescoring import (
    control_prior,
    prior_for_rows,
    train_marginal_from_prior,
)
from birdology.phase_c.context import build_context_matrix
from birdology.prior.backoff import FLOOR
from birdology.prior.tensor import (
    OutOfDomainError,
    PriorSpec,
    PriorTensor,
    Restriction,
    restricted_tensor,
)

C1, C2, C3 = "841f059ffffffff", "841f05bffffffff", "841f051ffffffff"
SPEC = PriorSpec(resolution=4, source="eod", variant="full")
BRIDGE = {"a": 10, "b": 20}
# Domain: on-axis (C1,5) (C1,52) (C2,5) (C2,9); (C3,7) queried, never observed.
DOMAIN = [(C1, 5), (C1, 52), (C2, 5), (C2, 9), (C3, 7)]
ON_AXIS = [(C1, 5), (C1, 52), (C2, 5), (C2, 9)]
BLOCK = np.array([[0.1, 0.2, 0.3, 0.05], [0.4, 0.5, 0.6, 0.7]])


def _restricted():
    restriction = Restriction("/q.parquet", "cd" * 32, tuple(DOMAIN), {})
    t = restricted_tensor(BLOCK, [10, 20], ON_AXIS, SPEC, restriction)
    t.gbif_doi = "10.15468/dl.test"
    return t


def _unrestricted():
    dense = np.full((2, 2 * 52), 0.25)
    dense[0, 4] = 0.5
    return PriorTensor(SPEC, np.array([10, 20]), [C1, C2], sparse.csr_matrix(dense))


# -- Phase B: prior_for_rows ---------------------------------------------


def test_prior_for_rows_reads_in_domain_contexts():
    got = prior_for_rows(_restricted(), [C1, C2], [52, 9], ["b", "a"], BRIDGE)
    assert got.probs.tolist() == [[0.5, 0.2], [0.7, 0.05]]
    assert got.n_floored == 0


def test_prior_for_rows_floors_an_in_domain_UNOBSERVED_cell_as_today():
    got = prior_for_rows(_restricted(), [C3], [7], ["a", "b"], BRIDGE)
    assert got.probs.tolist() == [[FLOOR, FLOOR]]
    assert got.n_floored == 2


def test_prior_for_rows_REFUSES_out_of_domain_contexts_naming_count_and_examples():
    with pytest.raises(OutOfDomainError) as exc:
        prior_for_rows(
            _restricted(), [C1, C1, C2, "841f2e5ffffffff"], [5, 6, 52, 1], ["a"], BRIDGE
        )
    message = str(exc.value)
    assert message.startswith("3 context(s)")
    assert repr((C1, 6)) in message and repr(("841f2e5ffffffff", 1)) in message


def test_prior_for_rows_folds_week_53_BEFORE_the_domain_check():
    got = prior_for_rows(_restricted(), [C1], [53], ["a"], BRIDGE)
    assert got.probs.tolist() == [[0.2]]


def test_prior_for_rows_on_an_UNRESTRICTED_prior_is_unchanged():
    got = prior_for_rows(_unrestricted(), [C1, C3], [5, 7], ["a", "b"], BRIDGE)
    assert got.probs.tolist() == [[0.5, 0.25], [FLOOR, FLOOR]]
    assert got.n_floored == 2


# -- Phase B: the training marginal and the sweep's own controls ------------


def test_train_marginal_of_a_restricted_prior_is_the_mean_over_its_DOMAIN():
    got = train_marginal_from_prior(_restricted(), ["a", "b", "zz"], BRIDGE)
    assert np.allclose(got, [BLOCK[0].mean(), BLOCK[1].mean(), FLOOR])


def test_train_marginal_of_an_unrestricted_prior_is_unchanged():
    t = _unrestricted()
    got = train_marginal_from_prior(t, ["a", "b"], BRIDGE)
    assert np.array_equal(got, t.dense().mean(axis=1))


@pytest.mark.parametrize("kind", ["uniform", "marginal", "space_shuffled", "time_shuffled"])
def test_the_sweep_controls_run_on_a_restricted_prior_and_stay_restricted(kind):
    t = _restricted()
    control = control_prior(kind, t, np.random.default_rng(0))
    assert control.restriction == t.restriction
    assert control.cells == t.cells
    D = control.domain_dense()
    assert D.shape == BLOCK.shape
    if kind == "uniform":
        assert np.allclose(D, 0.5)
    elif kind == "marginal":
        assert np.allclose(D, BLOCK.mean(axis=1, keepdims=True))
    elif kind == "space_shuffled":
        # (C1,5) and (C2,5) share week 5: the only queried-week group with
        # two cells. Everything else stays put.
        assert np.array_equal(D[:, [1, 3]], BLOCK[:, [1, 3]])
        assert sorted(D[:, [0, 2]].ravel()) == sorted(BLOCK[:, [0, 2]].ravel())
    else:
        # Weeks move only within a cell's queried weeks.
        for cols in ([0, 1], [2, 3]):
            assert np.array_equal(np.sort(D[:, cols], axis=1), np.sort(BLOCK[:, cols], axis=1))
    with pytest.raises(OutOfDomainError):
        prior_for_rows(control, [C1], [6], ["a"], BRIDGE)


# -- Phase C: ContextProvider ------------------------------------------------


def test_context_provider_reads_an_in_domain_context():
    provider = build_context_matrix(_restricted(), ["a", "b"], BRIDGE)
    vector, present = provider.vector(C2, 9)
    assert present == 1.0
    logp = np.log([0.05, 0.7])
    assert np.allclose(vector, (logp - logp.mean()) / (logp.std() + 1e-6), atol=1e-6)


def test_context_provider_absent_context_is_unchanged_for_null_and_unobserved():
    provider = build_context_matrix(_restricted(), ["a", "b"], BRIDGE)
    for cell, week in [(None, 5), (C1, None), (C3, 7)]:
        vector, present = provider.vector(cell, week)
        assert present == 0.0 and not vector.any()


def test_context_provider_REFUSES_an_out_of_domain_context():
    provider = build_context_matrix(_restricted(), ["a", "b"], BRIDGE)
    with pytest.raises(OutOfDomainError, match="1 context"):
        provider.vector(C1, 6)
    with pytest.raises(OutOfDomainError):
        provider.vector("841f2e5ffffffff", 5)  # off-axis AND off-domain


def test_context_provider_require_domain_checks_a_whole_table_at_once():
    provider = build_context_matrix(_restricted(), ["a", "b"], BRIDGE)
    provider.require_domain([C1, None, C3], [52, 3, 7])
    with pytest.raises(OutOfDomainError, match="^2 context"):
        provider.require_domain([C1, C2, C2], [6, 9, 10])


def test_context_provider_on_an_UNRESTRICTED_prior_is_unchanged():
    provider = build_context_matrix(_unrestricted(), ["a", "b"], BRIDGE)
    vector, present = provider.vector(C3, 7)
    assert present == 0.0 and not vector.any()
    provider.require_domain([C3], [7])
    _, present = provider.vector(C1, 6)
    assert present == 1.0
