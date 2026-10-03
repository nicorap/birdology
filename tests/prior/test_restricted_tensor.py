"""Restricted artifacts: sidecar format version 2 and out-of-domain refusals.

A prior built with `--query-set` holds values only at its declared domain.
Everywhere else the FULL build would hold a real estimate, so the floor there
is a silent wrong number: the tensor must refuse, and say so in its sidecar.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pytest
from scipy import sparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.backoff import FLOOR
from birdology.prior.tensor import (
    FORMAT_VERSION,
    OutOfDomainError,
    PriorSpec,
    PriorTensor,
    Restriction,
    load_prior,
    restricted_tensor,
    save_prior,
)

C1, C2, C3 = "841f059ffffffff", "841f05bffffffff", "841f051ffffffff"
DOI = "10.15468/dl.test"
SPEC = PriorSpec(resolution=4, source="eod", variant="full")


def _restriction(contexts):
    return Restriction(
        query_set_path="/q/query_set.parquet",
        query_set_sha256="ab" * 32,
        contexts=tuple(contexts),
        counts={"query_contexts": len(contexts)},
    )


def _restricted():
    # Domain: (C1,5) (C1,52) (C2,5) on the axis; (C3,7) queried but never
    # observed, so off the cell axis.
    restriction = _restriction([(C1, 5), (C1, 52), (C2, 5), (C3, 7)])
    block = np.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])
    t = restricted_tensor(
        block, [10, 20], [(C1, 5), (C1, 52), (C2, 5)], SPEC, restriction
    )
    t.gbif_doi = DOI
    return t


def _unrestricted():
    values = sparse.csr_matrix(np.full((2, 2 * 52), 0.25))
    return PriorTensor(SPEC, np.array([10, 20]), [C1, C2], values, gbif_doi=DOI)


def test_a_restricted_tensor_answers_its_domain():
    t = _restricted()
    assert t.cells == [C1, C2]
    assert t.lookup(20, C1, 52) == 0.5
    assert np.array_equal(t.vector(C2, 5), [0.3, 0.6])


def test_a_restricted_tensor_REFUSES_an_on_axis_context_outside_its_domain():
    t = _restricted()
    with pytest.raises(OutOfDomainError, match="outside"):
        t.vector(C1, 6)
    with pytest.raises(OutOfDomainError):
        t.lookup(10, C2, 52)
    assert isinstance(OutOfDomainError("x"), ValueError)


def test_require_domain_names_the_count_and_examples():
    t = _restricted()
    with pytest.raises(OutOfDomainError) as exc:
        t.require_domain([C1, C1, C2, C1], [5, 6, 7, 8])
    message = str(exc.value)
    assert message.startswith("3 context(s)")
    assert repr((C1, 6)) in message and repr((C2, 7)) in message
    t.require_domain([C1, C3], [52, 7])  # in-domain, on or off the axis


def test_an_in_domain_but_UNOBSERVED_cell_behaves_like_the_full_build():
    """Off the cell axis in both: the tensor raises its unknown-cell error,
    exactly as the full build's tensor does for a never-observed cell."""
    t = _restricted()
    assert t.in_domain(C3, 7)
    with pytest.raises(ValueError, match="not on this tensor's cell axis"):
        t.vector(C3, 7)


def test_dense_is_refused_and_domain_dense_is_the_block():
    t = _restricted()
    with pytest.raises(OutOfDomainError):
        t.dense()
    assert np.array_equal(t.domain_dense(), [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])
    cols, contexts = t.domain_columns()
    assert contexts == [(C1, 5), (C1, 52), (C2, 5)]
    assert cols.tolist() == [4, 51, 52 + 4]


def test_an_unrestricted_tensor_is_unchanged():
    t = _unrestricted()
    assert t.restriction is None
    assert t.in_domain(C1, 6)
    t.require_domain([C1, "anything"], [6, 9])  # no-op
    assert t.vector(C1, 6).tolist() == [0.25, 0.25]
    assert np.array_equal(t.domain_dense(), t.dense())


def test_the_restricted_sidecar_round_trips(tmp_path):
    npz = save_prior(_restricted(), tmp_path / "r.npz")
    meta = json.loads(npz.with_suffix(".json").read_text())
    assert meta["format_version"] == FORMAT_VERSION == 2
    assert meta["restricted"] is True
    assert meta["restriction"]["domain"] == "query_contexts"
    assert meta["restriction"]["query_set_sha256"] == "ab" * 32
    assert meta["restriction"]["contexts"] == sorted(
        [[C1, 5], [C1, 52], [C2, 5], [C3, 7]]
    )
    back = load_prior(npz)
    assert back.restriction == _restricted().restriction
    with pytest.raises(OutOfDomainError):
        back.vector(C1, 6)


def test_an_unrestricted_sidecar_says_so(tmp_path):
    npz = save_prior(_unrestricted(), tmp_path / "u.npz")
    meta = json.loads(npz.with_suffix(".json").read_text())
    assert meta["format_version"] == 2
    assert meta["restricted"] is False and meta["restriction"] is None
    assert load_prior(npz).restriction is None


def test_a_VERSION_1_sidecar_still_loads_as_unrestricted(tmp_path):
    """Every DOF artifact already on disk was written before restriction
    existed: no `format_version`, no `restricted`. They must still load."""
    npz = save_prior(_unrestricted(), tmp_path / "v1.npz")
    sidecar = npz.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    for key in ("format_version", "restricted", "restriction"):
        del meta[key]
    sidecar.write_text(json.dumps(meta, indent=2))
    t = load_prior(npz)
    assert t.restriction is None
    assert t.vector(C2, 9).tolist() == [0.25, 0.25]


@pytest.mark.parametrize("drop", ["restricted", "restriction"])
def test_a_version_2_sidecar_missing_its_declaration_is_refused(tmp_path, drop):
    npz = save_prior(_restricted(), tmp_path / "r.npz")
    sidecar = npz.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    del meta[drop]
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match=drop):
        load_prior(npz)


def test_an_unknown_format_version_is_refused(tmp_path):
    npz = save_prior(_unrestricted(), tmp_path / "u.npz")
    sidecar = npz.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    meta["format_version"] = 3
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="format_version"):
        load_prior(npz)


def test_a_restricted_flag_disagreeing_with_its_payload_is_refused(tmp_path):
    npz = save_prior(_restricted(), tmp_path / "r.npz")
    sidecar = npz.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    meta["restricted"] = False
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="disagree"):
        load_prior(npz)


def test_a_restricted_matrix_storing_OUTSIDE_its_domain_is_refused(tmp_path):
    npz = save_prior(_restricted(), tmp_path / "r.npz")
    sidecar = npz.with_suffix(".json")
    meta = json.loads(sidecar.read_text())
    meta["restriction"]["contexts"] = [[C1, 5], [C2, 5]]  # drop (C1, 52)
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="outside its declared domain"):
        load_prior(npz)


def test_restricted_tensor_refuses_a_stored_context_outside_the_domain():
    with pytest.raises(ValueError, match="outside the declared domain"):
        restricted_tensor(
            np.ones((1, 1)) * 0.5, [10], [(C1, 6)], SPEC, _restriction([(C1, 5)])
        )


def test_a_restriction_without_a_query_set_hash_is_refused():
    with pytest.raises(ValueError, match="sha256"):
        Restriction("/q", "", ((C1, 5),), {})


def test_an_UNSTORED_domain_context_reads_the_floor_in_domain_dense():
    """A hand-built restricted tensor may leave a domain column unstored;
    like every reader, `domain_dense` answers the floor there."""
    t = _restricted()
    cols, _ = t.domain_columns()
    values = t.values.tolil()
    values[1, int(cols[0])] = 0.0
    values = values.tocsr()
    values.eliminate_zeros()
    t.values = values
    D = t.domain_dense()
    assert D[1, 0] == FLOOR
    assert D[0, 0] == 0.1


def test_the_DOCUMENTED_recipe_refuses_out_of_domain_on_a_restricted_artifact(tmp_path):
    """The format doc ships with the release; its numpy-only `lookup` must
    refuse what this repository's reader refuses, and agree inside the domain."""
    import re

    doc = (Path(__file__).parent.parent.parent / "docs" / "prior-artifact-format.md").read_text()
    assert "## 2f. Restricted artifacts" in doc
    recipe = {}
    block = re.findall(r"```python\n(.*?)```", doc, re.S)[0]
    exec(block, recipe)  # noqa: S102 - the doc IS the test
    npz = save_prior(_restricted(), tmp_path / "r.npz")
    meta, grid = recipe["load_dense"](npz, npz.with_suffix(".json"))
    assert recipe["lookup"](meta, grid, 20, C1, 52) == 0.5
    with pytest.raises(ValueError, match="outside this restricted"):
        recipe["lookup"](meta, grid, 20, C1, 6)
