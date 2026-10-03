"""``--sequential``: build from the archive without the parallel parse.

Jobs 5846 and 6237: the parallel parse (``parse_and_aggregate``) did not
finish on the real EOD archive. 5846 read all 218 GiB, then spent 6h+ with
ONE parent thread at 100% and 91 idle workers, and hit its 12h limit with no
checkpoint; a 100M-row slice (6% of the archive) with 8 workers was still
inside the parse after 5h40. The sequential ``read_occurrences`` path is the
one that has actually completed 1.76B rows (job 5345, ~6.5h, before a guard
refusal that placeholder mode now resolves). ``--workers 1`` does NOT reach
it -- ``main`` always called ``parse_and_aggregate`` -- so it needed a flag.
"""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

import build_prior
from birdology.prior.checkpoint import checkpoint_path
from birdology.prior.tensor import load_prior
from tests.prior.test_checkpoint import DOI, _archive, _argv


def _forbid_parallel_parse(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("--sequential must not call parse_and_aggregate")

    monkeypatch.setattr(build_prior, "parse_and_aggregate", boom)


def test_sequential_never_calls_the_parallel_parse(tmp_path, monkeypatch):
    archive = _archive(tmp_path)
    out = tmp_path / "out"
    _forbid_parallel_parse(monkeypatch)
    build_prior.main(_argv(archive, out, "--no-test-split", "--sequential"))
    assert (out / "prior_eod_r4_full.npz").exists()


def test_sequential_build_equals_the_parallel_build(tmp_path):
    """Same archive, same artifact: the flag changes HOW the archive is
    parsed, never what the prior says."""
    archive = _archive(tmp_path)
    par, seq = tmp_path / "par", tmp_path / "seq"
    build_prior.main(_argv(archive, par, "--no-test-split"))
    build_prior.main(_argv(archive, seq, "--no-test-split", "--sequential"))
    a = load_prior(par / "prior_eod_r4_full.npz")
    b = load_prior(seq / "prior_eod_r4_full.npz")
    assert a.cells == b.cells
    assert (a.species_keys == b.species_keys).all()
    assert np.array_equal(a.values.toarray(), b.values.toarray())


def test_sequential_writes_no_parse_checkpoint(tmp_path):
    """The checkpoint holds parse_and_aggregate's output; the sequential
    path produces none, and must not leave a file --resume would trust."""
    archive = _archive(tmp_path)
    out = tmp_path / "out"
    build_prior.main(_argv(archive, out, "--no-test-split", "--sequential"))
    assert not checkpoint_path(out, "eod").exists()


@pytest.mark.parametrize("extra", [["--resume"], ["--workers", "4"]])
def test_sequential_refuses_parallel_only_flags(tmp_path, extra):
    archive = _archive(tmp_path)
    with pytest.raises(SystemExit) as exc:
        build_prior.main(
            _argv(archive, tmp_path / "out", "--no-test-split", "--sequential", *extra)
        )
    assert exc.value.code == 2
