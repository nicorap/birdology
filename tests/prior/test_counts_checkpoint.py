"""The GUARDED per-resolution counts are checkpointed; a re-run resumes there.

Every fleet iteration of the EOD build re-paid ~4.5h of parse and ~2h of
guards per resolution, because the sequential path writes no checkpoint and
the parse checkpoint (`checkpoint.py`) sits BEFORE the guards. The input to
`target_group_ratios` -- the guarded, aggregated (species, cell, week, n)
table -- is small by comparison and is now written to --out once the guards
have passed, with the provenance that decides whether it may be reused.
`--resume-counts` skips the parse AND the guards.
"""
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.checkpoint import StaleCheckpointError
from birdology.prior.counts_checkpoint import (
    COUNTS_CHECKPOINT_SCHEMA_VERSION,
    CountsIdentity,
    counts_checkpoint_path,
    load_counts_checkpoint,
    write_counts_checkpoint,
)
from birdology.prior.leakage import guards_applied, mark_guards_applied
from birdology.prior.tensor import load_prior

import build_prior
from tests.prior.test_restricted_build import (
    DOI,
    ON_GRID_QUERY,
    archive,
    query_set_file,
)


def _counts():
    return mark_guards_applied(
        pa.table(
            {
                "species_key": pa.array([1, 2], pa.int64()),
                "cell": pa.array(["841f059ffffffff"] * 2, pa.string()),
                "week": pa.array([3, 3], pa.int8()),
                "n": pa.array([4, 5], pa.int64()),
            }
        )
    )


def _identity(tmp_path, **override):
    fields = dict(
        source="eod",
        archive=str((tmp_path / "occ.zip").resolve()),
        resolution=4,
        split_sha256=None,
        contributor_mode="match",
    )
    fields.update(override)
    return CountsIdentity(**fields)


GUARD = {
    "banned_key_count": 0,
    "matched_key_count": 0,
    "xeno_canto_rows_removed": 1,
    "contributor_rows_removed": 0,
    "rows_before_contributor_guard": 9,
    "rows_after_guards": 9,
}


def test_the_counts_round_trip_with_their_guard_marker_and_provenance(tmp_path):
    path = counts_checkpoint_path(tmp_path, "eod", 4)
    assert path.name == "guarded_counts_eod_r4.parquet"
    write_counts_checkpoint(path, _counts(), identity=_identity(tmp_path), guard=GUARD)
    meta, counts = load_counts_checkpoint(path, identity=_identity(tmp_path))
    assert counts.equals(_counts())
    assert guards_applied(counts)
    assert meta["schema_version"] == COUNTS_CHECKPOINT_SCHEMA_VERSION
    assert meta["guard"] == GUARD
    assert meta["identity"]["resolution"] == 4


def test_unguarded_counts_are_REFUSED_at_write(tmp_path):
    unguarded = pa.table({"species_key": pa.array([1], pa.int64())})
    with pytest.raises(ValueError, match="guard"):
        write_counts_checkpoint(
            tmp_path / "c.parquet", unguarded, identity=_identity(tmp_path), guard=GUARD
        )


@pytest.mark.parametrize(
    "field, value",
    [
        ("source", "dof"),
        ("archive", "/elsewhere/occ.zip"),
        ("resolution", 5),
        ("split_sha256", "ff" * 32),
        ("contributor_mode", "placeholder"),
    ],
)
def test_a_stale_checkpoint_is_refused_NAMING_the_field(tmp_path, field, value):
    path = counts_checkpoint_path(tmp_path, "eod", 4)
    write_counts_checkpoint(path, _counts(), identity=_identity(tmp_path), guard=GUARD)
    with pytest.raises(StaleCheckpointError, match=field):
        load_counts_checkpoint(path, identity=_identity(tmp_path, **{field: value}))


def test_an_unknown_schema_version_is_refused(tmp_path):
    path = counts_checkpoint_path(tmp_path, "eod", 4)
    write_counts_checkpoint(path, _counts(), identity=_identity(tmp_path), guard=GUARD)
    table = pq.read_table(path)
    meta = json.loads(table.schema.metadata[b"birdology_counts_checkpoint"])
    meta["schema_version"] = 99
    md = dict(table.schema.metadata)
    md[b"birdology_counts_checkpoint"] = json.dumps(meta).encode()
    pq.write_table(table.replace_schema_metadata(md), path)
    with pytest.raises(StaleCheckpointError, match="schema_version"):
        load_counts_checkpoint(path, identity=_identity(tmp_path))


def test_a_missing_checkpoint_is_a_clear_error(tmp_path):
    with pytest.raises(ValueError, match="no guarded-counts checkpoint"):
        load_counts_checkpoint(tmp_path / "nope.parquet", identity=_identity(tmp_path))


# -- end to end through the CLI --------------------------------------------


def _argv(zip_path, out, *extra):
    return [
        "--archive", str(zip_path), "--source", "dof", "--resolutions", "4",
        "--out", str(out), "--gbif-doi", DOI, *extra,
    ]


@pytest.mark.parametrize("parse_path", [[], ["--sequential"]])
def test_every_build_writes_the_guarded_counts_checkpoint(tmp_path, parse_path):
    zip_path = archive(tmp_path)
    out = tmp_path / "out"
    build_prior.main(_argv(zip_path, out, "--no-test-split", *parse_path))
    path = counts_checkpoint_path(out, "dof", 4)
    meta, counts = load_counts_checkpoint(
        path,
        identity=CountsIdentity(
            source="dof",
            archive=str(zip_path.resolve()),
            resolution=4,
            split_sha256=None,
            contributor_mode="match",
        ),
    )
    assert counts.num_rows > 0
    assert meta["guard"]["banned_key_count"] == 0


def test_resume_counts_SKIPS_the_parse_and_the_guards_and_builds_the_same_prior(
    tmp_path, monkeypatch
):
    zip_path = archive(tmp_path)
    out = tmp_path / "out"
    build_prior.main(_argv(zip_path, out, "--no-test-split", "--sequential"))
    first = load_prior(out / "prior_dof_r4_full.npz")

    def boom(*a, **k):
        raise AssertionError("--resume-counts must not parse or guard")

    for name in ("read_occurrences", "parse_and_aggregate", "apply_leakage_guards",
                 "assign_cells"):
        monkeypatch.setattr(build_prior, name, boom)
    again = tmp_path / "again"
    # The checkpoint lives in --out; resume into the same directory.
    build_prior.main(_argv(zip_path, out, "--no-test-split", "--resume-counts"))
    second = load_prior(out / "prior_dof_r4_full.npz")
    assert np.array_equal(first.values.toarray(), second.values.toarray())
    assert not again.exists()


def test_resume_counts_can_change_the_QUERY_SET_without_reparsing(tmp_path, monkeypatch):
    """The point of checkpointing BEFORE estimation: the restriction is an
    estimation choice, so a new query set costs no parse and no guards."""
    zip_path = archive(tmp_path)
    out = tmp_path / "out"
    build_prior.main(_argv(zip_path, out, "--no-test-split", "--sequential"))
    full = load_prior(out / "prior_dof_r4_full.npz")
    monkeypatch.setattr(build_prior, "read_occurrences", lambda *a, **k: 1 / 0)
    qpath = query_set_file(tmp_path)
    build_prior.main(
        _argv(zip_path, out, "--no-test-split", "--resume-counts", "--query-set", str(qpath))
    )
    restricted = load_prior(out / "prior_dof_r4_full.npz")
    assert restricted.restriction is not None
    for cell, week in ON_GRID_QUERY:
        assert np.array_equal(full.vector(cell, week), restricted.vector(cell, week))


def test_resume_counts_refuses_a_checkpoint_from_another_split(tmp_path):
    zip_path = archive(tmp_path)
    out = tmp_path / "out"
    build_prior.main(_argv(zip_path, out, "--no-test-split", "--sequential"))
    split = tmp_path / "split.csv"
    split.write_text("lat,lon,week,recorded_by\n55.7,12.5,1,someone\n")
    with pytest.raises(StaleCheckpointError, match="split_sha256"):
        build_prior.main(
            _argv(zip_path, out, "--test-split", str(split), "--resume-counts")
        )


def test_the_split_sha256_recorded_is_the_split_FILE_hash(tmp_path):
    from build_prior import split_digest

    split = tmp_path / "split.csv"
    split.write_text("lat,lon,week,recorded_by\n55.7,12.5,1,x\n")
    assert split_digest(split) == hashlib.sha256(split.read_bytes()).hexdigest()
    assert split_digest(None) is None


@pytest.mark.parametrize(
    "extra", [["--resume"], ["--workers", "2"], ["--sequential"]]
)
def test_resume_counts_refuses_flags_it_would_silently_ignore(tmp_path, extra):
    with pytest.raises(SystemExit) as exc:
        build_prior.main(
            _argv(tmp_path / "occ.zip", tmp_path / "o", "--no-test-split",
                  "--resume-counts", *extra)
        )
    assert exc.value.code == 2


def test_resume_counts_RECHECKS_the_match_rate_it_is_resumed_with(tmp_path):
    """A checkpoint written under a permissive --min-split-match-rate must not
    let a stricter re-run skip the check: clause 2 is re-evaluated from the
    recorded guard counts."""
    guard = dict(GUARD, banned_key_count=100, matched_key_count=1,
                 contributor_rows_removed=3)
    build_prior.check_resumed_guards(guard, 4, 0.0, "match")
    with pytest.raises(build_prior.PartialMatchGuardError, match="0.0100"):
        build_prior.check_resumed_guards(guard, 4, 0.05, "match")
