"""Checkpoint the expensive parse so a guard refusal costs minutes, not hours.

Job 5345: read_occurrences spent ~6.5h single-threaded over 1.76B rows, the
guard phase another ~2.5h -- and then the zero-match guard (correctly)
refused, throwing ALL of it away. The refusal is right; losing the parse to
it is not. The parse+aggregate result is therefore persisted to ``--out``
BEFORE the guards run, and ``--resume`` starts the next invocation at the
guards.
"""
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

import h3

from birdology.prior.checkpoint import (
    CHECKPOINT_SCHEMA_VERSION,
    StaleCheckpointError,
    checkpoint_path,
    load_checkpoint,
    write_checkpoint,
)
from birdology.prior.parallel_parse import parse_and_aggregate

import build_prior
from build_prior import ZeroMatchGuardError

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)
DOI = "10.15468/dl.test"
POINTS = [
    (55.6867, 12.5700),
    (55.9000, 12.5000),
    (56.1629, 10.2039),
    (57.0488, 9.9217),
]
DATES = ("2025-01-07", "2025-06-10", "2025-11-14")


def _row(i, sk, lat, lon, d, pub="pub", who="obs"):
    return "\t".join(
        [str(i), "ds", pub, str(sk), f"sp{sk}", str(lat), str(lon), d, who]
    )


def _archive(tmp_path, name="occ.zip"):
    rows = []
    i = 0
    for wi, d in enumerate(DATES):
        for pi, (lat, lon) in enumerate(POINTS):
            for sk in (1001, 1002):
                # Varying counts: a uniform fixture makes every cell block
                # of the prior identical and the shuffle_space control
                # correctly refuses to ship a no-op null.
                for _ in range(1 + ((wi + pi + sk) % 4)):
                    i += 1
                    rows.append(_row(i, sk, lat + i * 1e-7, lon, d))
    # One row per drop reason, so the counters round-trip non-trivially.
    rows.append(_row(900, "", *POINTS[0], DATES[0]).replace("sp", "s"))
    rows.append("\t".join(["901", "ds", "pub", "1001", "sp", "", "",
                           DATES[0], "x"]))
    rows.append("\t".join(["902", "ds", "pub", "1001", "sp", "55.0", "12.0",
                           "2025-11", "x"]))
    zip_path = tmp_path / name
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return zip_path


def _result(tmp_path, resolutions=(3, 4)):
    return parse_and_aggregate(_archive(tmp_path), list(resolutions), workers=1)


# --------------------------------------------------------------------------
# Round trip.
# --------------------------------------------------------------------------


def test_checkpoint_round_trips_tables_and_metadata(tmp_path):
    archive = _archive(tmp_path)
    result = parse_and_aggregate(archive, [3, 4], workers=1)
    path = write_checkpoint(
        checkpoint_path(tmp_path / "out", "eod"),
        result,
        source="eod",
        archive=archive,
        resolutions=[3, 4],
    )
    assert path.exists()
    meta, tables = load_checkpoint(path)
    assert meta["schema_version"] == CHECKPOINT_SCHEMA_VERSION
    assert meta["source"] == "eod"
    assert meta["archive"] == str(archive)
    assert meta["resolutions"] == [3, 4]
    assert meta["dropped"] == result.dropped
    assert set(tables) == {3, 4}
    for resolution in (3, 4):
        assert tables[resolution].sort_by(
            [(c, "ascending") for c in tables[resolution].column_names]
        ).equals(
            result.tables[resolution].sort_by(
                [(c, "ascending")
                 for c in result.tables[resolution].column_names]
            )
        )


def test_checkpoint_records_the_drop_counters(tmp_path):
    """The counters are provenance: a resumed run must report the same parse
    the checkpoint came from, and a checkpoint that lost them could not."""
    archive = _archive(tmp_path)
    result = parse_and_aggregate(archive, [3], workers=1)
    assert sum(result.dropped.values()) == 3  # the fixture plants one each
    path = write_checkpoint(
        checkpoint_path(tmp_path / "out", "eod"), result,
        source="eod", archive=archive, resolutions=[3],
    )
    meta, _ = load_checkpoint(path)
    assert meta["dropped"] == {
        "no_species_key": 1, "bad_coordinates": 1, "unparseable_date": 1,
    }
    assert meta["total_rows_read"] == result.total


# --------------------------------------------------------------------------
# Staleness: refused loudly, naming the mismatched field.
# --------------------------------------------------------------------------


def _written(tmp_path):
    archive = _archive(tmp_path)
    result = parse_and_aggregate(archive, [3, 4], workers=1)
    path = write_checkpoint(
        checkpoint_path(tmp_path / "out", "eod"), result,
        source="eod", archive=archive, resolutions=[3, 4],
    )
    return archive, path


def test_stale_checkpoint_wrong_source_is_refused_naming_the_field(tmp_path):
    archive, path = _written(tmp_path)
    with pytest.raises(StaleCheckpointError, match="source"):
        load_checkpoint(path, source="dof", archive=archive,
                        resolutions=[3, 4])


def test_stale_checkpoint_wrong_archive_is_refused_naming_the_field(tmp_path):
    archive, path = _written(tmp_path)
    other = _archive(tmp_path, name="other.zip")
    with pytest.raises(StaleCheckpointError, match="archive"):
        load_checkpoint(path, source="eod", archive=other,
                        resolutions=[3, 4])


def test_stale_checkpoint_missing_resolution_is_refused(tmp_path):
    archive, path = _written(tmp_path)
    with pytest.raises(StaleCheckpointError, match="resolution"):
        load_checkpoint(path, source="eod", archive=archive,
                        resolutions=[3, 4, 5])


def test_checkpoint_with_extra_resolutions_serves_a_subset(tmp_path):
    """A checkpoint built for 3 4 can serve a 3-only rerun: the data is a
    superset, and refusing it would force a re-parse for nothing."""
    archive, path = _written(tmp_path)
    meta, tables = load_checkpoint(path, source="eod", archive=archive,
                                   resolutions=[3])
    assert 3 in tables


def test_unknown_schema_version_is_refused(tmp_path):
    archive, path = _written(tmp_path)
    import json
    import pyarrow.parquet as pq
    table = pq.read_table(path)
    metadata = dict(table.schema.metadata or {})
    meta = json.loads(metadata[b"birdology_parse_checkpoint"])
    meta["schema_version"] = CHECKPOINT_SCHEMA_VERSION + 1
    metadata[b"birdology_parse_checkpoint"] = json.dumps(meta).encode()
    pq.write_table(table.replace_schema_metadata(metadata), path)
    with pytest.raises(StaleCheckpointError, match="schema_version"):
        load_checkpoint(path, source="eod", archive=archive,
                        resolutions=[3, 4])


# --------------------------------------------------------------------------
# The CLI contract: --resume skips the parse; a guard refusal leaves the
# checkpoint intact so the NEXT run starts at the guards.
# --------------------------------------------------------------------------


def _argv(archive, out, *extra):
    return [
        "--archive", str(archive), "--source", "eod",
        "--resolutions", "4", "--out", str(out), "--gbif-doi", DOI,
        *extra,
    ]


def test_main_writes_the_checkpoint_before_the_guards_run(tmp_path):
    """The fleet failure, synthetically: a split that matches nothing refuses
    the build AFTER the parse -- and the checkpoint survives the refusal."""
    archive = _archive(tmp_path)
    out = tmp_path / "out"
    split = tmp_path / "split.csv"
    split.write_text(
        "lat,lon,week,recorded_by\n-33.8688,151.2093,2,HSN\n",
        encoding="utf-8",
    )
    with pytest.raises(ZeroMatchGuardError):
        build_prior.main(_argv(archive, out, "--test-split", str(split)))
    assert checkpoint_path(out, "eod").exists()
    assert not list(out.glob("*.npz"))


def test_resume_skips_the_parse_entirely(tmp_path, monkeypatch):
    archive = _archive(tmp_path)
    out = tmp_path / "out"
    build_prior.main(_argv(archive, out, "--no-test-split"))
    assert checkpoint_path(out, "eod").exists()

    calls = []

    def counting(*args, **kwargs):
        calls.append(args)
        raise AssertionError("--resume must not re-parse the archive")

    monkeypatch.setattr(build_prior, "parse_and_aggregate", counting)
    build_prior.main(_argv(archive, out, "--no-test-split", "--resume"))
    assert calls == []
    assert (out / "prior_eod_r4_full.npz").exists()


def test_resume_after_a_guard_refusal_builds_from_the_checkpoint(
    tmp_path, monkeypatch
):
    """The whole point: refusal, then a corrected invocation that costs
    minutes. The re-run uses placeholder mode (the actual job-5345 fix) and
    the parse function is poisoned to prove it never runs again."""
    archive = _archive(tmp_path)
    out = tmp_path / "out"
    split = tmp_path / "split.csv"
    lat, lon = POINTS[0]
    split.write_text(
        f"lat,lon,week,recorded_by\n{lat},{lon},2,HSN\n", encoding="utf-8"
    )
    with pytest.raises(ZeroMatchGuardError):
        build_prior.main(_argv(archive, out, "--test-split", str(split)))

    monkeypatch.setattr(
        build_prior, "parse_and_aggregate",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("re-parsed")),
    )
    build_prior.main(
        _argv(archive, out, "--test-split", str(split), "--resume",
              "--split-contributor-mode", "placeholder")
    )
    assert (out / "prior_eod_r4_full.npz").exists()


def test_resume_without_a_checkpoint_is_refused_loudly(tmp_path):
    archive = _archive(tmp_path)
    with pytest.raises(ValueError, match="checkpoint"):
        build_prior.main(
            _argv(archive, tmp_path / "empty", "--no-test-split", "--resume")
        )


def test_resume_with_a_stale_checkpoint_is_refused_not_repaired(tmp_path):
    """A checkpoint from a DIFFERENT archive must refuse, never silently
    rebuild or silently serve the wrong parse."""
    archive = _archive(tmp_path)
    out = tmp_path / "out"
    build_prior.main(_argv(archive, out, "--no-test-split"))
    other = _archive(tmp_path, name="other.zip")
    with pytest.raises(StaleCheckpointError, match="archive"):
        build_prior.main(_argv(other, out, "--no-test-split", "--resume"))


def test_a_checkpointed_build_equals_a_direct_build(tmp_path):
    """Checkpoint + resume is bit-exact against the straight-through path:
    the artifact a resumed run writes is the artifact the original run would
    have written."""
    archive = _archive(tmp_path)
    direct_out = tmp_path / "direct"
    build_prior.main(_argv(archive, direct_out, "--no-test-split"))

    resumed_out = tmp_path / "resumed"
    build_prior.main(
        [
            "--archive", str(archive), "--source", "eod",
            "--resolutions", "4", "--out", str(resumed_out),
            "--gbif-doi", DOI, "--no-test-split",
        ]
    )
    build_prior.main(
        [
            "--archive", str(archive), "--source", "eod",
            "--resolutions", "4", "--out", str(resumed_out),
            "--gbif-doi", DOI, "--no-test-split", "--resume",
        ]
    )
    from birdology.prior.tensor import load_prior
    import numpy as np

    a = load_prior(direct_out / "prior_eod_r4_full.npz")
    b = load_prior(resumed_out / "prior_eod_r4_full.npz")
    assert a.cells == b.cells
    assert (a.species_keys == b.species_keys).all()
    assert np.array_equal(a.values.toarray(), b.values.toarray())
