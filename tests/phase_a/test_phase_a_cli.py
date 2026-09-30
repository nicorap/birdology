"""End-to-end manifest build from fixture metadata, and the one-shard CLI.

No network: `fetch_species_dates` is patched at ITS IMPORT SITE
(`phase_a_manifest.fetch_species_dates`) — the driver imports the name into
its own namespace, so patching the source module would miss it.
"""
import io
import logging
import os
import subprocess
import sys
import tarfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import h3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import soundfile as sf

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from phase_a_manifest import (
    build_all,
    collapsed_cells,
    date_focal_species,
    species_tag,
)

from birdology.phase_a.embed import ShardResult
from birdology.phase_a.embed_cli import main as embed_main
from birdology.phase_a.embed_cli import slice_manifest_to_shard
from birdology.phase_a.xc_dates import DateFetchFailed


def _write_corpus(tmp_path, config="XCM"):
    d = tmp_path / config / config
    d.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "filepath": pa.array(
                    ["data/xc/parus_major/XC1.ogg", "data/xc/parus_major/XC2.ogg"],
                    pa.string(),
                ),
                "lat": pa.array([55.6867, 55.7], pa.float64()),
                "long": pa.array([12.57, 12.6], pa.float64()),
                "ebird_code": pa.array(["gretit", "gretit"], pa.string()),
                "genus": pa.array(["parus", "parus"], pa.string()),
                "id": pa.array([1, 2], pa.int64()),
            }
        ),
        d / f"{config}_metadata.parquet",
    )
    return tmp_path


def test_build_all_writes_a_manifest(tmp_path):
    corpus = _write_corpus(tmp_path)
    key = tmp_path / "key"
    key.write_text("k\n")
    with patch(
        "phase_a_manifest.fetch_species_dates",
        return_value={1: date(2021, 10, 15), 2: date(2021, 6, 1)},
    ):
        path = build_all(
            corpus, config="XCM", resolution=4, key_file=key,
            out_dir=tmp_path / "out",
        )
    assert path.exists()
    t = pq.read_table(path)
    assert t.num_rows == 2
    assert set(t.column("week").to_pylist()) == {41, 22}


def test_build_all_records_coverage_in_the_output(tmp_path):
    corpus = _write_corpus(tmp_path)
    key = tmp_path / "key"
    key.write_text("k\n")
    with patch(
        "phase_a_manifest.fetch_species_dates", return_value={1: date(2021, 10, 15)}
    ):
        path = build_all(
            corpus, config="XCM", resolution=4, key_file=key,
            out_dir=tmp_path / "out",
        )
    meta = {k.decode(): v.decode() for k, v in pq.read_table(path).schema.metadata.items()}
    assert meta["kept_rows"] == "1"
    assert meta["dropped_no_date"] == "1"


def test_build_all_REFUSES_a_manifest_with_no_rows(tmp_path):
    """An empty manifest is a failed build, not a valid artifact."""
    corpus = _write_corpus(tmp_path)
    key = tmp_path / "key"
    key.write_text("k\n")
    with patch("phase_a_manifest.fetch_species_dates", return_value={}):
        with pytest.raises(ValueError, match="no usable recordings"):
            build_all(
                corpus, config="XCM", resolution=4, key_file=key,
                out_dir=tmp_path / "out",
            )


def test_build_all_REFUSES_coverage_below_the_floor(tmp_path):
    """A build that lost most of its corpus must not ship silently."""
    corpus = _write_corpus(tmp_path)
    key = tmp_path / "key"
    key.write_text("k\n")
    with patch(
        "phase_a_manifest.fetch_species_dates", return_value={1: date(2021, 10, 15)}
    ):
        with pytest.raises(ValueError, match="coverage"):
            build_all(
                corpus, config="XCM", resolution=4, key_file=key,
                out_dir=tmp_path / "out", min_coverage=0.9,
            )


# ── species-tag derivation ──────────────────────────────────────────────────
# A malformed XC tag returns ZERO recordings SILENTLY, so the failure mode is
# a species dated 0/N. These pin what the derivation DOES; the coverage floor
# is the backstop for all of them.


def test_species_tag_trinomial_directory_yields_the_last_epithet():
    """`parus_major_newtoni` -> `newtoni`: the query tag is still well-formed
    (`gen:parus sp:newtoni`) but matches only XC records tagged with the
    subspecies epithet."""
    assert species_tag("parus", "data/xc/parus_major_newtoni/XC1.ogg") == (
        "parus",
        "newtoni",
    )


def test_species_tag_sp_style_directory_yields_sp():
    """`tinamus_sp` -> `sp`: well-formed, matches XC's undetermined records."""
    assert species_tag("tinamus", "data/xc/tinamus_sp/XC2.ogg") == ("tinamus", "sp")


def test_species_tag_empty_species_part_yields_empty_string():
    """A trailing underscore yields '' — the tag `sp:` is malformed and the
    species will be dated 0/N; the coverage floor catches it corpus-wide."""
    assert species_tag("parus", "data/xc/parus_/XC3.ogg") == ("parus", "")


# ── per-species dating: resilience and resume ───────────────────────────────


def test_ONE_BROKEN_SPECIES_does_not_abort_the_others(tmp_path):
    """DateFetchFailed for one species of three is caught, counted, and the
    other two are still dated."""

    def fetch(genus, sp, *, key):
        if sp == "broken":
            raise DateFetchFailed("XC API error")
        return {{"major": 1, "minor": 2}[sp]: date(2021, 6, 1)}

    with patch("phase_a_manifest.fetch_species_dates", side_effect=fetch):
        dates, failed = date_focal_species(
            [("parus", "major"), ("x", "broken"), ("parus", "minor")],
            key="k",
            progress_path=tmp_path / "progress.json",
        )
    assert failed == 1
    assert dates == {1: date(2021, 6, 1), 2: date(2021, 6, 1)}


def test_resume_does_NOT_refetch_already_done_species(tmp_path):
    """The progress file makes a crashed XCL-scale run resumable: a species
    already persisted is never fetched again."""
    progress = tmp_path / "progress.json"
    first = MagicMock(return_value={1: date(2021, 6, 1)})
    with patch("phase_a_manifest.fetch_species_dates", first):
        date_focal_species([("parus", "major")], key="k", progress_path=progress)
    assert first.call_count == 1

    second = MagicMock(return_value={9: date(2020, 1, 1)})
    with patch("phase_a_manifest.fetch_species_dates", second):
        dates, failed = date_focal_species(
            [("parus", "major")], key="k", progress_path=progress
        )
    second.assert_not_called()
    assert dates == {1: date(2021, 6, 1)}
    assert failed == 0


def test_a_TORN_progress_file_warns_and_restarts_from_scratch(tmp_path, caplog):
    """A half-written JSON (filesystem-level corruption — _save_progress is
    atomic) must never crash the driver or silently skip species: warn naming
    the file, discard it, re-fetch everything. Kills narrowing the except
    clause in _load_progress (Task 4 M8/M9 precedent: correct-but-untested
    defensive branches need the killing test)."""
    progress = tmp_path / "progress.json"
    progress.write_bytes(b'{"parus major": {"1": "2021-')  # torn mid-write
    fetch = MagicMock(return_value={1: date(2021, 6, 1)})
    with caplog.at_level(logging.WARNING, logger="phase_a"):
        with patch("phase_a_manifest.fetch_species_dates", fetch):
            dates, failed = date_focal_species(
                [("parus", "major")], key="k", progress_path=progress
            )
    assert fetch.call_count == 1  # torn progress discarded -> full re-fetch
    assert dates == {1: date(2021, 6, 1)}
    assert failed == 0
    assert any(
        str(progress) in r.getMessage() and "unreadable" in r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
    )


def test_resume_fetches_only_the_missing_species(tmp_path):
    """Partial progress: done species come from the file, the rest from the
    API."""
    progress = tmp_path / "progress.json"
    with patch(
        "phase_a_manifest.fetch_species_dates",
        return_value={1: date(2021, 6, 1)},
    ):
        date_focal_species([("parus", "major")], key="k", progress_path=progress)

    fetch = MagicMock(return_value={2: date(2021, 7, 1)})
    with patch("phase_a_manifest.fetch_species_dates", fetch):
        dates, _ = date_focal_species(
            [("parus", "major"), ("parus", "minor")],
            key="k",
            progress_path=progress,
        )
    assert fetch.call_count == 1
    assert fetch.call_args.args == ("parus", "minor")
    assert dates == {1: date(2021, 6, 1), 2: date(2021, 7, 1)}


# ── per-cell week spread (the one-bad-station signature) ────────────────────


def _write_soundscape_corpus(tmp_path, config="HSN_test"):
    """Two cells: cell A collapsed (50 recordings, ONE week), cell B spread."""
    d = tmp_path / config / config
    d.mkdir(parents=True)
    paths, lats, lons = [], [], []
    for i in range(50):  # cell A: every file the same date -> one ISO week
        paths.append(f"HSN_{i:03d}_20210705_061805.ogg")
        lats.append(55.6867)
        lons.append(12.57)
    spread_dates = ["20210705", "20210712", "20210719"]
    for i in range(60):  # cell B: three ISO weeks
        paths.append(f"PAR_{i:03d}_{spread_dates[i % 3]}_061805.ogg")
        lats.append(48.8)
        lons.append(2.3)
    pq.write_table(
        pa.table(
            {
                "filepath": pa.array(paths, pa.string()),
                "lat": pa.array(lats, pa.float64()),
                "long": pa.array(lons, pa.float64()),
                "ebird_code": pa.array([""] * len(paths), pa.string()),
            }
        ),
        d / f"{config}_metadata.parquet",
    )
    return tmp_path


def test_a_collapsed_cell_in_a_soundscape_build_is_WARNED_by_name(tmp_path, caplog):
    """>=50 recordings in one cell with exactly ONE distinct week is the
    one-bad-station signature. WARNING, not refusal: single-week deployments
    exist; the warning plus the run log is the audit trail."""
    corpus = _write_soundscape_corpus(tmp_path)
    cell_a = h3.latlng_to_cell(55.6867, 12.57, 4)
    cell_b = h3.latlng_to_cell(48.8, 2.3, 4)
    with caplog.at_level(logging.WARNING, logger="phase_a"):
        path = build_all(
            corpus, config="HSN_test", resolution=4, key_file=None,
            out_dir=tmp_path / "out",
        )
    assert path.exists()  # a warning, never a refusal
    warned = " ".join(
        r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
    )
    assert cell_a in warned
    assert cell_b not in warned


def test_collapsed_cells_ignores_small_single_week_cells():
    """A cell under the 50-recording threshold is too small to call."""
    table = pa.table(
        {
            "cell": pa.array(["a"] * 49 + ["b"] * 50, pa.string()),
            "week": pa.array([1] * 49 + [2] * 50, pa.int8()),
        }
    )
    assert collapsed_cells(table) == ["b"]


# ── the one-shard CLI ───────────────────────────────────────────────────────


def _wav_bytes():
    sr = 16000
    t = np.linspace(0, 0.1, 1600, endpoint=False)
    buf = io.BytesIO()
    sf.write(buf, np.sin(2 * np.pi * 440 * t).astype("float32"), sr, format="WAV")
    return buf.getvalue()


def _make_shard(tmp_path, name, members):
    path = tmp_path / name
    with tarfile.open(path, "w:gz") as tf:
        for member_name, payload in members.items():
            info = tarfile.TarInfo(member_name)
            info.size = len(payload)
            tf.addfile(info, io.BytesIO(payload))
    return path


def _manifest_table(filepaths):
    n = len(filepaths)
    return pa.table(
        {
            "filepath": pa.array(filepaths, pa.string()),
            "cell": pa.array(["8428309ffffffff"] * n, pa.string()),
            "week": pa.array([27] * n, pa.int8()),
            "ebird_code": pa.array(["gretit"] * n, pa.string()),
            "split": pa.array(["train"] * n, pa.string()),
        }
    )


def _write_manifest(tmp_path, filepaths):
    path = tmp_path / "manifest.parquet"
    pq.write_table(_manifest_table(filepaths), path)
    return path


def test_embed_cli_slices_the_manifest_to_the_shards_members(tmp_path):
    """XCL's manifest is ~500k rows; embed_shard iterates its manifest via
    to_pylist(), so the CLI must pass only THIS shard's slice onward."""
    shard = _make_shard(tmp_path, "XCM_shard_0001.tar.gz", {"in_shard.wav": _wav_bytes()})
    manifest = _write_manifest(tmp_path, ["in_shard.wav", "other_shard.wav"])
    seen = {}

    def fake_embed(shard_path, manifest_table, encoder, *, out_dir):
        seen["filepaths"] = manifest_table.column("filepath").to_pylist()
        return ShardResult(shard=shard_path.name, embedded=1, expected=1)

    with patch("birdology.phase_a.embed_cli.embed_shard", side_effect=fake_embed):
        rc = embed_main(
            ["--shard", str(shard), "--manifest", str(manifest),
             "--out", str(tmp_path / "out"), "--encoder", "melsummary"]
        )
    assert rc == 0
    assert seen["filepaths"] == ["in_shard.wav"]


def test_slice_manifest_to_shard_keeps_only_members():
    table = _manifest_table(["a.wav", "b.wav", "c.wav"])
    sliced = slice_manifest_to_shard(table, {"b.wav", "z.wav"})
    assert sliced.column("filepath").to_pylist() == ["b.wav"]


def test_embed_cli_handles_a_dotted_shard_name(tmp_path):
    """`XCM_shard_0001.tar.gz` -> outputs stemmed `XCM_shard_0001.parquet`,
    never `XCM_shard_0001.tar.gz.parquet`."""
    shard = _make_shard(tmp_path, "XCM_shard_0001.tar.gz", {"a.wav": _wav_bytes()})
    manifest = _write_manifest(tmp_path, ["a.wav"])
    out = tmp_path / "out"
    rc = embed_main(
        ["--shard", str(shard), "--manifest", str(manifest),
         "--out", str(out), "--encoder", "melsummary"]
    )
    assert rc == 0
    assert (out / "XCM_shard_0001.parquet").exists()
    assert not (out / "XCM_shard_0001.tar.gz.parquet").exists()


def test_embed_cli_REFUSES_a_complete_shard_without_force(tmp_path):
    """Exit 2 on an already-complete output: one array task owns one shard,
    and silently redoing a done shard is how mixed generations merge."""
    shard = _make_shard(tmp_path, "XCM_shard_0001.tar.gz", {"a.wav": _wav_bytes()})
    manifest = _write_manifest(tmp_path, ["a.wav"])
    out = tmp_path / "out"
    args = ["--shard", str(shard), "--manifest", str(manifest),
            "--out", str(out), "--encoder", "melsummary"]
    assert embed_main(args) == 0
    assert embed_main(args) == 2
    assert embed_main(args + ["--force"]) == 0


def test_embed_cli_exits_1_when_every_expected_file_failed(tmp_path):
    """All-failed is a failure, not a completed task: exit 0 would leave a
    valid empty parquet that shard_is_complete accepts on a re-run."""
    shard = _make_shard(
        tmp_path, "XCM_shard_0002.tar.gz", {"bad.wav": b"not audio at all"}
    )
    manifest = _write_manifest(tmp_path, ["bad.wav"])
    rc = embed_main(
        ["--shard", str(shard), "--manifest", str(manifest),
         "--out", str(tmp_path / "out"), "--encoder", "melsummary"]
    )
    assert rc == 1


def test_embed_cli_logs_embedded_over_expected(tmp_path, caplog):
    shard = _make_shard(tmp_path, "XCM_shard_0003.tar.gz", {"a.wav": _wav_bytes()})
    manifest = _write_manifest(tmp_path, ["a.wav"])
    with caplog.at_level(logging.INFO, logger="phase_a.embed"):
        rc = embed_main(
            ["--shard", str(shard), "--manifest", str(manifest),
             "--out", str(tmp_path / "out"), "--encoder", "melsummary"]
        )
    assert rc == 0
    assert any(
        "embedded=1/expected=1" in r.getMessage() for r in caplog.records
    )


def test_embed_cli_raises_on_ZERO_manifest_overlap(tmp_path):
    """Slicing first would starve embed_shard's own zero-overlap guard, so
    the CLI re-asserts it: zero overlap is a systematic key mismatch, not a
    valid empty shard."""
    shard = _make_shard(tmp_path, "XCM_shard_0004.tar.gz", {"a.wav": _wav_bytes()})
    manifest = _write_manifest(tmp_path, ["completely/other/path.ogg"])
    with pytest.raises(ValueError, match="ZERO overlap"):
        embed_main(
            ["--shard", str(shard), "--manifest", str(manifest),
             "--out", str(tmp_path / "out"), "--encoder", "melsummary"]
        )


# ── the sbatch script ───────────────────────────────────────────────────────
#
# LESSON (this repo's 5th and 6th instance of the defect): asserting a token
# `in script` is satisfiable by the very COMMENTS that explain why the token
# matters — removing `--nv` from the active apptainer line, or deleting the
# whole mountpoint guard, left every sbatch test green. Every assertion below
# is therefore anchored to a syntactic CATEGORY of line: #SBATCH directives,
# active (bash-executed) lines, or header comments — never the whole file.


def _sbatch_text():
    return (
        Path(__file__).parent.parent.parent
        / "scripts" / "cluster" / "phase_a_embed.sbatch"
    ).read_text()


def _sbatch_directive_lines(text):
    """#SBATCH lines — comments to bash, directives to Slurm."""
    return [ln for ln in text.splitlines() if ln.lstrip().startswith("#SBATCH")]


def _active_lines(text):
    """Lines bash actually executes: non-empty, not comments (which includes
    the #SBATCH directives — those are their own category above)."""
    return [
        ln
        for ln in text.splitlines()
        if ln.strip() and not ln.lstrip().startswith("#")
    ]


def test_the_sbatch_script_requests_a_gpu_in_a_directive():
    """No --gres = torch.cuda.is_available() False INSIDE the container even
    with --nv (measured). Must be a real #SBATCH directive, not the comment
    explaining it."""
    assert any(
        "--gres=gpu:" in ln for ln in _sbatch_directive_lines(_sbatch_text())
    )


def test_the_active_apptainer_line_passes_nv_and_the_weka_bind():
    """Both are silent failures: no bind = empty dir, no --nv = no CUDA
    driver. Both flags must sit on the ACTIVE line that invokes apptainer —
    the header comments naming them do not count."""
    apptainer = [
        ln for ln in _active_lines(_sbatch_text()) if "apptainer" in ln
    ]
    assert len(apptainer) == 1
    assert "--nv" in apptainer[0]
    assert "--bind /mnt/weka:/mnt/weka" in apptainer[0]


def test_the_sbatch_script_is_fleet_wide():
    """WEKA is fleet-wide (verified 2026-09-30): no node pin."""
    assert "--nodelist" not in _sbatch_text()


def test_the_sbatch_script_fails_if_weka_is_not_mounted():
    """20 nodes mount independently; one silently losing the mount fabricates
    an empty corpus on that node only. The guard must be an ACTIVE line — a
    comment about the guard satisfied the old whole-file grep."""
    assert any(
        "mountpoint -q /mnt/weka" in ln for ln in _active_lines(_sbatch_text())
    )


# ── Slurm log-dir ordering (I2) ─────────────────────────────────────────────
# Slurm opens the #SBATCH --output path BEFORE the job runs, so the in-job
# mkdir is too late on the first-ever submission. The Makefile manifest target
# (which runs on a WEKA-mounted node before any array) pre-creates it, and the
# sbatch header must tell the operator.


def _makefile_recipe(target):
    """Active (non-comment) recipe lines of one Makefile target."""
    lines = (Path(__file__).parent.parent.parent / "Makefile").read_text().splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith(f"{target}:"))
    recipe = []
    for ln in lines[start + 1:]:
        if not ln.startswith("\t"):
            break
        recipe.append(ln.strip())
    return [ln for ln in recipe if not ln.startswith("#") and not ln.startswith("@#")]


def test_the_manifest_make_target_precreates_the_slurm_log_dir():
    assert any(
        "mkdir -p /mnt/weka/ML/phase_a/logs" in ln
        for ln in _makefile_recipe("phase-a-manifest")
    )


def test_the_sbatch_header_documents_the_log_dir_precreation():
    header = [
        ln
        for ln in _sbatch_text().splitlines()
        if ln.startswith("#") and not ln.startswith("#SBATCH")
    ]
    assert any("mkdir -p /mnt/weka/ML/phase_a/logs" in ln for ln in header)


# ── array-range coverage and the zero-shard guard (I3 / P1) ─────────────────
# The block from shard discovery through the index guard is extracted from
# the COMMITTED script and run under bash, so the tests exercise the real
# lines: deleting the warning block or the zero-shard exit-2 guard from the
# script must fail these.


def _sbatch_guard_block():
    lines = _sbatch_text().splitlines()
    # The shard listing is now an if/else on $SPLIT (split-specific shard
    # globs, measured on WEKA); the guard block starts at that branch.
    start = next(
        i for i, ln in enumerate(lines) if ln.startswith('if [ -n "$SPLIT" ]')
    )
    stop = next(
        i
        for i, ln in enumerate(lines)
        if "nothing to do" in ln and not ln.lstrip().startswith("#")
    )
    return "\n".join(lines[start : stop + 1])


def _modern_bash():
    """A bash >= 4 (mapfile). macOS /bin/bash is 3.2; the cluster and CI run
    4+. Skip (loudly) rather than fake the guard when none is available."""
    candidates = ["bash"] + [
        p for p in ("/opt/homebrew/bin/bash", "/usr/local/bin/bash", "/bin/bash")
        if Path(p).exists()
    ]
    for bash in candidates:
        try:
            r = subprocess.run(
                [bash, "-c", "echo ${BASH_VERSINFO[0]}"],
                capture_output=True, text=True,
            )
            if r.returncode == 0 and int(r.stdout.strip()) >= 4:
                return bash
        except (OSError, ValueError):
            continue
    pytest.skip("no bash >= 4 available for the sbatch guard-block tests")


def _run_guard_block(tmp_path, n_shards, *, task_max):
    corpus = tmp_path / "corpus"
    corpus.mkdir(exist_ok=True)
    for i in range(n_shards):
        (corpus / f"XCM_shard_{i:04d}.tar.gz").write_bytes(b"")
    script = (
        f'set -euo pipefail\nCORPUS="{corpus}"\nCONFIG="XCM"\nSPLIT=""\n'
        + _sbatch_guard_block()
    )
    return subprocess.run(
        [_modern_bash(), "-c", script],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "SLURM_ARRAY_TASK_ID": "0",
            "SLURM_ARRAY_TASK_MAX": str(task_max),
        },
    )


def test_an_UNDERPROVISIONED_array_warns_loudly_in_task_0(tmp_path):
    """200 shards, --array=0-181: shards 182+ would silently never embed —
    the warning in task 0's log is the only signal (never a nonzero exit:
    a deliberate partial re-run of a range is legitimate)."""
    r = _run_guard_block(tmp_path, 200, task_max=181)
    assert r.returncode == 0
    assert "WARNING: array covers 182 of 200 shards" in r.stdout
    assert "182..199 will NOT be embedded" in r.stdout


def test_a_fully_provisioned_array_does_not_warn(tmp_path):
    r = _run_guard_block(tmp_path, 200, task_max=199)
    assert r.returncode == 0
    assert "WARNING" not in r.stdout


def test_zero_shards_is_a_FATAL_exit_2_not_a_silent_success(tmp_path):
    """Without the guard, an empty corpus (e.g. a bad CONFIG) yields
    'nothing to do' exit 0 on every task — a fully green, fully empty run."""
    r = _run_guard_block(tmp_path, 0, task_max=0)
    assert r.returncode == 2
    assert "no shards" in r.stdout


def test_split_selects_the_named_parquet_and_names_the_artifact(tmp_path):
    """HSN ships THREE metadata parquets (measured on WEKA); without --split
    the fallback chain could only ever reach train, and a shared output name
    would let train and test silently overwrite each other."""
    corpus = tmp_path / "HSN" / "HSN"
    corpus.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "filepath": pa.array(["HSN_001_20150708_061805.ogg"], pa.string()),
                "lat": pa.array([37.0], pa.float64()),
                "long": pa.array([-118.5], pa.float64()),
                "ebird_code": pa.array(["whcspa"], pa.string()),
            }
        ),
        corpus / "HSN_metadata_test_5s.parquet",
    )
    # A decoy train parquet the default chain would have picked.
    pq.write_table(
        pa.table({"filepath": pa.array(["x.ogg"], pa.string())}),
        corpus / "HSN_metadata_train.parquet",
    )
    out = build_all(
        tmp_path, config="HSN", resolution=4, key_file=None,
        out_dir=tmp_path / "out", split="test_5s",
    )
    assert out.name == "HSN_test_5s_r4_manifest.parquet"
    assert pq.read_table(out).num_rows == 1


def test_split_that_does_not_exist_fails_loudly_naming_the_alternatives(tmp_path):
    corpus = tmp_path / "HSN" / "HSN"
    corpus.mkdir(parents=True)
    pq.write_table(
        pa.table({"filepath": pa.array(["x.ogg"], pa.string())}),
        corpus / "HSN_metadata_train.parquet",
    )
    with pytest.raises(FileNotFoundError, match="HSN_metadata_train"):
        build_all(
            tmp_path, config="HSN", resolution=4, key_file=None,
            out_dir=tmp_path / "out", split="nope",
        )
