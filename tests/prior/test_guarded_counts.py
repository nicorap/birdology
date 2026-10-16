"""guarded_counts == aggregate_counts(apply_leakage_guards(...)) and its guard record."""
import sys
from datetime import date
from pathlib import Path

import h3
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from test_parallel_parse import DATES, POINTS, _messy_archive  # noqa: E402

from birdology.prior.archive_parquet import convert_archive  # noqa: E402
from birdology.prior.guarded_counts import guarded_counts  # noqa: E402
from birdology.prior.indexing import aggregate_counts, assign_cells, iso_week  # noqa: E402
from birdology.prior.leakage import TestRecording, apply_leakage_guards  # noqa: E402
from birdology.prior.occurrence_table import build_occurrence_table  # noqa: E402
from birdology.prior.occurrences import read_occurrences  # noqa: E402

RES = [3, 5]


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("gc")
    archive = _messy_archive(tmp, n_clean=1200)
    convert_archive(archive, tmp / "raw", row_group_rows=300, block_size=8 << 10)
    build_occurrence_table(tmp / "raw", tmp / "occ", RES, workers=1, row_groups_per_file=1)
    return archive, tmp / "occ"


def _recordings(resolution, who):
    week = lambda d: iso_week(date.fromisoformat(d))  # noqa: E731
    return [
        TestRecording(cell=h3.latlng_to_cell(*POINTS[0], resolution), week=week(DATES[0]), recorded_by=who),
        TestRecording(cell=h3.latlng_to_cell(*POINTS[2], resolution).upper(), week=week(DATES[2]), recorded_by=who),
        TestRecording(cell=h3.latlng_to_cell(*POINTS[3], resolution), week=week(DATES[1]), recorded_by="nobody"),
    ]


def _reference(archive, resolution, recordings, mode):
    guarded = apply_leakage_guards(assign_cells(read_occurrences(archive), resolution), recordings, mode)
    guard = {
        "banned_key_count": guarded.banned_key_count,
        "matched_key_count": guarded.matched_key_count,
        "xeno_canto_rows_removed": guarded.xeno_canto_rows_removed,
        "contributor_rows_removed": guarded.contributor_rows_removed,
        "rows_before_contributor_guard": guarded.rows_before_contributor_guard,
        "rows_after_guards": guarded.table.num_rows,
    }
    return aggregate_counts(guarded.table), guard


def _rows(t):
    return sorted(zip(*(t[c].to_pylist() for c in ("species_key", "cell", "week", "n"))))


@pytest.mark.parametrize("resolution", RES)
@pytest.mark.parametrize("mode,who", [("match", "Ole Hansen "), ("placeholder", "DKX test site")])
def test_counts_and_guard_match_the_reference(built, resolution, mode, who):
    archive, occ = built
    recordings = _recordings(resolution, who)
    counts, guard, _ = guarded_counts(occ, resolution, recordings, mode, workers=2, buckets=4)
    ref_counts, ref_guard = _reference(archive, resolution, recordings, mode)
    assert guard == ref_guard
    assert guard["contributor_rows_removed"] > 0 and guard["xeno_canto_rows_removed"] > 0
    assert _rows(counts) == _rows(ref_counts)


def test_no_split(built):
    archive, occ = built
    counts, guard, _ = guarded_counts(occ, 5, [], "placeholder", workers=1, buckets=2)
    ref_counts, ref_guard = _reference(archive, 5, [], "placeholder")
    assert guard == ref_guard and guard["banned_key_count"] == 0
    assert _rows(counts) == _rows(ref_counts)


def test_resolution_mismatch_raises(built):
    _, occ = built
    with pytest.raises(ValueError, match="resolution"):
        guarded_counts(occ, 5, _recordings(3, "x"), "placeholder", workers=1, buckets=2)
    with pytest.raises(ValueError, match="not resolution 4"):
        guarded_counts(occ, 4, [], "placeholder", workers=1, buckets=2)
