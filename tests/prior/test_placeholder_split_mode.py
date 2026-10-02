"""The placeholder-contributor split mode for the leakage guard.

THE FLEET FAILURE THIS EXISTS FOR (job 5345, reproduced here synthetically)
---------------------------------------------------------------------------
``splits_eval.csv`` rows carry ``recorded_by`` values like ``HSN`` / ``NES``
-- the TEST-SET NAME, a placeholder, because soundscape recordings have no
contributor. An eBird observer ID can never equal ``HSN``, so the
(cell, week, contributor) join is STRUCTURALLY empty against EOD and the
zero-match guard correctly refused after ~9 hours of parsing. The guard was
right; the split key was wrong for cross-dataset use.

The fix is an EXPLICIT mode, never an auto-detected one: in
``contributor_mode="placeholder"`` the banned key is (cell, week) -- every
occurrence in a test site's cell-week is removed regardless of who recorded
it. That is MORE conservative than contributor matching (it bans strangers'
rows too), which is the right direction for a leakage guard.
"""
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

import h3

from birdology.prior.indexing import assign_cells
from birdology.prior.leakage import (
    TestRecording,
    apply_leakage_guards,
)
from birdology.prior.occurrences import read_occurrences

import build_prior
from build_prior import (
    PartialMatchGuardError,
    ZeroMatchGuardError,
    build_from_archive,
)

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)

DOI = "10.15468/dl.test"

#: Four well-separated Danish points (distinct cells at res 3, 4 and 5).
POINTS = [
    (55.6867, 12.5700),   # Copenhagen
    (55.9000, 12.5000),   # North Zealand
    (56.1629, 10.2039),   # Aarhus
    (57.0488, 9.9217),    # Aalborg
]
#: ISO weeks 2, 24 and 46.
DATES = ("2025-01-07", "2025-06-10", "2025-11-14")
WEEKS = (2, 24, 46)


def _row(i, sk, lat, lon, d, pub="pub", who="ole hansen"):
    return "\t".join(
        [str(i), "ds", pub, str(sk), f"sp{sk}", str(lat), str(lon), d, who]
    )


def _zip(tmp_path, rows, name="occ.zip"):
    zip_path = tmp_path / name
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return zip_path


def _archive(tmp_path, who="ole hansen"):
    """Every point x every week x two species, all recorded by *who*.

    The contributor namespace here ('ole hansen') is DISJOINT from the
    placeholder names the split carries ('HSN', 'NES'), exactly as eBird
    observer IDs are disjoint from test-set names. That disjointness is what
    makes the match-mode join structurally empty.
    """
    rows = []
    i = 0
    for wi, d in enumerate(DATES):
        for pi, (lat, lon) in enumerate(POINTS):
            for sk in (1001, 1002):
                # UNBALANCED and varying by cell and week: identical counts
                # everywhere make every cell block of the prior equal, and
                # the shuffle_space control then (correctly) refuses -- a
                # permutation cannot destroy structure that is not there.
                for _ in range(1 + ((wi + pi + sk) % 4)):
                    i += 1
                    rows.append(_row(i, sk, lat + i * 1e-7, lon, d, who=who))
    return _zip(tmp_path, rows)


def _placeholder_split(resolution):
    """A split the way job 5345's splits_eval.csv is shaped: real sites,
    placeholder contributors ('HSN', 'NES') that no occurrence can match."""
    lat0, lon0 = POINTS[0]
    lat1, lon1 = POINTS[2]
    return [
        TestRecording(
            cell=h3.latlng_to_cell(lat0, lon0, resolution),
            week=WEEKS[0],
            recorded_by="HSN",
        ),
        TestRecording(
            cell=h3.latlng_to_cell(lat1, lon1, resolution),
            week=WEEKS[1],
            recorded_by="NES",
        ),
    ]


def _guarded_table(tmp_path, resolution=4):
    return assign_cells(read_occurrences(_archive(tmp_path)), resolution)


# --------------------------------------------------------------------------
# The reproduction pin: match mode against a placeholder split is a
# STRUCTURAL zero-match, and the guard refuses. This is current behavior and
# must stay: silence here is the bug the fleet run paid 9 hours to expose.
# --------------------------------------------------------------------------


def test_match_mode_refuses_a_placeholder_split_structurally(tmp_path):
    split = _placeholder_split(4)
    with pytest.raises(ZeroMatchGuardError, match="matched no occurrence"):
        build_from_archive(
            _archive(tmp_path),
            resolution=4,
            source="eod",
            test_recordings=split,
            out_dir=tmp_path / "priors",
            gbif_doi=DOI,
        )
    assert not list((tmp_path / "priors").glob("*.npz"))


# --------------------------------------------------------------------------
# Placeholder mode: the banned key is (cell, week).
# --------------------------------------------------------------------------


def test_placeholder_mode_bans_cell_weeks_regardless_of_contributor(tmp_path):
    """The exact rows the fleet run could not remove ARE removed: same split,
    same disjoint contributor namespace, mode='placeholder'."""
    table = _guarded_table(tmp_path)
    split = _placeholder_split(4)
    result = apply_leakage_guards(
        table, split, contributor_mode="placeholder"
    )
    assert result.contributor_rows_removed > 0
    assert result.banned_key_count == 2
    assert result.matched_key_count == 2
    assert not result.zero_match
    # Every surviving row is OUTSIDE the banned cell-weeks.
    banned = {(r.cell, r.week) for r in split}
    kept = set(
        zip(
            result.table.column("cell").to_pylist(),
            result.table.column("week").to_pylist(),
        )
    )
    assert not (kept & banned)


def test_placeholder_mode_removes_anonymous_rows_too(tmp_path):
    """(cell, week) banning is MORE conservative than contributor matching:
    a row with a NULL recorded_by in a banned cell-week is removed, where
    match mode always keeps it (a null can never match a contributor)."""
    lat, lon = POINTS[0]
    rows = [
        "\t".join(["1", "ds", "pub", "1001", "sp", str(lat), str(lon),
                   DATES[0], ""]),  # anonymous, in the banned cell-week
        _row(2, 1001, *POINTS[3], DATES[2]),  # elsewhere, kept
    ]
    table = assign_cells(read_occurrences(_zip(tmp_path, rows)), 4)
    split = [_placeholder_split(4)[0]]

    matched = apply_leakage_guards(table, split, contributor_mode="match")
    assert matched.contributor_rows_removed == 0  # null never matches

    placeholder = apply_leakage_guards(
        table, split, contributor_mode="placeholder"
    )
    assert placeholder.contributor_rows_removed == 1
    assert placeholder.table.num_rows == 1


def test_match_mode_is_the_default_and_unchanged(tmp_path):
    """No silent semantics change: omitting the mode is match mode, and a
    placeholder split under it still matches nothing."""
    table = _guarded_table(tmp_path)
    result = apply_leakage_guards(table, _placeholder_split(4))
    assert result.contributor_rows_removed == 0
    assert result.zero_match
    assert result.contributor_mode == "match"


def test_an_unknown_contributor_mode_is_refused(tmp_path):
    table = _guarded_table(tmp_path)
    with pytest.raises(ValueError, match="contributor_mode"):
        apply_leakage_guards(
            table, _placeholder_split(4), contributor_mode="auto"
        )


# --------------------------------------------------------------------------
# The zero-match and match-rate clauses stay OPERATIVE in placeholder mode:
# the mode changes the key, not the escalation.
# --------------------------------------------------------------------------


def test_zero_match_still_fires_in_placeholder_mode(tmp_path):
    """A split whose SITES match nothing (Sydney against Danish occurrences)
    still refuses -- and the error names the (cell, week) key shape and the
    mode, so the operator triages the right join."""
    ghost = [
        TestRecording(
            cell=h3.latlng_to_cell(-33.8688, 151.2093, 4),
            week=2,
            recorded_by="HSN",
        )
    ]
    with pytest.raises(ZeroMatchGuardError) as excinfo:
        build_from_archive(
            _archive(tmp_path),
            resolution=4,
            source="eod",
            test_recordings=ghost,
            out_dir=tmp_path / "priors",
            gbif_doi=DOI,
            split_contributor_mode="placeholder",
        )
    message = str(excinfo.value)
    assert "(cell, week)" in message
    assert "placeholder" in message


def test_partial_match_rate_still_fires_in_placeholder_mode(tmp_path):
    """One live cell-week among twenty-one dead ones is below the default
    5% rate; the per-key clause must bite with the new denominator."""
    live = _placeholder_split(4)[0]
    dead = [
        TestRecording(
            cell=h3.latlng_to_cell(-33.8688 + i * 0.5, 151.2093, 4),
            week=2,
            recorded_by="HSN",
        )
        for i in range(25)
    ]
    with pytest.raises(PartialMatchGuardError) as excinfo:
        build_from_archive(
            _archive(tmp_path),
            resolution=4,
            source="eod",
            test_recordings=[live, *dead],
            out_dir=tmp_path / "priors",
            gbif_doi=DOI,
            split_contributor_mode="placeholder",
        )
    message = str(excinfo.value)
    assert "(cell, week)" in message
    assert "placeholder" in message


def test_placeholder_mode_builds_a_complete_release(tmp_path):
    """GREEN for the fleet rerun: the same (archive, split) pair that match
    mode structurally refuses builds a complete artifact set in placeholder
    mode, with the banned cell-weeks removed."""
    paths = build_from_archive(
        _archive(tmp_path),
        resolution=4,
        source="eod",
        test_recordings=_placeholder_split(4),
        out_dir=tmp_path / "priors",
        gbif_doi=DOI,
        split_contributor_mode="placeholder",
    )
    assert paths
    assert all(p.exists() for p in paths)


def test_the_error_message_names_the_mode_in_match_mode_too(tmp_path):
    """The match-mode refusal must now point at the escape hatch: an operator
    whose split carries placeholder contributors needs to learn the flag from
    the error, not from the source."""
    with pytest.raises(ZeroMatchGuardError) as excinfo:
        build_from_archive(
            _archive(tmp_path),
            resolution=4,
            source="eod",
            test_recordings=_placeholder_split(4),
            out_dir=tmp_path / "priors",
            gbif_doi=DOI,
        )
    message = str(excinfo.value)
    assert "match" in message
    assert "placeholder" in message  # the hint toward the mode


# --------------------------------------------------------------------------
# CLI wiring.
# --------------------------------------------------------------------------


def test_cli_accepts_the_mode_and_defaults_to_match():
    ns = build_prior._parse_args(
        ["--archive", "a.zip", "--source", "eod", "--gbif-doi", DOI,
         "--no-test-split"]
    )
    assert ns.split_contributor_mode == "match"
    ns = build_prior._parse_args(
        ["--archive", "a.zip", "--source", "eod", "--gbif-doi", DOI,
         "--test-split", "s.csv", "--split-contributor-mode", "placeholder"]
    )
    assert ns.split_contributor_mode == "placeholder"


def test_cli_refuses_placeholder_mode_without_a_split():
    """`--split-contributor-mode placeholder --no-test-split` is an operator
    contradiction: a mode for a split that does not exist. Refused at parse
    time rather than silently ignored."""
    with pytest.raises(SystemExit):
        build_prior._parse_args(
            ["--archive", "a.zip", "--source", "eod", "--gbif-doi", DOI,
             "--no-test-split", "--split-contributor-mode", "placeholder"]
        )
