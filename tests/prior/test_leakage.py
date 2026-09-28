"""Leakage guards: the prior must not contain the answers."""
import logging
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.indexing import aggregate_counts, assign_cells
from birdology.prior.leakage import (
    REPLACEMENT_CHARACTER,
    XENO_CANTO_PUBLISHER_KEYS,
    TestRecording,
    apply_leakage_guards,
    drop_test_contributor_cells,
    drop_xeno_canto,
    normalise_contributor,
)

XC = next(iter(XENO_CANTO_PUBLISHER_KEYS))


def _table(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "scientific_name": pa.array(["A"] * len(rows), pa.string()),
            "lat": pa.array([r[1] for r in rows], pa.float64()),
            "lon": pa.array([r[2] for r in rows], pa.float64()),
            "event_date": pa.array([r[3] for r in rows], pa.date32()),
            "publisher_key": pa.array([r[4] for r in rows], pa.string()),
            "recorded_by": pa.array([r[5] for r in rows], pa.string()),
        }
    )


def test_drops_xeno_canto_publisher():
    t = _table(
        [
            (1, 55.6, 12.5, date(2025, 3, 3), XC, "obs"),
            (1, 55.6, 12.5, date(2025, 3, 3), "other-pub", "obs"),
        ]
    )
    out = drop_xeno_canto(t)
    assert out.num_rows == 1
    assert out.column("publisher_key")[0].as_py() == "other-pub"


def test_drops_same_contributor_cell_week():
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Jens"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 1
    assert out.column("recorded_by")[0].as_py() == "Jens"


def test_keeps_same_contributor_different_week():
    t = assign_cells(
        _table([(1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole")]),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=2, recorded_by="Ole")]
    )
    assert out.num_rows == 1


def test_guarded_table_differs_from_unguarded():
    """Review Focus 5: a record that would leak must actually be removed."""
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), XC, "Ole"),
                (2, 56.0000, 10.0000, date(2025, 6, 1), "pub", "Jens"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    guarded = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert guarded.num_rows < t.num_rows
    assert XC not in guarded.column("publisher_key").to_pylist()


def test_no_test_recordings_is_a_noop_beyond_xc():
    t = assign_cells(
        _table([(1, 55.6, 12.5, date(2025, 3, 3), "pub", "Ole")]), resolution=4
    )
    assert apply_leakage_guards(t, []).num_rows == 1


# --------------------------------------------------------------------------
# (C) The Xeno-Canto publisher key must be the real one.
# --------------------------------------------------------------------------


def test_xeno_canto_key_is_the_verified_publishing_organisation():
    """Pins the GBIF publishing-organisation key for Xeno-canto Foundation.

    Verified 2026-09-29 against
    https://api.gbif.org/v1/organization/1f00d75c-f6fc-4224-a595-975e82d7689c
    -> "Xeno-canto Foundation for Nature Sounds".

    The task brief named e2e717bf-551a-4917-bdc9-4fa0f342c530, which the same
    endpoint resolves to the Cornell Lab of Ornithology. Using it would leak
    every Xeno-Canto occurrence AND delete the whole eBird population, so this
    test locks both the right key in and the wrong key out.
    """
    assert XENO_CANTO_PUBLISHER_KEYS == frozenset(
        {"1f00d75c-f6fc-4224-a595-975e82d7689c"}
    )
    assert "e2e717bf-551a-4917-bdc9-4fa0f342c530" not in XENO_CANTO_PUBLISHER_KEYS


def test_null_publisher_row_survives_the_xeno_canto_drop():
    """pc.is_in(null, value_set) is false, not null, so an unattributed row is
    KEPT — the correct direction: absence of a publisher is not evidence of
    Xeno-Canto provenance."""
    t = pa.table(
        {
            "species_key": pa.array([1, 2], pa.int64()),
            "scientific_name": pa.array(["A", "A"], pa.string()),
            "lat": pa.array([55.6, 55.6], pa.float64()),
            "lon": pa.array([12.5, 12.5], pa.float64()),
            "event_date": pa.array([date(2025, 3, 3)] * 2, pa.date32()),
            "publisher_key": pa.array([None, XC], pa.string()),
            "recorded_by": pa.array(["obs", "obs"], pa.string()),
        }
    )
    out = drop_xeno_canto(t)
    assert out.num_rows == 1
    assert out.column("publisher_key")[0].as_py() is None


def test_xeno_canto_drop_removes_every_listed_key():
    """Guards against a one-key-only implementation if the set ever grows."""
    keys = sorted(XENO_CANTO_PUBLISHER_KEYS)
    t = _table(
        [(1, 55.6, 12.5, date(2025, 3, 3), k, "obs") for k in keys]
        + [(1, 55.6, 12.5, date(2025, 3, 3), "other-pub", "obs")]
    )
    out = drop_xeno_canto(t)
    assert out.num_rows == 1
    assert out.column("publisher_key").to_pylist() == ["other-pub"]


# --------------------------------------------------------------------------
# (A) A blank contributor on a TestRecording must never become a join key.
# --------------------------------------------------------------------------


def _one_anonymous_and_one_named():
    return assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", None),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
            ]
        ),
        resolution=4,
    )


@pytest.mark.parametrize("blank", ["", "   ", "\t\n", None])
def test_test_recording_rejects_blank_contributor(blank):
    """A blank contributor would match every anonymous occurrence in the
    cell-week and silently delete the entire anonymous population there."""
    with pytest.raises(ValueError, match="recorded_by"):
        TestRecording(cell="841f059ffffffff", week=46, recorded_by=blank)


def test_blank_contributor_recording_removes_zero_rows():
    """The accumulated-requirement regression: constructing such a recording
    must raise rather than silently wipe the anonymous population.

    Stated as the brief's reviewer stated it — 'must remove ZERO rows (or
    raise)'. We implement raise, and assert the table is untouched on the
    path that bypasses construction (a duck-typed caller)."""
    t = _one_anonymous_and_one_named()
    cell = t.column("cell")[0].as_py()

    with pytest.raises(ValueError):
        TestRecording(cell=cell, week=46, recorded_by="")

    # A duck-typed recording that skipped __post_init__ must still be skipped
    # by the guard, not used as a join key.
    blank = SimpleNamespace(cell=cell, week=46, recorded_by="   ")
    out = drop_test_contributor_cells(t, [blank])
    assert out.num_rows == t.num_rows
    assert out.column("recorded_by").to_pylist() == [None, "Ole"]


def test_null_recorded_by_never_matches_a_real_contributor():
    """An anonymous occurrence must survive a guard aimed at a named one."""
    t = _one_anonymous_and_one_named()
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 1
    assert out.column("recorded_by")[0].as_py() is None


def test_test_recording_rejects_blank_cell():
    with pytest.raises(ValueError, match="cell"):
        TestRecording(cell="  ", week=46, recorded_by="Ole")


@pytest.mark.parametrize("week", [0, 53, -1, 99])
def test_test_recording_rejects_out_of_range_week(week):
    """Upstream weeks are int8 1..52 with 53 folded into 52, so a recording
    outside that range can only be a units bug and would never match."""
    with pytest.raises(ValueError, match="week"):
        TestRecording(cell="841f059ffffffff", week=week, recorded_by="Ole")


# --------------------------------------------------------------------------
# (B) Contributor names are normalised on both sides before the join.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "occurrence_name,test_name",
    [
        ("Ole Hansen", "ole hansen"),        # case
        ("ole hansen", "Ole Hansen"),        # case, other direction
        ("Ole  Hansen", "Ole Hansen"),       # doubled inner space
        ("  Ole Hansen  ", "Ole Hansen"),    # surrounding whitespace
        ("Ole\tHansen", "Ole Hansen"),       # tab vs space
        ("Ole Hansen", "OLE HANSEN"),        # upper case
        ("Ole Hansen", "Ole Hansen"),        # identity (control)
    ],
)
def test_contributor_match_is_normalised(occurrence_name, test_name):
    """A MISSED contributor match leaves a leaking occurrence in the prior,
    which is the exact failure the guard exists to prevent. Over-matching only
    shrinks data, so normalisation is deliberately aggressive."""
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", occurrence_name),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Someone Else"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by=test_name)]
    )
    assert out.num_rows == 1, f"{occurrence_name!r} should match {test_name!r}"
    assert out.column("recorded_by")[0].as_py() == "Someone Else"


def test_normalisation_does_not_merge_distinct_contributors():
    """Aggressive normalisation must still not collapse two different people."""
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole Hansen"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole Hansens"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Jens Hansen"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole Hansen")]
    )
    assert out.column("recorded_by").to_pylist() == ["Ole Hansens", "Jens Hansen"]


def test_normalise_contributor_blank_and_none_become_none():
    for blank in ("", "   ", "\t\n", None):
        assert normalise_contributor(blank) is None
    assert normalise_contributor("Ole Hansen") == "ole hansen"


def test_nfkc_compatibility_forms_match():
    """read_occurrences does not NFKC-normalise, so a full-width or ligature
    spelling can reach the table; NFKC folds them onto the plain form."""
    assert normalise_contributor("Ｏｌｅ") == normalise_contributor("Ole")
    assert normalise_contributor("ﬁsher") == normalise_contributor("fisher")


def test_replacement_character_rows_are_flagged(caplog):
    """read_occurrences decodes with errors='replace', so a non-UTF-8 byte in a
    contributor name becomes U+FFFD and that row can no longer match its test
    recording. We cannot repair it, so we warn loudly."""
    mojibake = "Ole H" + REPLACEMENT_CHARACTER + "nsen"
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", mojibake),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole Hansen"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    with caplog.at_level(logging.WARNING, logger="birdology.prior.leakage"):
        out = drop_test_contributor_cells(
            t, [TestRecording(cell=cell, week=46, recorded_by="Ole Hansen")]
        )

    # The mojibaked row survives (it genuinely is a different string) ...
    assert out.column("recorded_by").to_pylist() == [mojibake]
    # ... but the operator is told, with a count, that it happened.
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("U+FFFD" in m for m in warnings), warnings
    assert any("1 occurrence" in m for m in warnings), warnings


def test_no_warning_when_no_replacement_characters(caplog):
    """Guards the previous test against a vacuous pass from an always-on warning."""
    t = assign_cells(
        _table([(1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole Hansen")]),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    with caplog.at_level(logging.WARNING, logger="birdology.prior.leakage"):
        drop_test_contributor_cells(
            t, [TestRecording(cell=cell, week=46, recorded_by="Jens")]
        )
    assert not [r for r in caplog.records if "U+FFFD" in r.getMessage()]


# --------------------------------------------------------------------------
# Guard composition, ordering and non-vacuity.
# --------------------------------------------------------------------------


def test_cell_is_part_of_the_key():
    """A same-contributor, same-week record in a DIFFERENT cell must survive."""
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
                (1, 40.0000, -3.7000, date(2025, 11, 14), "pub", "Ole"),
            ]
        ),
        resolution=4,
    )
    cells = t.column("cell").to_pylist()
    assert cells[0] != cells[1]
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cells[0], week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 1
    assert out.column("cell")[0].as_py() == cells[1]


def test_multiple_test_recordings_all_apply():
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Jens"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Mette"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t,
        [
            TestRecording(cell=cell, week=46, recorded_by="Ole"),
            TestRecording(cell=cell, week=46, recorded_by="Mette"),
        ],
    )
    assert out.column("recorded_by").to_pylist() == ["Jens"]


def test_apply_leakage_guards_runs_both_guards():
    """Neither guard alone suffices: the XC row is not contributor-matched and
    the contributor row is not XC-published, so a one-guard implementation
    leaves a leaking row behind."""
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), XC, "Nobody"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Jens"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert out.column("recorded_by").to_pylist() == ["Jens"]
    assert out.column("publisher_key").to_pylist() == ["pub"]


def test_guards_preserve_schema_and_column_order():
    """Task 5 and aggregate_counts index these columns by name; a filter must
    not reorder or drop them."""
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), XC, "Ole"),
                (2, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Jens"),
            ]
        ),
        resolution=4,
    )
    out = apply_leakage_guards(
        t, [TestRecording(cell=t.column("cell")[0].as_py(), week=46, recorded_by="Ole")]
    )
    assert out.schema == t.schema


def test_guards_on_an_empty_table_are_a_noop():
    t = assign_cells(_table([]), resolution=4)
    out = apply_leakage_guards(
        t, [TestRecording(cell="841f059ffffffff", week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 0
    assert out.schema == t.schema


def test_guards_run_before_aggregation_loses_provenance():
    """Contract check: aggregate_counts discards publisher_key and recorded_by,
    so the guards cannot run after it. This test fails loudly if that contract
    ever changes under Task 4's feet."""
    t = assign_cells(
        _table([(1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole")]),
        resolution=4,
    )
    aggregated = aggregate_counts(t)
    assert "publisher_key" not in aggregated.schema.names
    assert "recorded_by" not in aggregated.schema.names
    # ... and the guarded-then-aggregated path really is smaller.
    cell = t.column("cell")[0].as_py()
    guarded = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert aggregate_counts(guarded).num_rows == 0
    assert aggregated.num_rows == 1
