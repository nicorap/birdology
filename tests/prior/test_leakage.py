"""Leakage guards: the prior must not contain the answers."""
import logging
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import h3
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import birdology.prior.leakage as leakage_module
from birdology.prior.indexing import aggregate_counts, assign_cells
from birdology.prior.leakage import (
    REPLACEMENT_CHARACTER,
    XENO_CANTO_PUBLISHER_KEYS,
    TestRecording,
    apply_leakage_guards,
    banned_keys,
    drop_test_contributor_cells,
    drop_xeno_canto,
    keep_mask,
    normalise_cell,
    normalise_contributor,
    normalise_week,
)

XC = next(iter(XENO_CANTO_PUBLISHER_KEYS))

#: Real H3 indexes. `normalise_cell` casefolds and `_assert_one_resolution`
#: calls `h3.get_resolution`, so a placeholder like "C" is neither a valid H3
#: index nor stable under normalisation; unit tests must use real ones.
CELL_A = h3.latlng_to_cell(55.6867, 12.5700, 4)
CELL_B = h3.latlng_to_cell(40.0000, -3.7000, 4)
CELL_A_RES5 = h3.latlng_to_cell(55.6867, 12.5700, 5)


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


# --------------------------------------------------------------------------
# The two blank-contributor layers, pinned INDEPENDENTLY.
#
# banned_keys and keep_mask are each individually sufficient to stop a blank
# contributor wiping an anonymous population, so an end-to-end test cannot
# tell which one is doing the work: mutating either alone leaves the suite
# green. Unit tests on each layer make the redundancy observable.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("blank", ["", "   ", "\t\n", None])
def test_banned_keys_never_admits_a_blank_contributor(blank):
    """Layer 1, alone: a blank contributor must not become a join key."""
    rec = SimpleNamespace(cell="841f059ffffffff", week=46, recorded_by=blank)
    assert banned_keys([rec]) == set()


def test_banned_keys_warns_when_it_skips_a_blank_contributor(caplog):
    rec = SimpleNamespace(cell="841f059ffffffff", week=46, recorded_by="")
    with caplog.at_level(logging.WARNING, logger="birdology.prior.leakage"):
        banned_keys([rec])
    assert any("blank contributor" in r.getMessage() for r in caplog.records)


def test_banned_keys_normalises_and_keeps_real_contributors():
    rec = SimpleNamespace(cell="841f059ffffffff", week=46, recorded_by="  Ole  Hansen ")
    assert banned_keys([rec]) == {("841f059ffffffff", 46, "ole hansen")}


@pytest.mark.parametrize("blank", ["", "   ", None])
def test_keep_mask_keeps_blank_contributor_rows_even_if_banned_holds_a_blank_key(blank):
    """Layer 2, alone: even if a blank OR None key reached `banned` by some
    other route, a row with a blank contributor must still be KEPT.

    The None key matters: `normalise_contributor` maps every blank spelling to
    None, so a None-keyed ban is precisely what would collide with — and wipe —
    the whole anonymous population of a cell-week."""
    poisoned = {(CELL_A, 46, ""), (CELL_A, 46, "   "), (CELL_A, 46, None)}
    mask = keep_mask([CELL_A], [46], [blank], poisoned)
    assert mask == [True]


def test_keep_mask_still_drops_a_genuine_match():
    """Guards the previous test against a vacuously all-True mask."""
    banned = {(CELL_A, 46, "ole hansen")}
    mask = keep_mask(
        [CELL_A, CELL_A, CELL_A],
        [46, 46, 47],
        ["Ole Hansen", "Jens", "Ole Hansen"],
        banned,
    )
    assert mask == [False, True, True]


def test_keep_mask_is_empty_for_an_empty_table():
    assert keep_mask([], [], [], {(CELL_A, 46, "ole")}) == []


# --------------------------------------------------------------------------
# F4: layer 2's contribution to the COMPOSED path, made observable.
#
# Layer 1 (banned_keys) makes a None-contributor key unreachable through the
# public API, so no integration test can reach layer 2's blank-row branch —
# the reviewer reimplemented keep_mask's body inline inside
# drop_test_contributor_cells WITHOUT the blank clause and the whole suite
# stayed green. Monkeypatching banned_keys injects the key layer 1 refuses to
# produce, which is the only way to exercise layer 2 through the composition.
# --------------------------------------------------------------------------


def test_composed_guard_keeps_null_contributor_rows_against_a_none_key(monkeypatch):
    """Layer 2, THROUGH drop_test_contributor_cells.

    With `banned` poisoned by a None-contributor key, a table whose
    `recorded_by` is null must lose ZERO rows. Without layer 2's
    `normalise_contributor(name) is None or ...` clause, `normalise_contributor`
    of a null name IS None, so the key matches and the anonymous population is
    wiped — which is exactly the mutation that previously survived.
    """
    t = _one_anonymous_and_one_named()
    cell = t.column("cell")[0].as_py()
    monkeypatch.setattr(
        leakage_module, "banned_keys", lambda _recordings: {(cell, 46, None)}
    )
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == t.num_rows
    assert out.column("recorded_by").to_pylist() == [None, "Ole"]


def test_composed_guard_with_a_blank_string_key_keeps_anonymous_rows(monkeypatch):
    """Same, for the blank-string spelling of the poisoned key."""
    t = _one_anonymous_and_one_named()
    cell = t.column("cell")[0].as_py()
    monkeypatch.setattr(
        leakage_module,
        "banned_keys",
        lambda _recordings: {(cell, 46, ""), (cell, 46, "   ")},
    )
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == t.num_rows


def test_composed_guard_still_drops_a_real_match_when_banned_is_patched(monkeypatch):
    """Guards the two above against a vacuous pass: the monkeypatched path must
    still be able to remove a row, so an all-keep implementation fails here."""
    t = _one_anonymous_and_one_named()
    cell = t.column("cell")[0].as_py()
    monkeypatch.setattr(
        leakage_module, "banned_keys", lambda _recordings: {(cell, 46, "ole")}
    )
    out = drop_test_contributor_cells(t, [])
    assert out.column("recorded_by").to_pylist() == [None]


# --------------------------------------------------------------------------
# F1: the CELL is normalised on both sides.
#
# Controller-reproduced silent no-ops on a real res-4 table: an UPPERCASE cell,
# a whitespace-padded cell and a res-5 cell each kept the row that the guard
# should have removed — no exception, no log line, leak survives. Test-split
# metadata is caller-side CSV/JSON, which is exactly where uppercase and
# padded hex comes from, and h3.get_resolution accepts an uppercased cell so
# no validity check catches it.
# --------------------------------------------------------------------------


def _one_ole_at_cell_a_week_46():
    return assign_cells(
        _table([(1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole")]),
        resolution=4,
    )


def test_exact_cell_removes_the_row_control():
    """Control for the three variants below: the guard does work in the base case."""
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    assert cell == CELL_A
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 0


@pytest.mark.parametrize(
    "variant,label",
    [
        (lambda c: c.upper(), "UPPERCASE"),
        (lambda c: "  " + c + " ", "whitespace-padded"),
        (lambda c: c.title(), "mixed case"),
        (lambda c: "\u200b" + c, "zero-width-prefixed"),
    ],
)
def test_cell_spelling_variants_still_remove_the_row(variant, label):
    """Each of these was a TOTAL silent no-op before the cell was normalised."""
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=variant(cell), week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 0, f"{label} cell was a silent no-op"


def test_cell_normalisation_applies_to_the_occurrence_side_too():
    """Normalising only the test side is still a no-op if the table's cell is
    spelled differently. Simulated by handing keep_mask an uppercase table
    cell against a canonical banned key."""
    banned = {(CELL_A, 46, "ole")}
    assert keep_mask([CELL_A.upper()], [46], ["Ole"], banned) == [False]
    assert keep_mask(["  " + CELL_A + " "], [46], ["Ole"], banned) == [False]


def test_test_recording_stores_the_normalised_cell():
    """The value held must be the value compared — a frozen dataclass storing a
    raw cell while banned_keys normalises a different one is how the two sides
    drift apart."""
    rec = TestRecording(cell="  " + CELL_A.upper() + " ", week=46, recorded_by="Ole")
    assert rec.cell == CELL_A


def test_banned_keys_normalises_the_cell_for_duck_typed_callers():
    rec = SimpleNamespace(cell="  " + CELL_A.upper() + " ", week=46, recorded_by="Ole")
    assert banned_keys([rec]) == {(CELL_A, 46, "ole")}


def test_normalise_cell_is_case_and_whitespace_insensitive():
    assert normalise_cell(CELL_A.upper()) == CELL_A
    assert normalise_cell("  " + CELL_A + "\n") == CELL_A
    assert normalise_cell(None) is None


def test_cell_in_key_at_a_shared_week_keep_mask_layer():
    """F7: the cell component of the key was pinned by only ONE test at the
    keep_mask layer. Two different cells, SAME week, SAME contributor: only the
    banned cell's row may be dropped, so dropping `cell` from the key is caught
    here and not only end-to-end."""
    banned = {(CELL_A, 46, "ole")}
    assert keep_mask(
        [CELL_A, CELL_B], [46, 46], ["Ole", "Ole"], banned
    ) == [False, True]


def test_week_in_key_at_a_shared_cell_keep_mask_layer():
    """F7, the week component: same cell, same contributor, different weeks."""
    banned = {(CELL_A, 46, "ole")}
    assert keep_mask(
        [CELL_A, CELL_A], [46, 47], ["Ole", "Ole"], banned
    ) == [False, True]


# --------------------------------------------------------------------------
# F2: a cross-resolution cell must RAISE, not silently match nothing.
# --------------------------------------------------------------------------


def test_cross_resolution_test_cell_raises():
    """drop_test_contributor_cells is the only place where the test split's
    resolution and the assign_cells resolution are both in scope, so the
    invariant lives here. A res-5 cell against a res-4 table matches nothing at
    all — total silent leakage — and is never intentional."""
    t = _one_ole_at_cell_a_week_46()
    with pytest.raises(ValueError) as excinfo:
        drop_test_contributor_cells(
            t, [TestRecording(cell=CELL_A_RES5, week=46, recorded_by="Ole")]
        )
    message = str(excinfo.value)
    assert "resolution" in message
    assert "5" in message and "4" in message
    # names which side is which
    assert "test recording" in message and "occurrence table" in message


def test_cross_resolution_raises_through_apply_leakage_guards():
    t = _one_ole_at_cell_a_week_46()
    with pytest.raises(ValueError, match="resolution"):
        apply_leakage_guards(
            t, [TestRecording(cell=CELL_A_RES5, week=46, recorded_by="Ole")]
        )


def test_cross_resolution_raises_even_when_uppercased():
    """The resolution check must run on the normalised cell, so F1's casefold
    cannot let a mismatched cell slip past it."""
    t = _one_ole_at_cell_a_week_46()
    with pytest.raises(ValueError, match="resolution"):
        drop_test_contributor_cells(
            t, [TestRecording(cell=CELL_A_RES5.upper(), week=46, recorded_by="Ole")]
        )


def test_matching_resolution_does_not_raise():
    """Guards the three above against a check that always raises."""
    t = _one_ole_at_cell_a_week_46()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=CELL_B, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 1


def test_resolution_check_is_skipped_on_an_empty_table():
    """An empty table has no resolution to compare against; it must not raise."""
    t = assign_cells(_table([]), resolution=4)
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=CELL_A_RES5, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 0


def test_invalid_test_cell_raises_rather_than_matching_nothing():
    """A cell that is not an H3 index at all is a caller-side bug, and would
    otherwise be another silent no-op."""
    t = _one_ole_at_cell_a_week_46()
    with pytest.raises(ValueError, match="valid H3"):
        drop_test_contributor_cells(
            t, [TestRecording(cell="not-a-cell", week=46, recorded_by="Ole")]
        )


# --------------------------------------------------------------------------
# F3: a zero-effect guard must not be silent.
# --------------------------------------------------------------------------


def test_warns_when_a_non_empty_banned_set_matches_nothing(caplog):
    """The shared observable symptom of every remaining silent-failure mode:
    contributor-spelling miss, mojibake, an un-wired eval split. Without it, a
    guard that removes nothing produces no output at all."""
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    with caplog.at_level(logging.WARNING, logger="birdology.prior.leakage"):
        out = drop_test_contributor_cells(
            t, [TestRecording(cell=cell, week=46, recorded_by="Somebody Else")]
        )
    assert out.num_rows == 1
    messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("matched zero" in m for m in messages), messages
    assert any("1 banned" in m for m in messages), messages


def test_no_zero_match_warning_when_the_guard_removed_rows(caplog):
    """Guards the previous test against an always-on warning."""
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    with caplog.at_level(logging.WARNING, logger="birdology.prior.leakage"):
        drop_test_contributor_cells(
            t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
        )
    assert not [r for r in caplog.records if "matched zero" in r.getMessage()]


def test_no_zero_match_warning_when_the_banned_set_is_empty(caplog):
    """An empty banned set legitimately matches nothing; warning there would
    fire on every un-guarded build and train the operator to ignore it."""
    t = _one_ole_at_cell_a_week_46()
    with caplog.at_level(logging.WARNING, logger="birdology.prior.leakage"):
        drop_test_contributor_cells(t, [])
    assert not [r for r in caplog.records if "matched zero" in r.getMessage()]


# --------------------------------------------------------------------------
# F5: zero-width and soft-hyphen codepoints must not survive normalisation.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("invisible", ["\u200b", "\u200c", "\u200d", "\u00ad", "\ufeff"])
def test_invisible_codepoints_do_not_block_a_contributor_match(invisible):
    """NFKC does not strip Cf codepoints and str.split() does not treat them as
    whitespace, so 'Ole\\u200bHansen' could never match 'ole hansen'. GBIF
    recordedBy is free text and does carry these."""
    assert normalise_contributor("Ole" + invisible + " Hansen") == "ole hansen"
    assert normalise_contributor("Ole" + invisible + "Hansen") == "olehansen"


def test_zero_width_contributor_is_still_guarded_end_to_end():
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole\u200b Hansen"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Jens"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole Hansen")]
    )
    assert out.column("recorded_by").to_pylist() == ["Jens"]


def test_invisible_only_contributor_is_blank():
    """A name made only of invisible codepoints must normalise to None, not to
    a usable join key that would wipe an anonymous population."""
    assert normalise_contributor("\u200b\u00ad") is None
    with pytest.raises(ValueError, match="recorded_by"):
        TestRecording(cell=CELL_A, week=46, recorded_by="\u200b")


# --------------------------------------------------------------------------
# F6: week must not be silently coerced.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("week", [46.9, 46.0, "46", None, True])
def test_test_recording_rejects_non_integral_week(week):
    """int(46.9) silently bans week 46, so a caller-side units bug (a
    day-of-year, a string) would quietly guard the WRONG week."""
    with pytest.raises(ValueError, match="week"):
        TestRecording(cell=CELL_A, week=week, recorded_by="Ole")


def test_test_recording_stores_the_coerced_week():
    """__post_init__ used to validate int(self.week) and then throw it away, so
    banned_keys independently re-coerced the raw value: two derivations of one
    number is how the two sides drift."""
    rec = TestRecording(cell=CELL_A, week=46, recorded_by="Ole")
    assert rec.week == 46
    assert type(rec.week) is int


def test_test_recording_coerces_an_index_like_week_to_a_plain_int():
    """A week arriving as numpy.int8 (pyarrow/pandas test-split metadata) or any
    other __index__-able wrapper must be STORED as a plain int. If
    __post_init__ validates a coercion it does not keep, the dataclass holds a
    value that is not `== 46` and does not hash like 46, so equality,
    deduplication and the banned key all diverge from what was validated."""

    class Int8Like:
        def __init__(self, value: int) -> None:
            self._value = value

        def __index__(self) -> int:
            return self._value

    rec = TestRecording(cell=CELL_A, week=Int8Like(46), recorded_by="Ole")  # type: ignore[arg-type]
    assert rec.week == 46
    assert type(rec.week) is int
    assert hash(rec) == hash(TestRecording(cell=CELL_A, week=46, recorded_by="Ole"))
    assert rec == TestRecording(cell=CELL_A, week=46, recorded_by="Ole")


def test_normalise_week_accepts_integral_and_rejects_the_rest():
    assert normalise_week(46) == 46
    for bad in (46.9, 46.0, "46", None, True):
        with pytest.raises(ValueError, match="week"):
            normalise_week(bad)


def test_banned_keys_rejects_a_non_integral_week_for_duck_typed_callers():
    rec = SimpleNamespace(cell=CELL_A, week=46.9, recorded_by="Ole")
    with pytest.raises(ValueError, match="week"):
        banned_keys([rec])
