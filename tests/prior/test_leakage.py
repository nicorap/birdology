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
    result = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    guarded = result.table
    assert guarded.num_rows < t.num_rows
    assert XC not in guarded.column("publisher_key").to_pylist()
    # ...and the RESULT reports what each guard did, which is what
    # `scripts/build_prior.py` escalates on. MEASURED, not assumed: on THIS
    # fixture Ole's only row is ALSO the Xeno-Canto one, so guard 1 removes it
    # and guard 2 then finds nothing left to match -- which is a genuine
    # zero match for the split, and the counts say so rather than crediting
    # guard 2 with guard 1's work.
    assert result.xeno_canto_rows_removed == 1
    assert result.contributor_rows_removed == 0
    assert result.banned_key_count == 1
    assert result.zero_match is True


def test_no_test_recordings_is_a_noop_beyond_xc():
    t = assign_cells(
        _table([(1, 55.6, 12.5, date(2025, 3, 3), "pub", "Ole")]), resolution=4
    )
    result = apply_leakage_guards(t, [])
    assert result.table.num_rows == 1
    # An empty split is the legitimate `--no-test-split` mode: ZERO banned
    # keys, so `zero_match` is FALSE even though nothing was removed. That
    # distinction is the whole reason the count is returned rather than
    # re-derived from `len(test_recordings)` by the caller.
    assert result.banned_key_count == 0
    assert result.contributor_rows_removed == 0
    assert result.zero_match is False


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
    ).table
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
    ).table
    assert out.schema == t.schema


def test_guards_on_an_empty_table_are_a_noop():
    t = assign_cells(_table([]), resolution=4)
    out = apply_leakage_guards(
        t, [TestRecording(cell="841f059ffffffff", week=46, recorded_by="Ole")]
    ).table
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
    ).table
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


def _with_table_cell(table, text):
    """Rewrite the TABLE's `cell` value, keeping the schema untouched.

    The odd formatting has to sit on the OCCURRENCE side here: the test-side
    spellings are already pinned above, and `_assert_one_resolution` derives the
    table's resolution from this column alone.
    """
    index = table.schema.get_field_index("cell")
    field = table.schema.field(index)
    return table.set_column(
        index, field, pa.array([text] * table.num_rows, field.type)
    )


@pytest.mark.parametrize(
    "variant,label",
    [
        (lambda c: "\u200b" + c, "zero-width-prefixed"),
        (lambda c: c[:4] + "\u00ad" + c[4:], "soft-hyphen inside"),
        (lambda c: "  " + c.upper() + " ", "uppercase and padded"),
    ],
)
def test_resolution_derivation_normalises_the_table_cell(variant, label):
    """`_assert_one_resolution` must derive the resolution from the NORMALISED
    table cell, not the raw one.

    A table whose `cell` was not written by `assign_cells` — a hand-built or
    round-tripped table, caller-supplied metadata — can carry invisible
    formatting. `h3.get_resolution` tolerates case and ordinary whitespace but
    rejects Cf codepoints outright, so comparing the raw cell turns a row the
    guard should remove into a spurious "not a valid H3 index" ValueError. The
    guard must still remove the row, since `keep_mask` normalises the same cell.
    """
    t = _one_ole_at_cell_a_week_46()
    t = _with_table_cell(t, variant(CELL_A))
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=CELL_A, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 0, f"{label} table cell broke the guard"


def test_resolution_mismatch_is_detected_through_an_odd_table_cell():
    """The mismatch raise must survive odd table-cell formatting too: comparing
    the raw cell would report an H3-validity error instead of the resolution
    mismatch, which points the reader at the wrong side of the join."""
    t = _one_ole_at_cell_a_week_46()
    t = _with_table_cell(t, "\u200b" + CELL_A_RES5)
    with pytest.raises(ValueError) as excinfo:
        drop_test_contributor_cells(
            t, [TestRecording(cell=CELL_A, week=46, recorded_by="Ole")]
        )
    message = str(excinfo.value)
    assert "resolution" in message
    assert "4" in message and "5" in message


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
# C1: the zero-match condition is RETURNED DATA, not a log record.
#
# `scripts/build_prior.py` must FAIL a build whose supplied split matched
# nothing. It used to detect that by attaching a logging handler to this
# module's logger and string-matching the WARNING above; with
# `logging.basicConfig(level=logging.ERROR)` set before `main()` the guard was
# silently dead and a complete release was written with exit 0.
#
# These tests pin the DATA channel, and the last one pins it UNDER
# `logging.disable()` -- the strongest form of the configuration that killed
# the old mechanism.
# --------------------------------------------------------------------------


def test_the_guard_result_reports_zero_match_on_a_split_that_bites_nothing():
    """A non-empty split that removes nothing sets `zero_match`."""
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    result = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Somebody Else")]
    )
    assert result.banned_key_count == 1
    assert result.contributor_rows_removed == 0
    assert result.rows_before_contributor_guard == 1
    assert result.zero_match is True


def test_the_guard_result_does_not_report_zero_match_when_rows_were_removed():
    """Guards the previous test against an always-true `zero_match`."""
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    result = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert result.contributor_rows_removed == 1
    assert result.zero_match is False


def test_the_guard_result_does_not_report_zero_match_for_an_empty_split():
    """`--no-test-split` is legitimate and must not read as a zero match.

    Its own test rather than a branch of the one above: a `zero_match` defined
    as `contributor_rows_removed == 0` alone would be TRUE here, which would
    make the legitimate no-split mode unbuildable and the pressure would be to
    delete the guard rather than fix it.
    """
    result = apply_leakage_guards(_one_ole_at_cell_a_week_46(), [])
    assert result.banned_key_count == 0
    assert result.contributor_rows_removed == 0
    assert result.zero_match is False


def test_the_zero_match_signal_survives_logging_being_disabled_entirely():
    """THE C1 REGRESSION TEST: the signal is DATA and logging cannot reach it.

    `logging.disable(logging.CRITICAL)` suppresses every record at every
    level on every logger -- strictly stronger than the
    `logging.basicConfig(level=logging.ERROR)` that was reproduced disarming
    the old handler-based detector. The counts are computed by the function
    that filtered, so they are unchanged.

    The handler count is asserted too, so this cannot pass by the mechanism
    having quietly grown a second, log-based opinion: there is no handler to
    attach and none is left behind.
    """
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    recordings = [TestRecording(cell=cell, week=46, recorded_by="Nobody At All")]

    logging.disable(logging.CRITICAL)
    try:
        records_before = len(logging.getLogger("birdology.prior.leakage").handlers)
        result = apply_leakage_guards(t, recordings)
    finally:
        logging.disable(logging.NOTSET)

    assert result.zero_match is True
    assert result.banned_key_count == 1
    assert result.contributor_rows_removed == 0
    # No handler was added or left behind: the mechanism does not touch logging
    # at all, so there is nothing to leak into the rest of the process.
    assert len(logging.getLogger("birdology.prior.leakage").handlers) == records_before


def test_the_zero_match_signal_survives_the_module_logger_being_renamed(monkeypatch):
    """The other half of C1: the old detector hard-coded the LOGGER NAME.

    Renaming this module's logger -- one letter -- bypassed the guard with
    nothing raised, because the caller attached its handler to
    `"birdology.prior.leakage"` by string. Nothing the data path does depends
    on the logger's identity, so rebinding it to a logger nobody is listening
    to changes no count.
    """
    monkeypatch.setattr(
        leakage_module, "logger", logging.getLogger("somewhere.else.entirely")
    )
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    result = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Nobody At All")]
    )
    assert result.zero_match is True
    assert result.banned_key_count == 1


def test_the_banned_key_count_is_KEYS_not_the_recording_count():
    """Two recordings that normalise to ONE key report one key, not two.

    The count is what the guard HELD, which is what decides whether it could
    bite. A caller re-deriving intent from `len(test_recordings)` -- which is
    what the old `and test_recordings` conjunct did -- would be counting
    something else, and a split whose rows all collided would be reported as
    two keys where the guard only ever had one.
    """
    t = _one_ole_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    result = apply_leakage_guards(
        t,
        [
            TestRecording(cell=cell, week=46, recorded_by="Ole"),
            # Same key after normalisation: casefold + whitespace collapse.
            TestRecording(cell=cell.upper(), week=46, recorded_by="  OLE  "),
        ],
    )
    assert result.banned_key_count == 1


def test_the_guard_result_separates_the_two_guards_row_counts():
    """One mutant per guard needs one count per guard.

    A single `rows_removed` total cannot tell a Xeno-Canto drop from a
    contributor drop, so an escalation on "the split matched nothing" would be
    satisfied by the XC guard having removed something entirely unrelated.
    """
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
    result = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert result.xeno_canto_rows_removed == 1
    assert result.contributor_rows_removed == 1
    # The contributor guard saw the POST-XC table, so its denominator is 2.
    assert result.rows_before_contributor_guard == 2
    assert result.table.num_rows == 1


def test_the_guard_result_is_not_a_table_so_an_old_caller_fails_loudly():
    """Widening a return value must BREAK old call sites, not accommodate them.

    `apply_leakage_guards` used to return a `pa.Table`. If the new result were
    a tuple or a table subclass, a caller that never learned about the counts
    would keep working and the escalation would be silently absent -- which is
    exactly how the log-scraping mechanism survived three review passes. A
    `LeakageGuardResult` has no `num_rows`, so such a caller raises at the
    line that needs changing.
    """
    result = apply_leakage_guards(_one_ole_at_cell_a_week_46(), [])
    assert not isinstance(result, pa.Table)
    with pytest.raises(AttributeError):
        getattr(result, "num_rows")  # noqa: B009 - the point is that it raises
    assert isinstance(result.table, pa.Table)


# --------------------------------------------------------------------------
# C1 (final gate): the PER-KEY match count.
#
# `zero_match` is a claim about the WHOLE split, and ONE live key satisfies it
# for an arbitrarily large one. Reproduced: a 100-key split with one real
# contributor and ninety-nine Sydney coordinates built five artifacts, while
# the all-dead control raised. `matched_key_count` is the quantity that makes
# the partial case visible; these tests pin it independently of the build
# script, which owns the escalation POLICY.
# --------------------------------------------------------------------------


def _three_contributors_at_cell_a_week_46():
    """Three named contributors sharing one (cell, week)."""
    return assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Jens"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Mette"),
            ]
        ),
        resolution=4,
    )


def test_matched_key_count_is_ONE_when_one_of_three_keys_bites():
    """THE C1 FIXTURE, in miniature: a partial split reports a partial count.

    Two of the three banned contributors appear nowhere in the table. The
    guard removes a row, so `contributor_rows_removed` is non-zero and
    `zero_match` is FALSE -- which is exactly the state in which the total
    condition released the reproduced 1-of-100 split. `matched_key_count`
    says one, and that is the only quantity here that distinguishes this from
    a split that worked.
    """
    t = _three_contributors_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    result = apply_leakage_guards(
        t,
        [
            TestRecording(cell=cell, week=46, recorded_by="Ole"),
            TestRecording(cell=cell, week=46, recorded_by="nobody-one"),
            TestRecording(cell=cell, week=46, recorded_by="nobody-two"),
        ],
    )
    assert result.banned_key_count == 3
    assert result.matched_key_count == 1
    assert result.contributor_rows_removed == 1
    # The total condition is satisfied by that one live key -- which is the
    # defect. Asserted so the fixture is known to exercise the gap.
    assert result.zero_match is False
    assert result.match_rate == pytest.approx(1 / 3)


def test_matched_key_count_equals_the_banned_count_when_every_key_bites():
    """Guards the previous test against an always-partial count."""
    t = _three_contributors_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    result = apply_leakage_guards(
        t,
        [
            TestRecording(cell=cell, week=46, recorded_by=who)
            for who in ("Ole", "Jens", "Mette")
        ],
    )
    assert result.banned_key_count == 3
    assert result.matched_key_count == 3
    assert result.match_rate == 1.0
    assert result.contributor_rows_removed == 3


def test_matched_key_count_is_ZERO_when_nothing_bites():
    t = _three_contributors_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    result = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Nobody At All")]
    )
    assert result.matched_key_count == 0
    assert result.match_rate == 0.0
    assert result.zero_match is True


def test_matched_key_count_counts_KEYS_not_ROWS():
    """One key removing three rows is ONE match, not three.

    Its own test because the cheap wrong implementation -- counting removed
    rows, or incrementing per matching row -- is numerically identical on
    every one-row-per-key fixture above, and makes the rate exceed 1.0 on a
    real archive where a contributor has many occurrences in one cell-week.
    """
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
                (2, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
                (3, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    result = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert result.contributor_rows_removed == 3
    assert result.banned_key_count == 1
    assert result.matched_key_count == 1
    assert result.match_rate == 1.0


def test_matched_key_count_never_exceeds_the_banned_key_count():
    """The rate is a fraction, and the invariant that makes it one.

    Asserted on the fixture where over-counting is easiest: three rows, one
    key. A `matched_key_count` above `banned_key_count` would give a rate
    above 1 and no threshold comparison would mean anything.
    """
    t = _three_contributors_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    for recordings in (
        [TestRecording(cell=cell, week=46, recorded_by="Ole")],
        [
            TestRecording(cell=cell, week=46, recorded_by=who)
            for who in ("Ole", "Jens", "Mette", "Nobody")
        ],
    ):
        result = apply_leakage_guards(t, recordings)
        assert result.matched_key_count <= result.banned_key_count
        assert 0.0 <= result.match_rate <= 1.0


def test_match_rate_RAISES_for_an_empty_split_rather_than_returning_a_number():
    """`--no-test-split` has no rate, and 0/0 has no honest value.

    Returning 0.0 would make the legitimate no-split mode fail every rate
    check; returning 1.0 would claim a split that does not exist worked
    perfectly. Both fold together the two states this whole class exists to
    keep apart, so the property refuses instead.
    """
    result = apply_leakage_guards(_one_ole_at_cell_a_week_46(), [])
    assert result.banned_key_count == 0
    with pytest.raises(ValueError, match="match_rate is undefined"):
        result.match_rate


def test_a_blank_contributor_row_never_contributes_a_match():
    """Layer 2 short-circuits BEFORE the key is formed, so it cannot match.

    Without this, a blank-contributor row could be credited to a poisoned
    blank key and inflate `matched_key_count` -- reporting a healthy rate for
    a split whose only "matches" were the anonymous population it must never
    touch.
    """
    t = _one_anonymous_and_one_named()
    cell = t.column("cell")[0].as_py()
    keep, matched = leakage_module._mask_and_matched_keys(
        t.column("cell").to_pylist(),
        t.column("week").to_pylist(),
        t.column("recorded_by").to_pylist(),
        {(cell, 46, None), (cell, 46, "")},
    )
    assert keep == [True, True]
    assert matched == set()


def test_the_matched_key_set_and_the_row_mask_are_ONE_traversal():
    """The mask and the matched set must agree, by construction.

    `keep_mask` is the projection of `_mask_and_matched_keys`; a second,
    independent traversal applying the same three normalisations is how the
    number a guard escalates on and the rows it removed come to disagree.
    Asserted behaviourally: every False in the mask corresponds to a key in
    the matched set, and the matched set holds nothing the mask kept.
    """
    t = _three_contributors_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    cells = t.column("cell").to_pylist()
    weeks = t.column("week").to_pylist()
    names = t.column("recorded_by").to_pylist()
    banned = {(cell, 46, "ole"), (cell, 46, "mette"), (cell, 46, "nobody")}

    keep, matched = leakage_module._mask_and_matched_keys(
        cells, weeks, names, banned
    )
    assert keep == leakage_module.keep_mask(cells, weeks, names, banned)
    dropped = {
        (normalise_cell(c), normalise_week(w), normalise_contributor(n))
        for c, w, n, k in zip(cells, weeks, names, keep)
        if not k
    }
    assert matched == dropped
    assert matched <= banned


def test_a_partial_match_is_WARNED_about_for_the_human(caplog):
    """The log line beside the data channel. It is NOT the control flow.

    `scripts/build_prior.py` escalates on `match_rate`; this warning exists
    so a human reading a build log sees the same fact. Deleting it deafens
    the log without disarming anything, which is the arrangement C1 settled
    on for the zero-match case and is kept here for the same reason.
    """
    t = _three_contributors_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    with caplog.at_level(logging.WARNING, logger="birdology.prior.leakage"):
        apply_leakage_guards(
            t,
            [
                TestRecording(cell=cell, week=46, recorded_by="Ole"),
                TestRecording(cell=cell, week=46, recorded_by="nobody-one"),
            ],
        )
    messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("only 1 of 2 banned" in m for m in messages), messages


def test_no_partial_match_warning_when_every_key_bit(caplog):
    """Guards the previous test against an always-on warning."""
    t = _three_contributors_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    with caplog.at_level(logging.WARNING, logger="birdology.prior.leakage"):
        apply_leakage_guards(
            t,
            [
                TestRecording(cell=cell, week=46, recorded_by=who)
                for who in ("Ole", "Jens", "Mette")
            ],
        )
    assert not [r for r in caplog.records if "matched any occurrence" in r.getMessage()]


def test_the_match_rate_survives_logging_being_disabled_entirely():
    """C1's rule applied to the new clause: it is DATA, not a log record.

    The partial case has a WARNING too, and a caller that scraped it would
    reintroduce the exact defect the returned counts replaced. Under
    `logging.disable(CRITICAL)` every count is unchanged.
    """
    t = _three_contributors_at_cell_a_week_46()
    cell = t.column("cell")[0].as_py()
    recordings = [
        TestRecording(cell=cell, week=46, recorded_by="Ole"),
        TestRecording(cell=cell, week=46, recorded_by="nobody-one"),
        TestRecording(cell=cell, week=46, recorded_by="nobody-two"),
    ]
    logging.disable(logging.CRITICAL)
    try:
        result = apply_leakage_guards(t, recordings)
    finally:
        logging.disable(logging.NOTSET)
    assert result.matched_key_count == 1
    assert result.banned_key_count == 3
    assert result.match_rate == pytest.approx(1 / 3)


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


def test_keep_mask_routes_the_week_column_through_normalise_week():
    """Pin the REQUIREMENT half of keep_mask's documented week CONTRACT.

    test_normalise_week_accepts_integral_and_rejects_the_rest pins the helper in
    isolation but never proves keep_mask actually routes its column through it.
    Without this, a keep_mask that used int(week) directly would pass the whole
    suite while silently truncating a units bug.

    Mutant E/M38 (normalise_week -> int) makes this killable rather than
    decorative: int(None) raises TypeError, not ValueError, so the assertion
    cannot quietly rot.
    """
    banned = {(CELL_A, 46, "ole")}
    for bad_week in (None, 46.9):
        with pytest.raises(ValueError, match="week"):
            keep_mask([CELL_A], [bad_week], ["Ole"], banned)  # type: ignore[list-item]


def test_banned_keys_rejects_a_non_integral_week_for_duck_typed_callers():
    rec = SimpleNamespace(cell=CELL_A, week=46.9, recorded_by="Ole")
    with pytest.raises(ValueError, match="week"):
        banned_keys([rec])
