"""Keep the answers out of the prior.

Xeno-Canto publishes its recordings to GBIF as occurrences. If a test
recording's own occurrence record reaches the prior, the prior trivially
"knows" the species, and every reported gain is leakage. Two guards:

1. Drop every occurrence published by Xeno-Canto.
2. Drop every occurrence sharing a (cell, week) with a test recording from the
   same contributor — the same person's other records from the same outing.

Both guards MUST run on the ``assign_cells`` output, before
``aggregate_counts``, which discards ``publisher_key`` and ``recorded_by``.

Every component of guard 2's join key — cell, week, contributor — is normalised
on BOTH sides before comparison. A component normalised on only one side is not
a partial failure but a TOTAL one: no key matches, no row is removed, no
exception is raised and no log line is emitted, so the leak simply survives.
The same applies across H3 resolutions, which cannot be normalised into
agreement, so a mismatch raises.

Publisher key verified against the GBIF registry on 2026-09-29::

    curl -s "https://api.gbif.org/v1/dataset/search?q=xeno-canto&limit=5"
    1f00d75c-f6fc-4224-a595-975e82d7689c | Xeno-canto - Soundscapes ...
    1f00d75c-f6fc-4224-a595-975e82d7689c | Xeno-canto - Bird sounds ...
    curl -s "https://api.gbif.org/v1/organization/1f00d75c-f6fc-4224-a595-975e82d7689c"
    -> {"title": "Xeno-canto Foundation for Nature Sounds", "country": "NL"}

All five Xeno-Canto datasets share one publishing organisation. The key
``e2e717bf-551a-4917-bdc9-4fa0f342c530`` named in the task brief belongs to the
**Cornell Lab of Ornithology**, not Xeno-Canto (verified via the same
``/v1/organization`` endpoint on 2026-09-29), so it is NOT used: it would have
leaked every Xeno-Canto occurrence while deleting the entire eBird population.
"""
from __future__ import annotations

import logging
import operator
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from typing import SupportsIndex

import h3
import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)

#: Xeno-canto Foundation for Nature Sounds, publishing organisation on GBIF.
#: Covers its bird, soundscape, Chiroptera, Anura and Orthoptera datasets.
XENO_CANTO_PUBLISHER_KEYS = frozenset({"1f00d75c-f6fc-4224-a595-975e82d7689c"})

#: The replacement character. ``read_occurrences`` decodes the archive with
#: ``errors="replace"``, so any non-UTF-8 byte in a contributor name becomes
#: U+FFFD and one contributor can appear under two spellings.
REPLACEMENT_CHARACTER = "\ufffd"


def _strip_format_characters(text: str) -> str:
    """Drop Unicode ``Cf`` (format) codepoints.

    NFKC leaves U+200B ZERO WIDTH SPACE, U+200C ZWNJ, U+00AD SOFT HYPHEN and
    friends intact, and ``str.split()`` does not treat them as whitespace, so
    ``"Ole\\u200bHansen"`` could never match ``"Ole Hansen"``. GBIF
    ``recordedBy`` is free text and does carry these. Removing them is the
    over-match direction, which is the recoverable one for a leakage guard.
    """
    return "".join(ch for ch in text if unicodedata.category(ch) != "Cf")


def normalise_contributor(name: str | None) -> str | None:
    """Canonical form of a contributor name for leakage comparison.

    A leakage guard is asymmetric: a MISSED match leaves a leaking occurrence
    in the prior, which is the exact failure the guard exists to prevent, while
    an over-match only removes extra data. So this normalisation is deliberately
    aggressive — NFKC, casefold, drop zero-width/format codepoints, collapse all
    whitespace runs to one space, and strip — to make near-spellings of one
    contributor collide.

    Returns ``None`` for ``None`` and for blank/whitespace-only input, so a
    blank contributor can never be used as a join key.
    """
    if name is None:
        return None
    folded = unicodedata.normalize("NFKC", name).casefold()
    collapsed = " ".join(_strip_format_characters(folded).split())
    return collapsed or None


def normalise_cell(cell: str | None) -> str | None:
    """Canonical form of an H3 cell index for leakage comparison.

    Every component of the join key must be normalised on BOTH sides, not just
    the contributor: a cell that differs only in case or surrounding whitespace
    is a TOTAL silent no-op for the guard — no exception, no log line, and the
    leaking occurrence stays in the prior. Test-split metadata arrives as
    caller-side CSV/JSON, which is exactly where uppercase or padded hex comes
    from, and ``h3.get_resolution`` happily accepts an uppercased cell, so no
    validity check would catch it.

    H3 indexes are hexadecimal, hence case-insensitive, so casefold is safe.
    Returns ``None`` only for ``None`` (a null cell in a nullable column); a
    blank cell folds to ``""``, which :func:`drop_test_contributor_cells`
    rejects loudly as an invalid H3 index.
    """
    if cell is None:
        return None
    return _strip_format_characters(cell).strip().casefold()


def normalise_week(week: object) -> int:
    """Exact integer week, or raise.

    ``int(46.9)`` silently truncates to 46, so a caller-side units bug (a
    fractional day-of-year, a string) would quietly ban the WRONG week instead
    of failing. ``operator.index`` accepts only genuinely integral values, so
    the coercion cannot lose information.
    """
    if isinstance(week, bool):
        raise ValueError(f"week must be an integer in 1..52, got {week!r}")
    if not isinstance(week, SupportsIndex):
        raise ValueError(
            f"week must be an integer in 1..52, not {type(week).__name__} "
            f"{week!r} (int() would silently truncate a units bug)"
        )
    return operator.index(week)


@dataclass(frozen=True)
class TestRecording:
    """A recording in an evaluation split, as (where, when, who).

    Every key component is normalised at construction — cell via
    :func:`normalise_cell`, week via :func:`normalise_week`, and the contributor
    is validated with :func:`normalise_contributor` — so the value stored is the
    value compared. Normalising only some components leaves the rest as silent
    no-ops: an uppercase or whitespace-padded cell matches nothing, removes no
    rows, logs nothing, and the leak survives.

    ``recorded_by`` must be non-blank: a blank contributor used as a join key
    would match every anonymous occurrence in the cell-week and silently delete
    the whole anonymous population there. Arrow's null semantics protect the
    occurrence side (``pc.equal(null, x)`` is null, never true) but not this
    side, because these values come from recording metadata rather than from
    ``read_occurrences``.
    """

    __test__ = False  # not a pytest test class, despite the name

    cell: str
    week: int
    recorded_by: str

    def __post_init__(self) -> None:
        if not isinstance(self.cell, str) or not self.cell.strip():
            raise ValueError("TestRecording.cell must be a non-blank string")
        cell = normalise_cell(self.cell)
        try:
            week = normalise_week(self.week)
        except ValueError as exc:
            raise ValueError(f"TestRecording.{exc}") from exc
        if not 1 <= week <= 52:
            raise ValueError(
                f"TestRecording.week must be in 1..52 (week 53 folds into 52), got {week!r}"
            )
        if normalise_contributor(self.recorded_by) is None:
            raise ValueError(
                "TestRecording.recorded_by must be a non-blank contributor name; "
                "a blank one would match every anonymous occurrence in the cell-week"
            )
        # Frozen dataclass: store the canonical forms so the value compared is
        # the value held, never a second, differently-derived one.
        object.__setattr__(self, "cell", cell)
        object.__setattr__(self, "week", week)


def drop_xeno_canto(table: pa.Table) -> pa.Table:
    """Remove occurrences published by Xeno-Canto.

    ``pc.is_in(null, value_set)`` returns false, not null, so a null-publisher
    row is KEPT — the correct direction: an unattributed record is not evidence
    of Xeno-Canto provenance.
    """
    keys = pa.array(sorted(XENO_CANTO_PUBLISHER_KEYS), pa.string())
    mask = pc.invert(pc.is_in(table.column("publisher_key"), value_set=keys))
    kept = table.filter(mask)
    if kept.num_rows != table.num_rows:
        logger.info(
            "drop_xeno_canto: removed %d of %d occurrences",
            table.num_rows - kept.num_rows,
            table.num_rows,
        )
    return kept


def banned_keys(
    test_recordings: Iterable[TestRecording],
) -> set[tuple[str, int, str]]:
    """Normalised (cell, week, contributor) keys to exclude.

    EVERY key component is normalised here, not just the contributor: the cell
    via :func:`normalise_cell` and the week via :func:`normalise_week`. A
    duck-typed caller (or a ``TestRecording`` whose ``__post_init__`` was
    bypassed) can still hand over a raw uppercase or padded cell, and a cell
    normalised on only one side is a total silent no-op.

    Layer 1 of the blank-contributor defence: a recording whose contributor
    normalises to ``None`` NEVER becomes a key. Exposed separately from
    :func:`keep_mask` so each layer can be tested — and mutated — on its own;
    together they are redundant, and redundancy that cannot be observed is
    indistinguishable from a bug.
    """
    banned: set[tuple[str, int, str]] = set()
    for recording in test_recordings:
        who = normalise_contributor(recording.recorded_by)
        if who is None:
            # Unreachable via TestRecording.__post_init__; belt-and-braces for
            # duck-typed callers, because the failure mode is mass deletion.
            logger.warning(
                "skipping test recording with blank contributor in cell %s week %s",
                recording.cell,
                recording.week,
            )
            continue
        cell = normalise_cell(recording.cell)
        if cell is None:
            raise ValueError("test recording cell must not be None")
        banned.add((cell, normalise_week(recording.week), who))
    return banned


def keep_mask(
    cells: list[str],
    weeks: list[int],
    names: list[str | None],
    banned: set[tuple[str, int, str]],
) -> list[bool]:
    """Row mask: True to keep. Layer 2 of the blank-contributor defence.

    Both the cell and the contributor are normalised on THIS, the occurrence,
    side with exactly the transforms :func:`banned_keys` applies on the test
    side, so the two are compared in the same space.

    A row with a null/blank ``recorded_by`` is ALWAYS kept, independently of
    what is in *banned*. This holds even if a blank key reached *banned* by
    some other route, so an anonymous population can never be wiped wholesale.

    CONTRACT — *weeks* must contain non-null, integral values. Each one goes
    through :func:`normalise_week`, which RAISES ``ValueError`` on ``None`` or a
    non-integral value rather than coercing it, because ``int(46.9)`` would
    silently ban the wrong week. ``assign_cells`` guarantees this: it writes
    ``week`` as ``pa.int8()`` derived from a non-null ``event_date``, and
    ``read_occurrences`` already drops rows without a day-precision date. A
    table not produced by ``assign_cells`` can therefore turn a null week into a
    hard failure whose message talks about units bugs; normalise or drop such
    rows before calling this, and do NOT loosen :func:`normalise_week`.
    """
    return [
        normalise_contributor(name) is None
        or (normalise_cell(cell), normalise_week(week), normalise_contributor(name))
        not in banned
        for cell, week, name in zip(cells, weeks, names)
    ]


def _assert_one_resolution(
    table_cells: list[str | None], banned: set[tuple[str, int, str]]
) -> None:
    """Raise if the banned cells are not at the table's H3 resolution.

    :func:`drop_test_contributor_cells` is the ONLY place in the codebase where
    the test-split resolution and the ``assign_cells`` resolution are
    simultaneously in scope, so the invariant has to live here. A res-5 test
    cell against a res-4 table matches nothing at all: no exception, no log
    line, and every occurrence that should have been excluded stays in the
    prior. That is total silent leakage, never intentional, so it RAISES rather
    than warns.

    Cost is O(|banned| + 1) on a ``to_pylist()`` the caller already paid for.
    """
    table_resolution: int | None = None
    for cell in table_cells:
        if cell is not None:
            normalised = normalise_cell(cell)
            try:
                table_resolution = h3.get_resolution(normalised)
            except Exception as exc:  # noqa: BLE001 - h3 raises several types
                raise ValueError(
                    f"occurrence table cell {normalised!r} is not a valid H3 "
                    f"index: {exc}"
                ) from exc
            break
    if table_resolution is None:
        return  # empty table (or all-null cells): nothing to compare against

    for cell, _week, _who in sorted(banned):
        try:
            banned_resolution = h3.get_resolution(cell)
        except Exception as exc:  # noqa: BLE001 - h3 raises several types
            raise ValueError(
                f"test recording cell {cell!r} is not a valid H3 index: {exc}"
            ) from exc
        if banned_resolution != table_resolution:
            raise ValueError(
                "H3 resolution mismatch between the test split and the "
                f"occurrence table: test recording cell {cell!r} is resolution "
                f"{banned_resolution}, occurrence table cells are resolution "
                f"{table_resolution}. Cells are not comparable across "
                "resolutions, so the leakage guard would silently match "
                "nothing and every leaking occurrence would survive."
            )


def drop_test_contributor_cells(
    table: pa.Table, test_recordings: Iterable[TestRecording]
) -> pa.Table:
    """Remove rows matching a test recording's (cell, week, contributor).

    Every key component is normalised on BOTH sides before the join — cell via
    :func:`normalise_cell`, week via :func:`normalise_week`, contributor via
    :func:`normalise_contributor`. A row whose ``recorded_by`` is null can
    never match, since a blank key is rejected at ``TestRecording`` construction.

    Raises ``ValueError`` if the test split's H3 resolution differs from the
    table's, which would make the whole guard a silent no-op.

    CONTRACT — *table* must carry a non-null, integral ``week`` column, as
    written by ``assign_cells`` (``pa.int8()`` from a non-null ``event_date``).
    :func:`keep_mask` routes every week through :func:`normalise_week`, which
    raises rather than coercing, so a null or fractional week from a table built
    some other way is a hard build failure, not a silently mis-banned week.
    """
    banned = banned_keys(test_recordings)
    if not banned:
        return table

    cells = table.column("cell").to_pylist()
    weeks = table.column("week").to_pylist()
    who_raw = table.column("recorded_by").to_pylist()

    _assert_one_resolution(cells, banned)

    flagged = sum(
        1 for name in who_raw if name is not None and REPLACEMENT_CHARACTER in name
    )
    if flagged:
        logger.warning(
            "drop_test_contributor_cells: %d occurrence(s) carry U+FFFD in "
            "recorded_by; their contributor names were mojibaked on decode and "
            "may not match a test recording exactly",
            flagged,
        )

    keep = keep_mask(cells, weeks, who_raw, banned)
    kept = table.filter(pa.array(keep, pa.bool_()))
    if kept.num_rows != table.num_rows:
        logger.info(
            "drop_test_contributor_cells: removed %d of %d occurrences "
            "over %d banned (cell, week, contributor) keys",
            table.num_rows - kept.num_rows,
            table.num_rows,
            len(banned),
        )
    elif banned:
        # A non-empty banned set that matches NOTHING used to be completely
        # silent. That one line is the shared observable symptom of every
        # remaining silent-failure mode in this module — contributor-spelling
        # miss, mojibake, an un-wired eval split — so it fires unconditionally
        # rather than trying to diagnose which one it was. An EMPTY banned set
        # legitimately matches nothing and returned earlier, so it never warns.
        logger.warning(
            "drop_test_contributor_cells: %d banned (cell, week, contributor) "
            "key(s) matched zero of %d occurrences; check the H3 resolution and "
            "cell formatting of the test split",
            len(banned),
            table.num_rows,
        )
    return kept


def apply_leakage_guards(
    table: pa.Table, test_recordings: Iterable[TestRecording]
) -> pa.Table:
    """Both guards, in order."""
    return drop_test_contributor_cells(drop_xeno_canto(table), test_recordings)
