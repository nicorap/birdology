"""Keep the answers out of the prior.

Xeno-Canto publishes its recordings to GBIF as occurrences. If a test
recording's own occurrence record reaches the prior, the prior trivially
"knows" the species, and every reported gain is leakage. Two guards:

1. Drop every occurrence published by Xeno-Canto.
2. Drop every occurrence sharing a (cell, week) with a test recording from the
   same contributor — the same person's other records from the same outing.

Both guards MUST run on the ``assign_cells`` output, before
``aggregate_counts``, which discards ``publisher_key`` and ``recorded_by``.

THE GUARDS REPORT THEIR OWN COUNTS
----------------------------------
:func:`apply_leakage_guards` returns a :class:`LeakageGuardResult`, not a
table: the guarded table plus the banned-key count and the rows each guard
removed. That is not convenience. A caller that must FAIL when a supplied
split matched nothing can only do so on data; reading it off a log WARNING
made the guard depend on logging configuration, and one
``logging.basicConfig(level=logging.ERROR)`` disarmed it silently while a
complete release was written with exit 0. See :class:`LeakageGuardResult`.

THE GUARD MARKER
----------------
:func:`apply_leakage_guards` stamps its output with the Arrow schema-metadata
key :data:`GUARD_MARKER_KEY`, and ``effort.target_group_ratios`` REFUSES a
table that does not carry it. A guarded and an unguarded table are
structurally identical, so that refusal is the only mechanical way to know
which one reached the effort denominator; see :data:`GUARD_MARKER_KEY` for
what the unguarded denominator does to the prior.

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
from typing import SupportsIndex, cast

import h3
import pyarrow as pa
import pyarrow.compute as pc

from .arrow_strings import text_to_pylist, value_type

logger = logging.getLogger(__name__)

#: Xeno-canto Foundation for Nature Sounds, publishing organisation on GBIF.
#: Covers its bird, soundscape, Chiroptera, Anura and Orthoptera datasets.
XENO_CANTO_PUBLISHER_KEYS = frozenset({"1f00d75c-f6fc-4224-a595-975e82d7689c"})

#: The replacement character. ``read_occurrences`` decodes the archive with
#: ``errors="replace"``, so any non-UTF-8 byte in a contributor name becomes
#: U+FFFD and one contributor can appear under two spellings.
REPLACEMENT_CHARACTER = "\ufffd"

#: Arrow schema-metadata key stamped on a table that has been through BOTH
#: guards, in order.
#:
#: A guarded and an unguarded occurrence table are STRUCTURALLY IDENTICAL --
#: same columns, same dtypes, same nullability -- so no check anywhere
#: downstream can tell them apart by inspection. The failure that makes this
#: worth a marker is the quietest in the plan: an unguarded table reaching
#: ``effort.target_group_ratios`` leaves the leaked rows in the DENOMINATOR,
#: which under-weights every guarded cell-week uniformly and yields a smooth,
#: plausible, mildly-flat prior with no error, no nan and no missing row.
#: Invisible in the output and invisible in the paper.
#:
#: The marker lives here because this module is what "guarded" MEANS. It is a
#: convention rather than a type, so it is not proof against a caller who
#: stamps by hand -- the type-level form (a ``GuardedCounts`` wrapper) would
#: be -- but it does close the accidental path, which is the one that has
#: actually occurred.
GUARD_MARKER_KEY = b"leakage_guards_applied"
GUARD_MARKER_VALUE = b"1"


#: The two contributor-join modes guard 2 can run in. EXPLICIT, never
#: auto-detected: a guard that silently switched key shapes on a heuristic
#: ("none of these contributors look like observer IDs") would be deciding
#: the leakage semantics of a released artifact from the data it is
#: guarding. The operator states the split's nature; the guard enforces it.
#:
#: ``match`` -- the original key, (cell, week, contributor): ban the test
#: contributor's own other records from the same outing.
#:
#: ``placeholder`` -- the split's ``recorded_by`` values are TEST-SET NAMES,
#: not people (job 5345: soundscape splits carry ``HSN`` / ``NES`` because
#: the recordings have no contributor, and an eBird observer ID can never
#: equal one, so the match-mode join is STRUCTURALLY empty). The banned key
#: is (cell, week): every occurrence in a test site's cell-week is removed
#: regardless of who recorded it -- including anonymous rows that match
#: mode always keeps. That is MORE conservative than contributor matching,
#: which is the right direction for a leakage guard.
CONTRIBUTOR_MODES = ("match", "placeholder")


def _validate_contributor_mode(contributor_mode: str) -> str:
    """Reject an unknown mode rather than falling back to either known one.

    Falling back to ``match`` would silently restore the structural
    zero-match on a placeholder split; falling back to ``placeholder`` would
    silently widen the ban. Both are the silent repair at a boundary this
    plan forbids.
    """
    if contributor_mode not in CONTRIBUTOR_MODES:
        raise ValueError(
            f"contributor_mode must be one of {CONTRIBUTOR_MODES}, got "
            f"{contributor_mode!r}. The mode decides the leakage join key "
            "((cell, week, contributor) vs (cell, week)) and must be stated "
            "explicitly; guessing it from the split's contents would let a "
            "typo change which rows a released artifact excluded"
        )
    return contributor_mode


def key_shape(contributor_mode: str) -> str:
    """The banned-key shape a mode bans, for error and log messages.

    One derivation, used by the guard's own log lines and by the build
    script's escalation messages, so an operator reading either is told the
    key the join ACTUALLY used -- a message naming (cell, week, contributor)
    over a (cell, week) refusal would send them triaging a contributor join
    that never ran.
    """
    _validate_contributor_mode(contributor_mode)
    return (
        "(cell, week)"
        if contributor_mode == "placeholder"
        else "(cell, week, contributor)"
    )


def mark_guards_applied(table: pa.Table) -> pa.Table:
    """Stamp *table* as having been through both guards.

    Schema metadata is REPLACED wholesale by ``replace_schema_metadata``, so
    any existing keys are merged in rather than dropped: a caller that carries
    its own metadata must not lose it to a provenance stamp.
    """
    metadata = dict(table.schema.metadata or {})
    metadata[GUARD_MARKER_KEY] = GUARD_MARKER_VALUE
    return table.replace_schema_metadata(metadata)


def guards_applied(table: pa.Table) -> bool:
    """Whether *table* carries the guard marker.

    The VALUE is checked, not merely the key's presence: a key mapped to
    something else is not this module's stamp, and treating it as one would
    let an unrelated metadata convention authenticate an unguarded table.
    """
    metadata = table.schema.metadata or {}
    return metadata.get(GUARD_MARKER_KEY) == GUARD_MARKER_VALUE


#: The WARNING `drop_test_contributor_cells` emits when a NON-EMPTY banned
#: set matches nothing at all. It is the shared observable symptom of every
#: remaining silent-failure mode in this module, and it exists for the HUMAN
#: reading a build log.
#:
#: IT IS NOT A CONTROL-FLOW CHANNEL, AND NOTHING MAY ESCALATE ON IT.
#: `scripts/build_prior.py` used to attach a logging handler and turn this
#: message into a build failure. That made the highest-consequence guard in
#: the plan depend on logging CONFIGURATION: one `logging.basicConfig(
#: level=logging.ERROR)` in a CI wrapper, or a rename of this module's
#: logger, and the guard was silently dead while a complete, well-formed,
#: publishable release was written with exit 0. The escalation now reads
#: :class:`LeakageGuardResult`'s COUNTS, which no logging level, logger
#: rename or `logging.disable()` can reach. Keep this constant for the log
#: message and for the tests that pin the message's wording; do not
#: reintroduce a caller that branches on it.
ZERO_MATCH_WARNING_MARKER = "matched zero of"


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


@dataclass(frozen=True)
class LeakageGuardResult:
    """What :func:`apply_leakage_guards` did, as DATA rather than as log lines.

    THIS EXISTS BECAUSE A LOG RECORD IS NOT CONTROL FLOW
    ----------------------------------------------------
    ``scripts/build_prior.py`` must fail the build when a split was supplied
    and the contributor guard matched NOTHING -- the shared observable symptom
    of a contributor-spelling miss, mojibake, an un-wired eval split and a
    cell-format mismatch the resolution check cannot see. It used to detect
    that by attaching a :class:`logging.Handler` to this module's logger and
    string-matching the WARNING three frames down. Reproduced end to end: with
    ``logging.basicConfig(level=logging.ERROR)`` set before ``main()`` and a
    split matching nothing, the build wrote all ten artifacts and returned
    exit 0. Four other logging configurations did the same, and the logger
    NAME was a hard-coded string in the caller, so renaming this module's
    logger bypassed the guard too.

    Every field here is returned by the function that FILTERED, derived from
    the same two row counts the log line reports, so the caller's escalation
    is a comparison on integers. No logging level, no ``logging.disable()``,
    no handler configuration and no logger rename can reach it.

    ``banned_key_count`` is the count of normalised (cell, week, contributor)
    keys the split actually produced -- NOT ``len(test_recordings)``. The two
    differ when recordings collide after normalisation or when one is skipped,
    and it is the KEYS that decide what the guard can remove.
    """

    #: The guarded, STAMPED table. The reason this is a named attribute rather
    #: than the return value itself: a caller that forgets to unwrap gets an
    #: immediate `AttributeError` on a `pa.Table` method, never a silently
    #: mis-typed table flowing on into `aggregate_counts`.
    table: pa.Table
    #: Normalised keys derived from the split. Zero means NO split was in
    #: effect, which is the legitimate `--no-test-split` mode.
    banned_key_count: int
    #: Rows guard 1 removed (Xeno-Canto publisher).
    xeno_canto_rows_removed: int
    #: Rows guard 2 removed (test contributor's own cell-weeks).
    contributor_rows_removed: int
    #: Rows the contributor guard was offered, so a caller can report a rate.
    rows_before_contributor_guard: int
    #: Banned keys that removed AT LEAST ONE row.
    #:
    #: NOT DERIVABLE FROM THE ROW COUNTS, and that is the whole reason it
    #: exists. ``contributor_rows_removed > 0`` is a claim about the WHOLE
    #: split: one live key among ninety-nine dead ones satisfies it, and a
    #: 100-key split with a single real contributor and ninety-nine Sydney
    #: coordinates was REPRODUCED building a complete five-artifact release
    #: while the all-dead control correctly raised. The failure modes this
    #: guard exists to catch degrade MOST of a split, not all of it, so the
    #: quantity that makes them visible is per-key, not per-row.
    matched_key_count: int
    #: Which join key guard 2 ran with -- ``"match"`` for the original
    #: (cell, week, contributor) key, ``"placeholder"`` for the (cell, week)
    #: key a placeholder-contributor split needs (see
    #: :data:`CONTRIBUTOR_MODES`). Carried on the RESULT so an escalation
    #: message can name the key shape that actually joined: under
    #: placeholder banning, ``banned_key_count`` counts distinct CELL-WEEKS
    #: and ``matched_key_count`` counts cell-weeks that bit, so the
    #: zero-match and match-rate clauses read unchanged while meaning the
    #: new denominators.
    contributor_mode: str = "match"

    @property
    def zero_match(self) -> bool:
        """A non-empty split removed NOTHING: the guard was a silent no-op.

        The exact condition :func:`drop_test_contributor_cells` warns on,
        recomputed from the returned counts rather than read off the warning.
        ``banned_key_count > 0`` is what makes an empty split a legitimate
        no-op: it is derived from what the guard HELD, so it cannot be
        satisfied by a split that was supplied and then dropped.

        This is the TOTAL condition and it remains necessary: it is the one
        unambiguous statement about a split, true only when the join
        mechanism is globally dead. It is NOT sufficient -- see
        :attr:`match_rate` and :attr:`matched_key_count` for the partial case
        it cannot see.
        """
        return self.banned_key_count > 0 and self.contributor_rows_removed == 0

    @property
    def match_rate(self) -> float:
        """Fraction of the split's banned keys that removed at least one row.

        RAISES on an empty split rather than returning a number. ``0/0`` has
        no value here and every candidate is a lie: ``0.0`` says the split
        failed entirely and would make the legitimate ``--no-test-split``
        mode fail every rate check, ``1.0`` says it worked perfectly. A
        caller must test :attr:`banned_key_count` first, which is exactly the
        test that distinguishes "no split was supplied" from "a split was
        supplied and did nothing" -- the distinction this class exists for.
        """
        if self.banned_key_count <= 0:
            raise ValueError(
                "match_rate is undefined for a split with no banned keys "
                "(banned_key_count == 0): that is the legitimate "
                "`--no-test-split` mode, not a split that matched nothing. "
                "Test `banned_key_count` before reading the rate; returning "
                "0.0 or 1.0 here would silently fold the two cases together"
            )
        return self.matched_key_count / self.banned_key_count


def _drop_xeno_canto(table: pa.Table) -> tuple[pa.Table, int]:
    """Guard 1, returning (kept, removed). The ONE derivation of the count.

    :func:`drop_xeno_canto` is the table-only projection of this and the log
    line below is the human-readable one; both read this single subtraction,
    so the number an escalation sees and the number a log reports can never
    disagree.
    """
    # The value set is built in the column's OWN value type (job 5722: the
    # parse output carries dictionary<int32, large_string>, the sequential
    # path pa.string). Explicit rather than left to `is_in`'s implicit
    # coercion: the direction of an implicit cast is Arrow's choice, and a
    # cast of the COLUMN to pa.string is the very overflow being retired.
    keys = pa.array(
        sorted(XENO_CANTO_PUBLISHER_KEYS),
        value_type(table.column("publisher_key")),
    )
    mask = pc.invert(pc.is_in(table.column("publisher_key"), value_set=keys))
    kept = table.filter(mask)
    removed = table.num_rows - kept.num_rows
    if removed:
        logger.info(
            "drop_xeno_canto: removed %d of %d occurrences",
            removed,
            table.num_rows,
        )
    return kept, removed


def drop_xeno_canto(table: pa.Table) -> pa.Table:
    """Remove occurrences published by Xeno-Canto.

    ``pc.is_in(null, value_set)`` returns false, not null, so a null-publisher
    row is KEPT — the correct direction: an unattributed record is not evidence
    of Xeno-Canto provenance.
    """
    return _drop_xeno_canto(table)[0]


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


def _mask_and_matched_keys(
    cells: list[str],
    weeks: list[int],
    names: list[str | None],
    banned: set[tuple[str, int, str]],
) -> tuple[list[bool], set[tuple[str, int, str]]]:
    """The row mask AND the set of banned keys that actually bit.

    THE ONE DERIVATION OF BOTH. :func:`keep_mask` is the mask-only projection
    of this function, exactly as :func:`drop_xeno_canto` is the table-only
    projection of :func:`_drop_xeno_canto`. A caller escalating on "how much
    of this split was live" and a caller filtering rows must never be able to
    disagree, and two traversals applying the same three normalisations
    independently is how they would come to.

    WHY PER-KEY MATCHES ARE COMPUTED AT ALL
    ---------------------------------------
    A total row count answers "did ANY key bite", which is a claim about the
    WHOLE split. Reproduced: a 100-key split in which one key matched a real
    contributor and ninety-nine matched nothing (Sydney coordinates against
    Danish occurrences) built a complete five-artifact release, while the
    all-hundred-match-nothing control correctly raised. One live key disarmed
    the guard for an arbitrarily large split. The realistic degradations --
    a resolution-free coordinate bug, a contributor-spelling convention that
    changed mid-file, a mojibaked name column -- ruin 99% of a split, not
    100%, so a total-only signal is inert for every failure an operator would
    not already have noticed. The MATCHED KEY SET is what makes the rate
    knowable, and the rate is what makes a partial failure visible.

    *matched* holds the keys as they appear in *banned*, so its cardinality is
    directly comparable with ``len(banned)`` and can never exceed it.
    """
    keep: list[bool] = []
    matched: set[tuple[str, int, str]] = set()
    for cell, week, name in zip(cells, weeks, names):
        who = normalise_contributor(name)
        if who is None:
            # Layer 2 of the blank-contributor defence: ALWAYS kept, and the
            # key is never even formed, so a blank key that reached *banned*
            # by some other route cannot match it either.
            keep.append(True)
            continue
        key = (normalise_cell(cell), normalise_week(week), who)
        if key in banned:
            # `key in banned` is itself the proof that the cell component is
            # not None: `banned_keys` RAISES on a None cell, so no such key
            # can be in the set. The cast states that and costs nothing at
            # runtime; coercing the cell instead would be a silent repair.
            matched.add(cast("tuple[str, int, str]", key))
            keep.append(False)
        else:
            keep.append(True)
    return keep, matched


def keep_mask(
    cells: list[str],
    weeks: list[int],
    names: list[str | None],
    banned: set[tuple[str, int, str]],
) -> list[bool]:
    """Row mask: True to keep. Layer 2 of the blank-contributor defence.

    The mask-only projection of :func:`_mask_and_matched_keys`; see there for
    why the matched-key set is computed beside it.

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
    A blank-contributor row short-circuits BEFORE the week is normalised, so
    that contract binds only rows with a usable contributor.
    """
    return _mask_and_matched_keys(cells, weeks, names, banned)[0]


def banned_cell_weeks(
    test_recordings: Iterable[TestRecording],
) -> set[tuple[str, int]]:
    """Normalised (cell, week) keys for PLACEHOLDER mode.

    The contributor is deliberately NOT part of the key: in this mode the
    split's ``recorded_by`` values are test-set names rather than people, so
    joining on them is structurally empty (the job-5345 failure). Every
    occurrence in a test site's cell-week is banned, whoever recorded it.

    The recordings' contributors are still VALIDATED -- ``TestRecording``
    construction already rejected blanks -- but two recordings differing
    only in placeholder name collapse to one cell-week key, which is what
    ``banned_key_count`` should count in this mode: the number of distinct
    site-weeks the guard can act on.
    """
    banned: set[tuple[str, int]] = set()
    for recording in test_recordings:
        cell = normalise_cell(recording.cell)
        if cell is None:
            raise ValueError("test recording cell must not be None")
        banned.add((cell, normalise_week(recording.week)))
    return banned


def _mask_and_matched_cell_weeks(
    cells: list[str],
    weeks: list[int],
    banned: set[tuple[str, int]],
) -> tuple[list[bool], set[tuple[str, int]]]:
    """Placeholder mode's analogue of :func:`_mask_and_matched_keys`.

    ONE derivation of mask and matched set, for the same reason as the
    match-mode original. There is no contributor short-circuit: an
    anonymous row in a banned cell-week is REMOVED here, where match mode
    always keeps it -- that widening is the mode's entire point, and it is
    the conservative direction for a leakage guard.
    """
    keep: list[bool] = []
    matched: set[tuple[str, int]] = set()
    for cell, week in zip(cells, weeks):
        key = (normalise_cell(cell), normalise_week(week))
        if key in banned:
            matched.add(cast("tuple[str, int]", key))
            keep.append(False)
        else:
            keep.append(True)
    return keep, matched


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


def _drop_test_contributor_cells(
    table: pa.Table,
    test_recordings: Iterable[TestRecording],
    contributor_mode: str = "match",
) -> tuple[pa.Table, int, int, int]:
    """Guard 2, returning (kept, rows_removed, banned_key_count, matched_key_count).

    The ONE derivation of all three counts. :func:`drop_test_contributor_cells`
    is the table-only projection; the WARNING below,
    :attr:`LeakageGuardResult.zero_match` and
    :attr:`LeakageGuardResult.match_rate` are readings of these same integers,
    so a caller escalating on the data and a human reading the log can never
    be told different stories.

    ``matched_key_count`` is the number of banned keys that removed AT LEAST
    ONE row. It is not derivable from the other two: ``rows_removed`` is a
    claim about the whole split, and one live key among a hundred dead ones
    makes it non-zero. See :func:`_mask_and_matched_keys`.

    THE MODE CHANGES THE KEY, NOT THE COUNTS' MEANING TO A CALLER
    -------------------------------------------------------------
    In ``contributor_mode="placeholder"`` the banned key is (cell, week)
    (see :data:`CONTRIBUTOR_MODES`): ``banned_key_count`` is then the number
    of distinct test-site cell-weeks and ``matched_key_count`` the number
    that removed at least one row. Zero-match and match-rate escalations
    read the same two integers in both modes -- the denominators differ,
    the clauses do not. The log lines name the key shape via
    :func:`key_shape` so a human reading them triages the join that
    actually ran.
    """
    _validate_contributor_mode(contributor_mode)
    placeholder = contributor_mode == "placeholder"
    banned: set = (
        banned_cell_weeks(test_recordings)
        if placeholder
        else banned_keys(test_recordings)
    )
    if not banned:
        return table, 0, 0, 0

    # `text_to_pylist`, not `.to_pylist()`: identical values for plain
    # strings, and for the parse output's dictionary columns (job 5722) it
    # decodes each distinct value once instead of once per row.
    cells = text_to_pylist(table.column("cell"))
    weeks = table.column("week").to_pylist()
    who_raw = text_to_pylist(table.column("recorded_by"))

    _assert_one_resolution(cells, _as_triples(banned) if placeholder else banned)

    flagged = sum(
        1 for name in who_raw if name is not None and REPLACEMENT_CHARACTER in name
    )
    if flagged and not placeholder:
        # In placeholder mode the contributor is not part of the join key,
        # so mojibake in `recorded_by` cannot cause a miss; warning about it
        # would send an operator chasing a join component that never ran.
        logger.warning(
            "drop_test_contributor_cells: %d occurrence(s) carry U+FFFD in "
            "recorded_by; their contributor names were mojibaked on decode and "
            "may not match a test recording exactly",
            flagged,
        )

    if placeholder:
        keep, matched = _mask_and_matched_cell_weeks(cells, weeks, banned)
    else:
        keep, matched = _mask_and_matched_keys(cells, weeks, who_raw, banned)
    kept = table.filter(pa.array(keep, pa.bool_()))
    removed = table.num_rows - kept.num_rows
    shape = key_shape(contributor_mode)
    if removed:
        logger.info(
            "drop_test_contributor_cells: removed %d of %d occurrences "
            "over %d of %d banned %s keys",
            removed,
            table.num_rows,
            len(matched),
            len(banned),
            shape,
        )
        if len(matched) < len(banned):
            # The PARTIAL case, which a row total cannot express. One live key
            # in a hundred dead ones removes rows and looks healthy; the three
            # failure modes this guard exists to catch (a resolution-free
            # coordinate bug, a contributor-spelling convention that changed
            # mid-file, a mojibaked name column) all leave a small live
            # remainder rather than nothing. The BUILD SCRIPT escalates on
            # `LeakageGuardResult.match_rate`; this line is for the human, and
            # deleting it deafens the log without disarming anything.
            logger.warning(
                "drop_test_contributor_cells: only %d of %d banned "
                "%s key(s) matched any occurrence "
                "(%.1f%%); the remainder removed nothing",
                len(matched),
                len(banned),
                shape,
                100.0 * len(matched) / len(banned),
            )
    else:
        # A non-empty banned set that matches NOTHING used to be completely
        # silent. That one line is the shared observable symptom of every
        # remaining silent-failure mode in this module — contributor-spelling
        # miss, mojibake, an un-wired eval split — so it fires unconditionally
        # rather than trying to diagnose which one it was. An EMPTY banned set
        # legitimately matches nothing and returned earlier, so it never warns.
        #
        # THE WARNING IS FOR THE HUMAN. The build script's escalation reads
        # the RETURNED COUNTS (see `LeakageGuardResult.zero_match`), because a
        # handler-and-string-match detector died silently under one line of
        # ordinary logging config. Deleting this warning deafens the log; it
        # does not disarm the guard.
        logger.warning(
            "drop_test_contributor_cells: %d banned %s "
            "key(s) %s %d occurrences; check the H3 resolution and "
            "cell formatting of the test split",
            len(banned),
            shape,
            ZERO_MATCH_WARNING_MARKER,
            table.num_rows,
        )
    return kept, removed, len(banned), len(matched)


def _as_triples(cell_weeks: set[tuple[str, int]]) -> set[tuple[str, int, str]]:
    """Adapt placeholder-mode (cell, week) keys for `_assert_one_resolution`.

    The resolution check iterates (cell, week, who) triples and reads only
    the cell; a synthetic contributor keeps ONE check serving both modes
    rather than a second, drift-prone copy of the H3 comparison.
    """
    return {(cell, week, "-") for cell, week in cell_weeks}


def drop_test_contributor_cells(
    table: pa.Table,
    test_recordings: Iterable[TestRecording],
    contributor_mode: str = "match",
) -> pa.Table:
    """Remove rows matching a test recording's banned key.

    In the default ``contributor_mode="match"`` the key is
    (cell, week, contributor), every component normalised on BOTH sides
    before the join — cell via :func:`normalise_cell`, week via
    :func:`normalise_week`, contributor via :func:`normalise_contributor`.
    A row whose ``recorded_by`` is null can never match, since a blank key
    is rejected at ``TestRecording`` construction.

    In ``contributor_mode="placeholder"`` the key is (cell, week): the
    split's contributors are declared to be test-set names rather than
    people, and every occurrence in a test site's cell-week is removed
    whoever recorded it — anonymous rows included. See
    :data:`CONTRIBUTOR_MODES` for why the mode is explicit, never detected.

    Raises ``ValueError`` if the test split's H3 resolution differs from the
    table's, which would make the whole guard a silent no-op — in either
    mode.

    CONTRACT — *table* must carry a non-null, integral ``week`` column, as
    written by ``assign_cells`` (``pa.int8()`` from a non-null ``event_date``).
    Both modes route every week through :func:`normalise_week`, which
    raises rather than coercing, so a null or fractional week from a table built
    some other way is a hard build failure, not a silently mis-banned week.
    """
    return _drop_test_contributor_cells(table, test_recordings, contributor_mode)[0]


def apply_leakage_guards(
    table: pa.Table,
    test_recordings: Iterable[TestRecording],
    contributor_mode: str = "match",
) -> LeakageGuardResult:
    """Both guards, in order, STAMP the result, and REPORT WHAT THEY DID.

    Returns a :class:`LeakageGuardResult`, NOT a ``pa.Table``. That is the
    whole point: the caller that has to escalate on "a split was supplied and
    matched nothing" -- ``scripts/build_prior.py`` -- can only do so on DATA,
    and the previous signature forced it to attach a logging handler and
    string-match a WARNING three frames down. One
    ``logging.basicConfig(level=logging.ERROR)`` in a CI wrapper silently
    disarmed the highest-consequence guard in the plan while a complete,
    publishable release was written with exit 0.

    The return type is deliberately NOT a ``pa.Table`` subclass and the table
    is NOT the first element of a tuple that could be indexed by accident:
    every existing caller that treated the return value as a table now fails
    IMMEDIATELY with ``AttributeError`` on ``.num_rows`` / ``.schema``, at the
    line that needs updating. Widening a return value in a way that lets old
    call sites keep working is how a mechanism goes quietly dead.
    :func:`drop_xeno_canto` and :func:`drop_test_contributor_cells` still
    return bare tables for callers that genuinely want one guard and no
    counts.

    The stamp is applied here and nowhere else: this function is the only
    definition of "both guards have run", so a table that carries the marker
    without having been through it would have to have been stamped by hand.
    ``effort.target_group_ratios`` requires the marker, so the structural
    consequence is that the effort denominator can only be computed over a
    table this function produced.

    An EMPTY ``test_recordings`` still stamps, and reports
    ``banned_key_count == 0``. That is deliberate: ``[]`` is a legitimate mode
    (no evaluation split is wired in), the Xeno-Canto guard ran regardless,
    and refusing to stamp it would push every caller toward stamping by hand
    -- which is exactly the bypass the marker exists to close. Whether a split
    SHOULD have been supplied is knowledge only the build script has, and it
    is enforced there, by requiring an explicit ``--no-test-split`` opt-out.
    ``banned_key_count == 0`` is what makes that mode distinguishable from a
    supplied-but-inert split without the build script having to re-derive the
    keys itself.
    """
    after_xc, xc_removed = _drop_xeno_canto(table)
    kept, contributor_removed, banned_count, matched_count = (
        _drop_test_contributor_cells(after_xc, test_recordings, contributor_mode)
    )
    return LeakageGuardResult(
        table=mark_guards_applied(kept),
        banned_key_count=banned_count,
        xeno_canto_rows_removed=xc_removed,
        contributor_rows_removed=contributor_removed,
        rows_before_contributor_guard=after_xc.num_rows,
        matched_key_count=matched_count,
        contributor_mode=contributor_mode,
    )
