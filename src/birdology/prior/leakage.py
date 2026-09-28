"""Keep the answers out of the prior.

Xeno-Canto publishes its recordings to GBIF as occurrences. If a test
recording's own occurrence record reaches the prior, the prior trivially
"knows" the species, and every reported gain is leakage. Two guards:

1. Drop every occurrence published by Xeno-Canto.
2. Drop every occurrence sharing a (cell, week) with a test recording from the
   same contributor — the same person's other records from the same outing.

Both guards MUST run on the ``assign_cells`` output, before
``aggregate_counts``, which discards ``publisher_key`` and ``recorded_by``.

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
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass

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


def normalise_contributor(name: str | None) -> str | None:
    """Canonical form of a contributor name for leakage comparison.

    A leakage guard is asymmetric: a MISSED match leaves a leaking occurrence
    in the prior, which is the exact failure the guard exists to prevent, while
    an over-match only removes extra data. So this normalisation is deliberately
    aggressive — NFKC, casefold, collapse all whitespace runs to one space, and
    strip — to make near-spellings of one contributor collide.

    Returns ``None`` for ``None`` and for blank/whitespace-only input, so a
    blank contributor can never be used as a join key.
    """
    if name is None:
        return None
    folded = unicodedata.normalize("NFKC", name).casefold()
    collapsed = " ".join(folded.split())
    return collapsed or None


@dataclass(frozen=True)
class TestRecording:
    """A recording in an evaluation split, as (where, when, who).

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
        week = int(self.week)
        if not 1 <= week <= 52:
            raise ValueError(
                f"TestRecording.week must be in 1..52 (week 53 folds into 52), got {week!r}"
            )
        if normalise_contributor(self.recorded_by) is None:
            raise ValueError(
                "TestRecording.recorded_by must be a non-blank contributor name; "
                "a blank one would match every anonymous occurrence in the cell-week"
            )


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


def drop_test_contributor_cells(
    table: pa.Table, test_recordings: Iterable[TestRecording]
) -> pa.Table:
    """Remove rows matching a test recording's (cell, week, contributor).

    Contributor names are normalised on BOTH sides before the join (see
    :func:`normalise_contributor`). A row whose ``recorded_by`` is null can
    never match, since a blank key is rejected at ``TestRecording`` construction.
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
        banned.add((recording.cell, int(recording.week), who))

    if not banned:
        return table

    cells = table.column("cell").to_pylist()
    weeks = table.column("week").to_pylist()
    who_raw = table.column("recorded_by").to_pylist()

    keep: list[bool] = []
    flagged = 0
    for cell, week, name in zip(cells, weeks, who_raw):
        if name is not None and REPLACEMENT_CHARACTER in name:
            flagged += 1
        contributor = normalise_contributor(name)
        keep.append(
            contributor is None
            or (cell, int(week), contributor) not in banned
        )

    if flagged:
        logger.warning(
            "drop_test_contributor_cells: %d occurrence(s) carry U+FFFD in "
            "recorded_by; their contributor names were mojibaked on decode and "
            "may not match a test recording exactly",
            flagged,
        )

    kept = table.filter(pa.array(keep, pa.bool_()))
    if kept.num_rows != table.num_rows:
        logger.info(
            "drop_test_contributor_cells: removed %d of %d occurrences "
            "over %d banned (cell, week, contributor) keys",
            table.num_rows - kept.num_rows,
            table.num_rows,
            len(banned),
        )
    return kept


def apply_leakage_guards(
    table: pa.Table, test_recordings: Iterable[TestRecording]
) -> pa.Table:
    """Both guards, in order."""
    return drop_test_contributor_cells(drop_xeno_canto(table), test_recordings)
