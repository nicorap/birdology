"""Parallel parse+aggregate of a GBIF SIMPLE_CSV archive.

WHY THIS EXISTS (job 5345, measured, not assumed)
-------------------------------------------------
``read_occurrences`` pulled 1.76e9 rows through one Python thread in ~6.5h:
``csv.DictReader`` row construction, three ``float``/``int`` parses, a date
parse and the h3 assignment, all serialized. The DEFLATE stream itself is
not the bottleneck -- decompression runs at hundreds of MB/s in zlib C code
-- the per-row Python work is, so the per-row work is what moves to worker
processes.

THE DESIGN
----------
One reader streams the decompressed member in byte chunks EXTENDED TO A LINE
BOUNDARY (``read(chunk_bytes)`` + ``readline()``), so no row and no
multi-byte UTF-8 sequence is ever split across chunks: a UTF-8 code point
cannot span a ``\\n``, which is why per-chunk ``decode(errors="replace")``
produces byte-for-byte the same text as the single ``TextIOWrapper`` in
``read_occurrences``. Each worker parses+filters its chunk with EXACTLY the
``read_occurrences`` row rules (same drop reasons, same order of checks) and
aggregates it into partial (species, cell, week, publisher, contributor)
counts per resolution, returned as Arrow tables; the parent folds each
partial into per-resolution pending lists and compacts them GEOMETRICALLY
(see ``parse_and_aggregate``), so at no point does it hold all partials at
once -- the fleet node's r5 partials are large, and holding N of them would
multiply the parent's working set by the worker count.

MEASURED, NOT PROMISED (macOS arm64, 18 cores, 10M-row checklist-shaped
synthetic GBIF archive; ``scripts/measurements/measure_parallel_parse.py``
records every figure):

* raw DEFLATE decompression of the member: ~930 MB/s -- the zip stream is
  NOT the bottleneck;
* the worker phase alone (parse+filter+partial-aggregate, results
  discarded): 7.9s at 8 workers for what takes ~50s sequentially -- the
  parallelisable part scales ~6x;
* end-to-end ``parse_and_aggregate``: workers=1 67.2s, workers=4 40.5s,
  workers=8 41.6s -- 1.6-1.7x, NOT 6x, because the parent's Arrow merge
  (concat + group_by + final sort) is serial and this fixture is the
  WORST CASE for it: nearly every row is its own aggregation group, so
  the merge processes as many rows as the parse did. The merge cost
  scales with DISTINCT KEYS, not with rows read; real checklist data
  collapses by one to two orders of magnitude, which moves end-to-end
  toward the worker-phase scaling. Report figures from the recorded
  measurement file, never from this prose.

THE OUTPUT IS PRE-AGGREGATED, AND THAT IS EXACT
-----------------------------------------------
The tables carry (species_key, cell, week, publisher_key, recorded_by, n)
rather than one row per occurrence. Every downstream consumer -- both
leakage guards and ``aggregate_counts`` -- keys on exactly these columns, so
collapsing identical rows loses nothing: a guard that drops a group row
drops precisely the occurrences that row counts, and ``aggregate_counts``
sums ``n`` as a weight. Group counts are also what makes the checkpoint and
the guard phase small: 1.76e9 occurrences collapse to the number of DISTINCT
(species, cell, week, publisher, contributor) keys.

THE DROP-COUNTER CONTRACT
-------------------------
``dropped`` carries the same three reasons as ``read_occurrences``
(``no_species_key``, ``bad_coordinates``, ``unparseable_date``), counted by
the same checks in the same order, and ``total`` counts every data row read.
``tests/prior/test_parallel_parse.py`` pins workers=1 == workers=4 ==
``read_occurrences`` on aggregates AND counters.

KNOWN, STATED DIVERGENCE: rows are split on ``str.splitlines`` boundaries
inside a chunk rather than by ``TextIOWrapper`` newline translation. With
``QUOTE_NONE`` a field can never legally contain a newline, so the two agree
on every well-formed TSV; a lone ``\\r`` INSIDE a field (malformed for this
format) would split differently. GBIF SIMPLE_CSV never emits one.
"""
from __future__ import annotations

import csv
import os
import zipfile
from dataclasses import dataclass
from multiprocessing import get_context
from pathlib import Path

import h3
import pyarrow as pa

from .indexing import iso_week
from .occurrences import (
    _coordinates_usable,
    _missing_columns,
    _occurrence_member,
    _optional,
    _parse_date,
)

#: Bytes per chunk handed to a worker, before the extension to the next line
#: boundary. 32 MiB keeps pickling overhead negligible against the parse cost
#: of the ~200k rows a chunk holds, while bounding the reader's working set.
DEFAULT_CHUNK_BYTES = 32 << 20

#: The pre-aggregated schema. ``week`` is ``int8`` exactly as
#: ``assign_cells`` writes it, so a table from this module and one from the
#: sequential path are interchangeable downstream.
PARSE_SCHEMA = pa.schema(
    [
        ("species_key", pa.int64()),
        ("cell", pa.string()),
        ("week", pa.int8()),
        ("publisher_key", pa.string()),
        ("recorded_by", pa.string()),
        ("n", pa.int64()),
    ]
)

#: The three drop reasons, spelled once. The NAMES are `read_occurrences`'s:
#: the checkpoint records them, and a resumed run reports the parse it came
#: from in the same vocabulary the original logged.
DROP_REASONS = ("no_species_key", "bad_coordinates", "unparseable_date")


@dataclass
class ParseResult:
    """What one full parse produced: per-resolution aggregates + provenance.

    ``tables`` is keyed by H3 resolution; each table is PARSE_SCHEMA.
    ``dropped`` and ``total`` are the ``read_occurrences`` counters, carried
    so the checkpoint can record them and a resumed run can report the parse
    it is standing on.
    """

    tables: dict[int, pa.Table]
    dropped: dict[str, int]
    total: int


def _parse_chunk(
    payload: tuple[bytes, list[str], list[int]],
) -> tuple[dict[str, int], int, dict[int, dict]]:
    """Parse one line-aligned chunk: the per-row rules of ``read_occurrences``.

    Returns (dropped, total, {resolution: {(sk, cell, week, pub, who): n}}).

    MODULE-LEVEL ON PURPOSE: the spawn start method pickles the callable by
    reference, so a closure here is a runtime failure on macOS/Windows.

    The row rules below are a TRANSLITERATION of ``read_occurrences``'s loop
    -- same checks, same order, same coercions -- and the equality tests pin
    them to it. A new drop reason added there must be added here and to
    ``DROP_REASONS``, or the counter contract test fails.
    """
    chunk, fieldnames, resolutions = payload
    dropped: dict[str, int] = dict.fromkeys(DROP_REASONS, 0)
    total = 0
    partials: dict[int, dict] = {r: {} for r in resolutions}

    # csv.reader + positional access, NOT DictReader: profiling showed the
    # per-row dict construction costing ~25% of the whole parse. The
    # semantics are matched by hand: DictReader SKIPS blank rows (`total`
    # must not count them) and returns None for a column a short row does
    # not reach (the `or ""` below covers both None and the missing case).
    idx = {name: i for i, name in enumerate(fieldnames)}
    i_species = idx["speciesKey"]
    i_lat = idx["decimalLatitude"]
    i_lon = idx["decimalLongitude"]
    i_date = idx["eventDate"]
    i_pub = idx.get("publishingOrgKey")
    i_who = idx.get("recordedBy")

    def _field(row: list, i: int | None) -> str | None:
        if i is None or i >= len(row):
            return None
        return row[i]

    # Dates repeat enormously in real occurrence data (one survey day,
    # thousands of rows), so the date->week derivation is memoised per
    # chunk. Exactness is untouched: the cache maps equal strings, and
    # `_parse_date` is a pure function of the string.
    week_cache: dict[str, int | None] = {}

    text = chunk.decode("utf-8", errors="replace")
    for row in csv.reader(
        text.splitlines(), delimiter="\t", quoting=csv.QUOTE_NONE
    ):
        if not row:
            continue  # DictReader skips blank rows; so does total
        total += 1
        species_raw = (_field(row, i_species) or "").strip()
        try:
            species_key = int(species_raw)
        except ValueError:
            dropped["no_species_key"] += 1
            continue

        lat_raw = (_field(row, i_lat) or "").strip()
        lon_raw = (_field(row, i_lon) or "").strip()
        try:
            lat = float(lat_raw)
            lon = float(lon_raw)
        except ValueError:
            dropped["bad_coordinates"] += 1
            continue
        if not _coordinates_usable(lat, lon):
            dropped["bad_coordinates"] += 1
            continue

        date_raw = (_field(row, i_date) or "").strip()
        week = week_cache.get(date_raw, -1)
        if week == -1:
            event = _parse_date(date_raw)
            week = iso_week(event) if event is not None else None
            week_cache[date_raw] = week
        if week is None:
            dropped["unparseable_date"] += 1
            continue

        publisher = _optional(_field(row, i_pub))
        who = _optional(_field(row, i_who))
        for resolution in resolutions:
            cell = h3.latlng_to_cell(lat, lon, resolution)
            key = (species_key, cell, week, publisher, who)
            partial = partials[resolution]
            partial[key] = partial.get(key, 0) + 1

    # The partial dicts leave the worker as ARROW TABLES, not dicts. Two
    # measured reasons: a pa.Table pickles through Arrow IPC (3 ms / 8.8 MB
    # for a 50k-row partial, measured) where a dict of tuples pays
    # per-object pickling, and the parent's merge then runs in Arrow's C++
    # group_by instead of a Python loop -- the Python dict merge was
    # parent-side SERIAL time that capped the whole pool at ~1x the
    # sequential path (measured before/after on the recorded benchmark).
    return dropped, total, {r: _partial_table(partials[r]) for r in resolutions}


def _partial_table(counts: dict) -> pa.Table:
    """One chunk's partial counts as an UNSORTED PARSE_SCHEMA table."""
    if not counts:
        return PARSE_SCHEMA.empty_table()
    species, cells, weeks, publishers, who = zip(*counts.keys())
    return pa.table(
        [
            list(species),
            list(cells),
            list(weeks),
            list(publishers),
            list(who),
            list(counts.values()),
        ],
        schema=PARSE_SCHEMA,
    )


def _chunks(raw, chunk_bytes: int):
    """Line-aligned byte chunks of the already-opened member stream.

    ``read(chunk_bytes)`` then ``readline()`` extends to the next ``\\n`` (or
    EOF), so every chunk is a whole number of lines and every UTF-8 sequence
    is intact. The generator holds ONE chunk at a time.
    """
    while True:
        buf = raw.read(chunk_bytes)
        if not buf:
            return
        tail = raw.readline()
        yield buf + tail


def _merge(into: dict[int, list[pa.Table]], partials: dict[int, pa.Table]) -> None:
    """Append one worker's partial tables to the per-resolution pending lists.

    The actual summation happens in :func:`_compact`, in Arrow. The pending
    lists are compacted whenever their row total crosses
    ``_COMPACT_PENDING_ROWS``, so the parent's working set stays bounded by
    (distinct keys so far + one compaction's input) rather than growing with
    the number of chunks.
    """
    for resolution, partial in partials.items():
        if partial.num_rows:
            into[resolution].append(partial)


#: Floor for the geometric compaction policy: the uncompacted increment
#: must reach at least this many rows (and at least the compacted base's
#: size) before a compaction runs. Large enough that Arrow's group_by runs
#: over meaningful batches; the geometric clause, not this floor, is what
#: bounds total compaction work on a long run.
_COMPACT_PENDING_ROWS = 4_000_000


def _compact(pending: list[pa.Table]) -> pa.Table:
    """Sum duplicate keys across pending partial tables, in Arrow.

    ``group_by(...).aggregate(n, sum)`` returns int64 for an int64 input, so
    repeated compaction is associative and loss-free: compact(compact(a, b),
    c) == compact(a, b, c), which is what makes the incremental schedule
    exact whatever order ``imap_unordered`` delivers chunks in.
    """
    if not pending:
        return PARSE_SCHEMA.empty_table()
    stacked = pa.concat_tables(pending)
    grouped = stacked.group_by(
        ["species_key", "cell", "week", "publisher_key", "recorded_by"]
    ).aggregate([("n", "sum")])
    return grouped.rename_columns(
        ["species_key", "cell", "week", "publisher_key", "recorded_by", "n"]
    ).select(PARSE_SCHEMA.names).cast(PARSE_SCHEMA)


def _finalise(pending: list[pa.Table]) -> pa.Table:
    """Final compaction + the deterministic sort the checkpoint relies on.

    SORTED so the table -- and therefore the checkpoint file -- is
    byte-reproducible across runs and worker counts: ``imap_unordered``
    deliberately randomises arrival order. Keys are unique after
    ``_compact``, so the sort needs no tiebreak.
    """
    return _compact(pending).sort_by(
        [
            ("species_key", "ascending"),
            ("cell", "ascending"),
            ("week", "ascending"),
            ("publisher_key", "ascending"),
            ("recorded_by", "ascending"),
        ]
    )


def parse_and_aggregate(
    zip_path: Path,
    resolutions: list[int],
    *,
    workers: int | None = None,
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
) -> ParseResult:
    """The parallel replacement for read -> assign_cells -> (pre)aggregate.

    ``workers=None`` means ``os.cpu_count()``. ``workers=1`` runs the SAME
    chunking and parsing code inline with no pool at all -- one code path,
    so the equality tests compare chunk-boundary handling and merging rather
    than two unrelated implementations, and a monkeypatched test never has a
    pool to fight.

    Raises ``ValueError`` on ``workers < 1`` rather than clamping: a zero
    from a bad ``--workers`` expression silently becoming 1 would hide the
    units bug on exactly the machine where the operator wanted 32.
    """
    if workers is None:
        workers = os.cpu_count() or 1
    if workers < 1:
        raise ValueError(
            f"workers must be >= 1, got {workers}; a worker count of zero "
            "cannot parse anything and clamping it to 1 would silently "
            "serialize a job the operator asked to parallelise"
        )
    if chunk_bytes < 1:
        raise ValueError(f"chunk_bytes must be >= 1, got {chunk_bytes}")
    resolutions = list(resolutions)

    dropped: dict[str, int] = dict.fromkeys(DROP_REASONS, 0)
    total = 0
    pending: dict[int, list[pa.Table]] = {r: [] for r in resolutions}

    def _fold(chunk_dropped: dict[str, int], chunk_total: int,
              partials: dict[int, pa.Table]) -> None:
        nonlocal total
        for reason, n in chunk_dropped.items():
            dropped[reason] += n
        total += chunk_total
        _merge(pending, partials)
        # Incremental compaction, GEOMETRIC (LSM-style): compact when the
        # UNCOMPACTED increment has grown to the size of the compacted
        # base (or the floor, for the start). Each compaction then roughly
        # doubles the base, so the total compaction work over the whole
        # run is O(final size x log(chunks)) and the parent never holds
        # more than base + increment ~ 2x the final distinct-key count.
        #
        # Two rejected simpler policies, both measured on a 10M-row
        # archive: compacting when base+increment crossed a fixed
        # threshold was QUADRATIC once the base alone exceeded it (every
        # chunk re-grouped the whole base: 317s inline vs 49s sequential),
        # and a fixed increment-only threshold was linear-per-compaction
        # in the base size, which on the 1.76e9-row fleet run would
        # re-group a ~1e8-group base hundreds of times. The geometric
        # policy's amortised total is ~2x one final group_by.
        for resolution, tables in pending.items():
            base_rows = tables[0].num_rows if tables else 0
            increment = sum(t.num_rows for t in tables[1:])
            if increment >= max(_COMPACT_PENDING_ROWS, base_rows):
                pending[resolution] = [_compact(tables)]

    with zipfile.ZipFile(zip_path) as zf:
        member = _occurrence_member(zf)
        with zf.open(member) as raw:
            header = raw.readline().decode("utf-8", errors="replace")
            fieldnames = next(
                csv.reader([header], delimiter="\t", quoting=csv.QUOTE_NONE),
                [],
            )
            missing = _missing_columns(fieldnames)
            if missing:
                raise ValueError(
                    f"occurrence table {member} is missing columns: {missing}"
                )
            payloads = (
                (chunk, fieldnames, resolutions)
                for chunk in _chunks(raw, chunk_bytes)
            )
            # PEEK before spawning: a one-chunk stream (every test fixture,
            # any archive smaller than chunk_bytes) gains nothing from a
            # pool and would pay N spawn-process interpreter startups for
            # it. The pooled and inline paths run the SAME _parse_chunk on
            # the SAME line-aligned chunks either way.
            import itertools

            first = next(payloads, None)
            second = next(payloads, None) if first is not None else None
            if first is None:
                stream = iter(())
            elif second is None:
                stream = iter((first,))
            else:
                stream = itertools.chain((first, second), payloads)

            if workers == 1 or second is None:
                for chunk_dropped, chunk_total, partials in map(
                    _parse_chunk, stream
                ):
                    _fold(chunk_dropped, chunk_total, partials)
            else:
                # spawn, never fork: the parent holds an open zip handle and
                # (in the build script) Arrow state that fork would duplicate
                # into every worker on macOS unsafely.
                ctx = get_context("spawn")
                with ctx.Pool(processes=workers) as pool:
                    for chunk_dropped, chunk_total, partials in (
                        pool.imap_unordered(_parse_chunk, stream)
                    ):
                        _fold(chunk_dropped, chunk_total, partials)

    return ParseResult(
        tables={r: _finalise(pending[r]) for r in resolutions},
        dropped=dropped,
        total=total,
    )
