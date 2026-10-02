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

THE MERGE NEVER GROUPS ON TEXT (job 5722)
-----------------------------------------
The first fleet run of this module (job 5722, EOD, 1.76e9 rows, 90
workers) died six minutes in, in ``_compact``: ``Column 3: In chunk 1725:
Invalid: Negative offsets in binary array``. The text columns were
``pa.string()`` -- int32 offsets, 2 GiB of characters per chunk -- and the
hash-aggregate over hundreds of millions of distinct groups emitted one
publisher_key chunk (36 bytes per GROUP) past 2^31 bytes. The geometric
compaction below deliberately grows the compacted base, so a >2 GiB chunk
was inevitable at fleet scale and invisible at the 10M-row benchmark.

``pa.large_string()`` is NOT the fix, measured on pyarrow 25.0.1: a 5-key
``group_by`` with large_string keys ABORTED the process
(``std::length_error``) at 30M groups. So text never enters a merge key:

* a worker returns its partial with the text columns dictionary-encoded
  against a LOCAL dictionary (one chunk's worth -- small);
* the parent maps each local dictionary onto per-column global
  vocabularies (cell / publisher / contributor -> int32 code) and merges
  ``MERGE_SCHEMA`` tables: six fixed-width columns, ~29 bytes/row instead
  of ~90, and integer hashing instead of string hashing;
* ``_finalise`` turns codes back into ``arrow_strings.TEXT_TYPE`` columns
  in CANONICAL form (sorted, used-only dictionary), so the output is a pure
  function of the data whatever order chunks arrived in, and sorts on the
  canonical indices -- which order exactly as the strings do.

Measured effect on the recorded 10M-row benchmark (10M rows, r3/4/5):
workers=1 74.8s -> 39.4s, workers=8 40.6s -> 15.9s, parent RSS 15 -> 9.6 GB,
output digest identical. ``measure_parallel_parse_results.json`` is NOT
rewritten; the before/after is recorded in the commit message.

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
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .arrow_strings import TEXT_TYPE, dictionary_from_codes
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

#: The text columns, in schema order. Each is coded against its own
#: global vocabulary during the merge.
TEXT_COLUMNS = ("cell", "publisher_key", "recorded_by")

#: The pre-aggregated OUTPUT schema. ``week`` is ``int8`` exactly as
#: ``assign_cells`` writes it. The text columns are
#: ``dictionary<int32, large_string>`` in canonical form (see
#: ``arrow_strings.canonical_dictionary``): NO 32-bit-offset column exists
#: anywhere in the parse, its output or the checkpoint (job 5722). Every
#: consumer reads them by value -- the guards via
#: ``arrow_strings.text_to_pylist``, ``aggregate_counts`` by narrowing to
#: ``pa.string()`` in bounded chunks after aggregation.
PARSE_SCHEMA = pa.schema(
    [
        ("species_key", pa.int64()),
        ("cell", TEXT_TYPE),
        ("week", pa.int8()),
        ("publisher_key", TEXT_TYPE),
        ("recorded_by", TEXT_TYPE),
        ("n", pa.int64()),
    ]
)

#: What the parent MERGES: PARSE_SCHEMA with every text column replaced by
#: its int32 code into the parent's vocabulary. Fixed-width only -- see
#: "THE MERGE NEVER GROUPS ON TEXT" above; ``_compact`` refuses anything
#: else.
MERGE_SCHEMA = pa.schema(
    [
        (field.name, pa.int32() if field.name in TEXT_COLUMNS else field.type)
        for field in PARSE_SCHEMA
    ]
)

#: A worker's partial: text dictionary-encoded against the CHUNK'S OWN
#: dictionary. A chunk is at most ``chunk_bytes`` of input, so its text can
#: never approach the int32-offset limit.
_PARTIAL_SCHEMA = pa.schema(
    [
        (
            field.name,
            pa.dictionary(pa.int32(), pa.string())
            if field.name in TEXT_COLUMNS
            else field.type,
        )
        for field in PARSE_SCHEMA
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
    """One chunk's partial counts as an UNSORTED ``_PARTIAL_SCHEMA`` table.

    The text columns are dictionary-encoded HERE, in the worker, so the
    parent's per-chunk work is a lookup of each DISTINCT value rather than
    of every row.
    """
    if not counts:
        return _PARTIAL_SCHEMA.empty_table()
    species, cells, weeks, publishers, who = zip(*counts.keys())

    def text(values) -> pa.DictionaryArray:
        return pa.array(list(values), pa.string()).dictionary_encode()

    return pa.table(
        [
            pa.array(species, pa.int64()),
            text(cells),
            pa.array(weeks, pa.int8()),
            text(publishers),
            text(who),
            pa.array(list(counts.values()), pa.int64()),
        ],
        schema=_PARTIAL_SCHEMA,
    )


class _Vocabulary:
    """One text column's global string -> int32 code table (parent only).

    Codes are assigned in FIRST-SEEN order, which depends on the order
    ``imap_unordered`` delivers chunks in -- so codes are a private detail
    of one run and never leave this module: ``_finalise`` re-encodes them
    canonically. int32 is ample: the distinct cells, publishers and
    contributors of a GBIF archive number in the millions, not billions,
    and overflowing it raises rather than wraps.
    """

    def __init__(self) -> None:
        self.code_of: dict[str, int] = {}
        self.values: list[str] = []

    def codes_for(self, dictionary: pa.Array) -> pa.Array:
        """int32 global code for each entry of a chunk-local dictionary."""
        out = np.empty(len(dictionary), dtype=np.int32)
        code_of = self.code_of
        values = self.values
        for i, value in enumerate(dictionary.to_pylist()):
            code = code_of.get(value)
            if code is None:
                code = len(values)
                if code > np.iinfo(np.int32).max:
                    raise OverflowError(
                        "more distinct text values than int32 codes"
                    )
                code_of[value] = code
                values.append(value)
            out[i] = code
        return pa.array(out, pa.int32())

    def as_array(self) -> pa.Array:
        return pa.array(self.values, pa.large_string())


def _encode_partial(
    partial: pa.Table, vocabularies: dict[str, _Vocabulary]
) -> pa.Table:
    """A worker partial as a ``MERGE_SCHEMA`` table of global codes.

    ``take`` of the local->global code map by the local indices keeps a
    null index null: a null publisher or contributor stays null, and
    ``group_by`` groups nulls as one key exactly as it did for strings.
    """
    columns = []
    for field in MERGE_SCHEMA:
        column = partial.column(field.name)
        if field.name in TEXT_COLUMNS:
            vocabulary = vocabularies[field.name]
            column = pa.chunked_array(
                [
                    vocabulary.codes_for(chunk.dictionary).take(chunk.indices)
                    for chunk in column.chunks
                ],
                type=pa.int32(),
            )
        columns.append(column)
    return pa.Table.from_arrays(columns, schema=MERGE_SCHEMA)


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


def _merge(
    into: dict[int, list[pa.Table]],
    partials: dict[int, pa.Table],
    vocabularies: dict[str, _Vocabulary],
) -> None:
    """Append one worker's partials, CODED, to the per-resolution pending lists.

    The actual summation happens in :func:`_compact`, in Arrow. The pending
    lists are compacted whenever their row total crosses
    ``_COMPACT_PENDING_ROWS``, so the parent's working set stays bounded by
    (distinct keys so far + one compaction's input) rather than growing with
    the number of chunks.
    """
    for resolution, partial in partials.items():
        if partial.num_rows:
            into[resolution].append(_encode_partial(partial, vocabularies))


#: Floor for the geometric compaction policy: the uncompacted increment
#: must reach at least this many rows (and at least the compacted base's
#: size) before a compaction runs. Large enough that Arrow's group_by runs
#: over meaningful batches; the geometric clause, not this floor, is what
#: bounds total compaction work on a long run.
_COMPACT_PENDING_ROWS = 4_000_000


def _compact(pending: list[pa.Table]) -> pa.Table:
    """Sum duplicate keys across pending ``MERGE_SCHEMA`` tables, in Arrow.

    ``group_by(...).aggregate(n, sum)`` returns int64 for an int64 input, so
    repeated compaction is associative and loss-free: compact(compact(a, b),
    c) == compact(a, b, c), which is what makes the incremental schedule
    exact whatever order ``imap_unordered`` delivers chunks in.

    REFUSES a non-integer column (job 5722): a variable-width key is what
    overflowed int32 offsets on the fleet, and its 64-bit variant aborts
    Arrow's grouper. A regression that routes text back in fails here on
    the first fixture rather than at 2^31 bytes on a 90-worker node.
    """
    for table in pending:
        for field in table.schema:
            if not pa.types.is_integer(field.type):
                raise ValueError(
                    f"merge column {field.name!r} is {field.type}, not a "
                    "fixed-width integer: text must be coded before the "
                    "merge (job 5722 -- string keys overflow int32 offsets "
                    "and large_string keys abort the grouper at fleet scale)"
                )
    if not pending:
        return MERGE_SCHEMA.empty_table()
    stacked = pa.concat_tables(pending)
    grouped = stacked.group_by(
        ["species_key", "cell", "week", "publisher_key", "recorded_by"]
    ).aggregate([("n", "sum")])
    return grouped.rename_columns(
        ["species_key", "cell", "week", "publisher_key", "recorded_by", "n"]
    ).select(MERGE_SCHEMA.names).cast(MERGE_SCHEMA)


def _finalise(
    pending: list[pa.Table], vocabularies: dict[str, _Vocabulary]
) -> pa.Table:
    """Final compaction, canonical decode, and the deterministic sort.

    SORTED so the table -- and therefore the checkpoint file -- is
    byte-reproducible across runs and worker counts: ``imap_unordered``
    deliberately randomises arrival order (and with it the run-private
    codes). Keys are unique after ``_compact``, so the sort needs no
    tiebreak.

    The sort runs on the CANONICAL dictionary indices, never on text:
    Arrow cannot sort a dictionary column, and a canonical dictionary is
    sorted, so index order IS string order -- nulls last either way. The
    row order is therefore exactly the order the string sort produced.
    """
    merged = _compact(pending)
    columns = {}
    for name in MERGE_SCHEMA.names:
        column = merged.column(name)
        if name in TEXT_COLUMNS:
            column = dictionary_from_codes(column, vocabularies[name].as_array())
        columns[name] = column
    sort_keys = pa.table(
        {
            name: (
                pa.chunked_array(
                    [chunk.indices for chunk in columns[name].chunks],
                    type=pa.int32(),
                )
                if name in TEXT_COLUMNS
                else columns[name]
            )
            for name in ("species_key", "cell", "week", "publisher_key",
                         "recorded_by")
        }
    )
    order = pc.sort_indices(
        sort_keys,
        sort_keys=[
            ("species_key", "ascending"),
            ("cell", "ascending"),
            ("week", "ascending"),
            ("publisher_key", "ascending"),
            ("recorded_by", "ascending"),
        ],
    )
    table = pa.Table.from_arrays(
        [columns[name] for name in PARSE_SCHEMA.names], schema=PARSE_SCHEMA
    )
    return table.take(order)


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
    # ONE vocabulary per text column, shared across resolutions: a cell
    # code at r3 and one at r5 never meet in a key, so sharing costs only
    # code-space, and ``_finalise`` keeps each resolution's used values only.
    vocabularies = {name: _Vocabulary() for name in TEXT_COLUMNS}

    def _fold(chunk_dropped: dict[str, int], chunk_total: int,
              partials: dict[int, pa.Table]) -> None:
        nonlocal total
        for reason, n in chunk_dropped.items():
            dropped[reason] += n
        total += chunk_total
        _merge(pending, partials, vocabularies)
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
        tables={r: _finalise(pending[r], vocabularies) for r in resolutions},
        dropped=dropped,
        total=total,
    )
