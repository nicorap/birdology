"""Job 5722: 32-bit Arrow string offsets overflowed in the parse merge.

THE FAILURE (fleet, EOD archive, 1.76e9 rows, --workers 90): six minutes
into the parse, ``parallel_parse._compact`` died in its final ``.cast`` with
``ArrowInvalid: Column 3: In chunk 1725: Invalid: Negative offsets in binary
array``. ``PARSE_SCHEMA`` declared cell/publisher_key/recorded_by as
``pa.string()`` (int32 offsets, at most 2 GiB of character data per chunk),
and the hash-aggregate over hundreds of millions of distinct 5-key groups
emitted ONE output chunk per key column whose character data passed 2^31
bytes. Column 3 is publisher_key: a 36-byte UUID per GROUP.

REPRODUCED LOCALLY (pyarrow 25.0.1): 62M distinct 5-key groups of
string-typed keys -> the identical message, ``Column 3: In chunk 1803``.
``large_string`` keys are NOT a fix: Arrow's grouper ABORTS the process
(``std::length_error``) already at 30M such groups. Integer-coded keys
group 200M distinct 5-key rows in ~20s. Hence the contract pinned here:

* the MERGE groups on fixed-width integer codes only;
* the PARSE OUTPUT (and the checkpoint) carries the three text columns as
  ``dictionary<int32, large_string>`` in a CANONICAL form (dictionary
  sorted, unused entries dropped), so a table is a pure function of its
  contents and ``.equals`` across worker counts and a checkpoint round trip;
* ``pa.string()`` is produced ONLY at the post-aggregation boundary
  (``aggregate_counts``), re-chunked so no chunk can exceed the 32-bit
  limit -- ``tensor``/``effort``/``smoothing`` demand ``pa.string()`` cells
  and never cast.

None of these tests allocates 2 GiB; the real >2 GiB reproduction is in
``test_offset_safety_slow.py`` (opt-in, ``BIRDOLOGY_SLOW=1``).
"""
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior import parallel_parse
from birdology.prior.indexing import aggregate_counts, assign_cells
from birdology.prior.leakage import (
    XENO_CANTO_PUBLISHER_KEYS,
    drop_xeno_canto,
    mark_guards_applied,
)
from birdology.prior.occurrences import read_occurrences

TEXT_COLUMNS = ("cell", "publisher_key", "recorded_by")
XC = next(iter(XENO_CANTO_PUBLISHER_KEYS))

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)
POINTS = [(55.6867, 12.5700), (55.9000, 12.5000), (56.1629, 10.2039),
          (57.0488, 9.9217), (-33.8688, 151.2093)]
DATES = ("2025-01-07", "2025-06-10", "2025-11-14", "2025-03-03")


def _archive(tmp_path, n=300):
    rows = []
    for i in range(n):
        lat, lon = POINTS[i % len(POINTS)]
        who = ["ole hansen", "Žofia", "", "obs\u200bX", "S\ufffdren"][i % 5]
        pub = ["pub-a", "", XC, "pub-b"][i % 4]
        sk = 1001 + (i % 6)
        rows.append("\t".join(
            [str(i), "ds", pub, str(sk), f"sp{sk}", str(lat + (i % 9) * 1e-3),
             str(lon), DATES[i % len(DATES)], who]
        ))
    path = tmp_path / "occ.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return path


def _arrow_strings():
    # Imported lazily so each test reports its own RED reason instead of one
    # collection error taking out the file.
    from birdology.prior import arrow_strings

    return arrow_strings


def _string_data_bytes(chunk: pa.Array) -> int:
    """Bytes of character data a string chunk's offsets address."""
    if len(chunk) == 0:
        return 0
    offsets = chunk.buffers()[1]
    import numpy as np

    width = np.int32 if chunk.type == pa.string() else np.int64
    view = np.frombuffer(offsets, dtype=width)[chunk.offset: chunk.offset + len(chunk) + 1]
    return int(view[-1] - view[0])


# --------------------------------------------------------------------------
# 1. The schema contract: no 32-bit-offset column inside the parse/merge.
# --------------------------------------------------------------------------


def test_parse_schema_carries_no_32bit_offset_column():
    """The overflow class, structurally: a ``pa.string``/``pa.binary``
    column in the parse schema is what wrapped negative at 2^31 bytes."""
    for field in parallel_parse.PARSE_SCHEMA:
        assert field.type not in (pa.string(), pa.binary()), field
    for name in TEXT_COLUMNS:
        assert parallel_parse.PARSE_SCHEMA.field(name).type == pa.dictionary(
            pa.int32(), pa.large_string()
        )


def test_the_merge_groups_on_fixed_width_keys_only():
    """``large_string`` keys abort Arrow's grouper at ~30M groups (measured),
    so 64-bit offsets alone would turn an exception into a process abort.
    Every merge column must be a fixed-width integer."""
    schema = parallel_parse.MERGE_SCHEMA
    assert schema.names == parallel_parse.PARSE_SCHEMA.names
    for field in schema:
        assert pa.types.is_integer(field.type), field


def test_compact_refuses_a_variable_width_column():
    """A regression that routes strings back into the merge must fail at
    the first compaction on a fixture, not at 2^31 bytes on the fleet."""
    table = pa.table(
        {
            "species_key": pa.array([1], pa.int64()),
            "cell": pa.array(["841f05bffffffff"], pa.string()),
            "week": pa.array([3], pa.int8()),
            "publisher_key": pa.array([0], pa.int32()),
            "recorded_by": pa.array([0], pa.int32()),
            "n": pa.array([1], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="cell"):
        parallel_parse._compact([table])


# --------------------------------------------------------------------------
# 2. The parse output is canonical: identical tables, not just equal rows.
# --------------------------------------------------------------------------


def test_parse_output_is_canonical_and_identical_across_worker_counts(tmp_path):
    archive = _archive(tmp_path)
    one = parallel_parse.parse_and_aggregate(archive, [3, 4], workers=1)
    four = parallel_parse.parse_and_aggregate(
        archive, [3, 4], workers=4, chunk_bytes=700
    )
    for resolution in (3, 4):
        a, b = one.tables[resolution], four.tables[resolution]
        assert a.schema == parallel_parse.PARSE_SCHEMA
        assert a.equals(b), resolution
        for name in TEXT_COLUMNS:
            column = a.column(name)
            dictionaries = {id(c.dictionary) for c in column.chunks}
            assert len(dictionaries) <= 1, "chunks must share ONE dictionary"
            values = column.chunk(0).dictionary.to_pylist()
            assert values == sorted(values), "dictionary must be sorted"
            used = set(pc.unique(column).drop_null().to_pylist())
            assert set(values) == used, "dictionary must hold used values only"


def test_parse_output_decodes_to_the_sequential_reference(tmp_path):
    """The values -- nulls, mojibake, zero-width spaces -- survive the
    integer coding exactly; compared row for row with read_occurrences."""
    archive = _archive(tmp_path)
    result = parallel_parse.parse_and_aggregate(archive, [4], workers=3,
                                                chunk_bytes=900)
    reference = assign_cells(read_occurrences(archive), 4)
    keys = ["species_key", "cell", "week", "publisher_key", "recorded_by"]
    expected: dict[tuple, int] = {}
    for row in reference.select(keys).to_pylist():
        key = tuple(row[k] for k in keys)
        expected[key] = expected.get(key, 0) + 1
    got = {
        tuple(r[k] for k in keys): r["n"]
        for r in result.tables[4].to_pylist()
    }
    assert got == expected
    assert None in {k[3] for k in got} and None in {k[4] for k in got}


# --------------------------------------------------------------------------
# 3. The boundary: pa.string only after aggregation, in bounded chunks.
# --------------------------------------------------------------------------


def test_decode_strings_rechunks_below_the_bound():
    strings = _arrow_strings()
    values = ["aaaa", None, "b", "ccccccc", "aaaa", "dd", None, "eeeeeeeeee"] * 7
    column = strings.canonical_dictionary(
        pa.chunked_array([pa.array(values[:20]), pa.array(values[20:])])
    )
    out = strings.decode_strings(column, max_chunk_bytes=16)
    assert out.type == pa.string()
    assert out.to_pylist() == values
    assert out.num_chunks > 2
    assert all(_string_data_bytes(c) <= 16 for c in out.chunks)


def test_decode_strings_refuses_a_bound_beyond_32bit_offsets():
    strings = _arrow_strings()
    column = strings.canonical_dictionary(pa.chunked_array([pa.array(["a"])]))
    with pytest.raises(ValueError, match="max_chunk_bytes"):
        strings.decode_strings(column, max_chunk_bytes=1 << 31)


def test_canonical_dictionary_is_a_function_of_the_values():
    strings = _arrow_strings()
    a = strings.canonical_dictionary(
        pa.chunked_array([pa.array(["z", None, "a"]), pa.array(["m", "a"])])
    )
    b = strings.canonical_dictionary(
        pa.chunked_array(
            [pa.array(["z", None, "a", "m", "a", "unused"], pa.large_string())
             .dictionary_encode().slice(0, 5)]
        )
    )
    assert a.type == b.type == pa.dictionary(pa.int32(), pa.large_string())
    assert a.combine_chunks().equals(b.combine_chunks())
    assert a.chunk(0).dictionary.to_pylist() == ["a", "m", "z"]


def test_text_to_pylist_decodes_a_shared_dictionary_once():
    """Thousands of chunks (a guard filter, a group_by output) share one
    dictionary of up to millions of values; decoding it per CHUNK would be
    chunks x vocabulary work. Shared decode is observable as shared str
    objects across chunks."""
    strings = _arrow_strings()
    vocabulary = pa.array(["alpha" * 3, "beta" * 3], pa.large_string())
    chunks = [
        pa.DictionaryArray.from_arrays(pa.array([0, 1, None], pa.int32()),
                                       vocabulary)
        for _ in range(3)
    ]
    out = strings.text_to_pylist(pa.chunked_array(chunks))
    assert out == ["alpha" * 3, "beta" * 3, None] * 3
    assert out[0] is out[3] is out[6]
    # An EQUAL but distinct dictionary object (a parquet read) shares too.
    rebuilt = pa.DictionaryArray.from_arrays(
        pa.array([1], pa.int32()),
        pa.array(["alpha" * 3, "beta" * 3], pa.large_string()),
    )
    again = strings.text_to_pylist(pa.chunked_array(chunks[:1] + [rebuilt]))
    assert again[1] is again[3]


def test_decode_strings_handles_many_chunks_sharing_one_dictionary():
    strings = _arrow_strings()
    vocabulary = pa.array([f"cell{i:06d}" for i in range(1000)], pa.large_string())
    chunks = [
        pa.DictionaryArray.from_arrays(
            pa.array([i % 1000, (i * 7) % 1000], pa.int32()), vocabulary
        )
        for i in range(500)
    ]
    out = strings.decode_strings(pa.chunked_array(chunks), max_chunk_bytes=1000)
    assert out.to_pylist() == [
        f"cell{v:06d}" for i in range(500) for v in (i % 1000, (i * 7) % 1000)
    ]
    assert all(_string_data_bytes(c) <= 1000 for c in out.chunks)
    # Pieces are packed ACROSS input chunks, not one-or-more per chunk.
    assert out.num_chunks <= 500 * 2 * 10 // 1000 + 1


def test_aggregate_counts_emits_pa_string_in_bounded_chunks(tmp_path, monkeypatch):
    """``tensor._REQUIRED_TYPES`` demands ``cell: pa.string()`` and never
    casts, so the narrowing is upstream -- here -- and bounded: at fleet
    scale an unbounded ``.cast(pa.string())`` is the same overflow again."""
    strings = _arrow_strings()
    monkeypatch.setattr(strings, "MAX_STRING_CHUNK_BYTES", 64)
    archive = _archive(tmp_path)
    parsed = parallel_parse.parse_and_aggregate(archive, [4], workers=1)
    counts = aggregate_counts(mark_guards_applied(parsed.tables[4]))
    assert counts.schema.field("cell").type == pa.string()
    cell = counts.column("cell")
    assert cell.num_chunks > 1
    assert all(_string_data_bytes(c) <= 64 for c in cell.chunks)

    direct = aggregate_counts(
        mark_guards_applied(assign_cells(read_occurrences(archive), 4))
    )
    key = ["species_key", "cell", "week", "n"]
    assert sorted(tuple(r[k] for k in key) for r in counts.to_pylist()) == sorted(
        tuple(r[k] for k in key) for r in direct.to_pylist()
    )


def test_aggregate_counts_bounds_a_plain_string_input_too(tmp_path, monkeypatch):
    """Job 6787_5: the SEQUENTIAL path hands ``assign_cells``' plain
    ``pa.string()`` cell column straight to ``group_by``, whose string output
    at r5 crossed 2 GiB in one chunk and wrote negative offsets
    (``ArrowInvalid: Column 1: In chunk 4367: Invalid: Negative offsets in
    binary array`` from the counts-checkpoint parquet write). A plain string
    input must take the same integer-code route and bounded decode as the
    dictionary input, with the same counts."""
    strings = _arrow_strings()
    monkeypatch.setattr(strings, "MAX_STRING_CHUNK_BYTES", 64)
    archive = _archive(tmp_path)
    sequential = mark_guards_applied(assign_cells(read_occurrences(archive), 4))
    assert sequential.schema.field("cell").type == pa.string()
    counts = aggregate_counts(sequential)
    assert counts.schema.field("cell").type == pa.string()
    cell = counts.column("cell")
    assert cell.num_chunks > 1
    assert all(_string_data_bytes(c) <= 64 for c in cell.chunks)

    parsed = parallel_parse.parse_and_aggregate(archive, [4], workers=1)
    reference = aggregate_counts(mark_guards_applied(parsed.tables[4]))
    key = ["species_key", "cell", "week", "n"]
    assert sorted(tuple(r[k] for k in key) for r in counts.to_pylist()) == sorted(
        tuple(r[k] for k in key) for r in reference.to_pylist()
    )


def test_aggregate_counts_accepts_chunks_with_differing_dictionaries():
    """A parquet read or a filter can hand over dictionary chunks whose
    dictionaries differ; Arrow's group_by refuses those outright
    (``Unifying differing dictionaries``)."""
    a = pa.array(["c2", "c1"], pa.large_string()).dictionary_encode()
    b = pa.array(["c1", "c3", "c1"], pa.large_string()).dictionary_encode()
    table = mark_guards_applied(
        pa.table(
            {
                "species_key": pa.array([1, 1, 1, 1, 2], pa.int64()),
                "cell": pa.chunked_array([a, b]),
                "week": pa.array([5] * 5, pa.int8()),
                "n": pa.array([1, 2, 3, 4, 5], pa.int64()),
            }
        )
    )
    counts = aggregate_counts(table)
    assert counts.schema.field("cell").type == pa.string()
    assert sorted(
        (r["species_key"], r["cell"], r["n"]) for r in counts.to_pylist()
    ) == [(1, "c1", 2 + 3), (1, "c2", 1), (1, "c3", 4), (2, "c1", 5)]


# --------------------------------------------------------------------------
# 5. A REAL >2 GiB reproduction that costs ~0.3s and ~350 MB: K chunks
#    sharing ONE lazily-zeroed buffer. Logical text is 2.25 GiB, so any
#    step that concatenates the chunks' strings overflows exactly as the
#    fleet did; physically it is one 256 MiB calloc'd buffer.
# --------------------------------------------------------------------------


def _shared_buffer_column(chunks: int = 9, row_bytes: int = 1024):
    import numpy as np

    size = 256 << 20
    rows = size // row_bytes
    buffer = pa.py_buffer(np.zeros(size, dtype=np.uint8))
    offsets = pa.py_buffer(
        (np.arange(rows + 1, dtype=np.int32) * row_bytes).tobytes()
    )
    chunk = pa.Array.from_buffers(pa.string(), rows, [None, offsets, buffer])
    column = pa.chunked_array([chunk] * chunks)
    assert chunks * size > (1 << 31), "fixture must exceed int32 offsets"
    return column


def test_the_shared_buffer_fixture_really_overflows_a_concatenation():
    """Control: proves the fixture is in the overflow class at all."""
    with pytest.raises(pa.ArrowInvalid, match="offset overflow"):
        _shared_buffer_column().combine_chunks()


def test_dictionary_codes_never_concatenates_the_strings():
    strings = _arrow_strings()
    codes, distinct = strings.dictionary_codes(_shared_buffer_column())
    assert distinct.to_pylist() == ["\x00" * 1024]
    assert len(codes) == 9 * ((256 << 20) // 1024)
    assert int(codes.max()) == 0


def test_dictionary_codes_equal_the_combined_encoding():
    """Same codes, same first-occurrence order, as the concatenate-then-
    encode it replaces -- so tensor/smoothing output cannot move."""
    strings = _arrow_strings()
    column = pa.chunked_array(
        [pa.array(["c", "a", "c"]), pa.array([], pa.string()),
         pa.array(["b", "a", "d"])]
    )
    codes, distinct = strings.dictionary_codes(column)
    expected = column.combine_chunks().dictionary_encode()
    assert distinct.to_pylist() == expected.dictionary.to_pylist()
    assert codes.tolist() == expected.indices.to_pylist()


def test_to_tensor_packs_a_cell_column_beyond_2gib():
    """Post-aggregation landmine found while fixing 5722: ``to_tensor``
    concatenated the whole ``cell`` column before encoding it, so a full
    grid of more than ~143M contexts at 15-byte cells (2 GiB) -- about
    38 GB of projected peak, inside the fleet node's budget -- would have
    died after the parse, the guards and the backoff had all been paid
    for. Distinct species keep every (species, cell, week) unique."""
    import numpy as np

    from birdology.prior import tensor

    # 64 KiB rows: 2.25 GiB of text in only 36,864 rows, so the fixture
    # stays cheap for every tensor mutant too (T25's first-seen-order
    # mutant is quadratic in the row count and timed out at 2.4M rows).
    cell = _shared_buffer_column(row_bytes=64 << 10)
    n = len(cell)
    context = np.arange(n, dtype=np.int64)
    table = pa.table(
        {
            "species_key": pa.array(context // 52),
            "cell": cell,
            "week": pa.array((context % 52 + 1).astype(np.int8)),
            "p": pa.array(np.full(n, 0.5)),
        }
    )
    prior = tensor.to_tensor(
        table, tensor.PriorSpec(resolution=5, source="eod", variant="full")
    )
    assert prior.cells == ["\x00" * (64 << 10)]
    assert prior.values.nnz == n


def test_smoothing_codes_never_concatenate_the_strings():
    """``smooth`` coded its cell column via ``combine_chunks`` too. Its
    input is one species chunk of the full grid (50 species x cells x 52),
    which crosses 2 GiB at ~55k cells."""
    from birdology.prior import smoothing

    codes, cells = smoothing._codes(_shared_buffer_column())
    assert cells == ["\x00" * 1024]
    assert len(codes) == 9 * ((256 << 20) // 1024)


# --------------------------------------------------------------------------
# 4. The guards read every string layout the parse can hand them.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "layout",
    ["string", "large_string", "dict_string", "dict_large_string"],
)
def test_xeno_canto_guard_matches_every_string_layout(layout):
    """Explicit, not left to Arrow's implicit is_in coercion: the value set
    is built in the column's own value type. Null publisher stays KEPT."""
    values = [XC, "pub-a", None, XC]
    base = pa.large_string() if "large" in layout else pa.string()
    column = pa.array(values, base)
    if layout.startswith("dict"):
        column = column.dictionary_encode()
    table = pa.table({"publisher_key": column, "n": pa.array([1, 2, 3, 4])})
    kept = drop_xeno_canto(table)
    assert kept.column("n").to_pylist() == [2, 3]
    assert kept.schema == table.schema
