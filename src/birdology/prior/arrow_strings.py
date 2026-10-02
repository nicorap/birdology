"""Offset-safe text columns for the prior pipeline (job 5722).

THE FAILURE THIS MODULE EXISTS FOR
----------------------------------
Fleet job 5722 (EOD, 1.76e9 rows, 90 workers) died six minutes into the
parse in ``parallel_parse._compact``::

    ArrowInvalid: Column 3: In chunk 1725: Invalid: Negative offsets in
    binary array

``pa.string()`` addresses its character data with INT32 offsets, so one
chunk can hold at most 2^31 - 1 bytes of text. The merge's hash-aggregate
emitted publisher_key (a 36-byte UUID per group) for hundreds of millions of
groups in one chunk, the offsets wrapped negative, and Arrow noticed only at
the final ``.cast``'s validation. Reproduced locally on pyarrow 25.0.1 with
62M distinct 5-key groups: the identical message, ``Column 3``.

WHY NOT SIMPLY ``pa.large_string()`` (measured, not assumed)
------------------------------------------------------------
64-bit offsets remove the wrap, but Arrow's grouper itself does not survive
large variable-width keys: a 5-key ``group_by`` with ``large_string`` keys
ABORTED THE PROCESS (``libc++abi: std::length_error: vector``) at 30M
groups, where ``pa.string`` keys merely raised. An abort is worse than the
exception: no traceback, no checkpoint, nothing to triage. Integer keys, by
contrast, grouped 200M distinct 5-key rows in ~20s. So the design is:

* text never enters a ``group_by`` key at scale -- the parse merge groups on
  int32 CODES (``parallel_parse.MERGE_SCHEMA``), and ``aggregate_counts``
  groups on dictionary indices;
* text at rest (the parse output, the checkpoint) is
  ``dictionary<int32, large_string>`` -- :data:`TEXT_TYPE` -- in the
  CANONICAL form :func:`canonical_dictionary` defines;
* ``pa.string()`` is produced in exactly one place, :func:`decode_strings`,
  AFTER aggregation, in chunks bounded by :data:`MAX_STRING_CHUNK_BYTES`.
  ``tensor``/``effort``/``smoothing``/``backoff`` demand ``pa.string()``
  cells and deliberately never cast, so the narrowing belongs upstream of
  them, where the data is already small.

A cast is never the narrowing step: ``large_string -> string`` refuses an
array over 2 GiB ("input array too large"), and -- measured -- refuses even a
SMALL SLICE of such an array, because a slice keeps the parent's absolute
offsets. :func:`decode_strings` therefore re-materialises each bounded piece
with ``take`` (which rebases offsets) before casting it.
"""
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

#: The at-rest type of every text column of the parse output and checkpoint.
#: int32 indices: there are at most a few million distinct cells, publishers
#: or contributors; the dictionary values are 64-bit-offset text.
TEXT_TYPE = pa.dictionary(pa.int32(), pa.large_string())

#: Upper bound on the character data of one ``pa.string()`` chunk that
#: :func:`decode_strings` emits. Far below the 2^31 - 1 hard limit so that a
#: downstream concatenation of a few chunks is still safe; large enough that
#: chunk count stays negligible (a 2 GiB cell column is ~32 chunks). Read at
#: CALL time, so tests can shrink it.
MAX_STRING_CHUNK_BYTES = 64 << 20

#: The hard ceiling a ``pa.string()`` chunk can address.
_INT32_OFFSET_LIMIT = (1 << 31) - 1


def _as_chunked(column: pa.Array | pa.ChunkedArray) -> pa.ChunkedArray:
    if isinstance(column, pa.ChunkedArray):
        return column
    return pa.chunked_array([column], type=column.type)


def _is_text(type_: pa.DataType) -> bool:
    return pa.types.is_string(type_) or pa.types.is_large_string(type_)


def _same_values(a: pa.Array, b: pa.Array) -> bool:
    """Do two dictionaries hold the same values? Cheap when they share memory.

    ``DictionaryArray.dictionary`` returns a NEW Python wrapper on every
    access, so ``is`` never matches even for chunks that share one Arrow
    dictionary. Buffer identity (same address, same length) does, in O(1);
    ``equals`` is the fallback for equal dictionaries that do not share
    memory (a parquet reader rebuilds one per row group).
    """
    if a.type == b.type and len(a) == len(b) and a.offset == b.offset:
        buffers_a, buffers_b = a.buffers(), b.buffers()
        if all(
            (x is None and y is None)
            or (x is not None and y is not None and x.address == y.address
                and x.size == y.size)
            for x, y in zip(buffers_a, buffers_b)
        ):
            return True
    return a.equals(b)


def _dictionary_chunks(column: pa.ChunkedArray) -> list[pa.DictionaryArray]:
    """Every chunk as a DictionaryArray with large_string values.

    Plain text chunks are dictionary-encoded PER CHUNK (each chunk is
    already within its own offset limit, so encoding it is safe). Dictionary
    values are widened to ``large_string`` (the widening cast cannot fail)
    ONCE per distinct dictionary: chunks whose dictionary is the same object
    as, or equal to, the previous chunk's reuse the previous result, so the
    thousands of chunks of a group_by output or a parquet read that share
    one multi-million-entry dictionary keep sharing one object.
    """
    type_ = column.type
    if pa.types.is_dictionary(type_):
        if not _is_text(type_.value_type):
            raise TypeError(f"expected a text dictionary, got {type_}")
    elif not _is_text(type_):
        raise TypeError(f"expected a text column, got {type_}")

    chunks: list[pa.DictionaryArray] = []
    seen_in = None
    seen_out = None
    for chunk in column.chunks:
        if not pa.types.is_dictionary(chunk.type):
            chunk = chunk.dictionary_encode()
        dictionary = chunk.dictionary
        if seen_in is not None and _same_values(dictionary, seen_in):
            widened = seen_out
        else:
            widened = (
                dictionary
                if dictionary.type == pa.large_string()
                else dictionary.cast(pa.large_string())
            )
            seen_in, seen_out = dictionary, widened
        chunks.append(
            pa.DictionaryArray.from_arrays(chunk.indices.cast(pa.int32()), widened)
        )
    return chunks


def _runs(column: pa.ChunkedArray) -> list[tuple[pa.Array, pa.Array]]:
    """``(dictionary, int32 indices)`` per maximal run of chunks holding one
    dictionary, the run's indices concatenated.

    Concatenating INDICES is always offset-safe (fixed-width), and it is
    what lets per-dictionary work -- decoding the vocabulary, measuring its
    values -- happen once per run rather than once per chunk.
    """
    runs: list[tuple[pa.Array, list[pa.Array]]] = []
    for chunk in _dictionary_chunks(column):
        dictionary = chunk.dictionary
        if runs and _same_values(dictionary, runs[-1][0]):
            runs[-1][1].append(chunk.indices)
        else:
            runs.append((dictionary, [chunk.indices]))
    return [
        (
            dictionary,
            parts[0] if len(parts) == 1 else pa.concat_arrays(parts),
        )
        for dictionary, parts in runs
    ]


def canonical_dictionary(column: pa.Array | pa.ChunkedArray) -> pa.ChunkedArray:
    """*column* as :data:`TEXT_TYPE` in canonical form.

    CANONICAL means: every chunk shares ONE dictionary, that dictionary is
    sorted (byte order, which for UTF-8 is code-point order -- the order
    Python's ``sorted`` and Arrow's ``sort_by`` both use), and it holds
    exactly the non-null values in use. Nulls stay null indices.

    Three properties follow and are relied on:

    * the encoding is a FUNCTION OF THE VALUES, so two tables holding the
      same rows compare ``.equals`` whatever order chunks arrived in, which
      dictionary a parquet reader rebuilt, or which resolution's values the
      checkpoint's shared dictionary also carried;
    * Arrow's ``group_by`` accepts it (it refuses chunks with differing
      dictionaries: "Unifying differing dictionaries");
    * sorting by the INDICES sorts by the strings, so a table can be ordered
      on integer columns only.

    Accepts plain ``string``/``large_string`` or any text dictionary.
    """
    runs = _runs(_as_chunked(column))

    used_values = []
    for dictionary, indices in runs:
        used = pc.unique(indices).drop_null()
        if len(used):
            used_values.append(dictionary.take(used))
    if used_values:
        vocabulary = pc.unique(pa.concat_arrays(used_values)).drop_null()
        vocabulary = vocabulary.take(pc.sort_indices(vocabulary))
    else:
        vocabulary = pa.array([], pa.large_string())

    out: list[pa.DictionaryArray] = []
    for dictionary, indices in runs:
        remap = pc.index_in(dictionary, value_set=vocabulary)
        out.append(
            pa.DictionaryArray.from_arrays(
                remap.take(indices).cast(pa.int32()), vocabulary
            )
        )
    return pa.chunked_array(out, type=TEXT_TYPE)


def dictionary_from_codes(
    codes: pa.Array | pa.ChunkedArray, vocabulary: pa.Array
) -> pa.ChunkedArray:
    """Canonical :data:`TEXT_TYPE` column from integer codes into *vocabulary*.

    *vocabulary* is the code -> string table (``large_string``); null codes
    stay null. The result is canonical, so the run-specific code assignment
    (which depends on chunk ARRIVAL order) leaves no trace in it.
    """
    vocabulary = vocabulary.cast(pa.large_string())
    chunked = _as_chunked(codes)
    shared = [
        pa.DictionaryArray.from_arrays(chunk.cast(pa.int32()), vocabulary)
        for chunk in chunked.chunks
    ]
    return canonical_dictionary(
        pa.chunked_array(shared, type=TEXT_TYPE)
        if shared
        else pa.chunked_array([], type=TEXT_TYPE)
    )


def decode_strings(
    column: pa.Array | pa.ChunkedArray, max_chunk_bytes: int | None = None
) -> pa.ChunkedArray:
    """*column* as ``pa.string()``, no chunk holding more than the bound.

    THE ONE NARROWING POINT. Every chunk is cut so its character data is at
    most *max_chunk_bytes* (default :data:`MAX_STRING_CHUNK_BYTES`, read at
    call time) and re-materialised with ``take`` -- which rebases offsets --
    before the cast, because a cast of a slice of a >2 GiB array fails even
    when the slice is tiny (measured). A single value larger than the bound
    cannot be represented in a bounded chunk and raises rather than emitting
    an oversized one.
    """
    bound = MAX_STRING_CHUNK_BYTES if max_chunk_bytes is None else max_chunk_bytes
    if not 0 < bound <= _INT32_OFFSET_LIMIT:
        raise ValueError(
            f"max_chunk_bytes must be in 1..{_INT32_OFFSET_LIMIT} (the most "
            f"character data an int32-offset pa.string chunk can address), "
            f"got {bound}"
        )
    pieces: list[pa.Array] = []
    for dictionary, indices in _runs(_as_chunked(column)):
        if len(indices) == 0:
            continue
        value_lengths = pc.binary_length(dictionary).to_numpy(
            zero_copy_only=False
        ).astype(np.int64)
        valid = ~np.asarray(
            indices.is_null().to_numpy(zero_copy_only=False), dtype=bool
        )
        positions = indices.fill_null(0).to_numpy(zero_copy_only=False)
        row_bytes = np.where(valid, value_lengths[positions], 0)
        if row_bytes.max() > bound:
            raise ValueError(
                f"a single value of {int(row_bytes.max())} bytes exceeds the "
                f"{bound}-byte pa.string chunk bound"
            )
        cumulative = np.cumsum(row_bytes)
        start = 0
        while start < len(indices):
            base = int(cumulative[start - 1]) if start else 0
            stop = int(np.searchsorted(cumulative, base + bound, side="right"))
            stop = max(stop, start + 1)
            piece = dictionary.take(indices.slice(start, stop - start))
            pieces.append(piece.cast(pa.string()))
            start = stop
    return pa.chunked_array(pieces, type=pa.string())


def dictionary_codes(
    column: pa.Array | pa.ChunkedArray,
) -> tuple[np.ndarray, pa.Array]:
    """``(codes, distinct_values)`` without concatenating the strings.

    The offset-safe replacement for ``column.combine_chunks()
    .dictionary_encode()``: ``combine_chunks`` of a ``pa.string`` column
    whose total text passes 2 GiB raises "offset overflow while
    concatenating arrays". ``ChunkedArray.dictionary_encode`` builds ONE
    dictionary shared by every chunk, in first-occurrence order -- the same
    order the combined encode produced, so callers see identical codes --
    and concatenating dictionary chunks that share a dictionary moves only
    the int32 indices.
    """
    chunked = _as_chunked(column)
    if pa.types.is_dictionary(chunked.type):
        raise TypeError("dictionary_codes expects a plain text column")
    encoded = chunked.dictionary_encode()
    if encoded.num_chunks == 0:
        return np.zeros(0, dtype=np.int64), pa.array([], chunked.type)
    combined = encoded.combine_chunks()
    return (
        np.asarray(
            combined.indices.to_numpy(zero_copy_only=False), dtype=np.int64
        ),
        combined.dictionary,
    )


def text_to_pylist(column: pa.Array | pa.ChunkedArray) -> list[str | None]:
    """Python values of a text column, plain or dictionary-encoded.

    Identical to ``column.to_pylist()`` for every layout. For a dictionary
    column it decodes each DISTINCT value once and shares the ``str``
    objects across rows: measured on 10M rows over 200k distinct values,
    ``DictionaryArray.to_pylist()`` takes 5.3s against 0.29s here, and the
    shared objects make the guard phase's three per-row lists far smaller
    than one fresh ``str`` per row.
    """
    chunked = _as_chunked(column)
    if not pa.types.is_dictionary(chunked.type):
        return chunked.to_pylist()
    out: list[str | None] = []
    for dictionary, indices in _runs(chunked):
        values = dictionary.to_pylist()
        if indices.null_count:
            out.extend(
                None if i is None else values[i] for i in indices.to_pylist()
            )
        else:
            out.extend(
                map(values.__getitem__,
                    indices.to_numpy(zero_copy_only=False).tolist())
            )
    return out


def value_type(column: pa.Array | pa.ChunkedArray) -> pa.DataType:
    """The type of the VALUES a text column holds, through any dictionary."""
    type_ = column.type
    return type_.value_type if pa.types.is_dictionary(type_) else type_
