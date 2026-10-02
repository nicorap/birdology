"""Real >2 GiB reproductions of the job-5722 overflow class (opt-in, slow).

Skipped unless ``BIRDOLOGY_SLOW=1``: each test allocates 10-25 GB and runs
for tens of seconds. ``test_offset_safety.py`` pins the same contracts on
small fixtures by default; these prove the contracts are the RIGHT ones by
running the real size class once.

    BIRDOLOGY_SLOW=1 uv run pytest tests/prior/test_offset_safety_slow.py -s
"""
import os
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

pytestmark = pytest.mark.skipif(
    os.environ.get("BIRDOLOGY_SLOW") != "1",
    reason="allocates >2 GiB of string data; set BIRDOLOGY_SLOW=1",
)

KEYS = ["species_key", "cell", "week", "publisher_key", "recorded_by"]


def _fixed_width_strings(ids: np.ndarray, width: int, type_=pa.string()):
    """One string per id: the id's low hex digits, right-aligned, x-padded.

    Distinct for distinct ids below 16**min(width, 16).
    """
    count = len(ids)
    body = np.full((count, width), ord("x"), dtype=np.uint8)
    ids = ids.astype(np.uint64)
    for k in range(min(width, 16)):
        nibble = (ids >> np.uint64(4 * k)) & np.uint64(15)
        body[:, width - 1 - k] = np.where(
            nibble < 10, 48 + nibble, 87 + nibble
        ).astype(np.uint8)
    offset_type = np.int64 if type_ == pa.large_string() else np.int32
    offsets = np.arange(count + 1, dtype=offset_type) * width
    return pa.Array.from_buffers(
        type_, count, [None, pa.py_buffer(offsets.tobytes()),
                       pa.py_buffer(body.tobytes())]
    )


def _chunked(ids: np.ndarray, width: int, step: int = 4_000_000):
    return pa.chunked_array(
        [_fixed_width_strings(ids[s:s + step], width)
         for s in range(0, len(ids), step)],
        type=pa.string(),
    )


N_GROUPS = 62_000_000  # the size that reproduced job 5722's message locally


def _five_key_ids(n: int):
    ids = np.arange(n, dtype=np.int64)
    return {
        "species_key": ids // 50,
        "cell": ids % 2_000_000,
        "week": (ids % 52).astype(np.int8),
        "publisher_key": ids % 5_000,
        "recorded_by": ids % 1_000_000,
    }


def test_job_5722_reproduces_with_string_keys():
    """The fleet failure, on this machine: 62M distinct 5-key groups whose
    publisher_key is a 36-byte string. Kept as a test so a future pyarrow
    that fixes the grouper is NOTICED (this test then fails and says so)."""
    ids = _five_key_ids(N_GROUPS)
    table = pa.table(
        {
            "species_key": pa.array(ids["species_key"]),
            "cell": _chunked(ids["cell"], 15),
            "week": pa.array(ids["week"]),
            "publisher_key": _chunked(ids["publisher_key"], 36),
            "recorded_by": _chunked(ids["recorded_by"], 12),
            "n": pa.array(np.ones(N_GROUPS, dtype=np.int64)),
        }
    )
    with pytest.raises(pa.ArrowInvalid, match="Negative offsets"):
        grouped = table.group_by(KEYS).aggregate([("n", "sum")])
        grouped.validate(full=True)


def test_the_coded_merge_survives_the_job_5722_size():
    """The same 62M distinct groups through the real ``_compact`` +
    ``_finalise``: completes, validates, and every text value decodes."""
    from birdology.prior import parallel_parse as pp

    ids = _five_key_ids(N_GROUPS)
    vocabularies = {name: pp._Vocabulary() for name in pp.TEXT_COLUMNS}
    widths = {"cell": 15, "publisher_key": 36, "recorded_by": 12}
    sizes = {"cell": 2_000_000, "publisher_key": 5_000, "recorded_by": 1_000_000}
    for name in pp.TEXT_COLUMNS:
        values = _fixed_width_strings(np.arange(sizes[name]), widths[name])
        vocabularies[name].codes_for(values)
    merge = pa.Table.from_arrays(
        [
            pa.array(ids["species_key"]),
            pa.array(ids["cell"].astype(np.int32)),
            pa.array(ids["week"]),
            pa.array(ids["publisher_key"].astype(np.int32)),
            pa.array(ids["recorded_by"].astype(np.int32)),
            pa.array(np.ones(N_GROUPS, dtype=np.int64)),
        ],
        schema=pp.MERGE_SCHEMA,
    )
    del ids
    half = N_GROUPS // 2
    started = time.time()
    out = pp._finalise([merge.slice(0, half), merge.slice(half)], vocabularies)
    elapsed = time.time() - started
    out.validate(full=True)
    assert out.num_rows == N_GROUPS
    assert out.schema == pp.PARSE_SCHEMA
    assert pa.compute.sum(out.column("n")).as_py() == N_GROUPS
    print(f"\n_finalise over {N_GROUPS:,} distinct groups: {elapsed:.1f}s")


def test_to_tensor_packs_a_realistic_cell_column_beyond_2gib():
    """The realistic-shape companion of the default-suite shared-buffer
    test: 150M grid contexts over ~2.9M DISTINCT 15-byte cells = 2.1 GiB of
    cell text. Before the fix: "offset overflow while concatenating
    arrays" in ``to_tensor``'s ``combine_chunks``. 150M contexts is ~40 GB
    of projected peak, well inside the fleet node's budget."""
    from birdology.prior import tensor

    n = 150_000_000
    n_cells = 3_000_000
    context = np.arange(n, dtype=np.int64)
    cell_ids = (context // 52) % n_cells
    table = pa.table(
        {
            "species_key": pa.array(context // (52 * n_cells)),
            "cell": _chunked(cell_ids, 15, step=20_000_000),
            "week": pa.array((context % 52 + 1).astype(np.int8)),
            "p": pa.array(np.full(n, 0.5)),
        }
    )
    del context, cell_ids
    prior = tensor.to_tensor(
        table, tensor.PriorSpec(resolution=5, source="eod", variant="full")
    )
    assert prior.values.nnz == n
    assert len(prior.cells) == min(n_cells, -(-n // 52))
    assert prior.cells == sorted(prior.cells)
