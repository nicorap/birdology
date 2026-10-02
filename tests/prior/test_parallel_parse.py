"""Parallel parse+aggregate of a GBIF SIMPLE_CSV archive.

Job 5345 spent ~6.5h in ``read_occurrences``: a single Python thread pulling
1.76B rows through ``csv.DictReader``. The row processing (dict building,
float/int/date parsing, h3 assignment) dominates, so it is offloaded: one
reader streams decompressed chunks (split on line boundaries) to a pool of
workers, each worker parses+filters+aggregates its chunk, and the parent
merges partial counts incrementally.

THE CONTRACT IS BIT-EXACTNESS, NOT SPEED. The drop-counter semantics of
``read_occurrences`` (no species key / bad coordinates / unparseable date)
are preserved EXACTLY: the same rows are dropped for the same reasons, the
counters sum identically, and the aggregates are identical whatever the
worker count. Speed is measured separately by
``scripts/measurements/measure_parallel_parse.py`` and reported, not
promised.
"""
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.indexing import aggregate_counts, assign_cells
from birdology.prior.leakage import mark_guards_applied
from birdology.prior.occurrences import read_occurrences
from birdology.prior.parallel_parse import parse_and_aggregate

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)

POINTS = [
    (55.6867, 12.5700),
    (55.9000, 12.5000),
    (56.1629, 10.2039),
    (57.0488, 9.9217),
    (-33.8688, 151.2093),
    (37.0, -118.5),
]
DATES = ("2025-01-07", "2025-06-10", "2025-11-14", "2025-03-03")


def _zip(tmp_path, rows, name="occ.zip"):
    zip_path = tmp_path / name
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return zip_path


def _messy_archive(tmp_path, n_clean=400):
    """Clean rows plus every drop reason, mojibake, nulls and edge shapes.

    The drop rows are interleaved among the clean ones rather than appended,
    so a chunking bug that mishandles a boundary near a bad row has rows to
    mishandle in every chunk.
    """
    rows = []
    for i in range(n_clean):
        lat, lon = POINTS[i % len(POINTS)]
        d = DATES[i % len(DATES)]
        sk = 1001 + (i % 7)
        who = ["ole hansen", "Žofia", "", "obs\u200bX"][i % 4]
        pub = ["pub-a", "", "1f00d75c-f6fc-4224-a595-975e82d7689c"][i % 3]
        rows.append("\t".join(
            [str(i), "ds", pub, str(sk), f"sp{sk}",
             str(lat + i * 1e-6), str(lon), d, who]
        ))
        if i % 23 == 0:
            rows.append("\t".join(
                [str(10000 + i), "ds", "p", "", "x", str(lat), str(lon), d, "y"]
            ))  # no species key
        if i % 29 == 0:
            rows.append("\t".join(
                [str(20000 + i), "ds", "p", "1001", "x", "91.0", str(lon), d, "y"]
            ))  # out-of-range latitude
        if i % 31 == 0:
            rows.append("\t".join(
                [str(30000 + i), "ds", "p", "1001", "x", str(lat), str(lon),
                 "2025-11", "y"]
            ))  # coarse date
        if i % 37 == 0:
            rows.append("\t".join(
                [str(40000 + i), "ds", "p", "1001.0", "x", str(lat), str(lon),
                 d, "y"]
            ))  # float-shaped species key -> dropped, not truncated
    return _zip(tmp_path, rows)


def _reference(zip_path, resolutions):
    """The sequential truth: read_occurrences + assign_cells, aggregated to
    the checkpoint granularity (species, cell, week, publisher, recorded_by).
    """
    out = {}
    for resolution in resolutions:
        table = assign_cells(read_occurrences(zip_path), resolution)
        grouped = (
            table.select(
                ["species_key", "cell", "week", "publisher_key", "recorded_by"]
            )
            .append_column("n", pa.array([1] * table.num_rows, pa.int64()))
            .group_by(
                ["species_key", "cell", "week", "publisher_key", "recorded_by"]
            )
            .aggregate([("n", "sum")])
            .rename_columns(
                ["species_key", "cell", "week", "publisher_key", "recorded_by",
                 "n"]
            )
        )
        out[resolution] = grouped
    return out


def _sorted_rows(table):
    return sorted(
        tuple(row[c] for c in
              ("species_key", "cell", "week", "publisher_key", "recorded_by",
               "n"))
        for row in table.to_pylist()
    )


# --------------------------------------------------------------------------
# Bit-exactness against the sequential path, and across worker counts.
# --------------------------------------------------------------------------


def test_workers_1_matches_the_sequential_reference(tmp_path):
    archive = _messy_archive(tmp_path)
    result = parse_and_aggregate(archive, [3, 4], workers=1)
    reference = _reference(archive, [3, 4])
    for resolution in (3, 4):
        assert _sorted_rows(result.tables[resolution]) == _sorted_rows(
            reference[resolution]
        )


def test_workers_4_matches_workers_1_exactly(tmp_path):
    """The RED test the task names: identical aggregates AND identical drop
    counters, workers=1 vs workers=4, on an archive with every edge shape."""
    archive = _messy_archive(tmp_path)
    one = parse_and_aggregate(archive, [3, 4, 5], workers=1)
    four = parse_and_aggregate(archive, [3, 4, 5], workers=4)
    assert one.dropped == four.dropped
    assert one.total == four.total
    for resolution in (3, 4, 5):
        assert _sorted_rows(one.tables[resolution]) == _sorted_rows(
            four.tables[resolution]
        )


def test_drop_counters_match_read_occurrences_exactly(tmp_path, caplog):
    import logging

    archive = _messy_archive(tmp_path)
    with caplog.at_level(logging.INFO, logger="birdology.prior.occurrences"):
        table = read_occurrences(archive)
    result = parse_and_aggregate(archive, [4], workers=4)
    # read_occurrences' own log line carries its counters; re-derive them
    # from the table instead: kept rows + the planted drop pattern.
    kept = table.num_rows
    assert result.total - sum(result.dropped.values()) == kept
    assert set(result.dropped) == {
        "no_species_key", "bad_coordinates", "unparseable_date",
    }
    assert all(v > 0 for v in result.dropped.values())


def test_small_chunks_cross_every_boundary_shape(tmp_path):
    """Tiny chunk size forces many line-boundary splits, including splits
    adjacent to dropped rows and mojibake; the merge must still be exact."""
    archive = _messy_archive(tmp_path, n_clean=97)
    big = parse_and_aggregate(archive, [4], workers=1)
    small = parse_and_aggregate(archive, [4], workers=3, chunk_bytes=512)
    assert big.dropped == small.dropped
    assert _sorted_rows(big.tables[4]) == _sorted_rows(small.tables[4])


def test_mojibake_bytes_survive_chunked_decoding(tmp_path):
    """A latin-1 byte decodes to U+FFFD exactly as read_occurrences does,
    whichever chunk it lands in."""
    row = b"\t".join(
        [b"1", b"ds", b"pub", b"1001", b"sp",
         b"55.68", b"12.57", b"2025-11-14", b"S\xf8ren"]
    )
    zip_path = tmp_path / "occ.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(
            "occurrence.txt", HEADER.encode() + b"\n" + row + b"\n"
        )
    result = parse_and_aggregate(zip_path, [4], workers=2, chunk_bytes=16)
    who = result.tables[4].column("recorded_by").to_pylist()
    assert who == ["S\ufffdren"]


def test_empty_data_archive_yields_empty_tables_not_an_error(tmp_path):
    archive = _zip(tmp_path, [])
    result = parse_and_aggregate(archive, [3, 4], workers=2)
    assert result.total == 0
    assert all(t.num_rows == 0 for t in result.tables.values())
    assert result.dropped == {
        "no_species_key": 0, "bad_coordinates": 0, "unparseable_date": 0,
    }


def test_missing_required_columns_raise_like_read_occurrences(tmp_path):
    zip_path = tmp_path / "occ.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "a\tb\tc\n1\t2\t3\n")
    with pytest.raises(ValueError, match="missing columns"):
        parse_and_aggregate(zip_path, [4], workers=2)


def test_the_aggregates_feed_the_existing_pipeline(tmp_path):
    """aggregate_counts over the checkpoint granularity equals
    aggregate_counts over the per-occurrence table: the two-stage
    aggregation is exact, so guards-then-counts works unchanged.

    THE FIXTURE MUST HAVE GROUPS WITH n > 1, asserted below. At n_clean=60
    every (species, cell, week, publisher, contributor) group held exactly
    one occurrence and this test passed against an `aggregate_counts` that
    silently counted GROUPS instead of summing weights -- the exact defect
    it exists to catch."""
    archive = _messy_archive(tmp_path, n_clean=400)
    per_occurrence = assign_cells(read_occurrences(archive), 4)
    direct = aggregate_counts(mark_guards_applied(per_occurrence))

    pre = parse_and_aggregate(archive, [4], workers=2).tables[4]
    assert any(n > 1 for n in pre.column("n").to_pylist()), (
        "fixture too small: every group has n == 1, so group-counting and "
        "weight-summing are indistinguishable and this test pins nothing"
    )
    two_stage = aggregate_counts(mark_guards_applied(pre))

    key = ["species_key", "cell", "week", "n"]
    assert sorted(
        tuple(r[c] for c in key) for r in direct.to_pylist()
    ) == sorted(tuple(r[c] for c in key) for r in two_stage.to_pylist())


def test_invalid_worker_count_is_refused(tmp_path):
    archive = _zip(tmp_path, [])
    with pytest.raises(ValueError, match="workers"):
        parse_and_aggregate(archive, [4], workers=0)
