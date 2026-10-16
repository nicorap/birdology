"""Typed occurrence table == read_occurrences + assign_cells, row for row."""
import sys
import zipfile
from pathlib import Path

import h3
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from test_parallel_parse import HEADER, _messy_archive  # noqa: E402

from birdology.prior.archive_parquet import convert_archive  # noqa: E402
from birdology.prior.indexing import assign_cells  # noqa: E402
from birdology.prior.occurrence_table import MANIFEST, build_occurrence_table  # noqa: E402
from birdology.prior.occurrences import read_occurrences  # noqa: E402
from birdology.prior.parallel_parse import parse_and_aggregate  # noqa: E402

RES = [3, 5]


def _edge_archive(tmp_path):
    """Values whose parse is NOT a plain numeral: they take the Python fallback."""
    def row(i, sk="1001", lat="55.6867", lon="12.57", date="2025-11-14", who="x"):
        return "\t".join([str(i), "ds", "p", sk, "sp", lat, lon, date, who])
    rows = [
        row(1, lat=" 55.6867 "),                  # padded coordinate
        row(2, lat="5.56867e1"),                  # exponent
        row(3, lat="nan"),                        # parses, unusable -> bad_coordinates
        row(4, lat="+55.5"),                      # explicit sign
        row(5, sk=" 1001 "),                      # padded species key
        row(6, sk="1_001"),                       # Python int() accepts underscores
        row(7, date=" 2025-11-14T07:30 "),        # padded timestamp
        row(8, date="2025-11-14/2025-11-20"),     # range -> start date
        row(9, date="2025-W46-5"),                # ISO week date, Python accepts it
        row(10, lon="-180.0"),
        row(11, lon="180.0001"),                  # out of range
        row(12, who=" Ole "),
        "13\tds\tp\t1001",                        # short line -> rejected, then dropped
        row(14) + "\textra",                      # long line -> rejected, then kept
        row(15, sk=""),
    ]
    zip_path = tmp_path / "edge.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return zip_path


def _build(tmp_path, archive, **kw):
    convert_archive(archive, tmp_path / "raw")
    m = build_occurrence_table(tmp_path / "raw", tmp_path / "occ", RES, **kw)
    rows = [r for p in m["parts"] for r in pq.read_table(tmp_path / "occ" / p).to_pylist()]
    return m, rows


def _reference_rows(archive):
    ref = read_occurrences(archive)
    cells = {r: assign_cells(ref, r).column("cell").to_pylist() for r in RES}
    weeks = assign_cells(ref, RES[0]).column("week").to_pylist()
    out = []
    for i, r in enumerate(ref.to_pylist()):
        out.append((r["species_key"], r["lat"], r["lon"], r["event_date"], weeks[i],
                    *[cells[res][i] for res in RES], r["publisher_key"], r["recorded_by"]))
    return out


def _rows(rows):
    return [(r["species_key"], r["lat"], r["lon"], r["event_date"], r["week"],
             *[h3.int_to_str(r[f"cell_r{res}"]) for res in RES], r["publisher_key"], r["recorded_by"])
            for r in rows]


@pytest.mark.parametrize("make", [_messy_archive, _edge_archive])
def test_rows_match_read_occurrences_and_assign_cells(tmp_path, make):
    archive = make(tmp_path)
    _, rows = _build(tmp_path, archive)
    assert sorted(_rows(rows), key=repr) == sorted(_reference_rows(archive), key=repr)


@pytest.mark.parametrize("make", [_messy_archive, _edge_archive])
def test_drop_counters_match(tmp_path, make):
    archive = make(tmp_path)
    m, rows = _build(tmp_path, archive)
    ref = parse_and_aggregate(archive, RES, workers=1)   # pinned == read_occurrences' counters
    assert m["dropped"] == ref.dropped
    assert m["total"] == ref.total == m["source_rows"]
    assert m["kept"] == len(rows)


def test_order_is_kept_without_rejects(tmp_path):
    archive = _messy_archive(tmp_path)
    _, rows = _build(tmp_path, archive)
    assert _rows(rows) == _reference_rows(archive)


def test_worker_count_does_not_change_the_rows(tmp_path):
    archive = _messy_archive(tmp_path, n_clean=3000)
    convert_archive(archive, tmp_path / "raw", row_group_rows=200, block_size=8 << 10)
    a = build_occurrence_table(tmp_path / "raw", tmp_path / "a", RES, workers=1, row_groups_per_file=2)
    b = build_occurrence_table(tmp_path / "raw", tmp_path / "b", RES, workers=3, row_groups_per_file=2)
    assert len(a["parts"]) > 2
    read = lambda d, m: [r for p in m["parts"] for r in pq.read_table(d / p).to_pylist()]  # noqa: E731
    assert read(tmp_path / "a", a) == read(tmp_path / "b", b)
    assert {k: a[k] for k in ("total", "kept", "dropped")} == {k: b[k] for k in ("total", "kept", "dropped")}


def test_rarity_columns_are_typed(tmp_path):
    cols = ["gbifID", "speciesKey", "decimalLatitude", "decimalLongitude", "eventDate",
            "individualCount", "coordinateUncertaintyInMeters", "countryCode", "issue"]
    rows = ["1\t1001\t55.1\t12.1\t2025-11-14\t3\t25.5\tDK\tA;B",
            "2\t1001\t55.1\t12.1\t2025-11-14\tX\t\t\t"]
    zip_path = tmp_path / "r.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join(["\t".join(cols), *rows]) + "\n")
    _, out = _build(tmp_path, zip_path)
    assert [(r["gbif_id"], r["individual_count"], r["coordinate_uncertainty_m"], r["country_code"], r["issue"])
            for r in out] == [(1, 3, 25.5, "DK", "A;B"), (2, None, None, None, None)]
    assert out[0]["recorded_by"] is None          # absent column -> null, as _optional(None)


def test_refuses_to_overwrite(tmp_path):
    archive = _messy_archive(tmp_path, n_clean=50)
    _build(tmp_path, archive)
    with pytest.raises(FileExistsError):
        build_occurrence_table(tmp_path / "raw", tmp_path / "occ", RES)
    assert (tmp_path / "occ" / MANIFEST).exists()
