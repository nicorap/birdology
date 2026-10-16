"""One-pass GBIF archive -> Parquet conversion (raw text, nothing dropped)."""
import json
import sys
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior import occurrences as occ
from birdology.prior.archive_parquet import MANIFEST, REJECTS, convert_archive

COLS = ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species", "locality",
        "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy", "issue"]
KEPT = [c for c in COLS if c != "locality"]


def _line(i, who=b"Ole Ratzer", lat=b"55.6867", date=b"2025-11-14", species_key=b"2492576"):
    return b"\t".join([str(i).encode(), b"ds", b"pub-a", species_key, b"Turdus merula", b"Park",
                       lat, b"12.57", date, who, b""])


def _archive(tmp_path: Path, lines: list[bytes]) -> Path:
    zip_path = tmp_path / "occ.zip"
    payload = b"\n".join(["\t".join(COLS).encode(), *lines]) + b"\n"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("0001-test.csv", payload)
        zf.writestr("citations.txt", b"cite me")
    return zip_path


def _dictreader_rows(zip_path: Path) -> list[dict]:
    with zipfile.ZipFile(zip_path) as zf, zf.open(occ._occurrence_member(zf)) as raw:
        return list(occ._reader(occ._text(raw)))


def _parquet_rows(out: Path) -> list[dict]:
    files = sorted(out.glob("part-*.parquet"))
    return [r for f in files for r in pq.read_table(f).to_pylist()]


@pytest.fixture
def messy(tmp_path):
    lines = [
        _line(1),
        _line(2, who=b"J\xf8rgen \xe9"),               # Latin-1 bytes: invalid UTF-8
        _line(3, who=b"", lat=b""),                      # empty fields stay ""
        b"4\tds\tpub-a\t2492576",                        # short line
        _line(5) + b"\textra\tfields",                   # long line
        b"",                                             # blank line: skipped by both readers
        _line(6, who="Søren Ærø".encode()),
        _line(7, date=b"2025-11", species_key=b""),      # kept here; the prior drops it later
    ]
    return _archive(tmp_path, lines)


def test_rows_plus_rejects_equal_what_dictreader_iterates(messy, tmp_path):
    m = convert_archive(messy, tmp_path / "pq")
    assert m["source_rows"] == len(_dictreader_rows(messy)) == 7
    assert (m["rows"], m["rejected_lines"]) == (5, 2)
    assert json.loads((tmp_path / "pq" / MANIFEST).read_text()) == m


def test_values_match_dictreader_field_for_field(messy, tmp_path):
    convert_archive(messy, tmp_path / "pq")
    want = [{c: r[c] for c in KEPT} for r in _dictreader_rows(messy)
            if None not in r and None not in r.values()]   # the lines Arrow can hold
    assert _parquet_rows(tmp_path / "pq") == want


def test_invalid_utf8_is_replaced_as_read_occurrences_does(messy, tmp_path):
    convert_archive(messy, tmp_path / "pq")
    who = [r["recordedBy"] for r in _parquet_rows(tmp_path / "pq")]
    assert who[1] == "J�rgen �"
    assert who[3] == "Søren Ærø"
    assert who[2] == ""


def test_rejected_lines_are_kept_verbatim(messy, tmp_path):
    convert_archive(messy, tmp_path / "pq")
    texts = pq.read_table(tmp_path / "pq" / REJECTS).column("text").to_pylist()
    assert texts[0] == "4\tds\tpub-a\t2492576"
    assert texts[1].startswith("5\t") and texts[1].endswith("\textra\tfields")


def test_unkept_and_absent_columns(messy, tmp_path):
    m = convert_archive(messy, tmp_path / "pq")
    assert m["columns"] == KEPT
    assert "locality" not in m["columns"]
    assert "countryCode" in m["absent_columns"]


def test_splits_into_parts_and_keeps_order(tmp_path):
    zip_path = _archive(tmp_path, [_line(i) for i in range(5000)])
    m = convert_archive(zip_path, tmp_path / "pq", rows_per_file=1200, row_group_rows=500, block_size=16 << 10)
    assert len(m["parts"]) > 2
    assert [int(r["gbifID"]) for r in _parquet_rows(tmp_path / "pq")] == list(range(5000))


def test_refuses_to_overwrite(messy, tmp_path):
    convert_archive(messy, tmp_path / "pq")
    with pytest.raises(FileExistsError):
        convert_archive(messy, tmp_path / "pq")


def test_missing_required_column_raises(tmp_path):
    zip_path = tmp_path / "bad.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("0001.csv", "gbifID\tspecies\n1\tTurdus merula\n")
    with pytest.raises(ValueError, match="missing columns"):
        convert_archive(zip_path, tmp_path / "pq")
