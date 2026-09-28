"""Occurrence extraction from a GBIF SIMPLE_CSV archive."""
import io
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.occurrences import read_occurrences

HEADER = "\t".join(
    [
        "gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
        "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy",
    ]
)
GOOD = "\t".join(
    [
        "1", "ds", "pub-a", "2492576", "Turdus merula",
        "55.6867", "12.5700", "2025-11-14", "Ole Ratzer",
    ]
)
NO_COORD = "\t".join(
    ["2", "ds", "pub-a", "2492576", "Turdus merula", "", "", "2025-11-14", "X"]
)
NO_SPECIES = "\t".join(
    ["3", "ds", "pub-a", "", "", "55.1", "12.1", "2025-11-14", "X"]
)
NO_DATE = "\t".join(
    ["4", "ds", "pub-a", "2492576", "Turdus merula", "55.1", "12.1", "", "X"]
)


def _archive(tmp_path: Path, rows: list[str]) -> Path:
    zip_path = tmp_path / "occ.zip"
    payload = "\n".join([HEADER, *rows]) + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", payload)
    return zip_path


def test_reads_good_row(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD]))
    assert table.num_rows == 1
    assert table.column("species_key")[0].as_py() == 2492576
    assert table.column("publisher_key")[0].as_py() == "pub-a"
    assert table.column("lat")[0].as_py() == pytest.approx(55.6867)


def test_drops_rows_without_coordinates(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD, NO_COORD]))
    assert table.num_rows == 1


def test_drops_rows_without_species_key(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD, NO_SPECIES]))
    assert table.num_rows == 1


def test_drops_rows_without_event_date(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD, NO_DATE]))
    assert table.num_rows == 1


def test_event_date_parses_to_date(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD]))
    assert table.column("event_date")[0].as_py().isoformat() == "2025-11-14"


def test_event_date_accepts_timestamp_form(tmp_path):
    row = GOOD.replace("2025-11-14", "2025-11-14T07:30:00")
    table = read_occurrences(_archive(tmp_path, [row]))
    assert table.column("event_date")[0].as_py().isoformat() == "2025-11-14"
