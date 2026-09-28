"""Occurrence extraction from a GBIF SIMPLE_CSV archive."""
import io
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.occurrences import OCCURRENCE_COLUMNS, read_occurrences

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


# --- Hardening beyond the brief -------------------------------------------
# The brief's drop tests only count rows, so they would also pass if the
# filter were inverted and the *unusable* row survived. These pin the
# surviving row's identity, the exact schema Task 3 consumes, and the
# failure modes for malformed rows and unexpected archive contents.


@pytest.mark.parametrize("bad", [NO_COORD, NO_SPECIES, NO_DATE], ids=["coord", "species", "date"])
def test_dropping_keeps_the_good_row_not_the_bad_one(tmp_path, bad):
    table = read_occurrences(_archive(tmp_path, [GOOD, bad]))
    assert table.num_rows == 1
    assert table.column("recorded_by")[0].as_py() == "Ole Ratzer"
    assert table.column("scientific_name")[0].as_py() == "Turdus merula"


def test_schema_is_the_contract_task_3_consumes():
    assert OCCURRENCE_COLUMNS == (
        "species_key",
        "scientific_name",
        "lat",
        "lon",
        "event_date",
        "publisher_key",
        "recorded_by",
    )


def test_table_schema_matches_declared_types(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD]))
    assert tuple(table.column_names) == OCCURRENCE_COLUMNS
    assert table.schema.field("species_key").type == pa.int64()
    assert table.schema.field("scientific_name").type == pa.string()
    assert table.schema.field("lat").type == pa.float64()
    assert table.schema.field("lon").type == pa.float64()
    assert table.schema.field("event_date").type == pa.date32()
    assert table.schema.field("publisher_key").type == pa.string()
    assert table.schema.field("recorded_by").type == pa.string()


def test_all_rows_unusable_yields_empty_table_with_full_schema(tmp_path):
    table = read_occurrences(_archive(tmp_path, [NO_COORD, NO_SPECIES, NO_DATE]))
    assert table.num_rows == 0
    assert tuple(table.column_names) == OCCURRENCE_COLUMNS
    assert table.schema.field("event_date").type == pa.date32()


def test_header_only_archive_yields_empty_table(tmp_path):
    table = read_occurrences(_archive(tmp_path, []))
    assert table.num_rows == 0
    assert tuple(table.column_names) == OCCURRENCE_COLUMNS


def test_archive_without_occurrence_table_raises(tmp_path):
    zip_path = tmp_path / "wrong.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("meta.xml", "<archive/>")
        zf.writestr("citations.txt", "cite me")
    with pytest.raises(ValueError, match="no occurrence table"):
        read_occurrences(zip_path)


def test_real_download_member_name_is_accepted(tmp_path):
    """A live SIMPLE_CSV download names its member ``<downloadKey>.csv``."""
    zip_path = tmp_path / "0012345-250101.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("0012345-250101120000000.csv", HEADER + "\n" + GOOD + "\n")
    table = read_occurrences(zip_path)
    assert table.num_rows == 1
    assert table.column("species_key")[0].as_py() == 2492576


@pytest.mark.parametrize(
    "field,value",
    [
        ("speciesKey", "not-an-int"),
        ("decimalLatitude", "not-a-float"),
        ("decimalLongitude", "12,57"),
    ],
)
def test_drops_rows_with_non_numeric_values(tmp_path, field, value):
    idx = HEADER.split("\t").index(field)
    parts = GOOD.split("\t")
    parts[idx] = value
    table = read_occurrences(_archive(tmp_path, [GOOD, "\t".join(parts)]))
    assert table.num_rows == 1
    assert table.column("recorded_by")[0].as_py() == "Ole Ratzer"


def test_drops_row_with_unparseable_date(tmp_path):
    row = GOOD.replace("2025-11-14", "sometime in November")
    table = read_occurrences(_archive(tmp_path, [row]))
    assert table.num_rows == 0


def test_event_date_accepts_range_form(tmp_path):
    row = GOOD.replace("2025-11-14", "2025-11-14/2025-11-20")
    table = read_occurrences(_archive(tmp_path, [row]))
    assert table.column("event_date")[0].as_py().isoformat() == "2025-11-14"


def test_zero_coordinates_are_kept(tmp_path):
    """Null Island is a data-quality issue for GBIF to filter, not a missing value."""
    parts = GOOD.split("\t")
    parts[HEADER.split("\t").index("decimalLatitude")] = "0.0"
    parts[HEADER.split("\t").index("decimalLongitude")] = "0.0"
    table = read_occurrences(_archive(tmp_path, ["\t".join(parts)]))
    assert table.num_rows == 1
    assert table.column("lat")[0].as_py() == 0.0


def test_quoted_field_is_taken_literally(tmp_path):
    """SIMPLE_CSV is unquoted TSV; a stray quote must not swallow the row."""
    parts = GOOD.split("\t")
    parts[HEADER.split("\t").index("recordedBy")] = '"Ole Ratzer'
    table = read_occurrences(_archive(tmp_path, ["\t".join(parts)]))
    assert table.num_rows == 1
    assert table.column("recorded_by")[0].as_py() == '"Ole Ratzer'
