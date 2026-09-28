"""Occurrence extraction from a GBIF SIMPLE_CSV archive."""
import logging
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


# --- Round-1 review findings ----------------------------------------------


def _field(row: str, name: str, value: str) -> str:
    parts = row.split("\t")
    parts[HEADER.split("\t").index(name)] = value
    return "\t".join(parts)


# F1 — absent free-text fields are NULL, not "".


@pytest.mark.parametrize(
    "source_field,column",
    [
        ("recordedBy", "recorded_by"),
        ("publishingOrgKey", "publisher_key"),
        ("species", "scientific_name"),
    ],
)
def test_absent_text_field_is_null_not_empty_string(tmp_path, source_field, column):
    """Task 4's leakage guard keys on recorded_by: null never over-matches."""
    table = read_occurrences(_archive(tmp_path, [_field(GOOD, source_field, "")]))
    assert table.num_rows == 1
    assert table.column(column)[0].as_py() is None
    assert table.column(column)[0].as_py() != ""
    assert table.column(column).null_count == 1


@pytest.mark.parametrize(
    "source_field,column",
    [
        ("recordedBy", "recorded_by"),
        ("publishingOrgKey", "publisher_key"),
        ("species", "scientific_name"),
    ],
)
def test_whitespace_only_text_field_is_null(tmp_path, source_field, column):
    table = read_occurrences(_archive(tmp_path, [_field(GOOD, source_field, "   ")]))
    assert table.column(column)[0].as_py() is None


def test_null_recorded_by_never_equals_a_contributor(tmp_path):
    """Arrow semantics: pc.equal(null, x) is null, so the guard fails safe."""
    import pyarrow.compute as pc

    table = read_occurrences(
        _archive(tmp_path, [GOOD, _field(GOOD, "recordedBy", "")])
    )
    assert table.num_rows == 2
    for probe in ["Ole Ratzer", ""]:
        mask = pc.equal(table.column("recorded_by"), pa.scalar(probe))
        assert mask[1].as_py() is None, f"null row matched {probe!r}"
    assert pc.equal(table.column("recorded_by"), pa.scalar("Ole Ratzer"))[0].as_py()


def test_present_text_fields_are_not_nulled(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD]))
    assert table.column("recorded_by")[0].as_py() == "Ole Ratzer"
    assert table.column("publisher_key")[0].as_py() == "pub-a"
    assert table.column("scientific_name")[0].as_py() == "Turdus merula"
    for column in ("recorded_by", "publisher_key", "scientific_name"):
        assert table.column(column).null_count == 0


# F2 — member selection and header validation.


def test_selects_data_table_when_citations_csv_comes_first(tmp_path):
    """Zip member order is not guaranteed; first-.csv-wins yields 0 rows."""
    zip_path = tmp_path / "0012345-250101.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("citations.csv", "citation\nplease cite GBIF\n")
        zf.writestr("rights.csv", "rights\nCC-BY\n")
        zf.writestr("0012345-250101120000000.csv", HEADER + "\n" + GOOD + "\n")
    table = read_occurrences(zip_path)
    assert table.num_rows == 1
    assert table.column("species_key")[0].as_py() == 2492576


def test_member_with_unrelated_headers_raises_naming_missing_columns(tmp_path):
    zip_path = tmp_path / "wrong-header.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("citations.csv", "citation\tnote\nplease cite GBIF\tx\n")
    with pytest.raises(ValueError) as excinfo:
        read_occurrences(zip_path)
    message = str(excinfo.value)
    assert "missing columns" in message
    for column in ("speciesKey", "decimalLatitude", "decimalLongitude", "eventDate"):
        assert column in message


def test_comma_delimited_member_raises_instead_of_yielding_empty(tmp_path):
    zip_path = tmp_path / "comma.zip"
    payload = HEADER.replace("\t", ",") + "\n" + GOOD.replace("\t", ",") + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("0012345-250101120000000.csv", payload)
    with pytest.raises(ValueError, match="missing columns"):
        read_occurrences(zip_path)


def test_occurrence_txt_with_wrong_headers_raises(tmp_path):
    zip_path = tmp_path / "bad-occurrence.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "gbifID\tnote\n1\tx\n")
    with pytest.raises(ValueError, match="missing columns"):
        read_occurrences(zip_path)


def test_occurrence_txt_wins_over_a_stray_csv(tmp_path):
    zip_path = tmp_path / "both.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("citations.csv", "citation\nplease cite GBIF\n")
        zf.writestr("occurrence.txt", HEADER + "\n" + GOOD + "\n")
    assert read_occurrences(zip_path).num_rows == 1


# F3 — coordinate bounds and finiteness.


@pytest.mark.parametrize(
    "lat,lon",
    [
        ("955.1", "1200.1"),
        ("90.1", "12.57"),
        ("-90.1", "12.57"),
        ("55.68", "180.1"),
        ("55.68", "-180.1"),
        ("nan", "12.57"),
        ("55.68", "inf"),
        ("inf", "-inf"),
        ("55.68", "NaN"),
    ],
)
def test_drops_out_of_range_or_non_finite_coordinates(tmp_path, lat, lon):
    bad = _field(_field(GOOD, "decimalLatitude", lat), "decimalLongitude", lon)
    table = read_occurrences(_archive(tmp_path, [GOOD, bad]))
    assert table.num_rows == 1
    assert table.column("lat")[0].as_py() == pytest.approx(55.6867)


@pytest.mark.parametrize(
    "lat,lon", [("90.0", "180.0"), ("-90.0", "-180.0"), ("0.0", "0.0")]
)
def test_keeps_coordinates_on_the_boundary(tmp_path, lat, lon):
    row = _field(_field(GOOD, "decimalLatitude", lat), "decimalLongitude", lon)
    table = read_occurrences(_archive(tmp_path, [row]))
    assert table.num_rows == 1
    assert table.column("lat")[0].as_py() == pytest.approx(float(lat))
    assert table.column("lon")[0].as_py() == pytest.approx(float(lon))


# F4 — coarse dates are dropped on purpose, and losses are counted.


@pytest.mark.parametrize("coarse", ["2025-11", "2025"])
def test_coarse_precision_dates_are_dropped(tmp_path, coarse):
    """Sub-day precision is a deliberate drop, not an accident."""
    table = read_occurrences(_archive(tmp_path, [_field(GOOD, "eventDate", coarse)]))
    assert table.num_rows == 0


def test_logs_dropped_row_counts_by_reason(tmp_path, caplog):
    rows = [
        GOOD,
        NO_SPECIES,
        NO_COORD,
        _field(GOOD, "decimalLatitude", "955.1"),
        NO_DATE,
        _field(GOOD, "eventDate", "2025-11"),
    ]
    with caplog.at_level(logging.INFO, logger="birdology.prior.occurrences"):
        table = read_occurrences(_archive(tmp_path, rows))
    assert table.num_rows == 1
    message = "\n".join(r.getMessage() for r in caplog.records)
    assert "kept 1 of 6 rows" in message
    assert "dropped 5" in message
    assert "no species key: 1" in message
    assert "bad coordinates: 2" in message
    assert "unparseable date: 2" in message


# F5 — a stray non-UTF-8 byte must not abort the extract.


def test_latin1_byte_in_recorded_by_does_not_abort(tmp_path):
    zip_path = tmp_path / "latin1.zip"
    payload = (HEADER + "\n" + _field(GOOD, "recordedBy", "Ole R\u00e5tzer") + "\n")
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", payload.encode("latin-1"))
    table = read_occurrences(zip_path)
    assert table.num_rows == 1
    assert table.column("species_key")[0].as_py() == 2492576
    assert table.column("lat")[0].as_py() == pytest.approx(55.6867)
    assert table.column("event_date")[0].as_py().isoformat() == "2025-11-14"
    assert table.column("recorded_by")[0].as_py().startswith("Ole R")


def test_latin1_byte_does_not_corrupt_following_rows(tmp_path):
    zip_path = tmp_path / "latin1-multi.zip"
    payload = "\n".join(
        [HEADER, _field(GOOD, "recordedBy", "Ole R\u00e5tzer"), GOOD]
    ) + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", payload.encode("latin-1"))
    table = read_occurrences(zip_path)
    assert table.num_rows == 2
    assert table.column("recorded_by")[1].as_py() == "Ole Ratzer"
