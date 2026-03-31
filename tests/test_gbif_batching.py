"""Tests for DOF year-based batching logic — no live network calls."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.ingestion.gbif_dof import _fetch_year, fetch_dof_occurrences


def _make_gbif_response(records: list[dict], end_of_records: bool = True) -> MagicMock:
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.json.return_value = {"results": records, "endOfRecords": end_of_records}
    return mock


# ── _fetch_year ───────────────────────────────────────────────────────────────

def test_fetch_year_returns_records():
    records = [{"key": i, "scientificName": "Erithacus rubecula"} for i in range(5)]
    with patch("birdology.ingestion.gbif_dof.requests.get") as mock_get:
        mock_get.return_value = _make_gbif_response(records, end_of_records=True)
        result = _fetch_year(2024, remaining=10)
    assert len(result) == 5


def test_fetch_year_passes_year_param():
    with patch("birdology.ingestion.gbif_dof.requests.get") as mock_get:
        mock_get.return_value = _make_gbif_response([], end_of_records=True)
        _fetch_year(2022, remaining=300)
    call_params = mock_get.call_args[1]["params"]
    assert call_params["year"] == 2022


def test_fetch_year_respects_remaining_cap():
    """If remaining=5, we should never request more than 5 records."""
    records = [{"key": i} for i in range(5)]
    with patch("birdology.ingestion.gbif_dof.requests.get") as mock_get:
        mock_get.return_value = _make_gbif_response(records, end_of_records=True)
        result = _fetch_year(2024, remaining=5)
    call_params = mock_get.call_args[1]["params"]
    assert call_params["limit"] <= 5


def test_fetch_year_stops_at_offset_cap():
    """After _GBIF_OFFSET_CAP records, must stop even if endOfRecords=False."""
    from birdology.ingestion.gbif_dof import _GBIF_OFFSET_CAP, _PAGE_SIZE
    page = [{"key": i} for i in range(_PAGE_SIZE)]

    with patch("birdology.ingestion.gbif_dof.requests.get") as mock_get:
        mock_get.return_value = _make_gbif_response(page, end_of_records=False)
        result = _fetch_year(2024, remaining=_GBIF_OFFSET_CAP + _PAGE_SIZE * 5)

    assert len(result) <= _GBIF_OFFSET_CAP + _PAGE_SIZE


# ── fetch_dof_occurrences (year batching) ─────────────────────────────────────

def test_fetch_dof_occurrences_deduplicates():
    """The same key appearing in two years must not produce duplicate records."""
    rec = {"key": 42, "scientificName": "Erithacus rubecula"}

    with patch("birdology.ingestion.gbif_dof._fetch_year") as mock_year:
        mock_year.return_value = [rec]
        results = fetch_dof_occurrences(max_records=10)

    # rec is returned for every year; dedup should keep only one
    assert results.count(rec) == 1


def test_fetch_dof_occurrences_respects_max_records():
    records_per_year = [{"key": i + year * 1000} for i in range(100) for year in [0]]

    with patch("birdology.ingestion.gbif_dof._fetch_year") as mock_year:
        mock_year.return_value = [{"key": i} for i in range(50)]
        results = fetch_dof_occurrences(max_records=30)

    assert len(results) <= 30


def test_fetch_dof_occurrences_iterates_newest_year_first():
    """Records fetched for the most recent year should appear first."""
    call_years = []

    def mock_fetch(year, remaining):
        call_years.append(year)
        return [{"key": year * 100 + i} for i in range(2)]

    with patch("birdology.ingestion.gbif_dof._fetch_year", side_effect=mock_fetch):
        fetch_dof_occurrences(max_records=4)

    assert call_years[0] > call_years[1], "Should start from most recent year"
