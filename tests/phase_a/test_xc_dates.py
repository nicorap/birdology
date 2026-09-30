"""XC API v3 dating — no live network calls."""
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.xc_dates import (
    DateFetchFailed,
    fetch_species_dates,
    parse_xc_date,
    read_key,
)


def _resp(payload):
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = payload
    return r


def test_read_key_strips_whitespace(tmp_path):
    p = tmp_path / "k"
    p.write_text("  abc123\n")
    assert read_key(p) == "abc123"


def test_read_key_rejects_empty(tmp_path):
    p = tmp_path / "k"
    p.write_text("   \n")
    with pytest.raises(ValueError, match="empty"):
        read_key(p)


@pytest.mark.parametrize(
    "raw",
    [None, "", "0000-00-00", "2021", "2021-00-00", "not-a-date", "2021-13-01"],
)
def test_parse_xc_date_rejects_unusable(raw):
    """Review Focus 2: a partial date must be None, never a defaulted week."""
    assert parse_xc_date(raw) is None


def test_parse_xc_date_accepts_full_date():
    assert parse_xc_date("2021-10-15") == date(2021, 10, 15)


def test_fetch_species_dates_single_page():
    session = MagicMock()
    session.get.return_value = _resp(
        {
            "numRecordings": "2",
            "numPages": 1,
            "recordings": [
                {"id": "686892", "date": "2021-10-15"},
                {"id": "686179", "date": "2020-01-02"},
            ],
        }
    )
    out = fetch_species_dates("tinamus", "major", key="k", session=session)
    assert out == {686892: date(2021, 10, 15), 686179: date(2020, 1, 2)}


def test_fetch_species_dates_follows_pages():
    session = MagicMock()
    session.get.side_effect = [
        _resp({"numPages": 2, "recordings": [{"id": "1", "date": "2021-01-01"}]}),
        _resp({"numPages": 2, "recordings": [{"id": "2", "date": "2021-01-02"}]}),
    ]
    out = fetch_species_dates("parus", "major", key="k", session=session)
    assert set(out) == {1, 2}
    assert session.get.call_count == 2


def test_fetch_species_dates_omits_undatable_records():
    session = MagicMock()
    session.get.return_value = _resp(
        {
            "numPages": 1,
            "recordings": [
                {"id": "1", "date": "2021-01-01"},
                {"id": "2", "date": "0000-00-00"},
                {"id": "3"},
            ],
        }
    )
    out = fetch_species_dates("parus", "major", key="k", session=session)
    assert out == {1: date(2021, 1, 1)}


def test_fetch_species_dates_never_logs_the_key(caplog):
    import logging

    caplog.set_level(logging.DEBUG)
    session = MagicMock()
    session.get.return_value = _resp({"numPages": 1, "recordings": []})
    fetch_species_dates("parus", "major", key="sekrit", session=session)
    assert "sekrit" not in caplog.text


def test_fetch_species_dates_raises_on_api_error():
    session = MagicMock()
    session.get.return_value = _resp(
        {"error": "client_error", "message": "Missing or invalid 'key' parameter."}
    )
    with pytest.raises(DateFetchFailed, match="client_error"):
        fetch_species_dates("parus", "major", key="k", session=session)


def test_fetch_species_dates_sends_a_tag_and_the_key():
    session = MagicMock()
    session.get.return_value = _resp({"numPages": 1, "recordings": []})
    fetch_species_dates("parus", "major", key="k", session=session)
    params = session.get.call_args[1]["params"]
    assert params["query"] == "gen:parus sp:major"
    assert params["key"] == "k"
    assert params["per_page"] == 500
