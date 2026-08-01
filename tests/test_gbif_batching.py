"""Tests for DOF year-based batching logic — no live network calls."""
import sys
from collections import Counter
from datetime import date
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

    def mock_fetch(year, remaining, month=None):
        call_years.append(year)
        return [{"key": year * 100 + (month or 0) * 10 + i} for i in range(2)]

    with patch("birdology.ingestion.gbif_dof._fetch_year", side_effect=mock_fetch):
        fetch_dof_occurrences(max_records=4)

    assert call_years[0] >= call_years[-1], "Should start from most recent year"


# ── month coverage (regression: graph contained January-only observations) ────

def test_fetch_year_passes_month_param():
    """When a month is given it must be forwarded to GBIF as a filter."""
    with patch("birdology.ingestion.gbif_dof.requests.get") as mock_get:
        mock_get.return_value = _make_gbif_response([], end_of_records=True)
        _fetch_year(2024, remaining=300, month=7)
    call_params = mock_get.call_args[1]["params"]
    assert call_params["month"] == 7


def test_fetch_dof_occurrences_covers_all_twelve_months():
    """Regression: year-only batching hit the GBIF offset cap inside January,
    so the graph never contained a record from Feb-Dec. Every month of the
    newest year must be requested."""
    calls = []

    def mock_fetch(year, remaining, month=None):
        calls.append((year, month))
        return [{"key": f"{year}-{month}-{i}"} for i in range(2)]

    with patch("birdology.ingestion.gbif_dof._fetch_year", side_effect=mock_fetch):
        fetch_dof_occurrences(max_records=240)

    newest_year = max(y for y, _ in calls)
    months_hit = {m for y, m in calls if y == newest_year}
    assert months_hit == set(range(1, 13)), f"missing months: {set(range(1,13)) - months_hit}"


def test_fetch_dof_occurrences_spreads_budget_across_months():
    """No single month may consume the whole record budget."""
    quotas = []

    def mock_fetch(year, remaining, month=None):
        quotas.append(remaining)
        return [{"key": f"{year}-{month}-{i}"} for i in range(remaining)]

    with patch("birdology.ingestion.gbif_dof._fetch_year", side_effect=mock_fetch):
        fetch_dof_occurrences(max_records=1200)

    # 1200 records over 12 months → ~100 per month, never the full 1200
    assert max(quotas) <= 1200 // 12 + 1, f"a single month asked for {max(quotas)} records"


# ── seasonal balance (regression: a 3.4:1 Jan-Jul over-sample) ────────────────
# The month quota was derived once per year from the budget left at the start of
# that year. But the current year has no data after the current month, so those
# months' quotas were forfeited and the shortfall was refilled from earlier years
# — every month up to today got a second helping. That bias feeds straight into
# the phenology / observations_by_month output.

def test_fetch_dof_occurrences_balances_months_when_current_year_is_partial():
    """Today is mid-year, so the current year holds no Aug-Dec records.

    Those months must be filled from earlier years, not left short. The previous
    test cannot see this: its mock returns records for every (year, month),
    including months that have not happened yet.
    """
    current_year = date.today().year
    last_month_with_data = 7  # pretend it is July: Aug-Dec of this year do not exist

    def mock_fetch(year, remaining, month=None):
        if year == current_year and month > last_month_with_data:
            return []
        return [{"key": f"{year}-{month:02d}-{i}", "month": month} for i in range(remaining)]

    with patch("birdology.ingestion.gbif_dof._fetch_year", side_effect=mock_fetch):
        results = fetch_dof_occurrences(max_records=12000)

    per_month = Counter(r["month"] for r in results)
    assert set(per_month) == set(range(1, 13)), f"missing months: {per_month}"

    fewest, most = min(per_month.values()), max(per_month.values())
    assert most <= fewest * 1.1, (
        f"seasonal over-sample of {most / fewest:.1f}:1 — records per month: "
        f"{dict(sorted(per_month.items()))}"
    )
    assert len(results) >= 12000 * 0.99, f"budget under-spent: only {len(results)} records"
