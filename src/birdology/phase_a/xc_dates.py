"""Date focal recordings from the Xeno-Canto v3 API.

BirdSet's focal metadata has `local_time` (time of day) but no date. The XC v3
record carries `date`, and BirdSet's `filepath`/`id` give the XC recording
number, so the join is by id.

Bulk-by-species, not per-id: 10 sequential per-id lookups take ~1 s, so dating
XCL's 528k recordings one at a time is ~14 h of serial calls. `per_page=500` is
honoured, so a species costs ceil(n/500) calls.

https://xeno-canto.org/explore/api  (v2 retired 2025-10-10; v3 needs a key)
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import requests

XC_API = "https://xeno-canto.org/api/3/recordings"
_TIMEOUT = 60
_PER_PAGE = 500

log = logging.getLogger(__name__)


class DateFetchFailed(RuntimeError):
    """The XC API returned an error payload or an unusable response."""


def read_key(path: Path) -> str:
    """Read the API key from a file. Never accept it as an argv or env value."""
    key = Path(path).read_text(encoding="utf-8").strip()
    if not key:
        raise ValueError(f"XC key file {path} is empty")
    return key


def parse_xc_date(raw: str | None) -> date | None:
    """A full YYYY-MM-DD, or None.

    XC holds recordings back to the 1950s with partial dates: '2021',
    '2021-00-00' and '0000-00-00' all occur. None means "cannot place this
    recording in a week" -- the caller must DROP it, never default it, because
    a defaulted week is indistinguishable from a real one downstream.
    """
    if not raw:
        return None
    try:
        return date.fromisoformat(raw.strip())
    except ValueError:
        return None


def fetch_species_dates(
    genus: str,
    species: str,
    *,
    key: str,
    per_page: int = _PER_PAGE,
    session: requests.Session | None = None,
) -> dict[int, date]:
    """`{xc_id: date}` for one species, over every page.

    Recordings without a usable date are omitted rather than defaulted; the
    caller compares against its own id set to count what it lost.
    """
    http = session or requests.Session()
    query = f"gen:{genus} sp:{species}"
    out: dict[int, date] = {}
    page = 1
    pages = 1

    while page <= pages:
        # `key` goes in params, never in the URL string or a log line.
        resp = http.get(
            XC_API,
            params={"query": query, "key": key, "per_page": per_page, "page": page},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        if "error" in payload:
            raise DateFetchFailed(
                f"XC API error for {query!r}: {payload.get('error')} "
                f"{payload.get('message', '')}"
            )
        pages = int(payload.get("numPages") or 1)
        for rec in payload.get("recordings", []):
            parsed = parse_xc_date(rec.get("date"))
            if parsed is None:
                continue
            try:
                out[int(rec["id"])] = parsed
            except (KeyError, ValueError):
                continue
        log.info("%s page %d/%d: %d dated so far", query, page, pages, len(out))
        page += 1

    return out
