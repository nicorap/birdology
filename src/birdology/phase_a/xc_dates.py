"""Date focal recordings from the Xeno-Canto v3 API.

BirdSet's focal metadata has `local_time` (time of day) but no date. The XC v3
record carries `date`, and BirdSet's `filepath`/`id` give the XC recording
number, so the join is by id.

Bulk-by-species, not per-id: 10 sequential per-id lookups take ~1 s, so dating
XCL's 528k recordings one at a time is ~14 h of serial calls. `per_page=500` is
honoured, so a species costs ceil(n/500) calls.

Secrecy of the key is a property of the *output*, not of where it is passed.
`requests` builds the query string out of `params`, so by the time urllib3 logs
the request line or `requests` formats an HTTPError, "the key is in params" and
"the key is in the URL" are the same sentence. Both channels are scrubbed here
(`_KeyRedactingFilter`, `_redact`) because this module runs unattended in Slurm
jobs whose stdout/.err files sit on shared storage.

https://xeno-canto.org/explore/api  (v2 retired 2025-10-10; v3 needs a key)
"""
from __future__ import annotations

import logging
import urllib.parse
from datetime import date
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

XC_API = "https://xeno-canto.org/api/3/recordings"
_TIMEOUT = 60
_PER_PAGE = 500

# A filter set on a logger runs only for records logged *through that logger* --
# Python does not apply an ancestor's filters while a record propagates -- so
# filtering "urllib3" alone would miss "urllib3.connectionpool" (the request
# line) and "urllib3.util.retry" ("Incremented Retry for (url=...)"), both of
# which print the full URL. Enumerate the namespace instead of listing names.
_URLLIB3_ROOT = "urllib3"

log = logging.getLogger(__name__)


class DateFetchFailed(RuntimeError):
    """The XC API returned an error payload or an unusable response."""


def read_key(path: Path) -> str:
    """Read the API key from a file. Never accept it as an argv or env value."""
    key = Path(path).read_text(encoding="utf-8").strip()
    if not key:
        raise ValueError(f"XC key file {path} is empty")
    return key


def _redact(text: str, key: str) -> str:
    """Replace the key -- raw and percent-encoded -- with `***`.

    A key containing `/`, `+` or `=` reaches a log line or an exception message
    only in its URL-encoded form, so replacing the raw string alone would miss
    it.
    """
    if not key:
        return text
    out = text.replace(key, "***")
    quoted = urllib.parse.quote(key, safe="")
    if quoted != key:
        out = out.replace(quoted, "***")
    # `params` encoding uses quote_plus, so a space/`+` differs from quote().
    plus = urllib.parse.quote_plus(key)
    if plus not in (key, quoted):
        out = out.replace(plus, "***")
    return out


class _KeyRedactingFilter(logging.Filter):
    """Scrub the key from urllib3's DEBUG records, leaving the record itself.

    Silencing urllib3 would be cheaper and wrong: its DEBUG lines are the only
    connection-level diagnostics a Slurm job leaves behind.
    """

    def __init__(self, key: str) -> None:
        super().__init__()
        self._key = key

    def filter(self, record: logging.LogRecord) -> bool:
        # Format now and redact the result, rather than scrubbing `msg` and
        # `args` separately: urllib3 passes the URL as a `%r` argument and also
        # logs objects (`Retry(...)`) whose repr could carry it, and only the
        # formatted string is guaranteed to hold everything a handler emits.
        try:
            formatted = record.getMessage()
        except Exception:  # a broken format string is not ours to raise
            return True
        record.msg = _redact(formatted, self._key)
        record.args = ()
        return True


def _urllib3_loggers() -> list[logging.Logger]:
    """Every live logger in the `urllib3` namespace, resolved at call time."""
    names = [
        name
        for name in list(logging.Logger.manager.loggerDict)
        if name == _URLLIB3_ROOT or name.startswith(_URLLIB3_ROOT + ".")
    ]
    if _URLLIB3_ROOT not in names:
        names.append(_URLLIB3_ROOT)
    return [logging.getLogger(name) for name in names]


def _make_session() -> requests.Session:
    """A session that backs off instead of dropping a species on one 429.

    Same Retry policy as `_make_session()` in `birdology/migration.py` -- kept
    mirrored rather than imported so neither module reaches into the other's
    privates; change both together.
    """
    retry = Retry(
        total=5,
        backoff_factor=1.0,                      # 1s, 2s, 4s, 8s, 16s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


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
    http = session or _make_session()
    query = f"gen:{genus} sp:{species}"
    out: dict[int, date] = {}
    malformed = 0
    page = 1
    pages = 1

    redactor = _KeyRedactingFilter(key)
    filtered = _urllib3_loggers()
    for logger in filtered:
        logger.addFilter(redactor)
    try:
        while page <= pages:
            try:
                resp = http.get(
                    XC_API,
                    params={
                        "query": query,
                        "key": key,
                        "per_page": per_page,
                        "page": page,
                    },
                    timeout=_TIMEOUT,
                )
                resp.raise_for_status()
            except requests.RequestException as exc:
                # HTTPError/RetryError/ConnectionError all carry the full URL --
                # key included -- in their message, and an unhandled traceback
                # lands verbatim in a Slurm .err file. `from None` drops the
                # original exception so the chained traceback cannot reprint it.
                raise DateFetchFailed(
                    _redact(
                        f"XC request failed for {query!r} page {page}: "
                        f"{type(exc).__name__}: {exc}",
                        key,
                    )
                ) from None
            payload = resp.json()
            if "error" in payload:
                raise DateFetchFailed(
                    _redact(
                        f"XC API error for {query!r}: {payload.get('error')} "
                        f"{payload.get('message', '')}",
                        key,
                    )
                )
            pages = int(payload.get("numPages") or 1)
            for rec in payload.get("recordings", []):
                parsed = parse_xc_date(rec.get("date"))
                if parsed is None:
                    continue
                try:
                    out[int(rec["id"])] = parsed
                except (KeyError, TypeError, ValueError):
                    # XC payloads do contain `"id": null`; int(None) raises
                    # TypeError, which used to abort the species and discard
                    # every date already collected for it.
                    malformed += 1
                    continue
            log.info(
                "%s page %d/%d: %d dated so far, %d malformed ids skipped",
                query,
                page,
                pages,
                len(out),
                malformed,
            )
            page += 1
    finally:
        for logger in filtered:
            logger.removeFilter(redactor)

    return out
