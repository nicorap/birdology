"""GBIF Occurrence Download API client.

The paged /occurrence/search API cannot be used for bulk extraction: GBIF
hard-caps offset at ~10 000 and DOF alone holds 38.1 million records. The
Download API is asynchronous — request, poll, fetch — and needs a free GBIF
account.

Downloads are requested in GBIF's ``SIMPLE_CSV`` format: the fetched archive
is a zip containing a single tab-delimited ``occurrence.txt``. It is *not* a
Darwin Core Archive (DwC-A) — there is no ``meta.xml`` and no extension files,
and the column set is SIMPLE_CSV's fixed interpreted subset.

https://techdocs.gbif.org/en/data-use/api-downloads
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

_BASE = "https://api.gbif.org/v1"
_TIMEOUT = 60
_AVES_TAXON_KEY = 212

#: Terminal statuses that mean the download will never be available.
_FAILED_STATUSES = frozenset({"KILLED", "CANCELLED", "FAILED", "FILE_ERASED"})
#: The only statuses that justify polling again.
_PENDING_STATUSES = frozenset({"PREPARING", "RUNNING", "SUSPENDED"})

log = logging.getLogger(__name__)


class DownloadFailed(RuntimeError):
    """A GBIF download was killed, cancelled, truncated, or did not finish in time."""


def _predicate(
    dataset_key: str, taxon_key: int, year_from: int | None
) -> dict:
    predicates: list[dict] = [
        {"type": "equals", "key": "DATASET_KEY", "value": dataset_key},
        {"type": "equals", "key": "TAXON_KEY", "value": str(taxon_key)},
        {"type": "equals", "key": "HAS_COORDINATE", "value": "true"},
        {"type": "equals", "key": "HAS_GEOSPATIAL_ISSUE", "value": "false"},
    ]
    if year_from is not None:
        predicates.append(
            {"type": "greaterThanOrEquals", "key": "YEAR", "value": str(year_from)}
        )
    return {"type": "and", "predicates": predicates}


def request_download(
    dataset_key: str,
    *,
    taxon_key: int = _AVES_TAXON_KEY,
    year_from: int | None = None,
    user: str,
    password: str,
    email: str,
) -> str:
    """Ask GBIF to prepare a SIMPLE_CSV download; return its key."""
    body = {
        "creator": user,
        "notificationAddresses": [email],
        "sendNotification": False,
        "format": "SIMPLE_CSV",
        "predicate": _predicate(dataset_key, taxon_key, year_from),
    }
    log.info("requesting GBIF download for dataset %s", dataset_key)
    resp = requests.post(
        f"{_BASE}/occurrence/download/request",
        json=body,
        auth=(user, password),
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.text.strip()


def poll_download(
    download_key: str,
    *,
    user: str,
    password: str,
    interval_s: int = 60,
    timeout_s: int = 86_400,
) -> dict:
    """Block until the download succeeds; return its metadata.

    Transient failures (5xx, connection resets, read timeouts) are logged at
    WARNING and retried — a multi-hour download must not be abandoned because
    of one GBIF hiccup. Client errors (4xx) propagate unchanged. Every
    non-retryable outcome — a terminal status, an unrecognised status, or the
    wall-clock deadline — raises :class:`DownloadFailed`.
    """
    deadline = time.monotonic() + timeout_s
    status: str | None = None
    while True:
        meta: dict | None = None
        try:
            resp = requests.get(
                f"{_BASE}/occurrence/download/{download_key}",
                auth=(user, password),
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            meta = resp.json()
        except requests.RequestException as exc:
            code = getattr(getattr(exc, "response", None), "status_code", None)
            if code is not None and 400 <= code < 500:
                raise
            # No credentials in this message: only the key, the exception type
            # and the HTTP status.
            log.warning(
                "transient error polling download %s (%s, status=%s); retrying",
                download_key,
                type(exc).__name__,
                code,
            )

        if meta is not None:
            status = meta.get("status")
            if status == "SUCCEEDED":
                log.info("download %s ready (doi=%s)", download_key, meta.get("doi"))
                return meta
            if status in _FAILED_STATUSES:
                raise DownloadFailed(f"download {download_key} status={status}")
            if status not in _PENDING_STATUSES:
                raise DownloadFailed(
                    f"download {download_key} has unrecognised status={status!r}"
                )

        if time.monotonic() >= deadline:
            raise DownloadFailed(
                f"download {download_key} timed out after {timeout_s}s "
                f"(status={status})"
            )
        time.sleep(max(interval_s, 1))


def fetch_download(
    download_key: str, dest: Path, *, user: str, password: str
) -> Path:
    """Stream the prepared SIMPLE_CSV zip to *dest*; return the written path.

    The archive is a zip holding a tab-delimited ``occurrence.txt`` (SIMPLE_CSV),
    not a Darwin Core Archive.

    The body is written to a ``.part`` sibling and renamed only after the byte
    count matches the server's ``Content-Length`` (when advertised); a truncated
    or interrupted transfer raises :class:`DownloadFailed` and leaves no partial
    file behind.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{_BASE}/occurrence/download/request/{download_key}.zip"
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(
        url, auth=(user, password), stream=True, timeout=_TIMEOUT
    ) as resp:
        resp.raise_for_status()
        raw_length = resp.headers.get("Content-Length")
        try:
            expected = int(raw_length) if raw_length is not None else None
        except (TypeError, ValueError):
            expected = None

        written = 0
        try:
            with tmp.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    written += len(chunk)
            if expected is not None and written != expected:
                raise DownloadFailed(
                    f"truncated download: {written} of {expected} bytes"
                )
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
    tmp.rename(dest)
    return dest
