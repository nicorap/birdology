"""GBIF Occurrence Download API client.

The paged /occurrence/search API cannot be used for bulk extraction: GBIF
hard-caps offset at ~10 000 and DOF alone holds 38.1 million records. The
Download API is asynchronous — request, poll, fetch — and needs a free GBIF
account.

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

log = logging.getLogger(__name__)


class DownloadFailed(RuntimeError):
    """A GBIF download was killed, cancelled, or did not finish in time."""


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
    """Ask GBIF to prepare a download; return its key."""
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
    """Block until the download succeeds; return its metadata."""
    waited = 0
    while True:
        resp = requests.get(
            f"{_BASE}/occurrence/download/{download_key}",
            auth=(user, password),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        meta = resp.json()
        status = meta.get("status")
        if status == "SUCCEEDED":
            log.info("download %s ready (doi=%s)", download_key, meta.get("doi"))
            return meta
        if status in {"KILLED", "CANCELLED", "FAILED"}:
            raise DownloadFailed(f"download {download_key} status={status}")
        if waited >= timeout_s:
            raise DownloadFailed(
                f"download {download_key} timed out after {waited}s (status={status})"
            )
        time.sleep(interval_s)
        waited += max(interval_s, 1)


def fetch_download(
    download_key: str, dest: Path, *, user: str, password: str
) -> Path:
    """Stream the prepared archive to *dest*; return the written path."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{_BASE}/occurrence/download/request/{download_key}.zip"
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(
        url, auth=(user, password), stream=True, timeout=_TIMEOUT
    ) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    tmp.rename(dest)
    return dest
