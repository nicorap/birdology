"""GBIF Download API client — no live network calls."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.gbif_download import (
    DownloadFailed,
    poll_download,
    request_download,
)

DOF = "95db4db8-f762-11e1-a439-00145eb45e9a"


def test_request_download_returns_key():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = "0012345-260928120000000"
    with patch("birdology.prior.gbif_download.requests.post", return_value=resp) as post:
        key = request_download(
            DOF, user="u", password="p", email="e@example.org"
        )
    assert key == "0012345-260928120000000"
    body = post.call_args[1]["json"]
    assert body["predicate"]["type"] == "and"
    dataset_pred = [
        p for p in body["predicate"]["predicates"] if p["key"] == "DATASET_KEY"
    ]
    assert dataset_pred[0]["value"] == DOF


def test_request_download_never_logs_password(caplog):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = "key"
    with patch("birdology.prior.gbif_download.requests.post", return_value=resp):
        request_download(DOF, user="u", password="sekrit", email="e@example.org")
    assert "sekrit" not in caplog.text


def test_poll_download_returns_metadata_on_success():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "status": "SUCCEEDED",
        "doi": "10.15468/dl.abc123",
        "downloadLink": "https://api.gbif.org/v1/occurrence/download/request/k.zip",
    }
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        meta = poll_download("k", user="u", password="p", interval_s=0)
    assert meta["doi"] == "10.15468/dl.abc123"


def test_poll_download_raises_on_killed():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"status": "KILLED"}
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(DownloadFailed):
            poll_download("k", user="u", password="p", interval_s=0)


def test_poll_download_times_out():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"status": "RUNNING"}
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(DownloadFailed, match="timed out"):
            poll_download("k", user="u", password="p", interval_s=0, timeout_s=0)
