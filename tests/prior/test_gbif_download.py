"""GBIF Download API client — no live network calls."""
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.gbif_download import (
    DownloadFailed,
    fetch_download,
    poll_download,
    request_download,
)

DOF = "95db4db8-f762-11e1-a439-00145eb45e9a"


def _json_resp(payload):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


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
    caplog.set_level(logging.DEBUG, logger="birdology.prior.gbif_download")
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = "key"
    with patch(
        "birdology.prior.gbif_download.requests.post", return_value=resp
    ) as post:
        request_download(DOF, user="u", password="sekrit", email="e@example.org")
    assert "sekrit" not in caplog.text
    assert "sekrit" not in str(post.call_args[0])
    assert "sekrit" not in str(post.call_args[1]["json"])


def test_poll_download_returns_metadata_on_success():
    resp = _json_resp(
        {
            "status": "SUCCEEDED",
            "doi": "10.15468/dl.abc123",
            "downloadLink": "https://api.gbif.org/v1/occurrence/download/request/k.zip",
        }
    )
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        meta = poll_download("k", user="u", password="p", interval_s=0)
    assert meta["doi"] == "10.15468/dl.abc123"


def test_poll_download_raises_on_killed():
    resp = _json_resp({"status": "KILLED"})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(DownloadFailed):
            poll_download("k", user="u", password="p", interval_s=0)


def test_poll_download_times_out():
    resp = _json_resp({"status": "RUNNING"})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(DownloadFailed, match="timed out"):
            poll_download("k", user="u", password="p", interval_s=0, timeout_s=0)


def test_poll_download_survives_transient_error(caplog):
    """F2: a 5xx/connection blip must not abort a queued multi-hour download."""
    caplog.set_level(logging.DEBUG, logger="birdology.prior.gbif_download")
    running = _json_resp({"status": "RUNNING"})
    succeeded = _json_resp({"status": "SUCCEEDED", "doi": "10.15468/dl.xyz789"})
    get = MagicMock(
        side_effect=[requests.ConnectionError("reset by peer"), running, succeeded]
    )
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ) as sleep:
        meta = poll_download("k", user="u", password="sekrit", interval_s=0)
    assert meta["doi"] == "10.15468/dl.xyz789"
    assert get.call_count == 3
    assert sleep.call_count == 2
    assert "sekrit" not in caplog.text


def test_poll_download_propagates_client_error():
    """F2: 4xx is not retryable — let it out rather than looping for 24 h."""
    resp = MagicMock()
    resp.status_code = 404
    err = requests.HTTPError("404 Not Found", response=resp)
    resp.raise_for_status.side_effect = err
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp), patch(
        "birdology.prior.gbif_download.time.sleep"
    ):
        with pytest.raises(requests.HTTPError):
            poll_download("k", user="u", password="p", interval_s=0)


@pytest.mark.parametrize("status", ["FILE_ERASED", "WEIRD_NEW_STATE", None])
def test_poll_download_raises_on_unknown_status(status):
    """F4: unrecognised/terminal-erased statuses fail fast, not after 24 h."""
    resp = _json_resp({"status": status})
    get = MagicMock(return_value=resp)
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ) as sleep:
        with pytest.raises(DownloadFailed) as excinfo:
            poll_download("k", user="u", password="p", interval_s=0)
    assert str(status) in str(excinfo.value)
    assert "timed out" not in str(excinfo.value)
    assert get.call_count == 1
    assert sleep.call_count == 0


def _stream_resp(chunks, headers):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.headers = headers
    resp.iter_content.return_value = iter(chunks or [])
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    return resp


def test_fetch_download_rejects_truncated_body(tmp_path):
    """F3: a short body must fail loudly and leave no .part behind."""
    dest = tmp_path / "out" / "dof.zip"
    resp = _stream_resp([b"abc"], {"Content-Length": "10"})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(DownloadFailed, match="truncated download: 3 of 10 bytes"):
            fetch_download("k", dest, user="u", password="p")
    assert not dest.exists()
    assert not (dest.parent / "dof.zip.part").exists()


def test_fetch_download_cleans_up_on_stream_error(tmp_path):
    """F3: an exception mid-stream must not leave a partial .part file."""
    dest = tmp_path / "dof.zip"

    def _boom():
        yield b"abc"
        raise requests.ConnectionError("dropped mid-stream")

    resp = _stream_resp(None, {"Content-Length": "10"})
    resp.iter_content.return_value = _boom()
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(requests.ConnectionError):
            fetch_download("k", dest, user="u", password="p")
    assert not dest.exists()
    assert not (dest.parent / "dof.zip.part").exists()


def test_fetch_download_writes_complete_body(tmp_path):
    dest = tmp_path / "dof.zip"
    resp = _stream_resp([b"abcde", b"fghij"], {"Content-Length": "10"})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        out = fetch_download("k", dest, user="u", password="p")
    assert out == dest
    assert dest.read_bytes() == b"abcdefghij"
    assert not (dest.parent / "dof.zip.part").exists()
