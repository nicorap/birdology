"""GBIF Download API client — no live network calls."""
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

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


@pytest.mark.parametrize("status", ["KILLED", "CANCELLED", "FAILED", "FILE_ERASED"])
def test_poll_download_raises_on_a_TERMINAL_status_and_NAMES_it(status):
    """A terminal status must be reported AS ITSELF, not as 'unrecognised'.

    Asserting only the exception TYPE is not enough: with `_FAILED_STATUSES`
    emptied, every one of these falls through to the unrecognised-status branch
    and still raises `DownloadFailed` — so a type-only test passes while the
    operator loses the one piece of information that decides what to do next
    (KILLED and FILE_ERASED mean re-request; CANCELLED means someone cancelled
    it). So the message is asserted to name the status and NOT to claim it is
    unrecognised.
    """
    resp = _json_resp({"status": status})
    get = MagicMock(return_value=resp)
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ) as sleep:
        with pytest.raises(DownloadFailed) as excinfo:
            poll_download("k", user="u", password="p", interval_s=0)
    message = str(excinfo.value)
    assert f"status={status}" in message
    assert "unrecognised" not in message
    assert "timed out" not in message
    assert get.call_count == 1
    assert sleep.call_count == 0


def test_fetch_download_STAGES_through_a_part_file_never_the_destination(tmp_path):
    """A truncated transfer must not leave bytes at the destination PATH.

    The existing truncation tests assert `not dest.exists()`, which a mutant
    writing straight to `dest` also satisfies — it unlinks `dest` in its
    cleanup, so the post-conditions are identical and the staging disappears
    unnoticed. What distinguishes the two is WHERE the bytes are DURING the
    transfer: with staging, an existing file at `dest` is untouched until the
    rename, so a failed re-download leaves the previous good archive in place.
    Without it, `open(dest, "wb")` truncates it on the first byte and the
    cleanup then deletes it — a failed retry destroys a working download.
    """
    dest = tmp_path / "dof.zip"
    dest.write_bytes(b"PREVIOUS GOOD ARCHIVE")
    resp = _stream_resp([b"abc"], {"Content-Length": "10"})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(DownloadFailed, match="truncated download"):
            fetch_download("k", dest, user="u", password="p")
    assert dest.read_bytes() == b"PREVIOUS GOOD ARCHIVE", (
        "a failed transfer must not touch the destination path"
    )
    assert not (dest.parent / "dof.zip.part").exists()


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
    """F2: 4xx is not retryable — let it out rather than looping for 24 h.

    `timeout_s=0` for the same reason as the 401 test: with `sleep` mocked and
    the default 24-hour deadline, a mutant that swallowed the 4xx would spin
    inside this test forever, hanging the whole suite and attributable to
    nothing. Past the deadline it raises `DownloadFailed` instead, which
    `pytest.raises(requests.HTTPError)` rejects.
    """
    resp = MagicMock()
    resp.status_code = 404
    err = requests.HTTPError("404 Not Found", response=resp)
    resp.raise_for_status.side_effect = err
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp), patch(
        "birdology.prior.gbif_download.time.sleep"
    ):
        with pytest.raises(requests.HTTPError):
            poll_download("k", user="u", password="p", interval_s=0, timeout_s=0)


@pytest.mark.parametrize("status", ["FILE_ERASED", "WEIRD_NEW_STATE", None])
def test_poll_download_raises_on_unknown_status(status):
    """F4: unrecognised/terminal-erased statuses fail fast, not after 24 h.

    `timeout_s=0` is load-bearing: with the default 24-hour deadline and
    `sleep` mocked, a mutant that DELETED the unrecognised-status branch would
    loop forever and could be attributed to no test at all. With the deadline
    already past, such a mutant raises the TIMEOUT message instead — which the
    `"timed out" not in ...` assertion rejects — so it dies here, attributably
    and in milliseconds. `get.call_count == 1` still pins the fail-fast.
    """
    resp = _json_resp({"status": status})
    get = MagicMock(return_value=resp)
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ) as sleep:
        with pytest.raises(DownloadFailed) as excinfo:
            poll_download("k", user="u", password="p", interval_s=0, timeout_s=0)
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


# --- I1: what a harness needs pinned in a module that cannot be run live ----
# This module needs a GBIF account and a multi-hour queue, so nothing here is
# ever exercised against the real API. Everything below is mocked HTTP. The
# gaps that mocking CANNOT close are listed in
# tests/prior/mutation/run_gbif_download_mutants.py's docstring rather than
# papered over with a test that only asserts the mock.


def test_fetch_download_url_matches_the_downloadLink_GBIF_ADVERTISES(tmp_path):
    """`fetch_download` RECONSTRUCTS the zip URL; `poll_download` is handed
    the authoritative one and discards it.

    That is a live risk — if GBIF changed the download path, `poll_download`
    would keep working (it reads `downloadLink` out of the metadata) while
    `fetch_download` silently 404ed, or worse fetched something else. Nothing
    in the module compares the two.

    So compare them here, against the exact shape GBIF's documented metadata
    carries (and which `test_poll_download_returns_metadata_on_success`'s
    fixture also carries). This is a CONTRACT test, not a mock assertion: it
    fails if the reconstruction changes. It does NOT prove the URL is right
    today — only that the module's two halves agree about it.
    """
    key = "0012345-260928120000000"
    advertised = (
        f"https://api.gbif.org/v1/occurrence/download/request/{key}.zip"
    )
    captured = {}

    def _get(url, **kwargs):
        captured["url"] = url
        raise RuntimeError("stop before any write")

    with patch("birdology.prior.gbif_download.requests.get", _get):
        with pytest.raises(RuntimeError):
            fetch_download(key, tmp_path / "dof.zip", user="u", password="p")
    assert captured["url"] == advertised


def test_fetch_download_sends_the_credentials_as_BASIC_AUTH_not_in_the_url(tmp_path):
    """A password interpolated into a URL lands in every proxy and access log.

    Pinned because the URL here is built by f-string and the auth is passed
    separately; a "convenience" edit that moved the credentials into the URL
    would keep every existing test green.
    """
    resp = _stream_resp([b"ab"], {"Content-Length": "2"})
    with patch(
        "birdology.prior.gbif_download.requests.get", return_value=resp
    ) as get:
        fetch_download("k", tmp_path / "dof.zip", user="u", password="sekrit")
    url = get.call_args[0][0]
    assert "sekrit" not in url
    assert "u@" not in url
    assert get.call_args[1]["auth"] == ("u", "sekrit")


@pytest.mark.parametrize("status", [401, 403])
def test_poll_download_does_NOT_retry_an_auth_failure(status, caplog):
    """401/403 means the credentials are wrong; retrying cannot fix it.

    The retry branch keys on `400 <= code < 500`, so an auth failure must
    propagate on the FIRST response rather than be retried for 24 hours — and
    the password must not reach the log on the way out. Separate from the
    404 test because 401 is the failure an operator actually hits (a wrong
    `GBIF_PASSWORD` in the environment) and it is the one the ledger recorded
    as untested.

    `timeout_s=0` is load-bearing, not tidiness: with the default 24-hour
    deadline and `sleep` mocked, a mutant that swallowed the 4xx would spin
    forever and the harness could attribute nothing. With the deadline already
    past, such a mutant raises `DownloadFailed` instead of `HTTPError` and dies
    here, attributably and in milliseconds.
    """
    caplog.set_level(logging.DEBUG, logger="birdology.prior.gbif_download")
    resp = MagicMock()
    resp.status_code = status
    resp.raise_for_status.side_effect = requests.HTTPError(
        f"{status} Client Error", response=resp
    )
    get = MagicMock(return_value=resp)
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ) as sleep:
        with pytest.raises(requests.HTTPError):
            poll_download("k", user="u", password="sekrit", interval_s=0, timeout_s=0)
    assert get.call_count == 1, "an auth failure must not be retried"
    assert sleep.call_count == 0
    assert "sekrit" not in caplog.text


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_poll_download_RETRIES_a_server_error(status):
    """5xx is transient: GBIF restarting must not abandon a queued download.

    The retry test above uses a `ConnectionError`, which carries no `.response`
    and therefore never reaches the status-code comparison at all — so the
    boundary of `400 <= code < 500` was untested on its upper side. A mutant
    widening the client-error range to `400 <= code` survives everything else
    in this file.
    """
    bad = MagicMock()
    bad.status_code = status
    bad.raise_for_status.side_effect = requests.HTTPError(
        f"{status} Server Error", response=bad
    )
    succeeded = _json_resp({"status": "SUCCEEDED", "doi": "10.15468/dl.a"})
    get = MagicMock(side_effect=[bad, succeeded])
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ):
        meta = poll_download("k", user="u", password="p", interval_s=0)
    assert meta["doi"] == "10.15468/dl.a"
    assert get.call_count == 2, f"HTTP {status} must be retried, not raised"


def test_request_download_propagates_an_auth_failure(caplog):
    """The same 401 on the REQUEST call, which has no retry loop at all.

    `resp.raise_for_status()` is the only thing between a rejected credential
    and `resp.text.strip()` returning an HTML error page AS A DOWNLOAD KEY —
    which would then be polled forever and finally fetched as a 404 zip.
    """
    caplog.set_level(logging.DEBUG, logger="birdology.prior.gbif_download")
    resp = MagicMock()
    resp.status_code = 401
    resp.text = "<html>Unauthorized</html>"
    resp.raise_for_status.side_effect = requests.HTTPError(
        "401 Client Error", response=resp
    )
    with patch("birdology.prior.gbif_download.requests.post", return_value=resp):
        with pytest.raises(requests.HTTPError):
            request_download(DOF, user="u", password="sekrit", email="e@example.org")
    assert "sekrit" not in caplog.text


def test_fetch_download_propagates_an_auth_failure(tmp_path):
    """A 401 on the fetch must leave no `.part` and no destination file.

    `raise_for_status()` fires inside the `with requests.get(...)` block but
    BEFORE the `tmp.open`, so the cleanup path is a different one from the
    mid-stream failure already tested.
    """
    dest = tmp_path / "dof.zip"
    resp = _stream_resp(None, {})
    resp.status_code = 401
    resp.raise_for_status.side_effect = requests.HTTPError(
        "401 Client Error", response=resp
    )
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(requests.HTTPError):
            fetch_download("k", dest, user="u", password="p")
    assert not dest.exists()
    assert not (dest.parent / "dof.zip.part").exists()


def test_poll_download_requests_the_download_KEY_not_the_search_endpoint():
    """The polled URL is pinned because a wrong one returns 200 with JSON.

    `/occurrence/search` answers without auth and returns a dict with no
    `status` key, which `poll_download` would classify as an unrecognised
    status and raise on — the right outcome by accident, for the wrong reason.
    Pin the URL so the reason is the right one.
    """
    resp = _json_resp({"status": "SUCCEEDED", "doi": "10.15468/dl.abc123"})
    with patch(
        "birdology.prior.gbif_download.requests.get", return_value=resp
    ) as get:
        poll_download("0012345-260928120000000", user="u", password="p", interval_s=0)
    assert get.call_args[0][0] == (
        "https://api.gbif.org/v1/occurrence/download/0012345-260928120000000"
    )


def test_request_download_asks_for_SIMPLE_CSV_and_coordinate_filtered_AVES():
    """Every predicate is load-bearing and none is re-checked downstream.

    `occurrences.py` parses SIMPLE_CSV's flat TSV and would find no
    `occurrence.txt` in a DWCA zip; `indexing.py` needs a coordinate on every
    row; and without `TAXON_KEY` the download is every occurrence GBIF holds,
    which is neither Aves nor finishable. A wrong predicate produces a
    perfectly valid archive of the wrong records, hours later.
    """
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = "0012345-260928120000000"
    with patch(
        "birdology.prior.gbif_download.requests.post", return_value=resp
    ) as post:
        request_download(
            DOF, year_from=1990, user="u", password="p", email="e@example.org"
        )
    body = post.call_args[1]["json"]
    assert body["format"] == "SIMPLE_CSV"
    preds = {p["key"]: p for p in body["predicate"]["predicates"]}
    assert preds["TAXON_KEY"]["value"] == "212"
    assert preds["HAS_COORDINATE"]["value"] == "true"
    assert preds["HAS_GEOSPATIAL_ISSUE"]["value"] == "false"
    assert preds["YEAR"]["value"] == "1990"
    assert preds["YEAR"]["type"] == "greaterThanOrEquals"
    assert body["predicate"]["type"] == "and"


def test_request_download_omits_the_YEAR_predicate_when_no_year_is_given():
    """`year_from=None` must mean NO year filter, not year 0 or year None.

    A `{"value": "None"}` predicate is accepted by the API shape and rejects
    every record, so the download succeeds and is EMPTY.
    """
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = "k"
    with patch(
        "birdology.prior.gbif_download.requests.post", return_value=resp
    ) as post:
        request_download(DOF, user="u", password="p", email="e@example.org")
    keys = [p["key"] for p in post.call_args[1]["json"]["predicate"]["predicates"]]
    assert "YEAR" not in keys


def test_fetch_download_accepts_a_body_with_no_content_length(tmp_path):
    """GBIF streams chunked for large downloads: absent Content-Length is
    normal and must NOT be treated as a length of zero.

    A `int(raw_length or 0)` would compare `written != 0` and reject every
    chunked download as truncated — after transferring the whole 4 GB.
    """
    dest = tmp_path / "dof.zip"
    resp = _stream_resp([b"abc", b"de"], {})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        out = fetch_download("k", dest, user="u", password="p")
    assert out == dest
    assert dest.read_bytes() == b"abcde"


def test_fetch_download_rejects_a_body_LONGER_than_advertised(tmp_path):
    """The length check is an equality, not a floor.

    A body longer than `Content-Length` means the stream is not what the
    server described, and a `written < expected` check would accept it — then
    `occurrences.py` would open a zip whose central directory disagrees with
    its contents and report "no occurrence table found".
    """
    dest = tmp_path / "dof.zip"
    resp = _stream_resp([b"abcdefghijkl"], {"Content-Length": "10"})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        with pytest.raises(DownloadFailed, match="truncated download: 12 of 10 bytes"):
            fetch_download("k", dest, user="u", password="p")
    assert not dest.exists()
    assert not (dest.parent / "dof.zip.part").exists()


def test_fetch_download_tolerates_a_MALFORMED_content_length(tmp_path):
    """A non-numeric header must disable the check, not crash the transfer.

    `int("bogus")` raises ValueError, which is caught; a mutant that removed
    the catch would abandon a completed multi-hour download because of a
    misbehaving proxy's header.
    """
    dest = tmp_path / "dof.zip"
    resp = _stream_resp([b"abc"], {"Content-Length": "bogus"})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        out = fetch_download("k", dest, user="u", password="p")
    assert out == dest
    assert dest.read_bytes() == b"abc"


def test_poll_download_sleeps_at_least_one_second_between_polls():
    """`interval_s=0` must not become a tight loop against GBIF's API.

    The `max(interval_s, 1)` floor is the only thing preventing a caller's
    `interval_s=0` — which every test here passes — from hammering the
    endpoint thousands of times a second in production.
    """
    running = _json_resp({"status": "RUNNING"})
    succeeded = _json_resp({"status": "SUCCEEDED", "doi": "10.15468/dl.a"})
    get = MagicMock(side_effect=[running, succeeded])
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ) as sleep:
        poll_download("k", user="u", password="p", interval_s=0)
    assert sleep.call_args_list == [call(1)]


def test_a_transient_failure_SLEEPS_before_retrying():
    """A retry that skips the sleep spins on a persistent connection error.

    Every poll path — success-pending, terminal, transient — must pass through
    the one `time.sleep` at the bottom of the loop. Asserted as a COUNT against
    the number of retries so a `continue` inserted anywhere above the sleep
    fails, rather than as "sleep was called at least once", which a single
    pending poll would satisfy on its own.
    """
    boom = requests.ConnectionError("reset by peer")
    succeeded = _json_resp({"status": "SUCCEEDED", "doi": "10.15468/dl.a"})
    get = MagicMock(side_effect=[boom, boom, succeeded])
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ) as sleep:
        poll_download("k", user="u", password="p", interval_s=0)
    assert get.call_count == 3
    assert sleep.call_count == 2, "each transient failure must sleep before retrying"


@pytest.mark.parametrize("status", ["PREPARING", "RUNNING", "SUSPENDED"])
def test_poll_download_retries_every_PENDING_status(status):
    """All three pending statuses mean 'keep waiting', including SUSPENDED.

    GBIF suspends a download when its cluster is loaded and resumes it later.
    Treating any of the three as unrecognised raises on the FIRST poll and
    throws away a request that was about to run — and the message would blame
    an unrecognised status rather than say the download is still queued.
    """
    pending = _json_resp({"status": status})
    succeeded = _json_resp({"status": "SUCCEEDED", "doi": "10.15468/dl.a"})
    get = MagicMock(side_effect=[pending, succeeded])
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ):
        meta = poll_download("k", user="u", password="p", interval_s=0)
    assert meta["doi"] == "10.15468/dl.a"
    assert get.call_count == 2, f"{status} must be polled again, not raised on"


def test_poll_download_deadline_is_checked_AFTER_a_success_is_seen():
    """A download that succeeds on the last poll must be returned, not timed out.

    The status check precedes the deadline check, so a SUCCEEDED response
    arriving past the wall clock still returns its metadata. Pinned because
    swapping the two blocks reads identically and loses a finished multi-hour
    download to a one-second overrun.
    """
    resp = _json_resp({"status": "SUCCEEDED", "doi": "10.15468/dl.late"})
    with patch("birdology.prior.gbif_download.requests.get", return_value=resp):
        meta = poll_download("k", user="u", password="p", interval_s=0, timeout_s=0)
    assert meta["doi"] == "10.15468/dl.late"


def test_poll_download_never_logs_the_password_on_a_transient_failure(caplog):
    """The retry WARNING is the one log line that formats exception state.

    `requests` puts the full request URL in some exception reprs, so a
    `log.warning("... %s", exc)` instead of `type(exc).__name__` could emit a
    credential. The module deliberately logs the TYPE and the status code
    only.
    """
    caplog.set_level(logging.DEBUG, logger="birdology.prior.gbif_download")
    boom = requests.ConnectionError(
        "HTTPSConnectionPool(host='u:sekrit@api.gbif.org', port=443)"
    )
    succeeded = _json_resp({"status": "SUCCEEDED", "doi": "10.15468/dl.a"})
    get = MagicMock(side_effect=[boom, succeeded])
    with patch("birdology.prior.gbif_download.requests.get", get), patch(
        "birdology.prior.gbif_download.time.sleep"
    ):
        poll_download("k", user="u", password="sekrit", interval_s=0)
    assert "sekrit" not in caplog.text
    assert "ConnectionError" in caplog.text

