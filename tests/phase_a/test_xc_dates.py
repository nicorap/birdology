"""XC API v3 dating — no live network calls.

The only server here is an `http.server` bound to 127.0.0.1:0, which is not
network egress; it exists because the key-leak channels (urllib3's request-line
log and `requests`' HTTPError message) only exist once a real request is built,
and a mocked session can never prove they are closed.
"""
import http.server
import io
import logging
import sys
import threading
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import birdology.phase_a.xc_dates as xc_dates
from birdology.phase_a.xc_dates import (
    DateFetchFailed,
    _make_session,
    _redact,
    fetch_species_dates,
    parse_xc_date,
    read_key,
)

# Obviously fake, and long enough that it cannot collide with JSON keywords,
# a genus name or an HTTP reason phrase inside an assertion.
FAKE_KEY = "FAKE_XC_KEY_do_not_use_12345"


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
    out = fetch_species_dates("tinamus", "major", key=FAKE_KEY, session=session)
    assert out == {686892: date(2021, 10, 15), 686179: date(2020, 1, 2)}


def test_fetch_species_dates_follows_pages():
    session = MagicMock()
    session.get.side_effect = [
        _resp({"numPages": 2, "recordings": [{"id": "1", "date": "2021-01-01"}]}),
        _resp({"numPages": 2, "recordings": [{"id": "2", "date": "2021-01-02"}]}),
    ]
    out = fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)
    assert set(out) == {1, 2}
    assert session.get.call_count == 2


def test_fetch_species_dates_advances_the_page_parameter():
    """I4: counting the calls does not prove page 2 was ever asked for.

    Re-requesting page 1 `numPages` times would return 500 of 3000 dates and
    look perfectly healthy, which is this project's recurring failure mode.
    """
    session = MagicMock()
    session.get.side_effect = [
        _resp({"numPages": 3, "recordings": [{"id": "1", "date": "2021-01-01"}]}),
        _resp({"numPages": 3, "recordings": [{"id": "2", "date": "2021-01-02"}]}),
        _resp({"numPages": 3, "recordings": [{"id": "3", "date": "2021-01-03"}]}),
    ]
    fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)
    pages = [c[1]["params"]["page"] for c in session.get.call_args_list]
    assert pages == [1, 2, 3]


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
    out = fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)
    assert out == {1: date(2021, 1, 1)}


def test_fetch_species_dates_skips_a_null_id_without_losing_the_species():
    """I3: `int(None)` raises TypeError, which used to abort the whole species.

    Every date already collected for those 500-record pages went with it.
    """
    session = MagicMock()
    session.get.return_value = _resp(
        {
            "numPages": 1,
            "recordings": [
                {"id": None, "date": "2021-01-01"},
                {"id": "9", "date": "2021-01-02"},
            ],
        }
    )
    out = fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)
    assert out == {9: date(2021, 1, 2)}


def test_fetch_species_dates_counts_skipped_malformed_records(caplog):
    caplog.set_level(logging.INFO)
    session = MagicMock()
    session.get.return_value = _resp(
        {
            "numPages": 1,
            "recordings": [
                {"id": None, "date": "2021-01-01"},
                {"id": "not-an-int", "date": "2021-01-01"},
                {"id": "9", "date": "2021-01-02"},
            ],
        }
    )
    fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)
    assert "2 malformed ids skipped" in caplog.text
    assert FAKE_KEY not in caplog.text


def test_fetch_species_dates_raises_on_api_error():
    session = MagicMock()
    session.get.return_value = _resp(
        {"error": "client_error", "message": "Missing or invalid 'key' parameter."}
    )
    with pytest.raises(DateFetchFailed, match="client_error"):
        fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)


def test_fetch_species_dates_sends_a_tag_and_the_key():
    session = MagicMock()
    session.get.return_value = _resp({"numPages": 1, "recordings": []})
    fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)
    params = session.get.call_args[1]["params"]
    assert params["query"] == "gen:parus sp:major"
    assert params["key"] == FAKE_KEY
    assert params["per_page"] == 500


# --------------------------------------------------------------------------
# C1 — the key must not reach the URL string, a log record, or an exception.
# --------------------------------------------------------------------------


def test_redact_covers_the_url_encoded_form_of_the_key():
    """A key with `/` or `+` reaches a log line only percent-encoded."""
    key = "ab/cd+ef gh"
    assert _redact(f"...?key={key}&page=1", key) == "...?key=***&page=1"
    assert _redact("...?key=ab%2Fcd%2Bef%20gh&page=1", key) == "...?key=***&page=1"
    assert _redact("...?key=ab%2Fcd%2Bef+gh&page=1", key) == "...?key=***&page=1"


def test_fetch_species_dates_keeps_the_key_out_of_the_url_argument():
    """The key travels as a param, and NOTHING interpolates it into the URL.

    Asserting only `params["key"]` cannot fail on a mutation that ALSO builds
    `f"{XC_API}?key={key}"`; asserting the positional URL argument can.
    """
    session = MagicMock()
    session.get.return_value = _resp({"numPages": 1, "recordings": []})
    fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)

    args, kwargs = session.get.call_args
    assert kwargs["params"]["key"] == FAKE_KEY
    url = args[0] if args else kwargs["url"]
    assert FAKE_KEY not in url
    assert "key=" not in url


class _Always429(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"numPages":1,"recordings":[]}'
        self.send_response(429)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep pytest output clean
        pass


@pytest.fixture
def local_429_server():
    """A 127.0.0.1:0 server — no egress — so a real request is actually built."""
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Always429)
    srv.daemon_threads = True
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{srv.server_port}/api/3/recordings"
    finally:
        srv.shutdown()
        srv.server_close()
        thread.join(timeout=5)


def test_a_real_failing_request_leaks_the_key_to_neither_logs_nor_exception(
    local_429_server, monkeypatch
):
    """C1, both channels, measured the way a Slurm job would see them.

    urllib3 logs the full request line at DEBUG and `requests` puts the full URL
    in HTTPError's message; a mocked session builds neither, which is why the
    old caplog-only guard could not fail.
    """
    monkeypatch.setattr(xc_dates, "XC_API", local_429_server)

    # Attach a capturing handler to the root logger at DEBUG, the state a job
    # run with a debug flag is in. (`logging.basicConfig(force=True)` would do
    # the same but CLOSES pytest's own handlers, which swallows the report.)
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setLevel(logging.DEBUG)
    root = logging.getLogger()
    old_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        # One retry round only: the point is the message, not the backoff.
        session = xc_dates.requests.Session()
        session.mount(
            "http://",
            xc_dates.HTTPAdapter(
                max_retries=Retry(total=1, backoff_factor=0, status_forcelist=(429,))
            ),
        )
        with pytest.raises(DateFetchFailed) as excinfo:
            fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)
        logs = buf.getvalue()
    finally:
        root.removeHandler(handler)
        root.setLevel(old_level)

    assert "127.0.0.1" in logs, "urllib3 DEBUG diagnostics must stay visible"
    assert FAKE_KEY not in logs, f"key leaked into logs: {logs!r}"
    assert FAKE_KEY not in str(excinfo.value)
    assert FAKE_KEY not in repr(excinfo.getrepr())
    assert "***" in str(excinfo.value)


def test_urllib3_filter_is_removed_after_the_call(local_429_server, monkeypatch):
    """The filter's lifetime is the call's, not the process's."""
    monkeypatch.setattr(xc_dates, "XC_API", local_429_server)
    before = {
        logger.name: list(logger.filters) for logger in xc_dates._urllib3_loggers()
    }
    with pytest.raises(DateFetchFailed):
        fetch_species_dates("parus", "major", key=FAKE_KEY)
    for name, filters in before.items():
        assert list(logging.getLogger(name).filters) == filters


def test_every_urllib3_sublogger_is_covered_not_just_the_root():
    """`urllib3.util.retry` logs the raw URL too; a name list misses it.

    Filters do NOT run on ancestor loggers during propagation, so covering
    `urllib3` alone leaves `urllib3.connectionpool` and `urllib3.util.retry`
    unredacted — which is how the first version of this fix still leaked.
    """
    logging.getLogger("urllib3.util.retry")  # ensure it is instantiated
    names = {logger.name for logger in xc_dates._urllib3_loggers()}
    assert {"urllib3", "urllib3.connectionpool", "urllib3.util.retry"} <= names


def test_filter_redacts_a_key_passed_as_a_format_argument():
    """urllib3 logs `url=%r`; scrubbing only `record.msg` would miss it."""
    redactor = xc_dates._KeyRedactingFilter(FAKE_KEY)
    record = logging.LogRecord(
        "urllib3.util.retry",
        logging.DEBUG,
        __file__,
        1,
        "Incremented Retry for (url=%r)",
        (f"/api/3/recordings?key={FAKE_KEY}&page=1",),
        None,
    )
    redactor.filter(record)
    assert FAKE_KEY not in record.getMessage()
    assert "***" in record.getMessage()


# --------------------------------------------------------------------------
# I2 — the default session retries instead of dropping a species on one 429.
# --------------------------------------------------------------------------


def test_default_session_retries_429_with_backoff():
    session = _make_session()
    retries = session.get_adapter("https://xeno-canto.org/").max_retries
    assert isinstance(retries, Retry)
    assert 429 in retries.status_forcelist
    assert retries.total == 5
    assert retries.backoff_factor == 1.0
    assert retries.respect_retry_after_header is True


def test_injected_session_is_used_unchanged():
    """The `session=` seam the other tests inject through must keep working."""
    session = MagicMock()
    session.get.return_value = _resp({"numPages": 1, "recordings": []})
    fetch_species_dates("parus", "major", key=FAKE_KEY, session=session)
    assert session.get.call_count == 1
