#!/usr/bin/env python3
"""Mutation harness for `src/birdology/prior/gbif_download.py`.

Run from the repository root:

    uv run python tests/prior/mutation/run_gbif_download_mutants.py
    uv run python tests/prior/mutation/run_gbif_download_mutants.py --only G1 G9
    uv run python tests/prior/mutation/run_gbif_download_mutants.py --verify-labels

WHAT IS AND IS NOT EXERCISED — read this before trusting the table
=================================================================
This module cannot be run live in a test: it needs a free GBIF account, and a
national-scale download sits in GBIF's queue for hours. EVERY test behind
every mutant below is mocked HTTP (`unittest.mock.patch` over
`requests.get` / `requests.post`). That is a real limit on what the table
means, and it is stated here rather than implied.

GENUINELY EXERCISED, with mocked HTTP
-------------------------------------
* The REQUEST BODY, field by field: `format == "SIMPLE_CSV"` (the only format
  `occurrences.py` can parse), the `and`-combined predicate, `DATASET_KEY`,
  `TAXON_KEY == "212"` (Aves), `HAS_COORDINATE`, `HAS_GEOSPATIAL_ISSUE`, and
  `YEAR` present-when-given / ABSENT-when-None. A wrong predicate produces a
  perfectly valid archive of the wrong records hours later, which is the worst
  kind of defect this module can have and the one mocking CAN catch.
* THE POLL STATE MACHINE in full: SUCCEEDED returns, the four terminal
  statuses raise, an UNRECOGNISED status raises immediately rather than after
  24 h, the wall-clock deadline raises, a 5xx/connection blip is RETRIED, a
  4xx PROPAGATES (401 and 403 explicitly, which the ledger recorded as
  untested), and the `max(interval_s, 1)` sleep floor holds.
* THE FETCH's byte accounting: a short body raises and unlinks the `.part`, a
  LONG body raises too (the check is an equality, not a floor), an absent
  `Content-Length` disables the check rather than meaning zero, a malformed
  one is tolerated, a mid-stream exception leaves no `.part`, and a 401 before
  the first write leaves neither `.part` nor destination.
* CREDENTIAL HYGIENE: the password never appears in a URL, is passed as basic
  auth, and never reaches the log — including from the retry WARNING, whose
  fixture puts `u:sekrit@api.gbif.org` inside the exception message so a
  `log.warning("%s", exc)` would leak it.
* THE URL `fetch_download` RECONSTRUCTS, compared against the `downloadLink`
  shape GBIF's metadata advertises. See the honest limit below.

NOT EXERCISED, and mocking cannot fix it
----------------------------------------
1. THAT THE RECONSTRUCTED URL IS THE RIGHT ONE. `fetch_download` builds
   `{_BASE}/occurrence/download/request/{key}.zip` from the key; `poll_download`
   receives the AUTHORITATIVE `downloadLink` in the metadata and discards it.
   `test_fetch_download_url_matches_the_downloadLink_GBIF_ADVERTISES` pins the
   reconstruction against the documented shape, so a change to the
   reconstruction dies — but if GBIF moved the path, BOTH the module and that
   test would be wrong together and no mock could tell us. Using
   `poll_download`'s `downloadLink` would remove the class entirely; that is a
   change to a module this harness only mutates, and it is recorded in the
   report rather than made here.
2. THAT GBIF'S REAL RESPONSES HAVE THE SHAPES THESE MOCKS RETURN. Every status
   string (`PREPARING`, `SUSPENDED`, `FILE_ERASED`, ...) is taken from GBIF's
   docs, not observed. A status GBIF actually emits and we have not listed
   raises `DownloadFailed` on the FIRST poll, which is fail-fast and therefore
   the safe direction, but it would abandon a download that was merely in an
   unexpected-but-transient state.
3. THE REAL ARCHIVE. Nothing here proves that a live SIMPLE_CSV download is a
   zip with the members `occurrences.py` expects. That seam is only covered by
   `test_occurrences.py`'s hand-built archives, which encode our BELIEF about
   the format.
4. NETWORK REALITY: TLS, redirects, GBIF's rate limits, a connection dropping
   between the last chunk and `tmp.rename`, and the actual retry behaviour of
   `requests`' connection pool. `_TIMEOUT` (60 s) is never tested to have any
   effect — the mock answers instantly.
5. AUTHENTICATION ITSELF. A 401 is simulated by making `raise_for_status`
   raise; nothing proves the credentials are transmitted in a form GBIF
   accepts.

Consequently a green table here means "the module's LOGIC is pinned", not "the
download works". The honest test of this module is a live run, and it has not
been done on this branch.

WHY THIS HARNESS EXISTS — final-gate finding I1
----------------------------------------------
186 lines with no harness. It is the head of the pipeline and the only module
that touches credentials.
"""
# ---------------------------------------------------------------------------
# TEST SCOPE (final-gate finding I1)
# ---------------------------------------------------------------------------
# Every mutant is confronted with the WHOLE `tests/prior` directory. Derived
# consumers of gbif_download.py, by grep of every import under src/, scripts/
# and tests/prior:
#
#   (none)
#
# It is imported by NOTHING in the repository — not even `scripts/build_prior.py`,
# which takes an already-downloaded zip path. So unlike every other harness the
# wide scope buys this module nothing directly, and the honest statement is
# that `tests/prior/test_gbif_download.py` IS its consumer set. The directory
# scope is kept anyway, for one reason that is not cosmetic: it is how a mutant
# that breaks an import shared with another module (`requests`, `time`) shows up
# as a CRASHED row rather than a local kill, and it keeps every harness in this
# directory answerable to the same command. Cost: 0.22s vs 1.27s per mutant.
# ---------------------------------------------------------------------------

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _harness  # noqa: E402  -- sibling module; this file runs as a script

REPO = _harness.REPO
TARGET = Path("src/birdology/prior/gbif_download.py")
TESTS = "tests/prior/test_gbif_download.py"

# (id, description, killer_test, [(old, new), ...])
MUTANTS: list[tuple[str, str, str, list[tuple[str, str]]]] = [
    # ---- the request predicate: a wrong archive, hours later ---------------
    (
        "G1",
        "the download format changed to DWCA. The archive is then a Darwin "
        "Core Archive with a `meta.xml` and extension files, and "
        "`occurrences.py` — which looks for a flat tab-delimited member — "
        "raises 'no occurrence table found' after a multi-hour queue and a "
        "multi-gigabyte transfer",
        "test_request_download_asks_for_SIMPLE_CSV_and_coordinate_filtered_AVES",
        [('        "format": "SIMPLE_CSV",', '        "format": "DWCA",')],
    ),
    (
        "G2",
        "`HAS_COORDINATE` flipped to false, so the download is exactly the "
        "records `indexing.assign_cells` cannot place: every row is dropped "
        "for a missing coordinate and the prior is empty. The request "
        "succeeds, the download succeeds, and the failure appears three "
        "modules later as a zero-row table",
        "test_request_download_asks_for_SIMPLE_CSV_and_coordinate_filtered_AVES",
        [
            (
                '{"type": "equals", "key": "HAS_COORDINATE", "value": "true"}',
                '{"type": "equals", "key": "HAS_COORDINATE", "value": "false"}',
            )
        ],
    ),
    (
        "G3",
        "`HAS_GEOSPATIAL_ISSUE` flipped to true, which asks GBIF for ONLY the "
        "records it has flagged as geospatially broken — transposed "
        "coordinates, zero-island, country-centroid pins. Every row has a "
        "coordinate, so nothing downstream rejects them, and the prior is "
        "built entirely from records GBIF already knows are misplaced",
        "test_request_download_asks_for_SIMPLE_CSV_and_coordinate_filtered_AVES",
        [
            (
                '{"type": "equals", "key": "HAS_GEOSPATIAL_ISSUE", "value": "false"}',
                '{"type": "equals", "key": "HAS_GEOSPATIAL_ISSUE", "value": "true"}',
            )
        ],
    ),
    (
        "G4",
        "the Aves taxon key changed to 359 (Mammalia). Every predicate is "
        "still well-formed and GBIF answers happily; the download is mammals, "
        "and a bird prior built from it would be silently, completely wrong",
        "test_request_download_asks_for_SIMPLE_CSV_and_coordinate_filtered_AVES",
        [("_AVES_TAXON_KEY = 212", "_AVES_TAXON_KEY = 359")],
    ),
    (
        "G5",
        "the TAXON_KEY predicate dropped entirely, so the download is every "
        "occurrence in the dataset — plants, insects, everything — which for "
        "a national dataset is both far larger than asked for and no longer a "
        "bird target-group background, quietly changing what `ratio` MEANS",
        "test_request_download_asks_for_SIMPLE_CSV_and_coordinate_filtered_AVES",
        [
            (
                '        {"type": "equals", "key": "TAXON_KEY", "value": str(taxon_key)},\n',
                "",
            )
        ],
    ),
    (
        "G6",
        "the dataset predicate keyed on TAXON_KEY as well, so `dataset_key` is "
        "sent as a taxon key and the DATASET filter disappears: the download "
        "is all of GBIF matching a nonsense taxon — most likely empty, "
        "possibly enormous, and in neither case the requested dataset",
        "test_request_download_returns_key",
        [
            (
                '{"type": "equals", "key": "DATASET_KEY", "value": dataset_key}',
                '{"type": "equals", "key": "TAXON_KEY", "value": dataset_key}',
            )
        ],
    ),
    (
        "G7",
        "the YEAR predicate emitted UNCONDITIONALLY, so `year_from=None` "
        "becomes the string 'None' as a year bound. GBIF's shape check "
        "accepts it and the predicate matches nothing, so the download "
        "SUCCEEDS and is empty — the failure that looks most like success",
        "test_request_download_omits_the_YEAR_predicate_when_no_year_is_given",
        [("    if year_from is not None:", "    if True:")],
    ),
    (
        "G8",
        "the YEAR bound weakened from greaterThanOrEquals to greaterThan, so "
        "`year_from=1990` silently excludes 1990 itself. One year of a "
        "multi-decade archive, invisible in every aggregate",
        "test_request_download_asks_for_SIMPLE_CSV_and_coordinate_filtered_AVES",
        [
            (
                '{"type": "greaterThanOrEquals", "key": "YEAR", "value": str(year_from)}',
                '{"type": "greaterThan", "key": "YEAR", "value": str(year_from)}',
            )
        ],
    ),
    (
        "G9",
        "the predicates combined with `or` instead of `and`, so any ONE "
        "condition matching is enough: the download becomes every "
        "coordinate-bearing occurrence in GBIF. Structurally valid, "
        "catastrophically large, and it would be discovered only by the queue "
        "timing out",
        "test_request_download_asks_for_SIMPLE_CSV_and_coordinate_filtered_AVES",
        [('    return {"type": "and", "predicates": predicates}', '    return {"type": "or", "predicates": predicates}')],
    ),
    (
        "G10",
        "`raise_for_status` dropped from the request call, so a 401's HTML "
        "error page is returned AS A DOWNLOAD KEY. That key is then polled "
        "(404 -> retried as transient, because a 404 on a nonexistent key "
        "looks like a client error only if the status is read) and the "
        "operator gets a 24-hour wait instead of 'wrong password'",
        "test_request_download_propagates_an_auth_failure",
        [
            (
                "    resp.raise_for_status()\n    return resp.text.strip()",
                "    return resp.text.strip()",
            )
        ],
    ),
    # ---- the poll state machine ------------------------------------------
    (
        "G11",
        "the 4xx exclusion removed from the retry branch, so a 401 is treated "
        "as transient and retried until the 24-hour deadline. The operator's "
        "wrong password becomes a day of silence and then a timeout message "
        "that does not mention credentials. Its killer passes `timeout_s=0` so "
        "this mutant raises DownloadFailed rather than HANGING — a hanging "
        "mutant is attributable to nothing and was scored TIMEOUT/CRASHED "
        "before that fixture change",
        "test_poll_download_does_NOT_retry_an_auth_failure",
        [
            (
                "            if code is not None and 400 <= code < 500:\n                raise",
                "            if False:\n                raise",
            )
        ],
    ),
    (
        "G11b",
        "the client-error range widened to include 5xx (`400 <= code`), the "
        "opposite disjunct: a transient GBIF 503 now aborts a queued "
        "multi-hour download instead of being retried. SURVIVED the first run "
        "of this harness — `test_poll_download_survives_transient_error` raises "
        "a `ConnectionError`, which carries no `.response` and so never reaches "
        "the status comparison at all, leaving the upper edge of the range "
        "untested. Killed now by a test that retries an actual 5xx RESPONSE",
        "test_poll_download_RETRIES_a_server_error",
        [
            (
                "            if code is not None and 400 <= code < 500:",
                "            if code is not None and 400 <= code:",
            )
        ],
    ),
    (
        "G12",
        "a `continue` added after the transient-error warning, so a retry "
        "skips the SLEEP and the loop spins on a persistent connection error "
        "— thousands of requests a second against GBIF for up to 24 hours, "
        "with a warning line per attempt. The retry still 'works', which is "
        "why a test asserting only that the download eventually succeeds "
        "passes under it",
        "test_poll_download_survives_transient_error",
        [
            (
                "        if meta is not None:\n            status = meta.get(\"status\")",
                "            continue\n\n        if meta is not None:\n            status = meta.get(\"status\")",
            )
        ],
    ),
    (
        "G13",
        "the terminal-status set emptied, so KILLED / CANCELLED / FAILED / "
        "FILE_ERASED fall through to the 'unrecognised status' branch. Still "
        "raises `DownloadFailed`, so a test that only checks the EXCEPTION "
        "TYPE passes — which is exactly what happened: this mutant SURVIVED "
        "the first run of this harness, because `test_poll_download_raises_"
        "on_killed` asserted the type and nothing else. What is lost is the "
        "message naming the real status, which is the operator's only clue "
        "whether to re-request or to ask GBIF. Killed now by a test asserting "
        "the message names the status and does NOT say 'unrecognised'",
        "test_poll_download_raises_on_a_TERMINAL_status_and_NAMES_it",
        [
            (
                '_FAILED_STATUSES = frozenset({"KILLED", "CANCELLED", "FAILED", "FILE_ERASED"})',
                "_FAILED_STATUSES = frozenset()",
            )
        ],
    ),
    (
        "G14",
        "the unrecognised-status branch removed, so a status GBIF invents "
        "tomorrow — or a typo in the pending set — is treated as pending and "
        "polled for 24 hours before timing out. Fail-fast becomes fail-slow, "
        "and the timeout message blames the wall clock. Its killer passes "
        "`timeout_s=0` so the mutant raises the TIMEOUT message rather than "
        "HANGING; before that, this mutant looped until the harness's own "
        "deadline and was scored TIMEOUT/CRASHED, attributable to no test",
        "test_poll_download_raises_on_unknown_status",
        [
            (
                "            if status not in _PENDING_STATUSES:",
                "            if False:",
            )
        ],
    ),
    (
        "G15",
        "`SUSPENDED` dropped from the PENDING set. GBIF suspends a download "
        "when its cluster is under load and resumes it; treating that as "
        "unrecognised raises `DownloadFailed` on the first poll and throws "
        "away a queued request that was about to run. Separate mutant per "
        "pending status would be three near-identical rows, and this is the "
        "one whose meaning is least obvious from its name — the killer "
        "parametrises all three",
        "test_poll_download_retries_every_PENDING_status",
        [
            (
                '_PENDING_STATUSES = frozenset({"PREPARING", "RUNNING", "SUSPENDED"})',
                '_PENDING_STATUSES = frozenset({"PREPARING", "RUNNING"})',
            )
        ],
    ),
    (
        "G16",
        "the success test widened to `status != None`, so a KILLED download "
        "returns its metadata as if it had succeeded and `fetch_download` is "
        "then asked for a zip that does not exist",
        "test_poll_download_raises_on_killed",
        [
            (
                '            if status == "SUCCEEDED":',
                "            if status is not None:",
            )
        ],
    ),
    (
        "G17",
        "the deadline check moved BEFORE the status read (modelled by "
        "deleting the status block's guard on `meta is not None`, so the "
        "timeout wins). A download that succeeds on its last poll is thrown "
        "away after hours of waiting",
        "test_poll_download_deadline_is_checked_AFTER_a_success_is_seen",
        [
            (
                "        if meta is not None:\n            status = meta.get(\"status\")",
                "        if time.monotonic() >= deadline:\n            raise DownloadFailed(\n"
                '                f"download {download_key} timed out"\n'
                "            )\n"
                "        if meta is not None:\n            status = meta.get(\"status\")",
            )
        ],
    ),
    (
        "G18",
        "the sleep floor removed (`time.sleep(interval_s)`), so a caller's "
        "`interval_s=0` — which every test in this repository passes — becomes "
        "a tight loop hammering GBIF's API thousands of times a second. "
        "Invisible in any test that mocks `sleep` without asserting its "
        "argument",
        "test_poll_download_sleeps_at_least_one_second_between_polls",
        [
            (
                "        time.sleep(max(interval_s, 1))",
                "        time.sleep(interval_s)",
            )
        ],
    ),
    (
        "G19",
        "the retry WARNING formats the EXCEPTION rather than its type. "
        "`requests` puts the request URL in some exception reprs, so this is "
        "how a credential reaches a log file — the module deliberately logs "
        "the type and the HTTP status only",
        "test_poll_download_never_logs_the_password_on_a_transient_failure",
        [
            (
                "                type(exc).__name__,",
                "                exc,",
            )
        ],
    ),
    # ---- the fetch: byte accounting and cleanup --------------------------
    (
        "G20",
        "the truncation check weakened from `!=` to `<`, so a body LONGER "
        "than advertised is accepted. The zip's central directory then "
        "disagrees with its contents and `occurrences.py` reports 'no "
        "occurrence table found' — a message about the wrong module",
        "test_fetch_download_rejects_a_body_LONGER_than_advertised",
        [
            (
                "            if expected is not None and written != expected:",
                "            if expected is not None and written < expected:",
            )
        ],
    ),
    (
        "G21",
        "the truncation check skipped entirely, so an interrupted transfer is "
        "renamed to the destination and read as a complete archive. `zipfile` "
        "may or may not notice, depending on where the cut fell",
        "test_fetch_download_rejects_truncated_body",
        [
            (
                "            if expected is not None and written != expected:",
                "            if False:",
            )
        ],
    ),
    (
        "G22",
        "an absent `Content-Length` treated as ZERO rather than as 'no check' "
        "(`int(raw_length or 0)`), so every chunked download — which is how "
        "GBIF serves the large ones — is rejected as truncated AFTER the "
        "whole body has been transferred",
        "test_fetch_download_accepts_a_body_with_no_content_length",
        [
            (
                "            expected = int(raw_length) if raw_length is not None else None",
                "            expected = int(raw_length or 0)",
            )
        ],
    ),
    (
        "G23",
        "`ValueError` dropped from the malformed-header catch, leaving only "
        "`TypeError`, so a proxy's `Content-Length: bogus` raises out of the "
        "middle of a completed multi-gigabyte transfer instead of disabling "
        "the check. One mutant per disjunct of the compound except clause; the "
        "`TypeError` half is unreachable given the `is not None` test above it "
        "and is left alone rather than listed as an expected survivor",
        "test_fetch_download_tolerates_a_MALFORMED_content_length",
        [
            (
                "        except (TypeError, ValueError):\n            expected = None",
                "        except TypeError:\n            expected = None",
            )
        ],
    ),
    (
        "G24",
        "the `.part` cleanup removed, so a failed transfer leaves a partial "
        "file beside the destination. Harmless-looking, and the next run's "
        "`tmp.open('wb')` truncates it — the real cost is a half-written "
        "multi-gigabyte file left on a scratch volume after a crash",
        "test_fetch_download_cleans_up_on_stream_error",
        [
            (
                "        except BaseException:\n            tmp.unlink(missing_ok=True)\n            raise",
                "        except BaseException:\n            raise",
            )
        ],
    ),
    (
        "G25",
        "the body written STRAIGHT TO THE DESTINATION with no `.part` "
        "staging. SURVIVED the first run of this harness: every truncation "
        "test asserted `not dest.exists()`, which the mutant also satisfies "
        "because its cleanup unlinks `dest` — identical post-conditions, so "
        "the staging vanished unnoticed. What distinguishes them is an "
        "EXISTING file at `dest`: with staging a failed re-download leaves the "
        "previous good archive intact; without it, `open(dest, 'wb')` "
        "truncates it and the cleanup then deletes it, so a failed retry "
        "destroys a working download",
        "test_fetch_download_STAGES_through_a_part_file_never_the_destination",
        [
            (
                '    tmp = dest.with_suffix(dest.suffix + ".part")',
                "    tmp = dest",
            )
        ],
    ),
    (
        "G26",
        "the rename dropped, so a complete, verified download never appears "
        "at `dest` — the function returns a path to a file that does not "
        "exist and a caller's `read_occurrences(dest)` fails with "
        "FileNotFoundError after hours of transfer",
        "test_fetch_download_writes_complete_body",
        [("    tmp.rename(dest)\n    return dest", "    return dest")],
    ),
    (
        "G27",
        "empty chunks written rather than skipped. `requests` yields b'' for "
        "keep-alive chunks, so this changes nothing measurable today — kept "
        "live to record that the skip is a defensive no-op and not a "
        "load-bearing branch. EQUIVALENT: expected to be reported as such by "
        "surviving, which is why it is NOT in the live set below",
        "test_fetch_download_writes_complete_body",
        [
            (
                "                    if not chunk:\n                        continue",
                "                    if False:\n                        continue",
            )
        ],
    ),
    (
        "G28",
        "the credentials moved INTO the URL, which is how a password reaches "
        "every proxy log and `requests`' own exception messages. Every "
        "existing behavioural test stays green under it",
        "test_fetch_download_sends_the_credentials_as_BASIC_AUTH_not_in_the_url",
        [
            (
                '    url = f"{_BASE}/occurrence/download/request/{download_key}.zip"',
                '    url = f"https://{user}:{password}@api.gbif.org/v1/occurrence/download/request/{download_key}.zip"',
            )
        ],
    ),
    (
        "G29",
        "the fetched URL pointed at the metadata endpoint rather than the zip "
        "(the `/request/` segment and `.zip` dropped), so the 'archive' is a "
        "JSON document that `zipfile` rejects as a bad zip file — one module "
        "downstream, with no mention of the download",
        "test_fetch_download_url_matches_the_downloadLink_GBIF_ADVERTISES",
        [
            (
                '    url = f"{_BASE}/occurrence/download/request/{download_key}.zip"',
                '    url = f"{_BASE}/occurrence/download/{download_key}"',
            )
        ],
    ),
    (
        "G30",
        "the polled URL pointed at `/occurrence/search`, which answers 200 "
        "without auth and returns a dict with no `status` key. "
        "`poll_download` then raises 'unrecognised status=None' — the right "
        "outcome for entirely the wrong reason, and the operator is sent "
        "looking at GBIF's queue instead of at a wrong URL",
        "test_poll_download_requests_the_download_KEY_not_the_search_endpoint",
        [
            (
                '                f"{_BASE}/occurrence/download/{download_key}",',
                '                f"{_BASE}/occurrence/search",',
            )
        ],
    ),
    (
        "G31",
        "`raise_for_status` dropped from the fetch, so a 401's HTML error "
        "page is streamed to disk and renamed to `dof.zip`. The "
        "`Content-Length` matches (it is the error page's own length), so "
        "the truncation check passes and the failure surfaces as a corrupt "
        "archive",
        "test_fetch_download_propagates_an_auth_failure",
        [
            (
                "        resp.raise_for_status()\n        raw_length = resp.headers.get(\"Content-Length\")",
                '        raw_length = resp.headers.get("Content-Length")',
            )
        ],
    ),
    (
        "G32",
        "the destination's parent directory no longer created, so a fetch to "
        "a fresh output directory dies on FileNotFoundError when opening the "
        "`.part` — after the request has been issued",
        "test_fetch_download_rejects_truncated_body",
        [
            (
                "    dest.parent.mkdir(parents=True, exist_ok=True)",
                "    pass  # dest.parent.mkdir(parents=True, exist_ok=True)",
            )
        ],
    ),
]

# G27 is retired as EQUIVALENT, with the condition named rather than an
# aggregate quoted: the mutated branch is live only when `iter_content` yields
# a FALSY chunk, and writing b'' to a binary file advances neither the file
# position nor `written`. `requests` yields b'' for keep-alive packets, so the
# branch IS reachable — and provably behaviour-preserving when it is, because
# `fh.write(b"")` returns 0 and `len(b"") == 0`. There is therefore no fixture
# that distinguishes the two, and a mutant that cannot be killed by any
# possible test must not sit in the table as a survivor.
MUTANTS = [m for m in MUTANTS if m[0] != "G27"]


def main() -> int:
    return _harness.main(
        target=TARGET,
        mutants=MUTANTS,
        own_tests=TESTS,
        allow_unnamed=False,
    )


if __name__ == "__main__":
    raise SystemExit(main())
