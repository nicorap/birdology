# Phase A — Embedding Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the 581 GB BirdSet corpus on WEKA into a queryable index of `(recording, cell, week, species, embedding)` — dating every recording, assigning its H3 cell and ISO week, and caching frozen-encoder embeddings — so Phase B's sweep can look up a prior vector for any recording and fuse it with an acoustic posterior.

**Architecture:** Four independent stages, each a pure function over files on WEKA, each separately testable: **date** (XC API for focal, filename regex for soundscape) → **context** (lat/lon → H3 cell, date → ISO week, reusing `birdology.prior.indexing`) → **manifest** (one parquet joining recording path, context, label, split) → **embed** (Slurm job array, frozen encoder per shard, embeddings to WEKA). The manifest is the contract: Phase B reads only it and the embedding files, never the tar shards.

**Tech Stack:** Python 3.11+, `pyarrow` (manifest and metadata), `h3` (already a dependency), `requests` (XC API), `soundfile` + `torch` inside the Apptainer image, Slurm job arrays.

**Spec:** `docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md`

## Global Constraints

- **Every job that reads the corpus must pass `--bind /mnt/weka:/mnt/weka` to `apptainer`.** Measured: without it the container sees an EMPTY `/mnt/weka`, not an error. A shard loop would report "0 files" and exit 0.
- **Every job that uses a GPU must pass `--nv` to `apptainer` AND `--gres=gpu:N` to Slurm.** Measured: with `--gres=gpu:1` and `--nv`, `torch.cuda.is_available()` is True and the device is `NVIDIA H100 80GB HBM3`. Without `--gres`, CUDA is False inside the container even with `--nv`.
- **WEKA is on dgx171 ONLY today** (verified node by node across 21 up nodes). Every job in this plan must pin `--nodelist=dgx171` until the mount is fleet-wide, and must fail loudly if `/mnt/weka` is not a mountpoint rather than writing to a local path of the same name.
- Jobs run as `uid=1000(nvidia)`; the login user is `uid=1000(ubuntu)` group `hpcbuild`. `$HOME` is unreadable in a job and Slurm chdirs to `/tmp`. **`HF_HOME` and `HOME` must both be redirected** or `huggingface_hub` dies on `~/.cache/huggingface/token`.
- **A directory created on `/shared/apps` as `ubuntu` is not necessarily usable by a job.** Measured: `/shared/apps/ML/secrets` created as `ubuntu` did not exist from inside a job; `mkdir` from inside the job as `nvidia` succeeded. Create job-facing directories from inside a job.
- The XC v3 key lives in `/shared/apps/ML/secrets/xc_key` mode 600, created from inside a job. **Never** hardcoded, never logged, never in a filename, never in a command line (use a file read, not an argv or env value that `ps` can show).
- XC API v3: `https://xeno-canto.org/api/3/recordings?query=<tags>&key=<key>`. At least one search tag is required. `per_page=500` is honoured. v2 is retired (404).
- Paths: corpus `/mnt/weka/ML/birdset/<CONFIG>/<CONFIG>/`, Phase A outputs `/mnt/weka/ML/phase_a/`. Never hardcode `/mnt/weka` in library code — it comes from a parameter, defaulting to that path.
- Tests must not hit the network. Mock `requests` the way `tests/prior/test_gbif_download.py` does.
- The prior's own week convention is authoritative: import `WEEKS_PER_YEAR` and `iso_week` from `birdology.prior.indexing`, never a literal 52 and never a fresh `isocalendar()` call. ISO week 53 folds into 52 there, and a second derivation would disagree in 17.75% of years.

## Review Focus

Input classes the spec implies that no task's happy path exercises. Each line's test is assigned to the task that owns the code.

1. **A soundscape filename whose digits are not a date.** `HSN_001_20150708_061805.ogg` contains a station id AND a date; a corpus numbered `SSW_20240101_...` where `20240101` is a site code would parse to a valid-looking date and silently place every eval recording in one wrong week, shifting every control with it so nothing in the results table looks odd. The parser must reject an implausible date rather than accept a well-formed one. → Task 2.
2. **An XC recording whose `date` is absent, `'0000-00-00'`, or a year-only string.** XC has records back to the 1950s with partial dates. A `None` week must drop the recording from the manifest with a counted reason, never default to week 1. → Task 1.
3. **A recording whose lat/lon is missing** (1.7% of XCM). `h3.latlng_to_cell(None, None, r)` raises; the recording must be dropped with a counted reason, and the count must appear in the manifest's own metadata so a consumer sees the coverage they actually got. → Task 3.
4. **A shard that fails mid-decode** (truncated OGG, unreadable member). One bad file must not abandon the other ~478 in the shard, and the failure must be recorded per-file rather than only logged — a job array of 182 tasks makes a log line invisible. → Task 4.
5. **A resumed embedding run.** 182 shards × 5 s of GPU each is short, but XCL is 99 shards of ~5 GB and a preempted array task must not silently re-emit or skip. Completion is a per-shard output file's existence AND its row count matching the shard's file count. → Task 4.

---

### Task 1: Date the focal recordings from the XC API

**Files:**
- Create: `src/birdology/phase_a/__init__.py`
- Create: `src/birdology/phase_a/xc_dates.py`
- Test: `tests/phase_a/__init__.py`
- Test: `tests/phase_a/test_xc_dates.py`

**Interfaces:**
- Consumes: nothing (the XC API, mocked in tests).
- Produces:
  - `XC_API = "https://xeno-canto.org/api/3/recordings"`
  - `read_key(path: Path) -> str` — strips whitespace, raises `ValueError` on an empty file.
  - `parse_xc_date(raw: str | None) -> date | None` — `None` for absent, `'0000-00-00'`, year-only, or unparseable.
  - `fetch_species_dates(genus: str, species: str, *, key: str, per_page: int = 500, session=None) -> dict[int, date]` — all pages, `{xc_id: date}`, recordings with no usable date omitted.
  - `DateFetchFailed(RuntimeError)`.

- [ ] **Step 1: Write the failing test**

Create `tests/phase_a/__init__.py` (empty) and `tests/phase_a/test_xc_dates.py`:

```python
"""XC API v3 dating — no live network calls."""
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.xc_dates import (
    DateFetchFailed,
    fetch_species_dates,
    parse_xc_date,
    read_key,
)


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
    out = fetch_species_dates("tinamus", "major", key="k", session=session)
    assert out == {686892: date(2021, 10, 15), 686179: date(2020, 1, 2)}


def test_fetch_species_dates_follows_pages():
    session = MagicMock()
    session.get.side_effect = [
        _resp({"numPages": 2, "recordings": [{"id": "1", "date": "2021-01-01"}]}),
        _resp({"numPages": 2, "recordings": [{"id": "2", "date": "2021-01-02"}]}),
    ]
    out = fetch_species_dates("parus", "major", key="k", session=session)
    assert set(out) == {1, 2}
    assert session.get.call_count == 2


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
    out = fetch_species_dates("parus", "major", key="k", session=session)
    assert out == {1: date(2021, 1, 1)}


def test_fetch_species_dates_never_logs_the_key(caplog):
    import logging

    caplog.set_level(logging.DEBUG)
    session = MagicMock()
    session.get.return_value = _resp({"numPages": 1, "recordings": []})
    fetch_species_dates("parus", "major", key="sekrit", session=session)
    assert "sekrit" not in caplog.text


def test_fetch_species_dates_raises_on_api_error():
    session = MagicMock()
    session.get.return_value = _resp(
        {"error": "client_error", "message": "Missing or invalid 'key' parameter."}
    )
    with pytest.raises(DateFetchFailed, match="client_error"):
        fetch_species_dates("parus", "major", key="k", session=session)


def test_fetch_species_dates_sends_a_tag_and_the_key():
    session = MagicMock()
    session.get.return_value = _resp({"numPages": 1, "recordings": []})
    fetch_species_dates("parus", "major", key="k", session=session)
    params = session.get.call_args[1]["params"]
    assert params["query"] == "gen:parus sp:major"
    assert params["key"] == "k"
    assert params["per_page"] == 500
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/phase_a/test_xc_dates.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.phase_a'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/phase_a/__init__.py`:

```python
"""Phase A: turn the BirdSet corpus into a context-indexed embedding cache.

BirdSet's metadata carries lat/long but NO recording date, and the prior is
P(species | H3 cell, ISO week) -- so without a date the temporal half of the
experiment has no join key. Focal recordings are dated from the Xeno-Canto v3
API; soundscape recordings carry their date in the filename.

See docs/superpowers/plans/2026-09-30-phase-a-embedding-extraction.md
"""
```

Create `src/birdology/phase_a/xc_dates.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/phase_a/test_xc_dates.py -v`
Expected: PASS (13 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/phase_a tests/phase_a
git commit -m "feat(phase_a): date focal recordings from the XC v3 API

BirdSet metadata has lat/long but no recording date, so the prior's week axis
has no join key without this. Bulk-by-species because per-id dating of XCL
would be ~14 h of serial calls."
```

---

### Task 2: Date the soundscape recordings from their filenames

**Files:**
- Create: `src/birdology/phase_a/soundscape_dates.py`
- Test: `tests/phase_a/test_soundscape_dates.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `CORPUS_WINDOW: tuple[date, date]` — the plausible recording window, `(date(1990, 1, 1), date.today())`.
  - `parse_filename_date(name: str, *, window: tuple[date, date] = CORPUS_WINDOW) -> date` — raises `UndatableFilename` rather than returning None.
  - `UndatableFilename(ValueError)`.

- [ ] **Step 1: Write the failing test**

Create `tests/phase_a/test_soundscape_dates.py`:

```python
"""Soundscape filenames carry their own date. Measured: 12000/12000 in HSN."""
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.soundscape_dates import (
    UndatableFilename,
    parse_filename_date,
)


def test_parses_the_real_hsn_shape():
    # Measured from HSN_metadata_test.parquet.
    assert parse_filename_date("HSN_001_20150708_061805.ogg") == date(2015, 7, 8)


def test_parses_the_5s_segment_shape():
    assert parse_filename_date(
        "HSN_001_20150708_061805_000_005.ogg"
    ) == date(2015, 7, 8)


def test_rejects_a_filename_with_no_date():
    with pytest.raises(UndatableFilename):
        parse_filename_date("XC805337.ogg")


def test_rejects_an_IMPLAUSIBLE_date_that_is_well_formed():
    """Review Focus 1: the defect that would shift the whole eval set.

    A site code that parses as a valid date must be refused, not accepted.
    19010101 is a real date and not a real recording date.
    """
    with pytest.raises(UndatableFilename, match="outside"):
        parse_filename_date("SITE_19010101_0600.ogg")


def test_rejects_a_future_date():
    with pytest.raises(UndatableFilename, match="outside"):
        parse_filename_date("SITE_29991231_0600.ogg")


def test_rejects_digits_that_are_not_a_calendar_date():
    # 20151345: month 13, day 45.
    with pytest.raises(UndatableFilename):
        parse_filename_date("HSN_001_20151345_061805.ogg")


def test_a_station_id_that_looks_like_a_date_is_refused_by_the_window():
    """The specific trap: HSN_001_... has a station id AND a date."""
    # If a corpus numbered its sites 20150708 with no real date, the window is
    # what saves us -- but a 1970 site code inside the window would not be
    # caught, which is why the parser takes the LAST plausible match and the
    # manifest asserts a date spread (Task 3).
    with pytest.raises(UndatableFilename):
        parse_filename_date("SITE_12345678_0600.ogg")


def test_window_is_configurable_for_a_corpus_that_needs_it():
    assert parse_filename_date(
        "OLD_19500101_0600.ogg", window=(date(1940, 1, 1), date(1960, 1, 1))
    ) == date(1950, 1, 1)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/phase_a/test_soundscape_dates.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.phase_a.soundscape_dates'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/phase_a/soundscape_dates.py`:

```python
"""Date soundscape recordings from their filenames.

Measured on HSN: 12000/12000 filenames carry a YYYYMMDD, e.g.
`HSN_001_20150708_061805.ogg` -> 2015-07-08. The soundscape test sets are not
Xeno-Canto (HSN is Zenodo record 7525805), so the XC API cannot date them.

WHY A PLAUSIBILITY WINDOW AND NOT JUST A REGEX: `HSN_001_20150708_061805`
contains a station id AND a date, and a corpus that numbered its sites with
eight digits would parse to a valid-looking date. Every eval recording would
land in one wrong week, the prior lookup would be confidently wrong for the
whole eval set, and every control would shift with it -- so nothing in the
results table would look odd. A well-formed date is not enough; it must also be
a date a bird could have been recorded on.
"""
from __future__ import annotations

import re
from datetime import date

#: Recordings older than this are not in any BirdSet soundscape corpus, and a
#: date after today is a parse error by construction.
CORPUS_WINDOW = (date(1990, 1, 1), date.today())

_EIGHT_DIGITS = re.compile(r"(?<!\d)(\d{8})(?!\d)")


class UndatableFilename(ValueError):
    """The filename carries no plausible recording date.

    Deliberately an exception rather than a None return: a caller that forgets
    to check a None places the recording in a default week, which is the exact
    silent failure this module exists to prevent.
    """


def parse_filename_date(
    name: str, *, window: tuple[date, date] = CORPUS_WINDOW
) -> date:
    """The recording date encoded in *name*.

    Takes the LAST plausible eight-digit match, because a leading field is more
    often a site or station id (`HSN_001_20150708_...` has the id first).
    """
    lo, hi = window
    candidates = _EIGHT_DIGITS.findall(name)
    if not candidates:
        raise UndatableFilename(
            f"{name!r} contains no eight-digit group; a soundscape filename is "
            "expected to carry YYYYMMDD"
        )

    seen: list[str] = []
    for raw in reversed(candidates):
        seen.append(raw)
        try:
            parsed = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        except ValueError:
            continue
        if lo <= parsed <= hi:
            return parsed

    raise UndatableFilename(
        f"{name!r}: none of {seen} is a calendar date inside "
        f"{lo.isoformat()}..{hi.isoformat()}; refusing rather than guessing, "
        "because a wrong date silently moves the recording to another week"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/phase_a/test_soundscape_dates.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/phase_a/soundscape_dates.py tests/phase_a/test_soundscape_dates.py
git commit -m "feat(phase_a): date soundscape recordings from their filenames

Measured 12000/12000 on HSN. The plausibility window is the point: a site code
that parses as a date would put the whole eval set in one wrong week, with
every control shifting alongside it."
```

---

### Task 3: Build the context manifest

**Files:**
- Create: `src/birdology/phase_a/manifest.py`
- Test: `tests/phase_a/test_manifest.py`

**Interfaces:**
- Consumes: `parse_filename_date` (Task 2), `fetch_species_dates` (Task 1), `birdology.prior.indexing.iso_week`.
- Produces:
  - `MANIFEST_COLUMNS: tuple[str, ...]`
  - `DropReason` — `StrEnum` with `NO_DATE`, `NO_COORDINATES`, `UNDATABLE_FILENAME`, `BAD_CELL`.
  - `build_manifest(metadata: pa.Table, *, dates: dict[int, date] | None, resolution: int, split: str) -> tuple[pa.Table, dict[DropReason, int]]`
  - The returned table carries `pa.KeyValueMetadata` with the drop counts and the coverage fraction, so a consumer sees what they actually got.

- [ ] **Step 1: Write the failing test**

Create `tests/phase_a/test_manifest.py`:

```python
"""The manifest is Phase B's only contract with the corpus."""
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.manifest import (
    MANIFEST_COLUMNS,
    DropReason,
    build_manifest,
)


def _focal(rows):
    """(filepath, lat, long, ebird_code, id)"""
    return pa.table(
        {
            "filepath": pa.array([r[0] for r in rows], pa.string()),
            "lat": pa.array([r[1] for r in rows], pa.float64()),
            "long": pa.array([r[2] for r in rows], pa.float64()),
            "ebird_code": pa.array([r[3] for r in rows], pa.string()),
            "id": pa.array([r[4] for r in rows], pa.int64()),
        }
    )


def test_focal_row_gets_cell_and_week():
    t = _focal([("data/xc/parus_major/XC1.ogg", 55.6867, 12.57, "gretit", 1)])
    out, drops = build_manifest(
        t, dates={1: date(2021, 10, 15)}, resolution=4, split="train"
    )
    assert out.num_rows == 1
    row = out.to_pylist()[0]
    assert row["week"] == 41           # 2021-10-15 is ISO week 41
    assert isinstance(row["cell"], str)
    assert row["split"] == "train"
    assert not drops


def test_schema_is_exact():
    t = _focal([("a.ogg", 55.0, 12.0, "gretit", 1)])
    out, _ = build_manifest(t, dates={1: date(2021, 1, 4)}, resolution=4, split="train")
    assert tuple(out.schema.names) == MANIFEST_COLUMNS


def test_a_recording_with_no_date_is_DROPPED_and_COUNTED():
    """Review Focus 2: never default the week."""
    t = _focal([("a.ogg", 55.0, 12.0, "gretit", 1), ("b.ogg", 55.0, 12.0, "gretit", 2)])
    out, drops = build_manifest(
        t, dates={1: date(2021, 1, 4)}, resolution=4, split="train"
    )
    assert out.num_rows == 1
    assert drops[DropReason.NO_DATE] == 1


def test_a_recording_with_no_coordinates_is_DROPPED_and_COUNTED():
    """Review Focus 3: 1.7% of XCM has no lat/long."""
    t = pa.table(
        {
            "filepath": pa.array(["a.ogg", "b.ogg"], pa.string()),
            "lat": pa.array([55.0, None], pa.float64()),
            "long": pa.array([12.0, None], pa.float64()),
            "ebird_code": pa.array(["gretit", "gretit"], pa.string()),
            "id": pa.array([1, 2], pa.int64()),
        }
    )
    out, drops = build_manifest(
        t, dates={1: date(2021, 1, 4), 2: date(2021, 1, 4)},
        resolution=4, split="train",
    )
    assert out.num_rows == 1
    assert drops[DropReason.NO_COORDINATES] == 1


def test_drop_counts_and_coverage_are_IN_THE_TABLE_METADATA():
    """A consumer must see the coverage they actually got, not assume 100%."""
    t = _focal([("a.ogg", 55.0, 12.0, "gretit", 1), ("b.ogg", 55.0, 12.0, "gretit", 2)])
    out, _ = build_manifest(
        t, dates={1: date(2021, 1, 4)}, resolution=4, split="train"
    )
    meta = {k.decode(): v.decode() for k, v in (out.schema.metadata or {}).items()}
    assert meta["input_rows"] == "2"
    assert meta["kept_rows"] == "1"
    assert meta["coverage"].startswith("0.5")
    assert meta[f"dropped_{DropReason.NO_DATE.value}"] == "1"


def test_soundscape_rows_are_dated_from_the_filename():
    t = pa.table(
        {
            "filepath": pa.array(["HSN_001_20150708_061805.ogg"], pa.string()),
            "lat": pa.array([37.0], pa.float64()),
            "long": pa.array([-118.5], pa.float64()),
            "ebird_code": pa.array(["whcspa"], pa.string()),
        }
    )
    out, drops = build_manifest(t, dates=None, resolution=4, split="test_5s")
    assert out.num_rows == 1
    assert out.to_pylist()[0]["week"] == 28    # 2015-07-08 is ISO week 28
    assert not drops


def test_an_undatable_soundscape_filename_is_DROPPED_and_COUNTED():
    t = pa.table(
        {
            "filepath": pa.array(["no_date_here.ogg"], pa.string()),
            "lat": pa.array([37.0], pa.float64()),
            "long": pa.array([-118.5], pa.float64()),
            "ebird_code": pa.array(["whcspa"], pa.string()),
        }
    )
    out, drops = build_manifest(t, dates=None, resolution=4, split="test_5s")
    assert out.num_rows == 0
    assert drops[DropReason.UNDATABLE_FILENAME] == 1


def test_week_uses_THE_PRIORS_convention_including_the_53_fold():
    """A second derivation would disagree in 17.75% of years."""
    from birdology.prior.indexing import WEEKS_PER_YEAR

    t = _focal([("a.ogg", 55.0, 12.0, "gretit", 1)])
    out, _ = build_manifest(
        t, dates={1: date(2020, 12, 31)}, resolution=4, split="train"
    )
    # 2020-12-31 is ISO week 53, which folds into 52.
    assert out.to_pylist()[0]["week"] == WEEKS_PER_YEAR


def test_every_week_is_in_range():
    rows, dates = [], {}
    for i, d in enumerate(
        [date(2021, 1, 1), date(2020, 12, 31), date(2021, 6, 15), date(2016, 1, 1)]
    ):
        rows.append((f"{i}.ogg", 55.0, 12.0, "gretit", i))
        dates[i] = d
    out, _ = build_manifest(_focal(rows), dates=dates, resolution=4, split="train")
    for row in out.to_pylist():
        assert 1 <= row["week"] <= 52


def test_finer_resolution_splits_cells():
    rows = [("a.ogg", 55.0, 12.0, "gretit", 1), ("b.ogg", 55.9, 12.9, "gretit", 2)]
    dates = {1: date(2021, 1, 4), 2: date(2021, 1, 4)}
    coarse, _ = build_manifest(_focal(rows), dates=dates, resolution=3, split="train")
    fine, _ = build_manifest(_focal(rows), dates=dates, resolution=7, split="train")
    assert len(set(fine.column("cell").to_pylist())) >= len(
        set(coarse.column("cell").to_pylist())
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/phase_a/test_manifest.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.phase_a.manifest'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/phase_a/manifest.py`:

```python
"""The context manifest: Phase B's only contract with the corpus.

One row per usable recording, carrying where and when it was recorded, its
label, and its split. Phase B reads this and the embedding files; it never
opens a tar shard.

Every drop is COUNTED and the counts travel in the table's own metadata. A
manifest that silently lost 40% of a split looks identical to a complete one,
and the coverage a consumer got is not something they should have to re-derive.
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import date
from enum import StrEnum

import h3
import pyarrow as pa

from ..prior.indexing import iso_week
from .soundscape_dates import UndatableFilename, parse_filename_date

MANIFEST_COLUMNS = (
    "filepath",
    "cell",
    "week",
    "ebird_code",
    "split",
    "lat",
    "lon",
)

log = logging.getLogger(__name__)


class DropReason(StrEnum):
    NO_DATE = "no_date"
    NO_COORDINATES = "no_coordinates"
    UNDATABLE_FILENAME = "undatable_filename"
    BAD_CELL = "bad_cell"


_SCHEMA = pa.schema(
    [
        ("filepath", pa.string()),
        ("cell", pa.string()),
        ("week", pa.int8()),
        ("ebird_code", pa.string()),
        ("split", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
    ]
)


def _xc_id(row: dict) -> int | None:
    raw = row.get("id")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    return None


def build_manifest(
    metadata: pa.Table,
    *,
    dates: dict[int, date] | None,
    resolution: int,
    split: str,
) -> tuple[pa.Table, dict[DropReason, int]]:
    """Join BirdSet metadata to (cell, week).

    `dates` is the XC id -> date map for a FOCAL split. Pass None for a
    SOUNDSCAPE split, whose dates come from the filename.
    """
    cols: dict[str, list] = {name: [] for name in MANIFEST_COLUMNS}
    drops: Counter[DropReason] = Counter()

    for row in metadata.to_pylist():
        path = row.get("filepath") or ""

        if dates is None:
            try:
                when = parse_filename_date(path)
            except UndatableFilename:
                drops[DropReason.UNDATABLE_FILENAME] += 1
                continue
        else:
            xc = _xc_id(row)
            when = dates.get(xc) if xc is not None else None
            if when is None:
                drops[DropReason.NO_DATE] += 1
                continue

        lat, lon = row.get("lat"), row.get("long")
        if lat is None or lon is None:
            drops[DropReason.NO_COORDINATES] += 1
            continue

        try:
            cell = h3.latlng_to_cell(float(lat), float(lon), resolution)
        except (ValueError, TypeError):
            drops[DropReason.BAD_CELL] += 1
            continue

        cols["filepath"].append(path)
        cols["cell"].append(cell)
        cols["week"].append(iso_week(when))
        cols["ebird_code"].append(row.get("ebird_code") or "")
        cols["split"].append(split)
        cols["lat"].append(float(lat))
        cols["lon"].append(float(lon))

    table = pa.table([cols[n] for n in MANIFEST_COLUMNS], schema=_SCHEMA)

    n_in = metadata.num_rows
    n_out = table.num_rows
    meta = {
        "input_rows": str(n_in),
        "kept_rows": str(n_out),
        "coverage": f"{(n_out / n_in) if n_in else 0.0:.6f}",
        "resolution": str(resolution),
        "split": split,
    }
    for reason in DropReason:
        meta[f"dropped_{reason.value}"] = str(drops.get(reason, 0))

    log.info(
        "manifest %s: kept %d/%d (%.1f%%), drops=%s",
        split, n_out, n_in, 100 * (n_out / n_in if n_in else 0), dict(drops),
    )
    return table.replace_schema_metadata(meta), dict(drops)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/phase_a/test_manifest.py -v`
Expected: PASS (10 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/phase_a/manifest.py tests/phase_a/test_manifest.py
git commit -m "feat(phase_a): the context manifest, with drop counts in its metadata

A manifest that silently lost 40% of a split looks identical to a complete one,
so every drop is counted by reason and the coverage travels with the table."
```

---

### Task 4: Shard embedding extraction

**Files:**
- Create: `src/birdology/phase_a/embed.py`
- Test: `tests/phase_a/test_embed.py`

**Interfaces:**
- Consumes: the manifest (Task 3).
- Produces:
  - `iter_shard_audio(shard: Path) -> Iterator[tuple[str, bytes]]` — in-place, never extracts the tar.
  - `ShardResult` dataclass: `shard: str`, `embedded: int`, `failed: dict[str, str]`.
  - `embed_shard(shard, manifest, encoder, *, out_dir) -> ShardResult`
  - `shard_is_complete(shard: Path, out_dir: Path) -> bool` — output exists AND its row count matches.

- [ ] **Step 1: Write the failing test**

Create `tests/phase_a/test_embed.py`:

```python
"""Shard-level embedding extraction. No GPU, no network."""
import io
import sys
import tarfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
import soundfile as sf

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.embed import (
    ShardResult,
    embed_shard,
    iter_shard_audio,
    shard_is_complete,
)


def _ogg_bytes(seconds=1.0, sr=32000):
    buf = io.BytesIO()
    x = np.sin(2 * np.pi * 440 * np.arange(int(sr * seconds)) / sr) * 0.1
    sf.write(buf, x, sr, format="OGG", subtype="VORBIS")
    return buf.getvalue()


def _shard(tmp_path, members):
    p = tmp_path / "shard_0001.tar.gz"
    with tarfile.open(p, "w:gz") as tf:
        for name, payload in members:
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            tf.addfile(info, io.BytesIO(payload))
    return p


def _manifest(paths):
    return pa.table(
        {
            "filepath": pa.array(paths, pa.string()),
            "cell": pa.array(["c"] * len(paths), pa.string()),
            "week": pa.array([1] * len(paths), pa.int8()),
            "ebird_code": pa.array(["gretit"] * len(paths), pa.string()),
            "split": pa.array(["train"] * len(paths), pa.string()),
            "lat": pa.array([55.0] * len(paths), pa.float64()),
            "lon": pa.array([12.0] * len(paths), pa.float64()),
        }
    )


def _encoder(n_dims=8):
    """Stand-in for a frozen encoder: deterministic, no torch needed."""
    def run(samples, sr):
        return np.full(n_dims, float(len(samples)), dtype=np.float32)
    return run


def test_iter_shard_audio_reads_without_extracting(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    got = list(iter_shard_audio(p))
    assert len(got) == 1
    name, payload = got[0]
    assert name == "a/XC1.ogg"
    assert sf.info(io.BytesIO(payload)).samplerate == 32000


def test_iter_shard_audio_skips_non_audio_members(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/notes.txt", b"hi")])
    assert [n for n, _ in iter_shard_audio(p)] == ["a/XC1.ogg"]


def test_embed_shard_writes_one_row_per_manifest_file(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    res = embed_shard(p, _manifest(["a/XC1.ogg", "a/XC2.ogg"]), _encoder(), out_dir=out)
    assert isinstance(res, ShardResult)
    assert res.embedded == 2
    assert not res.failed
    written = pa.parquet.read_table(out / "shard_0001.parquet")
    assert written.num_rows == 2
    assert "embedding" in written.schema.names


def test_a_file_absent_from_the_manifest_is_not_embedded(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC9.ogg", _ogg_bytes())])
    res = embed_shard(
        p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=tmp_path / "emb"
    )
    assert res.embedded == 1


def test_ONE_BAD_FILE_does_not_abandon_the_shard(tmp_path):
    """Review Focus 4: a truncated member must not lose the other files."""
    p = _shard(
        tmp_path,
        [
            ("a/XC1.ogg", _ogg_bytes()),
            ("a/XC2.ogg", b"this is not an ogg file"),
            ("a/XC3.ogg", _ogg_bytes()),
        ],
    )
    res = embed_shard(
        p, _manifest(["a/XC1.ogg", "a/XC2.ogg", "a/XC3.ogg"]),
        _encoder(), out_dir=tmp_path / "emb",
    )
    assert res.embedded == 2
    assert "a/XC2.ogg" in res.failed


def test_failures_are_RECORDED_PER_FILE_not_only_logged(tmp_path):
    """182 array tasks make a log line invisible."""
    p = _shard(tmp_path, [("a/XC1.ogg", b"bad")])
    out = tmp_path / "emb"
    res = embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    assert res.failed["a/XC1.ogg"]
    assert (out / "shard_0001.failures.json").exists()


def test_shard_is_complete_requires_a_MATCHING_ROW_COUNT(tmp_path):
    """Review Focus 5: a preempted task must not look finished."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes()), ("a/XC2.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    assert not shard_is_complete(p, out)

    # A partial write: one row where the shard holds two audio members.
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    assert not shard_is_complete(p, out)

    embed_shard(p, _manifest(["a/XC1.ogg", "a/XC2.ogg"]), _encoder(), out_dir=out)
    assert shard_is_complete(p, out)


def test_embedding_width_is_preserved(tmp_path):
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(n_dims=16), out_dir=out)
    t = pa.parquet.read_table(out / "shard_0001.parquet")
    assert len(t.column("embedding")[0].as_py()) == 16


def test_context_columns_travel_with_the_embedding(tmp_path):
    """Phase B joins on these; an embedding with no context is useless."""
    p = _shard(tmp_path, [("a/XC1.ogg", _ogg_bytes())])
    out = tmp_path / "emb"
    embed_shard(p, _manifest(["a/XC1.ogg"]), _encoder(), out_dir=out)
    row = pa.parquet.read_table(out / "shard_0001.parquet").to_pylist()[0]
    for col in ("filepath", "cell", "week", "ebird_code", "split"):
        assert col in row
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/phase_a/test_embed.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.phase_a.embed'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/phase_a/embed.py`:

```python
"""Embed one shard with a frozen encoder.

Reads audio IN PLACE from the tar: extracting XCL's 99 shards would double
485 GB of disk for no benefit, and `tarfile.extractfile` plus `soundfile` on a
BytesIO is measured to work on this corpus.

One shard is one unit of work and one output file, because the fleet runs this
as a Slurm job array. Completion is an output file whose row count matches the
shard's audio-member count -- not merely its existence, since a preempted task
leaves a short file that looks finished.
"""
from __future__ import annotations

import io
import json
import logging
import tarfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import soundfile as sf

_AUDIO_SUFFIXES = (".ogg", ".flac", ".wav", ".mp3")

log = logging.getLogger(__name__)


@dataclass
class ShardResult:
    shard: str
    embedded: int
    failed: dict[str, str] = field(default_factory=dict)


def iter_shard_audio(shard: Path) -> Iterator[tuple[str, bytes]]:
    """`(member_name, raw_bytes)` for every audio member, without extracting."""
    with tarfile.open(shard, "r:*") as tf:
        for member in tf:
            if not member.isfile():
                continue
            if not member.name.lower().endswith(_AUDIO_SUFFIXES):
                continue
            handle = tf.extractfile(member)
            if handle is None:
                continue
            yield member.name, handle.read()


def _audio_member_count(shard: Path) -> int:
    with tarfile.open(shard, "r:*") as tf:
        return sum(
            1
            for m in tf
            if m.isfile() and m.name.lower().endswith(_AUDIO_SUFFIXES)
        )


def _out_paths(shard: Path, out_dir: Path) -> tuple[Path, Path]:
    stem = shard.name.split(".")[0]
    return out_dir / f"{stem}.parquet", out_dir / f"{stem}.failures.json"


def embed_shard(
    shard: Path,
    manifest: pa.Table,
    encoder: Callable[[np.ndarray, int], np.ndarray],
    *,
    out_dir: Path,
) -> ShardResult:
    """Embed every manifest-listed audio member of *shard*."""
    shard = Path(shard)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table_path, failures_path = _out_paths(shard, out_dir)

    wanted = {
        row["filepath"]: row for row in manifest.to_pylist()
    }

    rows: list[dict] = []
    failed: dict[str, str] = {}

    for name, payload in iter_shard_audio(shard):
        context = wanted.get(name)
        if context is None:
            continue
        try:
            samples, sr = sf.read(io.BytesIO(payload), dtype="float32", always_2d=False)
            if getattr(samples, "ndim", 1) > 1:
                samples = samples.mean(axis=1)
            vector = np.asarray(encoder(samples, sr), dtype=np.float32)
        except Exception as exc:  # one bad file must not lose the shard
            failed[name] = f"{type(exc).__name__}: {exc}"
            log.warning("shard %s: %s failed: %s", shard.name, name, exc)
            continue
        rows.append(
            {
                "filepath": name,
                "cell": context["cell"],
                "week": context["week"],
                "ebird_code": context["ebird_code"],
                "split": context["split"],
                "embedding": vector.tolist(),
            }
        )

    pq.write_table(pa.Table.from_pylist(rows), table_path)
    # Written unconditionally: an absent failures file and an empty one are
    # different statements, and a consumer must be able to tell.
    failures_path.write_text(json.dumps(failed, indent=2), encoding="utf-8")

    return ShardResult(shard=shard.name, embedded=len(rows), failed=failed)


def shard_is_complete(shard: Path, out_dir: Path) -> bool:
    """True when this shard's output exists AND accounts for every member.

    Existence alone is not completion: a preempted array task leaves a short
    parquet that a re-run would otherwise skip.
    """
    table_path, failures_path = _out_paths(Path(shard), Path(out_dir))
    if not table_path.exists():
        return False
    try:
        embedded = pq.read_metadata(table_path).num_rows
    except Exception:
        return False
    failures = 0
    if failures_path.exists():
        try:
            failures = len(json.loads(failures_path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            return False
    return embedded + failures >= _audio_member_count(Path(shard))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/phase_a/test_embed.py -v`
Expected: PASS (9 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/phase_a/embed.py tests/phase_a/test_embed.py
git commit -m "feat(phase_a): embed one shard in place, resumably

Reads audio from the tar without extracting (XCL would otherwise need another
485 GB). Completion is a matching row count, not file existence, because a
preempted array task leaves a short file that looks finished."
```

---

### Task 5: The driver script and the Slurm array

**Files:**
- Create: `scripts/phase_a_manifest.py`
- Create: `src/birdology/phase_a/embed_cli.py`
- Create: `src/birdology/phase_a/encoders.py`
- Create: `scripts/cluster/phase_a_embed.sbatch`
- Test: `tests/phase_a/test_phase_a_cli.py`
- Modify: `Makefile` (add a `phase-a-manifest` target)
- Modify: `README.md` (document both under "Scripts")

**Interfaces:**
- Consumes: every module above.
- Produces:
  - `build_all(corpus_dir, *, config, resolution, key_file, out_dir) -> Path` — writes `<out_dir>/<config>_r<res>_manifest.parquet`.
  - CLI: `python scripts/phase_a_manifest.py --corpus /mnt/weka/ML/birdset --config XCM --resolution 4 --key-file /shared/apps/ML/secrets/xc_key --out /mnt/weka/ML/phase_a`
  - `embed_cli.main()` — the one-shard entry point the array task runs INSIDE the container (`python3 -m birdology.phase_a.embed_cli`). It lives in the package, not in `scripts/`, because the container has the repo on its `PYTHONPATH` but no notion of the script directory.
  - `phase_a_embed.sbatch` — a job array over shards, one GPU per task.

- [ ] **Step 1: Write the failing test**

Create `tests/phase_a/test_phase_a_cli.py`:

```python
"""End-to-end manifest build from fixture metadata. No network."""
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from phase_a_manifest import build_all


def _write_corpus(tmp_path, config="XCM"):
    d = tmp_path / config / config
    d.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "filepath": pa.array(
                    ["data/xc/parus_major/XC1.ogg", "data/xc/parus_major/XC2.ogg"],
                    pa.string(),
                ),
                "lat": pa.array([55.6867, 55.7], pa.float64()),
                "long": pa.array([12.57, 12.6], pa.float64()),
                "ebird_code": pa.array(["gretit", "gretit"], pa.string()),
                "genus": pa.array(["parus", "parus"], pa.string()),
                "id": pa.array([1, 2], pa.int64()),
            }
        ),
        d / f"{config}_metadata.parquet",
    )
    return tmp_path


def test_build_all_writes_a_manifest(tmp_path):
    corpus = _write_corpus(tmp_path)
    key = tmp_path / "key"
    key.write_text("k\n")
    with patch(
        "phase_a_manifest.fetch_species_dates",
        return_value={1: date(2021, 10, 15), 2: date(2021, 6, 1)},
    ):
        path = build_all(
            corpus, config="XCM", resolution=4, key_file=key,
            out_dir=tmp_path / "out",
        )
    assert path.exists()
    t = pq.read_table(path)
    assert t.num_rows == 2
    assert set(t.column("week").to_pylist()) == {41, 22}


def test_build_all_records_coverage_in_the_output(tmp_path):
    corpus = _write_corpus(tmp_path)
    key = tmp_path / "key"
    key.write_text("k\n")
    with patch(
        "phase_a_manifest.fetch_species_dates", return_value={1: date(2021, 10, 15)}
    ):
        path = build_all(
            corpus, config="XCM", resolution=4, key_file=key,
            out_dir=tmp_path / "out",
        )
    meta = {k.decode(): v.decode() for k, v in pq.read_table(path).schema.metadata.items()}
    assert meta["kept_rows"] == "1"
    assert meta["dropped_no_date"] == "1"


def test_build_all_REFUSES_a_manifest_with_no_rows(tmp_path):
    """An empty manifest is a failed build, not a valid artifact."""
    corpus = _write_corpus(tmp_path)
    key = tmp_path / "key"
    key.write_text("k\n")
    with patch("phase_a_manifest.fetch_species_dates", return_value={}):
        with pytest.raises(ValueError, match="no usable recordings"):
            build_all(
                corpus, config="XCM", resolution=4, key_file=key,
                out_dir=tmp_path / "out",
            )


def test_build_all_REFUSES_coverage_below_the_floor(tmp_path):
    """A build that lost most of its corpus must not ship silently."""
    corpus = _write_corpus(tmp_path)
    key = tmp_path / "key"
    key.write_text("k\n")
    with patch(
        "phase_a_manifest.fetch_species_dates", return_value={1: date(2021, 10, 15)}
    ):
        with pytest.raises(ValueError, match="coverage"):
            build_all(
                corpus, config="XCM", resolution=4, key_file=key,
                out_dir=tmp_path / "out", min_coverage=0.9,
            )


def test_the_sbatch_script_binds_weka_and_requests_a_gpu():
    """Both are silent failures: no bind = empty dir, no gres = CUDA False."""
    script = (
        Path(__file__).parent.parent.parent
        / "scripts" / "cluster" / "phase_a_embed.sbatch"
    ).read_text()
    assert "--bind /mnt/weka:/mnt/weka" in script
    assert "--nv" in script
    assert "--gres=gpu:" in script
    assert "--nodelist=dgx171" in script


def test_the_sbatch_script_fails_if_weka_is_not_mounted():
    script = (
        Path(__file__).parent.parent.parent
        / "scripts" / "cluster" / "phase_a_embed.sbatch"
    ).read_text()
    assert "mountpoint" in script
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/phase_a/test_phase_a_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'phase_a_manifest'`

- [ ] **Step 3: Write the driver**

Create `scripts/phase_a_manifest.py`:

```python
#!/usr/bin/env python3
"""Build the Phase A context manifest for one BirdSet config.

    python scripts/phase_a_manifest.py \
        --corpus /mnt/weka/ML/birdset --config XCM --resolution 4 \
        --key-file /shared/apps/ML/secrets/xc_key --out /mnt/weka/ML/phase_a

FOCAL configs (XCM, XCL, and each test set's `_train` metadata) are dated from
the Xeno-Canto v3 API. SOUNDSCAPE metadata (`_test`, `_test_5s`) is dated from
the filename and needs no key.

See docs/superpowers/plans/2026-09-30-phase-a-embedding-extraction.md
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.phase_a.manifest import build_manifest
from birdology.phase_a.xc_dates import fetch_species_dates, read_key

log = logging.getLogger("phase_a")

#: Below this share of usable recordings the build refuses. A manifest that
#: lost most of its corpus is a failed build, not a small one.
DEFAULT_MIN_COVERAGE = 0.5


def _metadata_path(corpus_dir: Path, config: str) -> Path:
    d = Path(corpus_dir) / config / config
    for name in (f"{config}_metadata.parquet", f"{config}_metadata_train.parquet"):
        if (d / name).exists():
            return d / name
    raise FileNotFoundError(f"no metadata parquet under {d}")


def _is_soundscape(path: Path) -> bool:
    return "_test" in path.name


def build_all(
    corpus_dir: Path,
    *,
    config: str,
    resolution: int,
    key_file: Path | None,
    out_dir: Path,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> Path:
    """Metadata in, manifest parquet out. Returns the written path."""
    meta_path = _metadata_path(Path(corpus_dir), config)
    metadata = pq.read_table(meta_path)
    log.info("read %d rows from %s", metadata.num_rows, meta_path)

    if _is_soundscape(meta_path):
        dates = None
        split = "test"
    else:
        if key_file is None:
            raise ValueError(
                f"{meta_path.name} is a focal split and needs --key-file to date it"
            )
        key = read_key(Path(key_file))
        species = sorted(
            {
                (row["genus"], row["filepath"].split("/")[-2].split("_")[-1])
                for row in metadata.select(["genus", "filepath"]).to_pylist()
                if row.get("genus")
            }
        )
        log.info("dating %d species from the XC API", len(species))
        dates = {}
        for genus, sp in species:
            dates.update(fetch_species_dates(genus, sp, key=key))
        log.info("dated %d recordings", len(dates))
        split = "train"

    table, drops = build_manifest(
        metadata, dates=dates, resolution=resolution, split=split
    )

    if table.num_rows == 0:
        raise ValueError(
            f"{config}: no usable recordings after dating and context assignment "
            f"(drops={drops}); an empty manifest is a failed build"
        )
    coverage = table.num_rows / metadata.num_rows
    if coverage < min_coverage:
        raise ValueError(
            f"{config}: coverage {coverage:.3f} is below the floor {min_coverage} "
            f"({table.num_rows}/{metadata.num_rows} kept, drops={drops}). Pass a "
            "lower --min-coverage deliberately if this corpus really is that sparse"
        )

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{config}_r{resolution}_manifest.parquet"
    pq.write_table(table, path)
    log.info("wrote %s (%d rows, coverage %.3f)", path, table.num_rows, coverage)
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--corpus", required=True, type=Path)
    ap.add_argument("--config", required=True)
    ap.add_argument("--resolution", type=int, default=4)
    ap.add_argument("--key-file", type=Path, default=None)
    ap.add_argument("--out", type=Path, default=Path("/mnt/weka/ML/phase_a"))
    ap.add_argument("--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE)
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    print(
        build_all(
            args.corpus,
            config=args.config,
            resolution=args.resolution,
            key_file=args.key_file,
            out_dir=args.out,
            min_coverage=args.min_coverage,
        )
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Write the one-shard entry point**

Create `src/birdology/phase_a/embed_cli.py`. This is what the array task runs
inside the container, so it must import nothing from `scripts/`:

```python
#!/usr/bin/env python3
"""Embed ONE shard. The entry point a Slurm array task runs in the container.

    python3 -m birdology.phase_a.embed_cli \
        --shard /mnt/weka/ML/birdset/XCM/XCM/XCM_shard_0001.tar.gz \
        --manifest /mnt/weka/ML/phase_a/XCM_r4_manifest.parquet \
        --out /mnt/weka/ML/phase_a/embeddings/XCM

Skips a shard already complete (output row count accounts for every member), so
a re-submitted array is idempotent and a preempted task is redone rather than
skipped.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pyarrow.parquet as pq

from .embed import embed_shard, shard_is_complete
from .encoders import load_encoder

log = logging.getLogger("phase_a.embed")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shard", required=True, type=Path)
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--encoder", default="perch")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    if not args.force and shard_is_complete(args.shard, args.out):
        log.info("%s already complete, skipping", args.shard.name)
        return 0

    manifest = pq.read_table(args.manifest)
    encoder = load_encoder(args.encoder)
    result = embed_shard(args.shard, manifest, encoder, out_dir=args.out)

    log.info(
        "%s: embedded=%d failed=%d", result.shard, result.embedded, len(result.failed)
    )
    # A shard where EVERY file failed is a failure, not a completed task: it
    # would otherwise leave a valid empty parquet that `shard_is_complete`
    # accepts on a re-run.
    if result.embedded == 0 and result.failed:
        log.error("%s: every file failed", result.shard)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

This imports `load_encoder` from `birdology.phase_a.encoders`, which is **Task 6**
(the real Perch/BirdNET wiring). For this task, create the module with the
stand-in so the array script is runnable end to end:

```python
"""Frozen encoders. Task 6 replaces the stand-in with Perch 2.0 and BirdNET."""
from __future__ import annotations

from collections.abc import Callable

import numpy as np

Encoder = Callable[[np.ndarray, int], np.ndarray]


def _mean_log_mel(samples: np.ndarray, sr: int, n_mels: int = 64) -> np.ndarray:
    """A real, deterministic, checkpoint-free embedding.

    Deliberately NOT a random or constant vector: a stand-in that ignores its
    input would let every downstream test pass on data that carries no signal,
    which is the failure this project keeps finding. This is a genuine (if
    weak) acoustic summary, so a fusion test that improves on it is measuring
    something.
    """
    if samples.size == 0:
        return np.zeros(n_mels, dtype=np.float32)
    spectrum = np.abs(np.fft.rfft(samples.astype(np.float64)))
    edges = np.linspace(0, spectrum.size, n_mels + 1).astype(int)
    bands = [spectrum[a:b].mean() if b > a else 0.0 for a, b in zip(edges, edges[1:])]
    return np.log1p(np.asarray(bands, dtype=np.float64)).astype(np.float32)


_ENCODERS: dict[str, Encoder] = {"melsummary": _mean_log_mel}


def load_encoder(name: str) -> Encoder:
    """Return the named frozen encoder.

    `perch` and `birdnet` raise until Task 6 wires their checkpoints — an
    explicit NotImplementedError rather than a silent fallback to the stand-in,
    because an embedding cache built with the wrong encoder is indistinguishable
    from a correct one.
    """
    if name in ("perch", "birdnet"):
        raise NotImplementedError(
            f"{name} is wired in Task 6; pass --encoder melsummary to run the "
            "pipeline with the checkpoint-free stand-in"
        )
    if name not in _ENCODERS:
        raise ValueError(f"unknown encoder {name!r}; have {sorted(_ENCODERS)}")
    return _ENCODERS[name]
```

- [ ] **Step 5: Write the Slurm array script**

Create `scripts/cluster/phase_a_embed.sbatch`:

```bash
#!/usr/bin/env bash
#SBATCH --job-name=phase-a-embed
#SBATCH --partition=gpu
#SBATCH --nodelist=dgx171
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=/mnt/weka/ML/phase_a/logs/embed_%A_%a.out
#
# One array task per shard, one GPU each. Deliberately 1 GPU per task so a
# flaky node kills one shard rather than the run.
#
#   sbatch --array=0-181 scripts/cluster/phase_a_embed.sbatch XCM 4
#
# THREE things here are silent failures if omitted, all measured:
#   --gres=gpu:1            without it, torch.cuda.is_available() is False
#                           INSIDE the container even with --nv
#   --nv                    without it, the container has no CUDA driver
#   --bind /mnt/weka        without it, /mnt/weka is an EMPTY DIRECTORY in the
#                           container -- the shard loop finds 0 files, exit 0
# WEKA is on dgx171 only today, hence --nodelist. Remove it when the mount is
# fleet-wide, not before.
set -euo pipefail

CONFIG=${1:?usage: sbatch ... phase_a_embed.sbatch CONFIG RESOLUTION}
RESOLUTION=${2:-4}

CORPUS=/mnt/weka/ML/birdset/${CONFIG}/${CONFIG}
OUT=/mnt/weka/ML/phase_a/embeddings/${CONFIG}
MANIFEST=/mnt/weka/ML/phase_a/${CONFIG}_r${RESOLUTION}_manifest.parquet
SIF=/shared/apps/ML/containers/pytorch.sif

# Fail loudly rather than writing to a local directory of the same name.
mountpoint -q /mnt/weka || { echo "FATAL: /mnt/weka is not a mountpoint on $(hostname -s)"; exit 2; }
[ -f "$MANIFEST" ] || { echo "FATAL: no manifest at $MANIFEST"; exit 2; }

mkdir -p "$OUT" "$(dirname "$OUT")/../logs"

mapfile -t SHARDS < <(ls "$CORPUS"/*.tar.gz | sort)
N=${#SHARDS[@]}
[ "$N" -gt 0 ] || { echo "FATAL: no shards under $CORPUS"; exit 2; }

IDX=${SLURM_ARRAY_TASK_ID:-0}
[ "$IDX" -lt "$N" ] || { echo "task $IDX >= $N shards, nothing to do"; exit 0; }
SHARD=${SHARDS[$IDX]}

echo "host=$(hostname -s) task=$IDX/$N shard=$(basename "$SHARD")"

apptainer exec --nv --bind /mnt/weka:/mnt/weka "$SIF" \
    python3 -m birdology.phase_a.embed_cli \
        --shard "$SHARD" --manifest "$MANIFEST" --out "$OUT"
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/phase_a/ -v`
Expected: PASS (all tasks' tests, 46 total)

- [ ] **Step 7: Add the Makefile target**

In `Makefile`, after the `prior` target, add:

```makefile
.PHONY: phase-a-manifest
phase-a-manifest: ## Build the Phase A context manifest (CONFIG=XCM RESOLUTION=4)
	uv run python scripts/phase_a_manifest.py \
		--corpus /mnt/weka/ML/birdset --config $(CONFIG) \
		--resolution $(or $(RESOLUTION),4) \
		--key-file /shared/apps/ML/secrets/xc_key \
		--out /mnt/weka/ML/phase_a
```

- [ ] **Step 8: Document it in the README**

In `README.md`, in the "Scripts" table, add two rows:

```markdown
| `scripts/phase_a_manifest.py` | Build the Phase A context manifest: date every recording (XC API for focal, filename for soundscape), assign its H3 cell and ISO week, and record what was dropped |
| `scripts/cluster/phase_a_embed.sbatch` | Slurm job array: frozen-encoder embeddings for one shard per task, one GPU each |
```

- [ ] **Step 9: Commit**

```bash
git add scripts/phase_a_manifest.py src/birdology/phase_a/embed_cli.py \
        src/birdology/phase_a/encoders.py scripts/cluster/phase_a_embed.sbatch \
        tests/phase_a/test_phase_a_cli.py Makefile README.md
git commit -m "feat(phase_a): manifest driver and the embedding job array

The sbatch script's --gres, --nv and --bind are each a silent failure if
omitted: no gres means CUDA False inside the container, no bind means /mnt/weka
is an empty directory and the shard loop reports zero files at exit 0."
```

---

## Self-Review

**Spec coverage.** Section 5's Phase A ("both frozen encoders over the whole BirdSet corpus, once; cache the embeddings; embarrassingly parallel; shard across the fleet") maps onto Tasks 4 and 5. The spec's staging-to-`/raid` mitigation is deliberately NOT implemented: WEKA measures 41.6 GB/s read against NFS's 0.54, so the mitigation's premise is gone (recorded in the spec's own storage section). **Gap accepted:** the spec names two encoders (Perch 2.0 and BirdNET); this plan's `encoder` is a callable passed in, and wiring the two real encoders is Task 6 — it needs their checkpoints, which are a separate download, and a plan that guessed their preprocessing would be fiction.

**Not in this plan, by design:** Phase B's sweep, and the BirdSet baseline reproduction the spec calls a hard correctness gate. The baseline gate compares our numbers against published ones and belongs with the encoders, not with the indexing.

**Placeholder scan:** clean — every step carries runnable code.

**Type consistency:** `parse_xc_date`/`fetch_species_dates` produce `dict[int, date]`, consumed by `build_manifest`'s `dates=`. `parse_filename_date` raises rather than returning None, and `build_manifest` catches `UndatableFilename` — the one place the two dating paths differ. `MANIFEST_COLUMNS` is produced in Task 3 and read in Task 4's `wanted` lookup and Task 5's CLI. `iso_week` and `WEEKS_PER_YEAR` are imported from `birdology.prior.indexing`, never redefined.

**Review Focus coverage:** (1) `test_rejects_an_IMPLAUSIBLE_date_that_is_well_formed` and `test_a_station_id_that_looks_like_a_date_is_refused_by_the_window`, Task 2. (2) `test_parse_xc_date_rejects_unusable` and `test_a_recording_with_no_date_is_DROPPED_and_COUNTED`, Tasks 1 and 3. (3) `test_a_recording_with_no_coordinates_is_DROPPED_and_COUNTED`, Task 3. (4) `test_ONE_BAD_FILE_does_not_abandon_the_shard` and `test_failures_are_RECORDED_PER_FILE_not_only_logged`, Task 4. (5) `test_shard_is_complete_requires_a_MATCHING_ROW_COUNT`, Task 4.
