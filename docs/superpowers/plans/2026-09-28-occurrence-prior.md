# Spatiotemporal Occurrence Prior — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `P(species | H3 cell, ISO week)` prior tensors from GBIF occurrence data (eBird EOD global + DOF Denmark), with effort correction, hierarchical smoothing, leakage guards, and the four control variants — released CC BY 4.0.

**Architecture:** A new `src/birdology/prior/` package, independent of the RDF graph. Occurrences arrive as a GBIF *download* (not the paged search API — see Global Constraints), are reduced to a `(species_key, cell, week) → count` table, then transformed through a pipeline of pure functions: leakage filtering → target-group background → hierarchical backoff → kernel smoothing → sparse tensor. Each stage is separately testable on small fixtures.

**Tech Stack:** Python 3.11+, `h3` (equal-area hex indexing), `pyarrow` (columnar count tables), `numpy`/`scipy.sparse` (tensors), `requests` (GBIF), `pytest`.

**Spec:** `docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md`

## Global Constraints

- **Do not use the GBIF paged search API for bulk pulls.** DOF alone is 38 143 540 occurrences and GBIF hard-caps offset at ~10 000 per query slice. `src/birdology/ingestion/gbif_dof.py::_fetch_year` exists for the app's incremental graph updates and must not be reused here.
- **The existing `output/birdology.ttl` must never be used to build a prior.** Its month histogram is flat (4166 observations for 11 of 12 months) because `build_graph.py` quotas by (month, year). That is the exact signal the prior encodes.
- GBIF Download API requires a free GBIF account; credentials come from `GBIF_USER` / `GBIF_PASSWORD` / `GBIF_EMAIL` env vars, never hardcoded, never logged.
- DOF dataset key: `95db4db8-f762-11e1-a439-00145eb45e9a`. EOD dataset key: `4fa7b334-ce0d-4e88-aaae-2e0c138d049e`. Aves taxon key: `212`.
- H3 resolutions built: **3, 4, 5**. Default 4. Weeks are ISO weeks **1–52**, circular; ISO week 53 folds into 52.
- Licence of all released prior artifacts: **CC BY 4.0**. Attribution string must cite the GBIF download DOI.
- Tests must not hit the network. Mock `requests` the way `tests/test_gbif_batching.py` does.
- Prior artifacts are written to `/shared/apps/ML/priors/` on the cluster; locally to `output/priors/`. Path comes from a parameter, never hardcoded to the cluster.

## Review Focus

These are input classes the spec implies but no single task's happy path exercises. Each line's test is assigned to the task that owns the code.

1. **A species present in the label set but absent from all occurrence data** (e.g. a Hawaiian endemic under a Denmark prior) must yield a defined, non-zero floor probability — not a divide-by-zero or a `KeyError`. → Task 6.
2. **A cell-week with zero total bird records** (nobody looked) must back off to the coarser level, not produce `0/0`. → Task 6.
3. **ISO week 53** exists in 71 of 400 years and in real GBIF data; it must fold into week 52 rather than creating a 53rd index that breaks the circular kernel. → Task 3.
4. **Antimeridian and polar cells**: H3 handles them, but a naive lat/lon neighbour lookup would not. Neighbour smoothing must use `h3.grid_disk`, and a cell at ±180° longitude must have 6 neighbours like any other. → Task 7.
5. **A test recording whose own occurrence record is in the source data** must be excluded by the leakage guard; a prior built with the guard must differ from one built without it on a fixture where such a record exists. → Task 4.

---

### Task 1: GBIF download client

**Files:**
- Create: `src/birdology/prior/__init__.py`
- Create: `src/birdology/prior/gbif_download.py`
- Test: `tests/prior/test_gbif_download.py`
- Create: `tests/prior/__init__.py`
- Modify: `pyproject.toml` (add `bench` optional-dependency group)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `request_download(dataset_key: str, *, taxon_key: int = 212, year_from: int | None = None, user: str, password: str, email: str) -> str` returns a GBIF download key.
  - `poll_download(download_key: str, *, user: str, password: str, interval_s: int = 60, timeout_s: int = 86400) -> dict` blocks until `SUCCEEDED`, returns the download metadata dict including `doi` and `downloadLink`.
  - `fetch_download(download_key: str, dest: Path, *, user: str, password: str) -> Path` downloads the DwC-A zip, returns its path.
  - `DownloadFailed(Exception)`.

- [ ] **Step 1: Add the dependency group**

In `pyproject.toml`, under `[project.optional-dependencies]`, add:

```toml
bench = [
    "h3>=4.1.0",
    "pyarrow>=17.0.0",
    "numpy>=1.26.0",
    "scipy>=1.11.0",
]
```

- [ ] **Step 2: Write the failing test**

Create `tests/prior/__init__.py` (empty) and `tests/prior/test_gbif_download.py`:

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_gbif_download.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior'`

- [ ] **Step 4: Write the implementation**

Create `src/birdology/prior/__init__.py`:

```python
"""Spatiotemporal occurrence priors, built from GBIF occurrence downloads.

Deliberately independent of the RDF graph in `birdology.graph`: the app's
graph is quota-balanced by (month, year) for browsing, which destroys the
temporal signal a prior encodes. See
docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md
"""
```

Create `src/birdology/prior/gbif_download.py`:

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_gbif_download.py -v`
Expected: PASS (5 tests)

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/birdology/prior tests/prior
git commit -m "feat(prior): GBIF Download API client

The paged search API cannot bulk-extract: GBIF caps offset at ~10 000 and
DOF alone holds 38.1M records. Use the async Download API instead."
```

---

### Task 2: Occurrence table extraction

**Files:**
- Create: `src/birdology/prior/occurrences.py`
- Test: `tests/prior/test_occurrences.py`

**Interfaces:**
- Consumes: the DwC-A zip path from `fetch_download`.
- Produces:
  - `OCCURRENCE_COLUMNS: tuple[str, ...]` — the columns kept.
  - `read_occurrences(zip_path: Path) -> pyarrow.Table` with columns
    `species_key: int64`, `scientific_name: string`, `lat: float64`, `lon: float64`,
    `event_date: date32`, `publisher_key: string`, `recorded_by: string`.
  - Rows lacking coordinates, species key, or event date are dropped.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_occurrences.py`:

```python
"""Occurrence extraction from a GBIF SIMPLE_CSV archive."""
import io
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.occurrences import read_occurrences

HEADER = "\t".join(
    [
        "gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
        "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy",
    ]
)
GOOD = "\t".join(
    [
        "1", "ds", "pub-a", "2492576", "Turdus merula",
        "55.6867", "12.5700", "2025-11-14", "Ole Ratzer",
    ]
)
NO_COORD = "\t".join(
    ["2", "ds", "pub-a", "2492576", "Turdus merula", "", "", "2025-11-14", "X"]
)
NO_SPECIES = "\t".join(
    ["3", "ds", "pub-a", "", "", "55.1", "12.1", "2025-11-14", "X"]
)
NO_DATE = "\t".join(
    ["4", "ds", "pub-a", "2492576", "Turdus merula", "55.1", "12.1", "", "X"]
)


def _archive(tmp_path: Path, rows: list[str]) -> Path:
    zip_path = tmp_path / "occ.zip"
    payload = "\n".join([HEADER, *rows]) + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", payload)
    return zip_path


def test_reads_good_row(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD]))
    assert table.num_rows == 1
    assert table.column("species_key")[0].as_py() == 2492576
    assert table.column("publisher_key")[0].as_py() == "pub-a"
    assert table.column("lat")[0].as_py() == pytest.approx(55.6867)


def test_drops_rows_without_coordinates(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD, NO_COORD]))
    assert table.num_rows == 1


def test_drops_rows_without_species_key(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD, NO_SPECIES]))
    assert table.num_rows == 1


def test_drops_rows_without_event_date(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD, NO_DATE]))
    assert table.num_rows == 1


def test_event_date_parses_to_date(tmp_path):
    table = read_occurrences(_archive(tmp_path, [GOOD]))
    assert table.column("event_date")[0].as_py().isoformat() == "2025-11-14"


def test_event_date_accepts_timestamp_form(tmp_path):
    row = GOOD.replace("2025-11-14", "2025-11-14T07:30:00")
    table = read_occurrences(_archive(tmp_path, [row]))
    assert table.column("event_date")[0].as_py().isoformat() == "2025-11-14"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_occurrences.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior.occurrences'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/prior/occurrences.py`:

```python
"""Reduce a GBIF SIMPLE_CSV download to the columns a prior needs."""
from __future__ import annotations

import csv
import io
import zipfile
from datetime import date
from pathlib import Path

import pyarrow as pa

OCCURRENCE_COLUMNS = (
    "species_key",
    "scientific_name",
    "lat",
    "lon",
    "event_date",
    "publisher_key",
    "recorded_by",
)

_SCHEMA = pa.schema(
    [
        ("species_key", pa.int64()),
        ("scientific_name", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("event_date", pa.date32()),
        ("publisher_key", pa.string()),
        ("recorded_by", pa.string()),
    ]
)


def _parse_date(raw: str) -> date | None:
    """GBIF eventDate is ISO, sometimes with a time part or a range."""
    if not raw:
        return None
    head = raw.split("/", 1)[0].split("T", 1)[0]
    try:
        return date.fromisoformat(head)
    except ValueError:
        return None


def _occurrence_member(zf: zipfile.ZipFile) -> str:
    for name in zf.namelist():
        if name.endswith("occurrence.txt") or name.endswith(".csv"):
            return name
    raise ValueError("no occurrence table found in archive")


def read_occurrences(zip_path: Path) -> pa.Table:
    """Extract the prior-relevant columns, dropping unusable rows."""
    cols: dict[str, list] = {name: [] for name in OCCURRENCE_COLUMNS}

    with zipfile.ZipFile(zip_path) as zf:
        member = _occurrence_member(zf)
        with zf.open(member) as raw:
            reader = csv.DictReader(
                io.TextIOWrapper(raw, encoding="utf-8"),
                delimiter="\t",
                quoting=csv.QUOTE_NONE,
            )
            for row in reader:
                species_raw = (row.get("speciesKey") or "").strip()
                lat_raw = (row.get("decimalLatitude") or "").strip()
                lon_raw = (row.get("decimalLongitude") or "").strip()
                event = _parse_date((row.get("eventDate") or "").strip())
                if not species_raw or not lat_raw or not lon_raw or event is None:
                    continue
                try:
                    species_key = int(species_raw)
                    lat = float(lat_raw)
                    lon = float(lon_raw)
                except ValueError:
                    continue
                cols["species_key"].append(species_key)
                cols["scientific_name"].append((row.get("species") or "").strip())
                cols["lat"].append(lat)
                cols["lon"].append(lon)
                cols["event_date"].append(event)
                cols["publisher_key"].append(
                    (row.get("publishingOrgKey") or "").strip()
                )
                cols["recorded_by"].append((row.get("recordedBy") or "").strip())

    return pa.table([cols[name] for name in OCCURRENCE_COLUMNS], schema=_SCHEMA)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_occurrences.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/prior/occurrences.py tests/prior/test_occurrences.py
git commit -m "feat(prior): extract occurrence columns from a GBIF archive"
```

---

### Task 3: H3 cell and ISO week assignment

**Files:**
- Create: `src/birdology/prior/indexing.py`
- Test: `tests/prior/test_indexing.py`

**Interfaces:**
- Consumes: the `pyarrow.Table` from `read_occurrences`.
- Produces:
  - `WEEKS_PER_YEAR = 52`
  - `iso_week(d: date) -> int` — 1..52, folding ISO week 53 into 52.
  - `assign_cells(table: pa.Table, resolution: int) -> pa.Table` — adds `cell: string` (H3 index) and `week: int8`.
  - `CountTable` = `pa.Table` with columns `species_key: int64`, `cell: string`, `week: int8`, `n: int64`.
  - `aggregate_counts(table: pa.Table) -> CountTable`.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_indexing.py`:

```python
"""H3 cell + ISO week assignment."""
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.indexing import (
    WEEKS_PER_YEAR,
    aggregate_counts,
    assign_cells,
    iso_week,
)


def _table(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "scientific_name": pa.array([r[1] for r in rows], pa.string()),
            "lat": pa.array([r[2] for r in rows], pa.float64()),
            "lon": pa.array([r[3] for r in rows], pa.float64()),
            "event_date": pa.array([r[4] for r in rows], pa.date32()),
            "publisher_key": pa.array([r[5] for r in rows], pa.string()),
            "recorded_by": pa.array([r[6] for r in rows], pa.string()),
        }
    )


def test_iso_week_ordinary_date():
    assert iso_week(date(2025, 11, 14)) == 46


def test_iso_week_53_folds_into_52():
    # 2020 has an ISO week 53.
    assert date(2020, 12, 31).isocalendar().week == 53
    assert iso_week(date(2020, 12, 31)) == WEEKS_PER_YEAR


def test_iso_week_always_in_range():
    for year in (2015, 2016, 2020, 2021, 2026):
        for month, day in ((1, 1), (6, 15), (12, 28), (12, 31)):
            assert 1 <= iso_week(date(year, month, day)) <= WEEKS_PER_YEAR


def test_assign_cells_adds_cell_and_week():
    t = _table([(1, "Turdus merula", 55.6867, 12.5700, date(2025, 11, 14), "p", "o")])
    out = assign_cells(t, resolution=4)
    assert out.column("week")[0].as_py() == 46
    assert isinstance(out.column("cell")[0].as_py(), str)


def test_same_location_same_cell():
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2025, 1, 5), "p", "o"),
            (1, "A", 55.6868, 12.5701, date(2025, 1, 5), "p", "o"),
        ]
    )
    out = assign_cells(t, resolution=4)
    assert out.column("cell")[0].as_py() == out.column("cell")[1].as_py()


def test_finer_resolution_splits_cells():
    t = _table(
        [
            (1, "A", 55.6000, 12.5000, date(2025, 1, 5), "p", "o"),
            (1, "A", 55.7000, 12.6000, date(2025, 1, 5), "p", "o"),
        ]
    )
    coarse = assign_cells(t, resolution=3)
    fine = assign_cells(t, resolution=7)
    assert fine.column("cell")[0].as_py() != fine.column("cell")[1].as_py()
    assert len(set(coarse.column("cell").to_pylist())) <= len(
        set(fine.column("cell").to_pylist())
    )


def test_aggregate_counts_sums_duplicates():
    t = _table(
        [
            (1, "A", 55.6867, 12.5700, date(2025, 11, 14), "p", "o"),
            (1, "A", 55.6867, 12.5700, date(2025, 11, 14), "p", "o"),
            (2, "B", 55.6867, 12.5700, date(2025, 11, 14), "p", "o"),
        ]
    )
    counts = aggregate_counts(assign_cells(t, resolution=4))
    by_species = {
        row["species_key"]: row["n"] for row in counts.to_pylist()
    }
    assert by_species[1] == 2
    assert by_species[2] == 1


def test_aggregate_counts_schema():
    t = _table([(1, "A", 55.6, 12.5, date(2025, 3, 3), "p", "o")])
    counts = aggregate_counts(assign_cells(t, resolution=4))
    assert counts.schema.names == ["species_key", "cell", "week", "n"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_indexing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior.indexing'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/prior/indexing.py`:

```python
"""Map occurrences onto an (H3 cell, ISO week) grid and aggregate to counts.

H3 is used rather than a lat/lon grid because the data spans 0-60+ degrees of
latitude, where lat/lon cells differ in area by a factor of two. Hexagons are
equal-area and have six equidistant neighbours, which the smoothing kernel in
`smoothing.py` relies on.
"""
from __future__ import annotations

from datetime import date

import h3
import pyarrow as pa
import pyarrow.compute as pc

WEEKS_PER_YEAR = 52


def iso_week(d: date) -> int:
    """ISO week number, with week 53 folded into 52.

    71 of every 400 years have an ISO week 53. Keeping it would create a 53rd
    index that the circular smoothing kernel would treat as a real week with a
    seventh of the usual data behind it.
    """
    week = d.isocalendar().week
    return WEEKS_PER_YEAR if week > WEEKS_PER_YEAR else week


def assign_cells(table: pa.Table, resolution: int) -> pa.Table:
    """Add `cell` (H3 index at *resolution*) and `week` columns."""
    lats = table.column("lat").to_pylist()
    lons = table.column("lon").to_pylist()
    dates = table.column("event_date").to_pylist()

    cells = [h3.latlng_to_cell(lat, lon, resolution) for lat, lon in zip(lats, lons)]
    weeks = [iso_week(d) for d in dates]

    return table.append_column(
        "cell", pa.array(cells, pa.string())
    ).append_column("week", pa.array(weeks, pa.int8()))


def aggregate_counts(table: pa.Table) -> pa.Table:
    """Reduce to (species_key, cell, week) -> n."""
    grouped = (
        table.select(["species_key", "cell", "week"])
        .append_column("n", pa.array([1] * table.num_rows, pa.int64()))
        .group_by(["species_key", "cell", "week"])
        .aggregate([("n", "sum")])
    )
    return grouped.rename_columns(["species_key", "cell", "week", "n"]).select(
        ["species_key", "cell", "week", "n"]
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_indexing.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/prior/indexing.py tests/prior/test_indexing.py
git commit -m "feat(prior): H3 cell and ISO week indexing

Week 53 folds into 52 so the circular kernel never sees a short week."
```

---

### Task 4: Leakage guards

**Files:**
- Create: `src/birdology/prior/leakage.py`
- Test: `tests/prior/test_leakage.py`

**Interfaces:**
- Consumes: the celled `pa.Table` from `assign_cells`.

> **CORRECTED 2026-09-29 — normalise EVERY key component on BOTH sides.**
> An earlier draft of this plan required both-sides normalisation only for the
> contributor name. That was too narrow. A leakage-guard key is a tuple, and any
> un-normalised component silently disables the whole guard. Verified on a real res-4
> table where the guard should have removed the row: an UPPERCASED cell kept it, a
> whitespace-padded cell kept it, and a res-5 cell kept it — no exception, no log line,
> leak survives. `h3.get_resolution` accepts an uppercased cell, so validity checks do
> not catch it.
> Rules for any guard in this plan:
>   1. Normalise every string key component on both sides (contributor AND cell).
>   2. Raise on a cross-resolution cell mismatch — never warn. It is never intentional
>      and the consequence is total silent leakage. The guard function holds both the
>      table and the recordings, so it is the only place the invariant can be checked.
>   3. WARN when a non-empty banned set removes ZERO rows. That one line is the shared
>      observable symptom of every silent under-match mode.
- Produces:
  - `XENO_CANTO_PUBLISHER_KEYS: frozenset[str]`
  - `TestRecording` dataclass: `cell: str`, `week: int`, `recorded_by: str`.
  - `drop_xeno_canto(table: pa.Table) -> pa.Table`
  - `drop_test_contributor_cells(table: pa.Table, test_recordings: Iterable[TestRecording]) -> pa.Table`
  - `apply_leakage_guards(table, test_recordings) -> pa.Table` — both, in order.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_leakage.py`:

```python
"""Leakage guards: the prior must not contain the answers."""
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.indexing import assign_cells
from birdology.prior.leakage import (
    XENO_CANTO_PUBLISHER_KEYS,
    TestRecording,
    apply_leakage_guards,
    drop_test_contributor_cells,
    drop_xeno_canto,
)

XC = next(iter(XENO_CANTO_PUBLISHER_KEYS))


def _table(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "scientific_name": pa.array(["A"] * len(rows), pa.string()),
            "lat": pa.array([r[1] for r in rows], pa.float64()),
            "lon": pa.array([r[2] for r in rows], pa.float64()),
            "event_date": pa.array([r[3] for r in rows], pa.date32()),
            "publisher_key": pa.array([r[4] for r in rows], pa.string()),
            "recorded_by": pa.array([r[5] for r in rows], pa.string()),
        }
    )


def test_drops_xeno_canto_publisher():
    t = _table(
        [
            (1, 55.6, 12.5, date(2025, 3, 3), XC, "obs"),
            (1, 55.6, 12.5, date(2025, 3, 3), "other-pub", "obs"),
        ]
    )
    out = drop_xeno_canto(t)
    assert out.num_rows == 1
    assert out.column("publisher_key")[0].as_py() == "other-pub"


def test_drops_same_contributor_cell_week():
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole"),
                (1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Jens"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert out.num_rows == 1
    assert out.column("recorded_by")[0].as_py() == "Jens"


def test_keeps_same_contributor_different_week():
    t = assign_cells(
        _table([(1, 55.6867, 12.5700, date(2025, 11, 14), "pub", "Ole")]),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    out = drop_test_contributor_cells(
        t, [TestRecording(cell=cell, week=2, recorded_by="Ole")]
    )
    assert out.num_rows == 1


def test_guarded_table_differs_from_unguarded():
    """Review Focus 5: a record that would leak must actually be removed."""
    t = assign_cells(
        _table(
            [
                (1, 55.6867, 12.5700, date(2025, 11, 14), XC, "Ole"),
                (2, 56.0000, 10.0000, date(2025, 6, 1), "pub", "Jens"),
            ]
        ),
        resolution=4,
    )
    cell = t.column("cell")[0].as_py()
    guarded = apply_leakage_guards(
        t, [TestRecording(cell=cell, week=46, recorded_by="Ole")]
    )
    assert guarded.num_rows < t.num_rows
    assert XC not in guarded.column("publisher_key").to_pylist()


def test_no_test_recordings_is_a_noop_beyond_xc():
    t = assign_cells(
        _table([(1, 55.6, 12.5, date(2025, 3, 3), "pub", "Ole")]), resolution=4
    )
    assert apply_leakage_guards(t, []).num_rows == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_leakage.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior.leakage'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/prior/leakage.py`:

```python
"""Keep the answers out of the prior.

Xeno-Canto publishes its recordings to GBIF as occurrences. If a test
recording's own occurrence record reaches the prior, the prior trivially
"knows" the species, and every reported gain is leakage. Two guards:

1. Drop every occurrence published by Xeno-Canto.
2. Drop every occurrence sharing a (cell, week) with a test recording from the
   same contributor — the same person's other records from the same outing.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

# Xeno-canto Foundation for Nature Sounds, publishing organisation on GBIF.
# CORRECTED 2026-09-29: this plan originally named e2e717bf-551a-4917-bdc9-4fa0f342c530,
# which api.gbif.org/v1/organization resolves to the CORNELL LAB OF ORNITHOLOGY (the eBird
# publisher). Using it would have leaked every Xeno-Canto occurrence into the prior AND
# deleted the entire eBird population — both failure modes at once. Verified against the
# live API: the key below is "Xeno-canto Foundation for Nature Sounds".
XENO_CANTO_PUBLISHER_KEYS = frozenset(
    {"1f00d75c-f6fc-4224-a595-975e82d7689c"}
)


@dataclass(frozen=True)
class TestRecording:
    """A recording in an evaluation split, as (where, when, who)."""

    cell: str
    week: int
    recorded_by: str


def drop_xeno_canto(table: pa.Table) -> pa.Table:
    """Remove occurrences published by Xeno-Canto."""
    keys = pa.array(sorted(XENO_CANTO_PUBLISHER_KEYS), pa.string())
    mask = pc.invert(pc.is_in(table.column("publisher_key"), value_set=keys))
    return table.filter(mask)


def drop_test_contributor_cells(
    table: pa.Table, test_recordings: Iterable[TestRecording]
) -> pa.Table:
    """Remove rows matching a test recording's (cell, week, contributor)."""
    banned = {
        (r.cell, int(r.week), r.recorded_by) for r in test_recordings
    }
    if not banned:
        return table

    cells = table.column("cell").to_pylist()
    weeks = table.column("week").to_pylist()
    who = table.column("recorded_by").to_pylist()
    keep = [
        (c, int(w), o) not in banned for c, w, o in zip(cells, weeks, who)
    ]
    return table.filter(pa.array(keep, pa.bool_()))


def apply_leakage_guards(
    table: pa.Table, test_recordings: Iterable[TestRecording]
) -> pa.Table:
    """Both guards, in order."""
    return drop_test_contributor_cells(drop_xeno_canto(table), test_recordings)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_leakage.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Verify the Xeno-Canto publisher key against GBIF**

This is the one constant that cannot be unit-tested offline. Run once, by hand,
and paste the result into the module docstring as a dated comment:

```bash
curl -s "https://api.gbif.org/v1/dataset/search?q=xeno-canto&limit=5" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); [print(r['publishingOrganizationKey'], r['title']) for r in d['results']]"
```

Expected: `1f00d75c-f6fc-4224-a595-975e82d7689c` for all five Xeno-canto datasets.
If the key differs, update `XENO_CANTO_PUBLISHER_KEYS` and re-run the tests.

DO NOT use `e2e717bf-551a-4917-bdc9-4fa0f342c530` — an earlier draft of this plan named it
as Xeno-Canto, but it is the Cornell Lab of Ornithology (eBird). `tests/prior/test_leakage.py`
asserts that key is ABSENT from `XENO_CANTO_PUBLISHER_KEYS`; do not weaken that test.

- [ ] **Step 6: Commit**

```bash
git add src/birdology/prior/leakage.py tests/prior/test_leakage.py
git commit -m "feat(prior): leakage guards against Xeno-Canto occurrences

Xeno-Canto publishes to GBIF; without these guards the prior contains the
test recordings' own occurrence records."
```

---

### Task 5: Target-group background effort correction

**Files:**
- Create: `src/birdology/prior/effort.py`
- Test: `tests/prior/test_effort.py`

**Interfaces:**
- Consumes: `CountTable` from `aggregate_counts`.
- Produces:
  - `RatioTable` = `pa.Table` with `species_key: int64`, `cell: string`, `week: int8`, `n: int64`, `total: int64`, `ratio: float64`.
  - `target_group_ratios(counts: pa.Table) -> pa.Table`.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_effort.py`:

```python
"""Target-group background: correct for observer effort, not bird abundance."""
import sys
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.effort import target_group_ratios


def _counts(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "n": pa.array([r[3] for r in rows], pa.int64()),
        }
    )


def test_ratio_is_share_of_cell_week_total():
    t = _counts([(1, "c", 5, 3), (2, "c", 5, 1)])
    out = {r["species_key"]: r for r in target_group_ratios(t).to_pylist()}
    assert out[1]["total"] == 4
    assert out[1]["ratio"] == pytest.approx(0.75)
    assert out[2]["ratio"] == pytest.approx(0.25)


def test_ratios_in_a_cell_week_sum_to_one():
    t = _counts([(1, "c", 5, 3), (2, "c", 5, 1), (3, "c", 5, 4)])
    total = sum(r["ratio"] for r in target_group_ratios(t).to_pylist())
    assert total == pytest.approx(1.0)


def test_effort_cancels_between_busy_and_quiet_cells():
    """A city cell with 100x the records gives the same ratio as a rural one."""
    t = _counts(
        [
            (1, "city", 5, 750), (2, "city", 5, 250),
            (1, "rural", 5, 3), (2, "rural", 5, 1),
        ]
    )
    rows = {(r["species_key"], r["cell"]): r["ratio"] for r in target_group_ratios(t).to_pylist()}
    assert rows[(1, "city")] == pytest.approx(rows[(1, "rural")])


def test_cells_are_independent():
    t = _counts([(1, "a", 5, 1), (2, "b", 5, 1)])
    for row in target_group_ratios(t).to_pylist():
        assert row["ratio"] == pytest.approx(1.0)


def test_weeks_are_independent():
    t = _counts([(1, "c", 5, 1), (2, "c", 40, 1)])
    for row in target_group_ratios(t).to_pylist():
        assert row["ratio"] == pytest.approx(1.0)


def test_empty_table_returns_empty():
    assert target_group_ratios(_counts([])).num_rows == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_effort.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior.effort'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/prior/effort.py`:

```python
"""Target-group background correction.

Raw occurrence counts measure observers, not birds: Copenhagen looks
species-rich and west Jutland looks empty, and both are artifacts of where
birders live. Using each species' *share* of all bird records in the same
cell-week puts observer effort in both numerator and denominator, where it
cancels to first order.

Limitation, stated in the paper: this corrects for effort density, not
observer preference. Rarities are reported disproportionately.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def target_group_ratios(counts: pa.Table) -> pa.Table:
    """Add per-(cell, week) totals and each species' share of them."""
    if counts.num_rows == 0:
        return counts.append_column(
            "total", pa.array([], pa.int64())
        ).append_column("ratio", pa.array([], pa.float64()))

    totals = (
        counts.select(["cell", "week", "n"])
        .group_by(["cell", "week"])
        .aggregate([("n", "sum")])
        .rename_columns(["cell", "week", "total"])
    )
    joined = counts.join(totals, keys=["cell", "week"], join_type="inner")
    ratio = pc.divide(
        pc.cast(joined.column("n"), pa.float64()),
        pc.cast(joined.column("total"), pa.float64()),
    )
    return joined.append_column("ratio", ratio).select(
        ["species_key", "cell", "week", "n", "total", "ratio"]
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_effort.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/prior/effort.py tests/prior/test_effort.py
git commit -m "feat(prior): target-group background effort correction"
```

---

### Task 6: Hierarchical backoff

**Files:**
- Create: `src/birdology/prior/backoff.py`
- Test: `tests/prior/test_backoff.py`

**Interfaces:**
- Consumes: `RatioTable` from `target_group_ratios`.
- Produces:
  - `FLOOR: float = 1e-6`
  - `BackoffParams` dataclass: `cell_week_weight: float = 10.0`, `cell_weight: float = 50.0`, `floor: float = FLOOR`.
  - `hierarchical_estimate(ratios: pa.Table, species_keys: Sequence[int], params: BackoffParams = BackoffParams()) -> pa.Table` with `species_key`, `cell`, `week`, `p`.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_backoff.py`:

```python
"""Three-level hierarchical backoff for a very sparse grid."""
import sys
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.backoff import (
    FLOOR,
    BackoffParams,
    hierarchical_estimate,
)
from birdology.prior.effort import target_group_ratios


def _counts(rows):
    return target_group_ratios(
        pa.table(
            {
                "species_key": pa.array([r[0] for r in rows], pa.int64()),
                "cell": pa.array([r[1] for r in rows], pa.string()),
                "week": pa.array([r[2] for r in rows], pa.int8()),
                "n": pa.array([r[3] for r in rows], pa.int64()),
            }
        )
    )


def _lookup(table):
    return {(r["species_key"], r["cell"], r["week"]): r["p"] for r in table.to_pylist()}


def test_dense_cell_week_dominated_by_local_estimate():
    """With 1000 local records, the local ratio should win."""
    rows = [(1, "c", 5, 1000), (2, "c", 5, 0)]
    rows += [(2, "other", 5, 1000)]
    out = _lookup(hierarchical_estimate(_counts(rows), [1, 2]))
    assert out[(1, "c", 5)] > 0.9


def test_sparse_cell_week_shrinks_toward_coarser_level():
    """One local record must not produce a probability of 1.0."""
    rows = [(1, "c", 5, 1), (2, "c", 40, 100), (1, "c", 40, 1)]
    out = _lookup(hierarchical_estimate(_counts(rows), [1, 2]))
    assert out[(1, "c", 5)] < 1.0


def test_species_absent_everywhere_gets_floor_not_zero():
    """Review Focus 1: a label-set species with no occurrence data."""
    out = _lookup(hierarchical_estimate(_counts([(1, "c", 5, 10)]), [1, 999]))
    assert out[(999, "c", 5)] >= FLOOR
    assert out[(999, "c", 5)] > 0.0


def test_species_absent_everywhere_does_not_raise():
    hierarchical_estimate(_counts([(1, "c", 5, 10)]), [1, 999, 1000])


def test_empty_cell_week_backs_off_without_dividing_by_zero():
    """Review Focus 2: a (cell, week) nobody visited."""
    ratios = _counts([(1, "a", 5, 10)])
    out = hierarchical_estimate(ratios, [1], cells=["a", "empty"], weeks=[5])
    values = _lookup(out)
    assert values[(1, "empty", 5)] >= FLOOR
    assert all(v == v for v in values.values())  # no NaN


def test_probabilities_are_bounded():
    rows = [(1, "c", 5, 7), (2, "c", 5, 3)]
    for p in _lookup(hierarchical_estimate(_counts(rows), [1, 2])).values():
        assert 0.0 <= p <= 1.0


def test_stronger_shrinkage_pulls_toward_global():
    rows = [(1, "c", 5, 1), (2, "c", 40, 100)]
    weak = _lookup(
        hierarchical_estimate(_counts(rows), [1, 2], params=BackoffParams(cell_week_weight=0.1))
    )
    strong = _lookup(
        hierarchical_estimate(_counts(rows), [1, 2], params=BackoffParams(cell_week_weight=1000.0))
    )
    assert strong[(1, "c", 5)] < weak[(1, "c", 5)]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_backoff.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior.backoff'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/prior/backoff.py`:

```python
"""Three-level hierarchical backoff.

Most (cell, week, species) combinations are empty, and empty almost always
means "nobody looked" rather than "absent". A raw ratio would therefore report
0.0 for unobserved-but-present species and 1.0 for a cell with one record.

Shrink each estimate toward the next coarser level, with weight set by how much
data stands behind it:

    cell-week  ->  cell's annual profile  ->  species' global profile

A species with no occurrences anywhere still gets `floor`, never zero: the
label set can legitimately contain species the occurrence data misses.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

FLOOR = 1e-6


@dataclass(frozen=True)
class BackoffParams:
    """Shrinkage strengths. Higher = pull harder toward the coarser level."""

    cell_week_weight: float = 10.0
    cell_weight: float = 50.0
    floor: float = FLOOR


def _shrink(local_num: float, local_den: float, coarse_p: float, weight: float) -> float:
    """Posterior mean with a `weight`-strength pseudo-count at `coarse_p`."""
    return (local_num + weight * coarse_p) / (local_den + weight)


def hierarchical_estimate(
    ratios: pa.Table,
    species_keys: Sequence[int],
    params: BackoffParams = BackoffParams(),
    *,
    cells: Sequence[str] | None = None,
    weeks: Sequence[int] | None = None,
) -> pa.Table:
    """Estimate P(species | cell, week) over the full requested grid."""
    rows = ratios.to_pylist()

    cell_week_n: dict[tuple[str, int], float] = {}
    cell_week_species: dict[tuple[int, str, int], float] = {}
    cell_n: dict[str, float] = {}
    cell_species: dict[tuple[int, str], float] = {}
    global_n = 0.0
    global_species: dict[int, float] = {}

    for row in rows:
        sk, cell, week, n = row["species_key"], row["cell"], int(row["week"]), row["n"]
        cell_week_species[(sk, cell, week)] = cell_week_species.get((sk, cell, week), 0.0) + n
        cell_week_n[(cell, week)] = cell_week_n.get((cell, week), 0.0) + n
        cell_species[(sk, cell)] = cell_species.get((sk, cell), 0.0) + n
        cell_n[cell] = cell_n.get(cell, 0.0) + n
        global_species[sk] = global_species.get(sk, 0.0) + n
        global_n += n

    all_cells = list(cells) if cells is not None else sorted(cell_n)
    all_weeks = list(weeks) if weeks is not None else sorted({w for _, w in cell_week_n})
    if not all_weeks:
        all_weeks = [1]

    out_species: list[int] = []
    out_cells: list[str] = []
    out_weeks: list[int] = []
    out_p: list[float] = []

    for sk in species_keys:
        p_global = (global_species.get(sk, 0.0) / global_n) if global_n else 0.0
        p_global = max(p_global, params.floor)
        for cell in all_cells:
            p_cell = _shrink(
                cell_species.get((sk, cell), 0.0),
                cell_n.get(cell, 0.0),
                p_global,
                params.cell_weight,
            )
            for week in all_weeks:
                p = _shrink(
                    cell_week_species.get((sk, cell, int(week)), 0.0),
                    cell_week_n.get((cell, int(week)), 0.0),
                    p_cell,
                    params.cell_week_weight,
                )
                out_species.append(sk)
                out_cells.append(cell)
                out_weeks.append(int(week))
                out_p.append(min(max(p, params.floor), 1.0))

    return pa.table(
        {
            "species_key": pa.array(out_species, pa.int64()),
            "cell": pa.array(out_cells, pa.string()),
            "week": pa.array(out_weeks, pa.int8()),
            "p": pa.array(out_p, pa.float64()),
        }
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_backoff.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/prior/backoff.py tests/prior/test_backoff.py
git commit -m "feat(prior): hierarchical backoff with a non-zero floor

Empty means 'nobody looked', not 'absent'. A species absent from all
occurrence data still gets a floor probability rather than zero."
```

---

### Task 7: Spatial and temporal smoothing

**Files:**
- Create: `src/birdology/prior/smoothing.py`
- Test: `tests/prior/test_smoothing.py`

**Interfaces:**
- Consumes: the estimate table from `hierarchical_estimate`.
- Produces:
  - `SmoothingParams` dataclass: `space_weight: float = 0.3`, `time_weight: float = 0.3`.
  - `smooth(estimates: pa.Table, params: SmoothingParams = SmoothingParams()) -> pa.Table` — same schema, smoothed `p`.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_smoothing.py`:

```python
"""Neighbour smoothing over H3 rings and adjacent weeks."""
import sys
from pathlib import Path

import h3
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.smoothing import SmoothingParams, smooth


def _est(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "p": pa.array([r[3] for r in rows], pa.float64()),
        }
    )


def _lookup(t):
    return {(r["species_key"], r["cell"], r["week"]): r["p"] for r in t.to_pylist()}


CPH = h3.latlng_to_cell(55.6867, 12.5700, 4)
NEIGHBOUR = [c for c in h3.grid_disk(CPH, 1) if c != CPH][0]


def test_neighbouring_cell_pulls_value_up():
    t = _est([(1, CPH, 20, 1.0), (1, NEIGHBOUR, 20, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, NEIGHBOUR, 20)] > 0.0


def test_adjacent_week_pulls_value_up():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 21)] > 0.0


def test_week_52_and_week_1_are_adjacent():
    """The year is circular: late December informs early January."""
    t = _est([(1, CPH, 52, 1.0), (1, CPH, 1, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 1)] > 0.0


def test_distant_week_is_not_pulled():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 40, 0.0)])
    out = _lookup(smooth(t))
    assert out[(1, CPH, 40)] == pytest.approx(0.0, abs=1e-9)


def test_antimeridian_cell_has_six_neighbours():
    """Review Focus 4: H3 handles the seam; a lat/lon grid would not."""
    seam = h3.latlng_to_cell(0.0, 179.9, 4)
    assert len([c for c in h3.grid_disk(seam, 1) if c != seam]) == 6


def test_antimeridian_cell_smooths_without_error():
    seam = h3.latlng_to_cell(0.0, 179.9, 4)
    other = [c for c in h3.grid_disk(seam, 1) if c != seam][0]
    out = _lookup(smooth(_est([(1, seam, 20, 1.0), (1, other, 20, 0.0)])))
    assert out[(1, other, 20)] > 0.0


def test_zero_weights_are_a_noop():
    t = _est([(1, CPH, 20, 1.0), (1, CPH, 21, 0.0)])
    out = _lookup(smooth(t, SmoothingParams(space_weight=0.0, time_weight=0.0)))
    assert out[(1, CPH, 20)] == pytest.approx(1.0)
    assert out[(1, CPH, 21)] == pytest.approx(0.0)


def test_schema_is_preserved():
    t = _est([(1, CPH, 20, 0.5)])
    assert smooth(t).schema.names == ["species_key", "cell", "week", "p"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_smoothing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior.smoothing'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/prior/smoothing.py`:

```python
"""Smooth the prior across neighbouring hexes and adjacent weeks.

Neighbours come from `h3.grid_disk`, never from lat/lon arithmetic: a hex at
the antimeridian or near a pole has six neighbours like any other, which a
naive +/- degree lookup would get wrong.

Weeks are circular — week 52 is adjacent to week 1 — because a migration
that peaks in late December does not restart in January.
"""
from __future__ import annotations

from dataclasses import dataclass

import h3
import pyarrow as pa

from .indexing import WEEKS_PER_YEAR


@dataclass(frozen=True)
class SmoothingParams:
    """Neighbour weights, relative to the cell's own value of 1.0."""

    space_weight: float = 0.3
    time_weight: float = 0.3


def _adjacent_weeks(week: int) -> tuple[int, int]:
    prev = week - 1 or WEEKS_PER_YEAR
    nxt = week + 1 if week < WEEKS_PER_YEAR else 1
    return prev, nxt


def smooth(
    estimates: pa.Table, params: SmoothingParams = SmoothingParams()
) -> pa.Table:
    """Weighted average of each point with its space and time neighbours."""
    rows = estimates.to_pylist()
    values = {
        (r["species_key"], r["cell"], int(r["week"])): r["p"] for r in rows
    }
    neighbours: dict[str, list[str]] = {}

    out_p: list[float] = []
    for row in rows:
        sk, cell, week = row["species_key"], row["cell"], int(row["week"])
        total = row["p"]
        weight = 1.0

        if params.space_weight:
            if cell not in neighbours:
                neighbours[cell] = [c for c in h3.grid_disk(cell, 1) if c != cell]
            for other in neighbours[cell]:
                if (sk, other, week) in values:
                    total += params.space_weight * values[(sk, other, week)]
                    weight += params.space_weight

        if params.time_weight:
            for other_week in _adjacent_weeks(week):
                if (sk, cell, other_week) in values:
                    total += params.time_weight * values[(sk, cell, other_week)]
                    weight += params.time_weight

        out_p.append(total / weight)

    return pa.table(
        {
            "species_key": estimates.column("species_key"),
            "cell": estimates.column("cell"),
            "week": estimates.column("week"),
            "p": pa.array(out_p, pa.float64()),
        }
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_smoothing.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/prior/smoothing.py tests/prior/test_smoothing.py
git commit -m "feat(prior): H3-ring and circular-week smoothing

Neighbours come from h3.grid_disk so antimeridian and polar cells behave;
week 52 is adjacent to week 1."
```

---

### Task 8: Prior tensor serialisation

**Files:**
- Create: `src/birdology/prior/tensor.py`
- Test: `tests/prior/test_tensor.py`

**Interfaces:**
- Consumes: the smoothed table from `smooth`.
- Produces:
  - `PriorSpec` dataclass: `resolution: int`, `source: str`, `variant: str`.
  - `PriorTensor` dataclass: `spec`, `species_keys: np.ndarray`, `cells: list[str]`, `values: scipy.sparse.csr_matrix` (rows = species, cols = cell*52 + week-1), plus `lookup(species_key, cell, week) -> float`.
  - `to_tensor(table: pa.Table, spec: PriorSpec) -> PriorTensor`
  - `save_prior(t: PriorTensor, path: Path) -> Path` / `load_prior(path: Path) -> PriorTensor` — `.npz` plus a sidecar JSON carrying spec, GBIF DOI, licence.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_tensor.py`:

```python
"""Sparse prior tensor, round-tripped through disk."""
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.tensor import PriorSpec, load_prior, save_prior, to_tensor

SPEC = PriorSpec(resolution=4, source="dof", variant="full")


def _table(rows):
    return pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "p": pa.array([r[3] for r in rows], pa.float64()),
        }
    )


def test_lookup_returns_stored_value():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.lookup(1, "cell-a", 46) == pytest.approx(0.25)


def test_lookup_unknown_species_returns_zero():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.lookup(999, "cell-a", 46) == 0.0


def test_lookup_unknown_cell_returns_zero():
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    assert t.lookup(1, "cell-zz", 46) == 0.0


def test_round_trip_preserves_values(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25), (2, "cell-b", 3, 0.5)]), SPEC)
    path = save_prior(t, tmp_path / "p.npz")
    back = load_prior(path)
    assert back.lookup(1, "cell-a", 46) == pytest.approx(0.25)
    assert back.lookup(2, "cell-b", 3) == pytest.approx(0.5)


def test_round_trip_preserves_spec(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    back = load_prior(save_prior(t, tmp_path / "p.npz"))
    assert back.spec == SPEC


def test_sidecar_records_licence_and_doi(tmp_path):
    t = to_tensor(_table([(1, "cell-a", 46, 0.25)]), SPEC)
    t.gbif_doi = "10.15468/dl.abc123"
    path = save_prior(t, tmp_path / "p.npz")
    meta = json.loads(path.with_suffix(".json").read_text())
    assert meta["licence"] == "CC BY 4.0"
    assert meta["gbif_doi"] == "10.15468/dl.abc123"


def test_vector_returns_all_species_for_a_context():
    t = to_tensor(
        _table([(1, "cell-a", 46, 0.25), (2, "cell-a", 46, 0.75)]), SPEC
    )
    vec = t.vector("cell-a", 46)
    assert vec.shape == (2,)
    assert vec.sum() == pytest.approx(1.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_tensor.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior.tensor'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/prior/tensor.py`:

```python
"""Sparse (species x cell x week) prior tensor and its on-disk format.

Stored as a CSR matrix with rows = species and columns = cell * 52 + (week - 1),
plus a sidecar JSON carrying the spec, the GBIF download DOI and the licence.
The sidecar is what makes the release citable.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
from scipy import sparse

from .indexing import WEEKS_PER_YEAR

LICENCE = "CC BY 4.0"


@dataclass(frozen=True)
class PriorSpec:
    """Identifies one prior variant."""

    resolution: int
    source: str   # "eod" | "dof" | "eod+dof"
    variant: str  # "full" | "uniform" | "marginal" | "shuffle_space" | "shuffle_time"


@dataclass
class PriorTensor:
    spec: PriorSpec
    species_keys: np.ndarray
    cells: list[str]
    values: sparse.csr_matrix
    gbif_doi: str = ""
    _species_index: dict[int, int] = field(default_factory=dict, repr=False)
    _cell_index: dict[str, int] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not self._species_index:
            self._species_index = {int(k): i for i, k in enumerate(self.species_keys)}
        if not self._cell_index:
            self._cell_index = {c: i for i, c in enumerate(self.cells)}

    def _column(self, cell: str, week: int) -> int | None:
        ci = self._cell_index.get(cell)
        if ci is None:
            return None
        return ci * WEEKS_PER_YEAR + (int(week) - 1)

    def lookup(self, species_key: int, cell: str, week: int) -> float:
        """P(species | cell, week); 0.0 for unknown species or cell."""
        row = self._species_index.get(int(species_key))
        col = self._column(cell, week)
        if row is None or col is None:
            return 0.0
        return float(self.values[row, col])

    def vector(self, cell: str, week: int) -> np.ndarray:
        """The prior over every species for one context."""
        col = self._column(cell, week)
        if col is None:
            return np.zeros(len(self.species_keys), dtype=np.float64)
        return np.asarray(self.values[:, col].todense()).ravel()


def to_tensor(table: pa.Table, spec: PriorSpec) -> PriorTensor:
    rows = table.to_pylist()
    species_keys = sorted({int(r["species_key"]) for r in rows})
    cells = sorted({r["cell"] for r in rows})
    s_index = {k: i for i, k in enumerate(species_keys)}
    c_index = {c: i for i, c in enumerate(cells)}

    data, row_idx, col_idx = [], [], []
    for r in rows:
        if r["p"] == 0.0:
            continue
        data.append(float(r["p"]))
        row_idx.append(s_index[int(r["species_key"])])
        col_idx.append(c_index[r["cell"]] * WEEKS_PER_YEAR + (int(r["week"]) - 1))

    matrix = sparse.csr_matrix(
        (data, (row_idx, col_idx)),
        shape=(len(species_keys), len(cells) * WEEKS_PER_YEAR),
        dtype=np.float64,
    )
    return PriorTensor(
        spec=spec,
        species_keys=np.asarray(species_keys, dtype=np.int64),
        cells=cells,
        values=matrix,
    )


def save_prior(tensor: PriorTensor, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    sparse.save_npz(path, tensor.values.tocoo())
    npz = path if path.suffix == ".npz" else path.with_suffix(".npz")
    meta = {
        "spec": asdict(tensor.spec),
        "species_keys": tensor.species_keys.tolist(),
        "cells": tensor.cells,
        "weeks_per_year": WEEKS_PER_YEAR,
        "licence": LICENCE,
        "gbif_doi": tensor.gbif_doi,
    }
    npz.with_suffix(".json").write_text(json.dumps(meta), encoding="utf-8")
    return npz


def load_prior(path: Path) -> PriorTensor:
    meta = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
    return PriorTensor(
        spec=PriorSpec(**meta["spec"]),
        species_keys=np.asarray(meta["species_keys"], dtype=np.int64),
        cells=list(meta["cells"]),
        values=sparse.load_npz(path).tocsr(),
        gbif_doi=meta.get("gbif_doi", ""),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_tensor.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/prior/tensor.py tests/prior/test_tensor.py
git commit -m "feat(prior): sparse tensor format with a citable sidecar"
```

---

### Task 9: Control variants

**Files:**
- Create: `src/birdology/prior/controls.py`
- Test: `tests/prior/test_controls.py`

**Interfaces:**
- Consumes: a `PriorTensor` with `spec.variant == "full"`.
- Produces:
  - `uniform_prior(t) -> PriorTensor`
  - `marginal_prior(t) -> PriorTensor`
  - `shuffle_space(t, seed: int) -> PriorTensor`
  - `shuffle_time(t, seed: int) -> PriorTensor`
  - `CONTROLS: dict[str, Callable]` keyed by variant name.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_controls.py`:

```python
"""The four control priors. A gain surviving all four is a real effect."""
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.controls import (
    CONTROLS,
    marginal_prior,
    shuffle_space,
    shuffle_time,
    uniform_prior,
)
from birdology.prior.tensor import PriorSpec, to_tensor

SPEC = PriorSpec(resolution=4, source="dof", variant="full")


def _tensor():
    rows = []
    for cell in ("a", "b"):
        for week in (1, 20):
            for sk, p in ((1, 0.8 if cell == "a" else 0.2), (2, 0.2 if cell == "a" else 0.8)):
                rows.append((sk, cell, week, p if week == 20 else p / 2))
    return to_tensor(
        pa.table(
            {
                "species_key": pa.array([r[0] for r in rows], pa.int64()),
                "cell": pa.array([r[1] for r in rows], pa.string()),
                "week": pa.array([r[2] for r in rows], pa.int8()),
                "p": pa.array([r[3] for r in rows], pa.float64()),
            }
        ),
        SPEC,
    )


def test_uniform_is_flat_across_species():
    u = uniform_prior(_tensor())
    vec = u.vector("a", 20)
    assert np.allclose(vec, vec[0])


def test_uniform_variant_is_labelled():
    assert uniform_prior(_tensor()).spec.variant == "uniform"


def test_marginal_ignores_cell_and_week():
    m = marginal_prior(_tensor())
    assert np.allclose(m.vector("a", 20), m.vector("b", 1))


def test_marginal_still_varies_across_species():
    """The point of the control: label frequency survives, context does not."""
    m = marginal_prior(_tensor())
    vec = m.vector("a", 20)
    assert not np.allclose(vec, vec[0])


def test_shuffle_space_permutes_cells_deterministically():
    t = _tensor()
    a = shuffle_space(t, seed=0)
    b = shuffle_space(t, seed=0)
    assert np.allclose(a.vector("a", 20), b.vector("a", 20))


def test_shuffle_space_preserves_the_value_multiset():
    t = _tensor()
    s = shuffle_space(t, seed=1)
    assert sorted(t.values.toarray().ravel()) == pytest.approx(
        sorted(s.values.toarray().ravel())
    )


def test_shuffle_time_preserves_the_value_multiset():
    t = _tensor()
    s = shuffle_time(t, seed=1)
    assert sorted(t.values.toarray().ravel()) == pytest.approx(
        sorted(s.values.toarray().ravel())
    )


def test_controls_registry_covers_all_four():
    assert set(CONTROLS) == {"uniform", "marginal", "shuffle_space", "shuffle_time"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_controls.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'birdology.prior.controls'`

- [ ] **Step 3: Write the implementation**

Create `src/birdology/prior/controls.py`:

```python
"""Control priors.

A reported gain must survive all four or it is not a spatiotemporal effect:

- uniform        : no information at all
- marginal       : species frequency only, no space, no time. The critical one
                   — it answers "is the gain just label-frequency exploitation?"
- shuffle_space  : real values, wrong places
- shuffle_time   : real values, wrong weeks
"""
from __future__ import annotations

from collections.abc import Callable

import numpy as np
from scipy import sparse

from .indexing import WEEKS_PER_YEAR
from .tensor import PriorSpec, PriorTensor


def _respec(t: PriorTensor, variant: str, values: sparse.csr_matrix) -> PriorTensor:
    return PriorTensor(
        spec=PriorSpec(t.spec.resolution, t.spec.source, variant),
        species_keys=t.species_keys,
        cells=t.cells,
        values=values.tocsr(),
        gbif_doi=t.gbif_doi,
    )


def uniform_prior(t: PriorTensor) -> PriorTensor:
    n_species = len(t.species_keys)
    dense = np.full(t.values.shape, 1.0 / max(n_species, 1), dtype=np.float64)
    return _respec(t, "uniform", sparse.csr_matrix(dense))


def marginal_prior(t: PriorTensor) -> PriorTensor:
    """Each species' mean probability, repeated for every context."""
    dense = t.values.toarray()
    per_species = dense.mean(axis=1, keepdims=True)
    total = per_species.sum()
    if total > 0:
        per_species = per_species / total
    return _respec(t, "marginal", sparse.csr_matrix(np.tile(per_species, (1, dense.shape[1]))))


def shuffle_space(t: PriorTensor, seed: int = 0) -> PriorTensor:
    """Permute whole cells, keeping each cell's weekly profile intact."""
    rng = np.random.default_rng(seed)
    dense = t.values.toarray()
    n_cells = len(t.cells)
    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)
    blocks = blocks[:, rng.permutation(n_cells), :]
    return _respec(t, "shuffle_space", sparse.csr_matrix(blocks.reshape(dense.shape)))


def shuffle_time(t: PriorTensor, seed: int = 0) -> PriorTensor:
    """Permute weeks, keeping each cell's set of weekly values intact."""
    rng = np.random.default_rng(seed)
    dense = t.values.toarray()
    n_cells = len(t.cells)
    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)
    blocks = blocks[:, :, rng.permutation(WEEKS_PER_YEAR)]
    return _respec(t, "shuffle_time", sparse.csr_matrix(blocks.reshape(dense.shape)))


CONTROLS: dict[str, Callable[..., PriorTensor]] = {
    "uniform": uniform_prior,
    "marginal": marginal_prior,
    "shuffle_space": shuffle_space,
    "shuffle_time": shuffle_time,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_controls.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/birdology/prior/controls.py tests/prior/test_controls.py
git commit -m "feat(prior): the four control priors

The marginal-frequency control is the one a reviewer will demand."
```

---

### Task 10: End-to-end build script

**Files:**
- Create: `scripts/build_prior.py`
- Test: `tests/prior/test_build_prior.py`
- Modify: `Makefile` (add a `prior` target)
- Modify: `README.md` (document the script under "Scripts")

**Interfaces:**
- Consumes: every module above.
- Produces:
  - `build_from_archive(zip_path, *, resolution, source, test_recordings, out_dir, gbif_doi) -> list[Path]` — writes the full prior plus all four controls, returns their paths.
  - CLI: `python scripts/build_prior.py --archive PATH --source dof --resolutions 3 4 5 --out output/priors`

> **ACCEPTANCE CRITERION added 2026-09-29 — prove the GUARDED table is what reaches
> `target_group_ratios`.** A guarded and an unguarded `CountTable` are structurally
> identical (same four columns, same dtypes), so NO check inside `effort.py` can tell them
> apart — this is unenforceable anywhere except here. The failure it guards against is the
> worst kind: an unguarded denominator under-weights every guarded cell-week uniformly,
> producing a smooth, plausible, mildly-flat prior with no error, no nan and no missing
> row. Invisible in the output and invisible in the paper.
> Close it STRUCTURALLY, not by convention. Cheapest workable form: stamp the guard chain's
> output with Arrow schema metadata (e.g. `b'leakage_guards_applied'`), have
> `aggregate_counts` propagate it, and have `target_group_ratios` require it and raise
> otherwise (~5 lines, machine-checked, compatible with the no-try/except rule). Stronger
> form: a distinct type (`NewType`/wrapper `GuardedCounts`) so an unguarded table cannot be
> passed at all. Either way this task must carry an explicit test that the ONLY table
> reaching `target_group_ratios` is the post-guard one.
>
> **CORRECTED 2026-09-29 — the multi-resolution sweep and the test split are incompatible
> as originally written.** `build_from_archive` takes one `resolution`, but the CLI loops
> over `--resolutions 3 4 5` while `test_recordings` is supplied ONCE. A `TestRecording`'s
> `cell` is fixed at whatever resolution it was built at, so from Task 4 onward the guard
> RAISES `ValueError` on every resolution except the matching one (it used to silently
> no-op, which is why this was invisible in the original draft).
> Task 10 obligations, all four binding:
>   1. **Re-derive the split's cells per resolution.** Carry each test recording's lat/lon
>      and compute its cell with the SAME `resolution` value handed to `assign_cells` —
>      never reuse one pre-computed cell across resolutions. (Alternatively restrict the
>      sweep to the split's own resolution, but re-deriving is what the sweep needs.)
>   2. **Let a guard ValueError crash the build.** Never try/except it to keep the loop
>      going: a caught mismatch writes a leak-contaminated artifact under a normal-looking
>      filename, which is worse than the pre-fix silence because it looks deliberate. Fail
>      that resolution's artifact entirely and exit non-zero.
>   3. **Assert the split is non-empty.** `drop_test_contributor_cells` returns early and
>      silently on an empty `test_recordings` — correct for that module, and `[]` is a
>      legitimate mode. Task 10 is the ONLY place that knows a split was meant to be wired
>      in, so require `--test-split PATH` or an explicit `--no-test-split` opt-out. Without
>      this, the highest-consequence failure in the plan (a paper's numbers computed against
>      an unguarded prior) has no automated detector anywhere.
>   4. **Configure logging so WARNING reaches the console and the build log.** The
>      zero-match warning is the shared symptom of four silent failure modes; an
>      unconfigured logger discards it and re-silences all of them. Treat that specific
>      warning as a build failure when a non-empty split was supplied.

- [ ] **Step 1: Write the failing test**

Create `tests/prior/test_build_prior.py`:

```python
"""End-to-end: archive in, prior tensors out. No network."""
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.tensor import load_prior
from build_prior import build_from_archive

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)


def _row(i, sk, lat, lon, d, pub="pub", who="obs"):
    return "\t".join(
        [str(i), "ds", pub, str(sk), f"sp{sk}", str(lat), str(lon), d, who]
    )


def _archive(tmp_path):
    rows = []
    i = 0
    for week_date in ("2025-01-07", "2025-06-10", "2025-11-14"):
        for sk in (1001, 1002):
            for _ in range(5):
                i += 1
                rows.append(_row(i, sk, 55.6867 + i * 1e-4, 12.57, week_date))
    zip_path = tmp_path / "occ.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return zip_path


def test_writes_full_prior_and_four_controls(tmp_path):
    paths = build_from_archive(
        _archive(tmp_path),
        resolution=4,
        source="dof",
        test_recordings=[],
        out_dir=tmp_path / "priors",
        gbif_doi="10.15468/dl.test",
    )
    variants = {load_prior(p).spec.variant for p in paths}
    assert variants == {
        "full", "uniform", "marginal", "shuffle_space", "shuffle_time"
    }


def test_full_prior_has_probabilities_for_seen_species(tmp_path):
    paths = build_from_archive(
        _archive(tmp_path),
        resolution=4,
        source="dof",
        test_recordings=[],
        out_dir=tmp_path / "priors",
        gbif_doi="10.15468/dl.test",
    )
    full = next(p for p in paths if load_prior(p).spec.variant == "full")
    t = load_prior(full)
    assert set(t.species_keys.tolist()) == {1001, 1002}
    assert t.values.max() > 0.0


def test_doi_travels_into_every_artifact(tmp_path):
    paths = build_from_archive(
        _archive(tmp_path),
        resolution=4,
        source="dof",
        test_recordings=[],
        out_dir=tmp_path / "priors",
        gbif_doi="10.15468/dl.test",
    )
    for p in paths:
        assert load_prior(p).gbif_doi == "10.15468/dl.test"


def test_filenames_encode_source_resolution_and_variant(tmp_path):
    paths = build_from_archive(
        _archive(tmp_path),
        resolution=5,
        source="eod",
        test_recordings=[],
        out_dir=tmp_path / "priors",
        gbif_doi="d",
    )
    names = {p.name for p in paths}
    assert "prior_eod_r5_full.npz" in names
    assert "prior_eod_r5_marginal.npz" in names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_build_prior.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'build_prior'`

- [ ] **Step 3: Write the implementation**

Create `scripts/build_prior.py`:

```python
#!/usr/bin/env python3
"""Build spatiotemporal occurrence priors from a GBIF download archive.

    python scripts/build_prior.py --archive dof.zip --source dof \
        --resolutions 3 4 5 --out output/priors

Requesting the archive itself needs GBIF credentials:

    GBIF_USER / GBIF_PASSWORD / GBIF_EMAIL

See docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.prior.backoff import hierarchical_estimate
from birdology.prior.controls import CONTROLS
from birdology.prior.effort import target_group_ratios
from birdology.prior.indexing import aggregate_counts, assign_cells
from birdology.prior.leakage import TestRecording, apply_leakage_guards
from birdology.prior.occurrences import read_occurrences
from birdology.prior.smoothing import smooth
from birdology.prior.tensor import PriorSpec, save_prior, to_tensor


def build_from_archive(
    zip_path: Path,
    *,
    resolution: int,
    source: str,
    test_recordings: Sequence[TestRecording],
    out_dir: Path,
    gbif_doi: str,
) -> list[Path]:
    """Archive -> full prior + four controls. Returns the written paths."""
    table = assign_cells(read_occurrences(Path(zip_path)), resolution)
    table = apply_leakage_guards(table, test_recordings)

    counts = aggregate_counts(table)
    ratios = target_group_ratios(counts)
    species = sorted({int(k) for k in counts.column("species_key").to_pylist()})
    estimates = smooth(hierarchical_estimate(ratios, species))

    spec = PriorSpec(resolution=resolution, source=source, variant="full")
    full = to_tensor(estimates, spec)
    full.gbif_doi = gbif_doi

    written: list[Path] = []
    stem = f"prior_{source}_r{resolution}"
    written.append(save_prior(full, out_dir / f"{stem}_full.npz"))
    for name, make in CONTROLS.items():
        control = make(full)
        control.gbif_doi = gbif_doi
        written.append(save_prior(control, out_dir / f"{stem}_{name}.npz"))
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive", required=True, type=Path)
    ap.add_argument("--source", required=True, choices=["eod", "dof", "eod+dof"])
    ap.add_argument("--resolutions", nargs="+", type=int, default=[3, 4, 5])
    ap.add_argument("--out", type=Path, default=Path("output/priors"))
    ap.add_argument("--gbif-doi", default="")
    args = ap.parse_args()

    for resolution in args.resolutions:
        paths = build_from_archive(
            args.archive,
            resolution=resolution,
            source=args.source,
            test_recordings=[],
            out_dir=args.out,
            gbif_doi=args.gbif_doi,
        )
        for path in paths:
            print(path)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_build_prior.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Run the whole prior suite**

Run: `uv run pytest tests/prior/ -v`
Expected: PASS (58 tests across 9 files)

- [ ] **Step 6: Add the Makefile target**

In `Makefile`, after the `reason` target, add:

```makefile
.PHONY: prior
prior: ## Build occurrence priors from a GBIF archive (ARCHIVE=path SOURCE=dof)
	uv run python scripts/build_prior.py --archive $(ARCHIVE) --source $(SOURCE) --out output/priors
```

- [ ] **Step 7: Document it in the README**

In `README.md`, in the "Scripts" table, add a row:

```markdown
| `scripts/build_prior.py` | Build spatiotemporal occurrence priors (+ controls) from a GBIF download archive |
```

- [ ] **Step 8: Commit**

```bash
git add scripts/build_prior.py tests/prior/test_build_prior.py Makefile README.md
git commit -m "feat(prior): end-to-end prior build script

Archive in, full prior plus four controls out, at any H3 resolution."
```

---

## Self-Review

**Spec coverage.** Section 1 of the spec maps onto tasks as follows: resolution (Task 3), effort correction (Task 5), sparsity/smoothing (Tasks 6, 7), leakage guards (Task 4), two instantiations (Task 10's `--source`), data access (Task 1). Section 2's controls are Task 9. The released-artifacts list in Section 4 is Task 8's sidecar. **Gap accepted:** the spec's "proportional re-pull over a long window" is realised by Task 1's `year_from` parameter, but choosing that window is an open question in the spec and stays a runtime argument.

**Not in this plan, by design:** Phase A embedding extraction and Phase B the sweep are separate subsystems and get their own plans, which depend on Task 8's `PriorTensor.vector()` interface.

**Placeholder scan:** clean — every step carries runnable code.

**Type consistency:** `CountTable` columns `species_key/cell/week/n` flow from Task 3 to 5; `ratio` is added in Task 5 and consumed in Task 6; `p` is produced in Task 6, preserved by Task 7, consumed by Task 8. `PriorTensor.vector()` is the single interface downstream plans use.

**Review Focus coverage:** (1) `test_species_absent_everywhere_gets_floor_not_zero`, Task 6. (2) `test_empty_cell_week_backs_off_without_dividing_by_zero`, Task 6. (3) `test_iso_week_53_folds_into_52`, Task 3. (4) `test_antimeridian_cell_has_six_neighbours` and `test_antimeridian_cell_smooths_without_error`, Task 7. (5) `test_guarded_table_differs_from_unguarded`, Task 4.
