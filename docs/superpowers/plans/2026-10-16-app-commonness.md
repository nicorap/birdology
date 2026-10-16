# Commonness chip and rare-bird warning — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fugle DK shows, on every detection, how common the bird is at the user's place and week (from the DOF prior inside Denmark, the BirdNET geo model elsewhere), and warns on rare ones.

**Architecture:** birdology turns the DOF r5 prior into six commonness tiers per (cell, week, species), calibrated against the Danish rarities list, and writes a small three-file asset (sidecar, per-cell zlib tier blocks, lat/lon raster). The Flutter fork decodes the asset for the cell the user stands in, merges it with the existing geo-model commonness, and renders a tier chip plus a rare warning in `DetectionTile`.

**Tech Stack:** Python 3 (numpy, pyarrow, h3 4.x, pytest; uv locally, `apps/venvs/hf` on the fleet). Flutter/Dart (Riverpod 3, `dart:io` zlib, flutter_test, integration_test).

**Spec:** `docs/superpowers/specs/2026-10-16-app-commonness-design.md`

## Global Constraints

- Source prior: `data/prior/dof/prior_dof_r5_full.{npz,json}` (fleet), GBIF download `10.15468/dl.3j4czw`, licence `CC BY 4.0`.
- Tier byte = `ExploreTier.index`: `rare=0, scarce=1, uncommon=2, frequent=3, common=4, abundant=5`; `255` = no value.
- Starting thresholds on share of records p (lower bounds of scarce..abundant): `5e-5, 3e-4, 1e-3, 3e-3, 1e-2`.
- Checks: SU species rare in >= 95% of their cell-weeks; Turdus merula, Parus major, Fringilla coelebs common or abundant in >= 90% of cell-weeks in weeks 14-30 over all cells; Apus apus rare in >= 90% of cell-weeks in weeks 1-10 and 45-52.
- Weeks: ISO week, 53 folded into 52, 1..52. Not the geo model's 48-week scheme.
- Raster: step `0.02` degrees, uint16 little-endian, row-major from the south-west corner, `0xFFFF` = outside.
- Asset files: `assets/prior/dk_tiers.json`, `dk_tiers.bin`, `dk_raster.bin`; sidecar `version` is `1`.
- Rare warning text: confidence < 0.8 "Rare here this week. Double-check the ID."; >= 0.8 "Rare here this week. Worth reporting to DOFbasen if confirmed."
- Source sheet: "Based on DOFbasen records for this area and time of year (GBIF 10.15468/dl.3j4czw, CC BY 4.0)." / "Based on BirdNET's range model."
- Setting "Show how common birds are", default on.
- birdology: never `git commit -a`; stage files explicitly. Do not stage README.md, AGENTS.md, the screenshot PNG, `docs/superpowers/plans/2026-10-01-phase-b-fusion-sweep.md`, `tests/test_answer_grounding.py`.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Fleet: `tsh ssh --proxy=gimle.forsvaret.ai rapnic@slurm-login-01.infra.faic.dk`; B=/srv/home/rapnic/birdology; deploy with `git archive` as in earlier tasks; scratch never in `/tmp` on compute nodes.

## Review Focus

1. No location (GPS denied, manual location unset) → no chip, no warning, no crash. Test in Task 6.
2. Standing between cells or offshore (raster value `0xFFFF`) → the geo-model tier, not nothing and not an exception. Test in Task 5 and Task 6.
3. Dates around New Year and in ISO week 53 (e.g. 2026-12-31, 2027-01-01) → weeks 52/53→52 and 53→52, never 0 or 53. Test in Task 5.
4. Name variants between the app labels and DOF/GBIF names (Charadrius/Thinornis dubius, Accipiter/Astur gentilis) → matched through the synonym groups. Test in Task 2.
5. Manual detections (confidence 1.0, user-picked species) of a rare bird → chip plus the "worth reporting" wording, never "double-check". Test in Task 8.

---

### Task 1: SU list from DOF's Danish checklist

**Files:**
- Create: `src/birdology/prior/su_list.py`
- Create: `scripts/prior_su_list.py`
- Create: `src/birdology/prior/data/su_species.csv` (generated, committed)
- Create: `src/birdology/prior/data/su_species.source.json` (generated, committed)
- Test: `tests/prior/test_su_list.py`

**Interfaces:**
- Produces: `parse_checklist(text: str) -> list[tuple[str, str, str]]` (scientific, Danish, English names of SU species); `load_su_species(path: Path) -> set[str]`; `SOURCE_URL: str`.

- [ ] **Step 1: Write the failing test**

```python
"""SU species parsed from DOF's Danish checklist (pdftotext -layout output)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.su_list import load_su_species, parse_checklist  # noqa: E402

EXCERPT = """\
DRC CATEGORY SCIENTIFIC NAME          DANISH NAME            ENGLISH NAME                     STATUS
  X         A        Oxyura leucocephala                     Hvidhovedet Skarveand                    White-headed Duck                       A WM
                     Oxyura jamaicensis                      Amerikansk Skarveand                     Ruddy Duck
  X        C5.2       O. j. jamaicensis                       Amerikansk Skarveand                     North American Ruddy Duck              A MB, R PM WM SM
            A        Cygnus olor                             Knopsvane                                Mute Swan                               FC RB, C PM WM
     A     Clangula hyemalis         Havlit                 Long-tailed Duck                 C PM WM, R SM
X    A     Polysticta stelleri       Stellersand            Steller's Eider                  R Ir PM WM, VR SM
           Otis tarda                Stortrappe             Great Bustard
X    A      O. t. tarda               Stortrappe             Western Great Bustard           A MB, A PM WM SM
           Streptopelia turtur       Turteldue              European Turtle Dove
     A      S. t. turtur              Turteldue              Northern European Turtle Dove   R MB, R PM, A WM            -1969
           Branta canadensis         Canadagås              Canada Goose
X    A      B. c. parvipes            Lille Canadagås        Lesser Canada Goose             A PM
     C1     B. c. canadensis          Atlantisk Canadagås    Atlantic Canada Goose           R Ir RB, C PM WM, S SM
(X)  A     Aquila clanga             Stor Skrigeørn         Greater Spotted Eagle            VR PM
           Lyrurus tetrix            Urfugl                 Black Grouse
     A      L. t. tetrix †            Urfugl                 Eurasian Black Grouse           E RB                        Extinct
APPENDIX 2: Category D
X    D     Anas formosa              Sibirisk Krikand       Baikal Teal                      A PM
"""


def test_species_marked_on_their_own_row_or_on_every_subspecies():
    names = [s for s, _, _ in parse_checklist(EXCERPT)]
    assert names == ["Oxyura leucocephala", "Oxyura jamaicensis", "Polysticta stelleri", "Otis tarda"]


def test_regional_marks_partial_subspecies_and_later_appendices_are_not_su():
    names = {s for s, _, _ in parse_checklist(EXCERPT)}
    assert "Aquila clanga" not in names        # (X): only from some regions
    assert "Branta canadensis" not in names    # one subspecies marked, the other not
    assert "Anas formosa" not in names         # Appendix 2 is not the Danish list
    assert "Cygnus olor" not in names


def test_names_carry_danish_and_english():
    assert ("Polysticta stelleri", "Stellersand", "Steller's Eider") in parse_checklist(EXCERPT)


def test_load_su_species(tmp_path):
    p = tmp_path / "su.csv"
    p.write_text("scientific_name,danish_name,english_name\nOtis tarda,Stortrappe,Great Bustard\n")
    assert load_su_species(p) == {"Otis tarda"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/prior/test_su_list.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'birdology.prior.su_list'`

- [ ] **Step 3: Write the implementation**

`src/birdology/prior/su_list.py`:

```python
"""Species whose Danish records must be documented to the Danish Rarities
Committee (SU), from DOF's checklist "The Danish List" (updated Sept 2025).

The checklist marks such taxa with "X" in its first column, on the species row
or on subspecies rows; "(X)" means only records from some regions, which is not
a national rarity. A species counts as SU when its own row is marked, or when it
has subspecies rows and every one of them is marked. Only Appendix 1 (categories
A, B, C: the Danish list proper) is read.
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path

SOURCE_URL = (
    "https://www.dof.dk/images/udvalg/su/dokumenter/su_listen/"
    "The_Danish_List_-_A_Checklist_of_the_Birds_of_Denmark.pdf"
)
_SPECIES = re.compile(r"^[A-Z][a-z]+ [a-z]+(-[a-z]+)?$")
_SUBSPECIES = re.compile(r"^[A-Z]\. [a-z]+\.")
_CATEGORY = re.compile(r"^[A-F][0-9.]*( [A-F][0-9.]*)*$")


@dataclass
class _Row:
    name: str
    danish: str
    english: str
    mark: str
    subspecies_marks: list[str] = field(default_factory=list)


def parse_checklist(text: str) -> list[tuple[str, str, str]]:
    """(scientific, Danish, English) names of the SU species in checklist text."""
    rows: list[_Row] = []
    for line in text.splitlines():
        if "APPENDIX 2" in line:
            break
        fields = [f for f in re.split(r"\s{2,}", line.strip()) if f]
        if not fields:
            continue
        mark = fields.pop(0) if fields[0] in ("X", "(X)") else ""
        if fields and _CATEGORY.match(fields[0]):
            fields.pop(0)
        if not fields:
            continue
        name = fields[0].replace("†", "").strip()
        if _SPECIES.match(name):
            rows.append(_Row(name, fields[1] if len(fields) > 1 else "",
                             fields[2] if len(fields) > 2 else "", mark))
        elif _SUBSPECIES.match(name) and rows:
            rows[-1].subspecies_marks.append(mark)
    return [
        (r.name, r.danish, r.english)
        for r in rows
        if r.mark == "X" or (r.subspecies_marks and all(m == "X" for m in r.subspecies_marks))
    ]


def load_su_species(path: Path) -> set[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return {row["scientific_name"] for row in csv.DictReader(f)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/prior/test_su_list.py -v`
Expected: 4 PASS

- [ ] **Step 5: Write the generator script**

`scripts/prior_su_list.py`:

```python
#!/usr/bin/env python3
"""Download DOF's Danish checklist and write the SU species list.

    python scripts/prior_su_list.py --out src/birdology/prior/data

Needs pdftotext (poppler). Writes su_species.csv and su_species.source.json
(URL, download date, sha256 of the PDF, count).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

from birdology.prior.su_list import SOURCE_URL, parse_checklist


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)
    with tempfile.TemporaryDirectory() as tmp:
        pdf = Path(tmp) / "danish_list.pdf"
        with urllib.request.urlopen(SOURCE_URL, timeout=60) as r:
            pdf.write_bytes(r.read())
        text = subprocess.run(["pdftotext", "-layout", str(pdf), "-"], check=True,
                              capture_output=True, text=True).stdout
        sha = hashlib.sha256(pdf.read_bytes()).hexdigest()
    species = parse_checklist(text)
    if len(species) < 150:
        print(f"FAIL: only {len(species)} SU species parsed; the checklist layout changed?")
        return 1
    args.out.mkdir(parents=True, exist_ok=True)
    with open(args.out / "su_species.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["scientific_name", "danish_name", "english_name"])
        w.writerows(species)
    (args.out / "su_species.source.json").write_text(json.dumps({
        "url": SOURCE_URL, "downloaded": dt.date.today().isoformat(), "pdf_sha256": sha,
        "count": len(species), "citation": "Olsen, K. & Danish Rarities Committee 2025. "
        "The Danish List: A Checklist of the Birds of Denmark. Dansk Orn. Foren. Tidsskr. 119: 105-112.",
    }, indent=2) + "\n")
    print(f"{len(species)} SU species -> {args.out / 'su_species.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Generate the list and check it**

Run: `PYTHONPATH=src uv run python scripts/prior_su_list.py --out src/birdology/prior/data`
Expected: a count between 150 and 300. Then:
Run: `grep -c . src/birdology/prior/data/su_species.csv; grep -E "^(Polysticta stelleri|Otis tarda|Tarsiger cyanurus|Turdus merula|Parus major)," src/birdology/prior/data/su_species.csv`
Expected: Polysticta stelleri and Otis tarda present; Turdus merula and Parus major absent. If Tarsiger cyanurus is absent, open the PDF row for it and check its mark before continuing.

- [ ] **Step 7: Commit**

```bash
git add src/birdology/prior/su_list.py scripts/prior_su_list.py src/birdology/prior/data/su_species.csv src/birdology/prior/data/su_species.source.json tests/prior/test_su_list.py
git commit -m "feat(prior): SU species list from DOF's Danish checklist

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: App tiers: names, tiers, raster, asset

**Files:**
- Create: `src/birdology/prior/app_tiers.py`
- Test: `tests/prior/test_app_tiers.py`

**Interfaces:**
- Consumes: `birdology.phase_d.taxonomy.SYNONYMS` (dict old code -> tuple of names), `birdology.phase_d.birdnet3.load_labels(path) -> list[str]`.
- Produces:
  - `TIER_NAMES: tuple[str, ...]` = ("rare","scarce","uncommon","frequent","common","abundant"); `NO_VALUE = 255`; `WEEKS = 52`;
  - `BASE_THRESHOLDS: tuple[float, ...]` = (5e-5, 3e-4, 1e-3, 3e-3, 1e-2);
  - `tiers_from_p(p: np.ndarray, thresholds) -> np.ndarray` (uint8, same shape);
  - `load_prior(npz: Path, sidecar: Path) -> tuple[dict, np.ndarray]` (meta, grid `[species, cells*52]` float64);
  - `species_names_from_parquet(parquet_dir: Path) -> dict[int, str]`;
  - `match_names(prior_names: dict[int, str], app_names: list[str]) -> dict[int, str]`;
  - `p_grid(meta, grid, matched: dict[int, str], species: list[str]) -> np.ndarray` (`[cells, 52, len(species)]`);
  - `build_raster(cells: list[str], resolution: int, step: float = 0.02) -> tuple[dict, np.ndarray]` (meta keys `lat0, lon0, step, nlat, nlon`; array uint16 `[nlat, nlon]`);
  - `write_asset(out: Path, *, species, cells, tiers, raster_meta, raster, thresholds, calibration, gbif_doi, licence, attribution, export_date) -> dict` (the sidecar);
  - `read_asset(out: Path) -> tuple[dict, np.ndarray, np.ndarray]` (sidecar, tiers `[cells, 52, S]`, raster);
  - `synthetic_inputs() -> tuple[list[str], list[str], np.ndarray]` (cells, species, p `[cells, 52, S]`).

- [ ] **Step 1: Write the failing tests**

```python
"""Commonness tiers for the app from the DOF prior: tiers, names, raster, asset."""
import json
import sys
from pathlib import Path

import h3
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.app_tiers import (  # noqa: E402
    BASE_THRESHOLDS, NO_VALUE, build_raster, load_prior, match_names, p_grid, read_asset,
    synthetic_inputs, tiers_from_p, write_asset,
)


def test_thresholds_are_lower_bounds():
    p = np.array([1e-6, 4.99e-5, 5e-5, 3e-4, 1e-3, 3e-3, 9.99e-3, 1e-2, 0.5])
    assert tiers_from_p(p, BASE_THRESHOLDS).tolist() == [0, 0, 1, 2, 3, 4, 4, 5, 5]


def test_match_names_exact_and_through_synonyms():
    prior = {1: "Turdus merula", 2: "Thinornis dubius", 3: "Astur gentilis", 4: "Struthio camelus"}
    app = ["Turdus merula", "Charadrius dubius", "Accipiter gentilis"]
    assert match_names(prior, app) == {1: "Turdus merula", 2: "Charadrius dubius", 3: "Accipiter gentilis"}


def test_match_names_prefers_the_exact_name_on_a_collision():
    prior = {7: "Charadrius dubius", 8: "Thinornis dubius"}
    assert match_names(prior, ["Charadrius dubius"]) == {7: "Charadrius dubius"}


def _write_prior(tmp_path, cells, keys, dense):
    """dense: [species, cells*52]; written fully materialised like build_prior."""
    rows, cols = np.nonzero(np.ones_like(dense, dtype=bool))
    np.savez(tmp_path / "p.npz", format=np.bytes_(b"coo"), shape=np.array(dense.shape),
             row=rows, col=cols, data=dense[rows, cols])
    meta = {"species_keys": keys, "cells": cells, "weeks_per_year": 52, "missing_value": 1e-6,
            "nnz": int(dense.size), "dense_capacity": int(dense.size)}
    (tmp_path / "p.json").write_text(json.dumps(meta))
    return tmp_path / "p.npz", tmp_path / "p.json"


def test_load_prior_and_p_grid_follow_the_documented_column_layout(tmp_path):
    cells = sorted([h3.latlng_to_cell(55.68, 12.57, 5), h3.latlng_to_cell(56.15, 10.20, 5)])
    dense = np.full((2, 2 * 52), 1e-6)
    dense[0, 1 * 52 + (20 - 1)] = 0.02           # species key 10, cell 1, week 20
    npz, js = _write_prior(tmp_path, cells, [10, 11], dense)
    meta, grid = load_prior(npz, js)
    p = p_grid(meta, grid, {10: "Turdus merula", 11: "Apus apus"}, ["Apus apus", "Turdus merula"])
    assert p.shape == (2, 52, 2)
    assert p[1, 19, 1] == 0.02 and p[0, 19, 1] == 1e-6 and p[1, 19, 0] == 1e-6


def test_raster_pixels_agree_with_h3(tmp_path):
    cells, _, _ = synthetic_inputs()
    meta, raster = build_raster(cells, 5)
    hits = 0
    for i in range(0, meta["nlat"], 7):
        for j in range(0, meta["nlon"], 7):
            lat = meta["lat0"] + (i + 0.5) * meta["step"]
            lon = meta["lon0"] + (j + 0.5) * meta["step"]
            c = h3.latlng_to_cell(lat, lon, 5)
            want = cells.index(c) if c in cells else 0xFFFF
            assert raster[i, j] == want
            hits += want != 0xFFFF
    assert hits > 0


def test_asset_round_trip_and_sidecar(tmp_path):
    cells, species, p = synthetic_inputs()
    tiers = tiers_from_p(p, BASE_THRESHOLDS)
    tiers[0, 0, 0] = NO_VALUE
    rmeta, raster = build_raster(cells, 5)
    side = write_asset(tmp_path, species=species, cells=cells, tiers=tiers, raster_meta=rmeta,
                       raster=raster, thresholds=list(BASE_THRESHOLDS), calibration={"factor": 1.0},
                       gbif_doi="10.15468/dl.3j4czw", licence="CC BY 4.0",
                       attribution="Derived from GBIF occurrence download 10.15468/dl.3j4czw.",
                       export_date="2026-10-16")
    got_side, got_tiers, got_raster = read_asset(tmp_path)
    assert got_side == side and side["version"] == 1 and side["weeks"] == 52
    assert side["tier_names"] == ["rare", "scarce", "uncommon", "frequent", "common", "abundant"]
    assert (got_tiers == tiers).all() and (got_raster == raster).all()
    assert len(side["tiers"]["offsets"]) == len(cells)


def test_write_asset_refuses_an_empty_doi(tmp_path):
    cells, species, p = synthetic_inputs()
    rmeta, raster = build_raster(cells, 5)
    with pytest.raises(ValueError, match="DOI"):
        write_asset(tmp_path, species=species, cells=cells, tiers=tiers_from_p(p, BASE_THRESHOLDS),
                    raster_meta=rmeta, raster=raster, thresholds=list(BASE_THRESHOLDS), calibration={},
                    gbif_doi=" ", licence="CC BY 4.0", attribution="x", export_date="2026-10-16")


def test_synthetic_inputs_expected_tiers():
    cells, species, p = synthetic_inputs()
    t = tiers_from_p(p, BASE_THRESHOLDS)
    cph = cells.index(h3.latlng_to_cell(55.68, 12.57, 5))
    aar = cells.index(h3.latlng_to_cell(56.15, 10.20, 5))
    s = {n: i for i, n in enumerate(species)}
    assert t[cph, 19, s["Turdus merula"]] == 5      # abundant
    assert t[aar, 19, s["Turdus merula"]] == 3      # frequent
    assert t[cph, 19, s["Apus apus"]] == 4          # common in week 20
    assert t[cph, 1, s["Apus apus"]] == 0           # rare in week 2
    assert t[cph, 19, s["Tarsiger cyanurus"]] == 0  # rare
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/prior/test_app_tiers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'birdology.prior.app_tiers'`

- [ ] **Step 3: Write the implementation**

`src/birdology/prior/app_tiers.py`:

```python
"""Commonness tiers for the Fugle DK app, from a birdology occurrence prior.

A prior value is the smoothed share of a cell-week's records that are one
species. The app wants six tiers (its ExploreTier vocabulary) per (cell, week,
species), shipped as a small asset: a JSON sidecar, one zlib block of tiers per
cell, and a lat/lon raster mapping a position to its cell so the app needs no
H3 library. Layout: docs/superpowers/specs/2026-10-16-app-commonness-design.md.
"""
from __future__ import annotations

import json
import math
import zlib
from pathlib import Path

import h3
import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

from birdology.phase_d.taxonomy import SYNONYMS

TIER_NAMES = ("rare", "scarce", "uncommon", "frequent", "common", "abundant")
NO_VALUE = 255
WEEKS = 52
OUTSIDE = 0xFFFF
#: Lower bounds on p of scarce, uncommon, frequent, common, abundant.
BASE_THRESHOLDS = (5e-5, 3e-4, 1e-3, 3e-3, 1e-2)


def tiers_from_p(p: np.ndarray, thresholds) -> np.ndarray:
    """Tier index per value: the number of lower bounds p reaches."""
    return np.searchsorted(np.asarray(thresholds, np.float64), np.asarray(p), side="right").astype(np.uint8)


def load_prior(npz: Path, sidecar: Path) -> tuple[dict, np.ndarray]:
    """(meta, dense grid [species, cells*52]) per prior-artifact-format.md section 5."""
    meta = json.loads(Path(sidecar).read_text())
    with np.load(npz) as z:
        if bytes(z["format"]) != b"coo":
            raise ValueError("not a COO prior artifact")
        n_rows, n_cols = (int(x) for x in z["shape"])
        if n_rows != len(meta["species_keys"]) or n_cols != len(meta["cells"]) * meta["weeks_per_year"]:
            raise ValueError("prior matrix and sidecar axes disagree")
        grid = np.full((n_rows, n_cols), float(meta["missing_value"]), np.float64)
        grid[z["row"], z["col"]] = z["data"]
    if meta["weeks_per_year"] != WEEKS:
        raise ValueError(f"expected {WEEKS} weeks, got {meta['weeks_per_year']}")
    return meta, grid


def species_names_from_parquet(parquet_dir: Path) -> dict[int, str]:
    """GBIF species key -> its most frequent `species` name in an archive_parquet copy."""
    counts: dict[tuple[int, str], int] = {}
    for f in sorted(Path(parquet_dir).glob("part-*.parquet")):
        t = pq.read_table(f, columns=["speciesKey", "species"])
        g = t.group_by(["speciesKey", "species"]).aggregate([("speciesKey", "count")])
        for key, name, n in zip(*(g[c].to_pylist() for c in ("speciesKey", "species", "speciesKey_count"))):
            key, name = key.strip(), name.strip()
            if key.isdigit() and name:
                counts[(int(key), name)] = counts.get((int(key), name), 0) + n
    best: dict[int, tuple[int, str]] = {}
    for (key, name), n in counts.items():
        if key not in best or n > best[key][0]:
            best[key] = (n, name)
    return {k: v[1] for k, v in best.items()}


def match_names(prior_names: dict[int, str], app_names: list[str]) -> dict[int, str]:
    """GBIF key -> app scientific name: exact, else through a SYNONYMS group.
    When two keys reach one app name, the exact match wins."""
    app = set(app_names)
    groups = {n: g for g in SYNONYMS.values() for n in g}
    exact, via = {}, {}
    for key, name in sorted(prior_names.items()):
        if name in app:
            exact[key] = name
        else:
            for cand in groups.get(name, ()):
                if cand in app:
                    via[key] = cand
                    break
    taken = set(exact.values())
    out = dict(exact)
    for key, name in via.items():
        if name not in taken:
            out[key] = name
            taken.add(name)
    return out


def p_grid(meta: dict, grid: np.ndarray, matched: dict[int, str], species: list[str]) -> np.ndarray:
    """[cells, 52, len(species)] of p, species in the given order."""
    row_of = {k: i for i, k in enumerate(meta["species_keys"])}
    key_of = {name: key for key, name in matched.items()}
    rows = [row_of[key_of[s]] for s in species]
    n_cells = len(meta["cells"])
    return grid[rows].reshape(len(species), n_cells, WEEKS).transpose(1, 2, 0).copy()


def build_raster(cells: list[str], resolution: int, step: float = 0.02) -> tuple[dict, np.ndarray]:
    """uint16 [nlat, nlon]: index of the cell holding each pixel centre, else OUTSIDE."""
    if len(cells) >= OUTSIDE:
        raise ValueError("too many cells for a uint16 raster")
    pts = [p for c in cells for p in h3.cell_to_boundary(c)]
    lat_min = math.floor(min(p[0] for p in pts) / step) * step
    lon_min = math.floor(min(p[1] for p in pts) / step) * step
    nlat = math.ceil((max(p[0] for p in pts) - lat_min) / step) + 1
    nlon = math.ceil((max(p[1] for p in pts) - lon_min) / step) + 1
    index = {c: i for i, c in enumerate(cells)}
    raster = np.full((nlat, nlon), OUTSIDE, np.uint16)
    for i in range(nlat):
        lat = lat_min + (i + 0.5) * step
        for j in range(nlon):
            c = h3.latlng_to_cell(lat, lon_min + (j + 0.5) * step, resolution)
            if c in index:
                raster[i, j] = index[c]
    meta = {"lat0": round(lat_min, 6), "lon0": round(lon_min, 6), "step": step, "nlat": nlat, "nlon": nlon}
    return meta, raster


def write_asset(out: Path, *, species, cells, tiers, raster_meta, raster, thresholds, calibration,
                gbif_doi, licence, attribution, export_date) -> dict:
    """dk_tiers.json + dk_tiers.bin + dk_raster.bin into `out`; returns the sidecar."""
    if not str(gbif_doi).strip():
        raise ValueError("a GBIF download DOI is required: the CC BY 4.0 data must be attributed")
    tiers = np.asarray(tiers, np.uint8)
    if tiers.shape != (len(cells), WEEKS, len(species)):
        raise ValueError(f"tiers shape {tiers.shape} != ({len(cells)}, {WEEKS}, {len(species)})")
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    blocks, offsets, lengths, pos = [], [], [], 0
    for c in range(len(cells)):
        b = zlib.compress(tiers[c].tobytes(), 9)
        blocks.append(b)
        offsets.append(pos)
        lengths.append(len(b))
        pos += len(b)
    (out / "dk_tiers.bin").write_bytes(b"".join(blocks))
    (out / "dk_raster.bin").write_bytes(np.asarray(raster, "<u2").tobytes())
    side = {
        "version": 1, "source": "dof", "resolution": h3.get_resolution(cells[0]), "weeks": WEEKS,
        "species": list(species), "cells": list(cells), "tier_names": list(TIER_NAMES),
        "no_value": NO_VALUE, "thresholds": [float(t) for t in thresholds], "calibration": calibration,
        "gbif_doi": gbif_doi, "licence": licence, "attribution": attribution, "export_date": export_date,
        "tiers": {"file": "dk_tiers.bin", "layout": "per cell: zlib([week][species] uint8)",
                  "offsets": offsets, "lengths": lengths},
        "raster": {"file": "dk_raster.bin", "dtype": "uint16le", "outside": OUTSIDE, **raster_meta},
    }
    (out / "dk_tiers.json").write_text(json.dumps(side, indent=1) + "\n")
    return side


def read_asset(out: Path) -> tuple[dict, np.ndarray, np.ndarray]:
    out = Path(out)
    side = json.loads((out / "dk_tiers.json").read_text())
    blob = (out / "dk_tiers.bin").read_bytes()
    S, n = len(side["species"]), len(side["cells"])
    tiers = np.stack([
        np.frombuffer(zlib.decompress(blob[o:o + ln]), np.uint8).reshape(WEEKS, S)
        for o, ln in zip(side["tiers"]["offsets"], side["tiers"]["lengths"])
    ]) if n else np.zeros((0, WEEKS, S), np.uint8)
    r = side["raster"]
    raster = np.frombuffer((out / "dk_raster.bin").read_bytes(), "<u2").reshape(r["nlat"], r["nlon"])
    return side, tiers, raster


def synthetic_inputs() -> tuple[list[str], list[str], np.ndarray]:
    """A two-cell, three-species prior with known tiers; also the app's test fixture.

    Copenhagen: Turdus merula 0.02 (abundant); Apus apus 0.005 in weeks 18-33
    (common), 1e-6 otherwise (rare); Tarsiger cyanurus 1e-5 (rare).
    Aarhus: the same, but Turdus merula 0.002 (frequent).
    """
    cph = h3.latlng_to_cell(55.68, 12.57, 5)
    aar = h3.latlng_to_cell(56.15, 10.20, 5)
    cells = sorted([cph, aar])
    species = ["Apus apus", "Tarsiger cyanurus", "Turdus merula"]
    p = np.full((2, WEEKS, 3), 1e-6)
    for c, merula in ((cells.index(cph), 0.02), (cells.index(aar), 0.002)):
        p[c, :, 2] = merula
        p[c, 17:33, 0] = 0.005
        p[c, :, 1] = 1e-5
    return cells, species, p
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/prior/test_app_tiers.py -v`
Expected: 8 PASS

- [ ] **Step 5: Commit**

```bash
git add src/birdology/prior/app_tiers.py tests/prior/test_app_tiers.py
git commit -m "feat(prior): commonness tiers, name matching, raster and asset for the app

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Calibration and the export CLI

**Files:**
- Modify: `src/birdology/prior/app_tiers.py` (append calibration)
- Create: `scripts/prior_app_export.py`
- Test: `tests/prior/test_app_tiers_calibration.py`

**Interfaces:**
- Consumes: everything Task 2 produces; `load_su_species(path) -> set[str]` from Task 1; `birdology.phase_d.birdnet3.load_labels(path) -> list[str]`.
- Produces: `class CalibrationError(Exception)`; `check_tiers(tiers, species: list[str], su: set[str]) -> dict[str, dict]` (each `{"value": float, "need": float, "passed": bool}`); `calibrate(p, species, su, base=BASE_THRESHOLDS, factors=None) -> tuple[list[float], float, dict]` (thresholds, factor, checks); CLI `scripts/prior_app_export.py` with `--fixture DIR` and the real-export flags below.

- [ ] **Step 1: Write the failing tests**

```python
"""Calibrating the tier thresholds against the SU list, breeders and swift season."""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.app_tiers import BASE_THRESHOLDS, CalibrationError, calibrate, check_tiers, tiers_from_p  # noqa: E402

SPECIES = ["Turdus merula", "Parus major", "Fringilla coelebs", "Apus apus"] + [f"Rara su{i}" for i in range(10)]
SU = {f"Rara su{i}" for i in range(10)}


def _p(breeder=0.02, su=1e-5, swift_winter=1e-6):
    p = np.full((4, 52, len(SPECIES)), 1e-6)
    p[:, :, :3] = breeder
    p[:, 17:33, 3] = 0.005
    p[:, :10, 3] = p[:, 44:, 3] = swift_winter
    p[:, :, 4:] = su
    return p


def test_checks_pass_on_a_well_behaved_prior():
    checks = check_tiers(tiers_from_p(_p(), BASE_THRESHOLDS), SPECIES, SU)
    assert set(checks) == {"su_rare", "breeders_common", "swift_winter_rare"}
    assert all(c["passed"] for c in checks.values())


def test_calibrate_keeps_the_base_when_it_passes():
    thresholds, factor, checks = calibrate(_p(), SPECIES, SU)
    assert factor == 1.0 and thresholds == list(BASE_THRESHOLDS)


def test_calibrate_scales_the_thresholds_when_needed():
    # SU species at 1e-4 are scarce under the base (and at factor 2.0, where p equals
    # the bound); the next factor, 2 ** (9/8) = 2.18, makes them rare.
    thresholds, factor, checks = calibrate(_p(su=1e-4), SPECIES, SU)
    assert factor > 1.0 and checks["su_rare"]["passed"]
    assert thresholds == pytest.approx([t * factor for t in BASE_THRESHOLDS])


def test_calibrate_fails_loudly_when_no_factor_works():
    with pytest.raises(CalibrationError, match="breeders_common"):
        calibrate(_p(breeder=1e-5, su=1e-5), SPECIES, SU)


def test_missing_check_species_is_an_error():
    with pytest.raises(CalibrationError, match="Apus apus"):
        check_tiers(tiers_from_p(_p(), BASE_THRESHOLDS)[:, :, :3], SPECIES[:3], SU)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/prior/test_app_tiers_calibration.py -v`
Expected: FAIL with `ImportError: cannot import name 'CalibrationError'`

- [ ] **Step 3: Append the calibration to `src/birdology/prior/app_tiers.py`**

```python
BREEDERS = ("Turdus merula", "Parus major", "Fringilla coelebs")
SWIFT = "Apus apus"
_SUMMER = np.arange(13, 30)                                   # weeks 14-30, 0-based
_WINTER = np.r_[np.arange(0, 10), np.arange(44, 52)]          # weeks 1-10 and 45-52
MIN_SU_SPECIES = 10


class CalibrationError(Exception):
    pass


def check_tiers(tiers: np.ndarray, species: list[str], su: set[str]) -> dict[str, dict]:
    """The spec's three checks on tiers [cells, 52, S]."""
    col = {s: i for i, s in enumerate(species)}
    missing = [s for s in (*BREEDERS, SWIFT) if s not in col]
    if missing:
        raise CalibrationError(f"check species not exported: {missing}")
    su_cols = [col[s] for s in sorted(su) if s in col]
    if len(su_cols) < MIN_SU_SPECIES:
        raise CalibrationError(f"only {len(su_cols)} SU species exported; need {MIN_SU_SPECIES}")
    rare, common = 0, 4
    su_share = float((tiers[:, :, su_cols] == rare).mean())
    breed_share = float((tiers[:, _SUMMER][:, :, [col[s] for s in BREEDERS]] >= common).mean())
    swift_share = float((tiers[:, _WINTER, col[SWIFT]] == rare).mean())
    return {
        "su_rare": {"value": su_share, "need": 0.95, "passed": su_share >= 0.95},
        "breeders_common": {"value": breed_share, "need": 0.90, "passed": breed_share >= 0.90},
        "swift_winter_rare": {"value": swift_share, "need": 0.90, "passed": swift_share >= 0.90},
    }


def calibrate(p: np.ndarray, species: list[str], su: set[str], base=BASE_THRESHOLDS, factors=None):
    """(thresholds, factor, checks): the base thresholds scaled by the factor
    closest to 1 (in log terms) for which every check passes."""
    if factors is None:
        factors = [2.0 ** (k / 8) for k in range(-16, 17)]   # 0.25..4, exactly 1.0 and 2.0
    best = None
    for f in sorted(factors, key=lambda f: abs(math.log(f))):
        thresholds = [float(t * f) for t in base]
        checks = check_tiers(tiers_from_p(p, thresholds), species, su)
        if all(c["passed"] for c in checks.values()):
            return thresholds, float(f), checks
        if best is None:
            best = checks
    failed = ", ".join(k for k, c in best.items() if not c["passed"])
    raise CalibrationError(f"no threshold factor passes; at factor 1 failing: {failed} ({best})")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/prior/test_app_tiers_calibration.py tests/prior/test_app_tiers.py -v`
Expected: 13 PASS

- [ ] **Step 5: Write the CLI**

`scripts/prior_app_export.py`:

```python
#!/usr/bin/env python3
"""Export the app's commonness asset from the DOF prior.

Real export (on the fleet):
    python scripts/prior_app_export.py \\
        --prior data/prior/dof/prior_dof_r5_full \\
        --names data/prior/parquet/dof \\
        --labels apps/models/birdnet_v3/BirdNET+_V3.0-preview3.1_Global_10K-pruned_Labels.csv \\
        --su src/birdology/prior/data/su_species.csv \\
        --out data/phase_d/app_commonness

Test fixture for the app (synthetic prior, base thresholds, no calibration):
    python scripts/prior_app_export.py --fixture ../fugle-dk/test/fixtures/dk_tiers
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

from birdology.phase_d.birdnet3 import load_labels
from birdology.prior.app_tiers import (
    BASE_THRESHOLDS, CalibrationError, build_raster, calibrate, load_prior, match_names, p_grid,
    species_names_from_parquet, synthetic_inputs, tiers_from_p, write_asset,
)
from birdology.prior.su_list import load_su_species


def fixture(out: Path) -> int:
    cells, species, p = synthetic_inputs()
    rmeta, raster = build_raster(cells, 5)
    write_asset(out, species=species, cells=cells, tiers=tiers_from_p(p, BASE_THRESHOLDS),
                raster_meta=rmeta, raster=raster, thresholds=list(BASE_THRESHOLDS),
                calibration={"factor": 1.0, "fixture": True}, gbif_doi="10.15468/dl.3j4czw",
                licence="CC BY 4.0", attribution="Synthetic test fixture.", export_date="2026-10-16")
    print(f"fixture -> {out}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fixture", type=Path)
    ap.add_argument("--prior", type=Path, help="prior path without .npz/.json")
    ap.add_argument("--names", type=Path, help="archive_parquet copy of the prior's source archive")
    ap.add_argument("--labels", type=Path, help="the app's audio labels CSV")
    ap.add_argument("--su", type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args(argv)
    if args.fixture:
        return fixture(args.fixture)
    for name in ("prior", "names", "labels", "su", "out"):
        if getattr(args, name) is None:
            ap.error(f"--{name} is required for a real export")

    meta, grid = load_prior(args.prior.with_suffix(".npz"), args.prior.with_suffix(".json"))
    names = species_names_from_parquet(args.names)
    prior_names = {k: names[k] for k in meta["species_keys"] if k in names}
    app_species = load_labels(args.labels)
    matched = match_names(prior_names, app_species)
    species = sorted(matched.values())
    unmatched = sorted(set(prior_names.values()) - set(matched.values()))
    print(f"{len(meta['species_keys'])} prior species, {len(prior_names)} named, {len(species)} matched "
          f"to the app; {len(unmatched)} unmatched: {unmatched[:30]}{' ...' if len(unmatched) > 30 else ''}")
    p = p_grid(meta, grid, matched, species)
    su = load_su_species(args.su)
    try:
        thresholds, factor, checks = calibrate(p, species, su)
    except CalibrationError as e:
        print(f"FAIL: {e}")
        return 1
    for k, c in checks.items():
        print(f"check {k}: {c['value']:.3f} (need {c['need']}) {'ok' if c['passed'] else 'FAIL'}")
    print(f"thresholds {thresholds} (factor {factor:.3f})")
    rmeta, raster = build_raster(meta["cells"], meta["spec"]["resolution"] if "spec" in meta else 5)
    side = write_asset(args.out, species=species, cells=meta["cells"], tiers=tiers_from_p(p, thresholds),
                       raster_meta=rmeta, raster=raster, thresholds=thresholds,
                       calibration={"factor": factor, "checks": checks},
                       gbif_doi=meta["gbif_doi"], licence=meta["licence"], attribution=meta["attribution"],
                       export_date=dt.date.today().isoformat())
    sizes = {f.name: f.stat().st_size for f in args.out.iterdir()}
    print(json.dumps({"cells": len(side["cells"]), "species": len(side["species"]), "bytes": sizes}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Generate the app's test fixture and check it**

Run: `PYTHONPATH=src uv run python scripts/prior_app_export.py --fixture /Users/nrapin/Documents/repo/fugle-dk/test/fixtures/dk_tiers && ls -la /Users/nrapin/Documents/repo/fugle-dk/test/fixtures/dk_tiers`
Expected: `dk_tiers.json`, `dk_tiers.bin`, `dk_raster.bin`. (Committed in Task 5, in the fork.)

- [ ] **Step 7: Commit**

```bash
git add src/birdology/prior/app_tiers.py scripts/prior_app_export.py tests/prior/test_app_tiers_calibration.py
git commit -m "feat(prior): calibrate app tiers against SU list, breeders and swift season; export CLI

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Real export on the fleet

**Files:**
- Create (fleet only): `data/prior/parquet/dof/`, `data/phase_d/app_commonness/`
- Create (fork): `assets/prior/dk_tiers.json`, `assets/prior/dk_tiers.bin`, `assets/prior/dk_raster.bin`

**Interfaces:**
- Consumes: Task 3's CLI; `scripts/cluster/prior_archive_parquet.sbatch` (already deployed).
- Produces: the real asset in the fork's `assets/prior/`.

- [ ] **Step 1: Deploy birdology to the fleet**

```bash
cd /Users/nrapin/Documents/repo/birdology && SHA=$(git rev-parse --short HEAD) && git archive --format=tar HEAD | tsh ssh --proxy=gimle.forsvaret.ai rapnic@slurm-login-01.infra.faic.dk "B=/srv/home/rapnic/birdology; T=\$(mktemp -d); tar -x -C \$T; rsync -a --delete \$T/ \$B/apps/birdology/; echo $SHA > \$B/apps/birdology/DEPLOYED_SHA; rm -rf \$T"
```

- [ ] **Step 2: Convert the DOF archive to Parquet (names source)**

```bash
tsh ssh --proxy=gimle.forsvaret.ai rapnic@slurm-login-01.infra.faic.dk 'cd /srv/home/rapnic/birdology && sbatch -t 01:00:00 apps/birdology/scripts/cluster/prior_archive_parquet.sbatch data/prior/archives/dof.zip data/prior/parquet/dof'
```
Wait for the job; expected log line `... source rows = ... parquet + ... rejected`.

- [ ] **Step 3: Run the export**

```bash
tsh ssh --proxy=gimle.forsvaret.ai rapnic@slurm-login-01.infra.faic.dk 'B=/srv/home/rapnic/birdology; cd $B && srun -p gpu -c 4 --mem=16G -t 00:30:00 env HOME=/tmp PYTHONPATH=$B/apps/birdology/src $B/apps/venvs/hf/bin/python apps/birdology/scripts/prior_app_export.py --prior data/prior/dof/prior_dof_r5_full --names data/prior/parquet/dof --labels apps/models/birdnet_v3/BirdNET+_V3.0-preview3.1_Global_10K-pruned_Labels.csv --su apps/birdology/src/birdology/prior/data/su_species.csv --out data/phase_d/app_commonness'
```
Expected: three `check ... ok` lines, a thresholds line, and sizes with `dk_tiers.bin` under 3 MB. On `FAIL`, stop and report the check values to the user; do not edit thresholds by hand.

- [ ] **Step 4: Copy the asset into the fork and verify**

```bash
mkdir -p /Users/nrapin/Documents/repo/fugle-dk/assets/prior && rsync -a -e "tsh ssh --proxy=gimle.forsvaret.ai" rapnic@slurm-login-01.infra.faic.dk:/srv/home/rapnic/birdology/data/phase_d/app_commonness/ /Users/nrapin/Documents/repo/fugle-dk/assets/prior/ && shasum -a 256 /Users/nrapin/Documents/repo/fugle-dk/assets/prior/*
```
Compare with `sha256sum` of the fleet copies; all three must match.

- [ ] **Step 5: Record**

Append the export's printed check values, thresholds and sizes to the spec's section 1 as a dated "Export result" paragraph, then:
```bash
git add docs/superpowers/specs/2026-10-16-app-commonness-design.md
git commit -m "docs(spec): record the first app commonness export

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Dart asset decoder and ISO week (fork)

All remaining tasks run in `/Users/nrapin/Documents/repo/fugle-dk`, branch `danish`. Package name `birdnet_live`.

**Files:**
- Create: `lib/features/inference/dk_commonness.dart`
- Create: `test/fixtures/dk_tiers/` (from Task 3 Step 6)
- Test: `test/features/inference/dk_commonness_test.dart`

**Interfaces:**
- Consumes: `ExploreTier` from `lib/features/inference/geo_abundance.dart` (`rare, scarce, uncommon, frequent, common, abundant`).
- Produces:
  - `int isoWeek(DateTime date)`; `int priorWeek(DateTime date)` (1..52);
  - `class DkCommonness` with `factory DkCommonness.fromBytes({required String sidecarJson, required Uint8List raster, required Uint8List tiers})`, `static Future<DkCommonness> load(AssetBundle bundle, {String dir = 'assets/prior'})`, `int? cellIndexAt(double lat, double lon)`, `Map<String, ExploreTier>? tiersAt(double lat, double lon, int week)`, `ExploreTier? tierFor(String scientificName, double lat, double lon, DateTime date)`, `List<String> get species`.

- [ ] **Step 1: Write the failing test**

```dart
import 'dart:io';
import 'dart:typed_data';

import 'package:birdnet_live/features/inference/dk_commonness.dart';
import 'package:birdnet_live/features/inference/geo_abundance.dart';
import 'package:flutter_test/flutter_test.dart';

// The fixture is written by birdology's scripts/prior_app_export.py --fixture
// from app_tiers.synthetic_inputs(): Copenhagen Turdus merula abundant, Aarhus
// frequent; Apus apus common in weeks 18-33, rare otherwise; Tarsiger rare.
DkCommonness fixture() {
  const dir = 'test/fixtures/dk_tiers';
  return DkCommonness.fromBytes(
    sidecarJson: File('$dir/dk_tiers.json').readAsStringSync(),
    raster: File('$dir/dk_raster.bin').readAsBytesSync(),
    tiers: File('$dir/dk_tiers.bin').readAsBytesSync(),
  );
}

void main() {
  group('isoWeek / priorWeek', () {
    test('ordinary dates', () {
      expect(isoWeek(DateTime(2026, 10, 16)), 42);
      expect(isoWeek(DateTime(2026, 1, 5)), 2);
    });
    test('New Year edges and week 53 fold', () {
      expect(isoWeek(DateTime(2027, 1, 1)), 53); // Friday in ISO week 53 of 2026
      expect(priorWeek(DateTime(2027, 1, 1)), 52);
      expect(priorWeek(DateTime(2026, 12, 31)), 52);
      expect(isoWeek(DateTime(2024, 12, 30)), 1); // Monday, ISO week 1 of 2025
      expect(priorWeek(DateTime(2024, 12, 30)), 1);
    });
  });

  group('DkCommonness fixture', () {
    final dk = fixture();

    test('species and tiers in Copenhagen', () {
      final t = dk.tiersAt(55.68, 12.57, 20)!;
      expect(t['Turdus merula'], ExploreTier.abundant);
      expect(t['Apus apus'], ExploreTier.common);
      expect(t['Tarsiger cyanurus'], ExploreTier.rare);
      expect(dk.tiersAt(55.68, 12.57, 2)!['Apus apus'], ExploreTier.rare);
    });

    test('Aarhus is a different cell', () {
      expect(dk.tiersAt(56.15, 10.20, 20)!['Turdus merula'], ExploreTier.frequent);
    });

    test('outside every cell is null, not an exception', () {
      expect(dk.tiersAt(48.85, 2.35, 20), isNull); // Paris: outside the raster
      expect(dk.cellIndexAt(48.85, 2.35), isNull);
    });

    test('tierFor uses the prior week and unknown species are null', () {
      expect(dk.tierFor('Apus apus', 55.68, 12.57, DateTime(2026, 5, 20)), ExploreTier.common);
      expect(dk.tierFor('Corvus corax', 55.68, 12.57, DateTime(2026, 5, 20)), isNull);
    });

    test('a corrupt sidecar is rejected', () {
      expect(
        () => DkCommonness.fromBytes(sidecarJson: '{"version": 2}', raster: Uint8List(0), tiers: Uint8List(0)),
        throwsFormatException,
      );
    });
  });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `flutter test test/features/inference/dk_commonness_test.dart`
Expected: FAIL, `dk_commonness.dart` not found.

- [ ] **Step 3: Write the implementation**

`lib/features/inference/dk_commonness.dart`:

```dart
// =============================================================================
// DkCommonness — how common a species is in Denmark, from the DOF prior
// =============================================================================
//
// Reads the asset birdology's scripts/prior_app_export.py writes
// (assets/prior/dk_tiers.json, dk_tiers.bin, dk_raster.bin): six commonness
// tiers per (H3 r5 cell, ISO week, species), from DOFbasen records via GBIF
// download 10.15468/dl.3j4czw (CC BY 4.0).
//
// The raster maps a position to its cell, so no H3 library is needed. Only the
// current cell's block is inflated (about 20 KB).
//
// Weeks are ISO weeks 1..52 (53 folded into 52), as in the prior. This is NOT
// the geo model's 48-week scheme (GeoModel.dateTimeToWeek).
// =============================================================================

import 'dart:convert';
import 'dart:io' show ZLibCodec;
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:flutter/services.dart' show AssetBundle;

import 'geo_abundance.dart';

const String kDkCommonnessAssetDir = 'assets/prior';
const int _noValue = 255;
const int _outside = 0xFFFF;

/// ISO-8601 week number (1..53).
int isoWeek(DateTime date) {
  final d = DateTime.utc(date.year, date.month, date.day);
  final thursday = d.add(Duration(days: 4 - d.weekday));
  final jan1 = DateTime.utc(thursday.year, 1, 1);
  return 1 + thursday.difference(jan1).inDays ~/ 7;
}

/// The prior's week: ISO week with 53 folded into 52.
int priorWeek(DateTime date) => math.min(isoWeek(date), 52);

class DkCommonness {
  DkCommonness._({
    required this.species,
    required Map<String, int> speciesIndex,
    required int weeks,
    required double lat0,
    required double lon0,
    required double step,
    required int nlat,
    required int nlon,
    required ByteData raster,
    required Uint8List blocks,
    required List<int> offsets,
    required List<int> lengths,
  }) : _speciesIndex = speciesIndex,
       _weeks = weeks,
       _lat0 = lat0,
       _lon0 = lon0,
       _step = step,
       _nlat = nlat,
       _nlon = nlon,
       _raster = raster,
       _blocks = blocks,
       _offsets = offsets,
       _lengths = lengths;

  /// Throws [FormatException] when the asset does not match layout version 1.
  factory DkCommonness.fromBytes({
    required String sidecarJson,
    required Uint8List raster,
    required Uint8List tiers,
  }) {
    final side = jsonDecode(sidecarJson) as Map<String, dynamic>;
    if (side['version'] != 1 || side['weeks'] != 52) {
      throw const FormatException('dk_tiers.json: unsupported version or weeks');
    }
    final species = (side['species'] as List).cast<String>();
    final r = side['raster'] as Map<String, dynamic>;
    final nlat = r['nlat'] as int, nlon = r['nlon'] as int;
    if (raster.lengthInBytes != nlat * nlon * 2) {
      throw const FormatException('dk_raster.bin: size does not match the sidecar');
    }
    final t = side['tiers'] as Map<String, dynamic>;
    final offsets = (t['offsets'] as List).cast<int>();
    final lengths = (t['lengths'] as List).cast<int>();
    if (offsets.length != (side['cells'] as List).length ||
        (offsets.isNotEmpty && offsets.last + lengths.last != tiers.lengthInBytes)) {
      throw const FormatException('dk_tiers.bin: blocks do not match the sidecar');
    }
    return DkCommonness._(
      species: species,
      speciesIndex: {for (var i = 0; i < species.length; i++) species[i]: i},
      weeks: 52,
      lat0: (r['lat0'] as num).toDouble(),
      lon0: (r['lon0'] as num).toDouble(),
      step: (r['step'] as num).toDouble(),
      nlat: nlat,
      nlon: nlon,
      raster: ByteData.sublistView(raster),
      blocks: tiers,
      offsets: offsets,
      lengths: lengths,
    );
  }

  static Future<DkCommonness> load(
    AssetBundle bundle, {
    String dir = kDkCommonnessAssetDir,
  }) async {
    final side = await bundle.loadString('$dir/dk_tiers.json');
    final raster = await bundle.load('$dir/dk_raster.bin');
    final tiers = await bundle.load('$dir/dk_tiers.bin');
    return DkCommonness.fromBytes(
      sidecarJson: side,
      raster: raster.buffer.asUint8List(raster.offsetInBytes, raster.lengthInBytes),
      tiers: tiers.buffer.asUint8List(tiers.offsetInBytes, tiers.lengthInBytes),
    );
  }

  final List<String> species;
  final Map<String, int> _speciesIndex;
  final int _weeks;
  final double _lat0, _lon0, _step;
  final int _nlat, _nlon;
  final ByteData _raster;
  final Uint8List _blocks;
  final List<int> _offsets, _lengths;

  int? _cachedCell;
  Uint8List? _cachedBlock;

  /// Index of the prior cell holding (lat, lon), or null outside every cell.
  int? cellIndexAt(double lat, double lon) {
    final i = ((lat - _lat0) / _step).floor();
    final j = ((lon - _lon0) / _step).floor();
    if (i < 0 || j < 0 || i >= _nlat || j >= _nlon) return null;
    final v = _raster.getUint16((i * _nlon + j) * 2, Endian.little);
    return v == _outside ? null : v;
  }

  Uint8List _block(int cell) {
    if (_cachedCell != cell) {
      final start = _offsets[cell];
      _cachedBlock = Uint8List.fromList(
        ZLibCodec().decode(_blocks.sublist(start, start + _lengths[cell])),
      );
      _cachedCell = cell;
    }
    return _cachedBlock!;
  }

  /// Every exported species' tier at (lat, lon) in prior [week] (1..52), or
  /// null outside every cell. Species with no value are left out.
  Map<String, ExploreTier>? tiersAt(double lat, double lon, int week) {
    final cell = cellIndexAt(lat, lon);
    if (cell == null) return null;
    final block = _block(cell);
    final base = (week.clamp(1, _weeks) - 1) * species.length;
    final out = <String, ExploreTier>{};
    for (var s = 0; s < species.length; s++) {
      final v = block[base + s];
      if (v != _noValue && v < ExploreTier.values.length) {
        out[species[s]] = ExploreTier.values[v];
      }
    }
    return out;
  }

  ExploreTier? tierFor(String scientificName, double lat, double lon, DateTime date) {
    if (!_speciesIndex.containsKey(scientificName)) return null;
    return tiersAt(lat, lon, priorWeek(date))?[scientificName];
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `flutter test test/features/inference/dk_commonness_test.dart`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/features/inference/dk_commonness.dart test/features/inference/dk_commonness_test.dart test/fixtures/dk_tiers
git commit -m "feat(commonness): decode the DOF commonness asset; ISO prior week

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Providers, setting, asset registration (fork)

**Files:**
- Create: `lib/features/inference/commonness_providers.dart`
- Modify: `lib/core/constants/app_constants.dart` (class `PrefKeys`, after `showSciNames` at line 270)
- Modify: `lib/shared/providers/settings_providers.dart` (after `showSciNamesProvider`, line 668)
- Modify: `pubspec.yaml` (`flutter: assets:`, add `- assets/prior/`)
- Commit: `assets/prior/` (from Task 4)
- Test: `test/features/inference/commonness_providers_test.dart`

**Interfaces:**
- Consumes: `DkCommonness`, `priorWeek` (Task 5); `geoCommonnessProvider` and `GeoCommonnessEntry` (`lib/features/announcements/geo_commonness_provider.dart`); `currentLocationProvider` (`lib/features/explore/explore_providers.dart`, `FutureProvider<AppLocation?>`); `CommonnessBin` (`lib/features/announcements/domain/announcement_signals.dart`).
- Produces: `enum CommonnessSource { dof, geo }`; `class DetectionCommonness { ExploreTier tier; CommonnessSource source; bool isOutOfSeason; }`; `ExploreTier tierFromBin(CommonnessBin)`; `CommonnessBin binFromTier(ExploreTier)`; `Map<String, DetectionCommonness> mergeCommonness({Map<String, GeoCommonnessEntry>? geo, Map<String, ExploreTier>? dof})`; `priorWeekProvider` (`Provider<int>`); `dkCommonnessProvider` (`FutureProvider<DkCommonness?>`); `detectionCommonnessProvider` (`FutureProvider<Map<String, DetectionCommonness>?>`); `showCommonnessProvider` (`StateNotifierProvider<BoolSettingNotifier, bool>`, default true); `PrefKeys.showCommonness`.

- [ ] **Step 1: Write the failing test**

```dart
import 'package:birdnet_live/core/services/location_service.dart';
import 'package:birdnet_live/features/announcements/domain/announcement_signals.dart';
import 'package:birdnet_live/features/announcements/geo_commonness_provider.dart';
import 'package:birdnet_live/features/explore/explore_providers.dart';
import 'package:birdnet_live/features/inference/commonness_providers.dart';
import 'package:birdnet_live/features/inference/geo_abundance.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:flutter_test/flutter_test.dart';

GeoCommonnessEntry geo(CommonnessBin bin, {bool off = false}) => GeoCommonnessEntry(
  commonness: bin, isOutOfSeason: off, currentScore: 0.1, annualMax: 0.2,
);

void main() {
  test('CommonnessBin and ExploreTier share one order', () {
    for (final t in ExploreTier.values) {
      expect(binFromTier(t).name, t.name);
      expect(tierFromBin(binFromTier(t)), t);
    }
  });

  test('DOF tiers win; geo fills the rest and keeps out-of-season', () {
    final merged = mergeCommonness(
      geo: {'Apus apus': geo(CommonnessBin.common, off: true), 'Corvus corax': geo(CommonnessBin.frequent)},
      dof: {'Apus apus': ExploreTier.rare},
    );
    expect(merged['Apus apus']!.tier, ExploreTier.rare);
    expect(merged['Apus apus']!.source, CommonnessSource.dof);
    expect(merged['Apus apus']!.isOutOfSeason, isTrue);
    expect(merged['Corvus corax']!.source, CommonnessSource.geo);
    expect(merged['Corvus corax']!.tier, ExploreTier.frequent);
  });

  test('outside Denmark (no DOF tiers) is the geo model alone', () {
    final merged = mergeCommonness(geo: {'Apus apus': geo(CommonnessBin.scarce)}, dof: null);
    expect(merged['Apus apus']!.source, CommonnessSource.geo);
  });

  test('no location: the provider is null, no exception', () async {
    final c = ProviderContainer(overrides: [
      currentLocationProvider.overrideWith((ref) async => null),
    ]);
    addTearDown(c.dispose);
    expect(await c.read(detectionCommonnessProvider.future), isNull);
  });

  test('no asset and no geo data: null, not an exception', () async {
    final c = ProviderContainer(overrides: [
      currentLocationProvider.overrideWith((ref) async => const AppLocation(latitude: 55.68, longitude: 12.57)),
      dkCommonnessProvider.overrideWith((ref) async => null),
      geoCommonnessProvider.overrideWith((ref) async => null),
    ]);
    addTearDown(c.dispose);
    expect(await c.read(detectionCommonnessProvider.future), isNull);
  });

  test('offshore (outside every cell) falls back to the geo model', () async {
    final c = ProviderContainer(overrides: [
      currentLocationProvider.overrideWith((ref) async => const AppLocation(latitude: 48.85, longitude: 2.35)),
      dkCommonnessProvider.overrideWith((ref) async => null),
      geoCommonnessProvider.overrideWith((ref) async => {'Apus apus': geo(CommonnessBin.common)}),
    ]);
    addTearDown(c.dispose);
    final m = await c.read(detectionCommonnessProvider.future);
    expect(m!['Apus apus']!.source, CommonnessSource.geo);
  });
}
```

Before running, check `AppLocation`'s constructor in `lib/core/services/location_service.dart:105`; if it is not `const`, drop `const` in the test.

- [ ] **Step 2: Run test to verify it fails**

Run: `flutter test test/features/inference/commonness_providers_test.dart`
Expected: FAIL, `commonness_providers.dart` not found.

- [ ] **Step 3: Add the preference key and the setting provider**

In `lib/core/constants/app_constants.dart`, class `PrefKeys`, after `static const String showSciNames = 'show_sci_names';`:

```dart
  static const String showCommonness = 'show_commonness';
```

In `lib/shared/providers/settings_providers.dart`, after `showSciNamesProvider`:

```dart
/// Show the commonness chip and rare-bird warning on detections (default true).
final showCommonnessProvider =
    StateNotifierProvider<BoolSettingNotifier, bool>((ref) {
      final prefs = ref.watch(sharedPreferencesProvider);
      return BoolSettingNotifier(prefs, PrefKeys.showCommonness, true);
    });
```

- [ ] **Step 4: Write the providers**

`lib/features/inference/commonness_providers.dart`:

```dart
// =============================================================================
// Commonness of detected species, for the detection chip and announcements
// =============================================================================
//
// Inside Denmark the tier comes from the DOF prior (DkCommonness); elsewhere,
// and for species the prior lacks, from the geo model (geoCommonnessProvider).
// The out-of-season flag always comes from the geo model.
// =============================================================================

import 'dart:async';

import 'package:flutter/foundation.dart' show debugPrint;
import 'package:flutter/services.dart' show rootBundle;
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../announcements/domain/announcement_signals.dart';
import '../announcements/geo_commonness_provider.dart';
import '../explore/explore_providers.dart';
import 'dk_commonness.dart';
import 'geo_abundance.dart';

enum CommonnessSource { dof, geo }

class DetectionCommonness {
  const DetectionCommonness({
    required this.tier,
    required this.source,
    this.isOutOfSeason = false,
  });

  final ExploreTier tier;
  final CommonnessSource source;
  final bool isOutOfSeason;
}

/// CommonnessBin and ExploreTier list the same six levels in the same order.
ExploreTier tierFromBin(CommonnessBin bin) => ExploreTier.values[bin.index];
CommonnessBin binFromTier(ExploreTier tier) => CommonnessBin.values[tier.index];

Map<String, DetectionCommonness> mergeCommonness({
  Map<String, GeoCommonnessEntry>? geo,
  Map<String, ExploreTier>? dof,
}) {
  final out = <String, DetectionCommonness>{};
  geo?.forEach((name, e) {
    out[name] = DetectionCommonness(
      tier: tierFromBin(e.commonness),
      source: CommonnessSource.geo,
      isOutOfSeason: e.isOutOfSeason,
    );
  });
  dof?.forEach((name, tier) {
    out[name] = DetectionCommonness(
      tier: tier,
      source: CommonnessSource.dof,
      isOutOfSeason: geo?[name]?.isOutOfSeason ?? false,
    );
  });
  return out;
}

/// The prior's week now; rebuilt at the next Monday 00:00.
final priorWeekProvider = Provider<int>((ref) {
  final now = DateTime.now();
  final midnight = DateTime(now.year, now.month, now.day);
  final nextMonday = midnight.add(Duration(days: 8 - now.weekday));
  final timer = Timer(nextMonday.difference(now), ref.invalidateSelf);
  ref.onDispose(timer.cancel);
  return priorWeek(now);
});

/// The DOF asset, or null when it is missing or unreadable (logged once).
final dkCommonnessProvider = FutureProvider<DkCommonness?>((ref) async {
  try {
    return await DkCommonness.load(rootBundle);
  } catch (e) {
    debugPrint('DkCommonness unavailable, using the geo model only: $e');
    return null;
  }
});

/// scientificName -> commonness at the current location and week, or null
/// when there is no location or no data at all.
final detectionCommonnessProvider =
    FutureProvider<Map<String, DetectionCommonness>?>((ref) async {
      final location = await ref.watch(currentLocationProvider.future);
      if (location == null) return null;
      final week = ref.watch(priorWeekProvider);
      final dk = await ref.watch(dkCommonnessProvider.future);
      Map<String, GeoCommonnessEntry>? geo;
      try {
        geo = await ref.watch(geoCommonnessProvider.future);
      } catch (e) {
        geo = null;
      }
      final dof = dk?.tiersAt(location.latitude, location.longitude, week);
      if (geo == null && dof == null) return null;
      return mergeCommonness(geo: geo, dof: dof);
    });
```

- [ ] **Step 5: Register and commit the asset**

In `pubspec.yaml`, under `flutter: assets:`, after `- assets/announcements/` add `- assets/prior/`. Then:

Run: `flutter test test/features/inference/commonness_providers_test.dart test/features/inference/dk_commonness_test.dart`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add lib/features/inference/commonness_providers.dart lib/core/constants/app_constants.dart lib/shared/providers/settings_providers.dart pubspec.yaml assets/prior test/features/inference/commonness_providers_test.dart
git commit -m "feat(commonness): detection commonness provider (DOF in Denmark, geo elsewhere), setting, asset

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Announcements read the same commonness (fork)

**Files:**
- Modify: `lib/features/announcements/announcements_alert_sink.dart:48` (import) and `:145-170` (`_enrichWithCommonness`)
- Test: `test/features/announcements/announcements_commonness_source_test.dart`

**Interfaces:**
- Consumes: `detectionCommonnessProvider`, `binFromTier` (Task 6).
- Produces: `List<AnnouncementDetection> enrichWithCommonness(List<AnnouncementDetection> batch, Map<String, DetectionCommonness>? map)` (top-level, testable); `_enrichWithCommonness` delegates to it.

- [ ] **Step 1: Write the failing test**

```dart
import 'package:birdnet_live/features/announcements/announcements_alert_sink.dart';
import 'package:birdnet_live/features/announcements/announcements_controller.dart';
import 'package:birdnet_live/features/announcements/domain/announcement_signals.dart';
import 'package:birdnet_live/features/inference/commonness_providers.dart';
import 'package:birdnet_live/features/inference/geo_abundance.dart';
import 'package:flutter_test/flutter_test.dart';

AnnouncementDetection det(String id, {CommonnessBin? bin}) => AnnouncementDetection(
  speciesId: id, displayName: id, score: 0.9, at: DateTime(2026, 5, 20), commonness: bin,
);

void main() {
  test('the spoken commonness is the chip commonness', () {
    final out = enrichWithCommonness(
      [det('Apus apus'), det('Corvus corax'), det('Pica pica', bin: CommonnessBin.common)],
      {
        'Apus apus': const DetectionCommonness(tier: ExploreTier.rare, source: CommonnessSource.dof, isOutOfSeason: true),
        'Pica pica': const DetectionCommonness(tier: ExploreTier.abundant, source: CommonnessSource.dof),
      },
    );
    expect(out[0].commonness, CommonnessBin.rare);
    expect(out[0].isOutOfSeason, isTrue);
    expect(out[1].commonness, isNull);               // unknown species untouched
    expect(out[2].commonness, CommonnessBin.common); // pre-set value kept
  });

  test('no map: batch unchanged', () {
    final batch = [det('Apus apus')];
    expect(enrichWithCommonness(batch, null), same(batch));
  });
}
```

Check `AnnouncementDetection`'s constructor fields in `announcements_controller.dart:60-90` first; adjust the `det` helper to its required parameters.

- [ ] **Step 2: Run test to verify it fails**

Run: `flutter test test/features/announcements/announcements_commonness_source_test.dart`
Expected: FAIL, `enrichWithCommonness` not defined.

- [ ] **Step 3: Implement**

In `announcements_alert_sink.dart`, replace `import 'geo_commonness_provider.dart';` with `import '../inference/commonness_providers.dart';`, add above the class:

```dart
/// Attach commonness/season to each detection from [map] (the same commonness
/// the detection chip shows). Detections that already carry one are kept.
List<AnnouncementDetection> enrichWithCommonness(
  List<AnnouncementDetection> batch,
  Map<String, DetectionCommonness>? map,
) {
  if (map == null || map.isEmpty) return batch;
  return [
    for (final d in batch)
      if (d.commonness != null || map[d.speciesId] == null)
        d
      else
        AnnouncementDetection(
          speciesId: d.speciesId,
          displayName: d.displayName,
          score: d.score,
          at: d.at,
          commonness: binFromTier(map[d.speciesId]!.tier),
          isOutOfSeason: map[d.speciesId]!.isOutOfSeason,
        ),
  ];
}
```

and replace the body of `_enrichWithCommonness` with:

```dart
    return enrichWithCommonness(batch, _ref.read(detectionCommonnessProvider).value);
```

Update its doc comment to name `detectionCommonnessProvider` instead of `geoCommonnessProvider`.

- [ ] **Step 4: Run tests**

Run: `flutter test test/features/announcements/`
Expected: all PASS (existing announcement tests included).

- [ ] **Step 5: Commit**

```bash
git add lib/features/announcements/announcements_alert_sink.dart test/features/announcements/announcements_commonness_source_test.dart
git commit -m "feat(commonness): announcements speak the same commonness the chip shows

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Chip, rare warning, source sheet, setting tile, credit (fork)

**Files:**
- Create: `lib/features/live/widgets/commonness_badge.dart`
- Modify: `lib/features/live/widgets/detection_list_widget.dart` (`DetectionTile.build`, name row ~line 317-350 and after the scientific-name row)
- Modify: `lib/features/settings/settings_screen.dart` (after the species-filter `_ChoiceTile`, ~line 734)
- Modify: `lib/features/about/about_screen.dart` (after `Text(l10n.aboutCreditsDescription)`, line 169)
- Modify: `lib/l10n/app_en.arb` (new keys), then `flutter gen-l10n`
- Test: `test/features/live/commonness_badge_test.dart`

**Interfaces:**
- Consumes: `detectionCommonnessProvider`, `showCommonnessProvider`, `DetectionCommonness`, `CommonnessSource` (Task 6); `exploreTierColor`, `exploreTierLabel` (`lib/features/explore/explore_tier.dart`); `ScoreColors.of(context)`.
- Produces: `class CommonnessChip extends StatelessWidget` (`entry`), `class RareHereNote extends StatelessWidget` (`confidence`), `String rareHereText(AppLocalizations l10n, double confidence)`.

- [ ] **Step 1: Add the strings to `lib/l10n/app_en.arb`** (before the final `}`)

```json
  "commonnessRareCheck": "Rare here this week. Double-check the ID.",
  "@commonnessRareCheck": {"description": "Warning under a detection of a species that is rare at this place and week, confidence below 0.8"},
  "commonnessRareReport": "Rare here this week. Worth reporting to DOFbasen if confirmed.",
  "@commonnessRareReport": {"description": "Warning under a confident (>= 0.8) detection of a species that is rare here this week"},
  "commonnessHereThisWeek": "{tier} here this week",
  "@commonnessHereThisWeek": {"description": "Screen-reader label of the commonness chip", "placeholders": {"tier": {"type": "String"}}},
  "commonnessSheetTitle": "How common here?",
  "@commonnessSheetTitle": {"description": "Title of the sheet explaining the commonness chip"},
  "commonnessSourceDof": "Based on DOFbasen records for this area and time of year (GBIF 10.15468/dl.3j4czw, CC BY 4.0).",
  "@commonnessSourceDof": {"description": "Source of the commonness tier inside Denmark"},
  "commonnessSourceGeo": "Based on BirdNET's range model.",
  "@commonnessSourceGeo": {"description": "Source of the commonness tier outside Denmark"},
  "settingsShowCommonness": "Show how common birds are",
  "@settingsShowCommonness": {"description": "Setting: commonness chip and rare warning on detections"},
  "settingsShowCommonnessDescription": "A chip on each detection, and a warning when a bird is rare here this week",
  "@settingsShowCommonnessDescription": {"description": "Subtitle of the show-commonness setting"},
  "aboutDofCredit": "Commonness in Denmark is derived from DOFbasen records (GBIF occurrence download 10.15468/dl.3j4czw), CC BY 4.0.",
  "@aboutDofCredit": {"description": "Credit line for the DOF-derived commonness data"}
```

Run: `flutter gen-l10n`
Expected: regenerates `lib/l10n/app_localizations*.dart`; untranslated-message notes for other locales are fine (they fall back to English).

- [ ] **Step 2: Write the failing widget test**

```dart
import 'package:birdnet_live/features/inference/commonness_providers.dart';
import 'package:birdnet_live/features/inference/geo_abundance.dart';
import 'package:birdnet_live/features/live/live_session.dart';
import 'package:birdnet_live/features/live/widgets/detection_list_widget.dart';
import 'package:birdnet_live/l10n/app_localizations.dart';
import 'package:birdnet_live/shared/providers/app_providers.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';

void main() {
  late SharedPreferences prefs;
  setUp(() async {
    SharedPreferences.setMockInitialValues({});
    prefs = await SharedPreferences.getInstance();
  });

  DetectionRecord rec(String name, double conf, {DetectionSource source = DetectionSource.auto}) =>
      DetectionRecord(scientificName: name, commonName: name, confidence: conf,
          timestamp: DateTime(2026, 5, 20, 10), source: source);

  Future<void> pump(WidgetTester t, List<DetectionRecord> dets,
      Map<String, DetectionCommonness>? map, {bool show = true}) async {
    await prefs.setBool('show_commonness', show);
    await t.pumpWidget(ProviderScope(
      overrides: [
        sharedPreferencesProvider.overrideWithValue(prefs),
        detectionCommonnessProvider.overrideWith((ref) async => map),
      ],
      child: MaterialApp(
        localizationsDelegates: AppLocalizations.localizationsDelegates,
        supportedLocales: AppLocalizations.supportedLocales,
        home: Scaffold(body: DetectionList(detections: dets, isActive: false, showTips: false)),
      ),
    ));
    await t.pumpAndSettle();
  }

  const dof = CommonnessSource.dof;
  final map = {
    'Turdus merula': const DetectionCommonness(tier: ExploreTier.common, source: dof),
    'Tarsiger cyanurus': const DetectionCommonness(tier: ExploreTier.rare, source: dof),
    'Apus apus': const DetectionCommonness(tier: ExploreTier.scarce, source: dof),
  };

  testWidgets('chip shows the tier name', (t) async {
    await pump(t, [rec('Turdus merula', 0.9)], map);
    expect(find.text('Common'), findsOneWidget);
    expect(find.textContaining('Rare here'), findsNothing);
  });

  testWidgets('rare, low confidence: double-check', (t) async {
    await pump(t, [rec('Tarsiger cyanurus', 0.5)], map);
    expect(find.text('Rare here this week. Double-check the ID.'), findsOneWidget);
  });

  testWidgets('rare, confident: worth reporting', (t) async {
    await pump(t, [rec('Tarsiger cyanurus', 0.85)], map);
    expect(find.text('Rare here this week. Worth reporting to DOFbasen if confirmed.'), findsOneWidget);
  });

  testWidgets('rare manual entry: worth reporting, never double-check', (t) async {
    await pump(t, [rec('Tarsiger cyanurus', 1.0, source: DetectionSource.manual)], map);
    expect(find.textContaining('Worth reporting'), findsOneWidget);
    expect(find.textContaining('Double-check'), findsNothing);
  });

  testWidgets('scarce: chip only, no warning', (t) async {
    await pump(t, [rec('Apus apus', 0.9)], map);
    expect(find.text('Scarce'), findsOneWidget);
    expect(find.textContaining('Rare here'), findsNothing);
  });

  testWidgets('setting off hides chip and warning', (t) async {
    await pump(t, [rec('Tarsiger cyanurus', 0.5)], map, show: false);
    expect(find.text('Rare'), findsNothing);
    expect(find.textContaining('Rare here'), findsNothing);
  });

  testWidgets('no commonness data: nothing shown, no error', (t) async {
    await pump(t, [rec('Turdus merula', 0.9)], null);
    expect(find.text('Common'), findsNothing);
  });

  testWidgets('tapping the chip names the source', (t) async {
    await pump(t, [rec('Turdus merula', 0.9)], map);
    await t.tap(find.text('Common'));
    await t.pumpAndSettle();
    expect(find.textContaining('DOFbasen records'), findsOneWidget);
  });
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `flutter test test/features/live/commonness_badge_test.dart`
Expected: FAIL (no chip text found).

- [ ] **Step 4: Write the widgets**

`lib/features/live/widgets/commonness_badge.dart`:

```dart
// Commonness chip and rare-bird warning on detection rows.

import 'package:flutter/material.dart';

import '../../../core/theme/score_colors.dart';
import '../../../l10n/app_localizations.dart';
import '../../explore/explore_tier.dart';
import '../../inference/commonness_providers.dart';

const double kRareReportConfidence = 0.8;

String rareHereText(AppLocalizations l10n, double confidence) =>
    confidence >= kRareReportConfidence ? l10n.commonnessRareReport : l10n.commonnessRareCheck;

class CommonnessChip extends StatelessWidget {
  const CommonnessChip({super.key, required this.entry});

  final DetectionCommonness entry;

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    final label = exploreTierLabel(l10n, entry.tier);
    final color = exploreTierColor(ScoreColors.of(context), entry.tier);
    return Semantics(
      button: true,
      label: l10n.commonnessHereThisWeek(label),
      excludeSemantics: true,
      child: InkWell(
        borderRadius: BorderRadius.circular(10),
        onTap: () => showModalBottomSheet<void>(
          context: context,
          builder: (_) => Padding(
            padding: const EdgeInsets.fromLTRB(20, 16, 20, 32),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(l10n.commonnessSheetTitle, style: theme.textTheme.titleMedium),
                const SizedBox(height: 8),
                Text(l10n.commonnessHereThisWeek(label)),
                const SizedBox(height: 8),
                Text(entry.source == CommonnessSource.dof
                    ? l10n.commonnessSourceDof
                    : l10n.commonnessSourceGeo),
              ],
            ),
          ),
        ),
        child: Container(
          padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(10),
            border: Border.all(color: color.withAlpha(160)),
          ),
          child: Row(
            mainAxisSize: MainAxisSize.min,
            children: [
              Container(width: 8, height: 8, decoration: BoxDecoration(color: color, shape: BoxShape.circle)),
              const SizedBox(width: 4),
              Text(label, style: theme.textTheme.labelSmall),
            ],
          ),
        ),
      ),
    );
  }
}

class RareHereNote extends StatelessWidget {
  const RareHereNote({super.key, required this.confidence});

  final double confidence;

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    final text = rareHereText(l10n, confidence);
    return Padding(
      padding: const EdgeInsets.only(top: 2),
      child: Semantics(
        label: text,
        excludeSemantics: true,
        child: Row(
          children: [
            Icon(Icons.warning_amber_rounded, size: 14, color: theme.colorScheme.error),
            const SizedBox(width: 4),
            Expanded(
              child: Text(text, style: theme.textTheme.bodySmall?.copyWith(color: theme.colorScheme.error)),
            ),
          ],
        ),
      ),
    );
  }
}
```

- [ ] **Step 5: Wire into `DetectionTile.build`**

In `detection_list_widget.dart`, add imports:

```dart
import '../../../shared/providers/settings_providers.dart';
import '../../inference/commonness_providers.dart';
import '../../inference/geo_abundance.dart';
import 'commonness_badge.dart';
```

(skip any already present). After `final l10n = AppLocalizations.of(context)!;` in `DetectionTile.build`:

```dart
    final commonness = ref.watch(showCommonnessProvider)
        ? ref.watch(detectionCommonnessProvider).value?[detection.scientificName]
        : null;
```

In the name `Row`'s children, after the `if (detectionCount != null && detectionCount! > 1) Padding(...)` element:

```dart
                        if (commonness != null)
                          Padding(
                            padding: const EdgeInsets.only(left: 6),
                            child: CommonnessChip(entry: commonness),
                          ),
```

In the outer `Column`'s children, immediately after the scientific-name/confidence `Row(...)`:

```dart
                    if (commonness?.tier == ExploreTier.rare)
                      RareHereNote(
                        confidence: detection.source == DetectionSource.auto
                            ? detection.confidence
                            : 1.0,
                      ),
```

Check `_DetectionTile` row height helper (`static double ... textHeight` around line 260-272): if it computes a fixed row height, add the note's line height (`bodySmall` line height + 2) when the tier is rare, or the list clips the note. Read the helper and adjust it in the same step.

- [ ] **Step 6: Setting tile and credit**

In `settings_screen.dart`, directly after the species-filter `_ChoiceTile<String>(...)` (ends `ref.read(speciesFilterModeProvider.notifier).set(v),\n              ),`):

```dart
              SwitchListTile(
                title: Text(l10n.settingsShowCommonness),
                subtitle: Text(l10n.settingsShowCommonnessDescription),
                value: ref.watch(showCommonnessProvider),
                onChanged: (v) =>
                    ref.read(showCommonnessProvider.notifier).set(v),
              ),
```

In `about_screen.dart`, after `Text(l10n.aboutCreditsDescription),`:

```dart
                    const SizedBox(height: 8),
                    Text(l10n.aboutDofCredit),
```

- [ ] **Step 7: Run tests**

Run: `flutter test test/features/live/ test/features/inference/ test/features/announcements/`
Expected: all PASS, including the existing `detection_list_widget_test.dart`.

Run: `flutter analyze lib/features/live lib/features/inference lib/features/settings lib/features/about`
Expected: no new errors or warnings.

- [ ] **Step 8: Commit**

```bash
git add lib/features/live/widgets/commonness_badge.dart lib/features/live/widgets/detection_list_widget.dart lib/features/settings/settings_screen.dart lib/features/about/about_screen.dart lib/l10n test/features/live/commonness_badge_test.dart
git commit -m "feat(commonness): tier chip and rare warning on detections, source sheet, setting, credit

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: On the iPhone (fork)

**Files:**
- Create: `integration_test/commonness_test.dart`

**Interfaces:**
- Consumes: the real asset (Task 4/6), `DkCommonness.load(rootBundle)`, the widgets (Task 8).

- [ ] **Step 1: Write the integration test**

```dart
// Fugle DK on device: the bundled DOF commonness asset loads and answers for
// Copenhagen as the export's checks promise.
import 'package:birdnet_live/features/inference/dk_commonness.dart';
import 'package:birdnet_live/features/inference/geo_abundance.dart';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('bundled asset: Copenhagen blackbird common, winter swift rare', (t) async {
    final dk = await DkCommonness.load(rootBundle);
    expect(dk.species.length, greaterThan(150));
    final may = dk.tiersAt(55.68, 12.57, 20)!;
    expect([ExploreTier.common, ExploreTier.abundant], contains(may['Turdus merula']));
    expect(dk.tiersAt(55.68, 12.57, 3)!['Apus apus'], ExploreTier.rare);
    expect(dk.tiersAt(48.85, 2.35, 20), isNull); // Paris: geo fallback
  });
}
```

- [ ] **Step 2: Run on the simulator, then the iPhone**

Run: `flutter test integration_test/commonness_test.dart -d "iPhone 18 Pro"` (simulator; create it in Xcode if missing)
Expected: PASS.
With the phone plugged in and unlocked: `flutter test integration_test/commonness_test.dart -d 00008130-001221DA247A8D3A`
Expected: PASS.

- [ ] **Step 3: Manual look and reinstall**

In Settings set a manual location in Copenhagen (55.68, 12.57), open Live, play a blackbird recording from the speaker: the row shows "Common" or "Abundant". Then reinstall:

```bash
cd ios && xcodebuild -workspace Runner.xcworkspace -scheme Runner -configuration Release -destination id=00008130-001221DA247A8D3A -allowProvisioningUpdates -allowProvisioningDeviceRegistration build && xcrun devicectl device install app --device 00008130-001221DA247A8D3A ~/Library/Developer/Xcode/DerivedData/Runner-*/Build/Products/Release-iphoneos/Runner.app
```

- [ ] **Step 4: Commit**

```bash
git add integration_test/commonness_test.dart
git commit -m "test(commonness): on-device check of the bundled DOF asset

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
