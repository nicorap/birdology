"""I2: how much of a KNOWN signal survives the shrinkage and smoothing constants.

WHY THIS FILE EXISTS
--------------------
`BackoffParams.cell_week_weight = 10.0`, `cell_weight = 50.0`,
`SmoothingParams.space_weight = 0.3` and `time_weight = 0.3` are UNTUNED. They
were never swept, and they -- not the occurrence data -- set the size of the
seasonal effect the paper will report. A build at `cell_week_weight = 0.5`
retains about twice the seasonal amplitude of one at the default, on the same
records; a build at 50.0 retains about a third of it. Nothing raises, no value
leaves [0, 1], and the recovered PHASE is right in every case, so the flattening
is invisible in the artifact.

So this file generates an archive with a KNOWN seasonal and spatial signal,
builds it through `build_from_archive` -- the CLI's own entry point, not a
hand-assembled tensor -- and asserts a FLOOR on how much of that signal reaches
the `.npz`. A future edit to the four constants that flattens the artifact
fails here instead of shipping.

EVERY NUMBER BELOW CAME OUT OF `build_from_archive`
---------------------------------------------------
Measured on THIS fixture (260 sites, 8 species, ~38 000 records, seed
`PROBE_SEED`), over TWELVE independent seeds of the same generator, at the
CLI's default resolutions:

    res   cells   median cell-week effort   seasonal retention   spatial retention
    ---   -----   -----------------------   ------------------   -----------------
    3     ~17     20-49 records             0.557 - 0.728        0.831 - 0.892
    4     ~70     7-8 records               0.411 - 0.457        0.884 - 0.905
    5     ~180    3 records                 0.249 - 0.287        0.796 - 0.834

and on a MUCH larger build of the same generator (1500 sites, 12 species,
167 879 records, `.superpowers/.../i2_probe.py`), which is the table quoted in
`BackoffParams`:

    res 3   21 cells    median effort 102   seasonal 0.798   spatial 0.898
    res 4   98 cells    median effort  31   seasonal 0.679   spatial 0.947
    res 5   461 cells   median effort   6   seasonal 0.426   spatial 0.887
    res 6   1171 cells  median effort   2   seasonal 0.298   spatial 0.811

SEASONAL AMPLITUDE AND SPATIAL CONTRAST DO NOT DECAY THE SAME WAY, AND THAT IS
THE FINDING THIS FILE ADDS
------------------------------------------------------------------------------
Seasonal retention falls MONOTONICALLY with resolution -- 0.80 / 0.68 / 0.43 /
0.30 -- because a finer grid divides the same records into more cell-weeks, and
`cell_week_weight = 10` is a fixed pseudo-count measured in records: at res 5
the median cell-week holds 6 records against 10 pseudo-counts, so the coarse
level outvotes the data.

Spatial contrast does NOT: 0.898 / 0.947 / 0.887 / 0.811, high everywhere and
NOT monotone. The two shrinkages are not symmetric, and the reason is
structural rather than a property of this fixture:

* the CELL level is a sum over all 52 of a cell's weeks, so `cell_weight = 50`
  is 50 pseudo-counts against a denominator ~52x larger than the one
  `cell_week_weight` faces. Swept directly at res 5: `cell_weight` 5 / 50 / 500
  gives spatial retention 0.987 / 0.887 / 0.610 -- it does attenuate space, but
  the default sits at the flat end of its own curve while `cell_week_weight`'s
  default sits on the steep part of its.
* `space_weight` barely matters: 0.0 / 0.3 / 1.0 gives spatial retention
  0.917 / 0.887 / 0.881 at res 5. Six neighbours of a cell in a clumped
  observer field mostly resemble it. `time_weight` 0.0 / 0.3 / 1.0 costs
  seasonal retention 0.466 / 0.426 / 0.413 -- real, and an order of magnitude
  smaller than what `cell_week_weight` costs.

So the headline is `cell_week_weight` against SEASON. Spatial contrast is
asserted here anyway, with its own floor, because nobody had measured it and
"it does not decay" is a claim that needs a regression test as much as the one
that does.

WHY FLOORS AND NOT POINT ESTIMATES
-----------------------------------
The retentions above move by ~0.17 (res 3) across seeds of the SAME generator
at the SAME parameters. An equality or a tight band would fail on an innocent
change to the fixture, to H3, or to the random draw, and a threshold that fires
on a correct build gets deleted rather than fixed. The floors are therefore set
about 1.6x below the WORST of twelve seeds.

A floor with margin is only worth having if something realistic falls below it,
and that is not asserted here as a belief: `test_the_retention_floors_are_
DISCRIMINATING_not_merely_satisfied` rebuilds the SAME archive with
`cell_week_weight = 50.0` and asserts the result lands UNDER these floors. The
floors are proven to separate two real builds, in the same run, rather than
being a number that happens to be small enough.
"""
from __future__ import annotations

import math
import random
import sys
import zipfile
from collections import defaultdict
from datetime import date
from pathlib import Path

import h3
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.backoff import BackoffParams
from birdology.prior.indexing import (
    WEEKS_PER_YEAR,
    aggregate_counts,
    assign_cells,
)
from birdology.prior.occurrences import read_occurrences
from birdology.prior.smoothing import SmoothingParams
from birdology.prior.tensor import load_prior

from build_prior import build_from_archive

HEADER = "\t".join(
    ["gbifID", "datasetKey", "publishingOrgKey", "speciesKey", "species",
     "decimalLatitude", "decimalLongitude", "eventDate", "recordedBy"]
)

DOI = "10.15468/dl.amplitude"

#: Eight Danish population centres. Sites cluster around them rather than
#: filling a rectangle, because real observer effort is clumped and a uniform
#: site field would give every cell the same effort -- which is exactly the
#: regime in which `cell_week_weight` does the least damage, so a uniform
#: fixture would understate the effect this file measures.
CENTRES = [
    (55.68, 12.57), (55.40, 10.39), (56.16, 10.20), (57.05, 9.92),
    (55.47, 8.45), (54.91, 9.79), (56.46, 9.00), (55.23, 11.76),
]

PROBE_SITES = 260
PROBE_SEED = 20260929

#: Records per site-week-species before the seasonal and regional multipliers.
#: Tuned so the median cell-week effort at res 4 is single-digit -- the regime
#: the reviewer measured on real GBIF data (98 / 31 / 8 / 2 records at res
#: 3/4/5/6) -- rather than the dense regime in which nothing shrinks.
#:
#: NOT raised further. Above ~0.45 some seed concentrates a species' mass into
#: one cell AND one week, `separable_prior` then refuses the build outright
#: (its outer product exceeds 1.0), and the fixture would fail for a reason
#: that has nothing to do with amplitude.
PROBE_SCALE = 0.35

#: von-Mises concentration of the seasonal species. 3.0 gives a peak/trough
#: intensity ratio of e^6 ~ 403, which is a strong migratory signal and still
#: leaves every week populated. 6.0 (ratio ~1.6e5) empties the off-season
#: entirely and trips `separable_prior`'s probability bound on some seeds.
SEASON_STRENGTH = 3.0


def _season(peak: int):
    def f(week: int) -> float:
        theta = 2.0 * math.pi * (week - peak) / WEEKS_PER_YEAR
        return math.exp(SEASON_STRENGTH * (math.cos(theta) - 1.0))
    return f


def _flat(_week: int) -> float:
    return 1.0


#: (species_key, weekly multiplier, longitude exponent). The exponent makes
#: `exp(k * (lon - 10.5))` a west/east preference; 0.0 is spatially flat.
SPECIES = [
    (1001, _season(8), 0.0),    # winter
    (1002, _season(21), 0.0),   # spring
    (1003, _season(34), 0.0),   # late summer
    (1004, _season(47), 0.0),   # late autumn
    (2001, _flat, 1.1),         # eastern
    (2002, _flat, -1.1),        # western
    (3001, _season(26), 0.9),   # seasonal AND regional
    (4001, _flat, 0.0),         # resident: neither
]

SEASONAL_KEYS = (1001, 1002, 1003, 1004, 3001)
SPATIAL_KEYS = (2001, 2002, 3001)
#: Neither seasonal nor regional. The prior must not INVENT structure for it,
#: which is the other half of "retention is measured, not assumed".
FLAT_KEY = 4001

_INDEX = {key: i for i, (key, _s, _r) in enumerate(SPECIES)}

#: Floors on the fraction of the true signal that must reach the artifact, by
#: H3 resolution. Set ~1.6x below the WORST of twelve seeds; see the module
#: docstring for the measurements and for why a floor rather than a band.
SEASONAL_RETENTION_FLOOR = {3: 0.35, 4: 0.25, 5: 0.15}
SPATIAL_RETENTION_FLOOR = {3: 0.55, 4: 0.55, 5: 0.50}

#: What the CLI sweeps by default (`--resolutions` default `[3, 4, 5]`), which
#: is what "at each default resolution" means.
DEFAULT_RESOLUTIONS = (3, 4, 5)


def probe_sites(seed: int = PROBE_SEED) -> list[tuple[float, float]]:
    rng = random.Random(seed)
    return [
        (
            CENTRES[i % len(CENTRES)][0] + rng.gauss(0.0, 0.35),
            CENTRES[i % len(CENTRES)][1] + rng.gauss(0.0, 0.55),
        )
        for i in range(PROBE_SITES)
    ]


def generating_intensity(species_index: int, lon: float, week: int) -> float:
    """The rate the archive is drawn from. The SINGLE definition of the truth.

    Read by `build_probe_archive` (which samples it) and by `truth_grid`
    (which integrates it). Two definitions would let the "true" amplitude drift
    away from the one the records were actually drawn from, and the retention
    ratio would then measure the drift.
    """
    _key, season, region = SPECIES[species_index]
    return PROBE_SCALE * season(week) * math.exp(region * (lon - 10.5))


def _poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0.0:
        return 0
    limit = math.exp(-lam)
    k, prod = 0, rng.random()
    while prod > limit:
        k += 1
        prod *= rng.random()
    return k


def build_probe_archive(path: Path, sites, seed: int = PROBE_SEED) -> int:
    """A GBIF SIMPLE_CSV archive of Poisson draws from `generating_intensity`.

    Poisson rather than the expected count rounded: the shrinkage is a response
    to thin evidence, and a fixture with no sampling noise would hand the
    backoff a cleaner cell-week than any real archive contains.
    """
    rng = random.Random(seed + 7)
    dates = {
        w: date.fromisocalendar(2025, w, 3).isoformat()
        for w in range(1, WEEKS_PER_YEAR + 1)
    }
    rows = []
    gid = 0
    for si, (lat, lon) in enumerate(sites):
        for week in range(1, WEEKS_PER_YEAR + 1):
            when = dates[week]
            for idx, (key, _s, _r) in enumerate(SPECIES):
                for _ in range(_poisson(rng, generating_intensity(idx, lon, week))):
                    gid += 1
                    rows.append("\t".join([
                        str(gid), "ds", "pub", str(key), f"sp{key}",
                        f"{lat:.6f}", f"{lon:.6f}", when, f"obs{si % 30}",
                    ]))
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("occurrence.txt", "\n".join([HEADER, *rows]) + "\n")
    return gid


def truth_grid(sites, resolution: int, cells: list[str]) -> np.ndarray:
    """(n_species, n_cells, 52) of the TRUE share the pipeline estimates.

    The pipeline's `p` is P(species | a bird was recorded here this week) -- a
    SHARE of the cell-week's records, not an abundance. So the truth is the
    generating intensity normalised over species in the same cell-week, summed
    over the sites that fall in the cell. Comparing the artifact against a raw
    abundance instead would report the normalisation as lost amplitude.
    """
    per_cell = defaultdict(list)
    for lat, lon in sites:
        per_cell[h3.latlng_to_cell(lat, lon, resolution)].append(lon)
    lam = np.zeros((len(SPECIES), len(cells), WEEKS_PER_YEAR))
    for ci, cell in enumerate(cells):
        for lon in per_cell.get(cell, ()):
            for si in range(len(SPECIES)):
                for w in range(1, WEEKS_PER_YEAR + 1):
                    lam[si, ci, w - 1] += generating_intensity(si, lon, w)
    total = lam.sum(axis=0, keepdims=True)
    total[total == 0.0] = 1.0
    return lam / total


def prior_grid(tensor, cells: list[str]) -> np.ndarray:
    """(n_species, n_cells, 52) from the ARTIFACT, in `SPECIES` order."""
    dense = tensor.dense()
    keys = list(tensor.species_keys)
    grid = np.stack([dense[keys.index(key)] for key, _s, _r in SPECIES])
    grid = grid.reshape(len(SPECIES), len(tensor.cells), WEEKS_PER_YEAR)
    return grid[:, [tensor.cells.index(c) for c in cells], :]


def _cv(a: np.ndarray, axis: int) -> np.ndarray:
    """Coefficient of variation along `axis`. Scale-free ON PURPOSE.

    The prior is never renormalised, so its absolute level is not the true
    share's absolute level and a difference of MEANS would be measured as lost
    amplitude. CV divides that out: it asks how much the value swings relative
    to its own mean, which is the quantity a fusion step actually exploits.
    """
    mean = a.mean(axis=axis)
    std = a.std(axis=axis)
    out = np.zeros_like(std)
    ok = mean > 0
    out[ok] = std[ok] / mean[ok]
    return out


def seasonal_amplitude(grid: np.ndarray) -> np.ndarray:
    """Per-species weekly CV, averaged over cells."""
    return _cv(grid, axis=2).mean(axis=1)


def spatial_contrast(grid: np.ndarray) -> np.ndarray:
    """Per-species across-cell CV, averaged over weeks."""
    return _cv(grid, axis=1).mean(axis=1)


def median_cell_week_effort(archive: Path, resolution: int) -> float:
    """Records per (cell, week), the quantity the pseudo-counts compete with."""
    counts = aggregate_counts(assign_cells(read_occurrences(archive), resolution))
    per = defaultdict(int)
    for cell, week, n in zip(
        counts.column("cell").to_pylist(),
        counts.column("week").to_pylist(),
        counts.column("n").to_pylist(),
    ):
        per[(cell, week)] += n
    return float(np.median(list(per.values())))


def build_and_measure(archive: Path, sites, resolution: int, out_dir: Path,
                      **params) -> dict:
    """Build THROUGH `build_from_archive` and measure retention on the `.npz`.

    Everything asserted in this file comes back through `load_prior`, so no
    number here can come from an in-memory table the pipeline would not have
    written -- the fixture trap this branch has hit four times.
    """
    paths = build_from_archive(
        archive,
        resolution=resolution,
        source="dof",
        test_recordings=[],
        out_dir=out_dir,
        gbif_doi=DOI,
        **params,
    )
    full = load_prior(next(p for p in paths if p.name.endswith("_full.npz")))
    cells = list(full.cells)
    truth = truth_grid(sites, resolution, cells)
    prior = prior_grid(full, cells)
    return {
        "tensor": full,
        "cells": cells,
        "true_seasonal": seasonal_amplitude(truth),
        "prior_seasonal": seasonal_amplitude(prior),
        "true_spatial": spatial_contrast(truth),
        "prior_spatial": spatial_contrast(prior),
    }


def _retention(m: dict, key: int, kind: str) -> float:
    i = _INDEX[key]
    return float(m[f"prior_{kind}"][i] / m[f"true_{kind}"][i])


@pytest.fixture(scope="module")
def probe(tmp_path_factory):
    """One archive, built once, measured at every default resolution."""
    root = tmp_path_factory.mktemp("amplitude")
    sites = probe_sites()
    archive = root / "probe.zip"
    n_records = build_probe_archive(archive, sites)
    measured = {
        res: build_and_measure(archive, sites, res, root / f"r{res}")
        for res in DEFAULT_RESOLUTIONS
    }
    return {
        "root": root,
        "sites": sites,
        "archive": archive,
        "n_records": n_records,
        "measured": measured,
    }


def test_the_probe_archive_really_carries_the_signal_it_claims(probe):
    """PRECONDITION. Retention against a flat truth would be a ratio of noise.

    Asserted on the TRUTH grid, before any artifact is consulted: the seasonal
    species must have a large weekly CV and the regional ones a large
    across-cell CV, at every resolution. Without this a retention floor could
    be satisfied by a fixture with no signal in it -- the ratio of two small
    numbers is unbounded -- and the whole file would be measuring nothing.

    It also asserts the fixture is in the THIN regime the finding is about: the
    median cell-week must hold FEWER records than `cell_week_weight`'s 10
    pseudo-counts at res 4 and 5. A dense fixture would make every floor below
    pass by a mile and pin nothing.
    """
    assert probe["n_records"] > 20_000, probe["n_records"]
    for res, m in probe["measured"].items():
        for key in SEASONAL_KEYS:
            assert m["true_seasonal"][_INDEX[key]] > 0.5, (
                f"species {key} has weekly CV "
                f"{m['true_seasonal'][_INDEX[key]]:.4f} in the GENERATING "
                f"truth at res {res}; it is supposed to be strongly seasonal, "
                "and a retention ratio against a flat truth is a ratio of "
                "noise rather than a measurement"
            )
        for key in SPATIAL_KEYS:
            assert m["true_spatial"][_INDEX[key]] > 0.2, (
                f"species {key} has across-cell CV "
                f"{m['true_spatial'][_INDEX[key]]:.4f} in the GENERATING truth "
                f"at res {res}; it is supposed to be strongly regional"
            )

    effort = {
        res: median_cell_week_effort(probe["archive"], res)
        for res in DEFAULT_RESOLUTIONS
    }
    assert effort[4] < BackoffParams().cell_week_weight, (
        f"median cell-week effort at res 4 is {effort[4]}, which is NOT below "
        f"the {BackoffParams().cell_week_weight} pseudo-counts "
        "`cell_week_weight` adds. This fixture is no longer in the thin regime "
        "the finding is about, so the floors below would pass without "
        "constraining anything"
    )
    assert effort[5] <= effort[4] <= effort[3], (
        f"cell-week effort must fall as the grid gets finer, got {effort}; "
        "the monotone decay of seasonal retention is a consequence of that, "
        "so a fixture without it cannot demonstrate the finding"
    )


def test_seasonal_amplitude_survives_above_a_stated_floor_at_every_resolution(probe):
    """The artifact keeps a stated fraction of the TRUE seasonal amplitude.

    MEASURED, on `build_from_archive` output from this fixture (260 sites,
    ~38 000 records), over twelve seeds of the same generator:

        res 3   0.557 - 0.728      floor 0.35
        res 4   0.411 - 0.457      floor 0.25
        res 5   0.249 - 0.287      floor 0.15

    and on a 167 879-record build of the same generator, res 3/4/5/6:
    0.798 / 0.679 / 0.426 / 0.298.

    The floors sit ~1.6x below the worst seed, not at it. They are NOT a
    performance target: nothing here says 0.25 is enough seasonal amplitude for
    the paper, only that a build retaining less than that has had its effect
    size decided by `cell_week_weight` rather than by the data, and that such a
    change must be a deliberate, visible one.
    """
    for res, m in probe["measured"].items():
        floor = SEASONAL_RETENTION_FLOOR[res]
        for key in SEASONAL_KEYS:
            got = _retention(m, key, "seasonal")
            assert got > floor, (
                f"species {key} retains {got:.4f} of its true weekly "
                f"amplitude at res {res}, below the floor {floor}. The prior's "
                f"weekly CV is {m['prior_seasonal'][_INDEX[key]]:.4f} against "
                f"a true {m['true_seasonal'][_INDEX[key]]:.4f}. The shrinkage "
                "and smoothing constants -- not the data -- set this number: "
                "`cell_week_weight` is a fixed pseudo-count in records and the "
                "median cell-week here holds fewer records than that. If this "
                "change was deliberate, re-measure and move the floor, and put "
                "the new table in `BackoffParams`' docstring; the artifact's "
                "sidecar publishes the constants so a consumer can see it"
            )


def test_spatial_contrast_survives_above_a_stated_floor_at_every_resolution(probe):
    """Same claim for SPACE, which nobody had measured. It behaves differently.

    MEASURED on the same builds, twelve seeds:

        res 3   0.831 - 0.892     floor 0.55
        res 4   0.884 - 0.905     floor 0.55
        res 5   0.796 - 0.834     floor 0.50

    and on the 167 879-record build, res 3/4/5/6: 0.898 / 0.947 / 0.887 /
    0.811. Spatial contrast is high everywhere and NOT monotone in resolution,
    which is the opposite of what seasonal amplitude does. The asymmetry is
    structural: `cell_weight`'s denominator is a sum over all 52 weeks of a
    cell, so its 50 pseudo-counts face a denominator ~52x larger than the one
    `cell_week_weight`'s 10 face, and the default sits on the flat part of its
    own curve. Swept directly at res 5, `cell_weight` 5 / 50 / 500 gives
    spatial retention 0.987 / 0.887 / 0.610 -- so the parameter DOES attenuate
    space, just not at its current value.

    The floor is therefore lower relative to the measurement than the seasonal
    ones (~1.45x rather than ~1.6x) for a deliberate reason: it must not fire
    on the ordinary variation, but a `cell_weight` raised an order of magnitude
    should reach it.
    """
    for res, m in probe["measured"].items():
        floor = SPATIAL_RETENTION_FLOOR[res]
        for key in SPATIAL_KEYS:
            got = _retention(m, key, "spatial")
            assert got > floor, (
                f"species {key} retains {got:.4f} of its true across-cell "
                f"contrast at res {res}, below the floor {floor}. The prior's "
                f"spatial CV is {m['prior_spatial'][_INDEX[key]]:.4f} against "
                f"a true {m['true_spatial'][_INDEX[key]]:.4f}. `cell_weight` "
                "and `space_weight` act on this axis; see the docstring for "
                "the measured sweep"
            )


def test_seasonal_retention_FALLS_as_the_grid_gets_finer(probe):
    """The decay is monotone in resolution, and that is the mechanism.

    A single floor per resolution could be satisfied by an artifact whose
    retention had nothing to do with effort. This asserts the SHAPE the finding
    describes -- each finer resolution splits the same records into more
    cell-weeks, so the fixed pseudo-count wins more often -- using three
    numbers from the same run rather than a constant.

    Compared with a margin, because two adjacent resolutions on a small fixture
    can land close together and an exact `>` would be a coin flip. Measured
    gaps on this fixture are ~0.2 (res 3 -> 4) and ~0.17 (res 4 -> 5).
    """
    for key in SEASONAL_KEYS:
        by_res = {
            res: _retention(probe["measured"][res], key, "seasonal")
            for res in DEFAULT_RESOLUTIONS
        }
        assert by_res[3] > by_res[4] + 0.05, (key, by_res)
        assert by_res[4] > by_res[5] + 0.05, (key, by_res)


def test_the_prior_does_not_MANUFACTURE_structure_for_a_flat_species(probe):
    """The other half of retention: a flat species must stay flat.

    Retention below 1.0 is only meaningful if the pipeline is not free to
    invent amplitude, and a smoother that leaked structure across species would
    raise every retention ratio while making the artifact worse. Species 4001
    is drawn with no seasonal and no regional term.

    IT IS NOT ASSERTED AGAINST ZERO, and the reason is measured rather than
    assumed. Its weekly CV in the artifact is ~0.15-0.18, because a cell-week
    holding 3 records has real Poisson jitter in it and the prior reports that
    jitter honestly. A test demanding ~0 would fail on a correct build.

    What separates jitter from invention is the SHAPE. Across six seeds the
    flat species' weekly CV is essentially CONSTANT in resolution -- 0.151 /
    0.161 / 0.169 at res 3/4/5 on the default seed, worst ratio over six seeds
    1.155 -- while the genuinely seasonal species' collapses from ~0.87 to
    ~0.34 over the same range. Manufactured seasonality would GROW as the
    evidence thinned, because that is when the coarse levels and the smoother
    dominate. It does not.

    Both halves are asserted: the flat species stays well under the weakest
    real signal in the SAME artifact (measured ratios 0.16-0.22 / 0.28-0.32 /
    0.49-0.52 at res 3/4/5), and its absolute level does not climb with
    resolution.
    """
    flat_by_res = {}
    for res, m in probe["measured"].items():
        flat_weekly = float(m["prior_seasonal"][_INDEX[FLAT_KEY]])
        flat_by_res[res] = flat_weekly
        weakest_seasonal = min(
            float(m["prior_seasonal"][_INDEX[k]]) for k in SEASONAL_KEYS
        )
        assert flat_weekly < 0.75 * weakest_seasonal, (
            f"at res {res} the FLAT species reads weekly CV {flat_weekly:.4f} "
            f"against {weakest_seasonal:.4f} for the weakest genuinely "
            "seasonal one; the prior is manufacturing seasonality, so the "
            "retention ratios above are measuring invention rather than signal"
        )

    assert flat_by_res[5] < 1.5 * flat_by_res[3], (
        f"the FLAT species' weekly CV GROWS with resolution "
        f"({flat_by_res}); it is sampling jitter, which does not care about "
        "the grid, so growth means the coarse levels or the smoother are "
        "writing weekly structure into a species that has none -- and every "
        "retention ratio in this file is then inflated by it"
    )


def test_the_retention_floors_are_DISCRIMINATING_not_merely_satisfied(probe, tmp_path):
    """The floors separate two REAL builds. Proven here, not asserted.

    A floor set below every measurement is worthless unless a plausible edit
    lands under it. So this rebuilds THE SAME ARCHIVE with `cell_week_weight`
    raised from 10.0 to 50.0 -- one number, the kind of change this file exists
    to catch -- and asserts the result falls BELOW the seasonal floors the
    default build passes.

    Measured: at res 4 the 50.0 build retains ~0.19 against a floor of 0.25 and
    a default-build ~0.43; at res 5, ~0.08 against a floor of 0.15 and a
    default ~0.27.

    Without this, `SEASONAL_RETENTION_FLOOR` would be a number that passes,
    with no evidence that anything fails it -- a test that cannot fail, however
    green it looks.
    """
    flattened = {
        res: build_and_measure(
            probe["archive"], probe["sites"], res, tmp_path / f"r{res}",
            backoff_params=BackoffParams(cell_week_weight=50.0),
        )
        for res in (4, 5)
    }
    for res, m in flattened.items():
        floor = SEASONAL_RETENTION_FLOOR[res]
        worst = min(_retention(m, k, "seasonal") for k in SEASONAL_KEYS)
        assert worst < floor, (
            f"at res {res}, raising `cell_week_weight` from 10.0 to 50.0 still "
            f"leaves {worst:.4f} of the seasonal amplitude, above the floor "
            f"{floor}. The floor therefore separates nothing: either it is set "
            "too low to catch a 5x change in the shrinkage, or the shrinkage "
            "no longer attenuates amplitude and the finding has changed shape"
        )
        # ...and the DEFAULT build passes the same floor on the same archive,
        # so the separation is between two builds rather than an artifact of
        # the floor being low.
        default_worst = min(
            _retention(probe["measured"][res], k, "seasonal")
            for k in SEASONAL_KEYS
        )
        assert default_worst > floor > worst, (res, default_worst, floor, worst)


def test_the_artifact_PUBLISHES_the_constants_that_produced_these_numbers(probe):
    """I2's other half: the retention above is not recoverable from the `.npz`.

    Every number in this file needed the generating truth, which a consumer
    does not have. What a consumer CAN have is the constants, and they are in
    the sidecar -- so a res-5 artifact carries the evidence that it was built
    with `cell_week_weight = 10.0` and can be read against this table.

    Asserted through `load_prior`, on the artifact, for every resolution.
    """
    for res, m in probe["measured"].items():
        t = m["tensor"]
        assert t.backoff_params == BackoffParams(), (res, t.backoff_params)
        assert t.smoothing_params == SmoothingParams(), (res, t.smoothing_params)


def test_a_non_default_build_publishes_ITS_OWN_constants_not_the_defaults(
    probe, tmp_path
):
    """The sidecar records what the build USED, not what the module defaults to.

    A sidecar that always wrote `BackoffParams()` would be indistinguishable
    from one recording the truth on every default build -- which is every build
    the tests make -- and would silently lie about exactly the artifact whose
    parameters a reader most needs: a swept one.
    """
    swept = BackoffParams(cell_week_weight=2.5, cell_weight=7.5)
    smooth = SmoothingParams(space_weight=0.9, time_weight=0.1)
    m = build_and_measure(
        probe["archive"], probe["sites"], 3, tmp_path / "swept",
        backoff_params=swept, smoothing_params=smooth,
    )
    assert m["tensor"].backoff_params == swept
    assert m["tensor"].smoothing_params == smooth
    assert m["tensor"].backoff_params != BackoffParams(), (
        "the swept params are equal to the defaults, so this test cannot tell "
        "a recorded value from a hardcoded one"
    )
