#!/usr/bin/env python3
"""Build spatiotemporal occurrence priors from a GBIF download archive.

    python scripts/build_prior.py --archive dof.zip --source dof \
        --resolutions 3 4 5 --out output/priors \
        --gbif-doi 10.15468/dl.xxxxxx --test-split splits/eval.csv

Requesting the archive itself needs GBIF credentials:

    GBIF_USER / GBIF_PASSWORD / GBIF_EMAIL

See docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md

THIS IS THE ONLY STAGE A USER RUNS
----------------------------------
Everything upstream is a library. This script is where the plan's global
constraints stop being prose:

* every artifact is CC BY 4.0 and names the GBIF download DOI (`save_prior`
  REFUSES an empty one, so this is enforced rather than promised, and it
  applies to the four CONTROLS exactly as it does to the full prior -- a
  control with no provenance is as uncitable as a full one);
* output paths come from `--out`, never from a hardcoded cluster path;
* the test split is re-derived PER RESOLUTION, because a `TestRecording`'s
  cell is fixed at the resolution it was built at;
* a guard `ValueError` PROPAGATES: a failed artifact exits non-zero rather
  than landing on disk under a normal-looking name.

THE FULL 52-WEEK GRID IS PASSED EXPLICITLY (T10-R1)
----------------------------------------------------
`hierarchical_estimate`'s documented default axes are the OBSERVED ones. On a
real archive that is a handful of weeks, not 52. The two are NOT the same
artifact:

* a week the backoff NEVER SAW reads back as a bare `FLOOR` -- "no evidence";
* a week it DID see, with no occurrences of that species, gets a SHRUNK
  ESTIMATE: the cell's annual profile pulled toward the species' global
  profile. That is the entire point of the hierarchy, and it is the number
  the paper claims to publish.

Omitting `weeks=` silently downgrades ~98% of the artifact from "estimated"
to "no evidence". Nothing raises, every stored value is correct, and only the
COVERAGE is impoverished -- so it is invisible in the output. `cells=` is
passed for the same reason in the spatial direction, and explicitly rather
than by default so the axes the artifact publishes are a decision this script
made, not a side effect of which rows happened to survive the guards.

THE COST IS REAL AND IS MEASURED, NOT ASSUMED
----------------------------------------------
`_validate_p` forbids `p <= 0`, so EVERY estimated context is an explicit
nonzero. A full 52-week grid over the observed cells materialises
`n_species * n_cells * 52` stored entries: at Danish national scale (~600
species, ~3000 cells) that is 9.36e7 entries, ~1.1 GB of CSR, and Task 9's
controls call `t.dense()` on top, ~750 MB each.

So this script BUILDS IN SPECIES CHUNKS (`--species-chunk`) and reports peak
RSS at INFO. Chunking is bit-exact, not an approximation, and that is a
property of the backoff rather than a hope: every level of the hierarchy is
computed from the RATIO TABLE, which is passed whole to every chunk, so a
chunk's estimates do not depend on which other species are in the same call.
`test_build_prior.py` pins it against a single-call build value-for-value.

What chunking does NOT reduce is the PEAK, which is set by the controls:
`dense()` materialises the whole float64 grid in one array regardless of how
the tensor was assembled. The honest resolutions if that does not fit are the
two T10-R1 names: a documented week window -- which would have to be stated
in the sidecar, because an artifact whose grid is narrower than 52 weeks and
does not say so is a silent lie about its own coverage -- or a control
implementation that streams. Falling back to the observed axes is NOT one of
them: it reintroduces the exact defect this design prevents.
"""
from __future__ import annotations

import argparse
import csv
import logging
import resource
import sys
from collections.abc import Sequence
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pyarrow as pa

from birdology.prior.backoff import hierarchical_estimate
from birdology.prior.controls import CONTROLS
from birdology.prior.effort import target_group_ratios
from birdology.prior.indexing import WEEKS_PER_YEAR, aggregate_counts, assign_cells
from birdology.prior.leakage import (
    ZERO_MATCH_WARNING_MARKER,
    TestRecording,
    apply_leakage_guards,
)
from birdology.prior.occurrences import read_occurrences
from birdology.prior.smoothing import smooth
from birdology.prior.tensor import PriorSpec, save_prior, to_tensor

log = logging.getLogger("birdology.prior.build")

#: Columns a test-split CSV must carry. The cell is NOT among them and must
#: not be: a pre-computed cell is fixed at one H3 resolution, and reusing it
#: across a `--resolutions 3 4 5` sweep makes the leakage guard raise on every
#: resolution but one. `lat`/`lon`/`week`/`recorded_by` are resolution-free,
#: so each resolution derives its own cells from them.
TEST_SPLIT_COLUMNS = ("lat", "lon", "week", "recorded_by")

#: Default species per backoff call. Chosen so a national-scale build stays
#: well under the ~1.1 GB the full grid would need in one allocation, while
#: keeping the number of passes over the ratio table small.
DEFAULT_SPECIES_CHUNK = 50


class ZeroMatchGuardError(ValueError):
    """A non-empty test split matched no occurrence at all.

    Obligation 4: the zero-match WARNING is the shared observable symptom of
    four silent failure modes (contributor-spelling miss, mojibake, an
    un-wired eval split, a cell-format mismatch the resolution check cannot
    see). Logged and carried, it is a line in a build log nobody reads. When
    a split WAS supplied, this build treats it as a failure, because the
    artifact it would otherwise write is one whose leakage guard did nothing.

    A `ValueError` subclass, so it obeys this plan's uniform error contract
    while still being catchable by name for anyone who wants to distinguish
    it from a malformed split.
    """


class _ZeroMatchDetector(logging.Handler):
    """Turn the leakage module's zero-match WARNING into a build failure.

    A logging handler rather than a return value because the warning is
    emitted three call levels down, inside a function whose signature this
    plan deliberately does not widen: `drop_test_contributor_cells` is
    correct to WARN (an empty match is not always fatal -- a caller building
    a prior for a region the split does not cover is legitimate), and only
    this script knows that a split was MEANT to bite.

    It matches on `ZERO_MATCH_WARNING_MARKER`, imported from `leakage`, so
    the coupling is a named constant rather than a copied string that would
    go silently dead the next time the message is reworded.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.fired: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if ZERO_MATCH_WARNING_MARKER in message:
            self.fired.append(message)


def load_test_split(path: Path) -> list[dict]:
    """Read an evaluation split as resolution-free (lat, lon, week, who) rows.

    Returns dicts rather than `TestRecording`s ON PURPOSE. A `TestRecording`
    carries a CELL, and a cell only means anything at one H3 resolution; the
    sweep needs each resolution to derive its own. Handing the caller an
    already-built `TestRecording` would put the resolution decision here,
    where the resolution is not yet known.

    Every contract violation raises `ValueError` naming that check. The split
    is the thing standing between the released prior and evaluation leakage,
    so a malformed one stops the build rather than silently contributing
    fewer banned keys than the file claims.
    """
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(set(TEST_SPLIT_COLUMNS) - set(reader.fieldnames or ()))
        if missing:
            raise ValueError(
                f"test split {path} is missing columns {missing}; a split must "
                f"carry {list(TEST_SPLIT_COLUMNS)}. It deliberately does NOT "
                "carry a cell: a pre-computed cell is fixed at one H3 "
                "resolution and would make the leakage guard raise on every "
                "other resolution in the sweep"
            )
        rows = []
        for lineno, row in enumerate(reader, start=2):
            try:
                lat = float(row["lat"])
                lon = float(row["lon"])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"test split {path} line {lineno} has unparseable "
                    f"coordinates ({row['lat']!r}, {row['lon']!r}); a "
                    "recording whose location cannot be read contributes no "
                    "banned key, so its own occurrences would stay in the prior"
                ) from exc
            try:
                week = int(str(row["week"]).strip())
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"test split {path} line {lineno} has an unparseable week "
                    f"{row['week']!r}; the week is a third of the leakage join "
                    "key and a wrong one bans the wrong cell-week"
                ) from exc
            rows.append(
                {
                    "lat": lat,
                    "lon": lon,
                    "week": week,
                    "recorded_by": row["recorded_by"],
                }
            )
    if not rows:
        raise ValueError(
            f"test split {path} holds no recordings; "
            "`drop_test_contributor_cells` returns early and SILENTLY on an "
            "empty split, so this would build an unguarded prior that looks "
            "exactly like a guarded one. Pass --no-test-split if that is "
            "really what you want"
        )
    return rows


def recordings_at_resolution(
    split: Sequence[dict], resolution: int
) -> list[TestRecording]:
    """Build each recording's cell AT `resolution`.

    Called INSIDE the resolution loop, with the same `resolution` value handed
    to `assign_cells`, so the two sides of the leakage join are always at one
    resolution. A cell computed once and reused across the sweep matches
    nothing at every other resolution -- and since Task 4 that RAISES rather
    than silently no-opping, which is why it is visible at all.

    Deliberately NOT named `test_recordings_at`: pytest collects any
    module-level callable whose name starts with `test_` once a test module
    imports it, and a helper requiring two arguments then reports as a
    collection ERROR in an otherwise green run.
    """
    import h3

    return [
        TestRecording(
            cell=h3.latlng_to_cell(row["lat"], row["lon"], resolution),
            week=row["week"],
            recorded_by=row["recorded_by"],
        )
        for row in split
    ]


def _peak_rss_bytes() -> int:
    """Peak resident set size of this process, in bytes.

    `ru_maxrss` is in KILOBYTES on Linux and in BYTES on macOS/BSD. Getting
    that wrong by 1024x in either direction would make the memory obligation
    in T10-R1 report a number that is not a measurement, so the platform is
    checked rather than assumed.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def estimate_over_full_grid(
    ratios: pa.Table,
    species: Sequence[int],
    cells: Sequence[str],
    *,
    species_chunk: int = DEFAULT_SPECIES_CHUNK,
) -> pa.Table:
    """The backoff over the FULL (species x cells x 52) grid, in species chunks.

    The grid is `cells` x `range(1, WEEKS_PER_YEAR + 1)` -- both passed
    EXPLICITLY, never left to the observed-axes default. See the module
    docstring for why the two differ and why the difference is invisible.

    CHUNKING IS BIT-EXACT, not an approximation. Every level of the hierarchy
    -- global, cell, cell-week -- is derived from `ratios`, which each chunk
    receives whole; a chunk's output therefore does not depend on which other
    species share the call. `WEEKS_PER_YEAR` comes from `indexing`, never a
    literal 52, so one definition of the week axis serves the packing and the
    grid alike.

    Smoothing is applied PER CHUNK, which is also exact: the kernel blends a
    context with its spatial and temporal neighbours WITHIN one species, and
    never across species, so a chunk contains every row any of its own rows
    reads. Smoothing after concatenation would give the same numbers at
    several times the peak memory.
    """
    weeks = list(range(1, WEEKS_PER_YEAR + 1))
    if species_chunk < 1:
        raise ValueError(
            f"species_chunk must be >= 1, got {species_chunk}; a chunk size of "
            "zero or less would silently emit an EMPTY prior rather than fail"
        )

    ordered = list(species)
    pieces = [
        smooth(
            hierarchical_estimate(
                ratios,
                ordered[start : start + species_chunk],
                cells=list(cells),
                weeks=weeks,
            )
        )
        for start in range(0, len(ordered), species_chunk)
    ]
    if not pieces:
        # No species at all. `pa.concat_tables([])` cannot infer a schema, so
        # the empty grid is produced through the same call the loop would have
        # made -- one definition of the output schema, rather than a
        # hand-written empty table that could drift from it.
        return smooth(
            hierarchical_estimate(ratios, [], cells=list(cells), weeks=weeks)
        )
    return pa.concat_tables(pieces)


def species_axis(counts: pa.Table) -> list[int]:
    """The SORTED species axis for the grid.

    Sorted, and a named function rather than an inline set comprehension,
    because the sort is load-bearing in a way that leaves no trace in the
    artifact: `to_tensor` sorts the tensor's own axis, so an unsorted list
    changes no stored value. What it changes is which species fall in which
    `--species-chunk`, and a set's iteration order varies with the hash seed
    and insertion history -- so the artifact would be reproducible only
    within one interpreter run. A released dataset has to be reproducible
    across them.
    """
    return sorted({int(k) for k in counts.column("species_key").to_pylist()})


def cell_axis(counts: pa.Table) -> list[str]:
    """The SORTED cell axis for the grid. Sorted for the same reason."""
    return sorted({str(c) for c in counts.column("cell").to_pylist()})


def build_from_archive(
    zip_path: Path,
    *,
    resolution: int,
    source: str,
    test_recordings: Sequence[TestRecording],
    out_dir: Path,
    gbif_doi: str,
    species_chunk: int = DEFAULT_SPECIES_CHUNK,
) -> list[Path]:
    """Archive -> full prior + four controls. Returns the written paths.

    `gbif_doi` reaches EVERY artifact, controls included: `save_prior` refuses
    an empty one, and `controls._respec` carries it from the full prior into
    each control, so a control cannot be written with weaker provenance than
    the prior it is the null for.

    No try/except anywhere. A guard `ValueError` -- an H3 resolution mismatch
    between the split and the table, a contract breach in any stage -- reaches
    the caller and the process exits non-zero. Catching one here to keep a
    sweep going would write a leak-contaminated artifact under a
    normal-looking filename, which is worse than the pre-fix silence because
    it looks deliberate.
    """
    detector = _ZeroMatchDetector()
    logging.getLogger("birdology.prior.leakage").addHandler(detector)
    try:
        table = assign_cells(read_occurrences(Path(zip_path)), resolution)
        table = apply_leakage_guards(table, test_recordings)
    finally:
        # Removed in a `finally` so a raised guard error does not leave a
        # handler attached to a module-level logger for the rest of the
        # process. This is NOT catching the error: nothing is caught here and
        # the exception continues to propagate.
        logging.getLogger("birdology.prior.leakage").removeHandler(detector)

    if detector.fired and test_recordings:
        raise ZeroMatchGuardError(
            f"the test split matched no occurrence at resolution {resolution}: "
            f"{detector.fired[0]}. A split was supplied, so this is a build "
            "failure rather than a warning: the artifact would carry a leakage "
            "guard that removed nothing, and would be indistinguishable from "
            "one whose guard worked"
        )

    counts = aggregate_counts(table)
    ratios = target_group_ratios(counts)
    species = species_axis(counts)
    cells = cell_axis(counts)
    log.info(
        "resolution %d: %d occurrences, %d species, %d cells -> %d grid "
        "contexts over %d weeks",
        resolution,
        table.num_rows,
        len(species),
        len(cells),
        len(species) * len(cells) * WEEKS_PER_YEAR,
        WEEKS_PER_YEAR,
    )

    estimates = estimate_over_full_grid(
        ratios, species, cells, species_chunk=species_chunk
    )

    spec = PriorSpec(resolution=resolution, source=source, variant="full")
    full = to_tensor(estimates, spec)
    full.gbif_doi = gbif_doi

    written: list[Path] = []
    stem = f"prior_{source}_r{resolution}"
    written.append(save_prior(full, out_dir / f"{stem}_full.npz"))
    for name, make in CONTROLS.items():
        control = make(full)
        # Belt and braces beside `_respec`'s pass-through: this script is what
        # KNOWS the DOI, and a control shipped without it is as uncitable as a
        # full prior without it. `save_prior` refuses an empty one either way,
        # so a regression in `_respec` fails the build rather than releasing
        # an unattributed artifact.
        control.gbif_doi = gbif_doi
        written.append(save_prior(control, out_dir / f"{stem}_{name}.npz"))

    log.info(
        "resolution %d: wrote %d artifacts to %s; nnz=%d of dense capacity %d; "
        "peak RSS %.1f MiB",
        resolution,
        len(written),
        out_dir,
        full.values.nnz,
        full.dense_capacity,
        _peak_rss_bytes() / (1 << 20),
    )
    return written


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive", required=True, type=Path)
    ap.add_argument("--source", required=True, choices=["eod", "dof", "eod+dof"])
    ap.add_argument("--resolutions", nargs="+", type=int, default=[3, 4, 5])
    ap.add_argument("--out", type=Path, default=Path("output/priors"))
    ap.add_argument(
        "--gbif-doi",
        required=True,
        help="the DOI of the GBIF download this archive came from. Required: "
        "every artifact is released CC BY 4.0, which obliges attribution, and "
        "`save_prior` refuses to write without it",
    )
    ap.add_argument(
        "--species-chunk",
        type=int,
        default=DEFAULT_SPECIES_CHUNK,
        help="species per backoff call. Chunking is bit-exact; it bounds the "
        "working set of the full 52-week grid (default: %(default)s)",
    )
    split = ap.add_mutually_exclusive_group(required=True)
    split.add_argument(
        "--test-split",
        type=Path,
        help="CSV of evaluation recordings (lat, lon, week, recorded_by). Each "
        "resolution derives its own cells from lat/lon",
    )
    split.add_argument(
        "--no-test-split",
        action="store_true",
        help="build WITHOUT a leakage split. Explicit on purpose: "
        "`drop_test_contributor_cells` returns silently on an empty split, so "
        "an unguarded prior is indistinguishable from a guarded one and the "
        "highest-consequence failure in this plan would have no detector",
    )
    return ap.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)

    # BEFORE any pipeline call. The leakage module's zero-match WARNING is the
    # shared symptom of four silent failure modes, and an unconfigured root
    # logger discards it -- re-silencing all four. Configured here rather than
    # at import so importing this module for `build_from_archive` does not
    # reconfigure a caller's logging.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    split = [] if args.no_test_split else load_test_split(args.test_split)
    if args.no_test_split:
        log.warning(
            "building with NO leakage test split (--no-test-split): the "
            "contributor guard will remove nothing and the artifacts will not "
            "be safe to evaluate against any split"
        )

    for resolution in args.resolutions:
        # NO try/except around the loop body. A guard ValueError -- most
        # likely an H3 resolution mismatch -- must fail THIS artifact and exit
        # non-zero, rather than being carried so the sweep can write a
        # leak-contaminated file under a name that looks like every other one.
        paths = build_from_archive(
            args.archive,
            resolution=resolution,
            source=args.source,
            test_recordings=recordings_at_resolution(split, resolution),
            out_dir=args.out,
            gbif_doi=args.gbif_doi,
            species_chunk=args.species_chunk,
        )
        for path in paths:
            print(path)


if __name__ == "__main__":
    main()
