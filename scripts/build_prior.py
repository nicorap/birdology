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

THE COST IS REAL AND WAS MEASURED, NOT ASSUMED
-----------------------------------------------
`_validate_p` forbids `p <= 0`, so EVERY estimated context is an explicit
nonzero. A full 52-week grid over the observed cells materialises
`n_species * n_cells * 52` stored entries.

MEASURED peak RSS, by building real archives end to end (macOS arm64,
`ru_maxrss`), each row in ITS OWN SUBPROCESS -- `ru_maxrss` is a per-process
HIGH-WATER MARK, so two scales in one process report the larger one twice.
All figures NET of the ~77-82 MB interpreter-and-imports baseline:

    contexts   fill   post-to_tensor   post-controls   net bytes/context
    --------   ----   --------------   -------------   -----------------
    1.248e6    1.00          366 MB          390 MB                 263
    1.248e6    0.25          361 MB          385 MB                 259
    2.496e6    1.00          538 MB          595 MB                 218
    2.496e6    0.25          522 MB          579 MB                 211
    4.992e6    1.00         1085 MB         1180 MB                 232
    4.992e6    0.25         1066 MB         1161 MB                 228
    1.248e7    1.00         2347 MB         2585 MB                 211

Three findings, all of which contradict the estimate this design started from:

1. THE PEAK IS `to_tensor`, NOT `dense()` -- but "did not move the peak" was
   the WRONG WAY TO SAY IT, and the wrong phrasing would mislead future work.
   `ru_maxrss` is MONOTONE, so once `to_tensor` has set the high-water mark
   every later stage is invisible BY CONSTRUCTION; "the peak did not move"
   therefore says nothing about what the later stage COST. Measured against
   the POST-`to_tensor` mark, the four controls -- each of which calls
   `dense()` -- do add real memory: +24 MB at 1.25e6 contexts, +95 MB at
   4.99e6, +238 MB at 1.25e7. The conclusion still stands: the controls stay
   BELOW the `to_tensor` high-water mark at every scale measured, so
   `to_tensor`'s Arrow -> COO -> CSR conversion (which holds several
   representations of the same nonzeros at once) is what sizes the job. But
   anyone who later STREAMS `to_tensor` or reorders the stages inherits a
   false premise from the old phrasing: lower that mark and the controls'
   238 MB becomes the peak.

2. FILL BARELY MATTERS, which is the point of T10-R1. A quarter-filled
   occurrence grid costs 259 B/ctx against a full one's 263 -- because
   `hierarchical_estimate` materialises the REQUESTED grid whether or not
   anything was observed in it. The projection is a function of the grid
   shape, not of the data, which is exactly why a narrower week window is the
   only honest way to make a big resolution fit.

3. CHUNKING BOUNDS THE BACKOFF, NOT THE PEAK. Measured at 240 x 400:
   chunk 50 -> 1181 MiB, chunk 10 -> 1007 MiB, chunk 4 -> 993 MiB. Chunking
   caps the backoff's working set (the `estimates` table is built whole
   regardless, then converted whole), so it buys ~15% and then flattens.
   `--species-chunk` is therefore honest about what it does and is NOT a
   way to fit an arbitrarily large grid.

WHAT THAT IMPLIES FOR THE RELEASE, at ~600 Danish species (at 270 B/ctx, the
rounded-up WORST measured cost; the previous 160 under-predicted real builds
by 26-41%, in the direction that tells an operator a build is cheaper than it
is):

    resolution   cells over DK   contexts   projected peak
    ----------   -------------   --------   --------------
    3                       27   8.4e5          ~0.21 GB
    4                      147   4.6e6          ~1.2 GB
    5                      897   2.8e7          ~7.5 GB
    6                     5975   1.9e8            ~50 GB
    7                    40999   1.3e9           ~340 GB

Resolutions 3 and 4 fit on an ordinary machine; resolution 5 -- the top of the
default sweep -- needs ~8 GB and is the one to watch on a laptop. Resolution 6
needs a large node and 7 does not fit at all. The honest resolutions for those
are the two T10-R1 names: a documented week window, which would have to be
STATED IN THE SIDECAR because an artifact whose grid is narrower than 52 weeks
and does not say so is a silent lie about its own coverage, or a streaming
`to_tensor`. Falling back to the observed axes is NOT one of them: it
reintroduces the exact defect this design prevents.
"""
from __future__ import annotations

import argparse
import csv
import logging
import re
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

    Obligation 4: the zero-match condition is the shared observable symptom of
    four silent failure modes (contributor-spelling miss, mojibake, an
    un-wired eval split, a cell-format mismatch the resolution check cannot
    see). When a split WAS supplied, this build treats it as a failure,
    because the artifact it would otherwise write is one whose leakage guard
    did nothing.

    A `ValueError` subclass, so it obeys this plan's uniform error contract
    while still being catchable by name for anyone who wants to distinguish
    it from a malformed split.

    RAISED FROM RETURNED COUNTS, NEVER FROM A LOG RECORD
    ----------------------------------------------------
    The first version of this guard attached a `logging.Handler` to
    `birdology.prior.leakage` and string-matched the WARNING that module emits
    three frames down. It was reproduced dead, end to end: one
    `logging.basicConfig(level=logging.ERROR)` before `main()` -- exactly what
    a CI wrapper or cron harness does -- and a split matching nothing wrote
    all ten artifacts and returned exit 0. `main()`'s own `basicConfig` is a
    documented no-op once the root logger has handlers, so the warning never
    reached the detector. Four other logging configurations did the same, and
    because the logger NAME was a hard-coded string here, renaming leakage's
    logger bypassed the guard with nothing raised.

    A dead detector is worse than no detector, because its presence stops
    reviewers looking for a real one. The condition is now
    `LeakageGuardResult.zero_match` -- a comparison on two integers the guard
    function RETURNED -- so no logging level, no `logging.disable()`, no
    handler configuration and no logger rename can reach it. There is no
    second, log-based opinion: that path is deleted.
    """


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


#: MEASURED bytes of peak RSS per grid context, NET OF THE ~77-82 MB
#: INTERPRETER-AND-IMPORTS BASELINE. The figure excludes that baseline, so a
#: projection for a small grid under-states the process's total RSS by roughly
#: 80 MB; at any scale where this number matters the baseline is noise.
#:
#: RE-FITTED across THREE scales and TWO fill fractions (the previous 160 was
#: fitted to two points at one grid shape and under-predicted real builds by
#: 26-41%, in the direction that tells an operator a build is CHEAPER than it
#: is -- which is the OOM this logging exists to prevent). Each row is its own
#: SUBPROCESS, because `ru_maxrss` is a per-process high-water mark and two
#: scales in one process report the larger twice:
#:
#:     contexts   fill   baseline   post-to_tensor   post-controls   net B/ctx
#:     --------   ----   --------   --------------   -------------   ---------
#:     1.248e6    1.00      77 MB         366 MB          390 MB        263
#:     1.248e6    0.25      77 MB         361 MB          385 MB        259
#:     2.496e6    1.00      77 MB         538 MB          595 MB        218
#:     2.496e6    0.25      77 MB         522 MB          579 MB        211
#:     4.992e6    1.00      77 MB        1085 MB         1180 MB        232
#:     4.992e6    0.25      77 MB        1066 MB         1161 MB        228
#:     1.248e7    1.00      77 MB        2347 MB         2585 MB        211
#:
#: The constant is the ROUNDED-UP WORST case, 263 -> 270, not the mean (218).
#: Rounding to the mean is what produced the old under-prediction: this number
#: exists to stop an operator committing hours to a build that will OOM, and an
#: over-estimate costs them a bigger node while an under-estimate costs them the
#: whole run. FILL BARELY MATTERS (259 vs 263 at the small end, 228 vs 232 in
#: the middle): the grid is materialised in full by `hierarchical_estimate`
#: regardless of how much of it was ever observed, which is the entire point of
#: passing `weeks=`, so the projection is a function of the GRID and not of the
#: data. Cost per context FALLS with scale, so the worst case sits at the small
#: end and extrapolating it upward is conservative.
#:
#: Used only to WARN before a long job. It is a projection from measurements,
#: not a guarantee, and it is deliberately not a guard: refusing to build
#: would substitute this script's guess for the operator's knowledge of their
#: own machine.
MEASURED_BYTES_PER_CONTEXT = 270

#: The worst NET bytes/context any measured row produced, to one decimal. Its
#: only purpose is to pin `MEASURED_BYTES_PER_CONTEXT` against a recorded
#: measurement rather than against itself: a future edit that lowers the
#: constant back toward the mean has to argue with this number, and
#: `test_the_memory_constant_is_not_below_the_worst_measurement` fails if it
#: does not.
WORST_MEASURED_BYTES_PER_CONTEXT = 263.0


def project_peak_bytes(n_species: int, n_cells: int) -> int:
    """Projected peak RSS for a grid of this size, from measured scaling.

    EXCLUDES the ~80 MB interpreter-and-imports baseline; see
    `MEASURED_BYTES_PER_CONTEXT`.
    """
    return n_species * n_cells * WEEKS_PER_YEAR * MEASURED_BYTES_PER_CONTEXT


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

    No try/except ANYWHERE, and no `try` at all. A guard `ValueError` -- an H3
    resolution mismatch between the split and the table, a contract breach in
    any stage -- reaches the caller and the process exits non-zero. Catching
    one here to keep a sweep going would write a leak-contaminated artifact
    under a normal-looking filename, which is worse than the pre-fix silence
    because it looks deliberate.

    There is deliberately not even a bare `try/finally`. The previous version
    had one, to detach a logging handler, and a `return` added inside that
    `finally` would have swallowed EVERY in-flight exception including a guard
    `ValueError` -- an evasion the AST check could not see because it looked
    for `ExceptHandler` nodes and a finally-only `Try` has none. Deleting the
    handler deleted the `try`, and
    `test_the_pipeline_call_path_has_no_except_at_all` now forbids `ast.Try`
    in this function outright.

    THE ZERO-MATCH GUARD READS RETURNED COUNTS
    ------------------------------------------
    `apply_leakage_guards` returns a `LeakageGuardResult`. The escalation
    below compares two of its integers. It is not a log handler, not a string
    match, and not conditioned on any logging state; see
    `ZeroMatchGuardError` for the reproduced failure that motivated the
    change.
    """
    table = assign_cells(read_occurrences(Path(zip_path)), resolution)
    guarded = apply_leakage_guards(table, test_recordings)

    # DATA, not a log record. `zero_match` is
    # `banned_key_count > 0 and contributor_rows_removed == 0`, both computed
    # by the function that did the filtering. `test_recordings` is NOT part of
    # the condition: `banned_key_count` is the normalised key count the guard
    # actually held, which is the thing that decides whether the guard could
    # bite, and re-deriving intent from the argument here would let a split
    # that normalised away to nothing pass as legitimately empty.
    if guarded.zero_match:
        raise ZeroMatchGuardError(
            f"the test split matched no occurrence at resolution {resolution}: "
            f"{guarded.banned_key_count} banned (cell, week, contributor) "
            f"key(s) removed {guarded.contributor_rows_removed} of "
            f"{guarded.rows_before_contributor_guard} occurrences. A split was "
            "supplied, so this is a build failure rather than a warning: the "
            "artifact would carry a leakage guard that removed nothing, and "
            "would be indistinguishable from one whose guard worked"
        )

    counts = aggregate_counts(guarded.table)
    ratios = target_group_ratios(counts)
    species = species_axis(counts)
    cells = cell_axis(counts)
    log.info(
        "resolution %d: %d occurrences (%d dropped by the leakage guards over "
        "%d banned key(s)), %d species, %d cells -> %d grid "
        "contexts over %d weeks, projected peak ~%.1f GiB",
        resolution,
        guarded.table.num_rows,
        guarded.xeno_canto_rows_removed + guarded.contributor_rows_removed,
        guarded.banned_key_count,
        len(species),
        len(cells),
        len(species) * len(cells) * WEEKS_PER_YEAR,
        WEEKS_PER_YEAR,
        project_peak_bytes(len(species), len(cells)) / (1 << 30),
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


#: A DOI, in any of the spellings GBIF's download page offers: bare
#: (`10.15468/dl.abc123`), `doi:`-prefixed, or wrapped in a resolver URL. The
#: scheme and host are matched GENERICALLY rather than enumerated, both because
#: any resolver host is equally valid and because this module must not spell a
#: URL scheme literally: `test_the_build_script_does_not_import_the_download_client`
#: forbids network-client vocabulary in this file, and that check is worth more
#: than a hard-coded list of resolver prefixes.
#:
#: `10.\d{4,9}/` is the DOI registrant-prefix grammar from the DOI handbook.
#: Matched, never rewritten: the sidecar must carry the string the operator
#: attested to, and silently canonicalising an attribution field means the
#: artifact cites something the build was never told.
DOI_PATTERN = re.compile(
    r"^(?:[a-z][a-z0-9+.\-]*://[^/\s]+/|doi:)?10\.\d{4,9}/\S+$", re.IGNORECASE
)


def non_blank_doi(value: str) -> str:
    """argparse `type` for `--gbif-doi`: reject blank, then reject non-DOI.

    `required=True` REQUIRES THE FLAG, NOT A VALUE. Reproduced: `--gbif-doi ''`
    and `--gbif-doi '   '` were both accepted, and the whitespace one is the
    dangerous half -- `save_prior` refused an EMPTY doi but `'   '` is not
    empty, so all five artifacts wrote with sidecar `gbif_doi: '   '` and
    attribution `Derived from GBIF occurrence download    .` That is a CC BY
    4.0 attribution violation in a published artifact, reached by the most
    ordinary operator path there is:

        python scripts/build_prior.py --gbif-doi "$DOI" ...   # $DOI unset

    Rejected HERE as well as in `save_prior` because the two are
    independently load-bearing: this one turns a multi-hour national build
    into an instant usage error, and `save_prior`'s protects every OTHER
    caller of the library. Neither is redundant.

    NOT coerced. `.strip()` here would accept `'  10.15468/dl.x  '` and write
    a different string from the one the operator passed, which is the silent
    repair this plan forbids at a boundary.
    """
    if not value or not value.strip():
        raise argparse.ArgumentTypeError(
            f"--gbif-doi is blank or whitespace-only ({value!r}); every "
            "artifact is released CC BY 4.0, which obliges attribution, and a "
            "blank DOI writes a sidecar whose attribution line names no "
            "download at all. This is what `--gbif-doi \"$DOI\"` does with an "
            "unset variable, so it is rejected at parse time rather than "
            "hours later at the write"
        )
    if not DOI_PATTERN.match(value):
        raise argparse.ArgumentTypeError(
            f"--gbif-doi {value!r} is not shaped like a DOI (expected a "
            "registrant prefix `10.NNNN/suffix`, optionally `doi:`-prefixed "
            "or wrapped in a resolver URL); a GBIF download has BOTH a "
            "download KEY (e.g. 0012345-230530130807322) and a DOI (e.g. "
            "10.15468/dl.abc123), the two sit beside each other on the "
            "download page, and only the DOI is citable. A key, a dataset "
            "UUID or an archive filename in this field produces a well-formed, "
            "permanently mis-attributed release that no reader of the artifact "
            "can detect"
        )
    return value


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive", required=True, type=Path)
    ap.add_argument("--source", required=True, choices=["eod", "dof", "eod+dof"])
    ap.add_argument("--resolutions", nargs="+", type=int, default=[3, 4, 5])
    ap.add_argument("--out", type=Path, default=Path("output/priors"))
    ap.add_argument(
        "--gbif-doi",
        required=True,
        type=non_blank_doi,
        help="the DOI of the GBIF download this archive came from. Required: "
        "every artifact is released CC BY 4.0, which obliges attribution, and "
        "`save_prior` refuses to write without it. `required=True` requires "
        "the FLAG, so the VALUE is validated by `non_blank_doi`: a blank, "
        "whitespace-only or non-DOI string is a usage error",
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
