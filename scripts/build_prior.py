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

OPTION (b): BUILD ONLY WHERE THE PRIOR IS QUERIED (`--query-set`)
-----------------------------------------------------------------
The table above is for Denmark. The GLOBAL EOD archive (fleet job 6481:
1.76e9 occurrences after the guards, 10 319 species, 15 463 observed r3
cells) projects 8.3e9 contexts / ~2 TiB at r3 and 3.6e10 / ~9 TiB at r4; r5
segfaulted at ~800 GB. Every consumer of the prior only looks up the (cell,
week) contexts of its own recordings, so `--query-set` (written by
`scripts/build_query_set.py` from the Phase A manifests) restricts the
ESTIMATION -- never the parse, the guards or the RatioTable -- to those
contexts, and the result is BIT-IDENTICAL to the full build there:

1. the backoff's global and cell levels are ratios of sums over the WHOLE
   RatioTable, so a point's value depends on the table and on its own
   (species, cell, week) only -- which points are requested changes nothing
   (`backoff.hierarchical_estimate`; reproduced point-wise by
   `point_backoff.PointStatistics`);
2. smoothing is ONE pass in which (c, w) reads its six `grid_disk(c, 1)`
   neighbours at w and (c, w +- 1) with the 52 <-> 1 wrap, and a neighbour
   counts only if its cell is on the grid's cell list -- in the full build,
   the OBSERVED cells (`smoothing._neighbour_codes`).

So evaluating the backoff on the query contexts' SUPPORT -- themselves,
their adjacent weeks and their OBSERVED ring-1 neighbours at the same week
(`query_set.plan_restriction`) -- and applying the same pass
(`query_set.smooth_on_plan`) reproduces the full build's value at every
query context whose cell was observed. A query cell that was never observed
stays off the cell axis, exactly as in the full build. The artifact says it
is restricted (sidecar format version 2), and every consumer refuses a
lookup outside its declared domain instead of flooring it.

THE GUARDED COUNTS ARE CHECKPOINTED (`--resume-counts`)
-------------------------------------------------------
Once both leakage escalation clauses have passed, `main` writes the guarded
per-resolution counts -- the exact input to `target_group_ratios` -- to
`--out/guarded_counts_{source}_r{res}.parquet` with their provenance
(`counts_checkpoint.py`). `--resume-counts` starts the next run there,
skipping the parse and the guards; a checkpoint from another archive,
resolution, split file or contributor mode is refused naming the field.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import re
import resource
import sys
from collections.abc import Sequence
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pyarrow as pa

from birdology.prior.backoff import BackoffParams, hierarchical_estimate
from birdology.prior.checkpoint import (
    StaleCheckpointError,
    checkpoint_path,
    load_checkpoint,
    write_checkpoint,
)
from birdology.prior.controls import CONTROLS
from birdology.prior.counts_checkpoint import (
    CountsIdentity,
    counts_checkpoint_path,
    load_counts_checkpoint,
    write_counts_checkpoint,
)
from birdology.prior.effort import target_group_ratios
from birdology.prior.indexing import WEEKS_PER_YEAR, aggregate_counts, assign_cells
from birdology.prior.leakage import (
    CONTRIBUTOR_MODES,
    TestRecording,
    apply_leakage_guards,
    key_shape,
)
from birdology.prior.occurrences import read_occurrences
from birdology.prior.parallel_parse import parse_and_aggregate
from birdology.prior.point_backoff import PointStatistics
from birdology.prior.query_set import (
    QuerySet,
    RestrictionPlan,
    load_query_set,
    plan_restriction,
    smooth_on_plan,
)
from birdology.prior.smoothing import SmoothingParams, smooth
from birdology.prior.tensor import (
    PriorSpec,
    Restriction,
    restricted_tensor,
    save_prior,
    to_tensor,
)

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

    THIS IS THE TOTAL CLAUSE ONLY. It fires when NOT ONE banned key bit.
    `PartialMatchGuardError` is its per-key sibling and catches the case this
    one provably cannot see: a split in which a handful of keys are live and
    the rest are dead. See `build_from_archive` for why both are needed.

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


class PartialMatchGuardError(ValueError):
    """Most of a supplied test split was inert: the guard barely ran.

    THE DEFECT THIS EXISTS FOR, REPRODUCED
    --------------------------------------
    `LeakageGuardResult.zero_match` is `banned_key_count > 0 and
    contributor_rows_removed == 0` -- a claim about the WHOLE split. A 100-key
    split in which ONE key matched a real contributor and ninety-nine matched
    nothing (Sydney coordinates against Danish occurrences) built a complete
    five-artifact release and exited 0, while the all-hundred-dead control
    correctly raised. One live key disarms the total condition for an
    arbitrarily large split.

    That matters because the failure modes the guard exists to catch do not
    produce a clean 100%:

    * a resolution-free coordinate bug puts most recordings in the wrong cell;
    * a contributor-spelling convention that changed mid-file kills the rows
      on one side of the change;
    * a mojibaked name column kills every non-ASCII contributor, which in
      Danish data is most of them but never all of them.

    Every one of those degrades 99% of a split rather than 100%, so the total
    condition catches only the case an operator would probably have noticed
    anyway. This class is the per-key condition that catches the rest.

    A `ValueError` subclass like every other contract violation here, and a
    DISTINCT class from `ZeroMatchGuardError` because the two say different
    things: nothing matched at all, versus almost nothing did. An operator
    triaging the first looks for a broken join; triaging the second looks for
    a subpopulation of the split that is broken while the rest works.
    """


#: Minimum fraction of a supplied split's banned keys that must remove at
#: least one occurrence, below which `build_from_archive` REFUSES.
#:
#: THIS IS A POLICY, NOT A MEASUREMENT, AND IT IS SAID SO ON PURPOSE.
#: `MEASURED_BYTES_PER_CONTEXT` below is fitted to recorded measurements and
#: this number is not: there is no real evaluation split in this repository to
#: fit it to, and a threshold fitted to the synthetic fixtures would be a
#: number dressed as evidence. What CAN be argued is the shape of the choice.
#:
#: WHY NOT "FAIL ONLY AT ZERO, AND WARN WITH THE RATE OTHERWISE". That is the
#: behaviour this file already had, plus a log line, and it leaves the
#: reproduced defect intact: the 1-of-100 split still writes five artifacts
#: and exits 0. It also puts the only signal for the partial case back on the
#: logging channel that `ZeroMatchGuardError` documents as unusable for
#: control flow. Rejected.
#:
#: WHY NOT A MAJORITY (0.5). A test split's keys are (cell, week, contributor)
#: triples, and a key bites only if that contributor has some OTHER,
#: non-Xeno-Canto occurrence in the same cell-week -- guard 1 has already
#: removed the recording's own row. A recordist who publishes to Xeno-Canto
#: but not to DOFbasen contributes a key that legitimately matches nothing,
#: and there is no reason to expect most test recordists to be in both. A
#: majority threshold would therefore fail correct builds routinely, and the
#: pressure from a guard that fires on a legal build is to delete the guard
#: rather than to fix it. Rejected.
#:
#: WHY ONE KEY IN TWENTY. It is far below any rate at which a split could be
#: said to be guarding, and comfortably above the sparsity a real split can
#: produce for the reason above. It fires on the reproduced 1-of-100 (0.01).
#:
#: AND WHY THE "LEGITIMATELY STALE SPLIT" OBJECTION DOES NOT LAND. The
#: objection is that a correct build can fail this check. It can -- but the
#: artifact such a build would write carries a leakage guard that touched
#: under 5% of the held-out recordings, and that artifact should not be
#: released without the operator having looked. The check does not claim the
#: split FILE is malformed; it claims the leakage protection did not
#: substantially happen, which in that case is TRUE. So the failure is not a
#: false positive, it is an unwelcome true one -- and
#: `--min-split-match-rate` is the explicit, recorded escape for an operator
#: who has looked and accepts it, in the same spirit as `--no-test-split`.
#: Passing `0.0` reduces this check to the total clause, which no flag can
#: disable.
DEFAULT_MIN_SPLIT_MATCH_RATE = 0.05


def split_match_rate(value: str) -> float:
    """argparse `type` for `--min-split-match-rate`: a fraction in [0, 1].

    REJECTED, never clamped. A value outside the range is a units bug -- `5`
    meaning five percent is the obvious one -- and clamping it to 1.0 would
    turn "I meant 5%" into "every key must bite", failing every real build;
    clamping the other way would silently disarm the check. Both are the
    silent repair at a boundary this plan forbids.
    """
    try:
        rate = float(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            f"--min-split-match-rate {value!r} is not a number; it is the "
            "FRACTION of the test split's banned keys that must remove at "
            "least one occurrence, so it is a value in 0..1 (0.05 = one key "
            "in twenty)"
        ) from exc
    if not 0.0 <= rate <= 1.0:
        raise argparse.ArgumentTypeError(
            f"--min-split-match-rate {rate!r} is outside 0..1; it is a "
            "FRACTION, not a percentage, so five percent is 0.05 and not 5. "
            "Rejected rather than clamped: clamping 5 to 1.0 would demand "
            "that EVERY banned key bite and fail every real build, and "
            "clamping the other way would silently disarm the check"
        )
    return rate


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
    backoff_params: BackoffParams = BackoffParams(),
    smoothing_params: SmoothingParams = SmoothingParams(),
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
                backoff_params,
                cells=list(cells),
                weeks=weeks,
            ),
            smoothing_params,
        )
        for start in range(0, len(ordered), species_chunk)
    ]
    if not pieces:
        # No species at all. `pa.concat_tables([])` cannot infer a schema, so
        # the empty grid is produced through the same call the loop would have
        # made -- one definition of the output schema, rather than a
        # hand-written empty table that could drift from it. The SAME params
        # are passed here, so the empty path cannot silently use the defaults
        # while the populated one uses the caller's.
        return smooth(
            hierarchical_estimate(
                ratios, [], backoff_params, cells=list(cells), weeks=weeks
            ),
            smoothing_params,
        )
    return pa.concat_tables(pieces)


def estimate_over_query_set(
    ratios: pa.Table,
    species: Sequence[int],
    plan: RestrictionPlan,
    *,
    species_chunk: int = DEFAULT_SPECIES_CHUNK,
    backoff_params: BackoffParams = BackoffParams(),
    smoothing_params: SmoothingParams = SmoothingParams(),
    stats: PointStatistics | None = None,
) -> "np.ndarray":
    """The FULL build's values at ``plan.on_grid_contexts``, streamed by chunk.

    Returns ``(len(species), len(plan.on_grid_contexts))`` float64. Per
    species chunk: the backoff at ``plan.support_points`` (the query contexts,
    their adjacent weeks and their observed ring-1 neighbours), then the one
    smoothing pass evaluated only at the query contexts, written into the
    output block -- so the working set is one chunk's support block, never a
    halo table. Exactness argument: the module docstring, "OPTION (b)".

    `ratios` is the WHOLE RatioTable: restricting it would change every
    parent level of the hierarchy.
    """
    import numpy as np

    if species_chunk < 1:
        raise ValueError(
            f"species_chunk must be >= 1, got {species_chunk}; a chunk size of "
            "zero or less would silently emit an EMPTY prior rather than fail"
        )
    stats = stats if stats is not None else PointStatistics(ratios)
    ordered = list(species)
    out = np.empty((len(ordered), len(plan.on_grid_contexts)), dtype=np.float64)
    support = plan.support_points
    for start in range(0, len(ordered), species_chunk):
        chunk = ordered[start : start + species_chunk]
        out[start : start + len(chunk)] = smooth_on_plan(
            stats.estimate(chunk, support, backoff_params), plan, smoothing_params
        )
    return out


#: MEASURED net peak bytes per STORED context of a restricted build
#: (`build_from_counts` with a query set, end to end: estimation, the full
#: tensor, five controls and six `save_prior` writes). Slope of net
#: `ru_maxrss` against stored contexts at a fixed RatioTable, each point its
#: own subprocess (macOS arm64, 3.0e6 ratio rows, 8 000 on-grid contexts):
#:
#:     species   stored     net peak    slope vs previous
#:     -------   ------     --------    -----------------
#:         500   4.0e6        963 MB
#:       2 000   1.6e7      1 763 MB    69.9 B/stored
#:       4 000   3.2e7      2 734 MB    63.6 B/stored
#:
#: Rounded UP from the worst slope, for the reason `MEASURED_BYTES_PER_CONTEXT`
#: is: an under-estimate costs the operator the whole run.
RESTRICTED_BYTES_PER_STORED_CONTEXT = 72

#: MEASURED net peak bytes per RatioTable row of a restricted build: the
#: intercept of the series above (963 MB - 4.0e6 x 70 B over 2.98e6 rows =
#: ~232 B/row) -- `target_group_ratios`, the species/cell axes and
#: `point_backoff.PointStatistics` (68-80 B/row on its own, measured
#: separately at 2e6-2e7 rows). Rounded up. It does NOT include the guarded
#: counts table that already exists when estimation starts, nor the parse and
#: guards, which are unchanged by `--query-set` and dominate on the fleet.
RESTRICTED_BYTES_PER_RATIO_ROW = 240

#: Per support point, per species of one chunk: the support block and the
#: smoothing pass's temporaries (~6 float64 arrays of the chunk's size). An
#: ACCOUNTING, not a fit; it is the smallest of the three terms at fleet
#: scale (~2 GB at r5 with the default chunk).
RESTRICTED_BYTES_PER_CHUNK_SUPPORT_POINT = 48


def project_restricted_peak_bytes(
    n_species: int,
    n_stored_contexts: int,
    n_support_points: int,
    n_ratio_rows: int,
    species_chunk: int,
) -> int:
    """Projected peak RSS of a RESTRICTED build, excluding the parse/guards.

    Three terms, summed to stay on the side of telling an operator the
    build is MORE expensive than it is: the RatioTable-proportional cost,
    one chunk's support working set, and the stored result plus controls.
    """
    return (
        n_ratio_rows * RESTRICTED_BYTES_PER_RATIO_ROW
        + min(species_chunk, max(n_species, 1))
        * n_support_points
        * RESTRICTED_BYTES_PER_CHUNK_SUPPORT_POINT
        + n_species * n_stored_contexts * RESTRICTED_BYTES_PER_STORED_CONTEXT
    )


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

#: The worst NET bytes/context any measured row produced, to one decimal.
#:
#: READ FROM THE RECORDED MEASUREMENT, NOT TYPED BESIDE THE CONSTANT.
#: `test_the_memory_constant_is_not_below_the_worst_measurement` used to
#: assert `MEASURED >= WORST` plus `WORST > 160`, and that second clause was
#: an arbitrary tripwire rather than an anchor: lowering BOTH numbers to
#: 161/161.0 -- one above the tripwire -- left the whole suite green and
#: restored the under-prediction the re-measurement existed to remove. A
#: coordinated edit walked through the pin.
#:
#: The value is now DERIVED, at import, from
#: `scripts/measurements/measure_peak_results.json` -- the output of
#: `measure_peak.py`, committed (the plan directory holding the original is
#: gitignored, so it pinned nothing) so the constant is tied to an artifact
#: that a coordinated edit cannot reach by retyping a number. Lowering the
#: constant now requires editing the recorded measurements, which is a
#: visible, arguable act; `test_the_recorded_worst_case_is_READ_FROM_the_
#: measurement_file` fails if the derivation is replaced by a literal.
MEASURE_PEAK_RESULTS = (
    Path(__file__).parent / "measurements" / "measure_peak_results.json"
)


def worst_measured_bytes_per_context(path: Path = MEASURE_PEAK_RESULTS) -> float:
    """Worst net bytes/context over the recorded measurement rows.

    RAISES rather than falling back to a literal if the file is missing,
    unreadable or empty. A default would re-create exactly the defect this
    replaces: the constant would go back to being pinned by a number typed
    beside it, while the test still appeared to read a measurement.

    Does NOT check each row against its own raw observations -- that is
    `validate_measurement_file`, called from `main` before a build and
    asserted directly by the tests. The split is deliberate and the reason is
    mutation attribution: this function runs at import, and a check that
    raises at import takes the whole test MODULE out at collection time. A
    collection error names no test, so the harness cannot attribute the kill
    and scores the row MISLABELLED -- the evidence that the guard works is
    lost precisely when the guard works. Validation that a named test can fail
    on is worth more here than validation one frame earlier.
    """
    import json

    if not path.exists():
        raise ValueError(
            f"the recorded peak-RSS measurement {path} is missing; "
            "`MEASURED_BYTES_PER_CONTEXT` is pinned against this file and a "
            "fallback literal would restore the self-referential pin a "
            "coordinated 161/161.0 edit walked through. Re-run "
            "`.superpowers/sdd/2026-09-28-occurrence-prior/measure_peak.py` "
            "and commit its output here"
        )
    rows = json.loads(path.read_text(encoding="utf-8"))
    values = [
        float(row["bytes_per_context_net"])
        for row in rows
        if "bytes_per_context_net" in row
    ]
    if not values:
        raise ValueError(
            f"the recorded peak-RSS measurement {path} carries no "
            "`bytes_per_context_net` row, so it pins nothing"
        )
    return max(values)


def validate_measurement_file(path: Path = MEASURE_PEAK_RESULTS) -> None:
    """Refuse a measurement file whose rows disagree with their own numbers.

    EACH ROW IS CHECKED AGAINST ITS OWN RAW OBSERVATIONS. Committing the file
    stops a coordinated edit from retyping two numbers in the source, but on
    its own it just moves the retyping into the JSON -- `bytes_per_context_net:
    161.0` would pin the constant at 161 with no test able to object. The
    derived figure is redundant with `net_peak_bytes` and `contexts`, and
    `contexts` is redundant with the grid shape, so falsifying the ratio
    requires fabricating a coherent measurement (a peak RSS, a grid, and a
    context count that multiply out) rather than editing one number. That is
    the "visible, arguable act" the pin is supposed to demand.

    Called from `main` before any build work, so an operator whose measurement
    file has been tampered with is refused at the start of a job rather than
    after the expensive stage.
    """
    import json

    rows = json.loads(path.read_text(encoding="utf-8"))
    for i, row in enumerate(rows):
        _check_measurement_row(path, i, row)


def _check_measurement_row(path: Path, i: int, row: dict) -> None:
    """Reject a measurement row inconsistent with its own raw observations.

    Rejects rather than recomputing: a row whose stated ratio disagrees with
    its own peak and context count is not a row with one bad field, it is a
    row of unknown provenance, and silently preferring the recomputed value
    would be exactly the silent repair at a boundary the house rules forbid.
    """
    needed = ("n_species", "n_cells", "contexts", "net_peak_bytes",
              "bytes_per_context_net")
    if any(k not in row for k in needed):
        return
    contexts = int(row["contexts"])
    expected_contexts = int(row["n_species"]) * int(row["n_cells"]) * WEEKS_PER_YEAR
    if contexts != expected_contexts:
        raise ValueError(
            f"{path} row {i}: `contexts` is {contexts} but the recorded grid "
            f"{row['n_species']}x{row['n_cells']}x{WEEKS_PER_YEAR} contains "
            f"{expected_contexts}. The context count is what the bytes/context "
            "ratio is divided by, so a row that disagrees with its own grid "
            "pins an arbitrary number"
        )
    if contexts <= 0:
        raise ValueError(f"{path} row {i}: `contexts` is {contexts}")
    stated = float(row["bytes_per_context_net"])
    recomputed = float(row["net_peak_bytes"]) / contexts
    if abs(stated - recomputed) > 1e-6 * max(1.0, abs(recomputed)):
        raise ValueError(
            f"{path} row {i}: `bytes_per_context_net` is {stated} but "
            f"{row['net_peak_bytes']} bytes over {contexts} contexts is "
            f"{recomputed}. `MEASURED_BYTES_PER_CONTEXT` is pinned against "
            "this ratio, so a stated value that does not follow from the "
            "row's own peak RSS pins nothing"
        )


WORST_MEASURED_BYTES_PER_CONTEXT = worst_measured_bytes_per_context()


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


#: Schema version of the manifest written beside each resolution's artifacts.
#: Bumped when the SHAPE changes; `check_manifest` refuses a version it does
#: not know rather than guessing, because a manifest it half-understands is
#: worse than none (it would report a complete set from fields it skipped).
#:
#: 2 adds the `documentation` entry. The bump is what makes the addition
#: FAIL CLOSED: a version-1 manifest carries no documentation entry, and a
#: reader that merely skipped the absent key would authenticate the very
#: release this round exists to stop shipping -- artifacts with no decoding
#: contract beside them.
MANIFEST_VERSION = 2

#: The release's decoding contract, and the name it ships under.
#:
#: WHY THE BUILD COPIES IT (the round-5 packaging obligation, closed here)
#: ----------------------------------------------------------------------
#: `docs/prior-artifact-format.md` is what lets a third party with numpy and
#: the standard library decode the `.npz` and its sidecar CORRECTLY -- the
#: column packing, the rule that an unstored context is the floor and not
#: zero, the shrinkage constants and what they cost, and the CC BY 4.0
#: attribution the licence obliges. Until now nothing copied it beside the
#: files `save_prior` writes: every reference to it lived in a test reading it
#: out of the repository. An upload of `output/priors/` therefore shipped
#: artifacts whose format was documented only in a git checkout the consumer
#: does not have.
#:
#: So the build writes it into the output directory and the manifest accounts
#: for it exactly as it accounts for everything else: by NAME, with the rule
#: that an absent entry means an incomplete release.
#:
#: ONE COPY PER OUTPUT DIRECTORY, not one per resolution. A `--resolutions 3 4
#: 5` sweep writes the same bytes to the same path three times; that is
#: idempotent, and it is what a consumer of the directory wants (one document,
#: not one per resolution with nothing saying they are identical).
FORMAT_DOC_SOURCE = (
    Path(__file__).resolve().parent.parent / "docs" / "prior-artifact-format.md"
)
FORMAT_DOC_NAME = FORMAT_DOC_SOURCE.name


def format_documentation_bytes() -> bytes:
    """The decoding contract's bytes, or a refusal naming the missing file.

    RAISES rather than shipping a stub or skipping the copy. A release whose
    format documentation is absent is the defect this closes; substituting a
    placeholder, or writing the artifacts anyway, would reinstate it while
    looking discharged -- and the manifest would then attest to a document
    that says nothing.
    """
    if not FORMAT_DOC_SOURCE.exists():
        raise ValueError(
            f"the release format documentation {FORMAT_DOC_SOURCE} is missing; "
            "refusing to write an artifact set with no decoding contract "
            "beside it. Every artifact is released CC BY 4.0 and its sidecar "
            "is the only thing saying what a value means, what an unstored "
            "context is, and how hard the estimate was shrunk -- a consumer "
            "who has the files and not this document cannot decode them "
            "correctly and cannot know they got it wrong"
        )
    return FORMAT_DOC_SOURCE.read_bytes()


def documentation_digest(payload: bytes) -> str:
    """The sha256 the manifest records for the shipped documentation.

    A CONTENT hash, because the failure a name cannot see is DRIFT: the copy
    beside the artifacts is a copy, and a copy edited in place -- or truncated
    by a failed write -- still has the right name. The artifacts themselves
    are not hashed here; each one already carries a sidecar whose `nnz` the
    reader checks against the matrix, and hashing multi-hundred-megabyte
    `.npz` files on every build buys less than it costs.
    """
    return hashlib.sha256(payload).hexdigest()


def copy_format_documentation(out_dir: Path) -> Path:
    """Put the decoding contract BESIDE the artifacts. Returns the path written.

    Called before the first artifact of a resolution and therefore before the
    manifest, which is still the LAST file to appear: `write_manifest` hashes
    the bytes of the file this wrote, so it CANNOT run first. The ordering
    that makes a truncated build detectable is a data dependency here rather
    than a convention someone has to remember.
    """
    payload = format_documentation_bytes()
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / FORMAT_DOC_NAME
    path.write_bytes(payload)
    return path


def artifact_stem(source: str, resolution: int) -> str:
    """The filename stem every artifact of one resolution shares.

    One definition, used by the writer, by the manifest and by
    `check_manifest`. Inlined in two places it drifts silently: a manifest
    naming `prior_dof_r4_*` beside artifacts named something else reports a
    complete set as missing and a truncated one as fine.
    """
    return f"prior_{source}_r{resolution}"


def manifest_path(out_dir: Path, source: str, resolution: int) -> Path:
    """Where this resolution's manifest lives."""
    return out_dir / f"{artifact_stem(source, resolution)}_manifest.json"


#: Controls a RESTRICTED release does not ship, with the reason recorded in
#: its manifest's `omitted_controls`. See `_write_release` (job 6787_3).
RESTRICTED_OMITTED_CONTROLS = {
    "separable": (
        "not defined on a restricted prior: the query set is not a cell x "
        "week product, so the outer product of its marginals is unbounded "
        "(2.67 > 1.0 on the global EOD query set at r4, job 6787) and "
        "clamping is refused; no consumer schedules this control"
    ),
}


def write_manifest(
    out_dir: Path,
    source: str,
    resolution: int,
    written: Sequence[Path],
    gbif_doi: str,
    omitted_controls: dict[str, str] | None = None,
) -> Path:
    """Record every artifact this resolution is supposed to contain. I3.

    MANIFEST, NOT ATOMICITY -- AND WHAT THAT DELIBERATELY KEEPS
    -----------------------------------------------------------
    The defect: a failure part-way through one resolution leaves a
    fully-formed SUBSET on disk. Forcing an exception in the third control
    left `prior_dof_r4_{full,uniform,marginal}.{npz,json}` -- six files, each
    individually valid, each correctly provenanced, with nothing on disk
    recording that the remaining controls are missing. A consumer globbing
    the output directory cannot tell a complete release from a truncated one.

    Two fixes were available.

    ATOMICITY (build into a temp directory, rename on success) was REJECTED,
    for three reasons. (1) It is not actually atomic for a SET: POSIX
    guarantees `rename` for one path, so publishing five `.npz` + five `.json`
    is five-to-ten renames and a crash between them leaves the same partial
    set one directory along. A single directory rename would be atomic, but it
    changes the published layout from `--out/*.npz` to `--out/r4/*.npz`, which
    is the artifact format a third party consumes. (2) It doubles peak disk
    for the largest artifacts this pipeline produces, at the resolutions where
    the operator is already watching memory. (3) A crash still leaves the temp
    directory, so a "clean" output directory is achieved by leaving mess
    somewhere else, unnamed.

    THE MANIFEST is therefore the mechanism, and THIS IS THE FAILURE MODE IT
    KEEPS: a failed build STILL LEAVES ITS PARTIAL ARTIFACTS ON DISK. They are
    not removed, and a consumer that never checks is exactly as fooled as
    before. What changes is that the truncation is now DETECTABLE by a
    mechanical check rather than by knowing how many controls there ought to
    be -- `check_manifest` is that check, and it fails closed on an absent
    manifest.

    The manifest is deleted FIRST and written LAST. That ordering is the whole
    protocol and it closes the stale-manifest hole: re-running a failing build
    into a directory that already holds a COMPLETE set of the same resolution
    would otherwise overwrite artifacts one by one under a manifest still
    attesting to the old, complete set, and the check would pass over a
    directory whose contents are now a mixture of two builds.

    IT NAMES THE ARTIFACTS, IT DOES NOT COUNT THEM. The list is derived from
    the paths actually written, which come from `CONTROLS`. A fifth control
    added to that registry appears in the manifest with no edit here, and
    `check_manifest` then requires it. Nothing in this file hardcodes four,
    five, or the current control names.

    IT ALSO ACCOUNTS FOR THE FORMAT DOCUMENTATION, BY NAME AND BY CONTENT
    --------------------------------------------------------------------
    The decoding contract is part of the release, not a repository file: an
    artifact set shipped without it is undecodable by the third party it is
    published for. So it gets a `documentation` entry under the same protocol
    as everything else -- a missing entry means an incomplete release -- plus a
    `sha256`, because a NAME cannot see the one failure a copy has and an
    original does not: drift. The bytes are re-read HERE rather than taken from
    the caller, so the hash describes the file that is on disk at the moment
    the manifest attests to it.
    """
    import json

    path = manifest_path(out_dir, source, resolution)
    artifacts = []
    for npz in written:
        artifacts.append(
            {
                "npz": npz.name,
                "sidecar": npz.with_suffix(".json").name,
            }
        )
    doc = out_dir / FORMAT_DOC_NAME
    if not doc.exists():
        raise ValueError(
            f"the format documentation {doc} is not beside the artifacts this "
            "manifest is about to attest to; the manifest is the completion "
            "record for the whole release and a set with no decoding contract "
            "is not complete. `copy_format_documentation` writes it before the "
            "first artifact, so this means it was removed or never ran"
        )
    payload = {
        "manifest_version": MANIFEST_VERSION,
        "source": source,
        "resolution": resolution,
        "stem": artifact_stem(source, resolution),
        "gbif_doi": gbif_doi,
        # NAMED, never counted. `artifact_count` is recorded too, but
        # `check_manifest` verifies the NAMES: a count would be satisfied by
        # any five files, including five copies of the full prior.
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        # The release's decoding contract. `sha256` is what a name cannot do:
        # detect that the shipped copy drifted from the document this build
        # published. Read off disk, so the hash is of the shipped bytes.
        "documentation": {
            "path": doc.name,
            "sha256": documentation_digest(doc.read_bytes()),
        },
    }
    # Only when non-empty, so an unrestricted manifest is unchanged. A
    # control absent from `artifacts` with no entry here is a truncated set;
    # with one, it is a stated omission (restricted releases, job 6787_3).
    if omitted_controls:
        payload["omitted_controls"] = dict(sorted(omitted_controls.items()))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def check_manifest(out_dir: Path, source: str, resolution: int) -> list[Path]:
    """Verify a resolution's artifact set against its manifest. I3's reader.

    Returns the verified `.npz` paths. Raises `ValueError` naming the specific
    check otherwise.

    FAILS CLOSED ON AN ABSENT MANIFEST. That is the case the finding is about:
    a build that died part-way leaves artifacts and no manifest, so "no
    manifest" must mean "not a release", never "nothing to check". A consumer
    reading an artifact directory written by an older version of this script
    gets the same refusal, which is correct -- that directory genuinely
    carries no completion record.

    The `.json` sidecar is checked beside every `.npz`, because `save_prior`
    writes both and `load_prior` requires both: an `.npz` whose sidecar never
    landed is a truncated artifact rather than a truncated set, and the same
    mid-resolution failure produces it.

    THE FORMAT DOCUMENTATION IS CHECKED THE SAME WAY, AND ALSO BY HASH
    -----------------------------------------------------------------
    A set whose decoding contract is missing is incomplete in the sense that
    matters to the only audience the release has: a third party holding the
    files and nothing else cannot decode them correctly and cannot tell that
    they got it wrong. Absence fails closed exactly like a missing control.

    The `sha256` is checked too, which nothing else here needs: the artifacts
    are originals, the document is a COPY, and a copy can drift while keeping
    its name -- edited in place beside the artifacts, or left truncated by a
    failed write. A name check cannot see either.
    """
    import json

    path = manifest_path(out_dir, source, resolution)
    if not path.exists():
        raise ValueError(
            f"no artifact manifest at {path}: an artifact set without one is "
            "not a complete release. A build that failed part-way through a "
            "resolution leaves a fully-formed SUBSET on disk -- individually "
            "valid, correctly provenanced files with nothing recording which "
            "artifacts are missing -- and the manifest is written LAST, so "
            "its absence is exactly that state"
        )
    meta = json.loads(path.read_text(encoding="utf-8"))
    if meta.get("manifest_version") != MANIFEST_VERSION:
        raise ValueError(
            f"artifact manifest {path} has manifest_version "
            f"{meta.get('manifest_version')!r}, but this build understands "
            f"{MANIFEST_VERSION}; refusing to half-read it, because a "
            "manifest whose unknown fields are skipped would report a "
            "complete set from the fields that happened to parse"
        )
    if meta.get("resolution") != resolution or meta.get("source") != source:
        raise ValueError(
            f"artifact manifest {path} describes source "
            f"{meta.get('source')!r} resolution {meta.get('resolution')!r}, "
            f"not {source!r} resolution {resolution}; a manifest naming a "
            "different build cannot attest to this one"
        )
    entries = meta.get("artifacts") or []
    if not entries:
        raise ValueError(
            f"artifact manifest {path} names no artifacts at all; an empty "
            "manifest would authenticate an empty directory as a complete "
            "release"
        )
    missing = []
    for entry in entries:
        for key in ("npz", "sidecar"):
            name = entry.get(key)
            if not name or not (out_dir / name).exists():
                missing.append(name or f"<no {key} in manifest entry>")
    recorded_doc = meta.get("documentation") or {}
    documented = recorded_doc.get("path")
    if not documented:
        raise ValueError(
            f"artifact manifest {path} names no format documentation; the "
            "release's decoding contract is part of the release, and a "
            "manifest that does not account for it would authenticate a set "
            "of `.npz` files a third party cannot correctly decode"
        )
    doc_path = out_dir / documented
    if not doc_path.exists():
        missing.append(documented)
    if missing:
        raise ValueError(
            f"artifact manifest {path} names {len(entries)} artifact(s) but "
            f"{len(missing)} file(s) are missing from {out_dir}: "
            f"{sorted(missing)}. This is a truncated artifact set: each file "
            "present is individually valid and correctly provenanced, which "
            "is precisely why the absence cannot be seen without the manifest"
        )
    recorded_digest = recorded_doc.get("sha256")
    actual_digest = documentation_digest(doc_path.read_bytes())
    if recorded_digest != actual_digest:
        raise ValueError(
            f"the format documentation {doc_path} has sha256 {actual_digest} "
            f"but the manifest records {recorded_digest!r}; the shipped "
            "decoding contract is not the document this build published. A "
            "copy keeps its name when it is edited in place or left truncated "
            "by a failed write, so only the content hash can see this, and a "
            "consumer decoding against a drifted contract gets plausible "
            "wrong numbers rather than an error"
        )
    return [out_dir / entry["npz"] for entry in entries]


def build_from_archive(
    zip_path: Path,
    *,
    resolution: int,
    source: str,
    test_recordings: Sequence[TestRecording],
    out_dir: Path,
    gbif_doi: str,
    species_chunk: int = DEFAULT_SPECIES_CHUNK,
    min_split_match_rate: float = DEFAULT_MIN_SPLIT_MATCH_RATE,
    backoff_params: BackoffParams = BackoffParams(),
    smoothing_params: SmoothingParams = SmoothingParams(),
    split_contributor_mode: str = "match",
    occurrences: pa.Table | None = None,
    query_set: QuerySet | None = None,
    counts_identity: CountsIdentity | None = None,
) -> list[Path]:
    """Archive -> full prior + every registered control. Returns written paths.

    *query_set* restricts the ESTIMATION to the queried contexts (exactly;
    see the module docstring, "OPTION (b)"). *counts_identity*, when given,
    checkpoints the guarded counts under it once the guards have passed.

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

    THE LEAKAGE ESCALATION READS RETURNED COUNTS, AND IT HAS TWO CLAUSES
    -------------------------------------------------------------------
    `apply_leakage_guards` returns a `LeakageGuardResult`. Both clauses below
    are comparisons on its integers. Neither is a log handler, neither is a
    string match, and neither is conditioned on any logging state; see
    `ZeroMatchGuardError` for the reproduced failure that motivated the data
    channel.

    * CLAUSE 1, `guarded.zero_match` -- NOT ONE key bit. Unconditional: no
      flag can disable it, because a supplied split that removed nothing is
      never a release.
    * CLAUSE 2, the per-key RATE -- too few keys bit. This is the clause C1
      added, and it exists because clause 1 is a claim about the whole split
      that ONE live key satisfies for an arbitrarily large one. See
      `PartialMatchGuardError` and `DEFAULT_MIN_SPLIT_MATCH_RATE`.

    Clause 2 carries its own `banned_key_count > 0` conjunct rather than
    relying on clause 1 having already returned: `--no-test-split` reaches
    this code with zero banned keys, `match_rate` is undefined there and
    RAISES rather than inventing a 0/0, and the legitimate no-split mode must
    stay buildable. The two conjuncts are separately mutated for that reason.

    BOTH CLAUSES ARE MODE-AWARE IN WORDING ONLY (job 5345). Under
    `split_contributor_mode="placeholder"` the guard's banned key is
    (cell, week) -- the split's `recorded_by` values are test-set names, not
    people, so the (cell, week, contributor) join is STRUCTURALLY empty and
    clause 1 fires after the whole parse has been paid for. The integers the
    clauses compare are the same two fields in both modes; what changes is
    their denominator (distinct cell-weeks instead of distinct triples) and
    the key shape the messages name, via `leakage.key_shape`, so the
    operator triages the join that actually ran. The match-mode zero-match
    message now also NAMES the placeholder mode: it is the structural
    failure's one observable, and an operator who can only learn the escape
    hatch by reading this source pays a re-parse to find it.

    OCCURRENCES MAY ARRIVE PRE-PARSED (the checkpoint path). When
    *occurrences* is passed it is the (species_key, cell, week,
    publisher_key, recorded_by, n) aggregate for THIS resolution --
    `parallel_parse.PARSE_SCHEMA` -- and the archive is not read at all.
    The guards key on exactly those columns, and `aggregate_counts` sums
    `n` as a weight, so the guarded result is identical to the
    per-occurrence path; `zip_path` stays in the signature because it is
    what the provenance log names. Guard row counts then count GROUPS, and
    the escalation messages say "occurrence row(s)" advisedly: a removed
    group is all of its occurrences.

    THE ARTIFACT SET IS RECORDED IN A MANIFEST (I3)
    ----------------------------------------------
    See `write_manifest`: the manifest is written LAST, after every artifact
    of this resolution, so its presence is the completion record and its
    absence marks a truncated set. It names the artifacts rather than counting
    them, so a control added to `CONTROLS` is carried automatically.
    """
    if not 0.0 <= min_split_match_rate <= 1.0:
        raise ValueError(
            f"min_split_match_rate must be a fraction in 0..1, got "
            f"{min_split_match_rate!r}; it is the share of the split's banned "
            "keys that must bite, so five percent is 0.05 and not 5. Rejected "
            "rather than clamped, because clamping either way silently "
            "changes what the leakage guard demands"
        )
    table = (
        occurrences
        if occurrences is not None
        else assign_cells(read_occurrences(Path(zip_path)), resolution)
    )
    guarded = apply_leakage_guards(table, test_recordings, split_contributor_mode)

    # DATA, not a log record. `zero_match` is
    # `banned_key_count > 0 and contributor_rows_removed == 0`, both computed
    # by the function that did the filtering. `test_recordings` is NOT part of
    # the condition: `banned_key_count` is the normalised key count the guard
    # actually held, which is the thing that decides whether the guard could
    # bite, and re-deriving intent from the argument here would let a split
    # that normalised away to nothing pass as legitimately empty.
    shape = key_shape(split_contributor_mode)
    if guarded.zero_match:
        raise ZeroMatchGuardError(
            f"the test split matched no occurrence at resolution {resolution} "
            f"in {split_contributor_mode!r} contributor mode: "
            f"{guarded.banned_key_count} banned {shape} "
            f"key(s) removed {guarded.contributor_rows_removed} of "
            f"{guarded.rows_before_contributor_guard} occurrence row(s). A split "
            "was supplied, so this is a build failure rather than a warning: the "
            "artifact would carry a leakage guard that removed nothing, and "
            "would be indistinguishable from one whose guard worked. Check the "
            "H3 resolution and cell formatting of the split -- and note that "
            f"the Xeno-Canto guard ran FIRST and removed "
            f"{guarded.xeno_canto_rows_removed} row(s), so on a split whose "
            "recordings are themselves Xeno-Canto occurrences guard 1 may "
            "legitimately have consumed the rows guard 2 would have matched. "
            "IF THE SPLIT'S recorded_by VALUES ARE PLACEHOLDERS (a test-set "
            "name like 'HSN' standing in for contributor-less soundscape "
            "recordings), the (cell, week, contributor) join is structurally "
            "empty against any real source and no formatting fix can help: "
            "re-run with --split-contributor-mode placeholder, which bans "
            "every occurrence in the test sites' (cell, week)s instead"
        )

    # CLAUSE 2. Its own `banned_key_count > 0` conjunct: `--no-test-split`
    # arrives here with no keys, where a rate is undefined rather than zero.
    if guarded.banned_key_count > 0 and guarded.match_rate < min_split_match_rate:
        raise PartialMatchGuardError(
            f"only {guarded.matched_key_count} of {guarded.banned_key_count} "
            f"banned {shape} key(s) matched any occurrence "
            f"at resolution {resolution} in {split_contributor_mode!r} "
            f"contributor mode -- a match rate of "
            f"{guarded.match_rate:.4f}, below the required "
            f"{min_split_match_rate:.4f}. The split removed "
            f"{guarded.contributor_rows_removed} of "
            f"{guarded.rows_before_contributor_guard} occurrence row(s), so the "
            "total zero-match check passed and would have released this "
            "artifact: one live key disarms a whole-split condition. A "
            "coordinate bug, a contributor-spelling convention that changed "
            "mid-file and a mojibaked name column all leave exactly this "
            "signature. The Xeno-Canto guard removed "
            f"{guarded.xeno_canto_rows_removed} row(s) first and may "
            "legitimately have consumed some of them. If the split really is "
            "this sparse and you accept it, say so explicitly with "
            "--min-split-match-rate"
        )

    counts = aggregate_counts(guarded.table)
    guard = guard_record(guarded)
    # AFTER both escalation clauses, BEFORE any estimation: the guarded
    # counts are what every later stage is a function of, and the stage the
    # fleet iterates on (estimation, restriction) is cheap beside the parse
    # and guards that produced them. Only when the caller names an identity:
    # `main` always does; a library caller opts in.
    if counts_identity is not None:
        write_counts_checkpoint(
            counts_checkpoint_path(out_dir, source, resolution),
            counts,
            identity=counts_identity,
            guard=guard,
        )
    return build_from_counts(
        counts,
        guard,
        resolution=resolution,
        source=source,
        out_dir=out_dir,
        gbif_doi=gbif_doi,
        species_chunk=species_chunk,
        backoff_params=backoff_params,
        smoothing_params=smoothing_params,
        query_set=query_set,
    )


def guard_record(guarded) -> dict:
    """The guard's returned counts, as the dict the counts checkpoint records."""
    return {
        "banned_key_count": guarded.banned_key_count,
        "matched_key_count": guarded.matched_key_count,
        "xeno_canto_rows_removed": guarded.xeno_canto_rows_removed,
        "contributor_rows_removed": guarded.contributor_rows_removed,
        "rows_before_contributor_guard": guarded.rows_before_contributor_guard,
        "rows_after_guards": guarded.table.num_rows,
    }


def check_resumed_guards(
    guard: dict,
    resolution: int,
    min_split_match_rate: float,
    split_contributor_mode: str,
) -> None:
    """Both escalation clauses, re-run on a checkpoint's RECORDED guard counts.

    A resumed build skips the guards, not their verdict. Clause 1 cannot
    newly fire (the checkpoint was only written after it passed), but it is
    re-checked anyway because the record is a file; clause 2 depends on
    `--min-split-match-rate`, which the resumed invocation may set stricter
    than the one that wrote the checkpoint.
    """
    shape = key_shape(split_contributor_mode)
    banned = int(guard["banned_key_count"])
    matched = int(guard["matched_key_count"])
    if banned > 0 and int(guard["contributor_rows_removed"]) == 0:
        raise ZeroMatchGuardError(
            f"the resumed guarded-counts checkpoint records that the test split "
            f"matched no occurrence at resolution {resolution}: {banned} banned "
            f"{shape} key(s) removed nothing. Re-run without --resume-counts"
        )
    if banned > 0 and matched / banned < min_split_match_rate:
        raise PartialMatchGuardError(
            f"the resumed guarded-counts checkpoint records that only {matched} "
            f"of {banned} banned {shape} key(s) matched any occurrence at "
            f"resolution {resolution} in {split_contributor_mode!r} contributor "
            f"mode -- a match rate of {matched / banned:.4f}, below the required "
            f"{min_split_match_rate:.4f}. If the split really is this sparse and "
            "you accept it, say so explicitly with --min-split-match-rate"
        )


def build_from_counts(
    counts: pa.Table,
    guard: dict,
    *,
    resolution: int,
    source: str,
    out_dir: Path,
    gbif_doi: str,
    species_chunk: int = DEFAULT_SPECIES_CHUNK,
    backoff_params: BackoffParams = BackoffParams(),
    smoothing_params: SmoothingParams = SmoothingParams(),
    query_set: QuerySet | None = None,
) -> list[Path]:
    """GUARDED counts -> full prior + every registered control. Returns paths.

    The second half of `build_from_archive`, and the whole of a
    `--resume-counts` build. With *query_set* the estimation is restricted
    (see the module docstring, "OPTION (b)"); without it this is the full
    52-week grid, byte for byte the path the DOF priors were built with.
    """
    ratios = target_group_ratios(counts)
    species = species_axis(counts)
    cells = cell_axis(counts)
    if query_set is not None:
        full = _restricted_full_prior(
            ratios,
            species,
            cells,
            query_set,
            guard,
            resolution=resolution,
            source=source,
            species_chunk=species_chunk,
            backoff_params=backoff_params,
            smoothing_params=smoothing_params,
        )
        full.gbif_doi = gbif_doi
        return _write_release(
            full, out_dir, source, resolution, gbif_doi,
            backoff_params, smoothing_params,
        )
    log.info(
        "resolution %d: %d occurrences (%d dropped by the leakage guards over "
        "%d of %d banned key(s)), %d species, %d cells -> %d grid "
        "contexts over %d weeks, projected peak ~%.1f GiB",
        resolution,
        guard["rows_after_guards"],
        guard["xeno_canto_rows_removed"] + guard["contributor_rows_removed"],
        guard["matched_key_count"],
        guard["banned_key_count"],
        len(species),
        len(cells),
        len(species) * len(cells) * WEEKS_PER_YEAR,
        WEEKS_PER_YEAR,
        project_peak_bytes(len(species), len(cells)) / (1 << 30),
    )

    estimates = estimate_over_full_grid(
        ratios,
        species,
        cells,
        species_chunk=species_chunk,
        backoff_params=backoff_params,
        smoothing_params=smoothing_params,
    )

    spec = PriorSpec(resolution=resolution, source=source, variant="full")
    # The params reach the ARTIFACT, not just the computation (I2). They are
    # passed to `to_tensor` rather than set on the tensor afterwards so there
    # is no window in which a full prior exists with no record of how it was
    # shrunk -- and so the ONE object handed to `hierarchical_estimate` is the
    # one that lands in the sidecar.
    full = to_tensor(
        estimates,
        spec,
        backoff_params=backoff_params,
        smoothing_params=smoothing_params,
    )
    full.gbif_doi = gbif_doi
    return _write_release(
        full, out_dir, source, resolution, gbif_doi,
        backoff_params, smoothing_params,
    )


def _restricted_full_prior(
    ratios: pa.Table,
    species: list[int],
    cells: list[str],
    query_set: QuerySet,
    guard: dict,
    *,
    resolution: int,
    source: str,
    species_chunk: int,
    backoff_params: BackoffParams,
    smoothing_params: SmoothingParams,
):
    """The full prior, restricted to *query_set* at *resolution*. Exact."""
    plan = plan_restriction(query_set.contexts_at(resolution), cells)
    if not plan.on_grid_contexts:
        raise ValueError(
            f"the query set {query_set.path} touches no OBSERVED cell at "
            f"resolution {resolution} ({plan.counts['query_contexts']} query "
            "contexts, none on the observed-cell axis); a restricted artifact "
            "would store nothing. Check that the query set and the archive "
            "cover the same region"
        )
    stats = PointStatistics(ratios)
    log.info(
        "resolution %d: RESTRICTED to query set %s (sha256 %s): %d occurrences "
        "after the leakage guards (%d dropped over %d of %d banned key(s)), "
        "%d species, %d observed cells; %d query contexts over %d cells, %d "
        "on the observed grid (%d off it); halo %d cells, %d support points "
        "-> %d stored contexts instead of the full grid's %d, projected peak "
        "~%.1f GiB (ratio rows %d, species chunk %d; parse and guards not "
        "included)",
        resolution,
        query_set.path,
        query_set.sha256,
        guard["rows_after_guards"],
        guard["xeno_canto_rows_removed"] + guard["contributor_rows_removed"],
        guard["matched_key_count"],
        guard["banned_key_count"],
        len(species),
        len(cells),
        plan.counts["query_contexts"],
        plan.counts["query_cells"],
        plan.counts["on_grid_contexts"],
        plan.counts["off_grid_contexts"],
        plan.counts["halo_cells"],
        plan.counts["support_points"],
        len(species) * plan.counts["on_grid_contexts"],
        len(species) * len(cells) * WEEKS_PER_YEAR,
        project_restricted_peak_bytes(
            len(species),
            plan.counts["on_grid_contexts"],
            plan.counts["support_points"],
            stats.n_ratio_rows,
            species_chunk,
        )
        / (1 << 30),
        stats.n_ratio_rows,
        species_chunk,
    )
    block = estimate_over_query_set(
        ratios,
        species,
        plan,
        species_chunk=species_chunk,
        backoff_params=backoff_params,
        smoothing_params=smoothing_params,
        stats=stats,
    )
    restriction = Restriction(
        query_set_path=query_set.path,
        query_set_sha256=query_set.sha256,
        contexts=tuple(plan.query_contexts),
        counts={**plan.counts, "query_set_rows": len(query_set)},
    )
    return restricted_tensor(
        block,
        species,
        plan.on_grid_contexts,
        PriorSpec(resolution=resolution, source=source, variant="full"),
        restriction,
        backoff_params=backoff_params,
        smoothing_params=smoothing_params,
    )


def _write_release(
    full,
    out_dir: Path,
    source: str,
    resolution: int,
    gbif_doi: str,
    backoff_params: BackoffParams,
    smoothing_params: SmoothingParams,
) -> list[Path]:
    """The full prior, every registered control, the doc and the manifest."""
    written: list[Path] = []
    stem = artifact_stem(source, resolution)
    # DELETED FIRST, written LAST. A manifest surviving from an earlier,
    # COMPLETE build of this resolution would otherwise attest to the set
    # being overwritten right now, and a build that failed half way through
    # would leave a mixture of two builds under a manifest that verifies.
    manifest_path(out_dir, source, resolution).unlink(missing_ok=True)
    # BEFORE the first artifact. The decoding contract ships WITH the files,
    # not in a git checkout the consumer does not have (the round-5 packaging
    # obligation). Before, rather than after, so a build that dies part-way
    # leaves a directory that is honest about itself: partial artifacts, the
    # document that describes them, and NO manifest. Writing it after the
    # artifacts would add a second way to get a complete-looking set with no
    # contract beside it.
    copy_format_documentation(out_dir)
    written.append(save_prior(full, out_dir / f"{stem}_full.npz"))
    omitted: dict[str, str] = {}
    for name, make in CONTROLS.items():
        if full.restriction is not None and name in RESTRICTED_OMITTED_CONTROLS:
            # Job 6787_3: on the global EOD query set the separable outer
            # product reached 2.67 > 1.0. A query set is not a cell x week
            # product, so the product of its marginals is unbounded by
            # construction, and `controls` (rightly) refuses to clamp. No
            # consumer schedules it (phase_c_run.PRIOR_VARIANTS; phase_b's
            # four controls), so it is OMITTED -- by name, with the reason,
            # in the manifest -- rather than failing the release.
            omitted[name] = RESTRICTED_OMITTED_CONTROLS[name]
            log.warning(
                "resolution %d: control %r omitted: %s",
                resolution, name, omitted[name],
            )
            continue
        control = make(full)
        # Belt and braces beside `_respec`'s pass-through: this script is what
        # KNOWS the DOI, and a control shipped without it is as uncitable as a
        # full prior without it. `save_prior` refuses an empty one either way,
        # so a regression in `_respec` fails the build rather than releasing
        # an unattributed artifact.
        control.gbif_doi = gbif_doi
        # NOT belt and braces: `_respec` carries `gbif_doi` and does NOT carry
        # the params, so these two lines are the ONLY thing putting them on a
        # control artifact. Stated rather than claimed redundant, because a
        # comment asserting a redundancy that is not there is what gets the
        # line it defends deleted.
        #
        # They describe the BUILD -- the shrinkage that produced the full prior
        # this control is the null for -- not the control's own (destroyed)
        # structure. That is the reading that makes them useful: a control and
        # the prior it is compared against must be known to have come from the
        # same constants, or the comparison measures the constants.
        control.backoff_params = backoff_params
        control.smoothing_params = smoothing_params
        written.append(save_prior(control, out_dir / f"{stem}_{name}.npz"))

    # LAST, after every artifact of this resolution. See `write_manifest`.
    manifest = write_manifest(
        out_dir, source, resolution, written, gbif_doi, omitted_controls=omitted
    )

    log.info(
        "resolution %d: wrote %d artifacts to %s (manifest %s); nnz=%d of "
        "dense capacity %d; peak RSS %.1f MiB",
        resolution,
        len(written),
        out_dir,
        manifest.name,
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
            "or wrapped in a resolver URL). CHECK FOR SURROUNDING OR INTERNAL "
            "WHITESPACE FIRST: a DOI pasted off a citation line commonly "
            "carries a leading or trailing space, or a `DOI: ` label, and all "
            "three land here rather than looking malformed. The value is NOT "
            "trimmed for you -- the sidecar must carry the exact string the "
            "operator attested to, and silently repairing an attribution "
            "field means the artifact cites something the build was never "
            "told. Otherwise: a GBIF download has BOTH a "
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
    ap.add_argument(
        "--min-split-match-rate",
        type=split_match_rate,
        default=DEFAULT_MIN_SPLIT_MATCH_RATE,
        help="minimum FRACTION of the test split's banned (cell, week, "
        "contributor) keys that must remove at least one occurrence "
        "(default: %(default)s). Below it the build FAILS: a split where "
        "one key in a hundred bites passes the total zero-match check while "
        "the leakage guard has effectively not run. `0.0` reduces this to "
        "the total check, which cannot be disabled",
    )
    ap.add_argument(
        "--split-contributor-mode",
        choices=list(CONTRIBUTOR_MODES),
        default="match",
        help="how the test split's recorded_by column joins occurrences "
        "(default: %(default)s). `match`: the banned key is (cell, week, "
        "contributor) -- the split names real recordists. `placeholder`: "
        "the split's recorded_by values are TEST-SET NAMES (e.g. 'HSN' for "
        "contributor-less soundscape recordings), no real contributor can "
        "ever equal one, and the banned key is (cell, week): every "
        "occurrence in a test site's cell-week is removed regardless of "
        "recorder -- strictly more conservative. EXPLICIT on purpose: "
        "auto-detecting would decide a released artifact's leakage "
        "semantics from the data it is guarding",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=None,
        help="worker processes for the archive parse (default: all CPUs). "
        "The parse is bit-exact at any worker count; 1 disables the pool",
    )
    ap.add_argument(
        "--resume",
        action="store_true",
        help="skip the parse and start at the leakage guards, using the "
        "checkpoint a previous run of this SAME invocation (source, archive, "
        "resolutions) wrote into --out. A guard refusal leaves the "
        "checkpoint intact, so the corrected re-run costs minutes rather "
        "than the multi-hour parse. A stale checkpoint is REFUSED naming "
        "the mismatched field, never silently rebuilt",
    )
    ap.add_argument(
        "--sequential",
        action="store_true",
        help="parse with the sequential read_occurrences path instead of the "
        "parallel parse_and_aggregate. Jobs 5846/6237: the parallel merge did "
        "not finish on the real EOD archive (a single parent thread at 100%% "
        "for 6h+ after the read, 12h limit, no checkpoint), while the "
        "sequential path completed 1.76B rows in ~6.5h (job 5345). Writes no "
        "parse checkpoint (the checkpoint holds the parallel parse's output), "
        "so it cannot be combined with --resume or --workers",
    )
    ap.add_argument(
        "--query-set",
        type=Path,
        default=None,
        help="restrict the ESTIMATION to the (lat, lon, week) contexts in this "
        "parquet (scripts/build_query_set.py), exactly: the artifact equals the "
        "full build bit for bit on every queried context whose cell was "
        "observed, holds nothing elsewhere, and says so in its sidecar "
        "(restricted=true, the query set's sha256). The parse, the guards and "
        "the RatioTable are NOT restricted. Without it: the full 52-week grid, "
        "unchanged",
    )
    ap.add_argument(
        "--resume-counts",
        action="store_true",
        help="skip the parse AND the leakage guards: start from the guarded "
        "per-resolution counts a previous run wrote into --out "
        "(guarded_counts_{source}_r{res}.parquet). Refused, naming the field, "
        "if the checkpoint was written for another archive, source, "
        "resolution, split file or contributor mode. The escalation clauses "
        "are re-checked on the recorded guard counts",
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
    args = ap.parse_args(argv)
    if args.no_test_split and args.split_contributor_mode != "match":
        # A usage error, through argparse's own channel so it exits 2 like
        # every other one. A contributor MODE for a split that does not
        # exist is an operator contradiction, and silently ignoring the
        # flag would let a command line that LOOKS placeholder-guarded run
        # unguarded.
        ap.error(
            "--split-contributor-mode placeholder requires --test-split: "
            "the mode describes how the split's recorded_by column joins, "
            "and --no-test-split supplies no split to join"
        )
    if args.resume_counts and (
        args.resume or args.sequential or args.workers is not None
    ):
        ap.error(
            "--resume-counts cannot be combined with --resume, --sequential or "
            "--workers: it reads no archive at all, so each of those would be "
            "silently ignored"
        )
    if args.sequential and (args.resume or args.workers is not None):
        ap.error(
            "--sequential cannot be combined with --resume or --workers: "
            "the sequential path neither reads nor writes the parse "
            "checkpoint and uses no worker pool, so either flag would be "
            "silently ignored"
        )
    return args


def split_digest(path: Path | None) -> str | None:
    """sha256 of the test-split FILE's bytes, or None for --no-test-split.

    The guarded counts depend on the split's content; the file hash is the
    identity the counts checkpoint records for it.
    """
    if path is None:
        return None
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


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

    # BEFORE any pipeline call, and before the expensive stage: a measurement
    # file whose rows disagree with their own raw observations pins nothing,
    # and `MEASURED_BYTES_PER_CONTEXT` is derived from it. Checked here rather
    # than at import so a mutant that tampers with the file fails a NAMED test
    # instead of taking out collection, where the harness cannot attribute it.
    validate_measurement_file()

    split = [] if args.no_test_split else load_test_split(args.test_split)
    if args.no_test_split:
        log.warning(
            "building with NO leakage test split (--no-test-split): the "
            "contributor guard will remove nothing and the artifacts will not "
            "be safe to evaluate against any split"
        )
    if args.split_contributor_mode == "placeholder":
        log.warning(
            "split contributor mode PLACEHOLDER: the split's recorded_by "
            "values are treated as test-set names, and the leakage ban is "
            "every occurrence in the test sites' (cell, week)s regardless "
            "of recorder -- strictly wider than contributor matching"
        )

    # THE PARSE IS CHECKPOINTED, AND THE CHECKPOINT OUTLIVES A GUARD REFUSAL
    # ----------------------------------------------------------------------
    # Job 5345: ~6.5h of single-threaded parsing plus ~2.5h of guards, then a
    # correct ZeroMatchGuardError -- and the next attempt had to pay the
    # parse again. The parse result (per-resolution aggregated counts, with
    # provenance columns intact for the guards) is now written to --out
    # BEFORE any guard runs, so the refusal costs the guard phase only, and
    # `--resume` starts the corrected invocation there. The checkpoint is
    # deliberately NOT deleted on success either: it is cheap relative to
    # its archive, and the operator who wants another resolution subset or
    # a different split mode tomorrow is exactly who it exists for.
    query_set = load_query_set(args.query_set) if args.query_set else None
    if query_set is not None:
        log.info(
            "--query-set %s: %d (lat, lon, week) row(s), sha256 %s; the "
            "estimation is RESTRICTED to them",
            query_set.path,
            len(query_set),
            query_set.sha256,
        )
    split_sha256 = split_digest(None if args.no_test_split else args.test_split)

    def identity(resolution: int) -> CountsIdentity:
        return CountsIdentity(
            source=args.source,
            archive=str(Path(args.archive).resolve()),
            resolution=int(resolution),
            split_sha256=split_sha256,
            contributor_mode=args.split_contributor_mode,
        )

    if args.resume_counts:
        # Every resolution's checkpoint is loaded and checked BEFORE any
        # artifact is written, so a stale one for r5 cannot leave r3 and r4
        # rebuilt from a different run's counts beside it.
        resumed = {}
        for resolution in args.resolutions:
            meta, counts = load_counts_checkpoint(
                counts_checkpoint_path(args.out, args.source, resolution),
                identity=identity(resolution),
            )
            check_resumed_guards(
                meta["guard"],
                resolution,
                args.min_split_match_rate,
                args.split_contributor_mode,
            )
            resumed[resolution] = (meta, counts)
        for resolution in args.resolutions:
            meta, counts = resumed[resolution]
            log.info(
                "resolution %d: resumed guarded counts (%d row(s)); skipping "
                "the parse and the leakage guards",
                resolution,
                counts.num_rows,
            )
            paths = build_from_counts(
                counts,
                meta["guard"],
                resolution=resolution,
                source=args.source,
                out_dir=args.out,
                gbif_doi=args.gbif_doi,
                species_chunk=args.species_chunk,
                query_set=query_set,
            )
            for path in paths:
                print(path)
        return

    ckpt = checkpoint_path(args.out, args.source)
    if args.sequential:
        # Jobs 5846/6237: the parallel merge never finished on the real EOD
        # archive. Hand build_from_archive occurrences=None so it takes its
        # own read_occurrences + assign_cells fallback -- EXACTLY the path
        # job 5345 completed over 1.76B rows. It re-reads the archive once
        # per resolution; that is deliberate: holding one resolution's table
        # at a time is the memory profile 5345 actually survived, where one
        # shared read would hold all three at once (~3x the peak).
        log.info(
            "--sequential: build_from_archive's read_occurrences path, once "
            "per resolution (the job-5345 memory profile)"
        )
        tables = {resolution: None for resolution in args.resolutions}
    elif args.resume:
        # Staleness fails closed inside `load_checkpoint`, naming the
        # mismatched field; no try/except here, per this script's uniform
        # rule that a refusal exits non-zero at the raise site.
        meta, tables = load_checkpoint(
            ckpt,
            source=args.source,
            archive=args.archive,
            resolutions=args.resolutions,
        )
        log.info(
            "resumed from checkpoint %s: %d row(s) read in the original "
            "parse, dropped %s; skipping the parse entirely",
            ckpt,
            meta["total_rows_read"],
            meta["dropped"],
        )
    else:
        parsed = parse_and_aggregate(
            args.archive, args.resolutions, workers=args.workers
        )
        log.info(
            "parse_and_aggregate(%s): read %d row(s), dropped %s "
            "(no species key / bad coordinates / unparseable date)",
            args.archive,
            parsed.total,
            parsed.dropped,
        )
        write_checkpoint(
            ckpt,
            parsed,
            source=args.source,
            archive=args.archive,
            resolutions=args.resolutions,
        )
        log.info("parse checkpoint written to %s (BEFORE any guard)", ckpt)
        tables = parsed.tables

    for resolution in args.resolutions:
        # NO try/except around the loop body. A guard ValueError -- most
        # likely an H3 resolution mismatch -- must fail THIS artifact and exit
        # non-zero, rather than being carried so the sweep can write a
        # leak-contaminated file under a name that looks like every other one.
        # The checkpoint written above SURVIVES that exit: nothing here
        # deletes it, which is the entire recovery story.
        paths = build_from_archive(
            args.archive,
            resolution=resolution,
            source=args.source,
            test_recordings=recordings_at_resolution(split, resolution),
            out_dir=args.out,
            gbif_doi=args.gbif_doi,
            species_chunk=args.species_chunk,
            min_split_match_rate=args.min_split_match_rate,
            split_contributor_mode=args.split_contributor_mode,
            occurrences=tables[resolution],
            query_set=query_set,
            counts_identity=identity(resolution),
        )
        for path in paths:
            print(path)


if __name__ == "__main__":
    main()
