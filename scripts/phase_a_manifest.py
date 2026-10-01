#!/usr/bin/env python3
"""Build the Phase A context manifest for one BirdSet config.

    python scripts/phase_a_manifest.py \
        --corpus /faic/weka/ML/birdset --config XCM --resolution 4 \
        --key-file /shared/apps/ML/secrets/xc_key --out /faic/weka/ML/phase_a

FOCAL configs (XCM, XCL, and each test set's `_train` metadata) are dated from
the Xeno-Canto v3 API. SOUNDSCAPE metadata (`_test`, `_test_5s`) is dated from
the filename and needs no key.

The key travels ONLY as a file path (--key-file): argv is visible in `ps` and
env vars leak into `sbatch --export` and crash dumps, so neither may ever
carry the key itself.

See docs/superpowers/plans/2026-09-30-phase-a-embedding-extraction.md
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.phase_a.manifest import build_manifest
from birdology.phase_a.xc_dates import (
    DateFetchFailed,
    fetch_species_dates,
    read_key,
)

log = logging.getLogger("phase_a")

#: Below this share of usable recordings the build refuses. A manifest that
#: lost most of its corpus is a failed build, not a small one. This floor is
#: also the backstop for every per-species failure mode that yields zero
#: dates silently (a malformed XC tag, a species the API errors on).
DEFAULT_MIN_COVERAGE = 0.5

#: A soundscape cell with at least this many recordings but only ONE distinct
#: week gets a WARNING: that is the one-bad-station signature (a station id
#: that parses as an in-window date puts that station's every file in one
#: wrong week while split-level spread stays high).
MIN_CELL_RECORDINGS_FOR_SPREAD_CHECK = 50


def _metadata_path(corpus_dir: Path, config: str, split: str | None = None) -> Path:
    """The metadata parquet for one config, optionally for one named split.

    Found on WEKA (measured): HSN ships THREE parquets — `HSN_metadata_train`,
    `HSN_metadata_test`, `HSN_metadata_test_5s` — and the original fallback
    chain could only ever reach the train one, making the soundscape splits
    unreachable from the CLI. `--split` selects them explicitly; without it,
    the historic default chain applies (single-parquet configs like XCM).
    """
    d = Path(corpus_dir) / config / config
    if split is not None:
        p = d / f"{config}_metadata_{split}.parquet"
        if p.exists():
            return p
        raise FileNotFoundError(
            f"no metadata parquet for split {split!r} at {p}; available: "
            f"{sorted(q.name for q in d.glob(f'{config}_metadata*.parquet'))}"
        )
    for name in (f"{config}_metadata.parquet", f"{config}_metadata_train.parquet"):
        if (d / name).exists():
            return d / name
    raise FileNotFoundError(f"no metadata parquet under {d}")


def _is_soundscape(path: Path) -> bool:
    return "_test" in path.name


def species_tag(genus: str, filepath: str) -> tuple[str, str]:
    """Derive the XC query tag `(genus, species)` from a metadata row.

    The species epithet is the last `_`-separated token of the recording's
    parent directory (`data/xc/parus_major/XC1.ogg` -> `major`). Pinned
    edge cases, because a malformed tag returns ZERO recordings from the XC
    API *silently* and the species is then dated 0/N:

    - trinomial dir `parus_major_newtoni` -> `newtoni` (the subspecies
      epithet; the query `gen:parus sp:newtoni` is well-formed but matches
      only recordings XC tags with that epithet)
    - `tinamus_sp` -> `sp` (well-formed, matches XC's undetermined-species
      records)
    - a trailing underscore (`parus_`) -> `''` (the tag `sp:` is malformed)

    The coverage floor in `build_all` is the backstop for all of these: a
    species dated 0/N drags coverage down, and a corpus-wide derivation bug
    drops it below `min_coverage` and refuses the build.

    Raises ValueError for a filepath with NO parent directory (measured in the
    real XCM metadata: 2 of 89795 rows are bare `XC723553.ogg`) -- there is no
    epithet to derive, and the caller SKIPS and COUNTS the row rather than
    letting an IndexError abort the whole 409-species run. The recording then
    falls out as a counted NO_DATE drop, the designed path for undatable rows.
    """
    parts = filepath.split("/")
    if len(parts) < 2:
        raise ValueError(f"no parent directory in {filepath!r} to derive a species from")
    return genus, parts[-2].split("_")[-1]


def species_tag_from_code(
    ebird_code: str | None, mapping: dict[str, str]
) -> tuple[str, str]:
    """Derive the XC query tag from an eBird code via the KNOWLEDGE GRAPH's
    code->scientificName mapping (taxon:species/{code} dwc:scientificName,
    exported from output/birdology_reasoned.ttl — the graph this project
    exists to build; no redundant API pull).

    XCL is the case that needs this: its metadata has NO genus column and
    every filepath is a bare `XC208209.ogg` (measured 2026-09-30, all 528422
    rows), so `species_tag` has nothing to work with. It DOES carry
    ebird_code, and the graph knows 'gretit1' -> 'Parus major' -> the
    well-formed query `gen:parus sp:major`.

    Raises ValueError for a missing/unknown code or a mononomial sciName
    (genus-only entries can't form a `sp:` tag) — callers skip and count,
    same contract as species_tag.
    """
    if not ebird_code:
        raise ValueError("row has no ebird_code to derive a species from")
    sci = mapping.get(ebird_code)
    if not sci:
        raise ValueError(f"ebird_code {ebird_code!r} not in the species map")
    parts = sci.strip().lower().split()
    if len(parts) < 2:
        raise ValueError(
            f"ebird_code {ebird_code!r} maps to mononomial {sci!r}; no epithet"
        )
    return parts[0], parts[1]


def _load_progress(path: Path) -> dict[str, dict[str, str]]:
    """`{"genus species": {xc_id: iso_date}}` for species already fetched."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        log.warning("progress file %s unreadable; starting over", path)
        return {}


def _save_progress(path: Path, progress: dict[str, dict[str, str]]) -> None:
    """Atomic (tmp + os.replace): a crash mid-write must not corrupt resume."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(progress), encoding="utf-8")
    os.replace(tmp, path)


def date_focal_species(
    species: list[tuple[str, str]],
    *,
    key: str,
    progress_path: Path,
) -> tuple[dict[int, date], int]:
    """`({xc_id: date}, n_failed_species)` for every species, resumably.

    - Uses `fetch_species_dates`' default retrying session (429/5xx backoff);
      never constructs a bare `requests.Session`.
    - Persists per-species progress to *progress_path* after EVERY species,
      so an XCL-scale run (9735 species) resumes after a crash without
      re-fetching what it already has.
    - Catches `DateFetchFailed` per species: one broken species must not
      abort the other 9734. The failure count is returned and logged; the
      coverage floor catches systematic failure.
    """
    progress = _load_progress(progress_path)
    dates: dict[int, date] = {}
    failed = 0

    for genus, sp in species:
        name = f"{genus} {sp}"
        if name in progress:
            got = {int(k): date.fromisoformat(v) for k, v in progress[name].items()}
            log.info("%s: %d dates from progress file, not re-fetching", name, len(got))
        else:
            try:
                got = fetch_species_dates(genus, sp, key=key)
            except DateFetchFailed as exc:
                failed += 1
                log.warning("dating failed for %s: %s -- continuing", name, exc)
                continue
            progress[name] = {str(k): v.isoformat() for k, v in got.items()}
            _save_progress(progress_path, progress)
        dates.update(got)

    if failed:
        log.warning(
            "dating failed for %d/%d species; their recordings will be "
            "dropped as no_date and counted against the coverage floor",
            failed,
            len(species),
        )
    return dates, failed


def collapsed_cells(
    table, *, min_recordings: int = MIN_CELL_RECORDINGS_FOR_SPREAD_CHECK
) -> list[str]:
    """Cells with >= *min_recordings* rows but exactly ONE distinct week."""
    per_cell = table.group_by("cell").aggregate(
        [("week", "count_distinct"), ("cell", "count")]
    )
    return sorted(
        row["cell"]
        for row in per_cell.to_pylist()
        if row["cell_count"] >= min_recordings and row["week_count_distinct"] == 1
    )


def build_all(
    corpus_dir: Path,
    *,
    config: str,
    resolution: int,
    key_file: Path | None,
    out_dir: Path,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    split: str | None = None,
    species_map_path: Path | None = None,
    keep_undated: bool = False,
) -> Path:
    """Metadata in, manifest parquet out. Returns the written path."""
    meta_path = _metadata_path(Path(corpus_dir), config, split)
    metadata = pq.read_table(meta_path)
    log.info("read %d rows from %s", metadata.num_rows, meta_path)

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if _is_soundscape(meta_path):
        dates = None
        # The manifest's split label preserves the REQUESTED granularity:
        # test and test_5s are different eval sets (whole recordings vs 5 s
        # segments), and collapsing both to "test" would make them
        # indistinguishable in every downstream join.
        split_label = split if split is not None else "test"
    else:
        if key_file is None:
            raise ValueError(
                f"{meta_path.name} is a focal split and needs --key-file to date it"
            )
        # read_key(path), NEVER an env var or a raw argv value: argv shows in
        # `ps`, env leaks into sbatch exports.
        key = read_key(Path(key_file))
        species_map: dict[str, str] = {}
        if species_map_path is not None:
            species_map = json.loads(Path(species_map_path).read_text())
            log.info("species map loaded: %d eBird codes", len(species_map))
        cols = [c for c in ("genus", "filepath", "ebird_code") if c in metadata.schema.names]
        species_set = set()
        underivable = 0
        for row in metadata.select(cols).to_pylist():
            # Per-row: directory derivation first (XCM shape), else the
            # knowledge-graph ebird_code mapping (XCL shape: no genus column,
            # bare filepaths — measured on all 528422 XCL rows).
            try:
                if row.get("genus"):
                    species_set.add(species_tag(row["genus"], row.get("filepath") or ""))
                    continue
                raise ValueError("no genus column/value")
            except ValueError:
                pass
            try:
                species_set.add(species_tag_from_code(row.get("ebird_code"), species_map))
            except ValueError:
                underivable += 1
        if underivable:
            log.warning(
                "%d rows underivable by both directory and ebird_code map -- "
                "they will fall out as counted no_date drops",
                underivable,
            )
        species = sorted(species_set)
        log.info("dating %d species from the XC API", len(species))
        dates, _ = date_focal_species(
            species,
            key=key,
            progress_path=out_dir / f"{config}_dates_progress.json",
        )
        log.info("dated %d recordings", len(dates))
        split_label = split if split is not None else "train"

    table, drops = build_manifest(
        metadata, dates=dates, resolution=resolution, split=split_label,
        keep_undated=keep_undated,
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

    if dates is None:
        # WARNING, not refusal: a genuinely single-week deployment exists
        # (short campaigns). The warning plus the run log is the audit trail
        # for the one-bad-station signature.
        for cell in collapsed_cells(table):
            log.warning(
                "cell %s has >=%d recordings but exactly ONE distinct week -- "
                "the one-bad-station signature (a station id parsing as an "
                "in-window date); verify this station's deployment really was "
                "single-week",
                cell,
                MIN_CELL_RECORDINGS_FOR_SPREAD_CHECK,
            )

    # The split is part of the artifact name: HSN has train AND test AND
    # test_5s manifests, and a shared name would silently overwrite.
    suffix = f"_{split}" if split else ""
    path = out_dir / f"{config}{suffix}_r{resolution}_manifest.parquet"
    pq.write_table(table, path)
    log.info("wrote %s (%d rows, coverage %.3f)", path, table.num_rows, coverage)
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--corpus", required=True, type=Path)
    ap.add_argument("--config", required=True)
    ap.add_argument("--resolution", type=int, default=4)
    ap.add_argument("--key-file", type=Path, default=None)
    ap.add_argument("--out", type=Path, default=Path("/faic/weka/ML/phase_a"))
    ap.add_argument("--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE)
    ap.add_argument(
        "--split",
        default=None,
        help="named metadata split (train/test/test_5s) for configs that ship "
        "several parquets, e.g. HSN; omit for single-parquet configs like XCM",
    )
    ap.add_argument(
        "--species-map",
        type=Path,
        default=None,
        help="JSON {ebird_code: scientificName} exported from the birdology "
        "knowledge graph; enables XC dating for metadata with no genus "
        "column and bare filepaths (XCL). Rows underivable by BOTH the "
        "directory convention and this map fall out as counted no_date drops.",
    )
    ap.add_argument(
        "--keep-undated",
        action="store_true",
        help="RETAIN rows lacking date/coords with null cell/week instead "
        "of dropping them (phase_c manifests: the undated XCM rows train "
        "the flag-0 null-context path, and POW-val is all-undated). "
        "Kept-null rows are counted under kept_null_* metadata keys.",
    )
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
            split=args.split,
            species_map_path=args.species_map,
            keep_undated=args.keep_undated,
        )
    )


if __name__ == "__main__":
    main()
