"""Reproduction gate: OUR pipeline vs BirdSet's PUBLISHED Perch row.

THE HARD GATE (spec §4): before any prior-conditioned number is reported,
our Phase A cache + Phase B metrics stack must reproduce BirdSet's
published Perch inference baseline within tolerance on their soundscape
test splits. If it cannot, nothing downstream is trustworthy.

Published numbers: BirdSet (ICLR 2025, arXiv:2403.10380v3), Table 5
("Mean results across datasets and models for LT", p. 9), Perch
inference row — re-derived independently from the paper's pdftotext
(column-interleaved; blocks of 3 metrics x 8 test columns per model,
Perch last). Derivation recorded in the Task 3 report.

Tolerance: ±0.05 absolute cmAP. Our segment pooling differs from
BirdSet's eval harness (they score 5 s segments through their lightning
pipeline; we score per-recording pooled logits from the Phase A cache on
*_test_5s segment caches), and Table 5 reports a single run. The gate
catches stack-level breakage (wrong class axis, wrong labels, wrong
pooling — all of which shift cmAP by >>0.05), not harness-identity.

BirdNET has NO published BirdSet row (excluded for test-data leakage,
§4.1): BirdNET pairs are REPORT-ONLY — printed for the record, never
pass/fail, never able to flip the gate verdict.

Runs ON THE FLEET via main() (load_cache on WEKA cache dirs); every
comparison/scoring function is pure and tested in
tests/phase_b/test_repro_gate_logic.py without WEKA or network.
"""
from __future__ import annotations

import argparse
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from birdology.phase_b.cache import align_labels, class_index, load_cache
from birdology.phase_b.metrics import auroc_macro, cmap, gather_scores

#: Perch 2.0 paper (arXiv:2508.04665v2), Table 4 "BirdSet per-dataset
#: results", row "Perch 2.0 - Random", cmAP block — measured from the PDF
#: 2026-10-01. The cache's encoder IS Perch 2.0 (kagglehub perch_v2), so
#: THIS row is the reproduction target; BirdSet's own Table 5 Perch row
#: (arXiv:2403.10380v3: POW .30 PER .18 NES .39 UHH .27 HSN .45 NBP .63
#: SSW .28 SNE .29) is Perch 1.0 and comparing against it failed the
#: first fleet gate run with uniform POSITIVE deltas up to +0.189 — our
#: pipeline was reproducing the newer, stronger model. Against this row
#: the 2026-10-01 run matches to ≤0.001 on all six evaluable test sets
#: (PER/NES/HSN/SSW/SNE exact, UHH 0.381 vs 0.380). POW is absent from
#: Table 4; its Perch 1.0 value is retained (report-only: POW is not in
#: the evaluable roster).
PUBLISHED_PERCH_CMAP: dict[str, float] = {
    "POW": 0.30,  # Perch 1.0 (Table 4 has no POW row; report-only)
    "PER": 0.232,
    "NES": 0.403,
    "UHH": 0.380,
    "HSN": 0.533,
    "NBP": 0.661,
    "SSW": 0.469,
    "SNE": 0.341,
}

#: Same source/row as PUBLISHED_PERCH_CMAP (Perch 2.0 Table 4, ROC-AUC
#: block, "Random" row); report-only. 2026-10-01 fleet run agreement:
#: PER .786/.786 NES .953/.953 UHH .912/.912 HSN .915/.915 SSW .973/.973
#: SNE .883/.883 — exact on all six.
PUBLISHED_PERCH_AUROC: dict[str, float] = {
    "POW": 0.84,  # Perch 1.0 (no POW row in Table 4; report-only)
    "PER": 0.786,
    "NES": 0.953,
    "UHH": 0.912,
    "HSN": 0.915,
    "NBP": 0.933,
    "SSW": 0.973,
    "SNE": 0.883,
}

#: Absolute cmAP tolerance — see module docstring for the justification.
CMAP_TOLERANCE = 0.05

#: The one encoder with a published row; everything else is report-only.
GATED_ENCODER = "perch"

#: Every encoder token the gate knows. An unknown token (typo, case
#: mismatch like 'Perch', 'perch_v2') must RAISE, never silently become
#: a report-only pair — that would be a vacuous pass vector.
KNOWN_ENCODERS = frozenset({"perch", "birdnet"})


def parse_pair_spec(spec: str) -> tuple[str, str, str, str, str]:
    """Parse one ``ENCODER:TESTSET:CACHE_DIR:METADATA:LABELS`` spec.

    The encoder token is validated against :data:`KNOWN_ENCODERS` at
    parse time (exact, case-sensitive): a typo'd encoder would otherwise
    silently fall into the report-only branch and the gate could print
    REPRO_GATE_OK with zero published comparisons gated.
    """
    fields = spec.split(":", 4)
    if len(fields) != 5:
        raise ValueError(
            f"--pair spec needs 5 colon-separated fields "
            f"ENCODER:TESTSET:CACHE_DIR:METADATA:LABELS, got "
            f"{len(fields)}: {spec!r}"
        )
    encoder = fields[0]
    if encoder not in KNOWN_ENCODERS:
        raise ValueError(
            f"unknown encoder {encoder!r}; known: {sorted(KNOWN_ENCODERS)} "
            "(exact, case-sensitive — a typo must never become a "
            "report-only pair)"
        )
    return encoder, fields[1], fields[2], fields[3], fields[4]


def compare_to_published(
    test_set: str, cmap_value: float
) -> tuple[bool, float, float]:
    """Compare OUR cmAP on *test_set* against the published Perch value.

    Returns ``(ok, published, delta)`` with ``delta = ours - published``
    and ``ok`` iff ``|delta| <= CMAP_TOLERANCE`` (inclusive). An unknown
    test set raises ValueError naming it — a typo'd split must never
    silently pass the gate. A NaN cmAP (no scoreable classes) is an
    EXPLICIT failure, not a float-comparison accident.
    """
    if test_set not in PUBLISHED_PERCH_CMAP:
        raise ValueError(
            f"test set {test_set!r} has no published Perch row; known: "
            f"{sorted(PUBLISHED_PERCH_CMAP)}"
        )
    published = PUBLISHED_PERCH_CMAP[test_set]
    if np.isnan(cmap_value):
        return False, published, float("nan")
    delta = cmap_value - published
    # Round |delta| to 12 decimals before the boundary compare: published
    # values like 0.232 carry float dust, so `published + 0.05` can exceed
    # the tolerance by ~2e-17 and fail a boundary the docstring declares
    # inclusive. cmAP granularity on finite test sets is >> 1e-12, so no
    # real miss can slip through (same fix as phase_c_repro_gate).
    return round(abs(delta), 12) <= CMAP_TOLERANCE, published, delta


def calibrate_scores(
    scores: np.ndarray, *, logits_are_raw: bool
) -> np.ndarray:
    """Sigmoid raw logits; pass calibrated probabilities through AS-IS.

    BirdNET caches already hold sigmoid probabilities; Perch caches hold
    raw logits (CacheView.logits_are_raw, read from writer metadata).
    DISPLAY-ONLY: sigmoid is monotone in exact arithmetic, but in finite
    precision logits beyond ~17 (float32) saturate to exactly 1.0 and
    collapse distinct ranks into ties — so the gate's cmAP/AUROC are
    computed on the PRE-calibration scores (see score_pair), and these
    probabilities exist only so thresholds mean the same thing across
    encoders in printed numbers.
    """
    if not logits_are_raw:
        return scores
    return (1.0 / (1.0 + np.exp(-np.asarray(scores, dtype=np.float64)))).astype(
        np.float32
    )


def format_pair_line(encoder: str, test_set: str, cmap_value: float) -> str:
    """One parseable GATED line:
    ``ok|FAIL <encoder>/<testset>: cmap=X.XXX published=Y.YY delta=+Z.ZZZ``.

    A NaN cmAP (no scoreable classes) gets a distinguishable FAIL line —
    ``FAIL ... cmap=nan (no scoreable classes)`` — rather than silently
    breaking the line grammar; it can never render ``ok``.
    """
    ok, published, delta = compare_to_published(test_set, cmap_value)
    if np.isnan(cmap_value):
        return (
            f"FAIL {encoder}/{test_set}: cmap=nan (no scoreable classes) "
            f"published={published:.2f}"
        )
    return (
        f"{'ok' if ok else 'FAIL'} {encoder}/{test_set}: "
        f"cmap={cmap_value:.3f} published={published:.2f} delta={delta:+.3f}"
    )


def format_report_line(
    encoder: str, test_set: str, cmap_value: float, auroc_value: float
) -> str:
    """One NON-GATED (report-only) line — a distinct form with no
    ``ok|FAIL`` token and no ``published=`` value, so BirdNET's numbers
    can never read as a contradictory comparison against Perch's row:
    ``report <encoder>/<testset>: cmap=X.XXX auroc=X.XXX (no published row)``.

    Never consults the published dict, so a test set absent from it
    (BirdNET on a split Perch has no row for) renders fine.
    """
    return (
        f"report {encoder}/{test_set}: cmap={cmap_value:.3f} "
        f"auroc={auroc_value:.3f} (no published row)"
    )


def render_report(
    pairs: list[tuple[str, str, float, float]]
) -> tuple[str, bool]:
    """Render the gate's full output for ``(encoder, test_set, cmap,
    auroc)`` pairs: one line per pair, then the verdict line.

    Returns ``(text, ok)``. Only ``perch`` pairs are GATED (compared to
    the published row); other known encoders (BirdNET — no published
    BirdSet row, test-data leakage §4.1) render report-only lines that
    can never flip the verdict. An encoder outside KNOWN_ENCODERS raises
    (defense in depth below parse_pair_spec).

    ZERO gated pairs is itself a failure: a birdnet-only invocation or
    an empty pair list gates nothing, and printing REPRO_GATE_OK with no
    published comparison performed would be a vacuous pass. The final
    line is ``REPRO_GATE_OK`` iff at least one gated pair exists and
    every gated pair passes, else ``REPRO_GATE_FAIL``.
    """
    lines: list[str] = []
    ok = True
    n_gated = 0
    for encoder, test_set, cmap_value, auroc_value in pairs:
        if encoder not in KNOWN_ENCODERS:
            raise ValueError(
                f"unknown encoder {encoder!r}; known: "
                f"{sorted(KNOWN_ENCODERS)}"
            )
        if encoder == GATED_ENCODER:
            n_gated += 1
            line = format_pair_line(encoder, test_set, cmap_value)
            if line.startswith("FAIL"):
                ok = False
        else:
            line = format_report_line(
                encoder, test_set, cmap_value, auroc_value
            )
        lines.append(line)
    if n_gated == 0:
        ok = False
        lines.append(
            "FAIL zero gated pairs: no perch pair was compared to the "
            "published table -- the gate cannot pass vacuously"
        )
    lines.append("REPRO_GATE_OK" if ok else "REPRO_GATE_FAIL")
    return "\n".join(lines), ok


@dataclass(frozen=True)
class PairResult:
    """One (encoder, test-set) scoring: the gate's inputs plus context."""

    cmap: float
    n_scored: int  # classes that entered the cmAP mean
    auroc: float
    n_auroc: int  # classes that entered the AUROC mean
    scores: np.ndarray  # (n_rows, n_scoreable) calibrated probabilities
    unscoreable: list[str]  # label codes absent from the model head


def score_pair(
    cache_dir: Path, metadata_path: Path, labels_path: Path
) -> PairResult:
    """Score ONE (encoder cache, test-set metadata) pair end to end.

    Pipeline (Task 1 + Task 2 modules only — the comparison must exercise
    the exact stack the fusion sweep will use):

    1. ``load_cache`` — shard concatenation + the raw-logits fact.
    2. Metadata parquet joined to cache rows BY ``filepath`` — never
       positional; a cache recording missing from the metadata raises
       naming it (a silent inner join would quietly shrink the test set).
    3. ``class_index`` + ``align_labels`` — labels from
       ``ebird_code_multilabel`` keyed by eBird code.
    4. ``gather_scores`` — by-code column selection.
    5. ``cmap`` + ``auroc_macro`` on the PRE-calibration scores — rank
       metrics on calibrated probabilities would collapse ranks where
       sigmoid saturates (logits > ~17 all become exactly 1.0).
    6. ``calibrate_scores`` — sigmoid iff the cache says raw; the
       calibrated probabilities ride along for DISPLAY only.
    """
    view = load_cache(Path(cache_dir))
    cache_files: list[str] = view.table.column("filepath").to_pylist()

    meta = pq.read_table(
        metadata_path, columns=["filepath", "ebird_code_multilabel"]
    )
    labels_by_file = dict(
        zip(
            meta.column("filepath").to_pylist(),
            meta.column("ebird_code_multilabel").to_pylist(),
        )
    )
    missing = [f for f in cache_files if f not in labels_by_file]
    if missing:
        raise ValueError(
            f"{len(missing)} cache recording(s) missing from metadata "
            f"{metadata_path}: {missing[:5]} -- a silent inner join would "
            "shrink the test set; refusing"
        )

    # Re-emit the metadata rows IN CACHE ROW ORDER so Y rows align with
    # the logits rows, then let align_labels build the matrix by code.
    aligned = pa.table(
        {
            "ebird_code_multilabel": pa.array(
                [labels_by_file[f] for f in cache_files],
                pa.list_(pa.string()),
            )
        }
    )
    class_idx = class_index(Path(labels_path))
    Y, scoreable, unscoreable = align_labels(aligned, class_idx)

    logits = np.asarray(
        view.table.column("logits").to_pylist(), dtype=np.float32
    )
    scores = gather_scores(logits, class_idx, scoreable)

    # Metrics on PRE-calibration scores: sigmoid saturation (float32
    # logits > ~17 all map to exactly 1.0) would collapse distinct ranks
    # into ties and corrupt cmAP/AUROC in the saturation regime.
    cmap_value, n_scored = cmap(scores, Y)
    auroc_value, n_auroc = auroc_macro(scores, Y)

    # Calibrated probabilities are for DISPLAY/threshold comparability
    # only — they never feed the gate's metrics.
    calibrated = calibrate_scores(scores, logits_are_raw=view.logits_are_raw)
    return PairResult(
        cmap=cmap_value,
        n_scored=n_scored,
        auroc=auroc_value,
        n_auroc=n_auroc,
        scores=calibrated,
        unscoreable=unscoreable,
    )


def main(argv: list[str] | None = None) -> int:
    """Fleet entry point.

    Each ``--pair`` is ``encoder:testset:cache_dir:metadata_parquet:labels``
    (encoder validated against KNOWN_ENCODERS at parse time); prints one
    gate line per pair plus the verdict.

    Exit codes — a crash must never masquerade as a measured failure:
      0  REPRO_GATE_OK (>=1 perch pair, all within tolerance)
      1  REPRO_GATE_FAIL (a measured miss, or zero gated pairs)
      2  crash (bad spec, unreadable cache, ...) — traceback on stderr
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pair",
        action="append",
        required=True,
        metavar="ENCODER:TESTSET:CACHE_DIR:METADATA:LABELS",
        dest="pairs",
    )
    args = parser.parse_args(argv)

    try:
        scored: list[tuple[str, str, float, float]] = []
        for spec in args.pairs:
            encoder, test_set, cache_dir, metadata_path, labels_path = (
                parse_pair_spec(spec)
            )
            result = score_pair(
                Path(cache_dir), Path(metadata_path), Path(labels_path)
            )
            print(
                f"# {encoder}/{test_set}: auroc={result.auroc:.3f} "
                f"(n={result.n_auroc}) n_scored={result.n_scored} "
                f"unscoreable={len(result.unscoreable)}"
            )
            scored.append((encoder, test_set, result.cmap, result.auroc))

        text, ok = render_report(scored)
        print(text)
        return 0 if ok else 1
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
