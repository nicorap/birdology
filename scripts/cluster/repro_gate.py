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
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from birdology.phase_b.cache import align_labels, class_index, load_cache
from birdology.phase_b.metrics import auroc_macro, cmap, gather_scores

#: BirdSet Table 5 (arXiv:2403.10380v3, p. 9), Perch inference row, LT.
#: Keys are BirdSet test-set codes in the paper's column order
#: POW PER NES UHH HSN NBP SSW SNE. Verified against the pdftotext
#: interleaving 2026-10-01 (agrees with the controller's provisional
#: reading). cmAP is the gate metric; AUROC rides along for the report.
PUBLISHED_PERCH_CMAP: dict[str, float] = {
    "POW": 0.30,
    "PER": 0.18,
    "NES": 0.39,
    "UHH": 0.27,
    "HSN": 0.45,
    "NBP": 0.63,
    "SSW": 0.28,
    "SNE": 0.29,
}

PUBLISHED_PERCH_AUROC: dict[str, float] = {
    "POW": 0.84,
    "PER": 0.70,
    "NES": 0.90,
    "UHH": 0.76,
    "HSN": 0.86,
    "NBP": 0.91,
    "SSW": 0.91,
    "SNE": 0.83,
}

#: Absolute cmAP tolerance — see module docstring for the justification.
CMAP_TOLERANCE = 0.05

#: The one encoder with a published row; everything else is report-only.
GATED_ENCODER = "perch"


def compare_to_published(
    test_set: str, cmap_value: float
) -> tuple[bool, float, float]:
    """Compare OUR cmAP on *test_set* against the published Perch value.

    Returns ``(ok, published, delta)`` with ``delta = ours - published``
    and ``ok`` iff ``|delta| <= CMAP_TOLERANCE`` (inclusive). An unknown
    test set raises ValueError naming it — a typo'd split must never
    silently pass the gate.
    """
    if test_set not in PUBLISHED_PERCH_CMAP:
        raise ValueError(
            f"test set {test_set!r} has no published Perch row; known: "
            f"{sorted(PUBLISHED_PERCH_CMAP)}"
        )
    published = PUBLISHED_PERCH_CMAP[test_set]
    delta = cmap_value - published
    return abs(delta) <= CMAP_TOLERANCE, published, delta


def calibrate_scores(
    scores: np.ndarray, *, logits_are_raw: bool
) -> np.ndarray:
    """Sigmoid raw logits; pass calibrated probabilities through AS-IS.

    BirdNET caches already hold sigmoid probabilities; Perch caches hold
    raw logits (CacheView.logits_are_raw, read from writer metadata).
    Sigmoid is strictly monotone, so this CANNOT change cmAP/AUROC (rank
    metrics are monotone-invariant — pinned by test); it is applied
    anyway so thresholds mean the same thing across encoders in the
    gate's printed numbers.
    """
    if not logits_are_raw:
        return scores
    return (1.0 / (1.0 + np.exp(-np.asarray(scores, dtype=np.float64)))).astype(
        np.float32
    )


def format_pair_line(encoder: str, test_set: str, cmap_value: float) -> str:
    """One parseable gate line:
    ``ok|FAIL <encoder>/<testset>: cmap=X.XXX published=Y.YY delta=+Z.ZZZ``.
    """
    ok, published, delta = compare_to_published(test_set, cmap_value)
    return (
        f"{'ok' if ok else 'FAIL'} {encoder}/{test_set}: "
        f"cmap={cmap_value:.3f} published={published:.2f} delta={delta:+.3f}"
    )


def render_report(
    pairs: list[tuple[str, str, float]]
) -> tuple[str, bool]:
    """Render the gate's full output for ``(encoder, test_set, cmap)``
    pairs: one line per pair, then the verdict line.

    Returns ``(text, ok)``. The verdict considers ONLY ``perch`` pairs:
    BirdNET has no published BirdSet row (test-data leakage, §4.1), so
    its lines are printed for the record but can never flip the gate.
    The final line is ``REPRO_GATE_OK`` iff every gated pair passes,
    else ``REPRO_GATE_FAIL``.
    """
    lines: list[str] = []
    ok = True
    for encoder, test_set, value in pairs:
        line = format_pair_line(encoder, test_set, value)
        lines.append(line)
        if encoder == GATED_ENCODER and line.startswith("FAIL"):
            ok = False
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
    5. ``calibrate_scores`` — sigmoid iff the cache says raw.
    6. ``cmap`` + ``auroc_macro``.
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
    scores = calibrate_scores(scores, logits_are_raw=view.logits_are_raw)

    cmap_value, n_scored = cmap(scores, Y)
    auroc_value, n_auroc = auroc_macro(scores, Y)
    return PairResult(
        cmap=cmap_value,
        n_scored=n_scored,
        auroc=auroc_value,
        n_auroc=n_auroc,
        scores=scores,
        unscoreable=unscoreable,
    )


def main(argv: list[str] | None = None) -> int:
    """Fleet entry point.

    Each ``--pair`` is ``encoder:testset:cache_dir:metadata_parquet:labels``;
    prints one gate line per pair plus the verdict, exits 0 iff
    REPRO_GATE_OK (perch pairs only gate — BirdNET is report-only).
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

    scored: list[tuple[str, str, float]] = []
    for spec in args.pairs:
        encoder, test_set, cache_dir, metadata_path, labels_path = spec.split(
            ":", 4
        )
        result = score_pair(
            Path(cache_dir), Path(metadata_path), Path(labels_path)
        )
        print(
            f"# {encoder}/{test_set}: auroc={result.auroc:.3f} "
            f"(n={result.n_auroc}) n_scored={result.n_scored} "
            f"unscoreable={len(result.unscoreable)}"
        )
        scored.append((encoder, test_set, result.cmap))

    text, ok = render_report(scored)
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
