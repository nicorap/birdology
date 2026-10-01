"""Phase C reproduction gate: frozen checkpoints vs the published LT rows.

THE HARD GATE (spec §3): before any fine-tuned or conditioned Phase C
number is reported, the FROZEN checkpoint run through OUR pipeline
(decode → frontend → evaluate_model → cmAP) must land within ±0.05
absolute cmAP of BirdSet's published LT row per test set — ConvNext and
AST rows of Table 5 (arXiv:2403.10380v3, p. 9). If it cannot, nothing
downstream is trustworthy.

PUBLISHED NUMBERS — MEASURED, NOT TRUSTED (Task 6's measure-first item):
re-derived 2026-10-01 from the paper's own pdftotext (v3 PDF; the
`-layout` render keeps Table 5's rows intact: Eff.Net / ConvNext / AST /
EAT / W2V2 / Perch blocks of cmAP+AUROC+T1-Acc over the POW PER NES UHH
HSN NBP SSW SNE columns). The task brief's provisional 3-decimal rows
(ConvNext POW .340 …, AST POW .320 …) are FALSIFIED by this measurement:
Table 5 reports 2 decimals and different values. The measured values
win and are pinned by tests/phase_c/test_repro_gate_logic.py.

TOLERANCE ±0.05 INCLUSIVE, ULP-SAFE: |Δ| == 0.05 passes. Because the
published values are 2-decimal floats, ``(p + 0.05) - p`` can land a few
ULP above 0.05 (0.47 + 0.05 → Δ = 0.05000000000000004); the comparison
rounds the delta to 12 decimals before comparing so a measured pass can
never flip to FAIL on float representation dust, while 0.0501 still
fails cleanly.

THE FiLM IDENTITY GATE RIDES IN THE SAME SCRIPT (spec §3): step-0
bit-equality of the wrapped model against the unwrapped checkpoint on
the same input — ``torch.equal``, never allclose (film.py: γ=1, β=0 is
IEEE-exact). ``film_identity_check`` is a function of the LOADED model,
so the fleet runs it on the real pinned checkpoints and the tests run
the identical code on tiny saved models. The mutation pin (randomly
initialized heads must FAIL) keeps the gate non-vacuous (spec §5).

Runs ON THE FLEET via main(); every comparison/scoring function is pure
and tested in tests/phase_c/test_repro_gate_logic.py without WEKA,
network, or from_pretrained — mirroring scripts/cluster/repro_gate.py.

Exit codes — a crash must never masquerade as a measured failure:
  0  PHASE_C_REPRO_OK (>=1 pair, all within tolerance; identity OK)
  1  PHASE_C_REPRO_FAIL (a measured miss, zero pairs, NaN cmAP,
     or a broken FiLM identity)
  2  crash (bad spec, unreadable input, ...) — traceback on stderr
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from birdology.phase_b.metrics import cmap

#: BirdSet Table 5 (arXiv:2403.10380v3, p. 9), LT scenario, cmAP rows for
#: the two open checkpoints we fine-tune. Keys are BirdSet test-set codes
#: in the paper's column order POW PER NES UHH HSN NBP SSW SNE. MEASURED
#: from the paper's pdftotext 2026-10-01 (see module docstring — the task
#: brief's provisional rows are falsified; these values win). POW is the
#: paper's Val column — kept for the record; our POW is validation-only
#: (spec §2) so fleet invocations gate on the six evaluable test sets.
PUBLISHED_LT_CMAP: dict[str, dict[str, float]] = {
    "convnext": {
        "POW": 0.36,
        "PER": 0.19,
        "NES": 0.34,
        "UHH": 0.26,
        "HSN": 0.47,
        "NBP": 0.62,
        "SSW": 0.35,
        "SNE": 0.30,
    },
    "ast": {
        "POW": 0.33,
        "PER": 0.18,
        "NES": 0.32,
        "UHH": 0.21,
        "HSN": 0.44,
        "NBP": 0.61,
        "SSW": 0.33,
        "SNE": 0.28,
    },
}

#: Absolute cmAP tolerance (spec §3) — inclusive at the boundary.
CMAP_TOLERANCE = 0.05

#: Deltas are rounded here before the tolerance comparison: published
#: values are 2-decimal floats, so an exact-boundary |Δ| of 0.05 can
#: carry ~1e-17 of representation dust (0.47 + 0.05 − 0.47 > 0.05).
#: 12 decimals kills the dust while leaving 0.0501 a clean fail.
_DELTA_DECIMALS = 12


def compare_to_published(
    model: str, test_set: str, cmap_value: float
) -> tuple[bool, float, float]:
    """Compare OUR frozen-checkpoint cmAP against the published LT row.

    Returns ``(ok, published, delta)`` with ``delta = ours − published``
    and ``ok`` iff ``|delta| <= CMAP_TOLERANCE`` (inclusive, ULP-safe —
    see :data:`_DELTA_DECIMALS`). An unknown model or test set raises
    naming it — a typo must never silently pass the gate. A NaN cmAP
    (no scoreable classes) is an EXPLICIT failure with a NaN delta.
    """
    if model not in PUBLISHED_LT_CMAP:
        raise ValueError(
            f"unknown model {model!r}; known: {sorted(PUBLISHED_LT_CMAP)} "
            "(exact — a typo must never silently pass the gate)"
        )
    table = PUBLISHED_LT_CMAP[model]
    if test_set not in table:
        raise ValueError(
            f"test set {test_set!r} has no published {model} LT row; "
            f"known: {sorted(table)}"
        )
    published = table[test_set]
    if np.isnan(cmap_value):
        return False, published, float("nan")
    delta = cmap_value - published
    ok = abs(round(delta, _DELTA_DECIMALS)) <= CMAP_TOLERANCE
    return ok, published, delta


def gate_pair_cmap(
    scores: np.ndarray, y: np.ndarray
) -> tuple[float, int]:
    """cmAP for one (model, test set) pair — with the zero-rows refusal.

    ZERO eval rows raise: an empty test set scored as "pass" (or even
    as NaN-fail) would hide a staging/decode failure upstream — the
    silent-empty-corpus hazard the sbatch guards against, re-guarded at
    the metric seam. Rows with no positive labels anywhere return the
    metric's own ``(nan, 0)``, which the comparison renders as FAIL.
    """
    scores = np.asarray(scores)
    y = np.asarray(y)
    if scores.shape[0] == 0:
        raise ValueError(
            "zero eval rows reached the gate -- an empty eval set means "
            "the decode/staging upstream produced nothing, and gating on "
            "it would bless a hollow pipeline; refusing"
        )
    return cmap(scores, y)


def format_gate_line(model: str, test_set: str, cmap_value: float) -> str:
    """One parseable gate line:
    ``PHASE_C_REPRO {model} {test_set} ours=X.XXX published=Y.YY
    delta=±Z.ZZZ`` with `` FAIL`` appended on a miss. A NaN cmAP renders
    ``ours=nan ... FAIL (no scoreable classes)`` — same grammar, never ok.
    """
    ok, published, delta = compare_to_published(model, test_set, cmap_value)
    if np.isnan(cmap_value):
        return (
            f"PHASE_C_REPRO {model} {test_set} ours=nan "
            f"published={published:.2f} delta=nan FAIL "
            "(no scoreable classes)"
        )
    line = (
        f"PHASE_C_REPRO {model} {test_set} ours={cmap_value:.3f} "
        f"published={published:.2f} delta={delta:+.3f}"
    )
    return line if ok else f"{line} FAIL"


def render_report(pairs: list[tuple[str, str, float]]) -> tuple[str, bool]:
    """All gate lines plus the verdict for ``(model, test_set, cmap)``
    pairs. ZERO pairs is itself a failure — printing PHASE_C_REPRO_OK
    with no comparison performed would be a vacuous pass (the inherited
    repro_gate contract). Returns ``(text, ok)``.
    """
    lines: list[str] = []
    ok = True
    for model, test_set, cmap_value in pairs:
        line = format_gate_line(model, test_set, cmap_value)
        if "FAIL" in line:
            ok = False
        lines.append(line)
    if not pairs:
        ok = False
        lines.append(
            "FAIL zero pairs: no checkpoint was compared to the published "
            "table -- the gate cannot pass vacuously"
        )
    lines.append("PHASE_C_REPRO_OK" if ok else "PHASE_C_REPRO_FAIL")
    return "\n".join(lines), ok


def film_identity_check(
    bare_model,
    wrapper,
    inputs,
    context,
) -> tuple[bool, float]:
    """Step-0 identity: wrapped(x, ctx, flag=1) vs bare(x), bit-for-bit.

    ``torch.equal`` — never allclose: γ=1/β=0 is IEEE-exact (film.py),
    so ANY difference is a real conditioning leak. Pure function of the
    LOADED modules: the fleet hands it the real pinned checkpoint and
    its freshly-built wrapper; the tests hand it tiny saved models —
    identical code path. present is hard-wired to 1.0 because flag-0
    rows are identity BY CONSTRUCTION even with trained heads; the gate
    must exercise the path where leaks are possible.

    Returns ``(ok, max_abs_delta)`` — the delta names the leak's size
    when the gate fails.
    """
    import torch

    bare_model.eval()
    wrapper.eval()
    present = torch.ones(inputs.shape[0], 1, dtype=torch.float32)
    with torch.no_grad():
        bare_logits = bare_model(inputs).logits
        wrapped_logits = wrapper(inputs, context, present)
    ok = torch.equal(wrapped_logits, bare_logits)
    max_delta = float((wrapped_logits - bare_logits).abs().max().item()) if not ok else 0.0
    return ok, max_delta


def _run_identity_subcommand(args) -> int:
    """Fleet side of the FiLM identity gate: load the pinned checkpoint,
    wrap it, run the check on a real batch of frontend output."""
    import torch
    from transformers import (
        ASTForAudioClassification,
        ConvNextForImageClassification,
    )

    from birdology.phase_c.context import ContextEncoder
    from birdology.phase_c.film import FiLMAST, FiLMConvNext

    cls = {
        "convnext": ConvNextForImageClassification,
        "ast": ASTForAudioClassification,
    }[args.model]
    model = cls.from_pretrained(
        args.checkpoint, revision=args.revision
    )
    n_classes = model.config.num_labels
    torch.manual_seed(0)
    encoder = ContextEncoder(n_classes, d_ctx=args.d_ctx)
    wrapper = (
        FiLMConvNext(model, encoder)
        if args.model == "convnext"
        else FiLMAST(model, encoder)
    )
    if args.model == "convnext":
        inputs = torch.randn(2, 1, 128, 501)
    else:
        inputs = torch.randn(
            2, model.config.max_length, model.config.num_mel_bins
        )
    context = torch.randn(2, n_classes)
    ok, max_delta = film_identity_check(model, wrapper, inputs, context)
    print(
        f"PHASE_C_FILM_IDENTITY {args.model} "
        f"{'ok' if ok else 'FAIL'} max_delta={max_delta:.3e}"
    )
    return 0 if ok else 1


def _run_gate_subcommand(args) -> int:
    """Fleet side of the repro gate: score each ``--pair`` spec
    ``MODEL:TESTSET:SCORES_NPZ`` where the npz carries ``scores`` (raw
    logits on the model axis, from evaluate_model) and ``y`` (the
    EvalDataset's aligned label matrix) — produced on-fleet by the eval
    runner so the gate exercises the exact pipeline the arms will use.
    """
    scored: list[tuple[str, str, float]] = []
    for spec in args.pairs:
        fields = spec.split(":", 2)
        if len(fields) != 3:
            raise ValueError(
                f"--pair spec needs 3 colon-separated fields "
                f"MODEL:TESTSET:SCORES_NPZ, got {len(fields)}: {spec!r}"
            )
        model, test_set, npz_path = fields
        if model not in PUBLISHED_LT_CMAP:
            raise ValueError(
                f"unknown model {model!r}; known: "
                f"{sorted(PUBLISHED_LT_CMAP)}"
            )
        data = np.load(npz_path)
        value, n_scored = gate_pair_cmap(data["scores"], data["y"])
        print(f"# {model}/{test_set}: n_scored={n_scored}")
        scored.append((model, test_set, value))
    text, ok = render_report(scored)
    print(text)
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    """Entry point: ``gate`` and ``film-identity`` subcommands.

    Argparse lives here, under main — importing the module for its pure
    functions never parses argv (the repro_gate.py import-safety
    contract, pinned by test).
    """
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    gate = sub.add_parser(
        "gate", help="frozen checkpoint vs published LT rows"
    )
    gate.add_argument(
        "--pair",
        action="append",
        required=True,
        metavar="MODEL:TESTSET:SCORES_NPZ",
        dest="pairs",
    )

    identity = sub.add_parser(
        "film-identity", help="step-0 bit-equality on a real checkpoint"
    )
    identity.add_argument("--model", choices=("convnext", "ast"), required=True)
    identity.add_argument("--checkpoint", required=True)
    identity.add_argument("--revision", required=True)
    identity.add_argument("--d-ctx", type=int, default=256)

    args = parser.parse_args(argv)
    try:
        if args.command == "gate":
            return _run_gate_subcommand(args)
        return _run_identity_subcommand(args)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
