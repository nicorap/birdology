#!/usr/bin/env python3
"""Phase C run CLI — one (arm, model, resolution, seed) cell per invocation.

ONE CELL, ONE ROW, TWIN INCLUDED
--------------------------------
Each invocation trains ONE cell — a conditioned run (``--arm film`` /
``token``) or its twin (``--arm twin``) — on one (model, resolution,
seed), evaluates on one test set, and writes exactly ONE results row.
The twin is its own invocation on purpose: Slurm resume granularity is
the cell (the phase_b ruling), and a conditioned run and its twin are
separate Slurm tasks that pair later by (model, resolution, seed)
through ``phase_b.sweep.paired_delta``.

THE ROW REFUSES WITHOUT PROVENANCE (Global Constraints / phase_b contract)
--------------------------------------------------------------------------
Every results artifact names its inputs. ``build_row`` REFUSES — naming
the field — when any of ``git_sha, checkpoint_revision, prior_doi,
manifest_path, torch_version, cuda_version, arm, model_id,
context_source`` is missing or blank: a row that cannot name the
checkpoint revision or prior DOI it came from cannot be reproduced or
attributed, and silence here is how heterogeneous-stack rows sneak into
a table (the project's own ledger, spec §5).

SCHEMA: write_rows' DISCIPLINE ON PHASE C'S AXES
------------------------------------------------
Like ``phase_b.sweep.write_rows``: a fixed, versioned pyarrow schema; a
row dict missing a column or carrying an extra one fails the table
build loudly rather than writing a file other readers cannot join. The
phase_c axes (arm, model_id, resolution, seed — one seed per row, the
GPU-arm granularity) replace phase_b's (encoder, method, variant, tau,
5-seed cell); the Phase B table joins phase_c rows for analysis on the
shared columns (test_set, cmap, git_sha, prior_doi).

Import-safe: argparse lives under ``__main__``/``main`` — importing the
module for ``build_row``/``write_run_row`` never parses argv.
"""
from __future__ import annotations

import sys
import traceback
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

#: The arm vocabulary — must agree with train._ARMS.
_ARMS = ("film", "token", "twin")

#: Provenance columns a row REFUSES to exist without. Order is the
#: schema's column order for these fields.
REQUIRED_PROVENANCE = (
    "git_sha",
    "checkpoint_revision",
    "prior_doi",
    "manifest_path",
    "torch_version",
    "cuda_version",
    "arm",
    "model_id",
    "context_source",
)

RESULTS_SCHEMA_VERSION = "1"
_RESULTS_SCHEMA = pa.schema(
    [
        pa.field("arm", pa.string()),
        pa.field("model_id", pa.string()),
        pa.field("resolution", pa.int64()),
        pa.field("seed", pa.int64()),
        pa.field("test_set", pa.string()),
        pa.field("cmap", pa.float64()),
        pa.field("n_scored", pa.int64()),
        pa.field("val_cmap_history", pa.list_(pa.float64())),
        pa.field("epochs_ran", pa.int64()),
        pa.field("git_sha", pa.string()),
        pa.field("checkpoint_revision", pa.string()),
        pa.field("prior_doi", pa.string()),
        pa.field("manifest_path", pa.string()),
        pa.field("torch_version", pa.string()),
        pa.field("cuda_version", pa.string()),
        pa.field("context_source", pa.string()),
    ],
    metadata={b"phase_c_schema_version": RESULTS_SCHEMA_VERSION.encode()},
)


def build_row(
    *,
    provenance: dict,
    test_set: str,
    resolution: int,
    seed: int,
    cmap_value: float,
    n_scored: int,
    val_cmap_history: list[float],
    epochs_ran: int,
) -> dict:
    """One results-row dict — or a refusal naming the missing field.

    *provenance* must carry every :data:`REQUIRED_PROVENANCE` key with a
    non-blank value; ``arm`` is additionally validated against the arm
    vocabulary (an unknown arm would partition the paired analysis
    wrongly and silently).
    """
    for fname in REQUIRED_PROVENANCE:
        value = provenance.get(fname)
        if value is None or not str(value).strip():
            raise ValueError(
                f"refusing to build a results row with missing/empty "
                f"provenance field {fname!r} (got {value!r}); every "
                "results artifact names its inputs — a row that cannot "
                "cite its checkpoint revision, prior, or stack cannot be "
                "reproduced or attributed (the phase_b contract)"
            )
    arm = provenance["arm"]
    if arm not in _ARMS:
        raise ValueError(
            f"unknown arm {arm!r}; expected one of {_ARMS} — an unknown "
            "arm would silently mis-partition the paired twin analysis"
        )
    return {
        "arm": arm,
        "model_id": provenance["model_id"],
        "resolution": int(resolution),
        "seed": int(seed),
        "test_set": test_set,
        "cmap": float(cmap_value),
        "n_scored": int(n_scored),
        "val_cmap_history": [float(v) for v in val_cmap_history],
        "epochs_ran": int(epochs_ran),
        "git_sha": provenance["git_sha"],
        "checkpoint_revision": provenance["checkpoint_revision"],
        "prior_doi": provenance["prior_doi"],
        "manifest_path": provenance["manifest_path"],
        "torch_version": provenance["torch_version"],
        "cuda_version": provenance["cuda_version"],
        "context_source": provenance["context_source"],
    }


def write_run_row(row: dict, out_dir: str | Path) -> Path:
    """Write ONE row as one parquet file under *out_dir*; return its path.

    The fixed schema makes a malformed row dict fail the table build
    loudly (write_rows' discipline). One file per run: concurrent array
    tasks never share a writer, so the phase_b concurrency caveat is
    structurally unreachable here.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    columns = {
        field.name: [row[field.name]] for field in _RESULTS_SCHEMA
    }
    table = pa.table(columns, schema=_RESULTS_SCHEMA)
    path = out_dir / f"run-{uuid.uuid4().hex}.parquet"
    pq.write_table(table, path)
    return path


def _run_cell(args) -> int:
    """Fleet side: wire CropDataset + wrapper + train_arm + evaluate_model
    for one cell, then write the one row. Imports live here so the module
    stays importable without torch for the row-contract tests."""
    if args.model == "ast":
        # Refused BEFORE any download or GPU time: the only frontend
        # wired below is convnext_frontend, whose (1, 128, 501) mel is
        # not AST's input contract — it would shape-crash or
        # garbage-score at GPU cost.
        raise NotImplementedError(
            "ast_frontend not yet wired — BirdSet AST uses "
            "mel→resize-1024 with sign-flip std; see task-6-report "
            "concern 1. Refusing rather than feeding convnext_frontend "
            "output to AST."
        )

    import json

    import numpy as np
    import pyarrow.parquet as _pq
    import torch
    from transformers import (
        ASTForAudioClassification,
        ConvNextForImageClassification,
    )

    from birdology.phase_c.context import ContextEncoder, build_context_matrix
    from birdology.phase_c.data import (
        SAMPLE_RATE,
        CropDataset,
        convnext_frontend,
        make_label_map,
    )
    from birdology.phase_c.evaluate import build_eval_dataset, evaluate_model
    from birdology.phase_c.film import FiLMAST, FiLMConvNext
    from birdology.phase_c.token import TokenAST
    from birdology.phase_c.train import TrainConfig, train_arm
    from birdology.prior.tensor import load_prior

    model_cls = {
        "convnext": ConvNextForImageClassification,
        "ast": ASTForAudioClassification,
    }[args.model]
    model = model_cls.from_pretrained(
        args.checkpoint, revision=args.checkpoint_revision
    )
    label2id = make_label_map(model.config.id2label)
    class_codes = [
        model.config.id2label[i] for i in sorted(map(int, model.config.id2label))
    ]

    prior = load_prior(Path(args.prior))
    code_to_gbif = json.loads(Path(args.bridge).read_text())
    provider = build_context_matrix(prior, class_codes, code_to_gbif)

    def load_audio(filepath: str) -> np.ndarray:
        """Decoded mono audio @32 kHz from the staged audio root.

        Shards are staged (untarred) to /raid/scratch by the sbatch
        BEFORE this runs; filepaths resolve under --audio-root. Decode
        matches phase_a's: soundfile, mono-mean, refuse 0 samples (a
        truncated OGG decodes empty without raising — measured).
        """
        import soundfile as sf

        samples, sr = sf.read(
            str(Path(args.audio_root) / filepath),
            dtype="float32",
            always_2d=False,
        )
        if getattr(samples, "ndim", 1) > 1:
            samples = samples.mean(axis=1)
        if samples.size == 0:
            raise ValueError(f"{filepath}: decoded to 0 samples")
        if sr != SAMPLE_RATE:
            import librosa

            samples = librosa.resample(
                samples, orig_sr=sr, target_sr=SAMPLE_RATE
            )
        return np.asarray(samples, dtype=np.float32)

    manifest = _pq.read_table(args.manifest)
    train_ds = CropDataset(
        manifest, load_audio, provider, label2id, seed=args.seed
    )
    val_meta = _pq.read_table(args.val_metadata)
    val_ds = CropDataset(
        val_meta, load_audio, provider, label2id, seed=args.seed
    )

    if args.arm == "twin":
        trainee: torch.nn.Module = model
    else:
        torch.manual_seed(args.seed)
        encoder = ContextEncoder(len(class_codes), d_ctx=args.d_ctx)
        if args.arm == "film":
            trainee = (
                FiLMConvNext(model, encoder)
                if args.model == "convnext"
                else FiLMAST(model, encoder)
            )
        else:  # token — AST only (the 3-combo grid, spec decision 2)
            trainee = TokenAST(model, encoder)

    cfg = TrainConfig(arm=args.arm, seed=args.seed)
    result = train_arm(trainee, train_ds, val_ds, cfg, args.ckpt_dir)

    # Restore the VAL-BEST weights before evaluating: early stopping
    # leaves the trainee ≥patience epochs past its best, so evaluating
    # the loop's final state would silently degrade every fleet row —
    # and the frozen-checkpoint repro gate cannot catch it (no training
    # happens there). best.pt was saved from the TRAINEE's own
    # perspective (train_arm checkpoints the state_dict of exactly the
    # object it received: the bare model for the twin, the wrapper for
    # conditioned arms), so it loads back into the trainee.
    best_ckpt = torch.load(result.best_state_path, weights_only=True)
    trainee.load_state_dict(best_ckpt["model_state"])

    eval_meta = _pq.read_table(args.eval_metadata)
    eval_ds = build_eval_dataset(
        eval_meta, load_audio, provider, class_codes, frontend=convnext_frontend
    )
    scores = evaluate_model(trainee, eval_ds, arm=args.arm)
    from birdology.phase_b.metrics import cmap as _cmap

    cmap_value, n_scored = _cmap(scores, eval_ds.y)

    provenance = {
        "git_sha": args.git_sha,
        "checkpoint_revision": args.checkpoint_revision,
        "prior_doi": args.prior_doi,
        "manifest_path": str(args.manifest),
        "torch_version": torch.__version__,
        "cuda_version": torch.version.cuda or "",
        "arm": args.arm,
        "model_id": args.checkpoint,
        "context_source": args.context_source,
    }
    row = build_row(
        provenance=provenance,
        test_set=args.test_set,
        resolution=args.resolution,
        seed=args.seed,
        cmap_value=cmap_value,
        n_scored=n_scored,
        val_cmap_history=result.val_cmap_history,
        epochs_ran=result.epochs_ran,
    )
    path = write_run_row(row, args.out)
    print(
        f"PHASE_C_RUN {args.arm} {args.model} r{args.resolution} "
        f"seed={args.seed} {args.test_set} cmap={cmap_value:.3f} -> {path}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """One cell per invocation. Argparse lives here — importing the
    module never parses argv (import-safety, pinned by test)."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=_ARMS, required=True)
    parser.add_argument("--model", choices=("convnext", "ast"), required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--checkpoint-revision", required=True)
    parser.add_argument("--resolution", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--test-set", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--val-metadata", required=True)
    parser.add_argument("--eval-metadata", required=True)
    parser.add_argument("--audio-root", required=True)
    parser.add_argument("--prior", required=True)
    parser.add_argument("--prior-doi", required=True)
    parser.add_argument("--bridge", required=True)
    parser.add_argument("--context-source", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--ckpt-dir", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--d-ctx", type=int, default=256)
    args = parser.parse_args(argv)

    try:
        return _run_cell(args)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
