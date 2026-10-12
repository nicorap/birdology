#!/usr/bin/env python3
"""Phase C run CLI — one (arm, model, resolution, seed) cell per invocation.

ONE CELL, ONE TRAINING, ONE ROW PER TEST SET (finding I2)
---------------------------------------------------------
Each invocation trains ONE cell — a conditioned run (``--arm film`` /
``token``) or its twin (``--arm twin``) — on one (model, resolution,
seed), then loads the val-best checkpoint ONCE and loops evaluation
over every ``--test-sets`` entry, writing one results row per test set
(the ``test_set`` column distinguishes them). Training is test-set-
independent (XCM train, POW val), so retraining per test set would
multiply the training budget ×6 for identical weights (~1080 GPU-h vs
the spec §4 budget of ~600). The checkpoint dir is accordingly keyed
(arm, model, resolution, seed) WITHOUT test_set.

The twin is its own invocation on purpose: Slurm resume granularity is
the cell (the phase_b ruling), and a conditioned run and its twin are
separate Slurm tasks. Pairing: per-seed rows are aggregated at ANALYSIS
time over the per-seed rows keyed (model, resolution, seed) — a
``phase_b.sweep.paired_delta`` adapter is a ledgered follow-up; phase_c
rows do not satisfy its phase_b-shaped contract (seeds tuples,
cmap_per_seed, phase_b axis names) and claiming otherwise was finding
I4.

THE ROW REFUSES WITHOUT PROVENANCE (Global Constraints / phase_b contract)
--------------------------------------------------------------------------
Every results artifact names its inputs. ``build_row`` REFUSES — naming
the field — when any of ``git_sha, checkpoint_revision, prior_doi,
prior_variant, manifest_path, torch_version, cuda_version, device,
device_name, arm, model_id, context_source`` is missing or blank: a row that cannot name
the checkpoint revision or prior DOI it came from cannot be reproduced
or attributed, and silence here is how heterogeneous-stack rows sneak
into a table (the project's own ledger, spec §5). ``prior_variant``
(finding I1) is the controls axis — full/marginal/shuffle_space/
shuffle_time, spelled EXACTLY as build_prior.py's npz suffixes
(CONTROLS' own keys) — while ``context_source`` stays path provenance.

DEVICE vs BUILD (schema v3, fleet job 5730)
-------------------------------------------
``cuda_version`` is the torch BUILD's CUDA (``torch.version.cuda``;
``"cpu"`` for CPU-only builds) — STACK info, unchanged. It is NOT the
device: job 5730's twins ran 2h43m on CPU inside a CUDA-12.9 container,
and a v2 row would have said ``cuda_version="12.9"``. ``device``
(``"cuda:0"`` / ``"cpu"``) and ``device_name`` (the GPU's
``torch.cuda.get_device_name``; ``"cpu (<arch>)"`` otherwise) are read
from the trainee's parameters AFTER the move — the device the run
actually used — so a CPU run can never produce a row that reads like a
GPU run.

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

import logging
import sys
import traceback
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

#: The arm vocabulary — must agree with train._ARMS.
_ARMS = ("film", "token", "twin")

#: The prior-variant vocabulary (finding I1) — spelled EXACTLY as the
#: npz suffixes build_prior.py writes ({stem}_{name}.npz from
#: birdology.prior.controls.CONTROLS' keys, plus the full prior).
#: `uniform` is deliberately absent: the uniform-prior control collapses
#: to the twin by construction (spec decision 4); `separable` is not a
#: scheduled phase_c control arm.
PRIOR_VARIANTS = ("full", "marginal", "shuffle_space", "shuffle_time")

#: Provenance columns a row REFUSES to exist without. Order is the
#: schema's column order for these fields.
REQUIRED_PROVENANCE = (
    "git_sha",
    "checkpoint_revision",
    "prior_doi",
    "prior_variant",
    "manifest_path",
    "torch_version",
    "cuda_version",
    "device",
    "device_name",
    "arm",
    "model_id",
    "context_source",
)

#: v3 (job 5730): + device, device_name — the device the run actually
#: used, alongside cuda_version (the torch BUILD's CUDA, unchanged).
RESULTS_SCHEMA_VERSION = "3"
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
        pa.field("prior_variant", pa.string()),
        pa.field("manifest_path", pa.string()),
        pa.field("torch_version", pa.string()),
        pa.field("cuda_version", pa.string()),
        pa.field("device", pa.string()),
        pa.field("device_name", pa.string()),
        pa.field("context_source", pa.string()),
    ],
    metadata={b"phase_c_schema_version": RESULTS_SCHEMA_VERSION.encode()},
)


def _device_provenance(device) -> tuple[str, str]:
    """(device, device_name) for the device the run ACTUALLY used.

    ``cuda:<i>`` + ``torch.cuda.get_device_name(i)`` on CUDA; ``cpu`` +
    ``cpu (<machine arch>)`` otherwise (``mps`` and friends pass through
    with their type as the name). Never derived from the torch build.
    """
    import platform

    import torch

    device = torch.device(device)
    if device.type == "cuda":
        index = device.index if device.index is not None else 0
        return f"cuda:{index}", torch.cuda.get_device_name(index)
    if device.type == "cpu":
        return "cpu", f"cpu ({platform.machine() or 'unknown'})"
    return str(device), device.type


def _stack_provenance(args, resolved_revision: str, *, device) -> dict:
    """The provenance dict for this invocation's rows.

    CPU-only stacks carry ``torch.version.cuda = None`` — that is REAL
    provenance (the run had no CUDA), not missing provenance; mapping it
    to ``""`` would trip build_row's refusal and lock GPU-less runs (Mac
    probes, CPU smoke tests) out of writing rows entirely. Measured: the
    2026-10-01 Mac end-to-end probe refused after 49 minutes of successful
    training at exactly this seam. Recorded as ``"cpu"``.

    *device* is the device the run ACTUALLY used (the trainee's
    parameters after the move) — recorded as ``device``/``device_name``
    because ``cuda_version`` alone describes the build, not the run
    (job 5730). Keyword-only and required: there is no default that
    could silently say "cpu" or "cuda".
    """
    import torch

    device_str, device_name = _device_provenance(device)

    return {
        "git_sha": args.git_sha,
        "checkpoint_revision": resolved_revision,
        "prior_doi": args.prior_doi,
        "prior_variant": args.prior_variant,
        "manifest_path": str(args.manifest),
        "torch_version": torch.__version__,
        "cuda_version": torch.version.cuda or "cpu",
        "device": device_str,
        "device_name": device_name,
        "arm": args.arm,
        "model_id": args.checkpoint,
        "context_source": args.context_source,
    }


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
    variant = provenance["prior_variant"]
    if variant not in PRIOR_VARIANTS:
        raise ValueError(
            f"unknown prior_variant {variant!r}; expected one of "
            f"{PRIOR_VARIANTS} — the vocabulary is build_prior.py's own "
            "npz suffixes (CONTROLS' keys), and a respelling would "
            "silently detach the 135 control rows from the artifacts "
            "they ran on (finding I1)"
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
        "prior_variant": variant,
        "manifest_path": provenance["manifest_path"],
        "torch_version": provenance["torch_version"],
        "cuda_version": provenance["cuda_version"],
        "device": provenance["device"],
        "device_name": provenance["device_name"],
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


class _StdoutHandler(logging.StreamHandler):
    """A StreamHandler that always writes to the CURRENT ``sys.stdout``
    (the Slurm ``--output`` file), resolved per record — so it survives
    stdout being swapped (pytest capture, redirection) after setup."""

    @property
    def stream(self):  # type: ignore[override]
        return sys.stdout

    @stream.setter
    def stream(self, value) -> None:  # StreamHandler.__init__ assigns it
        pass


def _configure_logging(level: int = logging.INFO) -> None:
    """Route ``birdology.*`` INFO to stdout — idempotently.

    Fleet job 5730: the log held nothing after transformers' "Loading
    weights" for 2.5 h — nothing ever configured logging, so birdology
    INFO records fell through to logging's last-resort handler (WARNING+
    to stderr only). A handler on the ``birdology`` logger (not
    ``basicConfig`` on root: that would also unleash every third-party
    library's INFO and is a silent no-op if anything configured root
    first) makes progress visible without touching anyone else's logs.
    """
    lg = logging.getLogger("birdology")
    if not any(isinstance(h, _StdoutHandler) for h in lg.handlers):
        handler = _StdoutHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s: %(message)s"
            )
        )
        lg.addHandler(handler)
    lg.setLevel(level)


def _default_num_workers(device_spec: str) -> int:
    """8 DataLoader workers when training on CUDA (audio decode + mel
    frontend in the main process would starve the GPU), 0 on CPU (the
    workers would only compete with the model for the same cores)."""
    return 8 if device_spec.startswith("cuda") else 0


class AudioLoader:
    """Decoded mono audio @32 kHz from the staged audio root.

    A module-level class, not a closure (fleet job 5913): CropDataset and
    EvalDataset store the loader, and DataLoader workers started with
    ``forkserver`` (Python 3.14's Linux default) PICKLE the dataset into
    each worker. A closure over ``args`` cannot be pickled; this carries
    its root as plain state.

    Shards are staged (untarred) to /raid/scratch by the sbatch BEFORE
    this runs; filepaths resolve under ``root``. Decode matches
    phase_a's: soundfile, mono-mean, refuse 0 samples (a truncated OGG
    decodes empty without raising — measured).
    """

    def __init__(self, root: str) -> None:
        self.root = str(root)

    def __call__(self, filepath: str):
        import numpy as np
        import soundfile as sf

        from birdology.phase_c.data import SAMPLE_RATE

        samples, sr = sf.read(
            str(Path(self.root) / filepath),
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


def _resolve_device(spec: str):
    """``--device`` → ``torch.device`` — or a refusal. NEVER a fallback.

    Fleet job 5730: ten twin cells ran 2h43m at ~1300% CPU with the GPU
    at 0% utilization, because nothing ever moved the trainee off the
    CPU and the training loop inherits its device from the parameters.
    A CUDA request on a stack that cannot serve it now dies HERE, first
    thing in the cell, naming the measured causes — before any
    checkpoint load, dataset construction or training spend.
    """
    import torch

    device = torch.device(spec)
    if device.type == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError(
                f"--device {spec} requested but torch.cuda.is_available() "
                "is False — refusing instead of silently training on CPU "
                "(fleet job 5730: 2h43m at 0% GPU util). Likely causes: "
                "the Slurm allocation lacks --gres=gpu:N (no GPU is "
                "visible inside the container without it), the container "
                "was started without --nv, or this is a CPU-only torch "
                f"build (torch.version.cuda={torch.version.cuda!r}). For a "
                "deliberate CPU run (Mac probe, smoke test) pass "
                "--device cpu explicitly."
            )
        index = device.index if device.index is not None else 0
        if index >= torch.cuda.device_count():
            raise RuntimeError(
                f"--device {spec} requested but only "
                f"{torch.cuda.device_count()} CUDA device(s) are visible "
                f"(cuda:{index} does not exist); check --gres=gpu:N and "
                "CUDA_VISIBLE_DEVICES"
            )
        device = torch.device("cuda", index)
    return device


def _move_to_device(trainee, device):
    """Move the trainee — wrapper INCLUDING its inner frozen model and
    context encoder (they are registered submodules, so one ``.to``
    reaches them) — then VERIFY every parameter and buffer landed. The
    job-5730 failure was silent; this one is not."""
    trainee = trainee.to(device)
    stray = sorted(
        {
            str(t.device)
            for t in list(trainee.parameters()) + list(trainee.buffers())
            if t.device.type != device.type
        }
    )
    if stray:
        raise RuntimeError(
            f"trainee tensors on {stray} after .to({device}) — refusing to "
            f"train off the requested device {device} (fleet job 5730)"
        )
    return trainee


def _build_val_dataset(
    val_path, load_audio, provider, class_codes, frontend
):
    """POW-val as the MULTI-LABEL SOUNDSCAPE set the spec names (§2).

    POW is a soundscape test set: each 5 s window carries zero or more
    species (``ebird_code_multilabel``) and no date (all null-context,
    spec §1). It goes through the SAME path as eval —
    ``build_eval_dataset`` with ``require_context=False`` (null context
    is spec'd for POW, for every arm) and the model's own frontend — and
    ``train._validate`` scores the items' multi-hot ``labels``.

    Fleet job 5679: the val set was built as the focal single-label
    ``CropDataset`` from POW_r4_manifest.parquet, whose every row has
    ``ebird_code == ''``; all 4,560 rows were dropped and every cell
    died on "val set is empty". The table now comes from
    ``phase_c_build_eval_ctx.py --allow-undated``.

    Refusals, each naming *val_path*, all before any training spend:
    no ``ebird_code_multilabel`` column (the focal manifest was passed),
    zero rows, or zero rows with a positive ON the model axis — a val
    set with no positives cannot rank, so early stopping would be blind.
    """
    import pyarrow.parquet as _pq

    from birdology.phase_c.evaluate import build_eval_dataset
    from birdology.phase_c.train import ValMetricError

    val_meta = _pq.read_table(val_path)
    if "ebird_code_multilabel" not in val_meta.column_names:
        raise ValueError(
            f"--val-metadata {val_path} has no 'ebird_code_multilabel' "
            f"column (columns: {val_meta.column_names}) — POW-val is the "
            "multi-label soundscape table built by "
            "scripts/phase_c_build_eval_ctx.py --allow-undated, not the "
            "focal phase_a manifest (whose POW rows all carry "
            "ebird_code == '' — fleet job 5679)"
        )
    val_ds = build_eval_dataset(
        val_meta,
        load_audio,
        provider,
        class_codes,
        frontend=frontend,
        require_context=False,
    )
    if len(val_ds) == 0:
        raise ValMetricError(
            f"val set is empty: {val_path} has zero rows — cmAP over zero "
            "rows is undefined and early stopping would be blind"
        )
    n_positive_rows = int(val_ds.y.any(axis=1).sum())
    if n_positive_rows == 0:
        raise ValMetricError(
            f"val set {val_path} has {len(val_ds)} rows but ZERO with a "
            "positive label on the model's class axis (unscoreable codes: "
            f"{val_ds.unscoreable[:10]}) — a val set with no positives "
            "cannot rank; early stopping would be blind"
        )
    if val_ds.unscoreable:
        print(
            f"PHASE_C_VAL {val_path}: {len(val_ds)} rows, "
            f"{n_positive_rows} with a positive; codes off the model axis "
            f"(unscored): {val_ds.unscoreable}",
            file=sys.stderr,
        )
    return val_ds


def _run_cell(args) -> int:
    """Fleet side: wire CropDataset + wrapper + train_arm + evaluate_model
    for one cell, then write the one row. Imports live here so the module
    stays importable without torch for the row-contract tests."""
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
        AST_N_FRAMES,
        N_MELS,
        SAMPLE_RATE,
        CropDataset,
        ast_frontend,
        convnext_frontend,
        make_label_map,
    )
    from birdology.phase_c.evaluate import build_eval_dataset, evaluate_model
    from birdology.phase_c.film import FiLMAST, FiLMConvNext
    from birdology.phase_c.token import TokenAST
    from birdology.phase_c.train import TrainConfig, train_arm
    from birdology.prior.tensor import load_prior

    # FIRST: the device. A cuda request that cannot be served refuses
    # before any checkpoint load / staging-heavy dataset work (job 5730).
    device = _resolve_device(args.device)

    model_cls = {
        "convnext": ConvNextForImageClassification,
        "ast": ASTForAudioClassification,
    }[args.model]
    model = model_cls.from_pretrained(
        args.checkpoint, revision=args.checkpoint_revision
    )
    # The frontend is the MODEL's contract, selected with the model class:
    # convnext gets the card-recipe (1, 128, 501) mel; ast gets BirdSet's
    # vision pipeline padded/truncated to (1024, 128) with the measured
    # negative-std normalization (data.py: ast_frontend provenance block).
    frontend = {
        "convnext": convnext_frontend,
        "ast": ast_frontend,
    }[args.model]
    if args.model == "ast":
        # The checkpoint's OWN config is the input contract — refuse BY
        # NAME at load (before any training spend) when it does not
        # expect exactly ast_frontend's (AST_N_FRAMES, N_MELS). A
        # mismatch would otherwise shape-crash mid-epoch or, worse,
        # silently garbage-score at GPU cost.
        cfg_len = int(getattr(model.config, "max_length", -1))
        cfg_mels = int(getattr(model.config, "num_mel_bins", -1))
        if cfg_len != AST_N_FRAMES or cfg_mels != N_MELS:
            raise ValueError(
                f"AST checkpoint {args.checkpoint!r} expects inputs "
                f"(max_length={cfg_len}, num_mel_bins={cfg_mels}) but "
                f"ast_frontend emits ({AST_N_FRAMES}, {N_MELS}) — "
                "refusing before training; this checkpoint was not "
                "trained on the BirdSet mel\u2192resize-1024 pipeline "
                "ast_frontend reproduces"
            )
    # Resolve the revision REF to the commit SHA when transformers
    # exposes it (final-review minor): from_pretrained records the
    # resolved hub commit on config._commit_hash for hub downloads.
    # Local paths / stub checkpoints carry None there — then the ref is
    # recorded as given (still non-blank, so the provenance refusal
    # stays intact).
    resolved_revision = (
        getattr(model.config, "_commit_hash", None) or args.checkpoint_revision
    )
    label2id = make_label_map(model.config.id2label)
    class_codes = [
        model.config.id2label[i] for i in sorted(map(int, model.config.id2label))
    ]

    prior = load_prior(Path(args.prior))
    code_to_gbif = json.loads(Path(args.bridge).read_text())
    provider = build_context_matrix(prior, class_codes, code_to_gbif)

    load_audio = AudioLoader(args.audio_root)

    manifest = _pq.read_table(args.manifest)
    train_ds = CropDataset(
        manifest, load_audio, provider, label2id, seed=args.seed,
        frontend=frontend,
    )
    val_ds = _build_val_dataset(
        args.val_metadata, load_audio, provider, class_codes, frontend
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

    # ONCE, before train_arm: train_arm/evaluate_model take their device
    # from the parameters, so this move is what puts the run on the GPU.
    trainee = _move_to_device(trainee, device)

    cfg = TrainConfig(
        arm=args.arm,
        seed=args.seed,
        num_workers=args.num_workers,
        log_every=args.log_every,
    )
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

    from birdology.phase_b.metrics import cmap as _cmap

    # The device the run ACTUALLY used, read off the trainee after
    # training — not the build, not the request.
    provenance = _stack_provenance(
        args, resolved_revision, device=next(trainee.parameters()).device
    )

    # Finding I2: training is test-set-independent, so the cell trains
    # ONCE (above) and loops EVAL over every requested test set — one
    # row per test set. Finding C1: conditioned arms demand a
    # context-joined eval table (require_context) unless
    # --allow-null-context deliberately waives it; the twin never
    # consumes context, so it never requires it.
    require_context = args.arm != "twin" and not args.allow_null_context
    for test_set in args.test_sets:
        eval_path = args.eval_metadata_template.format(test_set=test_set)
        eval_meta = _pq.read_table(eval_path)
        eval_ds = build_eval_dataset(
            eval_meta,
            load_audio,
            provider,
            class_codes,
            frontend=frontend,
            require_context=require_context,
        )
        scores = evaluate_model(
            trainee, eval_ds, arm=args.arm, num_workers=args.num_workers
        )
        cmap_value, n_scored = _cmap(scores, eval_ds.y)

        row = build_row(
            provenance=provenance,
            test_set=test_set,
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
            f"seed={args.seed} {test_set} cmap={cmap_value:.3f} -> {path}"
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
    parser.add_argument(
        "--test-sets",
        required=True,
        action="append",
        help="Test sets to evaluate on after the ONE training (finding "
        "I2): comma-separated and/or repeated. One results row per "
        "test set.",
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument(
        "--val-metadata",
        required=True,
        help="POW-val table: the MULTI-LABEL soundscape table "
        "(filepath, ebird_code_multilabel, cell, week) from "
        "`phase_c_build_eval_ctx.py --allow-undated`, e.g. "
        "/srv/home/rapnic/birdology/data/phase_c/eval_ctx/POW_r4_eval_ctx.parquet — "
        "scored as multi-hot cmAP on the null-context path (spec §2). "
        "NOT the focal phase_a POW manifest (ebird_code == '' on every "
        "row; refused by name).",
    )
    parser.add_argument(
        "--eval-metadata-template",
        required=True,
        help="Per-test-set eval table path template containing "
        "'{test_set}', e.g. "
        "/srv/home/rapnic/birdology/data/phase_c/eval_ctx/{test_set}_eval_ctx.parquet — "
        "the CONTEXT-JOINED tables from phase_c_build_eval_ctx.py "
        "(finding C1), never the raw BirdSet metadata parquets.",
    )
    parser.add_argument("--audio-root", required=True)
    parser.add_argument("--prior", required=True)
    parser.add_argument("--prior-doi", required=True)
    parser.add_argument(
        "--prior-variant",
        choices=PRIOR_VARIANTS,
        required=True,
        help="Which prior artifact this run conditions on (finding I1) "
        "— recorded as the prior_variant results column; the controls "
        "axis of the 135 control runs.",
    )
    parser.add_argument("--bridge", required=True)
    parser.add_argument("--context-source", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--ckpt-dir", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--d-ctx", type=int, default=256)
    parser.add_argument(
        "--allow-null-context",
        action="store_true",
        help="Waive the C1 all-null-context refusal for conditioned "
        "arms — POW-val style tables and DELIBERATE no-context "
        "ablations only.",
    )
    parser.add_argument(
        "--device",
        default="cuda",
        help="Torch device to train/evaluate on (default: cuda). A cuda "
        "request on a stack without a visible GPU REFUSES (fleet job "
        "5730: no silent CPU fallback); Mac probes / CPU smoke runs must "
        "pass --device cpu explicitly. The device actually used is "
        "recorded in the row's device/device_name columns.",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=None,
        help="DataLoader worker processes for train/val/eval (default: "
        "8 on cuda, 0 on cpu). Batches are bit-identical to "
        "num_workers=0 (crops are (seed, epoch, idx)-keyed; pinned by "
        "test).",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=50,
        help="Log one INFO progress line every N train steps (default 50).",
    )
    args = parser.parse_args(argv)
    if args.num_workers is None:
        args.num_workers = _default_num_workers(args.device)
    if args.num_workers < 0:
        parser.error(f"--num-workers must be >= 0 (got {args.num_workers})")
    _configure_logging()
    # --test-sets accepts comma-separated and/or repeated flags.
    args.test_sets = [
        ts for chunk in args.test_sets for ts in chunk.split(",") if ts
    ]

    try:
        return _run_cell(args)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
