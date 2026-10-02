"""Phase C training loop — twins, two-group optimizer, loud val, resume.

THE TWIN IS A CONTROLLED COMPARISON, NOT A SECOND EXPERIMENT (spec §1)
----------------------------------------------------------------------
The twin receives the BARE HF model, the identical ``TrainConfig`` and
seed, and — controller ruling — its single optimizer group runs at
``lr * backbone_lr_scale``: the SAME rate the conditioned runs' backbone
group gets. Everything about the twin's schedule, batch order, crops and
backbone LR is identical to the conditioned run; the ONLY difference is
the absence of conditioning modules. Batch order is seeded by
``(cfg.seed, epoch)`` alone (never the global RNG), so twin and
conditioned runs with the same seed see the same indices by construction.

TWO PARAM GROUPS, SPLIT BY OWNERSHIP, NOT BY NAME
-------------------------------------------------
The wrappers never mutate the wrapped model (film.py / token.py), so the
split is structural: the inner ``wrapper.model``'s parameters are the
backbone group (``lr * backbone_lr_scale``); every OTHER wrapper
parameter — FiLM heads + context encoder, or null_token + project +
context encoder — is the conditioning group at full ``lr``. No name
lists to go stale when a wrapper grows a module.

REVIEW FOCUS 4 — THE VAL METRIC IS LOUD OR THE RUN IS DEAD
----------------------------------------------------------
An empty val set, a non-finite logit, or a NaN/empty cmAP raises
``ValMetricError`` IMMEDIATELY — before the epoch is checkpointed, so a
Slurm requeue can never resume into a poisoned state. Early stopping
compares real numbers or it does not run.

REVIEW FOCUS 5 — RESUME IS BIT-EXACT, AND HERE IS WHY
-----------------------------------------------------
``last.pt`` carries model + optimizer + epoch + torch/numpy/python RNG
states (plus the training device's CUDA generator when on CUDA), captured AFTER the epoch's validation — exactly the state the
next epoch would start from. On resume everything is restored and the
run continues at ``epoch + 1``. Bit-equality with an uninterrupted run
then follows from three facts: batch order is a pure function of
``(cfg.seed, epoch)``; crop content is a pure function of
``(dataset.seed, epoch, idx)`` (data.py — this loop SETS
``train_ds.epoch`` each epoch, including after resume); and AdamW's
state round-trips exactly through torch.save.

DATALOADER WORKERS DO NOT CHANGE A SINGLE BIT — PINNED, NOT CLAIMED
------------------------------------------------------------------
Fleet job 5730: with the model on the GPU, audio decode + the mel
frontend in the main process starve it, so ``num_workers`` is a CLI
knob (phase_c_run: 8 on cuda, 0 on cpu). Workers are value-neutral
because of four facts, each pinned in tests/phase_c/test_train.py
(TestDataLoaderWorkers, TestResumeWithWorkers):

1. Item VALUES are a pure function of (seed, epoch, idx): CropDataset's
   crop offset is ``default_rng((seed, epoch, idx))``, the frontend is
   deterministic, and no dataset touches the torch RNG — so which
   process evaluates an item is irrelevant. (Measured bit-identical,
   one epoch of inputs/context/present/label, 0 vs 2 workers, on the
   real CropDataset + convnext_frontend — including collate's float64
   ``present`` widening, which happens identically in either process.)
2. ORDER is the sampler's, in the main process; the multi-process
   iterator re-orders worker output back to sampler order.
3. NO LOADER EVER DRAWS FROM THE GLOBAL TORCH RNG. Every DataLoader
   iterator draws a ``_base_seed`` (worker seeding) from
   ``loader.generator`` at creation — from the GLOBAL generator when
   that is None. dropout/drop_path consume the global RNG in the main
   process, so a loader drawing there would couple weights to loader
   bookkeeping: a persistent val loader draws once per RUN, a fresh
   one once per EPOCH, and a resumed leg would diverge. Train loaders
   carry the (seed, epoch) generator (as before); the val loader now
   carries its own fixed-seed generator.
4. The train loader is built fresh per epoch and NEVER persistent:
   workers hold a pickled COPY of the dataset taken when the pool
   starts, so the epoch dial set in the main process only reaches
   workers through a new pool — a persistent pool on a reused loader
   would keep a stale ``train_ds.epoch``. The val loader (no epoch
   dial) is persistent when it has workers.

``pin_memory`` is on iff the device is CUDA (transfer-only; values
unchanged).

COLLATE WIDENS ``present`` TO FLOAT64 — CAST AT THE SEAM
--------------------------------------------------------
``CropDataset`` yields ``present`` as a python float; torch's default
collate builds a float64 batch tensor from python floats. The wrappers'
gate arithmetic must stay float32 (the identity gate's bit-exactness
lives there), so ``present`` and ``context`` are cast to float32 here,
once, where the batch is unpacked.

Plain torch throughout: AdamW, constant LR, no lightning, no schedulers
(the plan's YAGNI ruling).
"""
from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader

from birdology.phase_b.metrics import cmap

logger = logging.getLogger(__name__)

#: The arm vocabulary — the whole of it.
_ARMS = ("film", "token", "twin")

#: Checkpoint filenames inside ckpt_dir.
_LAST = "last.pt"
_BEST = "best.pt"


class ValMetricError(RuntimeError):
    """Validation produced nothing comparable (Review Focus 4).

    Raised when the val set is empty, val logits are non-finite, or cmAP
    is NaN/scored zero classes — the run aborts instead of silently
    training to max_epochs on a meaningless early-stop signal.
    """


@dataclass
class TrainConfig:
    """One arm's training recipe — shared verbatim between a conditioned
    run and its twin.

    ``arm`` ∈ {"film", "token", "twin"}. The twin receives the BARE HF
    model and this same config/seed; controller ruling: its single
    optimizer group runs at ``lr * backbone_lr_scale`` — identical to
    the conditioned runs' backbone group — so the twin differs from the
    conditioned run ONLY by the absence of conditioning modules, never
    by learning rate.
    """

    arm: str
    seed: int
    lr: float = 1e-4
    backbone_lr_scale: float = 0.1
    max_epochs: int = 2
    patience: int = 1
    batch_size: int = 64
    #: DataLoader workers. 0 = main-process loading (the library
    #: default; phase_c_run picks 8 on cuda). Bit-neutral — module
    #: docstring, "DATALOADER WORKERS", pinned by test.
    num_workers: int = 0
    #: Progress cadence: one INFO line every ``log_every`` train steps
    #: (fleet job 5730 — 2.5 h of silence hid a CPU-bound run).
    log_every: int = 50

    def __post_init__(self) -> None:
        if self.arm not in _ARMS:
            raise ValueError(
                f"unknown arm {self.arm!r}; expected one of {_ARMS}"
            )
        if self.num_workers < 0:
            raise ValueError(
                f"num_workers must be >= 0 (got {self.num_workers})"
            )
        if self.log_every < 1:
            raise ValueError(
                f"log_every must be >= 1 (got {self.log_every})"
            )


@dataclass
class TrainResult:
    """What Task 6's runner consumes."""

    val_cmap_history: list[float] = field(default_factory=list)
    best_state_path: str = ""
    epochs_ran: int = 0
    resumed: bool = False


def build_optimizer(
    model_or_wrapper: nn.Module, cfg: TrainConfig
) -> torch.optim.AdamW:
    """AdamW with the spec §1 Trainability split.

    Conditioned arms: group 0 = the inner ``wrapper.model`` (backbone)
    at ``lr * backbone_lr_scale``; group 1 = every other wrapper
    parameter (the conditioning modules) at full ``lr``. The split is by
    parameter OWNERSHIP — the wrappers never inject parameters into the
    wrapped model, so "not the inner model's" IS "conditioning".

    Twin: one group, all parameters at ``lr * backbone_lr_scale`` (the
    controller ruling in the class docstring).
    """
    backbone_lr = cfg.lr * cfg.backbone_lr_scale
    if cfg.arm == "twin":
        return torch.optim.AdamW(
            [{"params": list(model_or_wrapper.parameters()), "lr": backbone_lr}]
        )

    backbone = list(model_or_wrapper.model.parameters())
    backbone_ids = {id(p) for p in backbone}
    conditioning = [
        p for p in model_or_wrapper.parameters() if id(p) not in backbone_ids
    ]
    return torch.optim.AdamW(
        [
            {"params": backbone, "lr": backbone_lr},
            {"params": conditioning, "lr": cfg.lr},
        ]
    )


def _epoch_seed(seed: int, epoch: int) -> int:
    """Batch-order seed: a pure function of (run seed, epoch).

    Deliberately independent of the global torch RNG so twin and
    conditioned runs — and a resumed leg — derive identical shuffles
    from the config alone.
    """
    return seed * 1_000_003 + epoch


#: The val loader's own generator seed. Its value is irrelevant (val is
#: unshuffled and EvalDataset never reads torch RNG); its EXISTENCE is
#: what keeps the loader off the global torch RNG (module docstring,
#: fact 3).
_VAL_LOADER_SEED = 0


def _loader_kwargs(cfg: TrainConfig, device: torch.device) -> dict:
    return {
        "batch_size": cfg.batch_size,
        "num_workers": cfg.num_workers,
        "pin_memory": torch.device(device).type == "cuda",
    }


def make_train_loader(
    train_ds, cfg: TrainConfig, epoch: int, device: torch.device
) -> DataLoader:
    """One epoch's train loader: order from (cfg.seed, epoch) alone —
    twin-identical and resume-identical by construction — never
    persistent (fact 4), pinned on CUDA."""
    generator = torch.Generator()
    generator.manual_seed(_epoch_seed(cfg.seed, epoch))
    return DataLoader(
        train_ds,
        shuffle=True,
        generator=generator,
        persistent_workers=False,
        **_loader_kwargs(cfg, device),
    )


def make_val_loader(
    val_ds, cfg: TrainConfig, device: torch.device
) -> DataLoader:
    """The run's val loader: unshuffled, its OWN generator (fact 3),
    persistent iff it has workers (reused every epoch, no epoch dial)."""
    generator = torch.Generator()
    generator.manual_seed(_VAL_LOADER_SEED)
    return DataLoader(
        val_ds,
        shuffle=False,
        generator=generator,
        persistent_workers=cfg.num_workers > 0,
        **_loader_kwargs(cfg, device),
    )


def _unpack_inputs(batch: dict, device: torch.device):
    """Batch dict → (inputs, context, present (B,1)), float32.

    Default collate widens ``present`` (a per-item python float) to a
    float64 tensor; the wrappers' gate arithmetic is float32, so the
    cast happens here, once.
    """
    inputs = batch["inputs"].to(device=device, dtype=torch.float32)
    context = batch["context"].to(device=device, dtype=torch.float32)
    present = (
        batch["present"].to(device=device, dtype=torch.float32).unsqueeze(1)
    )
    return inputs, context, present


def _unpack(batch: dict, device: torch.device):
    """TRAIN batch → (inputs, context, present (B,1), labels): the focal
    single-int ``label`` (CropDataset's contract). Val batches are
    multi-hot and go through :func:`_unpack_inputs` + ``labels``."""
    inputs, context, present = _unpack_inputs(batch, device)
    labels = batch["label"].to(device=device)
    return inputs, context, present, labels


def _forward(
    model: nn.Module, arm: str, inputs, context, present
) -> torch.Tensor:
    """One forward, dispatched on the arm.

    Twin: the bare HF model — ``model(inputs).logits`` (pixel_values /
    input_values are both the first positional arg). Conditioned:
    the wrapper contract ``forward(inputs, context, present) -> logits``.
    """
    if arm == "twin":
        return model(inputs).logits
    return model(inputs, context, present)


def _validate(
    model: nn.Module, val_loader: DataLoader, cfg: TrainConfig, device
) -> float:
    """POW-val cmAP over the whole val set — finite or ValMetricError.

    POW is a MULTI-LABEL soundscape set (spec §2): each val item carries
    ``labels``, a multi-hot row on the model's class axis (EvalDataset's
    item contract) — zero or more species per 5 s window. All-zero rows
    are legitimate empty windows: they rank as negatives in every class,
    and classes with no positive are excluded by cmap's n_scored rule.
    (The original one-hot-from-``label`` path was the focal-train
    contract and silently mis-scored soundscapes — fleet job 5679.)

    Scores are raw logits (cmAP is rank-based; sigmoid is monotone and
    changes nothing). Non-finite logits abort here too: a diverged model
    must kill the run, not feed garbage ranks to early stopping.
    """
    n_rows = len(val_loader.dataset)
    logger.info("val start rows=%d device=%s", n_rows, device)
    t0 = time.perf_counter()
    model.eval()
    all_logits: list[np.ndarray] = []
    all_labels: list[np.ndarray] = []
    with torch.no_grad():
        for batch in val_loader:
            inputs, context, present = _unpack_inputs(batch, device)
            logits = _forward(model, cfg.arm, inputs, context, present)
            all_logits.append(logits.cpu().numpy())
            all_labels.append(batch["labels"].cpu().numpy())
    model.train()

    scores = np.concatenate(all_logits)
    if not np.isfinite(scores).all():
        raise ValMetricError(
            "validation logits contain non-finite values — the model has "
            "diverged; aborting instead of ranking garbage"
        )
    y = np.concatenate(all_labels).astype(np.int64)
    if y.shape != scores.shape:
        raise ValMetricError(
            f"val multi-hot labels {y.shape} do not match logits "
            f"{scores.shape} — the val set is not on the model's class axis"
        )

    value, n_scored = cmap(scores, y)
    if n_scored == 0 or not np.isfinite(value):
        raise ValMetricError(
            f"val cmAP is not comparable (value={value}, n_scored="
            f"{n_scored}) — early stopping cannot run; aborting (Review "
            "Focus 4)"
        )
    logger.info(
        "val end rows=%d cmap=%.4f n_scored=%d wall=%.1fs",
        n_rows, value, n_scored, time.perf_counter() - t0,
    )
    return value


def _rng_snapshot(device: torch.device) -> dict:
    """Every RNG the next epoch reads. On CUDA that includes the
    DEVICE's generator: dropout/drop_path on a CUDA tensor draw from it,
    not from the CPU generator (job-5730 follow-up — before the model
    ever reached the GPU, the CPU state was the whole story)."""
    snap = {
        "torch_rng": torch.get_rng_state(),
        "numpy_rng": np.random.get_state(),
        "python_rng": random.getstate(),
    }
    if torch.device(device).type == "cuda":
        snap["cuda_rng"] = torch.cuda.get_rng_state(device)
    return snap


def _rng_restore(ckpt: dict, device: torch.device) -> None:
    torch.set_rng_state(ckpt["torch_rng"])
    np.random.set_state(ckpt["numpy_rng"])
    random.setstate(ckpt["python_rng"])
    if torch.device(device).type == "cuda":
        if "cuda_rng" not in ckpt:
            raise RuntimeError(
                "resuming on CUDA from a checkpoint without 'cuda_rng' — "
                "it was written before the CUDA generator was "
                "checkpointed (or on CPU), so the resumed leg's "
                "dropout/drop_path masks would silently diverge; start "
                "the cell from an empty ckpt_dir"
            )
        torch.cuda.set_rng_state(ckpt["cuda_rng"], device)


def train_arm(
    model_or_wrapper: nn.Module,
    train_ds,
    val_ds,
    cfg: TrainConfig,
    ckpt_dir: str | Path,
) -> TrainResult:
    """Train one arm (or its twin); resume from ``ckpt_dir`` if preempted.

    An existing ``last.pt`` in *ckpt_dir* means a preempted run: model,
    optimizer, epoch, RNG states and early-stop bookkeeping are restored
    and training continues at the next epoch — bit-equal to a run that
    was never interrupted (Review Focus 5; see module docstring for the
    three facts this stands on).

    No-op resume: if the checkpoint already covers ``cfg.max_epochs``
    (i.e. ``start_epoch >= cfg.max_epochs``, e.g. a requeue that lands
    after the final epoch), no training or validation runs and the call
    returns silently with ``resumed=True``, ``epochs_ran == start_epoch``
    and the checkpointed ``val_cmap_history`` — callers (Task 6) can
    treat the call as idempotent rather than an error.

    ``epochs_ran`` counts epochs completed over the WHOLE run (both legs
    of a preempted run), not just this call.
    """
    ckpt_dir = Path(ckpt_dir)
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    last_path = ckpt_dir / _LAST
    best_path = ckpt_dir / _BEST

    if len(val_ds) == 0:
        raise ValMetricError(
            "val set is empty — cmAP over zero rows is undefined and "
            "early stopping would be blind; aborting (Review Focus 4)"
        )

    device = next(model_or_wrapper.parameters()).device
    criterion = nn.BCEWithLogitsLoss()
    optimizer = build_optimizer(model_or_wrapper, cfg)

    val_history: list[float] = []
    best_val = float("-inf")
    bad_epochs = 0
    start_epoch = 0
    resumed = False

    if last_path.exists():
        # Preemption resume: restore everything the checkpoint carries.
        ckpt = torch.load(last_path, weights_only=False)
        model_or_wrapper.load_state_dict(ckpt["model_state"])
        optimizer.load_state_dict(ckpt["optimizer_state"])
        _rng_restore(ckpt, device)
        start_epoch = ckpt["epoch"] + 1
        val_history = list(ckpt["val_history"])
        best_val = ckpt["best_val"]
        bad_epochs = ckpt["bad_epochs"]
        resumed = True
    else:
        # Fresh run: one deterministic starting point for the global RNGs
        # so dropout/anything-stochastic is a function of cfg.seed.
        torch.manual_seed(cfg.seed)

    val_loader = make_val_loader(val_ds, cfg, device)

    epochs_completed = start_epoch
    model_or_wrapper.train()
    for epoch in range(start_epoch, cfg.max_epochs):
        # The epoch dial: crop offsets are (seed, epoch, idx)-keyed in
        # data.py; the trainer owns setting it — including after resume.
        train_ds.epoch = epoch

        # Batch order from (cfg.seed, epoch) alone — twin-identical and
        # resume-identical by construction.
        train_loader = make_train_loader(train_ds, cfg, epoch, device)

        # Training steps. Note: a NaN training loss still executes its
        # optimizer.step() (poisoning the weights) before validation
        # below aborts the epoch — harmless, because the abort happens
        # BEFORE checkpointing, so nothing poisoned is ever persisted.
        n_steps = len(train_loader)
        t_epoch = time.perf_counter()
        n_seen = 0
        for step, batch in enumerate(train_loader, start=1):
            inputs, context, present, labels = _unpack(batch, device)
            logits = _forward(model_or_wrapper, cfg.arm, inputs, context, present)
            one_hot = torch.zeros_like(logits)
            one_hot[torch.arange(labels.size(0), device=device), labels] = 1.0
            loss = criterion(logits, one_hot)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            n_seen += labels.size(0)
            if step % cfg.log_every == 0:
                # loss.item() syncs the device — only on the log cadence.
                elapsed = time.perf_counter() - t_epoch
                logger.info(
                    "train epoch %d/%d step %d/%d loss=%.5f samples/s=%.1f "
                    "elapsed=%.1fs device=%s",
                    epoch + 1, cfg.max_epochs, step, n_steps, loss.item(),
                    n_seen / elapsed if elapsed > 0 else float("nan"),
                    elapsed, device,
                )

        # Validate BEFORE checkpointing: a poisoned val metric raises here
        # and the epoch is never persisted (Review Focus 4).
        val_value = _validate(model_or_wrapper, val_loader, cfg, device)
        val_history.append(val_value)
        epochs_completed = epoch + 1

        if val_value > best_val:
            best_val = val_value
            bad_epochs = 0
            torch.save(
                {"model_state": model_or_wrapper.state_dict(), "epoch": epoch},
                best_path,
            )
        else:
            bad_epochs += 1

        # RNG captured HERE — after train+val — is exactly the state the
        # next epoch starts from; resume restores it verbatim.
        torch.save(
            {
                "epoch": epoch,
                "model_state": model_or_wrapper.state_dict(),
                "optimizer_state": optimizer.state_dict(),
                "val_history": val_history,
                "best_val": best_val,
                "bad_epochs": bad_epochs,
                **_rng_snapshot(device),
            },
            last_path,
        )

        if bad_epochs >= cfg.patience:
            break

    return TrainResult(
        val_cmap_history=val_history,
        best_state_path=str(best_path),
        epochs_ran=epochs_completed,
        resumed=resumed,
    )
