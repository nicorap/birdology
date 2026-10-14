"""The Danish head: a classifier on frozen BirdNET embeddings, fused with BirdNET.

The head sees standardized 1024-d BirdNET embeddings and predicts one of the
species it saw at least ``min_train`` times in training (softmax over that
subset of BirdNET's axis). At inference its log-probabilities are spread back
onto BirdNET's full axis -- species it never saw get ``log(floor_q)`` -- and
added to BirdNET's log scores:

    fused = log s + alpha * log q

so BirdNET still decides between species the head knows nothing about, at a
fixed penalty. alpha, weight decay and learning rate are chosen on a held-out
validation split, never on the test set.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import torch

from birdology.phase_d.evaluate import log_scores

#: Two layers are enough; deeper heads overfit the ~120k European recordings.
HIDDEN = 1024


def make_head(kind: str, dim: int, n_classes: int) -> torch.nn.Module:
    if kind == "linear":
        return torch.nn.Linear(dim, n_classes)
    if kind == "mlp":
        return torch.nn.Sequential(
            torch.nn.Dropout(0.2), torch.nn.Linear(dim, HIDDEN), torch.nn.GELU(),
            torch.nn.Dropout(0.3), torch.nn.Linear(HIDDEN, n_classes),
        )
    raise ValueError(f"unknown head kind {kind!r} (expected 'linear' or 'mlp')")


@dataclass(frozen=True)
class TrainConfig:
    kind: str = "mlp"
    weight_decay: float = 1e-2
    lr: float = 1e-4
    max_epochs: int = 60
    patience: int = 5          # epochs without val-loss improvement before stopping
    batch_size: int = 512
    label_smoothing: float = 0.1
    seed: int = 0


@dataclass
class TrainResult:
    model: torch.nn.Module
    val_loss: float
    best_epoch: int
    val_top1: float


def train_head(
    X_train: torch.Tensor, y_train: torch.Tensor,
    X_val: torch.Tensor, y_val: torch.Tensor,
    n_classes: int, cfg: TrainConfig,
) -> TrainResult:
    """AdamW with early stopping on validation cross-entropy; keeps the best epoch."""
    torch.manual_seed(cfg.seed)
    model = make_head(cfg.kind, X_train.shape[1], n_classes)
    opt = torch.optim.AdamW(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)
    best, best_state, best_epoch, stale = float("inf"), None, -1, 0
    for epoch in range(cfg.max_epochs):
        model.train()
        perm = torch.randperm(len(y_train))
        for i in range(0, len(perm), cfg.batch_size):
            b = perm[i:i + cfg.batch_size]
            loss = torch.nn.functional.cross_entropy(model(X_train[b]), y_train[b], label_smoothing=cfg.label_smoothing)
            opt.zero_grad()
            loss.backward()
            opt.step()
        model.eval()
        with torch.no_grad():
            val_loss = torch.nn.functional.cross_entropy(model(X_val), y_val).item()
        if val_loss < best - 1e-4:
            best, best_epoch, stale = val_loss, epoch, 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            stale += 1
            if stale >= cfg.patience:
                break
    model.load_state_dict(best_state)
    model.eval()
    with torch.no_grad():
        top1 = (model(X_val).argmax(1) == y_val).float().mean().item()
    return TrainResult(model, best, best_epoch, top1)


@dataclass
class DanishHead:
    """A trained head plus everything needed to apply it to new embeddings."""

    model: torch.nn.Module
    kind: str
    classes: np.ndarray          # BirdNET-axis indices the head predicts, in head-output order
    n_axis: int                  # length of BirdNET's label axis
    mu: np.ndarray               # embedding standardization (train split)
    sd: np.ndarray
    alpha: float = 1.0
    floor_q: float = 1e-4
    remap: dict[str, str] = field(default_factory=dict)
    meta: dict = field(default_factory=dict)

    def log_q(self, E: np.ndarray) -> np.ndarray:
        """(n, n_axis) head log-probabilities; unseen species get log(floor_q)."""
        floor = np.log(self.floor_q)
        with torch.no_grad():
            z = torch.from_numpy(((np.asarray(E, np.float32) - self.mu) / self.sd).astype(np.float32))
            q = torch.log_softmax(self.model(z), 1).numpy().astype(np.float64)
        out = np.full((len(q), self.n_axis), floor)
        out[:, self.classes] = np.maximum(q, floor)
        return out

    def fuse(self, S: np.ndarray, E: np.ndarray, alpha: float | None = None) -> np.ndarray:
        """log s + alpha * log q on BirdNET's axis."""
        a = self.alpha if alpha is None else alpha
        return log_scores(S) + a * self.log_q(E)

    def head_only(self, E: np.ndarray) -> np.ndarray:
        """The head alone: species it never saw are -inf."""
        X = np.full((len(E), self.n_axis), -np.inf)
        X[:, self.classes] = self.log_q(E)[:, self.classes]
        return X

    def save(self, path: Path) -> None:
        torch.save({
            "state": self.model.state_dict(), "kind": self.kind, "classes": np.asarray(self.classes),
            "n_axis": self.n_axis, "mu": self.mu, "sd": self.sd, "alpha": self.alpha,
            "floor_q": self.floor_q, "remap": dict(self.remap), "meta": dict(self.meta),
        }, path)

    @classmethod
    def load(cls, path: Path) -> "DanishHead":
        ck = torch.load(path, weights_only=False)
        n_axis = ck.get("n_axis")
        if n_axis is None:                      # checkpoints written before n_axis was stored
            raise ValueError(f"{path}: checkpoint has no n_axis; re-save it with this module")
        model = make_head(ck["kind"], len(ck["mu"]), len(ck["classes"]))
        model.load_state_dict(ck["state"])
        model.eval()
        return cls(model=model, kind=ck["kind"], classes=np.asarray(ck["classes"]), n_axis=int(n_axis),
                   mu=ck["mu"], sd=ck["sd"], alpha=float(ck["alpha"]), floor_q=float(ck["floor_q"]),
                   remap=dict(ck.get("remap", {})), meta=dict(ck.get("meta", {})))
