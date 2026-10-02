"""Picklable dataset fixtures for the DataLoader-worker tests.

Lives in its own LIGHT module (numpy/torch only) on purpose: DataLoader
workers on macOS are `spawn`ed (and Python 3.14's Linux default is
`forkserver`), so every worker re-imports the module that defines the
dataset it unpickles. Defined in test_train.py, that import dragged in
transformers and the wrappers (~2.6 s per worker vs ~0.7 s here) — the
worker tests were minutes slower for nothing.
"""
from __future__ import annotations

import numpy as np
import torch

N_CLASSES = 5
CONVNEXT_SHAPE = (1, 32, 32)


class SyntheticDataset(torch.utils.data.Dataset):
    """CropDataset's item contract on synthetic rows.

    ``present`` is a PYTHON float (so default collate widens the batch to
    float64, exactly as with the real dataset — the trainer must cast),
    ``epoch`` is a plain mutable attribute, and inputs are keyed by
    (seed, epoch, idx) like data.py's crops. ``record`` (optional list)
    logs every ``(epoch, idx)`` access — the recording-sampler probe.
    """

    def __init__(
        self,
        n_rows: int = 32,
        shape: tuple = CONVNEXT_SHAPE,
        seed: int = 0,
        epoch: int = 0,
        record: list | None = None,
    ) -> None:
        self.n_rows = n_rows
        self.shape = shape
        self.seed = seed
        self.epoch = epoch
        self.record = record

    def __len__(self) -> int:
        return self.n_rows

    def __getitem__(self, idx: int) -> dict:
        if self.record is not None:
            self.record.append((self.epoch, idx))
        rng = np.random.default_rng((self.seed, self.epoch, idx))
        return {
            "inputs": torch.from_numpy(
                rng.standard_normal(self.shape).astype(np.float32)
            ),
            "context": torch.from_numpy(
                rng.standard_normal(N_CLASSES).astype(np.float32)
            ),
            "present": float(idx % 2),
            "label": idx % N_CLASSES,
            # The VAL contract is multi-hot (EvalDataset's ``labels``);
            # this dataset doubles as val, so it carries both shapes.
            "labels": torch.nn.functional.one_hot(
                torch.tensor(idx % N_CLASSES), N_CLASSES
            ),
        }


def fixture_audio(filepath: str) -> np.ndarray:
    """Deterministic per-file audio, LONGER than the crop (so the
    (seed, epoch, idx) offset RNG is exercised), one file shorter (the
    waveform-domain pad path)."""
    idx = int(filepath.split(".")[0][1:])
    n = 1800 if idx == 3 else 6000 + 997 * idx
    rng = np.random.default_rng(idx)
    return (rng.standard_normal(n) * 0.1).astype(np.float32)


class FixtureProvider:
    """Non-trivial context + mixed present flags (dated and undated)."""

    def vector(self, cell, week):
        if cell is None:
            return np.zeros(N_CLASSES, dtype=np.float32), 0.0
        rng = np.random.default_rng(int(week))
        return rng.standard_normal(N_CLASSES).astype(np.float32), 1.0
