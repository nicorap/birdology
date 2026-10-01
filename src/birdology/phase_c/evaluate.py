"""Phase C eval runner — test-set segments through a (wrapped) checkpoint.

LABELS JOIN BY FILEPATH, AND A DUPLICATE REFUSES (Review Focus 3)
-----------------------------------------------------------------
The test-set metadata parquet carries one row per 5 s segment with its
``filepath`` and ``ebird_code_multilabel`` ground truth. The label join
is BY FILEPATH — the exact contract of ``repro_gate.score_pair`` /
``sweep._align_eval_labels`` — and a filepath appearing TWICE in the
metadata is refused naming the offender: a duplicate would either
double-count its labels or silently keep one of two contradictory rows,
and phase_b already paid for this hazard once.

Here the metadata table IS the row source (there is no separate cache
table to join against — the model is run live), so "join by filepath"
degenerates to "the filepath column must be a key". The refusal keeps
the invariant that makes it a key.

LABELS LAND ON THE MODEL'S OWN CLASS AXIS, BY CODE
--------------------------------------------------
``class_codes`` is the model's axis in index order (the values of
``model.config.id2label`` sorted by key — measured: the values ARE
eBird codes). Column j of ``y`` holds ``class_codes[j]``; a metadata
label absent from the axis is counted and NAMED in ``unscoreable``,
never a KeyError and never a silent drop (the phase_b discipline).

EVAL WINDOWS ARE DETERMINISTIC — NO RANDOM CROPS
------------------------------------------------
A test row IS a 5 s segment: shorter audio zero-pads right in the
WAVEFORM domain (data.py's ruling — padding in normalized-dB space
would fake energy floors), longer audio takes the head. Two calls for
the same index are identical; eval never consumes RNG.

CONTEXT: NULL FOR UNDATED ROWS, AND FOR UNDATED TEST SETS
---------------------------------------------------------
Rows with null (cell, week) — and metadata parquets with NO cell/week
columns at all (POW ships without them; all of POW is the null-context
path, spec §2) — go through ``ContextProvider.vector(None, None)``:
zeros + present 0.0, the provider's own absent-context answer. Never a
KeyError on a missing column, never a fabricated prior.

``evaluate_model`` batches through torch's default collate exactly like
training — ``present`` arrives as a python float and widens to float64,
so the float32 cast happens at the same seam as train.py's ``_unpack``.
Scores are RAW logits: cmAP is rank-based and sigmoid is monotone, and
the gate's metrics must never rank saturated probabilities
(repro_gate.py's calibration lesson).
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Sequence

import numpy as np
import pyarrow as pa
import torch
from torch.utils.data import DataLoader

#: 5 s @ 32 kHz — the segment window every BirdSet test row represents.
SAMPLE_RATE = 32000
_CROP_SECONDS_DEFAULT = 5.0


@dataclass
class EvalDataset(torch.utils.data.Dataset):
    """Test-set segments: ``{inputs, context, present}`` per item, plus
    the aligned label matrix ``y`` (n_rows, n_classes) and the counted
    ``unscoreable`` codes. Built by :func:`build_eval_dataset`.
    """

    rows: list[dict]
    load_audio: Callable[[str], np.ndarray]
    context_provider: object
    class_codes: list[str]
    frontend: Callable[[torch.Tensor], torch.Tensor]
    crop_samples: int
    y: np.ndarray = field(repr=False)
    unscoreable: list[str] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.rows)

    def _window(self, wave: np.ndarray) -> np.ndarray:
        """Deterministic 5 s window: pad right (waveform domain) or head."""
        n = len(wave)
        if n < self.crop_samples:
            return np.pad(wave, (0, self.crop_samples - n))
        return wave[: self.crop_samples]

    def __getitem__(self, idx: int) -> dict:
        row = self.rows[idx]
        wave = np.ascontiguousarray(
            self.load_audio(row["filepath"]), dtype=np.float32
        )
        window = self._window(wave)
        inputs = self.frontend(torch.from_numpy(window))
        vector, present = self.context_provider.vector(
            row["cell"], row["week"]
        )
        return {
            "inputs": inputs,
            "context": torch.from_numpy(np.asarray(vector)),
            "present": float(present),
        }


def build_eval_dataset(
    metadata: pa.Table,
    load_audio: Callable[[str], np.ndarray],
    context_provider: object,
    class_codes: Sequence[str],
    frontend: Callable[[torch.Tensor], torch.Tensor],
    crop_s: float = _CROP_SECONDS_DEFAULT,
) -> EvalDataset:
    """One EvalDataset from a test-set metadata parquet table.

    *metadata* needs ``filepath`` + ``ebird_code_multilabel``; ``cell``
    and ``week`` are optional (absent columns = the whole set is
    null-context, POW's shape). A duplicate filepath REFUSES naming the
    offenders (Review Focus 3).
    """
    filepaths = metadata.column("filepath").to_pylist()
    counts = Counter(filepaths)
    duplicates = sorted(f for f, n in counts.items() if n > 1)
    if duplicates:
        raise ValueError(
            f"{len(duplicates)} duplicate filepath(s) in eval metadata: "
            f"{duplicates[:5]} -- labels join BY FILEPATH and a duplicate "
            "would double-count its labels or silently drop one of two "
            "contradictory rows; refusing (Review Focus 3, the phase_b "
            "join hazard)"
        )

    multilabels = metadata.column("ebird_code_multilabel").to_pylist()
    columns = metadata.column_names
    cells = (
        metadata.column("cell").to_pylist()
        if "cell" in columns
        else [None] * len(filepaths)
    )
    weeks = (
        metadata.column("week").to_pylist()
        if "week" in columns
        else [None] * len(filepaths)
    )

    # Label matrix on the model's axis, by code; gaps counted and named.
    class_codes = list(class_codes)
    col_of = {code: j for j, code in enumerate(class_codes)}
    y = np.zeros((len(filepaths), len(class_codes)), dtype=np.int64)
    unscoreable_counts: Counter[str] = Counter()
    for i, labels in enumerate(multilabels):
        for code in labels or []:
            j = col_of.get(code)
            if j is None:
                unscoreable_counts[code] += 1
            else:
                y[i, j] = 1

    rows = [
        {"filepath": f, "cell": c, "week": w}
        for f, c, w in zip(filepaths, cells, weeks)
    ]
    return EvalDataset(
        rows=rows,
        load_audio=load_audio,
        context_provider=context_provider,
        class_codes=class_codes,
        frontend=frontend,
        crop_samples=int(round(crop_s * SAMPLE_RATE)),
        y=y,
        unscoreable=sorted(unscoreable_counts),
    )


def evaluate_model(
    model_or_wrapper: torch.nn.Module,
    eval_ds: EvalDataset,
    *,
    arm: str,
    batch_size: int = 64,
    num_workers: int = 0,
) -> np.ndarray:
    """Per-segment raw logits ``(n_rows, n_classes)`` in dataset order.

    ``arm == "twin"`` calls the bare HF model (``model(inputs).logits``);
    any conditioned arm goes through the wrapper contract
    ``forward(inputs, context, present) -> logits`` — the same dispatch
    as ``train._forward``. An empty dataset returns ``(0, n_classes)``:
    the ZERO-ROWS REFUSAL belongs to the gate, which must fail loudly
    rather than have the runner invent a shape error.
    """
    n_classes = len(eval_ds.class_codes)
    if len(eval_ds) == 0:
        return np.zeros((0, n_classes), dtype=np.float32)

    device = next(model_or_wrapper.parameters()).device
    loader = DataLoader(
        eval_ds,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
    )

    was_training = model_or_wrapper.training
    model_or_wrapper.eval()
    all_logits: list[np.ndarray] = []
    try:
        with torch.no_grad():
            for batch in loader:
                inputs = batch["inputs"].to(device=device, dtype=torch.float32)
                context = batch["context"].to(
                    device=device, dtype=torch.float32
                )
                # Default collate widens the python-float present to
                # float64 — cast at the seam, as train._unpack does.
                present = (
                    batch["present"]
                    .to(device=device, dtype=torch.float32)
                    .unsqueeze(1)
                )
                if arm == "twin":
                    logits = model_or_wrapper(inputs).logits
                else:
                    logits = model_or_wrapper(inputs, context, present)
                all_logits.append(logits.cpu().numpy())
    finally:
        if was_training:
            model_or_wrapper.train()

    return np.concatenate(all_logits)
