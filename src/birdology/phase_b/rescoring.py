"""Bayesian rescoring of encoder posteriors by the occurrence prior.

The first fusion arm of Phase B, and its credibility backbone: a training-
free log-space correction

    log s + tau * (log p - log m)

(*s* the encoder score, *p* the context prior, *m* the training marginal,
*tau* a temperature on the prior exponent), plus the FOUR CONTROLS —
uniform, marginal, space-shuffled, time-shuffled — whose defining
properties separate a real spatiotemporal effect from an artifact. A gain
that survives all four controls is a gain the context genuinely paid for.

AXIS BRIDGE: the prior speaks GBIF species_key (int); the caches speak
eBird code (str). The bridge arrives as INPUT (``code_to_gbif``) and is
treated as PARTIAL: a scoreable species it cannot map — absent from the
dict, or mapped to a key not on the prior's species axis — gets the FLOOR
prior, counted in ``n_floored`` and named in ``missing_species``, never a
KeyError and never a silent drop. ``FLOOR`` is IMPORTED from
``birdology.prior.tensor`` (the artifact's own "no evidence" value), never
re-defined: a second constant could drift from the convention the tensor
reads under.

numpy only; scipy.sparse appears solely to rebuild control matrices in the
shape ``PriorTensor.__post_init__`` validates.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal, Sequence

import numpy as np
from scipy import sparse

from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import FLOOR, PriorTensor

#: spec.variant value carried by each control, matching PriorSpec's
#: documented vocabulary ("uniform" | "marginal" | "shuffle_space" |
#: "shuffle_time").
_CONTROL_VARIANTS = {
    "uniform": "uniform",
    "marginal": "marginal",
    "space_shuffled": "shuffle_space",
    "time_shuffled": "shuffle_time",
}


@dataclass(frozen=True)
class PriorLookup:
    """Per-recording prior probabilities aligned to the scoreable class axis.

    ``probs`` is ``(n_rows, n_scoreable)`` float64 and FLOORED everywhere a
    context could not be read; ``n_floored`` counts the (row, species)
    cells that fell to FLOOR for bridge/axis reasons (unknown cell,
    unmapped code, off-axis key) so the sweep can report how much of its
    prior signal was real; ``missing_species`` names every scoreable code
    with no usable prior at all.
    """

    probs: np.ndarray
    n_floored: int
    missing_species: list[str]


def prior_for_rows(
    prior: PriorTensor,
    cells: Sequence[str],
    weeks: Sequence[int],
    scoreable: list[str],
    code_to_gbif: dict[str, int],
) -> PriorLookup:
    """The prior probability matrix for a cache's rows, scoreable-aligned.

    Column *j* holds ``scoreable[j]``'s prior per row. The bridge is
    PARTIAL by contract: a code missing from *code_to_gbif*, or whose GBIF
    key is not on ``prior.species_keys``, yields a FLOOR column and an
    entry in ``missing_species``. A row whose cell is not on the prior's
    cell axis yields a FLOOR row — ``PriorTensor.vector`` raises for an
    unknown cell because there it is a caller error, but HERE an unknown
    cell is expected (a recording outside the prior's spatial support) and
    the no-evidence answer is exactly the floor. Every floored cell is
    COUNTED, never silently dropped.
    """
    if len(cells) != len(weeks):
        raise ValueError(
            f"{len(cells)} cells but {len(weeks)} weeks; the two are "
            "per-row parallel arrays and a length mismatch would silently "
            "misalign every row after the shorter one"
        )
    species_index = {int(k): i for i, k in enumerate(prior.species_keys)}
    known_cells = set(prior.cells)

    # Per-scoreable species: its row on the prior's species axis, or None.
    rows_on_prior: list[int | None] = []
    missing_species: list[str] = []
    for code in scoreable:
        key = code_to_gbif.get(code)
        row = species_index.get(int(key)) if key is not None else None
        rows_on_prior.append(row)
        if row is None:
            missing_species.append(code)

    n_rows = len(cells)
    n_scoreable = len(scoreable)
    probs = np.full((n_rows, n_scoreable), FLOOR, dtype=np.float64)
    n_floored = 0
    for i, (cell, week) in enumerate(zip(cells, weeks)):
        if cell not in known_cells:
            n_floored += n_scoreable
            continue
        vector = prior.vector(cell, week)
        for j, row in enumerate(rows_on_prior):
            if row is None:
                n_floored += 1
            else:
                probs[i, j] = vector[row]
    return PriorLookup(
        probs=probs, n_floored=n_floored, missing_species=missing_species
    )


def rescore(
    scores: np.ndarray,
    prior_probs: np.ndarray,
    tau: float,
    train_marginal: np.ndarray,
) -> np.ndarray:
    """``log s + tau * (log p - log m)`` — the Bayesian rescoring arm.

    ``tau == 0.0`` SHORT-CIRCUITS and returns *scores* itself: the sweep's
    tau=0 column must be bit-identical to the raw encoder scores (dtype
    included), not a log/exp round-trip of them, or the baseline the gains
    are measured against would differ from the baseline that was run.

    Both *p* and *m* are floored with the imported ``FLOOR`` BEFORE the
    log, so a hard 0.0 — which the artifact itself can never store but a
    control or a hand-built matrix might hand over — can never produce a
    -inf that vetoes a correct detection. Output is float64.
    """
    if tau == 0.0:
        return scores
    s = np.asarray(scores, dtype=np.float64)
    p = np.maximum(np.asarray(prior_probs, dtype=np.float64), FLOOR)
    m = np.maximum(np.asarray(train_marginal, dtype=np.float64), FLOOR)
    return np.log(s) + tau * (np.log(p) - np.log(m))


def _global_marginal(prior: PriorTensor) -> np.ndarray:
    """Per-species mean of the FULL (floored) prior over every cell-week
    column — "how common is this species, everywhere and always"."""
    return prior.dense().mean(axis=1)


def control_prior(
    kind: Literal["uniform", "marginal", "space_shuffled", "time_shuffled"],
    prior: PriorTensor,
    rng: np.random.Generator,
) -> PriorTensor:
    """One of the four controls, as a PriorTensor passing its own validation.

    * ``uniform`` — every (cell, week) vector is the constant
      ``1/n_species``: no information at all. A gain here is an artifact.
    * ``marginal`` — every vector is the species' GLOBAL marginal (mean of
      the full prior over all cell-week columns): species commonness
      without any spatiotemporal structure.
    * ``space_shuffled`` — the cells axis is permuted: the same cell
      vectors exist, in the wrong places. Destroys "where".
    * ``time_shuffled`` — weeks permuted WITHIN each cell: circular
      seasonal structure destroyed, cell identity kept. Destroys "when".

    Shuffles draw from *rng* and are deterministic given it. The returned
    tensor's ``spec.variant`` names the control; the values matrix is
    rebuilt dense-then-sparse so ``PriorTensor.__post_init__``'s axis
    validation holds. Densifying is the inherent cost of the controls (see
    ``PriorTensor.dense``), and control tensors carry EXPLICIT floor
    entries where the original stored nothing — the floor is a real value
    here, not an absence, so it must survive the shuffle as data.
    """
    if kind not in _CONTROL_VARIANTS:
        raise ValueError(
            f"unknown control kind {kind!r}; expected one of "
            f"{sorted(_CONTROL_VARIANTS)}"
        )
    n_species = len(prior.species_keys)
    n_cells = len(prior.cells)
    dense = prior.dense()

    if kind == "uniform":
        new = np.full_like(dense, 1.0 / n_species)
    elif kind == "marginal":
        new = np.broadcast_to(
            _global_marginal(prior)[:, None], dense.shape
        ).copy()
    elif kind == "space_shuffled":
        perm = rng.permutation(n_cells)
        blocks = dense.reshape(n_species, n_cells, WEEKS_PER_YEAR)
        new = blocks[:, perm, :].reshape(n_species, -1)
    else:  # time_shuffled
        blocks = dense.reshape(n_species, n_cells, WEEKS_PER_YEAR).copy()
        for ci in range(n_cells):
            week_perm = rng.permutation(WEEKS_PER_YEAR)
            blocks[:, ci, :] = blocks[:, ci, week_perm]
        new = blocks.reshape(n_species, -1)

    return PriorTensor(
        spec=replace(prior.spec, variant=_CONTROL_VARIANTS[kind]),
        species_keys=np.asarray(prior.species_keys, dtype=np.int64).copy(),
        cells=list(prior.cells),
        values=sparse.csr_matrix(new),
        gbif_doi=prior.gbif_doi,
        backoff_params=prior.backoff_params,
        smoothing_params=prior.smoothing_params,
    )


def train_marginal_from_prior(
    prior: PriorTensor, scoreable: list[str], code_to_gbif: dict[str, int]
) -> np.ndarray:
    """The default training marginal: the prior's own global marginal per
    scoreable species, FLOOR where the bridge cannot map the species —
    the same partial-bridge contract as :func:`prior_for_rows`."""
    species_index = {int(k): i for i, k in enumerate(prior.species_keys)}
    global_marginal = _global_marginal(prior)
    out = np.full(len(scoreable), FLOOR, dtype=np.float64)
    for j, code in enumerate(scoreable):
        key = code_to_gbif.get(code)
        row = species_index.get(int(key)) if key is not None else None
        if row is not None:
            out[j] = global_marginal[row]
    return np.maximum(out, FLOOR)
