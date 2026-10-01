"""Context provider — the prior as a conditioning vector on the MODEL's axis.

THE AXIS BRIDGE, AGAIN, AND STILL BY CODE
-----------------------------------------
The prior speaks GBIF ``species_key`` (int) in ``species_keys`` order; the
checkpoint speaks eBird code (str) in ``id2label`` order (measured: the
values ARE codes, e.g. ``ostric2``). ``build_context_matrix`` resolves the
join ONCE, at construction: for each class code it records the prior row
that code maps to through the ``code_to_gbif`` bridge, or ``None`` when the
bridge cannot reach the axis. Per-recording lookups then just gather. The
join is BY CODE — reordering ``class_codes`` reorders the output columns
with it, never against it.

THE BRIDGE IS PARTIAL, LIKE PHASE B'S (same ruling as
``phase_b.rescoring.prior_for_rows``): a code absent from the bridge, or
mapped to a key not on the prior's species axis, gets the FLOOR prior —
the artifact's own "no evidence" value, IMPORTED from
``birdology.prior.tensor``, never re-defined — and is NAMED in
``missing_codes``. Never a KeyError mid-epoch, never a silent drop.

ABSENT CONTEXT IS NOT "RARE EVERYWHERE" (Review Focus 2)
--------------------------------------------------------
Two different situations produce superficially similar vectors and MUST
stay distinguishable:

* A recording with NO usable (cell, week) — ``None`` either way, or a cell
  outside the prior's spatial support — has no context at all. Answer:
  zero vector, present-flag 0.0. Downstream, FiLM emits identity and the
  token arm emits its learned null token; the model is told "you know
  nothing", not handed a fabricated prior.
* A cell ON the axis with nothing stored is a REAL context whose value the
  backoff defines: all-FLOOR, meaning "rare everywhere, nobody looked".
  Answer: the layer-normed log-floor vector (which is all zeros, since a
  constant vector norms to zero) with present-flag 1.0. The FLAG is what
  keeps the two cases apart when the vectors coincide — conflating them
  would silently erase the sparse-region signal FiLM is meant to learn.

WEEK 53 FOLDS TO 52 BEFORE THE LOOKUP — the phase_b ruling, verbatim: 71
of every 400 years have an ISO week 53, ``PriorTensor.vector`` raises for
it, and a late-December recording must not sink a training batch. Any
OTHER out-of-range week stays loud: the tensor's own ValueError surfaces,
because week 54 is a caller bug, not a calendar fact.

NORMALISATION: ``(log p - mean) / (std + 1e-6)`` over the class axis —
the spec's "log(floored), layer-norm". The prior is a per-species
marginal that NEVER sums to one (see ``prior.tensor``); layer-norming the
log makes the conditioning input scale-free without pretending it is a
distribution.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import torch

from birdology.prior.tensor import FLOOR, PriorTensor

#: Layer-norm epsilon — the brief's value, spelled once.
_EPS = 1e-6


def build_context_matrix(
    prior: PriorTensor,
    class_codes: list[str],
    code_to_gbif: dict[str, int],
) -> "ContextProvider":
    """Resolve the code→prior-row join once; return the per-recording provider.

    *class_codes* is the model's OWN class axis in index order (the values
    of ``model.config.id2label`` sorted by key). A duplicate code is
    refused: in a by-code join a duplicate would hand two class columns
    the same prior while claiming the axis is a bijection.
    """
    seen: set[str] = set()
    for code in class_codes:
        if code in seen:
            raise ValueError(
                f"duplicate class code {code!r} in class_codes; the model's "
                "class axis maps one code to one column, so a duplicate "
                "means the id2label mapping handed over is not the model's"
            )
        seen.add(code)

    species_row = {int(k): i for i, k in enumerate(prior.species_keys)}
    rows_on_prior: list[int | None] = []
    missing_codes: list[str] = []
    for code in class_codes:
        key = code_to_gbif.get(code)
        row = species_row.get(int(key)) if key is not None else None
        rows_on_prior.append(row)
        if row is None:
            missing_codes.append(code)

    return ContextProvider(
        prior=prior,
        class_codes=list(class_codes),
        rows_on_prior=rows_on_prior,
        missing_codes=missing_codes,
    )


@dataclass
class ContextProvider:
    """Per-recording (cell, week) → conditioning vector on the model axis.

    Built by :func:`build_context_matrix`; ``missing_codes`` names every
    class code the bridge could not map to the prior's species axis (those
    columns read FLOOR — counted and named, the phase_b discipline).
    """

    prior: PriorTensor
    class_codes: list[str]
    rows_on_prior: list[int | None]
    missing_codes: list[str]
    _known_cells: set[str] = field(default_factory=set, repr=False)

    def __post_init__(self) -> None:
        self._known_cells = set(self.prior.cells)

    @property
    def n_classes(self) -> int:
        return len(self.class_codes)

    def vector(
        self, cell: str | None, week: int | None
    ) -> tuple[np.ndarray, float]:
        """(layer-normed log-prior on the model class axis, present_flag).

        Absent context — ``None`` cell or week, or a cell off the prior's
        axis — returns ``(zeros, 0.0)``. A cell ON the axis always returns
        flag 1.0, even when every value is the floor: that IS the prior's
        answer there (Review Focus 2).
        """
        if cell is None or week is None or cell not in self._known_cells:
            return np.zeros(self.n_classes, dtype=np.float32), 0.0

        # The phase_b ruling: ISO week 53 folds to 52 BEFORE the lookup.
        # Any other out-of-range week stays loud via the tensor's guard.
        if week == 53:
            week = 52

        on_prior = self.prior.vector(cell, week)  # species_keys order, floored
        probs = np.full(self.n_classes, FLOOR, dtype=np.float64)
        for j, row in enumerate(self.rows_on_prior):
            if row is not None:
                probs[j] = on_prior[row]

        logp = np.log(probs)
        normed = (logp - logp.mean()) / (logp.std() + _EPS)
        return normed.astype(np.float32), 1.0


class ContextEncoder(torch.nn.Module):
    """``Linear(n_classes+1, 512) → GELU → Linear(512, d_ctx)``.

    Input is the provider's vector CONCATENATED with the present-flag as a
    trailing feature — the flag rides inside the input so the MLP can
    learn a null representation for absent context rather than receiving
    an ambiguous all-zeros vector.
    """

    def __init__(self, n_classes: int, d_ctx: int = 256) -> None:
        super().__init__()
        self.mlp = torch.nn.Sequential(
            torch.nn.Linear(n_classes + 1, 512),
            torch.nn.GELU(),
            torch.nn.Linear(512, d_ctx),
        )

    def forward(self, vector: torch.Tensor, flag: torch.Tensor) -> torch.Tensor:
        """*vector* is ``(B, n_classes)``, *flag* is ``(B, 1)``."""
        return self.mlp(torch.cat([vector, flag], dim=-1))
