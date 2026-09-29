"""Control priors. A reported gain must survive all four or it is not real.

This module is where the paper's headline claim is either earned or lost. Each
control answers one alternative explanation for a measured gain:

- ``uniform``       : no information at all. If the gain survives nothing else,
                      it survives this.
- ``marginal``      : species frequency only, no space, no time. THE check --
                      it answers "is the gain just label-frequency
                      exploitation?", which is the first thing a reviewer will
                      demand.
- ``shuffle_space`` : real values, wrong places.
- ``shuffle_time``  : real values, wrong weeks.

SCALE IS PART OF THE NULL HYPOTHESIS (T9-R2)
--------------------------------------------
Every fusion method the sweep evaluates combines the prior with a posterior
either multiplicatively or through ``log prior``. A control that sits on a
DIFFERENT SCALE from the full prior it is compared against is therefore not
the null hypothesis it claims to be: the comparison measures a scale
difference and reports it as a context effect. Nothing downstream can detect
that -- every value is finite, in range and entirely plausible.

So ``uniform_prior`` does NOT fill with ``1 / n_species`` and
``marginal_prior`` does NOT renormalise over species. Both are built to
preserve the full prior's TOTAL MASS exactly, and ``marginal_prior`` preserves
each species' mass as well. The prior is never renormalised over species at
any stage (``backoff.py``, ``smoothing.py``, ``tensor.py`` all say so), and a
control that quietly normalises would be the one place in the pipeline that
does.

WHY THE INPUT MUST BE A DENSE GRID (an addition to T9-R2; see the report)
------------------------------------------------------------------------
``uniform_prior`` fills with the mean over the WHOLE matrix, so the
denominator is ``n_species * n_cells * WEEKS_PER_YEAR`` -- it counts contexts
this tensor holds no estimate for. Per ``tensor.py``, such a context is a
STRUCTURAL ZERO: a grid point the tensor was never handed, not a probability
of zero. Averaging over it is only meaningful when there are none.

That denominator is nevertheless the right one, and it is the ONLY constant
that preserves total mass, because both controls must be DENSE. They have to
be: a control that kept the full prior's sparsity pattern would carry the
presence/absence structure of the occurrence data, which is a large amount of
spatiotemporal information -- exactly what these two controls exist to remove.
Given a dense output, the mass-preserving fill is uniquely ``sum / size``.

The fix is therefore not a different denominator but a GUARD. ``to_tensor``'s
caller is already obliged to supply a dense grid (see ``tensor.py``'s
STRUCTURAL ZEROS section and T10-R1), and ``hierarchical_estimate`` emits one.
If that obligation is ever broken, every control here would be diluted by
``nnz / size`` relative to the full prior, silently, with no test in this plan
able to see it. ``_require_dense_grid`` refuses instead, for ALL FOUR
controls, so a partially-valid control set cannot reach the sweep.

Errors
------
Every contract violation raises ``ValueError`` naming that specific check.
There is no try/except, no repair and no silent skip: a control that repaired
its input would be compared against a full prior that was not repaired.
"""
from __future__ import annotations

from collections.abc import Callable

import numpy as np
from scipy import sparse

from .indexing import WEEKS_PER_YEAR
from .tensor import PriorSpec, PriorTensor


def _require_dense_grid(t: PriorTensor, where: str) -> np.ndarray:
    """Reject anything that is not a dense (species x cell x week) grid.

    Returns the dense matrix, because every control needs it anyway and
    materialising it twice is the only cost of checking.

    Three SEPARATE statements, one per clause, so that a fixture tripping one
    never exercises the others:

    1. the column count must agree with the cell axis (T9-R4). A reshape into
       ``(species, cell, week)`` blocks on a mismatch either raises an opaque
       numpy message that names no contract, or -- worse, if the sizes happen
       to divide -- succeeds with a WRONG block structure, and then
       ``shuffle_space`` permutes something that is not a cell. ``PriorTensor``
       checks this at construction, but ``PriorTensor`` is a mutable dataclass:
       reassigning ``cells`` or ``values`` afterwards re-opens the hole, and
       this module is the one that reshapes.
    2. every context must be STORED. A missing one is a structural zero, and
       averaging over it dilutes the control -- see the module docstring.
    3. no stored value may be zero. ``_validate_p`` forbids a ``p`` at or
       below 0.0, so an explicitly stored zero means a stage was bypassed; it
       is invisible to clause 2, which counts stored entries rather than
       non-zero ones, and it would drag the mean down by exactly the amount
       clause 2 exists to prevent.
    """
    n_rows, n_cols = t.values.get_shape()
    expected_cols = len(t.cells) * WEEKS_PER_YEAR
    if n_cols != expected_cols:
        raise ValueError(
            f"the {where} control cannot reshape this tensor: its column count "
            f"{n_cols} disagrees with the cell axis -- {len(t.cells)} cells x "
            f"{WEEKS_PER_YEAR} weeks needs {expected_cols}. Reshaping into "
            "(species, cell, week) blocks would either raise an opaque numpy "
            "error naming no contract or succeed with a WRONG block structure"
        )

    size = int(n_rows) * int(n_cols)
    if t.values.nnz != size:
        raise ValueError(
            f"the {where} control requires a DENSE grid, but this tensor "
            f"stores {t.values.nnz} of {size} (species, cell, week) contexts. "
            "A context absent from the tensor is a STRUCTURAL ZERO -- a grid "
            "point it was never handed, not a probability of zero -- so "
            "averaging over it would dilute every control by nnz/size and put "
            "the controls on a different scale from the full prior they are "
            "compared against"
        )
    if t.values.count_nonzero() != t.values.nnz:
        raise ValueError(
            f"the {where} control was given a tensor storing an explicit zero; "
            "the backoff clamps every value into [1e-06, 1.0], so a stored "
            "0.0 means a stage was bypassed, and it would drag the control's "
            "mean below the full prior's scale while passing the dense-grid "
            "count"
        )
    return np.asarray(t.values.toarray(), dtype=np.float64)


def _respec(t: PriorTensor, variant: str, values: np.ndarray) -> PriorTensor:
    """The control tensor: same axes, same provenance, new variant.

    ``cells`` is passed through as-is. It is an opaque, case-sensitive,
    lower-case canonical H3 string that was derived exactly once upstream; a
    control that re-derived or case-folded it would answer every lookup by a
    real cell with a miss, and the sweep would read that as the control
    scoring badly.

    ``gbif_doi`` is carried too: a released control artifact has to be
    traceable to the same download as the full prior it is the control for.
    """
    return PriorTensor(
        spec=PriorSpec(t.spec.resolution, t.spec.source, variant),
        species_keys=t.species_keys,
        cells=t.cells,
        values=sparse.csr_matrix(values).tocsr(),
        gbif_doi=t.gbif_doi,
    )


def uniform_prior(t: PriorTensor) -> PriorTensor:
    """No information at all: one constant everywhere, at the full prior's mass.

    The constant is the mean over the whole matrix, so total mass is preserved
    exactly (``mean * size == sum``). It is deliberately NOT ``1 / n_species``:
    that would renormalise each context to 1.0 and, on a prior whose contexts
    sum to whatever the pipeline produced, put this control on a different
    scale from the full prior -- see the module docstring.

    Flat across species AND across contexts. Either kind of variation would be
    information, and this control is the one that has none.
    """
    dense = _require_dense_grid(t, "uniform")
    fill = float(dense.mean())
    return _respec(t, "uniform", np.full(dense.shape, fill, dtype=np.float64))


def marginal_prior(t: PriorTensor) -> PriorTensor:
    """Species frequency only: each species' mean over contexts, everywhere.

    THE control. It holds the label-frequency signal the full prior also
    carries, and drops space and time entirely, so a gain that does not survive
    it is label-frequency exploitation rather than a spatiotemporal effect.

    The per-species mean is used AS-IS. There is no ``/ total``: dividing by
    the sum over species would renormalise a prior that is never renormalised
    at any stage, rescale this control away from the full prior, and -- because
    the divisor depends on how many species are in the tensor -- make the
    control's scale a function of the label set.

    The mean over contexts is what preserves mass: mean * n_contexts is exactly
    that species' original mass, so this control preserves per-species mass and
    therefore total mass too. A sum or a median here would not.
    """
    dense = _require_dense_grid(t, "marginal")
    per_species = dense.mean(axis=1, keepdims=True)
    return _respec(t, "marginal", np.tile(per_species, (1, dense.shape[1])))


def shuffle_space(t: PriorTensor, seed: int = 0) -> PriorTensor:
    """Real values, wrong places: relocate whole cells, weekly profile intact.

    ONE permutation of the cell axis, shared across every species (T9-R3).
    Sharing it is the point: it destroys the association between a value and
    WHERE it was observed while leaving each cell's internal weekly profile,
    and the cross-species structure within a cell, untouched. A per-species
    permutation would also scramble which species co-occur, so a gain could
    then fail this control for a reason that has nothing to do with space.
    """
    dense = _require_dense_grid(t, "shuffle_space")
    n_cells = len(t.cells)
    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)
    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]
    return _respec(t, "shuffle_space", blocks.reshape(dense.shape))


def shuffle_time(t: PriorTensor, seed: int = 0) -> PriorTensor:
    """Real values, wrong weeks: relocate week slots, each cell's set intact.

    ONE permutation of the 52 week slots, shared across every cell and every
    species (T9-R3), for the mirror-image reason: it destroys the association
    between a value and WHEN it was observed while preserving each cell's
    multiset of weekly values and each species' mass within every cell.
    """
    dense = _require_dense_grid(t, "shuffle_time")
    n_cells = len(t.cells)
    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)
    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]
    return _respec(t, "shuffle_time", blocks.reshape(dense.shape))


CONTROLS: dict[str, Callable[..., PriorTensor]] = {
    "uniform": uniform_prior,
    "marginal": marginal_prior,
    "shuffle_space": shuffle_space,
    "shuffle_time": shuffle_time,
}
