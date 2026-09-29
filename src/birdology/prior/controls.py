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

THE FULL PRIOR IS ``dense()``, NOT ``values`` -- T9-R2 RE-DERIVED
------------------------------------------------------------------
``tensor.py`` reads a context that is on both axes but carries no stored value
as ``backoff.FLOOR``, not as a hard zero: most (cell, week, species)
combinations are empty and empty almost always means "nobody looked" rather
than "absent". So there is no longer any such thing as an unobserved context.
Every context the axes span HAS a value, and ``PriorTensor.dense()`` is the
full prior itself rather than a dense-looking lie about one.

Two consequences, both load-bearing:

1. The quantity a control must preserve is ``t.dense().sum()``, NOT
   ``t.values.sum()``. The stored sum omits the floor mass of every context
   nobody surveyed, and on a realistically sparse prior -- a national grid is
   single-digit-percent full -- that is most of the grid. A control built to
   match the stored sum would sit BELOW the full prior the sweep compares it
   against, by exactly the floor mass.
2. The denominator is the DENSE CAPACITY, ``n_species * n_cells * 52``. Under
   the old structural-zero premise that denominator was a compromise; it is
   now simply the count of contexts that really do carry a value. Dividing by
   ``nnz`` instead would inflate every control by ``dense_capacity / nnz`` --
   a factor of ~52 at 2% fill -- which is the T9-R2 defect at a magnitude the
   results table would report as an enormous spatiotemporal effect.

Per control, writing ``D = t.dense()`` for the full prior as it is actually
read:

* ``uniform``       fills with ``D.mean()``. ``mean * size == sum`` exactly,
  so TOTAL mass is preserved. Per-species mass is destroyed on purpose:
  per-species mass IS label frequency, and this control has no information.
* ``marginal``      fills row *i* with ``D[i].mean()``. ``mean * n_contexts``
  is exactly that species' own mass, so PER-SPECIES mass is preserved and
  total mass follows. The per-species property is the stronger one and it is
  what makes this the label-frequency null: a reduction preserving the total
  while moving mass BETWEEN species would have altered the very signal under
  test.
* ``shuffle_space`` permutes whole cell blocks with one permutation shared
  across species, so the output is a PERMUTATION of ``D``. Every row's value
  multiset survives, hence every row's mass, hence the total.
* ``shuffle_time``  permutes week slots inside each (species, cell) block, so
  mass is preserved per species AND per cell -- a strictly stronger invariant
  than ``shuffle_space``'s, because a week permutation never crosses a cell
  boundary.

THE FLOOR MASS PARTICIPATES, AND THAT IS THE HONEST NULL
---------------------------------------------------------
On a 2%-full prior ``D`` is mostly ``FLOOR``, so it is worth being explicit
about what ``D.mean()`` is and is not.

It is NOT dominated by the floor. ``FLOOR`` is 1e-06 and observed values are
of order 1e-01, so at 2% fill the floor contributes ~4e-04 of total mass and
the uniform constant lands ~2000x above the floor. The control is data-derived,
not a flat 1e-06.

What including the floor DOES do is spread the full prior's whole mass over
every context, which puts the uniform constant at roughly ``nnz/size`` of the
observed mean. That is not dilution -- it is the correct scale, because the
FULL PRIOR also answers ~98% of its contexts with the floor.

Averaging over the stored entries only would build a control at ~52x the mass
of the prior it is the null for, and the sweep would report that scale gap as
a spatiotemporal effect. That is the defect, not the fix.

ARITHMETIC MASS IS MATCHED; LOG-SPACE LOCATION IS NOT, AND CANNOT BE
---------------------------------------------------------------------
An earlier revision of this docstring claimed control and prior were "one
object measured two ways". That is FALSE, and measurably so. On the sparse
fixture in ``tests/prior/test_controls.py`` (10 stored contexts of 520,
1.92% fill):

    mean log, full prior : -13.595   (geometric mean 1.25e-06)
    mean log, ``uniform``:  -5.652   (geometric mean 3.51e-03)
    offset               :   7.943 nats per context

and the sign of that offset is STRUCTURAL rather than incidental: among all
fills with the same arithmetic total, the CONSTANT one maximises the
geometric mean (AM-GM, with equality only when the prior is already
constant). So ``uniform`` is the mass-preserving control sitting furthest
ABOVE the full prior in log space, and no mass-preserving control can match
both the arithmetic total and the log-space location of a floor-dominated
grid. The two are different functionals of the same mass and a 98%-floor grid
separates them by construction.

Arithmetic mass is still the right thing to match, because that is what the
sweep reads: fusion is multiplicative, and a control at a different
arithmetic scale turns the headline comparison into a scale difference
(T9-R2). The trade-off is stated here rather than resolved, because it cannot
be resolved.

The consequence to carry downstream: a fusion mode that consumes ``log
prior`` ADDITIVELY sees a near-constant offset (~7.9 nats on this fixture,
larger the sparser the grid) between every control and the full prior. Any
decision threshold, calibration fit or tuned mixing weight learnt against one
must be refit against the other; a threshold transplanted from the full prior
to a control -- or a mixing weight tuned on the full prior and reused -- is
reading that offset as evidence. The arithmetic comparison the sweep reports
is unaffected.

So the paper's null hypothesis is: *the released prior's total mass,
redistributed with no context information at all* (``uniform``), or *with
label frequency but no context* (``marginal``). It is a statement about the
artifact AS IT IS READ, floor included -- not about the observed subset of it.
That is the honest null, because the sweep queries the full prior at every
context too, including the ones nobody surveyed.

WHY THE DENSE-GRID REFUSAL IS GONE
-----------------------------------
An earlier revision of this module REFUSED any tensor with ``nnz < size``, on
the then-current premise that an absent context was a structural zero and that
averaging over it silently diluted every control by ``nnz/size``. That premise
has been reversed in ``tensor.py``: an absent context reads ``FLOOR``, there is
nothing structural left to divide by, and the refusal made all four controls
raise on every realistic prior -- the paper's credibility section failing
before it computed anything.

The stored-zero clause went with it. ``tensor.py``'s ``_floor_missing`` maps an
explicit 0.0 to ``FLOOR`` exactly as it maps an unstored context, so no reader
-- including this one -- can tell the two apart. Refusing here would make the
controls stricter than the serialisation boundary they read through, on a
distinction that boundary has deliberately erased. ``tensor.py`` owns that
call and ``_validate_p`` makes it at write time.

The column-count guard survives untouched: it is about the RESHAPE, not about
sparsity, and nothing in the reversal touches it.

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


def _control_grid(t: PriorTensor, where: str) -> np.ndarray:
    """The full grid a control is built from: ``t.dense()``, guarded.

    The grid is taken from ``PriorTensor.dense()`` and is NEVER re-floored
    here. ``dense()`` is the single definition of "an unstored context reads
    the floor", shared with ``lookup`` and ``vector``, so the controls cannot
    drift from the readers the sweep uses. A local ``t.values.toarray()`` would
    be a SECOND derivation of an upstream value -- the defect this plan has
    found most often -- and it would read every unsurveyed context as 0.0 while
    the full prior reads it as ``FLOOR``, putting the control below the full
    prior's scale by exactly the floor mass. Silently: every value finite,
    in range and plausible.

    One guard, checked BEFORE the grid is materialised so that its message
    arrives instead of an opaque numpy one: the column count must agree with
    the cell axis (T9-R4). A reshape into ``(species, cell, week)`` blocks on a
    mismatch either dies on a numpy message that names no contract or -- worse,
    if the sizes happen to divide -- SUCCEEDS with a wrong block structure, and
    then ``shuffle_space`` permutes something that is not a cell.
    ``PriorTensor`` checks this at construction, but it is a MUTABLE dataclass:
    reassigning ``cells`` or ``values`` afterwards re-opens the hole, and this
    module is the one that reshapes.

    ``uniform`` and ``marginal`` do not reshape, and they go through the same
    guard anyway, on purpose: without it the mismatch surfaces two steps later
    out of ``_respec`` as ``PriorTensor``'s own dataclass message, which names
    a different contract and points the caller at the wrong layer.

    A CONTROL OF A CONTROL IS REFUSED
    ---------------------------------
    The consumed contract is a ``PriorTensor`` whose ``spec.variant`` is
    ``"full"``. Every control is defined as a reduction OF THE FULL PRIOR, and
    each is a fixed point or a near-fixed point of the others: ``uniform`` of
    ``uniform`` is ``uniform``, ``marginal`` of ``uniform`` is ``uniform``, and
    a shuffle of ``uniform`` is ``uniform`` again. Every one of them preserves
    mass, stays finite, stays in range, has the right shape and the right axes
    -- and ``_respec`` then RELABELS the output with the outer variant, so a
    second-order ``shuffle_space``-of-``uniform`` lands in the results table
    under a label that claims it is a first-order spatial null. Nothing
    downstream can detect that: the only evidence is the variant string this
    module just overwrote.

    A sweep loop that accumulated variants, or a rerun over a directory of
    saved priors that globbed the controls it wrote on the previous pass,
    produces exactly this. So it raises, naming the variant it received.

    The dense-grid and stored-zero clauses this guard used to carry are gone;
    the module docstring says why, and the mutation table keeps a regression
    mutant for each so neither can return unnoticed.
    """
    if t.spec.variant != "full":
        raise ValueError(
            f"the {where} control requires the FULL prior, but this tensor is "
            f"already the {t.spec.variant!r} control: a control of a control "
            "is meaningless -- every control is a reduction of the full prior, "
            "the reductions are fixed points of one another (a shuffle of "
            "uniform is uniform again), and the output would be RELABELLED "
            f"{where!r} and read as a first-order null"
        )
    n_cols = t.values.get_shape()[1]
    expected_cols = len(t.cells) * WEEKS_PER_YEAR
    if n_cols != expected_cols:
        raise ValueError(
            f"the {where} control cannot reshape this tensor: its column count "
            f"{n_cols} disagrees with the cell axis -- {len(t.cells)} cells x "
            f"{WEEKS_PER_YEAR} weeks needs {expected_cols}. Reshaping into "
            "(species, cell, week) blocks would either raise an opaque numpy "
            "error naming no contract or succeed with a WRONG block structure"
        )
    return t.dense()


def _respec(t: PriorTensor, variant: str, values: np.ndarray) -> PriorTensor:
    """The control tensor: same axes, same provenance, new variant.

    ``cells`` is passed through as-is. It is an opaque, case-sensitive,
    lower-case canonical H3 string that was derived exactly once upstream; a
    control that re-derived or case-folded it would answer every lookup by a
    real cell with a miss, and the sweep would read that as the control
    scoring badly.

    ``gbif_doi`` is carried too: a released control artifact has to be
    traceable to the same download as the full prior it is the control for.

    EVERY CONTROL IS DENSE, SO ITS ``fill`` IS 1.0 BY CONSTRUCTION
    --------------------------------------------------------------
    All four controls are built from ``t.dense()``, in which every context on
    the axes carries a value (most of them ``FLOOR``), and none of them can
    produce a zero from a positive grid -- a constant, a per-species constant
    and two permutations all map positive to positive. So the re-sparsified
    matrix stores ``dense_capacity`` entries, and each control's sidecar
    publishes ``nnz == dense_capacity`` -- a fill of 1.000 -- where the full
    prior it controls publishes, on the test fixture, 10 of 520 (0.019).

    That number is NOT comparable with the full prior's and must not be read
    as the control being better covered: it is a property of the construction,
    and the two ``fill`` values answer different questions (the prior's is
    "what fraction was observed", the control's is "what fraction is stored",
    which for a control is always all of it).

    The storage consequence, beside the peak-memory one the mutation harness
    records: a control is not a sparse artifact. At national figures each
    control is ~750 MB of float64 values plus ~375 MB of int32 CSR indices ON
    DISK, times four, to encode a matrix with a handful of distinct values --
    ONE for ``uniform``, at most ``n_species`` for ``marginal``, and the full
    prior's own multiset for the two shuffles. The released bundle is therefore
    dominated by the controls rather than by the prior, and a consumer sizing
    a download from the full prior's ``fill`` will be wrong by ~52x per control
    at 2% fill. Compression recovers much of it for ``uniform`` and
    ``marginal`` and almost none of it for the shuffles.
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

    The constant is the mean over the whole FLOORED grid, so total mass is
    preserved exactly (``mean * size == sum``). It is deliberately NOT
    ``1 / n_species``: that would renormalise each context to 1.0 and, on a
    prior whose contexts sum to whatever the pipeline produced, put this
    control on a different scale from the full prior.

    It is also deliberately not the mean over the STORED entries. On a
    realistically sparse prior that would sit at ~``size/nnz`` times the full
    prior's scale -- see the module docstring; the floor mass participates
    because the full prior answers those contexts with the floor too.

    Flat across species AND across contexts. Either kind of variation would be
    information, and this control is the one that has none.
    """
    dense = _control_grid(t, "uniform")
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
    that species' original mass on the floored grid, so this control preserves
    per-species mass and therefore total mass too. A sum or a median would not,
    and neither would a mean over that species' stored entries only.

    ITS CONTRAST DECAYS WITH FILL, AND THAT IS A PROPERTY OF THE CONSTRUCTION
    -------------------------------------------------------------------------
    The signal this control exists to carry is the RATIO between species. The
    floor is a common ADDITIVE baseline in every row, so with fill ``f``,
    observed value scale ``v`` and a true observed-mass ratio ``r``, the ratio
    this control actually emits is

        (f*v + (1-f)*FLOOR) / (f*v/r + (1-f)*FLOOR)  ->  1 as f*v -> FLOOR

    Measured (``FLOOR`` = 1e-06, r = 4):

        f=1.92e-02, v=1e-01 -> 3.994   <-- the sparse test fixture
        f=1.00e-03, v=1e-01 -> 3.885
        f=1.00e-04, v=1e-01 -> 3.143
        f=1.00e-04, v=1e-02 -> 1.600
        f=1.00e-05, v=1e-03 -> 1.007   <-- signal gone

    This is NOT a shared limitation of prior and control. It is asymmetric,
    and that asymmetry is why it matters: the FULL prior's label signal is
    concentrated in the ``f`` share of contexts it actually observed and is
    undiluted there, while ``marginal`` smears each species' mass across 100%
    of contexts, where the floor is most of the baseline. The floor dilutes the
    control's contrast without diluting the prior's. At sufficient sparsity
    ``marginal`` converges toward ``uniform``, stops carrying label frequency
    at all, and a gain then "survives the marginal control" trivially -- which
    is precisely the reviewer question the paper claims to answer.

    ``uniform`` is far more robust by comparison: at f=1e-03, v=1e-01 it still
    sits 101x above ``FLOOR``, and only collapses toward the floor around
    f<=1e-04 with v<=1e-02.

    ``test_marginal_reproduces_a_known_species_mass_ratio_on_a_sparse_prior``
    pins the ratio on the fixture; the build script must report the observed
    contrast on the real artifact, because these numbers are a function of the
    artifact's own ``f`` and ``v`` and no fixture can stand in for them.
    """
    dense = _control_grid(t, "marginal")
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

    It permutes the FLOORED grid, so an observed value can land where nobody
    ever surveyed and the floor can land on an observed context. That is
    correct and is the whole construction: "nobody looked here" is one of the
    prior's values, and a control that held it in place would be leaking the
    survey-effort map -- which is spatial information.
    """
    dense = _control_grid(t, "shuffle_space")
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

    It permutes the FLOORED grid, for the temporal mirror of ``shuffle_space``'s
    reason: when a survey happened is temporal information, so the floor has to
    move with everything else.
    """
    dense = _control_grid(t, "shuffle_time")
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
