"""Control priors. A reported gain must survive all five or it is not real.

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
- ``separable``     : BOTH marginals right, their INTERACTION destroyed. The
                      one that isolates the joint space x time term, and the
                      only control in the set that separates "this species is
                      common in this region" plus "this species is common in
                      this week" acting INDEPENDENTLY from the two acting
                      TOGETHER. See ``separable_prior``.

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
  multiset survives, hence every row's mass, hence the total. It REFUSES to
  emit a grid equal to ``D``: see ``_refuse_an_unmoved_shuffle``.
* ``shuffle_time``  permutes week slots inside each (species, cell) block, so
  mass is preserved per species AND per cell -- a strictly stronger invariant
  than ``shuffle_space``'s, because a week permutation never crosses a cell
  boundary. Same refusal, same reason.
* ``separable``     replaces each (species) block by the outer product of its
  own cell and week marginals, ``cellmean * weekmean / grandmean``. That
  identity is exactly mass-preserving per species (and so in total), and it
  reproduces BOTH marginals exactly; only the interaction between them is
  destroyed.

THE ARTIFACT THE PIPELINE ACTUALLY SHIPS IS FULLY MATERIALISED (fill 1.0)
-------------------------------------------------------------------------
Everything below about the floor mass is CORRECT AS SEMANTICS and was
MEASURED ON THE WRONG THING. ``scripts/build_prior.py`` passes an explicit
``cells=`` axis and weeks ``1..52``, and ``tensor._validate_p`` forbids
``p <= 0``, so every context the axes span is an explicit stored nonzero:
artifacts built by the CLI have ``nnz == dense_capacity``, fill exactly
1.0000, and NOT ONE entry equal to ``FLOOR``. Measured on pipeline builds
through ``build_from_archive``: 26 species x 115 res-4 cells x 52 weeks =
155 480 contexts, nnz 155 480, fill 1.0000, ``(dense == FLOOR).sum() == 0``,
for the full prior AND for every control.

So on a RELEASED artifact the floor mass is ZERO and every "at 2% fill"
magnitude in this docstring is a statement about
``tests/prior/test_controls.py``'s deliberately sparse 1.92%-fill fixture,
NOT about anything shipped. Every such figure below is now labelled
FIXTURE-ONLY and given the pipeline figure beside it. The DIRECTION of each
argument survives unchanged -- a hand-built sparse tensor, which this module
still accepts, lands in the fixture regime -- but a reader must not size the
effect from the fixture number.

THE FLOOR MASS PARTICIPATES, AND THAT IS THE HONEST NULL
---------------------------------------------------------
On a 2%-full prior ``D`` is mostly ``FLOOR`` (FIXTURE-ONLY regime; a released
artifact has no floored context at all), so it is worth being explicit about
what ``D.mean()`` is and is not.

It is NOT dominated by the floor. ``FLOOR`` is 1e-06 and observed values are
of order 1e-01, so at 1.92% fill the floor contributes ~2.7e-04 of total mass and
the uniform constant lands ~3600x above the floor. The control is data-derived,
not a flat 1e-06.

What including the floor DOES do is spread the full prior's whole mass over
every context, which puts the uniform constant at roughly ``nnz/size`` of the
observed mean. That is not dilution -- it is the correct scale, because the
FULL PRIOR also answers every floored context with the floor. On a released
artifact ``nnz/size`` is 1.0 and this correction is the identity, which is why
the decision is invisible there and load-bearing on a hand-built sparse
tensor.

Averaging over the stored entries only would build a control at ~52x the mass
of the prior it is the null for ON THE FIXTURE (and at exactly 1x on a
released artifact, where the two denominators coincide), and the sweep would
report that scale gap as a spatiotemporal effect. That is the defect, not the
fix.

ARITHMETIC MASS IS MATCHED; LOG-SPACE LOCATION IS NOT, AND CANNOT BE
---------------------------------------------------------------------
An earlier revision of this docstring claimed control and prior were "one
object measured two ways". That is FALSE, and measurably so.

FIXTURE-ONLY, on the sparse fixture in ``tests/prior/test_controls.py``
(10 stored contexts of 520, 1.92% fill):

    mean log, full prior : -13.593   (geometric mean 1.25e-06)
    mean log, ``uniform``:  -5.635   (geometric mean 3.57e-03)
    offset               :   7.958 nats per context

PIPELINE, on artifacts written by ``build_from_archive`` (fill 1.0000, no
floored context anywhere):

    26 species x 115 res-4 cells : mean log full -4.291, uniform -3.258,
                                   offset +1.033 nats
     6 species x  15 res-4 cells : mean log full -2.472, uniform -1.792,
                                   offset +0.681 nats

So the offset is REAL and its SIGN is structural, but on anything released it
is of order ONE nat rather than eight: the fixture's 7.958 is ~8x the
released magnitude and comes almost entirely from the floor mass a released
artifact does not have. The sign is structural rather than incidental: among
all fills with the same arithmetic total, the CONSTANT one maximises the
geometric mean (AM-GM, with equality only when the prior is already
constant). So ``uniform`` is the mass-preserving control sitting furthest
ABOVE the full prior in log space, and no mass-preserving control can match
both the arithmetic total and the log-space location of a non-constant grid.
The two are different functionals of the same mass and any non-constant grid
separates them by construction.

Arithmetic mass is still the right thing to match, because that is what the
sweep reads: fusion is multiplicative, and a control at a different
arithmetic scale turns the headline comparison into a scale difference
(T9-R2). The trade-off is stated here rather than resolved, because it cannot
be resolved.

The consequence to carry downstream: a fusion mode that consumes ``log
prior`` ADDITIVELY sees a near-constant offset between every control and the
full prior -- ~0.7 to 1.0 nats on a released artifact, ~7.9 on the sparse
fixture, larger the sparser the grid. Any decision threshold, calibration fit
or tuned mixing weight learnt against one must be refit against the other; a
threshold transplanted from the full prior to a control -- or a mixing weight
tuned on the full prior and reused -- is reading that offset as evidence. The
arithmetic comparison the sweep reports is unaffected.

So the paper's null hypothesis is: *the released prior's total mass,
redistributed with no context information at all* (``uniform``), or *with
label frequency but no context* (``marginal``), or *with both marginals but no
interaction* (``separable``). It is a statement about the artifact AS IT IS
READ, floor included -- not about the observed subset of it. That is the
honest null, because the sweep queries the full prior at every context too.

WHY THE DENSE-GRID REFUSAL IS GONE
-----------------------------------
An earlier revision of this module REFUSED any tensor with ``nnz < size``, on
the then-current premise that an absent context was a structural zero and that
averaging over it silently diluted every control by ``nnz/size``. That premise
has been reversed in ``tensor.py``: an absent context reads ``FLOOR``, there is
nothing structural left to divide by, and the refusal made every control
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
its input would be compared against a full prior that was not repaired. The
same rule covers a control that came out DEGENERATE rather than one whose
input was malformed -- a shuffle equal to the prior it is the null for raises
instead of shipping (``_refuse_an_unmoved_shuffle``), because the alternative
is an artifact nothing downstream can tell from the prior.
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
    ``uniform`` is ``uniform``, ``marginal`` of ``uniform`` is ``uniform``,
    a shuffle of ``uniform`` is ``uniform`` again, and ``separable`` of
    ``marginal`` is ``marginal`` (a per-species constant is already separable,
    so the outer product reproduces it exactly). Every one of them preserves
    mass, stays finite, stays in range, has the right shape and the right axes
    -- and ``_respec`` then RELABELS the output with the outer variant, so a
    second-order ``shuffle_space``-of-``uniform`` lands in the results table
    under a label that claims it is a first-order spatial null. Nothing
    downstream can detect that: the only evidence is the variant string this
    module just overwrote.

    ``separable`` makes this guard MORE load-bearing rather than less: it is
    the control most likely to be reached for twice, because "remove the
    interaction from a shuffled prior" reads like a sharper null than either
    alone, and ``separable`` of ``shuffle_time`` is a perfectly well-formed
    tensor that is not the null its label would claim.

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
    All five controls are built from ``t.dense()``, in which every context on
    the axes carries a value, and none of them can produce a zero from a
    positive grid -- a constant, a per-species constant, two permutations and
    a strictly-positive outer product all map positive to positive. So the
    re-sparsified matrix stores ``dense_capacity`` entries, and each control's
    sidecar publishes ``nnz == dense_capacity`` -- a fill of 1.000.

    THE FULL PRIOR BUILT BY THE PIPELINE PUBLISHES THE SAME 1.000. Measured on
    ``build_from_archive`` output: 26 species x 115 res-4 cells, nnz 155 480 of
    dense capacity 155 480, for the full prior and all five controls alike.
    ``build_prior.py`` passes the cell axis and weeks 1..52 explicitly, so
    every context is stored. The two ``fill`` values still answer DIFFERENT
    questions (the prior's is "what fraction was observed", the control's is
    "what fraction is stored") -- they just happen to agree at 1.000 on
    everything the CLI writes. They separate only on a HAND-BUILT sparse
    tensor, such as ``test_controls.py``'s 1.92%-fill fixture, where the full
    prior publishes 10 of 520 (0.019) and every control publishes 520 of 520.

    The storage consequence, beside the peak-memory one the mutation harness
    records: a control is not a sparse artifact, and neither is the released
    full prior. At national figures each control is ~750 MB of float64 values
    plus ~375 MB of int32 CSR indices ON DISK, times FIVE, to encode a matrix
    with a handful of distinct values -- ONE for ``uniform``, at most
    ``n_species`` for ``marginal``, ``n_species * (n_cells + 52)`` degrees of
    freedom for ``separable``, and the full prior's own multiset for the two
    shuffles. Compression recovers much of it for ``uniform`` and ``marginal``,
    some for ``separable`` (it is low-rank per species but not
    low-cardinality), and almost none for the shuffles.
    """
    return PriorTensor(
        spec=PriorSpec(t.spec.resolution, t.spec.source, variant),
        species_keys=t.species_keys,
        cells=t.cells,
        values=sparse.csr_matrix(values).tocsr(),
        gbif_doi=t.gbif_doi,
    )


def _refuse_an_unmoved_shuffle(
    shuffled: np.ndarray,
    dense: np.ndarray,
    where: str,
    axis: str,
    axis_len: int,
    seed: int,
) -> np.ndarray:
    """*shuffled*, unless it is the grid it was drawn from.

    A SHUFFLE THAT MOVED NOTHING IS THE PRIOR WEARING THE NULL'S LABEL
    ------------------------------------------------------------------
    ``np.random.default_rng(seed).permutation(n)`` is the IDENTITY whenever
    ``n <= 2`` at seed 0 -- measured, not feared:

        n=1  perm=[0]    identity  (the only permutation there is)
        n=2  perm=[0 1]  identity
        n=3  perm=[2 0 1]

    The final gate built a single-cell prior end to end through
    ``build_from_archive`` and got ``prior_dof_r4_shuffle_space.npz`` with
    ``max|control - full| = 0.000e+00`` and the SAME sha256 as the full prior:
    correctly packed, correctly provenanced, labelled ``"shuffle_space"`` in
    its own sidecar. A sweep reads that as "the spatial effect does not survive
    its control", from a control that IS the prior. Nothing downstream can
    detect it -- every value is finite, in range, on the right axes, and the
    masses match exactly, because the two grids are the same grid.

    This is the THIRD time a permutation draw has produced a control that
    cannot fail here. The first two (seed 1 was the identity on a 3-cell axis;
    seed 3 was its own inverse) were fixed by widening the TEST FIXTURE's axis,
    which left the shipped code with no guard at all -- so the defect moved out
    of the fixture and into the artifact. Hence a refusal in ``src/``: a lucky
    axis size is not a contract.

    WHY THE CHECK IS ON THE EMITTED GRID AND NOT ON THE PERMUTATION
    ---------------------------------------------------------------
    The obvious spelling is ``if np.array_equal(perm, np.arange(n)): raise``.
    It is strictly WEAKER, and the difference is a real artifact:

    * an identity permutation implies an unmoved grid, so this check fires
      everywhere that one would;
    * an unmoved grid does NOT imply an identity permutation. A prior whose
      cell blocks are all equal (or whose every week slot is equal) is
      relabelled by a perfectly non-degenerate permutation into itself. The
      permutation check passes and ships the same byte-identical file.

    So the property worth refusing is the one the sweep is actually misled by
    -- "this control equals the prior it is the null for" -- and it is the one
    the gate measured. Checking the permutation as WELL would add a clause that
    can never fire on its own, which is the dead-conjunct defect this round is
    also removing from ``smoothing.py``. One clause, and it is total.

    The message names both causes because the operator's fix differs: a
    degenerate axis wants a coarser build or a different seed, an axis with no
    structure wants a different control.

    WEAK-BUT-NOT-IDENTITY PERMUTATIONS ARE DELIBERATELY ACCEPTED
    ------------------------------------------------------------
    At seed 0 a 10-cell axis leaves 3 of 10 cells fixed, which looks like a
    30% failure and is not one. The number of fixed points of a UNIFORM random
    permutation is Poisson(1) for every n: mean 1, variance 1, and
    ``P(F >= 3) = 0.0803`` at n=10 and 0.0803 at n=27 -- identical, because the
    distribution does not depend on n. Measured over seeds 0..1999: mean 0.986
    at n=10, 1.008 at n=27, 1.000 at n=897.

    A fixed-point RATE threshold is therefore the arbitrary-constant trap this
    branch has hit before. Any threshold tight enough to reject the 3-of-10
    draw rejects ~8% of legitimate draws at EVERY axis length, and one that
    rejects any fixed point at all rejects ``1 - 1/e`` = 63% of all valid
    permutations -- it would be rejecting the null distribution itself. Worse,
    a control drawn from permutations CONDITIONED to have no fixed point is no
    longer a uniform random relabelling, so its null is a different null: a
    subtler version of the defect being fixed. So there is no rate check, and
    ``test_a_permutation_with_FIXED_POINTS_is_deliberately_accepted`` plus
    ``test_the_fixed_point_count_is_POISSON_ONE_so_a_RATE_THRESHOLD_pins_nothing``
    pin that absence as a number rather than leaving it to be re-litigated.

    WHY ``shuffle_time`` IS SAFER, STATED RATHER THAN OBSERVED
    ----------------------------------------------------------
    ``shuffle_time`` permutes ``WEEKS_PER_YEAR`` slots and ``shuffle_space``
    permutes ``len(t.cells)``. The asymmetry is not that 52 is bigger than 1:
    it is that 52 is a SCHEMA CONSTANT that no input can shrink, while the cell
    axis is a function of the archive -- a single-region case study, a smoke
    archive or a coarse resolution produces one or two cells. So the week axis
    cannot reach the degenerate regime at all and the cell axis reaches it on
    real inputs. Both shuffles route through this same guard anyway, because
    ``shuffle_time`` still has the OTHER cause available to it (a prior with a
    flat season in every cell is unmoved by any week permutation), and because
    a guard present on one shuffle and absent on its mirror is how the two
    drift apart.

    NOT APPLIED TO ``uniform``, ``marginal`` OR ``separable``
    ---------------------------------------------------------
    They are allowed to coincide with a degenerate prior and a guard here would
    misfire on them. ``uniform`` of an already-constant prior IS that prior,
    and that is the correct answer: "the prior's mass with no information at
    all" genuinely equals a prior that carries no information. ``marginal`` of
    a per-species-constant prior, and ``separable`` of a prior with no space x
    time interaction, are the same statement one structure further along. Each
    is a fixed point that MEANS something. A shuffle has no such reading: its
    entire definition is that values move, so a shuffle that moved nothing did
    not run the experiment.

    Cost: one ``dense_capacity`` boolean temporary, an eighth of the float64
    grid this function is already handed. ``np.array_equal`` is value equality,
    which coincides with byte equality here because ``tensor._validate_p``
    forbids ``p <= 0`` -- there is no ``nan`` to compare unequal to itself and
    no ``-0.0`` to compare equal to ``0.0``.
    """
    if np.array_equal(shuffled, dense):
        raise ValueError(
            f"the {where} control is byte-identical to the full prior it is "
            f"the null for: seed {seed} moved nothing on a {axis} axis of "
            f"length {axis_len}. Either the drawn permutation displaces no "
            f"{axis} -- unavoidable at length 1, where the identity is the "
            f"only permutation there is -- or every {axis} block of this "
            "prior carries the same values, so there is no structure along "
            "that axis for a shuffle to destroy. Refusing, because the "
            f"alternative is a released file labelled {where!r} whose bytes "
            "ARE the prior, which a sweep reads as 'the effect does not "
            "survive its control'"
        )
    return shuffled


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

    ITS CONTRAST DECAYS WITH FILL -- ON A SPARSE TENSOR, AND NOT ON A RELEASED
    ONE
    -------------------------------------------------------------------------
    The signal this control exists to carry is the RATIO between species.

    ON A RELEASED ARTIFACT THERE IS NO DECAY AT ALL, and this is the headline
    correction: a pipeline build has fill 1.0000 and no floored context, so
    there is no common additive baseline to dilute and the emitted ratio equals
    the full prior's per-species-mean ratio EXACTLY. Measured on
    ``build_from_archive`` output (6 species x 15 res-4 cells): full prior
    per-species mean ratio 7.161461, ``marginal`` emitted 7.161461, maximum
    absolute difference 0.000e+00 across every species. The 26-species build
    agrees: 16.962 against 16.962.

    The decay below is therefore a property of a HAND-BUILT SPARSE tensor --
    the regime ``test_controls.py``'s fixture and any third-party sparse
    artifact live in -- not of anything ``scripts/build_prior.py`` writes. The
    floor is a common ADDITIVE baseline in every row, so with fill ``f``,
    observed value scale ``v`` and a true observed-mass ratio ``r``, the ratio
    this control emits is

        (f*v + (1-f)*FLOOR) / (f*v/r + (1-f)*FLOOR)  ->  1 as f*v -> FLOOR

    Measured (``FLOOR`` = 1e-06, r = 4), ALL FIXTURE-ONLY:

        f=1.00e+00           -> 4.000   <-- every PIPELINE artifact: exact
        f=1.92e-02, v=1e-01 -> 3.994   <-- the sparse test fixture
        f=1.00e-03, v=1e-01 -> 3.885
        f=1.00e-04, v=1e-01 -> 3.143
        f=1.00e-04, v=1e-02 -> 1.600
        f=1.00e-05, v=1e-03 -> 1.007   <-- signal gone

    Where it does bite it is NOT a shared limitation of prior and control. It
    is asymmetric, and that asymmetry is why it matters: the FULL prior's label
    signal is concentrated in the ``f`` share of contexts it actually observed
    and is undiluted there, while ``marginal`` smears each species' mass across
    100% of contexts, where the floor is most of the baseline. The floor
    dilutes the control's contrast without diluting the prior's. At sufficient
    sparsity ``marginal`` converges toward ``uniform``, stops carrying label
    frequency at all, and a gain then "survives the marginal control"
    trivially -- which is precisely the reviewer question the paper claims to
    answer. A released artifact is immune to this because it is fully
    materialised; a consumer who re-sparsifies one to save space is not.

    ``uniform`` is far more robust by comparison: at f=1e-03, v=1e-01 it still
    sits 101x above ``FLOOR``, and only collapses toward the floor around
    f<=1e-04 with v<=1e-02.

    ``test_marginal_reproduces_a_known_species_mass_ratio_on_a_sparse_prior``
    pins the ratio on the fixture, and
    ``test_marginal_contrast_is_EXACT_on_a_pipeline_artifact`` in
    ``tests/prior/test_controls.py`` pins the no-decay result against an
    artifact built through ``build_from_archive``, so neither number can drift
    into standing for the other.
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

    IT REFUSES TO EMIT THE PRIOR ITSELF. See ``_refuse_an_unmoved_shuffle``:
    at ``len(t.cells) <= 2`` the drawn permutation is the identity at seed 0,
    and the gate shipped a ``shuffle_space`` artifact with the full prior's
    sha256 from a single-cell build. The guard is on the EMITTED GRID rather
    than on the permutation, because a prior whose cell blocks are all equal is
    unmoved by a perfectly good permutation too.
    """
    dense = _control_grid(t, "shuffle_space")
    n_cells = len(t.cells)
    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)
    blocks = blocks[:, np.random.default_rng(seed).permutation(n_cells), :]
    return _respec(
        t,
        "shuffle_space",
        _refuse_an_unmoved_shuffle(
            blocks.reshape(dense.shape),
            dense,
            "shuffle_space",
            "cell",
            n_cells,
            seed,
        ),
    )


def shuffle_time(t: PriorTensor, seed: int = 0) -> PriorTensor:
    """Real values, wrong weeks: relocate week slots, each cell's set intact.

    ONE permutation of the 52 week slots, shared across every cell and every
    species (T9-R3), for the mirror-image reason: it destroys the association
    between a value and WHEN it was observed while preserving each cell's
    multiset of weekly values and each species' mass within every cell.

    It permutes the FLOORED grid, for the temporal mirror of ``shuffle_space``'s
    reason: when a survey happened is temporal information, so the floor has to
    move with everything else.

    IT ROUTES THROUGH THE SAME REFUSAL, and not because the week axis is at
    risk of being degenerate. ``WEEKS_PER_YEAR`` is a schema constant no input
    can shrink, so the identity draw ``shuffle_space`` hits at one or two cells
    is unreachable here -- that is a property worth stating rather than an
    accident of 52 being large. What IS reachable is the other cause: a prior
    with a flat season in every cell is unmoved by any week permutation, and
    the emitted-grid guard catches that where a permutation check would not.
    """
    dense = _control_grid(t, "shuffle_time")
    n_cells = len(t.cells)
    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)
    blocks = blocks[:, :, np.random.default_rng(seed).permutation(WEEKS_PER_YEAR)]
    return _respec(
        t,
        "shuffle_time",
        _refuse_an_unmoved_shuffle(
            blocks.reshape(dense.shape),
            dense,
            "shuffle_time",
            "week",
            WEEKS_PER_YEAR,
            seed,
        ),
    )


def separable_prior(t: PriorTensor) -> PriorTensor:
    """Both marginals EXACTLY right, their INTERACTION destroyed. M1.

    The control a NeurIPS reviewer will demand and the only one in this set
    that isolates the joint space x time term. Each species' block is replaced
    by the outer product of its OWN cell and week marginals::

        out[s, c, w] = cellmean[s, c] * weekmean[s, w] / grandmean[s]

    WHAT IT PRESERVES, exactly and not approximately:

    * the per-cell annual marginal of every species (``block.mean(axis=week)``),
    * the per-week national marginal of every species
      (``block.mean(axis=cell)``),
    * therefore each species' own mass, and therefore the total mass.

    Measured on a ``build_from_archive`` artifact (6 species x 15 res-4 cells x
    52 weeks, fill 1.0000): cell-marginal max relative error 3.6e-16,
    week-marginal max relative error 6.0e-16, per-species mass max relative
    error 3.4e-16, total mass relative error 1.5e-16. On the 26 x 115 build the
    same four are 6.8e-16, 1.1e-15, 6.2e-16 and 0.0. All at float64 rounding.

    WHAT IT DESTROYS: the interaction, and only the interaction. The guarantee
    is EXACT IN LOG SPACE, which is where this construction is defined -- a
    product is additive in logs, so the log-space two-way ANOVA residual of the
    output is identically zero (measured 5.6e-16). The ARITHMETIC residual is
    what the sweep reads, and on real artifacts it falls sharply: 3.6766e-02 ->
    1.4248e-02 (factor 0.388) on a 6-species x 15-cell pipeline build, and
    9.7362e-03 -> 3.3908e-03 on a 26-species x 115-cell one.

    That arithmetic reduction is an EMPIRICAL property of real priors, not a
    theorem, and the difference matters. On a grid that is ADDITIVELY separable
    but not multiplicatively separable the arithmetic residual can GROW: on
    ``test_controls.py``'s ``_tensor()`` fixture, whose values are
    ``cell_term + week_term``, it goes from 4.9e-17 to 3.2e-02 while the
    log-space residual is correctly driven to zero. A real occurrence prior is
    multiplicative in character (abundance scales, it does not offset), so the
    arithmetic figure is the relevant one for anything released -- but a test
    that asserted "the arithmetic interaction always shrinks" would be asserting
    something false, and ``test_the_MAIN_fixture_has_NO_arithmetic_interaction_
    so_it_cannot_pin_this`` pins that trap as a number.

    WHY THIS CONTROL HAD TO EXIST. On that same artifact the interaction
    carries 20-57% of each species' total variation (mean 32%; the two
    flat-season residents are the most interaction-dominated at 52% and 57%,
    because their space and time marginals are nearly flat and nearly all their
    structure IS the joint term). And BOTH shuffles leave the interaction
    standard deviation at 3.6766e-02, byte-identical to the full prior's: they
    relabel one axis and carry the joint structure along with it, permuted.
    ``uniform`` destroys everything (0.0) and ``marginal`` destroys space AND
    time AND the interaction together (4.4e-17). So before this control there
    was no way to separate "this species is common in this region" plus "this
    species is common in this week" acting INDEPENDENTLY from the two acting
    TOGETHER, and a reviewer could fairly say the reported gain was never
    attributed to the joint term at all.

    WHY THE OUTER PRODUCT AND NOT A WITHIN-CELL WEEK PERMUTATION
    ------------------------------------------------------------
    The obvious alternative -- draw an INDEPENDENT week permutation inside each
    (species, cell) block -- was built and measured, and it FAILS the contract
    this control exists to satisfy. It preserves each cell's marginal (a
    permutation inside a block cannot change the block's own mean) but it
    DESTROYS the week marginal, because each cell's weekly values land under
    different week labels: measured week-marginal maximum relative error 1.36 on
    the 6 x 15 build and 2.57 on the 26 x 115 one, against 6.0e-16 here. It also
    INCREASES the interaction standard deviation -- to 4.665e-02 from the full
    prior's 3.6766e-02, and to 1.392e-02 from 9.7362e-03 -- because an
    independent permutation per cell manufactures joint structure rather than
    removing it. (Those two figures depend on the permutation drawn; the SIGN
    does not, and the week marginal is destroyed for every draw that is not the
    identity in every cell at once.) It is a fine control for something else;
    it is not this one.

    The ADDITIVE decomposition (``cellmean + weekmean - grandmean``) was also
    measured and is REFUSED as a construction: it emits NEGATIVE values -- 334
    of 4680 contexts on the 6 x 15 artifact (minimum -1.665e-02) and 17 741 of
    155 480 on the 26 x 115 one (minimum -8.993e-03) -- and a negative prior is
    not a probability, so ``save_prior`` could not write it. The multiplicative
    form is the one that stays inside the positive orthant for a strictly
    positive input, which the prior always is.

    NO SILENT REPAIR AT THE (0, 1] BOUNDARY
    ----------------------------------------
    The outer product can exceed 1.0 even when every input value is a
    probability: a block whose mass is concentrated in one cell AND one week
    (a "cross") has a large cell marginal and a large week marginal in the same
    place, and their product over a small grand mean can be arbitrarily large.
    Constructed and measured: a 15-cell block with one hot row and one hot
    column at 0.6 and 1e-06 elsewhere maps 0.6 to 7.09. Real Danish artifacts
    are nowhere near it (measured maxima 0.500 on the 6 x 15 build and 0.145 on
    the 26 x 115 one, both BELOW one), but "nowhere near on the builds we tried"
    is not a contract.

    So this raises, naming the check, rather than clamping. Clamping would
    silently break the very invariants this control is defined by -- a clamped
    grid no longer reproduces either marginal and no longer preserves mass --
    and the sweep would read a quietly-renormalised control as a weak
    spatiotemporal effect. ``ValueError`` is the honest answer; the operator
    then knows their prior is too concentrated for a separable surrogate to be
    a probability distribution.
    """
    dense = _control_grid(t, "separable")
    n_cells = len(t.cells)
    blocks = dense.reshape(dense.shape[0], n_cells, WEEKS_PER_YEAR)
    # Per SPECIES, never pooled: pooling would mix label frequency into the
    # marginals and the control would stop preserving each species' own mass.
    cell_mean = blocks.mean(axis=2, keepdims=True)
    week_mean = blocks.mean(axis=1, keepdims=True)
    grand_mean = blocks.mean(axis=(1, 2), keepdims=True)
    surrogate = cell_mean * week_mean / grand_mean
    if surrogate.max() > 1.0:
        raise ValueError(
            "the separable control cannot represent this prior as a "
            f"probability: the outer product of its marginals reaches "
            f"{float(surrogate.max()):.6g}, above 1.0, because some species' "
            "mass is concentrated in one cell AND one week at once. Clamping "
            "is refused -- a clamped surrogate reproduces NEITHER marginal "
            "and no longer preserves mass, so the sweep would read a silently "
            "renormalised control as a weak spatiotemporal effect"
        )
    return _respec(t, "separable", surrogate.reshape(dense.shape))


CONTROLS: dict[str, Callable[..., PriorTensor]] = {
    "uniform": uniform_prior,
    "marginal": marginal_prior,
    "shuffle_space": shuffle_space,
    "shuffle_time": shuffle_time,
    "separable": separable_prior,
}
