"""Sparse (species x cell x week) prior tensor and its on-disk format.

This is the SERIALISATION BOUNDARY of the occurrence prior. Everything
downstream -- embedding extraction, the fusion sweep, the released artifact --
reads the prior through `PriorTensor.vector()` / `PriorTensor.lookup()`, never
through the Arrow table that produced it.

Layout
------
A `scipy.sparse.csr_matrix` with

    row    = index of the species key in `species_keys` (ascending)
    column = index of the cell in `cells` (ascending) * WEEKS_PER_YEAR
             + (week - 1)

`WEEKS_PER_YEAR` is imported from `indexing`, never spelled as a literal, so
the layout has exactly one definition. `_column_index` is that definition and
is used by BOTH the writer (`to_tensor`) and the readers (`lookup`,
`vector`): a packing that is true in one path and false in the other is the
defect this plan has found most often.

Beside the `.npz` sits a JSON sidecar carrying the spec, the species and cell
axes, the value a missing context takes, the fill (`nnz` against the dense
capacity), the GBIF download DOI and the licence. The sidecar is what makes
the release citable: the matrix alone cannot say which species a row is, which
grid it was cut on, what an absent entry means, or under what terms it may be
redistributed.

`docs/prior-artifact-format.md` is the SAME format written for someone who has
downloaded the files and is not reading this module: the layout, the field
meanings, the floor convention below, a numpy-only decoding recipe and the
CC BY 4.0 / GBIF-DOI terms. It ships with the release, and
`test_the_artifact_documentation_matches_a_freshly_saved_artifact` runs its
recipe against a freshly saved artifact so it cannot rot away from this code.

A MISSING CONTEXT READS THE FLOOR, AND THAT IS NOT IMPUTATION
-------------------------------------------------------------
A `(species, cell, week)` context whose species IS on the species axis and
whose cell IS on the cell axis, but which has no stored value, reads
`backoff.FLOOR` -- not a hard `0.0`.

This is not the serialiser inventing a probability. `FLOOR` is precisely the
value the backoff DEFINES for "no evidence", in its own words: most
(cell, week, species) combinations are empty, empty almost always means
"nobody looked" rather than "absent", and a species with no occurrences
anywhere still gets `floor`, never zero. Neither the backoff nor the smoother
expands to a full grid; both pass observed rows through. So an unstored
context is not a grid point that does not exist -- it is a grid point whose
value is known a priori, and it is exactly the value it would have carried had
it been materialised. Storing 93 600 000 explicit copies of one constant
(~1.12 GB of CSR at national scale) into a container whose entire purpose is
sparsity would change no answer this module can give.

A hard `0.0` would be the defect: the floor exists so the prior can never veto
a correct detection, and a zero for "nobody looked there" does exactly that.

What stays LOUD, because it is a CALLER error rather than an absence of
evidence:

* an UNKNOWN `species_key` raises `ValueError`;
* an UNKNOWN `cell` raises `ValueError`;
* a `week` outside 1..WEEKS_PER_YEAR raises `ValueError`;
* a `week` that is not a whole number -- `46.9`, `True`, `'5'` -- raises
  `ValueError` rather than being truncated into a week the caller never
  asked for.

The axes are derived from the data, so a key that is not on them is a typo, a
wrong-resolution H3 string, or a mismatched label set -- never a context with
no evidence. Flooring those would turn a caller bug into a plausible number.

NEVER RENORMALISED
------------------
`p` is a per-species marginal and is not renormalised over species at any
stage -- not in `backoff.py`, not in `smoothing.py`, and not here. A context's
`vector()` routinely sums above 1.0 (a species absent from the occurrence data
still contributes the floor, on top of the rest) or far below it (only a
subset of species was requested). Code or a test that assumes it sums to one
is wrong about the pipeline, not about this module.

Errors
------
Every contract violation raises `ValueError` naming that specific check. There
is no try/except, no repair and no silent skip: at the serialisation boundary
a repaired value is indistinguishable from a real one for every reader that
follows.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
from scipy import sparse

from .backoff import FLOOR
from .indexing import WEEKS_PER_YEAR

LICENCE = "CC BY 4.0"
LICENCE_URL = "https://creativecommons.org/licenses/by/4.0/"

_REQUIRED_TYPES = {
    "species_key": pa.int64(),
    "cell": pa.string(),
    "week": pa.int8(),
    "p": pa.float64(),
}


def _validate_week(week, where: str) -> None:
    """Weeks are WHOLE NUMBERS in 1..WEEKS_PER_YEAR. Outside that the packing ALIASES.

    `cell_index * WEEKS_PER_YEAR + (week - 1)` is not injective once `week`
    leaves 1..52: week 53 of cell *i* is exactly the column of week 1 of cell
    *i + 1*, and week 0 is week 52 of cell *i - 1*. An aliased read returns a
    real probability belonging to a DIFFERENT cell, so it cannot be detected
    downstream. Upstream folds ISO week 53 into 52, but `lookup` and `vector`
    are the public interface every downstream plan calls with its own
    arguments, so the bound is enforced here too.

    INTEGRALITY IS PART OF THE BOUND, and it is checked on the value the
    CALLER passed rather than on a truncation of it. `int(46.9)` is 46: a
    caller asking for week 46.9 was previously answered with week 46's column,
    a DIFFERENT week, with nothing raised -- the same consequence shape the
    aliasing checks below exist to prevent, and a repair at a boundary whose
    docstring forbids repair. `'5'` was coerced to week 5 the same way.

    A `bool` is REJECTED rather than accepted as 0/1. `isinstance(True, int)`
    is True in Python, so an int-only type check would let it through and
    `True` would silently read week 1 while `False` aliased the previous
    cell's last week. No caller means "the first week of the year" by `True`,
    so the only reading that is not a repair is a refusal.

    An INTEGRAL float (`5.0`) IS accepted: it names exactly the week a caller
    asking for 5 would get, so admitting it converts nothing and hides
    nothing. The rejection is of values whose acceptance would change WHICH
    week is answered.

    The clauses are checked in separate statements on purpose: a fixture that
    trips one never exercises the others. They are ORDERED -- bool, then
    numeric type, then integrality, then the two bounds -- because each later
    clause presupposes what the earlier ones establish (`'5' % 1` raises
    TypeError, and `True < 1` is a perfectly meaningful comparison that would
    silently pass).
    """
    if isinstance(week, bool):
        raise ValueError(
            f"week {week!r} in {where} is a bool, not a week number; Python "
            "counts True as 1 and False as 0, so a bool week would be answered "
            f"as week {int(week)} -- a week the caller never asked for -- and "
            "this boundary does not repair the value it was handed"
        )
    if not isinstance(week, (int, float, np.integer, np.floating)):
        raise ValueError(
            f"week {week!r} in {where} is not a numeric week (got "
            f"{type(week).__name__}); a string such as '5' would be coerced to "
            "week 5 and answered as if it had been asked for as a number, "
            "which is a repair at a boundary that must not repair"
        )
    if week % 1 != 0:
        raise ValueError(
            f"week {week!r} in {where} is not a whole number; the tensor packs "
            f"cell * {WEEKS_PER_YEAR} + (week - 1), which has a column only "
            "for whole weeks, so truncating would answer with a DIFFERENT "
            "week's probability and nothing downstream could detect it"
        )
    if week < 1:
        raise ValueError(
            f"week {week!r} in {where} is below 1; the tensor packs "
            f"cell * {WEEKS_PER_YEAR} + (week - 1), so week 0 would alias the "
            "last week of the PREVIOUS cell and read a real probability "
            "belonging to somewhere else"
        )
    if week > WEEKS_PER_YEAR:
        raise ValueError(
            f"week {week!r} in {where} is above {WEEKS_PER_YEAR}; the tensor "
            f"packs cell * {WEEKS_PER_YEAR} + (week - 1), so week "
            f"{WEEKS_PER_YEAR + 1} would alias week 1 of the NEXT cell. ISO "
            f"week 53 is folded into {WEEKS_PER_YEAR} upstream"
        )


def _column_index(cell_index, week):
    """The one definition of the column packing; scalar or elementwise.

    Used by the writer and by both readers so the layout cannot drift between
    them. `week` must already have been validated by `_validate_week`.
    """
    return cell_index * WEEKS_PER_YEAR + (week - 1)


def _cell_of_column(col):
    """The INVERSE of `_column_index`'s cell half; scalar or elementwise.

    It sits directly beside the forward definition on purpose. The only reader
    of it is the duplicate-row error message, which must name the cell the
    duplicate is ACTUALLY in: a contract-breach message that names the wrong
    cell sends whoever reads it to look for a real duplicate in the wrong
    place, which is worse than no name at all.

    Spelling the inverse inline at that call site -- `cells[col //
    WEEKS_PER_YEAR]` -- put half of the packing in a module whose docstring
    claims the layout has exactly one definition, so a change to
    `_column_index` would leave the message silently naming a different cell
    with nothing to catch it. Here the two are adjacent, and
    `test_the_column_packing_and_its_inverse_round_trip_over_the_whole_grid`
    pins them against each other over every (cell, week) of a real grid.
    """
    return col // WEEKS_PER_YEAR


def _floor_missing(values):
    """The one definition of "unstored means the floor"; scalar or elementwise.

    `lookup`, `vector` and `dense` ALL read through this, so the semantics
    cannot be true on one path and false on another -- the drift this module
    guards hardest against.

    The test is numeric (`values == 0.0`) rather than an inspection of the
    sparsity pattern, and that is exact rather than an approximation:
    `_validate_p` refuses a stored `p` at or below 0.0, so "reads as 0.0" and
    "was never stored" are the SAME SET for any tensor this module built. A
    hand-assembled tensor carrying an explicit 0.0 is an upstream contract
    breach either way, and the floor is the right answer for it too -- the
    backoff clamps into [FLOOR, 1.0], so 0.0 is not a probability this
    pipeline can produce.
    """
    return np.where(values == 0.0, FLOOR, values)


def _single_array(column: pa.ChunkedArray | pa.Array) -> pa.Array:
    """One contiguous Arrow array, whatever the chunking of the input."""
    if isinstance(column, pa.ChunkedArray):
        return column.combine_chunks()
    return column


def _validate_table(table: pa.Table) -> None:
    """Reject anything that is not a smoothed estimate table.

    Never repairs and never casts: `cell` and `week` are derived exactly once,
    upstream, and a cast here would be a second derivation of a value this
    module is supposed to pass through byte for byte.
    """
    for name, expected in _REQUIRED_TYPES.items():
        if name not in table.column_names:
            raise ValueError(
                f"prior table is missing the {name!r} column; got "
                f"{table.column_names!r}"
            )
        actual = table.schema.field(name).type
        if actual != expected:
            raise ValueError(
                f"prior table column {name!r} must be {expected}, got {actual}"
            )
        if table.column(name).null_count:
            raise ValueError(
                f"prior table column {name!r} contains null values; a null key "
                "would be packed into a column that belongs to another context "
                "and a null `p` would be stored as a silent nan"
            )


def _validate_p(values: np.ndarray) -> None:
    """`p` must be finite and inside (0, 1].

    The upstream floor (`backoff.FLOOR = 1e-6`) exists precisely so the prior
    can never veto a correct detection with a hard zero, and the backoff
    clamps every emitted value into `[floor, 1.0]`. A `p` of exactly `0.0`
    arriving here is therefore an upstream CONTRACT BREACH, not a value to
    optimise away: storing it would put an explicit zero in the matrix, and
    `_floor_missing` reads an explicit zero exactly as it reads an unstored
    context, so no reader could tell it from one. A nan would be stored as a
    silent nan entry and a negative or above-one value is not a probability at
    all.

    Three separate statements, one per clause: a finiteness check passes on a
    column of zeros, and a positivity check passes on a column of nans.

    There is deliberately no empty-input early return: `np.all` of an empty
    array is True and `np.any` of one is False, so every clause below is
    already correct on an empty column. A guard no fixture can distinguish
    would be exactly the dead branch this plan removes elsewhere.
    """
    if not np.all(np.isfinite(values)):
        raise ValueError(
            "prior table column 'p' contains non-finite values; a nan or inf "
            "would be stored as an explicit sparse entry and read back as a "
            "probability"
        )
    # THIS CLAUSE IS A PRECONDITION OF `PriorTensor.lookup` (and of `vector`
    # and `dense`, which read through the same `_floor_missing`). Those readers
    # substitute the floor for any value that reads as 0.0, and that is only
    # observationally identical to "was never stored" BECAUSE this clause makes
    # a stored zero impossible. Relax it -- to `< 0.0`, or away entirely -- and
    # the released artifact starts round-tripping real stored zeros as 1e-06,
    # with no error at either site. The two are coupled; change neither alone.
    if np.any(values <= 0.0):
        raise ValueError(
            "prior table column 'p' contains values at or below 0.0; the "
            "backoff clamps every value into [1e-06, 1.0], so a non-positive "
            "`p` means a stage was bypassed. Storing it would put an explicit "
            "zero in the matrix, which no reader can tell from a context that "
            "was never stored at all"
        )
    if np.any(values > 1.0):
        raise ValueError(
            "prior table column 'p' contains values above 1.0; the backoff "
            "clamps every value into [1e-06, 1.0], so a `p` above one means a "
            "stage was bypassed"
        )


@dataclass(frozen=True)
class PriorSpec:
    """Identifies one prior variant."""

    resolution: int
    source: str  # "eod" | "dof" | "eod+dof"
    variant: str  # "full" | "uniform" | "marginal" | "shuffle_space" | "shuffle_time"


@dataclass
class PriorTensor:
    spec: PriorSpec
    species_keys: np.ndarray
    cells: list[str]
    values: sparse.csr_matrix
    gbif_doi: str = ""
    _species_index: dict[int, int] = field(default_factory=dict, repr=False)
    _cell_index: dict[str, int] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        # Always rebuilt from the axes, never trusted from the caller: an
        # index that disagrees with its axis silently reads the wrong row.
        self._species_index = {int(k): i for i, k in enumerate(self.species_keys)}
        self._cell_index = {c: i for i, c in enumerate(self.cells)}
        if len(self._species_index) != len(self.species_keys):
            raise ValueError(
                "species_keys contains a duplicate; the later copy would "
                "shadow the earlier one and every lookup of that species "
                "would read the wrong matrix row"
            )
        if len(self._cell_index) != len(self.cells):
            raise ValueError(
                "cells contains a duplicate; the later copy would shadow the "
                "earlier one and every lookup in that cell would read another "
                "cell's block of columns"
            )
        n_rows, n_cols = self.values.get_shape()
        if n_rows != len(self.species_keys):
            raise ValueError(
                f"matrix has {n_rows} rows but there are "
                f"{len(self.species_keys)} species keys; the axes and the "
                "matrix disagree, so every lookup would read a shifted row"
            )
        if n_cols != len(self.cells) * WEEKS_PER_YEAR:
            raise ValueError(
                f"matrix has {n_cols} columns but "
                f"{len(self.cells)} cells x {WEEKS_PER_YEAR} weeks needs "
                f"{len(self.cells) * WEEKS_PER_YEAR}; the axes and the matrix "
                "disagree, so every lookup would read a shifted column"
            )

    @property
    def dense_capacity(self) -> int:
        """How many contexts the axes span, stored or not.

        `nnz / dense_capacity` is the artifact's fill, and the sidecar
        publishes both so a released tensor carries visible evidence of how
        much of its own grid was ever observed.
        """
        return len(self.species_keys) * len(self.cells) * WEEKS_PER_YEAR

    def _column(self, cell: str, week: int | float) -> int:
        """The packed column for a context; raises for an unknown CELL.

        The week is validated BEFORE the cell is looked up: an out-of-range
        week on an unknown cell must raise the WEEK error, because a caller
        shown the cell error first would fix the wrong thing.

        `week` is handed to the guard AS THE CALLER PASSED IT, not as
        `int(week)`. Validating a truncation is validating a different value:
        `int(46.9)` is 46, so the guard would approve week 46 and this method
        would return week 46's column for a caller who asked for 46.9.
        """
        _validate_week(week, "a tensor lookup")
        ci = self._cell_index.get(cell)
        if ci is None:
            raise ValueError(
                f"cell {cell!r} is not on this tensor's cell axis "
                f"({len(self.cells)} cells); the axis is derived from the "
                "occurrence data, so an unknown cell is a caller error -- a "
                "typo or a wrong-resolution H3 string -- not a context with "
                "no evidence, and returning the floor for it would turn that "
                "bug into a plausible probability"
            )
        return int(_column_index(ci, int(week)))

    def _row(self, species_key: int) -> int:
        """The matrix row for a species; raises for an unknown KEY."""
        row = self._species_index.get(int(species_key))
        if row is None:
            raise ValueError(
                f"species_key {int(species_key)!r} is not on this tensor's "
                f"species axis ({len(self.species_keys)} species); the axis is "
                "derived from the occurrence data, so an unknown key is a "
                "caller error -- a typo or a mismatched label set -- not a "
                "context with no evidence, and returning the floor for it "
                "would turn that bug into a plausible probability"
            )
        return row

    def lookup(self, species_key: int, cell: str, week: int | float) -> float:
        """P(species | cell, week).

        Raises for an out-of-range week, an unknown cell or an unknown species
        key. For a context that is ON both axes but has no stored value,
        returns `backoff.FLOOR` -- the value the backoff defines for "no
        evidence" -- never a hard 0.0. See the module docstring.

        BECAUSE OF THAT, THIS METHOD NEVER REPORTS ABSENCE: it is TOTAL over
        the axis cross-product and answers every context the axes span, so a
        returned `FLOOR` cannot be distinguished from a stored one and
        `lookup` can never tell you whether a triple was OBSERVED. Anything
        needing coverage, support size, sparsity or a denominator must go
        through the stored keys instead -- the matrix's own coordinates, or
        `values.nnz` against `dense_capacity` in aggregate.
        """
        col = self._column(cell, week)
        row = self._row(species_key)
        # PRECONDITION, enforced in `_validate_p`, NOT here: a stored `p` at or
        # below 0.0 is refused at write time, so "reads as 0.0" and "was never
        # stored" are the same set and substituting the floor below is
        # observationally identical to answering an unstored context. Weaken
        # `_validate_p`'s `<= 0.0` clause and this line silently starts
        # round-tripping a real stored zero as 1e-06, with nothing raised.
        return float(_floor_missing(np.float64(self.values[row, col])))

    def vector(self, cell: str, week: int | float) -> np.ndarray:
        """The prior over every species for one context, in `species_keys` order.

        Exactly one entry per species on the axis, floored wherever nothing was
        stored -- the same rule `lookup` applies, through the same
        `_floor_missing`, so the two can never disagree.

        NOT normalised: the values are whatever the pipeline produced, and
        their sum carries no meaning. See the module docstring.
        """
        col = self._column(cell, week)
        stored = np.asarray(self.values[:, [col]].todense(), dtype=np.float64).ravel()
        return _floor_missing(stored)

    def dense(self) -> np.ndarray:
        """The full (species x cell-week) grid, floored where nothing was stored.

        The matrix-shaped spelling of the same semantics `lookup` and `vector`
        expose, through the same `_floor_missing`, for callers that genuinely
        need every context at once (the dense controls). It materialises
        `dense_capacity` float64s, which is the inherent cost of being dense.
        """
        return _floor_missing(np.asarray(self.values.toarray(), dtype=np.float64))


def to_tensor(table: pa.Table, spec: PriorSpec) -> PriorTensor:
    """Pack a smoothed estimate table into a sparse tensor."""
    _validate_table(table)

    species = _single_array(table.column("species_key")).to_numpy(zero_copy_only=False)
    weeks = _single_array(table.column("week")).to_numpy(zero_copy_only=False)
    weeks = weeks.astype(np.int64)
    p = _single_array(table.column("p")).to_numpy(zero_copy_only=False)

    _validate_p(p)
    if weeks.size:
        # The scalar guard is the single definition of the bound; calling it on
        # the extremes checks the whole column in two calls.
        _validate_week(int(weeks.min()), "the prior table")
        _validate_week(int(weeks.max()), "the prior table")

    # `cell` is an opaque, case-sensitive, lower-case canonical H3 string. It
    # is dictionary-encoded and re-ordered, never re-derived, re-parsed or
    # case-folded: this module must hand back the exact bytes it was given.
    encoded = _single_array(table.column("cell")).dictionary_encode()
    dictionary = encoded.dictionary.to_pylist()
    cells = sorted(set(dictionary))
    position = {c: i for i, c in enumerate(cells)}
    remap = np.array([position[c] for c in dictionary], dtype=np.int64)
    cell_idx = remap[encoded.indices.to_numpy(zero_copy_only=False).astype(np.int64)]

    species_keys = np.unique(species.astype(np.int64))
    row_idx = np.searchsorted(species_keys, species.astype(np.int64))
    col_idx = _column_index(cell_idx, weeks)

    n_cols = len(cells) * WEEKS_PER_YEAR
    _reject_duplicates(row_idx, col_idx, n_cols, species_keys, cells, weeks)

    matrix = sparse.csr_matrix(
        (p.astype(np.float64), (row_idx, col_idx)),
        shape=(len(species_keys), n_cols),
        dtype=np.float64,
    )
    return PriorTensor(
        spec=spec,
        species_keys=np.asarray(species_keys, dtype=np.int64),
        cells=cells,
        values=matrix,
    )


def _reject_duplicates(
    row_idx: np.ndarray,
    col_idx: np.ndarray,
    n_cols: int,
    species_keys: np.ndarray,
    cells: list[str],
    weeks: np.ndarray,
) -> None:
    """Two rows for one (species_key, cell, week) is a contract breach.

    `scipy.sparse.csr_matrix((data, (row, col)))` SUMS duplicate coordinates
    silently: two rows of `p = 0.4` for the same key become `0.8`, a plausible
    number with no error and no nan. Every upstream stage rejects duplicates
    explicitly and says why; this stage would otherwise quietly double them.

    No size guard: `np.diff` of a 0- or 1-element array is empty, so the
    fewer-than-two-rows case is already handled by the arithmetic.
    """
    packed = row_idx.astype(np.int64) * n_cols + col_idx.astype(np.int64)
    order = np.argsort(packed, kind="stable")
    collisions = np.flatnonzero(np.diff(packed[order]) == 0)
    if collisions.size:
        first = int(order[collisions[0]])
        raise ValueError(
            "duplicate (species_key, cell, week) "
            f"({int(species_keys[row_idx[first]])!r}, "
            f"{cells[int(_cell_of_column(col_idx[first]))]!r}, "
            f"{int(weeks[first])}) in the prior table; scipy SUMS duplicate "
            "sparse coordinates, so the two values would be silently added "
            "into one plausible-looking probability"
        )


def _npz_path(path: Path) -> Path:
    """The `.npz` file this path names -- scipy's own rule, reproduced.

    `scipy.sparse.save_npz` APPENDS `.npz` to any path that does not already
    end in it. This function reproduces that rule exactly, so the SIDECAR and
    the RETURNED PATH name the file scipy actually wrote: unnormalised, saving
    to `p.dat` wrote `p.dat.npz` while the sidecar and the returned path named
    something else, and `load_prior` then failed on a path `save_prior` had
    just returned as successful.

    The suffix is APPENDED, never SUBSTITUTED. `Path.with_suffix` truncates at
    the LAST dot, which silently collapses any name whose version carries one:
    `prior_v1.2` and `prior_v1.3` both become `prior_v1.npz`, so saving the
    second OVERWRITES the first with no error and the released directory holds
    one file claiming to be both. That is data loss at the release boundary,
    which is why the append form is the only correct one here.
    """
    return path if path.suffix == ".npz" else path.with_name(path.name + ".npz")


def _sidecar_path(npz: Path) -> Path:
    """The JSON sidecar beside an `.npz`.

    Derived by replacing only the `.npz` suffix, which is safe precisely
    because `_npz_path` guarantees the name ENDS in `.npz`: two artifacts with
    distinct `.npz` names therefore always have distinct sidecars.
    """
    return npz.with_suffix(".json")


def save_prior(tensor: PriorTensor, path: Path) -> Path:
    """Write the matrix and its citable sidecar; return the `.npz` written."""
    if not tensor.gbif_doi:
        raise ValueError(
            "refusing to write a prior artifact with an empty gbif_doi; this "
            f"release is licensed {LICENCE}, which REQUIRES attribution, and "
            "the GBIF download DOI is the only thing tying this tensor to the "
            "occurrence records it was derived from. Shipped without it the "
            "artifact cannot be cited, cannot be reproduced, and carries no "
            "trace of the terms its own sources were published under"
        )
    npz = _npz_path(path)
    npz.parent.mkdir(parents=True, exist_ok=True)
    sparse.save_npz(npz, tensor.values.tocoo())
    meta = {
        "spec": asdict(tensor.spec),
        "species_keys": [int(k) for k in tensor.species_keys],
        "cells": list(tensor.cells),
        "weeks_per_year": WEEKS_PER_YEAR,
        # The value a context on both axes but absent from the matrix takes.
        # Published because a third party decoding the `.npz` with a plain
        # sparse reader would otherwise read 0.0 there and silently veto every
        # unobserved context.
        "missing_value": FLOOR,
        # The artifact's own fill, so a release carries visible evidence of
        # how much of its grid was ever observed.
        "nnz": int(tensor.values.nnz),
        "dense_capacity": int(tensor.dense_capacity),
        "licence": LICENCE,
        "licence_url": LICENCE_URL,
        "gbif_doi": tensor.gbif_doi,
        "attribution": (
            f"Derived from GBIF occurrence download {tensor.gbif_doi}. "
            f"Released under {LICENCE} ({LICENCE_URL}); cite that DOI and this "
            "licence when redistributing or building on this artifact."
        ),
    }
    _sidecar_path(npz).write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return npz


def load_prior(path: Path) -> PriorTensor:
    """Read back a tensor written by `save_prior`."""
    npz = _npz_path(path)
    meta = json.loads(_sidecar_path(npz).read_text(encoding="utf-8"))
    if int(meta["weeks_per_year"]) != WEEKS_PER_YEAR:
        raise ValueError(
            f"sidecar was written with weeks_per_year={meta['weeks_per_year']!r} "
            f"but this build packs {WEEKS_PER_YEAR}; every column index in the "
            "matrix would be read against the wrong stride"
        )
    if float(meta["missing_value"]) != FLOOR:
        raise ValueError(
            f"sidecar was written with missing_value={meta['missing_value']!r} "
            f"but this build reads an absent context as {FLOOR}; every "
            "unobserved context would be answered against a different "
            "convention from the one the artifact was published under"
        )
    matrix = sparse.load_npz(npz).tocsr()
    if int(meta["nnz"]) != matrix.nnz:
        raise ValueError(
            f"sidecar reports nnz={meta['nnz']!r} but the matrix stores "
            f"{matrix.nnz}; the sidecar and the `.npz` are not halves of the "
            "same artifact, so neither the published fill nor the axes can be "
            "trusted to describe this matrix"
        )
    return PriorTensor(
        spec=PriorSpec(**meta["spec"]),
        species_keys=np.asarray(meta["species_keys"], dtype=np.int64),
        cells=list(meta["cells"]),
        values=matrix,
        gbif_doi=meta.get("gbif_doi", ""),
    )
