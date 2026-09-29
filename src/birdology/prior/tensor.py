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
axes, the GBIF download DOI and the licence. The sidecar is what makes the
release citable: the matrix alone cannot say which species a row is, which
grid it was cut on, or under what terms it may be redistributed.

STRUCTURAL ZEROS -- read this with Task 10's T10-R1
---------------------------------------------------
A `(species, cell, week)` context that is ABSENT from the input table is a
STRUCTURAL zero here, and `lookup` returns a hard `0.0` for it -- NOT the
upstream floor (`backoff.FLOOR = 1e-6`).

That is correct for this stage: it cannot invent a probability for a grid
point it was never handed, and inventing one would make a missing row
indistinguishable from a real estimate. It does, however, put an obligation on
the CALLER: the table passed to `to_tensor` must be a DENSE grid over all
`WEEKS_PER_YEAR` weeks (and over every cell and species the downstream sweep
will query), because a gap in that grid becomes a hard zero that can veto a
correct detection. `hierarchical_estimate` emits exactly such a dense grid and
clamps every value into `[floor, 1.0]`; anything that bypasses it must do the
same. Task 10 carries the matching obligation (T10-R1); the two must be read
together.

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

from .indexing import WEEKS_PER_YEAR

LICENCE = "CC BY 4.0"

_REQUIRED_TYPES = {
    "species_key": pa.int64(),
    "cell": pa.string(),
    "week": pa.int8(),
    "p": pa.float64(),
}


def _validate_week(week: int, where: str) -> None:
    """Weeks are 1..WEEKS_PER_YEAR. Outside that range the packing ALIASES.

    `cell_index * WEEKS_PER_YEAR + (week - 1)` is not injective once `week`
    leaves 1..52: week 53 of cell *i* is exactly the column of week 1 of cell
    *i + 1*, and week 0 is week 52 of cell *i - 1*. An aliased read returns a
    real probability belonging to a DIFFERENT cell, so it cannot be detected
    downstream. Upstream folds ISO week 53 into 52, but `lookup` and `vector`
    are the public interface every downstream plan calls with its own
    arguments, so the bound is enforced here too.

    The two bounds are checked in two separate statements on purpose: a
    fixture that trips one never exercises the other.
    """
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
    optimise away: skipping it would convert a real zero into a structural
    zero that `lookup` reports identically to "this context is not in the
    tensor at all". A nan would be stored as an explicit nan entry and a
    negative or above-one value is not a probability at all.

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
    if np.any(values <= 0.0):
        raise ValueError(
            "prior table column 'p' contains values at or below 0.0; the "
            "backoff clamps every value into [1e-06, 1.0], so a non-positive "
            "`p` means a stage was bypassed. Storing it would make a real "
            "zero indistinguishable from a context missing from the tensor"
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

    def _column(self, cell: str, week: int) -> int | None:
        """The packed column, or None when the CELL is not in the tensor.

        The week is validated BEFORE the cell is looked up: an out-of-range
        week on an unknown cell must still raise rather than be swallowed by
        the `None` path, which the caller cannot tell from a legitimate miss.
        """
        _validate_week(int(week), "a tensor lookup")
        ci = self._cell_index.get(cell)
        if ci is None:
            return None
        return int(_column_index(ci, int(week)))

    def lookup(self, species_key: int, cell: str, week: int) -> float:
        """P(species | cell, week).

        Returns a hard `0.0` for an unknown species or cell, and for any
        context absent from the table this tensor was built from. That is a
        STRUCTURAL zero, not the floor -- see the module docstring.
        """
        row = self._species_index.get(int(species_key))
        col = self._column(cell, week)
        if row is None or col is None:
            return 0.0
        return float(self.values[row, col])

    def vector(self, cell: str, week: int) -> np.ndarray:
        """The prior over every species for one context, in `species_keys` order.

        NOT normalised: the values are whatever the pipeline produced, and
        their sum carries no meaning. See the module docstring.
        """
        col = self._column(cell, week)
        if col is None:
            return np.zeros(len(self.species_keys), dtype=np.float64)
        return np.asarray(self.values[:, [col]].todense(), dtype=np.float64).ravel()


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
            f"{cells[int(col_idx[first]) // WEEKS_PER_YEAR]!r}, "
            f"{int(weeks[first])}) in the prior table; scipy SUMS duplicate "
            "sparse coordinates, so the two values would be silently added "
            "into one plausible-looking probability"
        )


def _npz_path(path: Path) -> Path:
    """The `.npz` file this path names.

    `scipy.sparse.save_npz` APPENDS `.npz` to any path that does not already
    end in it, so the suffix must be normalised BEFORE the save, not after:
    saving to `p.dat` writes `p.dat.npz` while the sidecar and the returned
    path would name `p.npz`, and `load_prior` would then fail on a path
    `save_prior` had just returned as successful.
    """
    return path if path.suffix == ".npz" else path.with_suffix(".npz")


def save_prior(tensor: PriorTensor, path: Path) -> Path:
    """Write the matrix and its citable sidecar; return the `.npz` written."""
    npz = _npz_path(path)
    npz.parent.mkdir(parents=True, exist_ok=True)
    sparse.save_npz(npz, tensor.values.tocoo())
    meta = {
        "spec": asdict(tensor.spec),
        "species_keys": [int(k) for k in tensor.species_keys],
        "cells": list(tensor.cells),
        "weeks_per_year": WEEKS_PER_YEAR,
        "licence": LICENCE,
        "gbif_doi": tensor.gbif_doi,
    }
    npz.with_suffix(".json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return npz


def load_prior(path: Path) -> PriorTensor:
    """Read back a tensor written by `save_prior`."""
    npz = _npz_path(path)
    meta = json.loads(npz.with_suffix(".json").read_text(encoding="utf-8"))
    if int(meta["weeks_per_year"]) != WEEKS_PER_YEAR:
        raise ValueError(
            f"sidecar was written with weeks_per_year={meta['weeks_per_year']!r} "
            f"but this build packs {WEEKS_PER_YEAR}; every column index in the "
            "matrix would be read against the wrong stride"
        )
    return PriorTensor(
        spec=PriorSpec(**meta["spec"]),
        species_keys=np.asarray(meta["species_keys"], dtype=np.int64),
        cells=list(meta["cells"]),
        values=sparse.load_npz(npz).tocsr(),
        gbif_doi=meta.get("gbif_doi", ""),
    )
