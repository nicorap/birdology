# The occurrence-prior artifact: file format, semantics and citation

This document describes the **released files**, not this repository's Python.
It is written for someone who has downloaded a prior artifact and intends to
decode it with their own tooling. Everything below was derived by reading
`src/birdology/prior/tensor.py` and inspecting an artifact it actually wrote.

If you read nothing else, read
[**A missing entry is NOT zero**](#a-missing-entry-is-not-zero) and
[**What is actually shipped**](#what-is-actually-shipped). The first is the
decoding convention; the second is the fact that every artifact released here
satisfies it trivially, because it stores every context it spans.

---

## 1. File layout

A prior artifact is **two files that must travel together**:

| file | content |
|---|---|
| `<name>.npz` | the sparse matrix, in SciPy's COO `.npz` container |
| `<name>.json` | the sidecar: axes, semantics, fill, licence, provenance |

The sidecar is the `.npz` name with its `.npz` suffix replaced by `.json`
(`prior_res4_dof_full.npz` -> `prior_res4_dof_full.json`). The `.npz` suffix is
**appended** to the requested name, never substituted, so `prior_v1.2` and
`prior_v1.3` are two distinct artifacts rather than one overwriting the other.

The matrix alone is not a release. It cannot say which species a row is, which
grid it was cut on, what an absent entry means, or under what terms it may be
redistributed. All of that is in the sidecar, and the sidecar is what makes the
artifact citable.

### The `.npz` container

`np.load` on the `.npz` yields exactly five members — SciPy's COO layout:

<!-- npz-members: col data format row shape -->

| member | dtype | meaning |
|---|---|---|
| `format` | bytes | the literal `b"coo"`; the container is coordinate format |
| `shape` | int64, length 2 | `[n_species, n_cells * weeks_per_year]` |
| `row` | int | row index of each stored entry |
| `col` | int | column index of each stored entry |
| `data` | **float64** | the stored probability of each entry |

`data` is float64 and must be read as float64: the floor is `1e-06` and the
values span several orders of magnitude, so float32 loses real precision.

You do not need SciPy. `numpy.load` plus the sidecar is enough, and the worked
example in section 5 uses nothing else.

---

## 2. Sidecar fields

The sidecar is UTF-8 JSON. A complete example, from an artifact with two
species and two cells:

```json
{
  "spec": {
    "resolution": 4,
    "source": "dof",
    "variant": "full"
  },
  "species_keys": [
    1,
    2
  ],
  "cells": [
    "841f059ffffffff",
    "841f05bffffffff"
  ],
  "weeks_per_year": 52,
  "missing_value": 1e-06,
  "nnz": 3,
  "dense_capacity": 208,
  "licence": "CC BY 4.0",
  "licence_url": "https://creativecommons.org/licenses/by/4.0/",
  "gbif_doi": "10.15468/dl.abc123",
  "attribution": "Derived from GBIF occurrence download 10.15468/dl.abc123. Released under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/); cite that DOI and this licence when redistributing or building on this artifact."
}
```

| field | meaning |
|---|---|
| `spec.resolution` | the H3 resolution the cell axis was cut at |
| `spec.source` | which occurrence sources were pooled (`eod`, `dof`, `eod+dof`) |
| `spec.variant` | `full` for the prior itself; `uniform` / `marginal` / `shuffle_space` / `shuffle_time` / `separable` identify **controls**, which are null models and not the prior |
| `species_keys` | the row axis: GBIF species keys, ascending. Row *i* of the matrix is `species_keys[i]` |
| `cells` | the cell axis: H3 cell strings, sorted, opaque and case-sensitive. Byte-for-byte what the source data carried |
| `weeks_per_year` | the column stride (52). ISO week 53 is folded into 52 upstream, so weeks run 1..52 inclusive |
| `missing_value` | **the value of any context that is on both axes but not stored** — `1e-06`. See section 3 |
| `nnz` | how many entries the matrix actually stores. Must equal `len(data)` |
| `dense_capacity` | `len(species_keys) * len(cells) * weeks_per_year` — how many contexts the axes span, stored or not. `nnz / dense_capacity` is the artifact's fill |
| `licence` | `CC BY 4.0` |
| `licence_url` | the licence deed |
| `gbif_doi` | the GBIF occurrence-download DOI this prior was derived from |
| `attribution` | a ready-made attribution string naming both the DOI and the licence |

`nnz` and `dense_capacity` are published so a release carries visible evidence
of how much of its own grid is stored. See
[What is actually shipped](#what-is-actually-shipped): on every artifact
released here the two are EQUAL.

| `spec.variant` | what it is |
|---|---|
| `full` | the prior itself |
| `uniform` | control: one constant everywhere, at the prior's total mass |
| `marginal` | control: species frequency only, no space, no time |
| `shuffle_space` | control: real values, wrong cells |
| `shuffle_time` | control: real values, wrong weeks |
| `separable` | control: both marginals right, their interaction destroyed |

---

## 2a. What is actually shipped

**Every artifact produced by `scripts/build_prior.py` is fully materialised:
`nnz == dense_capacity`, `fill == 1.0000`, and NO stored value equals
`missing_value`.**

This is not an accident and it is not going to change quietly. The build passes
the cell axis and the weeks `1..52` explicitly, and the writer refuses a
probability at or below `0.0`, so every context the axes span is written out as
an explicit non-zero entry. Measured on real builds (26 species x 115 H3 res-4
cells x 52 weeks): `nnz` 155 480 of `dense_capacity` 155 480, fill 1.0000,
`(dense == missing_value).sum()` 0 — for the full prior and for every control
alike.

Two things follow for you as a decoder:

1. **The artifact you downloaded has no missing entries**, so a decoder that
   fills with `0.0` and one that fills with `missing_value` will agree on it
   exactly. The convention in section 3 is still the contract, and you should
   still implement it, but on these files it is not what separates a correct
   read from a wrong one.
2. **`fill` is not a coverage statistic on these files.** It is 1.0 on the full
   prior *and* on every control, so it tells you nothing about how much of the
   grid was ever observed. The prior answers every context because the backoff
   and smoothing stages ESTIMATE every context — an entry is not evidence that
   anyone surveyed that cell in that week. If you need survey coverage, it is
   not in this artifact.

`missing_value` is retained in the sidecar for two reasons: forward
compatibility, so a future genuinely-sparse release decodes under a stated
convention rather than a guessed one; and because the reader in this repository
accepts **hand-built sparse tensors** that a third party may assemble, for which
the rule in section 3 is load-bearing.

---

## 3. A missing entry is NOT zero

**A `(species, cell, week)` context whose species is on `species_keys` and
whose cell is on `cells`, but which has no stored entry, has the value
`missing_value` = `1e-06`. It does not have the value `0.0`.**

On the artifacts released here this rule is **vacuous but still binding**: they
store every context (section 2a), so there is nothing for it to apply to. It
matters for a hand-built or re-sparsified tensor, and it is the convention any
future sparse release will be published under, so implement it.

The reasoning, which is the part worth carrying:

An empty `(cell, week, species)` combination almost always means *nobody
looked* rather than *the species is absent*. `1e-06` is the floor the upstream
backoff defines for exactly that case: a species with no occurrences anywhere
still gets the floor, never zero. An unstored context is therefore not a grid
point that does not exist — it is a grid point whose value is known a priori,
and it is exactly the value it would have carried had it been written out.
Storing many explicit copies of one constant into a container whose entire
purpose is sparsity would change no answer.

A hard `0.0` would be a defect rather than a conservative reading: the floor
exists so the prior can never veto a correct detection, and a zero for "nobody
looked there" does exactly that. The prior is fused **multiplicatively** with
an acoustic likelihood, so a zero is an absolute veto — and it would land on
precisely the rare and out-of-season detections that are hardest to get right.

Two consequences for a decoder:

1. **Fill the dense grid with `missing_value`, not with zeros.** Any sparse
   reader that materialises implicit zeros disagrees with the artifact's own
   semantics wherever a context is unstored. On a released file that is
   nowhere; on a sparse one it is everywhere it matters.
2. **The values never tell you what was observed.** Because an unstored context
   reads as the floor, you cannot recover "was this triple observed?" from a
   value. Nor can you recover it from the stored coordinates on a released
   artifact, which stores all of them. **Survey coverage is not recoverable
   from this artifact at all** — `nnz` against `dense_capacity` describes what
   is STORED, and on these files that is everything.

`0.0` never appears as a stored value: the writer refuses a probability at or
below `0.0`, precisely so that "reads as zero" and "was never stored" cannot be
different things.

---

## 4. How the matrix is indexed

The axes live in the sidecar and the matrix is indexed **by position in them**:

```text
row    = index of the species key in `species_keys`   (ascending)
column = index of the cell in `cells` (sorted) * weeks_per_year + (week - 1)
```

so, spelled as the packing:

    cell_index * 52 + (week - 1)

with `week` a whole number in `1..52`. The matrix is **species x cell-week**:
`shape[0]` is the number of species and `shape[1]` is
`len(cells) * weeks_per_year`. If you have these the other way round, you have
transposed the artifact and every value you read belongs to something else.

Inverting a column is `cell = cells[col // weeks_per_year]` and
`week = col % weeks_per_year + 1`.

Use `weeks_per_year` from the sidecar rather than hard-coding 52; an artifact
written under a different stride is a different packing, and reading it against
52 shifts every column silently.

A species key or cell string that is not on the published axes is **not** a
context with no evidence — the axes are derived from the data, so an unknown key
is a typo, a wrong-resolution H3 string, or a mismatched label set. Answering it
with the floor would turn that mistake into a plausible number. This
repository's reader raises instead, and yours should too.

The values are **per-species marginals and are never renormalised**. A single
context's probabilities do not sum to 1.0 and are not meant to; do not rescale
them.

---

## 5. Reconstructing the dense grid

NumPy and the standard library only — no SciPy, no code from this repository:

```python
import json

import numpy as np


def load_dense(npz_path, json_path):
    """The full (species x cell-week) grid, with unstored contexts FLOORED."""
    with open(json_path, encoding="utf-8") as fh:
        meta = json.load(fh)

    with np.load(npz_path) as z:
        assert bytes(z["format"]) == b"coo", "not a COO artifact"
        n_rows, n_cols = (int(x) for x in z["shape"])
        # The axes and the matrix must agree, or every read is shifted.
        assert n_rows == len(meta["species_keys"])
        assert n_cols == len(meta["cells"]) * meta["weeks_per_year"]
        # The sidecar and the .npz must be halves of the SAME artifact.
        assert len(z["data"]) == meta["nnz"]

        # THE LOAD-BEARING LINE: unstored means `missing_value`, not 0.0.
        grid = np.full((n_rows, n_cols), float(meta["missing_value"]), dtype=np.float64)
        grid[z["row"], z["col"]] = z["data"]

    assert grid.size == meta["dense_capacity"]
    return meta, grid


def lookup(meta, grid, species_key, cell, week):
    """P(species | cell, week). Raises for a key that is not on an axis."""
    if species_key not in meta["species_keys"]:
        raise ValueError(f"species_key {species_key!r} is not on the species axis")
    if cell not in meta["cells"]:
        raise ValueError(f"cell {cell!r} is not on the cell axis")
    if not 1 <= week <= meta["weeks_per_year"]:
        raise ValueError(f"week {week!r} is outside 1..{meta['weeks_per_year']}")
    row = meta["species_keys"].index(species_key)
    col = meta["cells"].index(cell) * meta["weeks_per_year"] + (week - 1)
    return float(grid[row, col])
```

`lookup` above is **total** over the axis cross-product: it answers every
(species, cell, week) the axes span, returning `missing_value` where nothing
was stored. It therefore never tells you that something was *not* observed, and
neither does anything else in the file:

```text
fill = nnz / dense_capacity
```

is the fraction of the grid that is **stored**, which on every artifact released
here is exactly `1.0` (section 2a). It is not a coverage or sampling-effort
statistic and must not be reported as one.

Materialising the dense grid costs 8 bytes per context — `dense_capacity`
float64s — and since these artifacts store every context, that is also roughly
what the `.npz` itself holds. At national resolutions that is large; if you only
need a few contexts, index the COO coordinates directly and substitute
`missing_value` for anything you do not find.

---

## 6. Licence, citation and provenance

The artifact is released under **CC BY 4.0**
(<https://creativecommons.org/licenses/by/4.0/>). CC BY **requires
attribution**.

The underlying occurrence records come from GBIF. The sidecar's `gbif_doi` is
the DOI of the exact occurrence download this prior was derived from, and it is
the only thing tying the artifact to those records: without it the prior cannot
be cited, cannot be reproduced, and carries no trace of the terms its own
sources were published under. The writer refuses to produce an artifact with an
empty `gbif_doi` for that reason.

When you redistribute this artifact or build on it, cite **both**:

1. the GBIF download DOI in `gbif_doi`, and
2. this artifact under CC BY 4.0.

The sidecar's `attribution` field is a ready-made string doing both. Keep the
sidecar with the matrix when you redistribute; a `.npz` on its own carries
neither the licence nor the provenance, and is not a lawful redistribution of
this release.

---

## 7. Controls are not the prior

An artifact whose `spec.variant` is not `"full"` is a **control**: a null model
built by destroying one kind of structure in the prior. There are five:

| variant | what is destroyed | what survives |
|---|---|---|
| `uniform` | everything | total mass only |
| `marginal` | space, time and their interaction | each species' own mass |
| `shuffle_space` | which cell a value belongs to | every cell's weekly profile, all masses |
| `shuffle_time` | which week a value belongs to | every cell's value multiset, all masses |
| `separable` | ONLY the space x time interaction | both marginals exactly, all masses |

Controls exist to be compared against the prior, not to be used as one.
`separable` is the one that isolates the joint space x time term: the two
shuffles relabel an axis and carry the joint structure along with it, so they
leave the interaction's magnitude unchanged.

Every control preserves the full prior's total mass exactly, and all but
`uniform` preserve each species' own mass, so a control is on the same scale as
the prior it is the null for. Controls are dense by construction, so their `nnz`
equals their `dense_capacity` — as does the released full prior's (section 2a),
so `fill` does not distinguish them either.

---

*This file is kept honest by two tests. Field names, constants and the decoding
recipe are executed against a freshly saved artifact by
`tests/prior/test_tensor.py::test_the_artifact_documentation_matches_a_freshly_saved_artifact`.
The fill regime section 2a asserts is pinned against an artifact built through
`build_from_archive` — a real pipeline output, not a hand-built fixture — by
`tests/prior/test_controls.py::test_the_documented_fill_regime_holds_on_a_PIPELINE_artifact`.
That second test exists because an earlier revision of this document claimed the
released grid was ~2% full, which was measured on a hand-built sparse fixture
and is false of everything the pipeline writes.*
