# The occurrence-prior artifact: file format, semantics and citation

This document describes the **released files**, not this repository's Python.
It is written for someone who has downloaded a prior artifact and intends to
decode it with their own tooling. Everything below was derived by reading
`src/birdology/prior/tensor.py` and inspecting an artifact it actually wrote.

If you read nothing else, read
[**Restricted artifacts**](#2f-restricted-artifacts--values-only-where-they-were-queried)
(check `restricted` first: a restricted artifact answers only its declared
contexts),
[**A missing entry is NOT zero**](#a-missing-entry-is-not-zero),
[**What is actually shipped**](#what-is-actually-shipped) and
[**How hard the artifact was smoothed**](#2b-how-hard-the-artifact-was-smoothed--and-why-it-matters-to-you).
The first is the decoding convention; the second is the fact that every
artifact released here satisfies it trivially, because it stores every context
it spans; the third is the one that changes what the numbers *mean* — at H3
resolution 5 an artifact retains under half of the real seasonal amplitude, and
the constants that decide that are published in the sidecar.

Two known limitations are stated as such rather than hidden: the sidecar's
recorded constants are **not verifiable** against the values (section 2b,
"Known limitation" — the released CLI path is protected, a library caller's is
not), and **week 52 is systematically shrunk less** than its neighbours because
ISO week 53 folds into it (section 2e).

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
  "backoff_params": {
    "cell_week_weight": 10.0,
    "cell_weight": 50.0,
    "floor": 1e-06
  },
  "smoothing_params": {
    "space_weight": 0.3,
    "time_weight": 0.3
  },
  "gbif_doi": "10.15468/dl.abc123",
  "attribution": "Derived from GBIF occurrence download 10.15468/dl.abc123. Released under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/); cite that DOI and this licence when redistributing or building on this artifact.",
  "format_version": 2,
  "restricted": false,
  "restriction": null
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
| `backoff_params` | **how hard this artifact was shrunk** — a JSON object, or `null`. See section 2b |
| `smoothing_params` | **how hard this artifact was smoothed** — a JSON object, or `null`. See section 2b |
| `gbif_doi` | the GBIF occurrence-download DOI this prior was derived from |
| `attribution` | a ready-made attribution string naming both the DOI and the licence |
| `format_version` | sidecar format version, currently `2`. **Absent means version 1** (written before restriction existed; such an artifact is unrestricted). A version you do not know must be refused |
| `restricted` | `false` for an artifact spanning its whole grid; `true` for one built only on a query set. **Read this before anything else** — see section 2f |
| `restriction` | `null`, or (when `restricted` is `true`) the declared domain and the query set that defined it. See section 2f |

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

## 2b. How hard the artifact was smoothed — and why it matters to you

**`backoff_params` and `smoothing_params` record the constants that produced
this artifact. They are not tuning trivia: they decide how much of the real
seasonal signal the values you are reading still contain.**

```json
"backoff_params":   {"cell_week_weight": 10.0, "cell_weight": 50.0, "floor": 1e-06},
"smoothing_params": {"space_weight": 0.3, "time_weight": 0.3}
```

Both fields are **always present**. Either may be `null`, which means *this
artifact does not record how it was built* — a hand-assembled tensor, not
something `scripts/build_prior.py` wrote. Every artifact from the build script
carries objects, never `null`. A sidecar in which the **key is missing
entirely** is a different thing again and this repository's reader **refuses**
it, rather than reading it as `null`: an absent key is a file that cannot be
asked, and treating it as an artifact that answered "no record" would be a
guess dressed as a fact. If you write your own decoder, do the same.

### What the numbers mean

The prior is built by shrinking each `(species, cell, week)` estimate toward
the coarser levels above it. A weight is a **pseudo-count measured in
records**: `cell_week_weight = 10` means the cell's annual profile is worth ten
records of local evidence, so **a cell-week with fewer than ten records is
decided more by its parent than by itself**. `space_weight` / `time_weight` are
neighbour weights relative to a cell's own value of 1.0.

### Measured consequence

Fraction of a known seasonal signal, and of a known spatial signal, that
survives into the `.npz`. Built through `scripts/build_prior.py` on a
167 879-record synthetic archive over 1500 Danish sites with a generating
intensity we control, so "how much survived" is a measurement rather than an
estimate:

| resolution | cells | median records per cell-week | seasonal amplitude retained | spatial contrast retained |
|---|---|---|---|---|
| 3 | 21 | 102 | 0.798 | 0.898 |
| 4 | 98 | 31 | 0.679 | 0.947 |
| 5 | 461 | 6 | 0.426 | 0.887 |
| 6 | 1171 | 2 | 0.298 | 0.811 |

Read this before you use a fine-resolution artifact for anything that depends
on the *size* of a seasonal swing:

1. **Seasonal amplitude decays with resolution; spatial contrast does not.** A
   res-5 artifact carries under half of the real seasonal swing, and a res-6
   one under a third. Spatial contrast stays near 0.9 throughout.
2. **The phase is not affected.** The week a species peaks in is recovered
   correctly at every resolution. It is the *depth* of the seasonal trough that
   is flattened, not its position. If your use is "when does this species
   peak?", the flattening does not concern you; if it is "how much more likely
   is this species in June than in January?", it does, and the answer you read
   off the artifact is a lower bound.
3. **The constants, not the data, set this.** On the same records at
   `cell_week_weight = 0.5` the res-5 seasonal retention is 1.09 rather than
   0.43, and at 50.0 it is 0.13. `cell_weight` 5 / 50 / 500 moves the *spatial*
   retention 0.987 / 0.887 / 0.610.
4. **They are untuned, and this release says so.** No sweep in this project
   justifies 10 / 50 / 0.3 / 0.3 over other values. Higher values are not
   simply worse: with almost no shrinkage a three-record cell-week reports its
   own sampling noise as signal. It is a bias/variance trade-off that has not
   been resolved against a downstream criterion.

Two artifacts that differ only in these fields are genuinely different
datasets. Compare `backoff_params` before comparing values across releases.

### Known limitation: the recorded params are not *verified* against the values

**Nothing in the artifact proves that the constants in the sidecar are the
constants the numbers were computed with.** The sidecar is a faithful record
of what the writer was *told*, not a checksum over the computation.

Reproduced, on two real builds through the pipeline's own entry point:

| | build A | build B |
|---|---|---|
| `backoff_params` | `BackoffParams(10.0, 50.0, 1e-06)` | different constants |
| seasonal CV of the values | 1.176 | 1.495 |
| `max|A − B|` over the shared grid | — | **0.176** |

So the constants do genuinely change the numbers. Build A's params object was
then assigned onto build B's tensor and saved. The result was a well-formed,
fully provenanced artifact — `nnz` 4420, fill 1.0, DOI present, manifest
written — whose sidecar claims A's constants over B's values. **Nothing was
raised, at write time or at read time.**

What this does and does not mean for you:

1. **The path that produces released artifacts IS protected.** Everything
   published here is built by `scripts/build_prior.py`, which threads a single
   params object into both the estimation stage and the writer, and that
   property is pinned by a test that fails if the constants stop reaching the
   computation (mutant `B-I2f`, killer
   `test_the_build_params_actually_CHANGE_the_values_not_just_the_sidecar`).
   If your files came from a release of this project, the sidecar describes
   the build.
2. **The gap is reachable only by a library caller.** Someone importing
   `birdology.prior` who constructs a tensor and then *assigns* a foreign
   `BackoffParams` / `SmoothingParams` object to it gets an artifact whose
   record is a confident lie. That is the same class as handing any stage the
   wrong data, but it is worth naming because these two fields are the ones
   whose entire purpose is to be believed.
3. **What you can check yourself, and what you cannot.** You can confirm the
   sidecar and the matrix are halves of the same artifact (`nnz` against
   `len(data)`, the axes against `shape` — the recipe in section 5 does both).
   You cannot confirm the *constants*: shrinkage is not invertible from the
   output, so no decoder, including this repository's, can recompute them. If
   the provenance of a third-party artifact matters to your result, treat
   `backoff_params` as an attestation by whoever built it and not as a
   measurement.

---

## 2c. The per-resolution manifest — its absence means the set is incomplete

Beside each resolution's artifacts, `scripts/build_prior.py` writes one more
file:

```text
prior_{source}_r{resolution}_manifest.json
```

for example `prior_dof_r4_manifest.json`. A real one:

```json
{
  "manifest_version": 2,
  "source": "dof",
  "resolution": 3,
  "stem": "prior_dof_r3",
  "gbif_doi": "10.15468/dl.abc123",
  "artifact_count": 6,
  "artifacts": [
    {"npz": "prior_dof_r3_full.npz",          "sidecar": "prior_dof_r3_full.json"},
    {"npz": "prior_dof_r3_uniform.npz",       "sidecar": "prior_dof_r3_uniform.json"},
    {"npz": "prior_dof_r3_marginal.npz",      "sidecar": "prior_dof_r3_marginal.json"},
    {"npz": "prior_dof_r3_shuffle_space.npz", "sidecar": "prior_dof_r3_shuffle_space.json"},
    {"npz": "prior_dof_r3_shuffle_time.npz",  "sidecar": "prior_dof_r3_shuffle_time.json"},
    {"npz": "prior_dof_r3_separable.npz",     "sidecar": "prior_dof_r3_separable.json"}
  ],
  "documentation": {
    "path": "prior-artifact-format.md",
    "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
  }
}
```

The `sha256` above is a placeholder and is the one field of this example that
is deliberately not a real value: it is the hash of *this document*, so no
example printed inside this document can carry it. Check it against the file
you received, never against anything written here.

| field | meaning |
|---|---|
| `manifest_version` | schema version of this file. Currently `2`. A reader that does not know the version must **refuse** the file, not half-read it |
| `source` | matches the artifacts' `spec.source` |
| `resolution` | matches the artifacts' `spec.resolution` |
| `stem` | the filename prefix every artifact of this resolution shares |
| `gbif_doi` | the same DOI every sidecar in the set carries |
| `artifact_count` | how many entries `artifacts` has. Informational — **verify the names, not this number**; any six files would satisfy a count, including six copies of the full prior |
| `artifacts` | one entry per artifact, each naming its `npz` and its `sidecar`, both as bare filenames in the same directory |
| `documentation` | **this document, shipped with the release** — `path` is its bare filename in the same directory, `sha256` is the hash of its bytes. See section 2d |

Version `1` of this manifest had no `documentation` entry. That is why the
version was bumped rather than the field simply added: a reader that skipped an
absent key would authenticate exactly the release this entry exists to prevent
— artifacts with no decoding contract beside them.

### The rule

**A resolution with no manifest is not a complete artifact set. Refuse it.**

The manifest is written **last**, after every `.npz` and `.json` of that
resolution, and any stale one is deleted **first**. So its presence is the
completion record and its absence is exactly the state a build that died
part-way leaves behind.

That state is the reason this file exists, and it is genuinely undetectable
without it: a build that failed during the third control leaves
`prior_dof_r4_{full,uniform,marginal}.{npz,json}` — six files, each one
individually valid, correctly packed and correctly provenanced, with nothing
recording that two controls never landed. Globbing the directory cannot tell
that from a complete release.

**What the manifest does not do:** it does not clean up. A failed build still
leaves its partial artifacts on disk, and a consumer who never checks the
manifest is exactly as misled as before. What changed is that the truncation is
now detectable mechanically, without knowing how many controls there are
supposed to be.

To check a set: read the manifest, refuse if it is absent, refuse if
`manifest_version` is not one you know, confirm `source` and `resolution` match
what you asked for, confirm every `npz` **and** every `sidecar` it names exists
in the directory, and confirm the `documentation` file it names exists and
hashes to the recorded `sha256`. An `.npz` whose sidecar never landed is a
truncated *artifact*, and the same mid-resolution failure produces it.

---

## 2d. This document ships with the artifacts

**A copy of this file is written into the artifact directory by the build, and
the manifest accounts for it by name and by content hash.** If you received a
directory of `.npz` files, `prior-artifact-format.md` is in it, and it is the
document those files were released with.

This exists because the artifacts are not self-describing enough to be used
without it. The sidecar says `missing_value` is `1e-06` but not that filling
with `0.0` instead is a silent veto on every unobserved context; it records
`cell_week_weight` but not that a res-5 artifact therefore retains under half
of the real seasonal amplitude; it carries a licence and a DOI but not the
obligation that CC BY places on you. Shipping the matrices without this file
would leave all of that in a source repository that a consumer of a data
archive has no reason to have.

Three properties worth knowing:

1. **It is byte-identical to the repository's copy.** The build copies the
   bytes; it does not render, template or summarise them.
2. **The manifest records its `sha256`.** A name cannot detect that a *copy*
   drifted — edited in place beside the artifacts, or left truncated by a
   failed write — and a consumer decoding against a drifted contract gets
   plausible wrong numbers rather than an error. The artifacts themselves are
   not hashed in the manifest: each already carries a sidecar whose `nnz` you
   check against the matrix.
3. **It is written before the first artifact, and the manifest is still
   last.** So a build that died part-way leaves partial artifacts, this
   document, and no manifest — which is exactly the state the rule in section
   2c tells you to refuse.

If the document is absent, or its hash does not match, treat the directory the
same way you would treat a missing control: **not a complete release.**

---

## 2f. Restricted artifacts — values only where they were queried

**An artifact whose sidecar says `"restricted": true` holds values only at
the contexts its `restriction.contexts` lists. Every other context is NOT
"no evidence": the full build would hold a real estimate there and this
artifact holds none. Do not fill it with `missing_value`; refuse it.**

Why they exist: the full build stores `species x cells x 52` contexts. On a
global archive that is ~8e9 contexts at H3 resolution 3 (~2 TiB to build)
and ~4e10 at resolution 4. Every consumer of the prior in this project only
looks up the (cell, week) contexts of its own recordings, so the global
priors are built only there.

A restricted sidecar (format version 2) carries:

```text
"format_version": 2,
"restricted": true,
"restriction": {
  "domain": "query_contexts",
  "query_set_path": "/abs/path/query_set.parquet",
  "query_set_sha256": "<sha256 of that file's bytes>",
  "counts": {"query_contexts": ..., "on_grid_contexts": ..., "off_grid_contexts": ...,
             "halo_cells": ..., "observed_cells": ..., "support_points": ...,
             "query_set_rows": ...},
  "contexts": [["<cell>", <week>], ...]
}
```

| field | meaning |
|---|---|
| `domain` | always `query_contexts`: the declared domain is a list of (cell, week) contexts, not of cells |
| `query_set_path` / `query_set_sha256` | the resolution-free `(lat, lon, week)` query set the domain was derived from (`scripts/build_query_set.py`), and the hash of its bytes |
| `contexts` | **the declared domain**: every distinct (cell, week) of the query set at this artifact's resolution, sorted |
| `counts` | the build's bookkeeping, for audit |

Three facts a decoder needs:

1. **The values in the domain are the full build's values, bit for bit.**
   The build evaluates the backoff and the one smoothing pass on the query
   contexts plus their observed one-step hex neighbours and adjacent weeks
   (the smoothing "halo") and keeps only the query contexts. Every level of
   the backoff is a ratio of sums over the whole occurrence table, so
   nothing else changes a value.
2. **A domain context whose cell was never observed is off the cell axis,**
   exactly as against the full build: the domain lists it, `cells` does not.
   Treat it as you would an unknown cell of the full build.
3. **The cell axis is the on-grid query cells, not every observed cell,**
   and the matrix stores exactly `len(species_keys) x (on-grid domain
   contexts)` entries. `nnz < dense_capacity` here, and the unstored
   columns are OUT OF DOMAIN, not floored. Section 5's `load_dense` fills
   them with `missing_value` and is therefore only correct inside the
   domain; its `lookup` refuses anything outside it.

**Controls built from a restricted prior are restricted too**, carry the
same `restriction`, and are defined over the domain rather than over a full
grid: `uniform` is the mean over the domain's contexts, `marginal` each
species' mean over them, `shuffle_space` permutes cells WITHIN each week's
queried contexts, `shuffle_time` permutes weeks WITHIN each cell's queried
contexts, and `separable` is the outer product of the domain's cell and
week marginals. They are null models of the prior *as queried*, not of the
global prior.

---

## 2e. Known limitation: ISO week 53 makes week 52 the least-shrunk week

**Week 52 systematically carries more records — and therefore less shrinkage —
than its neighbours. The effect is small, real, and undocumented anywhere
else.**

The ISO calendar gives some years a week 53. The build folds it into week 52
rather than publishing a 53rd column, because a 53rd index would be treated by
the circular smoothing kernel as a real week with a seventh of the usual data
behind it. That fold is correct and it has a consequence worth stating. (Both
halves are checkable: `weeks_per_year` is 52 and the sidecar says weeks run
`1..52` inclusive — see section 2.)

**71 of every 400 Gregorian years carry an ISO week 53 — 17.75%.** So in an
archive pooling many years, week 52 absorbs its own records *plus* every
week-53 record:

```text
expected week-52 record count / expected count of any other week = 1.1775
```

The *ratio* a prior reports is unaffected: record counts appear in the
numerator and the denominator of every level of the hierarchy, and every
species' week-53 records inflate together. What changes is the **effort
proxy**. The record count is what decides how strongly a cell-week is pulled
toward its parent — `cell_week_weight` is a pseudo-count *in records*
(section 2b) — so a week with 1.18x the records is shrunk *less*. Its local
evidence share, `total / (total + cell_week_weight)`, at the default
`cell_week_weight = 10.0` and the median records-per-cell-week of each
resolution in section 2b's table:

| resolution | median records/cell-week | local share, weeks 1–51 | local share, week 52 | week 52 is this much less shrunk |
|---|---|---|---|---|
| 3 | 102 | 0.911 | 0.923 | +1.4% |
| 4 | 31 | 0.756 | 0.785 | +3.8% |
| 5 | 6 | 0.375 | 0.414 | +10.4% |
| 6 | 2 | 0.167 | 0.191 | +14.4% |

So the bias grows as the grid gets finer, because that is where shrinkage is
doing the most work. It is a difference in *how much a week trusts itself*, not
a difference in the expected value, and it is second-order compared with the
amplitude decay in section 2b — but it is a systematic, data-dependent
asymmetry between one column and its 51 neighbours.

What to do about it:

- For "when does this species peak?", ignore it. It cannot move a peak by a
  week, and no peak estimate is decided by a few percent of shrinkage.
- For anything that compares weeks to each other at fine resolution —
  smoothness of a seasonal curve, a week-over-week change, an anomaly score
  — **treat week 52 as a known outlier** rather than reading its slightly
  sharper, slightly noisier value as signal.
- If you are building your own prior from this code, the honest alternatives
  are to drop week-53 records rather than fold them (losing ~0.34% of a
  multi-year archive) or to publish 53 columns (reintroducing a column with a
  seventh of the data). Neither is clearly better; the fold is the chosen
  trade-off and this is what it costs.

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
    # A RESTRICTED artifact (section 2f) answers only its declared domain;
    # the grid's other columns are out of domain, not floored.
    if meta.get("restricted") and [cell, week] not in meta["restriction"]["contexts"]:
        raise ValueError(f"({cell!r}, {week!r}) is outside this restricted artifact's domain")
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

*This file is kept honest by executable tests, not by review. Field names,
constants and the decoding recipe are executed against a freshly saved artifact
by
`tests/prior/test_tensor.py::test_the_artifact_documentation_matches_a_freshly_saved_artifact`.
The fill regime section 2a asserts is pinned against an artifact built through
`build_from_archive` — a real pipeline output, not a hand-built fixture — by
`tests/prior/test_controls.py::test_the_documented_fill_regime_holds_on_a_PIPELINE_artifact`.
That second test exists because an earlier revision of this document claimed the
released grid was ~2% full, which was measured on a hand-built sparse fixture
and is false of everything the pipeline writes. Section 2b's retention claims
are pinned by `tests/prior/test_amplitude_retention.py`, which generates a known
seasonal and spatial signal, builds it through the CLI's own entry point and
asserts floors under the retained fraction at every default resolution. Section
2b's params-provenance limitation is pinned by
`tests/prior/test_tensor.py::test_the_documented_PARAMS_PROVENANCE_GAP_is_stated_and_is_still_real`,
which REPRODUCES the gap — so if a later round closes it, the test fails and the
limitation is deleted rather than left standing as a stale warning. Section
2c's manifest shape and its named-artifacts rule are pinned by
`tests/prior/test_build_prior.py::test_the_documented_MANIFEST_shape_matches_a_real_one`.
Section 2d's claim that this document ships beside the artifacts is pinned by
`tests/prior/test_build_prior.py::test_the_build_SHIPS_the_format_documentation_beside_the_artifacts`,
which builds through `build_from_archive` and compares the shipped bytes with
this file's. Section 2f's restricted format, its out-of-domain refusal and
its recipe line are pinned by `tests/prior/test_restricted_tensor.py`, and its
bit-for-bit equality with the full build by
`tests/prior/test_restricted_build.py::test_the_restricted_values_EQUAL_the_full_build_bit_for_bit`.
Every number in section 2e — the 71-in-400 week-53 frequency, the
1.1775 inflation and all four shrinkage rows — is recomputed from the Gregorian
calendar and the shipped default weight by
`tests/prior/test_tensor.py::test_the_documented_WEEK_53_ASYMMETRY_matches_the_calendar_and_the_code`.*
