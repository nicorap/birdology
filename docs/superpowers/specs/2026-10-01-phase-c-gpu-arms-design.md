# Phase C — GPU arms: FiLM and context-token conditioning on open backbones

**Date:** 2026-10-01
**Status:** approved in design review (chat, 2026-10-01); spec written for planning
**Parent spec:** docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md (§2–§4)
**Depends on:** Phase A caches + manifests (done), Phase B metrics/controls/results contract (merged 6a059ea), EOD prior tensors (build pending fleet return)

## Decision record (what changed vs the parent spec, and why)

1. **Backbones are BirdSet's open PyTorch baselines — ConvNext and AST — not
   Perch/BirdNET.** Measured/cited facts forcing this: Perch's training
   parameters are unavailable (BirdSet paper, §4.2 discussion); BirdNET was
   excluded from BirdSet for test-data leakage; both exist for us only as
   frozen TF inference graphs. The frozen SOTA encoders carry the cheap arms
   (Phase B); the open baselines carry the deep conditioning. A Perch-FiLM
   feasibility spike is PARKED, deliberately: it reports an answer, not code.
2. **The grid is 3 method×model combos, not 2×2:** FiLM×ConvNext, FiLM×AST,
   token×AST. ConvNext has no token sequence; its "context token" analogue IS
   FiLM. Reported honestly rather than padded.
3. **GPU arms condition on the EOD prior only.** Every BirdSet test soundscape
   is Americas/Hawaii; the DOF prior answers FLOOR everywhere there, and
   conditioning on a constant measures nothing. The parent spec's own split
   ("global EOD for BirdSet evaluation, DOF for the density case study")
   already says this; the ×2-prior-sources multiplier in its §3 does not apply
   to the GPU arms. Denmark/density stays with the cheap arms.
4. **Every cell gets an unconditioned twin.** Fine-tuning from a checkpoint
   conflates "conditioning helps" with "fine-tuning helps". The headline
   number is conditioned − twin, paired per seed, identical schedule and seed.
   The uniform-prior control collapses to the twin by construction
   (documented, not re-run as a separate arm).

## 1. Architecture

New package `src/birdology/phase_c/`. PyTorch + transformers, pytorch.sif
(already on the fleet). Upstream model code is never edited — conditioning is
applied by thin wrapper modules around HF checkpoints.

- **Checkpoints:** `DBD-research-group/ConvNeXT-Base-BirdSet-XCL` and the
  DBD-research-group AST XCL checkpoint (exact repo id pinned at plan time by
  reading the HF org — measure, don't trust). Class axes are each model's own
  (XCL label set); eval alignment goes through `phase_b.cache.class_index` /
  `gather_scores` by eBird code, never positionally.
- **FiLM:** per-stage `(γ, β) = MLP(context)`, applied channel-wise after each
  of ConvNext's 4 stages; for AST after every 3rd transformer block.
  Identity-initialized (γ≡1, β≡0): at step 0 the wrapped model reproduces the
  checkpoint bit-for-bit on the same input — an executable gate (test), not an
  intention.
- **Context token (AST only):** one learned linear projection of the context
  vector, prepended to the patch sequence after the CLS token; a learned null
  token substitutes when context is absent.
- **Context input:** EOD prior vector at the recording's (cell, week), mapped
  to the model's class axis via the code→GBIF bridge, log(floored), layer-norm,
  concatenated with a 1-bit context-present flag. Missing context (no date/
  coords: 7.5% of XCL rows; all of POW) → zero vector + flag 0 → FiLM emits
  identity, token path emits the null token. Graceful degradation is therefore
  a trained path, not an untested branch, and null-context-at-eval doubles as
  a free no-context ablation.
- **Trainability:** conditioning modules at full LR; backbone at 0.1× LR.
  Twins use the identical optimizer/schedule/seed with conditioning modules
  absent.

## 2. Data flow

- **Train set:** the XCM subset (~90k focal recordings, the 409 test-set
  species — BirdSet's own MT subset definition) decoded from WEKA tarballs via
  the phase_a shard machinery (basename resolver, failures-json witnesses).
  5 s random crops, label from `ebird_code` (single-label focal), context from
  the XCM r4 manifest join (filepath → cell/week; the r3/r5 axes re-join from
  their manifests). The XCM train manifests and the POW-val manifest are
  built with `--keep-undated` (phase_a_manifest.py): rows lacking date/coords
  are RETAINED with null cell/week — they are what trains the flag-0
  null-context path §1 relies on, and POW (all undated) would otherwise
  yield an empty validation manifest.
- **Staging:** every GPU task stages its shards to `/raid/scratch` before
  decode (measured: 5.3 GB/s local vs ~300 MB/s shared at contention).
- **Frontends:** each model's own preprocessing from its HF config (ConvNext
  mel spectrogram; AST patch embedding). We do not re-implement frontends —
  share-the-code-path, the golden-gate lesson.
- **Eval:** the six evaluable soundscape test sets {HSN, NES, PER, SSW, SNE,
  UHH}, segment-level (test_5s), labels from the metadata parquet joined BY
  FILEPATH (phase_b seam, already fixed), metrics via `phase_b.metrics`
  (sklearn-exact cmAP). Conditioned evals read CONTEXT-JOINED eval tables
  produced by `scripts/phase_c_build_eval_ctx.py` from the dated test-set
  metadata (H3 cell + ISO week derived through phase_a's own functions) —
  the raw BirdSet metadata parquets carry no cell/week columns, and a
  conditioned eval fed one would silently run the whole set null-context
  (refused by name in `build_eval_dataset(require_context=True)`). POW
  serves as validation only; its missing dates put it on the null-context
  path, which is the degradation mode we want measured anyway.
- **Val/early-stop:** POW-val cmAP, patience 1 epoch, max 2 epochs.

## 3. Reproduction gate (hard, like Phase B's)

Before any fine-tune is reported: the FROZEN checkpoint run through OUR
pipeline (decode → frontend → eval → cmAP) must land within ±0.05 absolute
cmAP of the published LT row per test set (ConvNext and AST rows, Table 5,
arXiv:2403.10380v3 — the same derivation already triple-verified for Perch).
Same non-vacuous-gate contract as scripts/cluster/repro_gate.py: encoder-token
validation, zero-gated-pairs refusal, NaN loud, exit 0/1/2. The FiLM identity
gate (step-0 bit-equality with the unwrapped checkpoint) is part of the same
script.

## 4. The grid and budget

```
runs  = 3 combos × 3 resolutions × 5 seeds                    =  45 conditioned
      + 3 combos × 5 seeds (twins; resolution-independent)    =  15 twins
      + 3 combos × 3 res × 5 seeds × 3 controls               = 135 controls
        (marginal, space-shuffled, time-shuffled; uniform ≡ twin)
      ≈ 195 runs × ~3 GPU-h ≈ 600 GPU-h ≈ <4 h of the 192-GPU fleet
```

The ~600 GPU-h holds because each cell trains ONCE and loops evaluation
over all six test sets (train-once/eval-six — the checkpoint dir is keyed
without test_set); retraining per test set would ×6 the training budget.

One GPU per task (`--gres=gpu:1`), one results row per (run, test set)
through `phase_b.sweep.write_rows`' provenance contract (git SHA,
checkpoint revision, prior DOI, prior variant, manifest paths; refusal on
absence). Controls reuse
`rescoring.control_prior` tensors — the context input is the only thing that
changes between a conditioned run and its control twin.

- Seeds: (0,1,2,3,4), full-cell granularity (the Phase B ruling carries over).
- Paired statistics: conditioned−twin paired per seed at ANALYSIS time over
  the per-seed rows (keyed model, resolution, seed); a
  `phase_b.sweep.paired_delta` adapter is a ledgered follow-up — phase_c
  rows do not satisfy its phase_b-shaped contract.
- Stratified reporting (frequency decile, expected-under-prior) via the same
  phase_b strata columns.

## 5. Failure modes designed against (from this project's own ledger)

- Positional class-axis joins — forbidden; by-code via class_index everywhere.
- Pilot blind spots — the pilot runs ONE cell of every combo at full length
  plus one deliberately long-audio eval shard before the array launches.
- Silent stack heterogeneity — pytorch.sif pinned for every run; torch/CUDA
  versions recorded in each results row; a rebuilt image re-runs the repro
  gate before new rows join the table.
- Vacuous gates — zero-gated-pairs refusal inherited; the FiLM identity gate
  must FAIL when γ,β are randomly initialized (tested mutation).
- Fabricated spreads — the per-seed rows carry `seed`; degenerate-spread
  checks (seeds_degenerate's logic) run at ANALYSIS time over the per-seed
  rows. A twin's five seeds genuinely differ (SGD noise), so no degenerate
  path is expected, and a zero std raises a flag rather than passing
  silently.

## 6. Deliverables

- `src/birdology/phase_c/`: context encoder, FiLM/token wrappers, training
  loop, eval runner — all TDD'd with in-memory fixtures (tiny random
  checkpoints with the same config classes; no network/WEKA in tests).
- `scripts/cluster/phase_c_repro_gate.py`, `scripts/cluster/phase_c_train.sbatch`.
- Results parquet rows compatible with the Phase B table for joint analysis.
- The parked Perch-FiLM spike brief (one page, not scheduled).

## Out of scope

- Perch/BirdNET fine-tuning (parked spike).
- DOF-conditioned GPU runs (density case study stays with cheap arms).
- Multi-node training, new encoders, new datasets, from-scratch training.
