# Task 6 — Real Encoders: Perch 2.0 and BirdNET V2.4

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.
> This extends the Phase A plan (2026-09-30-phase-a-embedding-extraction.md, merged 7ed7e41).
> All facts below are cited from the 2026-09-30 reconnaissance (ledger: progress.md; full
> reports in the deleg_e909d8c8 transcripts) or measured on the fleet.

**Goal:** Replace the melsummary stand-in with the two frozen encoders the spec names,
each validated against the official tooling's own output before any production array runs,
and cache per-recording species logits alongside embeddings so Phase B has its acoustic
posterior without a second 581 GB pass.

**Verdict from reconnaissance — the design almost falls out of the facts:**

| | Perch 2.0 | BirdNET V2.4 |
|---|---|---|
| Distribution | Kaggle Models `google/bird-vocalization-classifier/tensorFlow2/perch_v2` (Apache 2.0) | `birdnet` pip 1.1.1, model auto-downloaded from Zenodo 15050749 (code MIT, model CC BY-NC-SA — research use explicitly permitted) |
| Runtime | TF ≥ 2.20 GPU SavedModel via `perch-hoplite` | TF ProtoBuf backend via `birdnet[and-cuda]` (TFLite is CPU-only; `pb` is the GPU path) |
| Input | **32 kHz native — our corpus needs NO resampling**; 5 s windows (160k samples), window mean-subtract + peak-normalize to 0.25 (perch-hoplite does this) | 48 kHz, 3 s windows; feed `(samples, 32000)` and the package resamples internally (scipy FFT) |
| Embedding | 1536-d, mean-pool over 5 s windows (the paper's own recipe, §2.5.1) | 1024-d, signature `embeddings`, mean-pool over valid segments (Ghani 2023 + BirdSet's own code) |
| Posterior | `label` logits, 14 795 classes, calibrated by hoplite (`*0.97 - 10.0`) | `basic`/`scores` signature, 6 522 classes; BirdSet max-pools logits |
| Gotchas | GPU variant REQUIRES a GPU visible at load; `perch_v2_cpu` exists for CPU | spawns one worker per core by default — pin `n_workers=1` under Slurm; `spawn` start method needs `__main__` guards; **BirdSet excluded BirdNET as a baseline for test-data leakage** — fine as an encoder arm, but say so in the paper |

**Architecture:** one new Apptainer image (`tf.sif`, TF 2.20 GPU) serving both encoders;
`encoders.py` gains two `load_encoder` entries that wrap frame→embed→mean-pool behind the
existing `encoder(samples, sr) -> vector` interface (nothing else in the pipeline changes);
the embedding parquet gains an optional `logits` column (Phase B's posterior; 14 795 × f32 ≈
58 KB/recording ≈ 31 GB for XCL — cheap on WEKA, expensive to recompute). Model weights are
downloaded ONCE to `/shared/apps/ML/models/` (KAGGLEHUB_CACHE / BIRDNET_APP_DATA point
there); compute nodes never re-download.

**The non-negotiable gate:** a golden-reference test per encoder. Embed a fixed recording
through the OFFICIAL path (perch-hoplite's own `embed()`; birdnet's own `encode_arrays`)
and through OUR wrapper, on the same machine, and require max|diff| ≤ 1e-5. An embedding
cache built with a subtly wrong window/normalization is indistinguishable from a correct
one downstream — this is the same failure class as the wrong-encoder NotImplementedError
rule, and the reference test is the only thing standing between it and 528k recordings.

## Global constraints (in addition to Phase A's)

- Encoder wiring must not touch `embed.py` / `embed_cli.py` beyond registering encoders
  and (Task 6.3) the optional logits column — the Phase A pipeline is proven; changes
  concentrate in `encoders.py`.
- Every fleet job: `--gres=gpu:1`, `--nv`, `--bind /mnt/weka`, HOME redirect (Phase A
  facts). TF inside Apptainer additionally needs `TF_FORCE_GPU_ALLOW_GROWTH=true` (H100
  80 GB; one process per GPU, 8 tasks per node — TF preallocates by default and two TF
  processes on one GPU OOM).
- Weights live under `/shared/apps/ML/models/`, downloaded from a compute node (egress
  verified: kaggle.com 200, pypi 200, zenodo reachable). Kaggle download is anonymous for
  public models; if it demands auth, a `KAGGLE_API_TOKEN` goes in
  `/shared/apps/ML/secrets/` like the XC key — never in the repo or an image.
- The local dev Mac runs NO TensorFlow: local tests mock the TF layer (contract tests on
  framing/pooling/dtype) and the golden-reference tests are cluster-marked
  (`@pytest.mark.cluster`, skipped loudly when TF is absent). CI stays green without TF.
- BirdNET model license is CC BY-NC-SA: research use is explicitly permitted by the
  BirdNET team; the plan records this and the paper must cite it.

## Tasks

### Task 6.1 — the TF container and the weights depot
Build `tf.sif` from `tensorflow/tensorflow:2.20.0-gpu` (+ `pip install perch-hoplite
birdnet soundfile pyarrow h3`); stage to `/shared/apps/ML/containers/`. Download both
models once into `/shared/apps/ML/models/`. Verify on one GPU node: TF sees the H100,
`load_model_by_name('perch_v2')` loads, `birdnet.load("acoustic","2.4","pb")` loads,
both embed a 1 s sine. Deliverable: `scripts/cluster/build_tf_sif.sh` + a verification
transcript in the ledger. (Estimated: container build ~20 min, downloads ~5 GB.)

### Task 6.2 — wire the encoders with golden-reference validation
`encoders.py`: `perch` (frame 5 s/hop 5 s — librosa framing exactly as hoplite does it,
mean-subtract + peak 0.25 per window, `serving_default`, mean-pool `embedding` → 1536-d)
and `birdnet` (pass-through to `encode_arrays` with `n_workers=1`, masked mean-pool →
1024-d). Both raise ImportError with install instructions when TF is absent (same
loud-failure rule as before). Local tests: framing/padding/pooling contracts with a mocked
model, mutation-verified (wrong hop, dropped normalization, unmasked pool must each fail).
Cluster test: golden-reference vs official tooling ≤ 1e-5, plus determinism across two runs.

### Task 6.3 — the logits column
`embed_shard` optionally records pooled logits (Perch: mean-pool logits per hoplite;
BirdNET: max-pool per BirdSet's convention — the asymmetry is deliberate and documented);
schema versioned; absent column stays valid (melsummary has no logits). Tests: schema
roundtrip, all-failed shard, mutation on the pooling direction.

### Task 6.4 — pilot arrays and the BirdSet baseline gate
Re-run HSN test_5s + one XCM shard with each real encoder into fresh out_dirs
(`embeddings/<CONFIG>_<split>_<encoder>/`). Then the spec's hard correctness gate: linear
probe on frozen Perch embeddings over HSN, compare against published numbers (Foundation
Models review: Perch ≈ 85.6 AUROC on BirdSet mix; BirdSet's own tables for HSN). A gate
failure blocks the full XCL run — that is its job. Throughput measurement decides whether
per-file encoder calls suffice or Task 6.5 (cross-file batching) is warranted.

## Explicitly out of scope
Fusion arms (Phase B), any training, XCL full run (follows the gate), Perch ONNX arm
(validated port exists, justinchuby/Perch-onnx, max diff 7.2e-7 — recorded as the fallback
if TF-in-Apptainer misbehaves, not built now).
