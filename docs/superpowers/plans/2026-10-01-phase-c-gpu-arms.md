# Phase C GPU Arms Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** FiLM and context-token conditioning on BirdSet's open ConvNext/AST checkpoints, trained against the EOD occurrence prior, with twin-baseline honesty and a hard reproduction gate.

**Architecture:** New `src/birdology/phase_c/` package (PyTorch + transformers). Thin wrapper modules apply conditioning around unmodified HF checkpoints; a context provider maps (cell, week) prior vectors onto each model's own class axis by eBird code. Every conditioned run has an unconditioned twin with identical schedule/seed; headline = conditioned − twin, paired per seed.

**Tech Stack:** torch, transformers, torchaudio (pytorch.sif on the fleet), pyarrow; `phase_b.metrics` / `phase_b.sweep.write_rows` reused for eval and results.

**Spec:** docs/superpowers/specs/2026-10-01-phase-c-gpu-arms-design.md

## Global Constraints

- Tests never touch network, WEKA, or HF downloads: tiny randomly-initialized models built from the same transformers config classes (`ConvNextConfig`, `ASTConfig`), in-memory/tmpdir fixtures only.
- Class-axis joins by eBird code via `model.config.id2label` (measured: values ARE eBird codes, e.g. `ostric2`), never positional. ConvNext-XCL and AST-XCL have their own 9,736-class axes; the prior maps through the code→GBIF bridge.
- Checkpoints (pinned, measured 2026-10-01): `DBD-research-group/ConvNeXT-Base-BirdSet-XCL`, `DBD-research-group/AST-BirdSet-XCL`. Loaded with pinned `revision=` recorded in every results row.
- ConvNext frontend (from the model card, verbatim): mono mel spectrogram, sr 32000, n_fft 1024, hop 320, n_mels 128, power 2.0, PowerToDB top_db 80, normalize mean −4.268 std 4.569, input shape (B, 1, 128, 334) for 5 s.
- FiLM is identity-initialized (γ≡1, β≡0): wrapped model must equal the unwrapped checkpoint bit-for-bit at step 0 (executable gate). The token arm has NO identity gate (prepended tokens renormalize attention — physically no bit-exact path); its honesty is the twin.
- Missing context = zero vector + present-flag 0 → FiLM identity / learned null token. Never silently substitute a prior.
- Twins: identical optimizer, schedule, seed, data order; conditioning modules absent. Uniform-prior control ≡ twin (documented, not re-run).
- Results rows carry provenance (git SHA, checkpoint revision, prior DOI, manifest path, torch/CUDA versions) and REFUSE to write without it (phase_b contract).
- Seeds (0,1,2,3,4); full-cell granularity; `--gres=gpu:1`; all shard I/O staged to `/raid/scratch` first.
- Python 3.11+, `uv run pytest -q` must stay green; commit per task.

## Review Focus

1. A recording whose eBird code exists in the manifest but NOT in the model's id2label (XCL checkpoint has 9,736 classes; our manifest spans 9,734+) — training must skip-and-count loudly, not KeyError mid-epoch. → test in Task 4.
2. A prior vector that is all-FLOOR (cell with no eBird data) is NOT missing context — present-flag 1, FiLM may learn from it; only absent (cell,week) rows get flag 0. Conflating the two silently erases the sparse-region signal. → test in Task 1.
3. AST eval on a segment whose filepath appears twice in the metadata (duplicate labels) — same join hazard as phase_b; must refuse, not double-count. → test in Task 6.
4. Early-stop on POW-val when ALL POW rows are null-context: the val metric must still be finite and comparable between twin and conditioned run (both see null context on POW) — a NaN/empty val set must abort, not silently train to max epochs. → test in Task 5.
5. Checkpoint resume after preemption (Slurm requeue) must restore model+optimizer+epoch+RNG state; a resumed run must not re-randomize crop order (seed-derived, epoch-keyed). → test in Task 5.

---

### Task 1: Context provider — prior vector on the model's class axis

**Files:**
- Create: `src/birdology/phase_c/context.py`
- Test: `tests/phase_c/test_context.py`

**Interfaces:**
- Consumes: `phase_b.cache.class_index` (code→column), prior `.npz` tensors via `birdology.prior.tensor` lookup (cell, week → per-GBIF-key probs), code→GBIF bridge dict.
- Produces:
  `build_context_matrix(prior, class_codes: list[str], code_to_gbif: dict[str,int]) -> ContextProvider` and
  `ContextProvider.vector(cell: str|None, week: int|None) -> tuple[np.ndarray, float]` returning (normalized log-prior on the model class axis, present_flag). Shape = (n_classes,), float32. Absent (cell,week) or None → (zeros, 0.0). All-FLOOR but present → (normalized floor vector, 1.0).
  `ContextEncoder(n_classes, d_ctx=256)` — torch MLP `Linear(n_classes+1, 512) → GELU → Linear(512, d_ctx)`; input = concat(vector, [flag]).

- [ ] **Step 1: Write failing tests** — vector on model axis by code (shuffled class list ≠ shuffled output: pin non-positional), absent-vs-floor distinction (Review Focus 2: all-FLOOR cell → flag 1.0 and finite layer-normed values; missing cell → zeros + 0.0), week-53→week-52 fold (same ruling as phase_b), unknown code in class_codes → that column gets log(floor) not KeyError, ContextEncoder output shape/dtype and determinism under fixed torch seed.
- [ ] **Step 2: Run, verify ImportError/FAIL.**
- [ ] **Step 3: Implement.** Layer-norm = (x − mean)/std over the log vector, eps 1e-6.
- [ ] **Step 4: `uv run pytest tests/phase_c/test_context.py -q` → PASS; full suite green.**
- [ ] **Step 5: Commit** `feat(phase_c): context provider — prior vectors on the model class axis`.

### Task 2: FiLM wrappers with the step-0 identity gate

**Files:**
- Create: `src/birdology/phase_c/film.py`
- Test: `tests/phase_c/test_film.py`

**Interfaces:**
- Consumes: `ContextEncoder` from Task 1; HF `ConvNextForImageClassification` / `ASTForAudioClassification` instances (tests build tiny ones from configs: ConvNext hidden_sizes=[8,16], AST hidden_size=16, 2 layers, num_labels=5).
- Produces: `FiLMConvNext(model, context_encoder)` and `FiLMAST(model, context_encoder)`, both `forward(inputs, context, present) -> logits`. Per-site `(γ, β)` heads: `Linear(d_ctx, 2*C)` per ConvNext stage (4 sites) / per every-3rd AST block; γ-head weight AND bias zero-init with output computed as `1 + γ_raw` (identity), β zero-init.

- [ ] **Step 1: Write failing tests** — (a) identity gate: wrapped(x, ctx, flag=1) == unwrapped(x) to `torch.equal` at init, both models; (b) the gate FAILS when γ-heads are randomly initialized (mutation pinned as a test asserting non-equality after `nn.init.normal_`); (c) gradients flow to γ/β heads and to backbone params under loss.backward(); (d) flag=0 forces identity even after heads have trained (null path); (e) wrapper never mutates the wrapped model's own modules (upstream untouched — state_dict of inner model unchanged after a training step on the wrapper, backbone grads flow through optimizer param groups instead).
- [ ] **Step 2: FAIL.** 
- [ ] **Step 3: Implement.** Hook-based injection after each stage output (forward hooks on `model.convnext.encoder.stages[i]` / `model.audio_spectrogram_transformer.encoder.layer[i]`), channel-wise broadcast.
- [ ] **Step 4: PASS; full suite green.**
- [ ] **Step 5: Commit** `feat(phase_c): FiLM wrappers, identity-gated at step 0`.

### Task 3: Context-token wrapper (AST only)

**Files:**
- Create: `src/birdology/phase_c/token.py`
- Test: `tests/phase_c/test_token.py`

**Interfaces:**
- Consumes: `ContextEncoder`; tiny AST as in Task 2.
- Produces: `TokenAST(model, context_encoder)` — `forward(inputs, context, present) -> logits`; projects d_ctx → hidden_size, prepends ONE token after the embedding layer (position: index 2, after CLS+distillation tokens); `null_token` = learned `nn.Parameter(hidden_size)` used when present==0.

- [ ] **Step 1: Write failing tests** — token count increases by exactly 1 in the encoder input (asserted via a forward hook capturing shapes); present=0 batches use null_token (pin: output differs between two distinct contexts when present=1, identical when present=0); mixed batches (flag 1 and 0 in one batch) route per-row; gradient reaches null_token only from flag-0 rows; upstream model modules unmutated (as Task 2e). NO identity gate — a test documents the step-0 delta is nonzero and small (assert not equal + max|Δlogits| < 1.0 on random init), referencing the spec's twin argument.
- [ ] **Step 2: FAIL.** **Step 3: Implement** (wrap `embeddings` forward, concat before encoder). **Step 4: PASS + suite.** 
- [ ] **Step 5: Commit** `feat(phase_c): AST context token with learned null token`.

### Task 4: Training data pipeline — XCM crops with context

**Files:**
- Create: `src/birdology/phase_c/data.py`
- Test: `tests/phase_c/test_data.py`

**Interfaces:**
- Consumes: XCM manifest parquet (filepath, ebird_code, cell, week, split — real phase_a schema), decoded-audio provider (injected callable `load_audio(filepath) -> np.ndarray @32kHz`; production impl uses the phase_a tar/basename machinery, tests inject synthetic sine arrays), `ContextProvider` (Task 1), ConvNext frontend per the Global Constraints card values.
- Produces: `CropDataset(manifest, load_audio, context_provider, label2id: dict[str,int], crop_s=5.0, seed, epoch)` — torch Dataset yielding `{inputs, context, present, label}`; `convnext_frontend(wave: Tensor) -> Tensor(1,128,334)`; `make_label_map(id2label) -> dict[str,int]`.

- [ ] **Step 1: Write failing tests** — frontend output shape (1,128,334) for 160,000 samples and finite after normalize; crop determinism: (seed, epoch, index) triple fully determines the crop offset, and epoch change changes it (Review Focus 5 dependency); manifest row whose ebird_code is NOT in label2id is EXCLUDED with a counted, logged skip — dataset length shrinks, a `skipped_codes` attribute names them, no KeyError (Review Focus 1); rows with null cell/week yield present=0 zeros-context; short recordings (<5 s) zero-pad right (document: matches BirdSet's 5 s clip handling).
- [ ] **Step 2: FAIL.** **Step 3: Implement.** **Step 4: PASS + suite.**
- [ ] **Step 5: Commit** `feat(phase_c): crop dataset with context join and loud label-gap skips`.

### Task 5: Training loop, twins, resume

**Files:**
- Create: `src/birdology/phase_c/train.py`
- Test: `tests/phase_c/test_train.py`

**Interfaces:**
- Consumes: wrappers (Tasks 2–3), `CropDataset` (Task 4), `phase_b.metrics.cmap` for val.
- Produces: `TrainConfig(arm: str, seed: int, lr=1e-4, backbone_lr_scale=0.1, max_epochs=2, patience=1, batch_size=64)`; `train_arm(model_or_wrapper, train_ds, val_ds, cfg, ckpt_dir) -> TrainResult(val_cmap_history, best_state_path, epochs_ran, resumed: bool)`. `arm` ∈ {"film","token","twin"}; twin receives the BARE model and the identical cfg/seed. Checkpoint file carries model+optimizer+epoch+torch RNG states.

- [ ] **Step 1: Write failing tests** (tiny models, 32-row synthetic datasets) — two optimizer param groups with backbone at `lr*0.1` (asserted from optimizer.param_groups); twin and conditioned run with same seed see identical batch order (capture first-epoch indices via a recording sampler); early-stop triggers after `patience` non-improving epochs; val set yielding NaN/empty cmAP ABORTS with a named error, never trains to max silently (Review Focus 4); resume: kill after epoch 0, reload, final weights bit-equal to an uninterrupted run (Review Focus 5 — RNG state restored, crop offsets epoch-keyed so the data path is identical by construction).
- [ ] **Step 2: FAIL.** **Step 3: Implement** (plain torch loop; AdamW; no lightning). **Step 4: PASS + suite.**
- [ ] **Step 5: Commit** `feat(phase_c): training loop with twins, loud val, bit-exact resume`.

### Task 6: Repro gate, eval runner, results rows, sbatch

**Files:**
- Create: `scripts/cluster/phase_c_repro_gate.py`, `src/birdology/phase_c/evaluate.py`, `scripts/phase_c_run.py`, `scripts/cluster/phase_c_train.sbatch`
- Test: `tests/phase_c/test_evaluate.py`, `tests/phase_c/test_repro_gate_logic.py`

**Interfaces:**
- Consumes: everything above; `phase_b.metrics`; test-set metadata parquets (labels joined BY FILEPATH); published LT rows hardcoded with citation comment: ConvNext {POW .340, PER .215, NES .318, UHH .264, HSN .427, SSW .363, SNE .292}, AST {POW .320, PER .178, NES .286, UHH .236, HSN .415, SSW .341, SNE .268} (VERIFY against arXiv:2403.10380v3 Table 5 before implementing — if the measured table differs, the measured values win and the brief's numbers are corrected in the commit message).
- Produces: `evaluate_model(model, eval_ds, class_codes) -> scores df`; gate CLI printing `PHASE_C_REPRO {model} {test_set} ours=X published=Y delta=Z` + `PHASE_C_REPRO_OK|FAIL`, exit 0/1/2, zero-rows refusal, ±0.05 tolerance; `phase_c_run.py` — one (arm, model, resolution, seed) cell per invocation, twin included, rows via `phase_b.sweep.write_rows`-compatible schema + columns `arm, model_id, checkpoint_revision, torch_version, cuda_version, context_source`.

- [ ] **Step 1: Write failing tests** — eval label join refuses duplicate filepaths (Review Focus 3); gate FAILS loud on zero eval rows and on NaN cmAP; gate tolerance boundary (|Δ|=0.05 passes, 0.0501 fails); results row refused when checkpoint_revision missing; sbatch is lint-checked for the three measured silent-failure constraints (`--gres=gpu:1`, `/raid/scratch` staging, weka bind) by a test that greps the file.
- [ ] **Step 2: FAIL.** **Step 3: Implement.** **Step 4: PASS + suite.**
- [ ] **Step 5: Commit** `feat(phase_c): repro gate, eval runner, run CLI, fleet sbatch`.

---

## Execution notes (not tasks)

- Fleet order after merge: (1) repro gate on frozen checkpoints (blocks everything), (2) FiLM identity gate on the real checkpoints, (3) one full-length pilot per combo + one long-audio eval shard, (4) the 195-run array.
- The published-row VERIFY in Task 6 and the AST frontend check (whether `ASTFeatureExtractor` or the BirdSet mel pipeline matches the checkpoint's training — read the model card and birdset configs at implementation time) are the two measure-don't-trust items left deliberately to implementers with loud instructions.
