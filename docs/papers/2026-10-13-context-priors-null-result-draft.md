# Spatiotemporal occurrence priors do not improve strong bird-sound classifiers

**Draft, 2026-10-13.** Results from the birdology Phase B and Phase C sweeps on the
Gimle (corespark) cluster. Every number below comes from the results parquets cited in
§7. Items marked **TODO** were not verified in the session that wrote this draft.

## Abstract

Bird-sound classifiers ignore where and when a recording was made, although both are
known for almost every field recording. We test whether a spatiotemporal occurrence
prior, `P(species | H3 cell, ISO week)` built from 2×10⁹ eBird records, improves
recognition on the six dated BirdSet soundscape test sets. We compare five ways of
using it: training-free Bayesian rescoring, learned logit offsets and a late-fusion
head on frozen BirdNET and Perch 2.0 embeddings, plus FiLM and context-token
conditioning trained into the open BirdSet ConvNeXt and AST checkpoints. Each method
runs against uniform, species-marginal, space-shuffled and time-shuffled controls,
over five seeds and three H3 resolutions. No method beats its no-context baseline by
a meaningful margin. The best training-free result is +0.003 cmAP (BirdNET, best of
the grid). Conditioned fine-tuning ranges from −0.017 to +0.004 against an
unconditioned twin trained identically. FiLM on ConvNeXt is consistently and
significantly worse (−0.014 to −0.017, p < 0.001). The prior carries real
information: it beats a spatially scrambled copy of itself by up to 0.1 cmAP. A
strong acoustic model simply recovers that information on its own, and the spatial
part of the prior is what hurts. We release the prior, the controls and the
evaluation harness.

## 1. Question

Does knowing *where and when* a recording was made improve species recognition by a
modern classifier, beyond what the audio already says? The intuition is strong: a
recording from a Danish garden in January should not score a summer migrant highly.
BirdNET already ships a crude location filter. Neither the size of the effect nor the
best way to inject context has been measured under controls that separate genuine
spatiotemporal signal from label-frequency exploitation.

## 2. Setup

**Prior.** `P(species | cell, week)` from the eBird Observation Dataset via GBIF
(download DOI 10.15468/dl.smqz7f), at H3 resolutions 3, 4 and 5, with effort
correction, hierarchical smoothing and leakage guards. The guards drop records from
the test recording's own contributor in the same (cell, week). Variants from the same
build: `full`, `marginal` (species frequency only), `shuffle_space` (cells permuted),
`shuffle_time` (weeks permuted) and `uniform`.

**Evaluation.** BirdSet's soundscape test sets HSN, NES, PER, SSW, SNE and UHH, at
segment level (test_5s). NBP and POW are excluded because their filenames carry no
recording date. The metric is cmAP, computed sklearn-exactly as in BirdSet; AUROC and
top-1 were also recorded. All six sets are in the Americas or Hawaii.

**Phase B: frozen encoders.** BirdNET and Perch 2.0 posteriors and embeddings,
combined with the prior by four methods:
- Bayesian rescoring (posterior × prior^τ ÷ training marginal);
- a global logit offset;
- a per-species logit offset;
- an MLP late-fusion head.

Each method was run at five seeds, and rescoring over τ ∈ {0.25, 0.5, 1.0, 2.0}.

**Phase C: conditioned fine-tuning.** We used the DBD-research-group BirdSet XCL
checkpoints (ConvNeXt-Base and AST), fine-tuned on the XCM subset (409 test-set
species) for at most 2 epochs, with early stopping on POW cmAP. Three
method × backbone combinations were tested:
- FiLM × ConvNeXt (per-stage γ/β);
- FiLM × AST (every 3rd block);
- context token × AST.

The context is the EOD prior vector at the recording's (cell, week) plus a
1-bit "context present" flag. FiLM starts at identity. Every conditioned cell has an
**unconditioned twin**: the same checkpoint, schedule, seed and data, with no
conditioning module. The headline number is *conditioned − twin*, paired per seed
and test set. Grid: 3 combos × 3 resolutions × 5 seeds, plus the three non-uniform
controls (uniform ≡ twin by construction), plus 10 twins (190 runs, 1,140 rows).

**Training data, two variants.** (a) XCM restricted to dated, located recordings
(85,885 rows). (b) The same data plus the 3,910 recordings lacking a date or location,
with null context, so the conditioned models also learn a no-context path
(89,795 rows). Variant (b) retrains the 45 conditioned cells and the 10 twins
(55 runs, 330 rows).

## 3. Results

### 3.1 Frozen encoders (Phase B)

Gain of the full prior over the uniform prior, in cmAP averaged over the six test
sets, at the best (resolution, τ) per encoder × method:

| encoder | method | full − uniform | full − marginal | full − shuffle_space | full − shuffle_time | uniform cmAP |
|---|---|---|---|---|---|---|
| BirdNET | rescoring (r3, τ=0.25) | **+0.0028** | +0.0028 | +0.024 | +0.004 | 0.442 |
| BirdNET | offset, global (r3) | +0.0021 | +0.0021 | +0.008 | +0.002 | 0.442 |
| BirdNET | offset, per species (r3) | +0.0020 | +0.0020 | +0.009 | +0.002 | 0.442 |
| Perch 2.0 | rescoring (r5, τ=0.25) | **−0.137** | −0.137 | +0.048 | +0.024 | 0.393 |
| Perch 2.0 | offset, per species (r5) | −0.155 | −0.155 | +0.035 | +0.024 | 0.393 |
| Perch 2.0 | offset, global (r5) | −0.157 | −0.157 | +0.038 | +0.024 | 0.393 |

These are *best-of-grid* figures, so they are optimistic. For BirdNET the gain is
concentrated on SSW (+0.008) and NES (+0.006), and HSN and SNE are unchanged. For
Perch the prior hurts every test set except HSN, by up to −0.34 on SSW. The MLP
late-fusion head produced degenerate cmAP (0.03–0.05 against 0.44 for the other
methods), so it is excluded. **TODO:** diagnose the MLP arm before submission.

### 3.2 Conditioned fine-tuning (Phase C)

Conditioned − twin, cmAP averaged over the six test sets, mean over 5 seeds (paired
t-test p in brackets):

| combo | r | (a) dated only | (b) + undated |
|---|---|---|---|
| FiLM × ConvNeXt | 3 | −0.0140 (p=1e-4) | −0.0157 (p=2e-4) |
| | 4 | −0.0144 (p=1e-4) | −0.0166 (p=2e-4) |
| | 5 | −0.0141 (p=3e-4) | −0.0161 (p=4e-4) |
| FiLM × AST | 3 | −0.0001 (0.98) | +0.0042 (0.42) |
| | 4 | −0.0029 (0.55) | +0.0043 (0.39) |
| | 5 | −0.0015 (0.73) | +0.0025 (0.63) |
| token × AST | 3 | −0.0090 (0.11) | +0.0018 (0.71) |
| | 4 | −0.0067 (0.18) | +0.0013 (0.77) |
| | 5 | −0.0047 (0.28) | +0.0019 (0.70) |

Twin cmAP for reference, variant (a), mean over seeds:

| | HSN | NES | PER | SNE | SSW | UHH |
|---|---|---|---|---|---|---|
| ConvNeXt | 0.480 | 0.335 | 0.159 | 0.291 | 0.343 | 0.265 |
| AST | 0.363 | 0.287 | 0.143 | 0.204 | 0.273 | 0.198 |

In variant (b) the AST twin itself loses 0.012 on HSN and 0.008 on SSW. Part of the
AST arms' move from slightly negative to slightly positive is therefore a weaker
baseline. Conditioned AST cmAP changes by +0.000 (FiLM) and +0.003 (token) between
the variants.

### 3.3 Controls (Phase C, variant a)

| combo | full − marginal | full − shuffle_space | full − shuffle_time |
|---|---|---|---|
| FiLM × ConvNeXt | −0.0075 to −0.0079 | −0.0051 to −0.0075 | −0.0007 to +0.0004 |
| FiLM × AST | −0.0074 to −0.0099 | −0.0062 to −0.0077 | −0.0003 to +0.0004 |
| token × AST | −0.0080 to −0.0001 | −0.0082 to −0.0000 | −0.0018 to +0.0033 |

Ranges are over r3/r4/r5. The full prior is *worse* than both the marginal and the
space-shuffled prior, and equal to the time-shuffled prior. Its spatial component
hurts conditioned models, and its temporal component is not used.

### 3.4 The one consistent positive

Both AST arms gain on NES at every resolution in both variants: +0.007 to +0.014
in variant (a) and +0.023 to +0.028 in variant (b). FiLM × ConvNeXt gains at most
+0.005 on NES in either variant. No other test set shows a consistent gain for any
arm. **TODO:** check whether NES differs in prior coverage or species composition.

## 4. Interpretation

1. **The prior is informative but redundant.** Beating its own space-shuffled copy
   by up to 0.1 cmAP (Perch rescoring) shows real location signal. Failing to beat
   *no prior* shows that strong encoders already capture what that signal would add
   on these test sets.
2. **The spatial component hurts conditioned training.** XCM training locations are
   global focal-recording sites; the test sets are six specific regions in the
   Americas. A conditioned model can learn "this cell ⇒ these species" as a
   shortcut that does not transfer. Including undated recordings, so the null-context
   path is trained, does not remove the effect. This is a hypothesis, not a measured
   mechanism.
3. **The calibration of the base model matters.** The same rescoring that is
   harmless on BirdNET costs Perch 0.14 cmAP. Multiplying a sharp posterior by a
   smoothed prior can only demote the correct species, and does so when the prior
   under-weights it.

## 5. Limitations

- All six test sets are in the Americas or Hawaii. Dense national priors, such as
  Denmark's DOF, were never tested where they apply. See §6.
- Phase C used one training schedule (≤ 2 epochs, backbone at 0.1× LR). Longer
  training could change the conditioned arms.
- The 1,140 + 330 rows span three code revisions (55fd29b, e59b2f3, 38f46a6 and
  4345483). The changes between them were to paths and job scripts.
  **TODO:** confirm, by diff, that no model or training code changed.
- **TODO:** the reproduction gate (frozen checkpoints within ±0.05 of the published
  BirdSet LT row) passed in Phase C planning, but this draft has not re-checked it.

## 6. A dense national prior on local recordings (DKX, preliminary)

The original motivation was better recognition of Danish birds, and no BirdSet test
set is Danish. **DKX** (`scripts/build_dkx_testset.py`) is built from Danish
Xeno-canto recordings uploaded on or after 2025-01-01 and graded A–C. Recordings
already in BirdSet's XCL are excluded (none overlapped), so is one corrupt file,
and so are recordings whose species is unidentified or cannot be mapped to an
eBird code. That leaves 622 recordings, 181 species and 36 recordists; the labels
are the main species plus the background species.

The prior is the DOF prior rebuilt **from pre-2025 observations only**
(`data/prior/dof_pre2025`: 34.2 M records kept, 4.0 M dropped). This is a temporal
hold-out: no observation from a test recording's own period can leak in, and it
mirrors deployment. The unit is the whole recording, so cmAP is not comparable
with §3.

BirdNET 2.4 on 618 scoreable recordings and 35 recordists. The 95% confidence
intervals come from a bootstrap that resamples recordists:

| rule | cmAP | top-1 | Δ top-1 vs raw [95% CI] |
|---|---|---|---|
| raw BirdNET | 0.739 | 0.864 | — |
| Phase B rescoring `log s + τ(log p − log m)`, τ=1 | 0.634 | 0.649 | −0.215 [−0.321, −0.156] |
| national filter: drop species DOF never recorded in Denmark | 0.726 | 0.879 | **+0.015 [+0.007, +0.027]** |
| `log s + τ log p`, r4, τ = 0.25 / 0.5 / 1 | 0.712 / 0.673 / 0.634 | 0.790 / 0.704 / 0.608 | −0.074 / −0.160 / −0.256 |
| same at r3 | 0.709 / 0.684 / 0.641 | 0.790 / 0.722 / 0.618 | −0.074 / −0.142 / −0.246 |

**Findings.**
- Only the coarse national species filter helps.
- Every use of the local (cell, week) probability hurts, even at τ = 0.25 and at
  r3, although the prior beats its own shuffled controls by up to 0.16.

**Why the Phase B rule fails nationally.**
- 6,039 of BirdNET's 6,522 classes are absent from the DOF species axis. For them
  p = m = FLOOR, so they are never penalised, while Danish species are scaled
  by p/m. Examples: Tundra Swan becomes Plumbeous Pigeon, and Water Rail becomes
  New Holland Honeyeater.
- Dividing by the marginal also inflates nationally rare species: Common
  Goldeneye becomes Spotted Crake.

**Why local weighting fails.** The remaining errors come from sparse single cells.
For example, a Pink-footed Goose recording becomes Grey Heron, because DOF has few
goose records in that particular cell and week.

**The full Phase B grid on DKX** (jobs 9556/9557; 180 rows per prior, 182 scored
classes; offsets fitted on the XCL training cache) gives the same picture:
- Rescoring with the full prior loses to its own uniform control at every τ.
  For BirdNET at r4, cmAP is 0.709 / 0.671 / 0.632 / 0.560 against uniform's
  0.717 / 0.698 / 0.688 / 0.688, for τ = 0.25 / 0.5 / 1 / 2.
- The fitted offsets are the only arms that don't lose. With the full prior,
  `offset_per_species` reaches cmAP 0.731–0.736 (r3–r5) against 0.731 for
  uniform, and `offset_global` reaches 0.729–0.732 against 0.730. That is a gain
  of at most +0.005, and it only matches raw BirdNET (0.736 on all 622
  recordings).
- In those offset arms, the full prior beats shuffle_space, which beats
  shuffle_time (0.732 > 0.718 > 0.707 for `offset_global` at r4). So the prior
  carries real signal, but too little to help an encoder that is already strong.
- The full-period DOF prior, which overlaps the test period, gives BirdNET
  numbers within 0.03 cmAP of these. Temporal leakage is therefore not what decides
  the result.

**Caveats.**
- These are focal recordings, chosen by recordists who favour notable birds, and
  raw BirdNET is already accurate on them. Danish passive soundscapes were not
  available.
- Perch results are not interpretable yet: it collapses under every rule (raw
  logits plus FLOOR handling). **TODO.**
- The national filter's cmAP dip comes from 7 recordings whose species are missing
  from the eBird↔GBIF bridge.

**Implication.** For local recognition, use the DOF prior as a species list
(presence in Denmark) and leave the scores alone. Its fine-grained probabilities
are better suited to flagging detections that are unusual for a given place and
week than to rescoring.

## 7. Provenance

- Phase B: `/srv/home/rapnic/birdology/data/phase_b/results_eod/*.parquet`
  (1,255 rows).
- Phase C (a): `/srv/home/rapnic/birdology/data/phase_c/results/*.parquet`
  (1,140 rows; job 8948 plus earlier r4 runs).
- Phase C (b): `/srv/home/rapnic/birdology/data/phase_c/keep_undated/results/*.parquet`
  (330 rows; job 9188).
- Comparison script: `/srv/home/rapnic/birdology/data/phase_c/keep_undated/compare_grids.py`.
- Design: `docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md`
  and `docs/superpowers/specs/2026-10-01-phase-c-gpu-arms-design.md`.
