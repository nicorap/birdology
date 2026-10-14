# Spatiotemporal occurrence priors do not improve strong bird-sound classifiers

**Draft, 2026-10-13.** Results from the birdology Phase B and Phase C sweeps on the
Gimle (corespark) cluster. Every number below comes from the results files cited in
§8. Items marked **TODO** were not verified in the session that wrote this draft.
§7 (Phase D) was added on 2026-10-14.

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
evaluation harness. What does help on Danish recordings is local training data: a small head on
BirdNET's own embeddings, trained on European Xeno-canto and fused with BirdNET, lifts
top-1 from 0.863 to 0.899 on clean recordings and from 0.677 to 0.868 under 0 dB of
urban noise.

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
- **Renamed taxa were dropped from every XCL- and XCM-derived set.** The Xeno-canto
  date lookup (`birdology.phase_a.xc_dates`) queries by the eBird code's *current*
  scientific name. Xeno-canto does not use that name for taxa renamed or split
  since BirdSet was built. Examples: `brnowl` resolves to *Tyto furcata*,
  `lirplo` to *Thinornis dubius*, and `hergul` has no name at all. Those
  recordings came back "undated" and were dropped. In the European part of XCL
  that is 4,380 recordings, almost all Barn Owl, Whimbrel, Little Ringed Plover,
  Goshawk, Herring Gull, Little Bittern, redpolls, House Martin, Nutcracker and
  Dotterel. Phases A–C trained and evaluated without those species' XCL/XCM
  recordings. Phase D (§7) works around it; the date lookup itself is not
  fixed yet.

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
- The national filter's cmAP dip comes from 9 recordings of species it removes.
  Seven are Danish rarities absent from DOF's pre-2025 species list: Cetti's
  Warbler (3), Song Sparrow (2), Rock Ptarmigan (1) and Swinhoe's Snipe (1).
  The other two are Little Ringed Plover, a common breeder: its old eBird code
  bridges to a GBIF key, *Thinornis dubius*, that DOF's records do not use. A
  hard species filter therefore costs exactly the detections birders care
  most about, and needs a name-level bridge.

**Implication.** For local recognition, use the DOF prior as a species list
(presence in Denmark) and leave the scores alone. Its fine-grained probabilities
are better suited to flagging detections that are unusual for a given place and
week than to rescoring.

## 7. What does help: a Danish head on BirdNET's embeddings (Phase D)

If the prior cannot add local knowledge, local *training data* might. Phase D
(`birdology.phase_d`) trains a small classifier head on BirdNET 2.4's frozen
1024-d embeddings of European Xeno-canto recordings, then fuses it with
BirdNET:

    fused = log s_BirdNET + α · log q_head

**Data.**
- **Training pool:** XCL recordings inside a European box (lat 46–71.5°,
  lon −11–32°).
- **Excluded:** DKX's 36 recordists, so no recordist appears in both training
  and test.
- **Labels:** remapped onto BirdNET's axis by scientific name
  (`phase_d.taxonomy`). Without the remap, 4,538 European recordings in 42
  renamed codes were dropped.
- **Fill:** the 4,380 recordings the XCL manifest had dropped as "undated" (§5)
  were embedded separately.
- **Size:** 130,704 labelled recordings. The newest 10% by Xeno-canto ID (ID ≥
  800,970, about 2023 onward) are held out for validation. Those are the uploads
  BirdNET 2.4 is least likely to have trained on: on them, raw BirdNET's top-1 is
  only 0.768.
- **Head output:** the 389 species with at least five training recordings, 348
  of them recorded in Denmark. They cover 99.2% of DKX's true species.

**Model selection, on validation only.** The search covered linear and 2-layer
MLP heads, four weight-decay/learning-rate pairs, and
α ∈ {0.25, 0.5, 1, 2, 4}. The chosen fusion is an MLP with weight decay 0.01,
learning rate 1e-4 and α = 1. It reaches validation top-1 0.849, against raw
BirdNET's 0.768. DKX was scored once, after selection.

**Clean DKX** (622 recordings, 36 recordists; CIs resample recordists and are
paired):

| scorer | cmAP | top-1 | Δ top-1 vs raw [95% CI] | Δ vs filter [95% CI] |
|---|---|---|---|---|
| raw BirdNET | 0.734 | 0.863 | — | −0.011 [−0.023, −0.003] |
| BirdNET + DOF filter (§6) | 0.716 | 0.875 | +0.011 [+0.003, +0.023] | — |
| head alone | 0.679 | 0.860 | −0.003 [−0.060, +0.022] | −0.014 [−0.074, +0.014] |
| **head + BirdNET (fusion)** | **0.753** | **0.899** | +0.035 [−0.007, +0.058] | +0.024 [−0.020, +0.045] |

**Noisy DKX.**
- **Construction:** the same 622 recordings with bird-free urban and outdoor
  noise mixed in at fixed signal-to-noise ratios (`phase_d.noise`).
- **Noise classes:** ESC-50 engine, car horn, train, siren, aircraft,
  helicopter, church bells, footsteps, wind and rain.
- **Test noise:** fold 5 only. 12 of its 80 clips were dropped because BirdNET
  heard a bird in them.
- **SNR:** measured over the whole recording, so during the calls themselves it
  is higher than nominal.

| SNR | raw BirdNET | BirdNET + filter | fusion | fusion Δ vs raw [95% CI] | fusion Δ vs filter [95% CI] |
|---|---|---|---|---|---|
| clean | 0.863 | 0.875 | 0.899 | +0.035 [−0.007, +0.058] | +0.024 [−0.020, +0.045] |
| 20 dB | 0.836 | 0.862 | 0.897 | +0.061 [+0.010, +0.086] | +0.035 [−0.008, +0.058] |
| 10 dB | 0.786 | 0.842 | 0.894 | +0.108 [+0.056, +0.130] | +0.051 [+0.019, +0.074] |
| 5 dB | 0.741 | 0.838 | 0.873 | +0.132 [+0.066, +0.169] | +0.035 [+0.000, +0.058] |
| 0 dB | 0.677 | 0.820 | 0.868 | +0.191 [+0.115, +0.224] | +0.048 [+0.014, +0.071] |

**Findings.**
- **Clean recordings:** the fused head improves BirdNET by 3.5 points top-1 and
  0.019 cmAP. The top-1 interval still includes zero.
- **Noise:** BirdNET loses 19 points between clean and 0 dB; the fused head loses
  3. From 20 dB down, the gain over raw BirdNET is significant. At 10 dB and
  0 dB it also beats the Danish species filter; at 5 dB that interval touches
  zero. The head never saw noisy audio.
- **Training on noise does not add much:** a second head (v5) was trained on the
  same pool plus a noisy copy of it: every recording re-embedded with ESC-50
  folds 1–4 mixed in at a random 0–20 dB (`phase_d.embed_noisy_cli`; disjoint
  from the fold-5 test noise). The search chose the same configuration. Against
  v4, paired: clean +0.000 [−0.013, +0.011], 20 dB +0.002, 10 dB +0.003,
  5 dB +0.008 [−0.004, +0.026], 0 dB +0.003 [−0.006, +0.013]. That is at most
  a handful of recordings, and no interval excludes zero. The robustness comes
  from the head itself, not from seeing noise in training.
- **The filter's share:** much of BirdNET's collapse in noise is it drifting to
  non-European species, which the filter alone recovers. The head still adds
  2–5 points on top.
- **Remaining errors:** Danish rarities the head pulls toward common species
  (Pallas's Leaf Warbler, Greenish Warbler, Red-rumped Swallow, Black-winged
  Kite), and Hooded Crow called Carrion Crow. The training pool is western
  European, where Carrion Crow dominates.

**Where BirdNET fails on DKX** (`scripts/phase_d_dkx_errors.py`). Of raw
BirdNET's 85 errors:
- 25 predict a background species that really is in the recording.
- 25 predict a species never recorded in Denmark.
- Only 4 confuse two species of the same genus.
- In 27, the true species is not even in BirdNET's top 5.

Hard errors concentrate where BirdNET is unsure: 76% of recordings whose top
score is below 0.25 are wrong, against 4.4% when it is above 0.8. Quality grade
barely matters (A 9.7%, C 10.6%). The failures are recognition failures, not
fine distinctions between close relatives, which is why a better-trained
decision layer on the same embeddings helps.

**Caveats.**
- The noise is synthetic, and DKX is still focal recordings.
- 36 recordists make the clean-data interval wide.
- ESC-50 is CC BY-NC.
- The repo implementation (`scripts/phase_d_train_head.py`, commit de70931)
  reproduces the first, scratch run (clean 0.902, 0 dB 0.873) to within the
  seed difference.

**Export** (`scripts/phase_d_export.py`, `phase_d.export_tf`). BirdNET 2.4 and
the head are packaged as one TensorFlow model: a SavedModel, plus a 68 MB TFLite
file for BirdNET-Analyzer's `--classifier`. It keeps BirdNET's input (3 s chunks
at 48 kHz) and BirdNET's 6,522 labels, with English and Danish label files. Its
output satisfies sigmoid(logits) = s · q^α, so detection thresholds keep their
meaning. The exported head is v5, the noise-augmented one (`data/phase_d/export_v5`,
`scripts/cluster/phase_d_export.sbatch`, job 11306); v4's export gave the same
numbers to within 0.002. Checked on DKX audio (622 recordings):

| check | result |
|---|---|
| backbone parity with the cached BirdNET scores | top-1 agreement 0.979 |
| per chunk, max over chunks (BirdNET-Analyzer's mode) | raw 0.859 → fused 0.875, +0.016 [−0.005, +0.033] |
| whole recording (scores max, embeddings mean; `recording` signature) | raw 0.859 → fused **0.904**, +0.045 [+0.010, +0.070] |
| TFLite against SavedModel | logits within 0.04, same arg-max |

The head was trained on whole-recording (mean-pooled) embeddings, so it helps
most when applied that way. A per-chunk detector would need a head trained on
per-chunk embeddings. **TODO.**

**The same head on BirdNET+ V3, for the phone app.** BirdNET Live (the iPhone
and Android app) runs BirdNET+ V3.0-preview3.1 (ONNX, 32 kHz, 9,789 species),
not 2.4, and scores a file as 3 s windows, max over windows. A head was
trained on V3's per-window embeddings (`scripts/phase_d_v3_train_head.py`; each
European training recording contributes its 3 windows where V3 scores the
label highest; 115,487 recordings, 379 species; linear, α = 2) and scored on DKX
the app's way (`scripts/phase_d_v3_eval.py`; 618 recordings, eurdot and lirplo
missing from an axis; CIs resample recordists, paired against the app default):

| top-1 | clean | 20 dB | 10 dB | 5 dB | 0 dB |
|---|---|---|---|---|---|
| V3 + geo filter (the app as shipped) | 0.850 | 0.840 | 0.843 | 0.850 | 0.846 |
| V3 raw | 0.856 | 0.840 | 0.846 | 0.845 | 0.838 |
| BirdNET 2.4 + head v5 (whole-recording) | 0.900 | 0.900 | 0.898 | 0.883 | 0.874 |
| **V3 + head** | **0.898** | **0.900** | 0.890 | 0.874 | 0.871 |
| V3 + head + geo filter | 0.882 | 0.888 | 0.880 | 0.867 | 0.862 |

V3 + head against the app: +0.049 [+0.031, +0.085] clean, +0.060 [+0.039,
+0.083] at 20 dB, +0.047 [+0.033, +0.079] at 10 dB; at 5 and 0 dB the CIs touch
zero (+0.024 [−0.002, +0.057], +0.024 [−0.005, +0.049]). cmAP 0.813 against
0.757 for 2.4 + head. V3 alone hardly degrades with noise, so the head's gain
there is smaller than on 2.4. The geo filter costs 1.6 points on top of the head
(it drops the true species in 2.8% of recordings). Caveat: V3's training data
may include 2025 Xeno-canto uploads, i.e. DKX.

Export (`phase_d.export_onnx`, `scripts/phase_d_v3_export.py`): the head is
appended to the app's ONNX graph with the same input and output names, so the
app needs only a new `model_config.json`. Its scores are sigmoid(log s + α log q
+ b), which ranks exactly like the fused score above; b = 2.94 is set on
validation so the app's 0.35 threshold passes as many recordings as V3 alone
(DKX clean: 0.979 against 0.964 for the app as shipped). A first export that
instead rescaled each window by its best head probability kept the threshold
but lost most of the gain (0.858): it lets a window where the head is unsure
compete with a clear call. Parity on DKX audio: original outputs identical,
fused scores within 5e-7 of NumPy.

## 8. Provenance

- Phase B: `/srv/home/rapnic/birdology/data/phase_b/results_eod/*.parquet`
  (1,255 rows).
- Phase C (a): `/srv/home/rapnic/birdology/data/phase_c/results/*.parquet`
  (1,140 rows; job 8948 plus earlier r4 runs).
- Phase C (b): `/srv/home/rapnic/birdology/data/phase_c/keep_undated/results/*.parquet`
  (330 rows; job 9188).
- Comparison script: `/srv/home/rapnic/birdology/data/phase_c/keep_undated/compare_grids.py`.
- Design: `docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md`
  and `docs/superpowers/specs/2026-10-01-phase-c-gpu-arms-design.md`.
- Phase D (repo code, commit de70931): head `data/phase_d/dk_head_v4/` (`head.pt`,
  `val_grid.csv`, `dkx_scores.csv`); noise curve `data/phase_d/noise_curve.csv`;
  noisy DKX `data/birdset/DKXN/` and `data/phase_a/embeddings/DKXN_snr*_birdnet`;
  XCL fill `data/phase_a/XCL_eu_undated_fill_manifest.parquet` and
  `data/phase_a/embeddings/XCL_eu_undated_fill_birdnet`; hard errors
  `data/phase_d/dkx_hard_errors.csv`.
- Phase D noise-augmented head (job 11181): `data/phase_d/dk_head_v5/`, trained
  with `data/phase_a/embeddings/XCL_eu_noisy_birdnet` (job 11180); v5 against v4
  in `data/phase_d/noise_curve_heads.csv` (job 11307).
- Phase D V3 (BirdNET Live app): window embeddings `data/phase_d/v3/` (DKX, DKXN,
  XCL_eu; jobs 11347, 11387); head `data/phase_d/v3_head_v2/` and app files
  `data/phase_d/app_export_v2/` (job 11485); DKX scores `data/phase_d/v3_dkx_head_v2.csv`.
