# Context-conditioned bioacoustic classification: spatiotemporal occurrence priors

**Date:** 2026-09-28
**Status:** draft, awaiting user review
**Target venue:** NeurIPS Evaluations & Datasets track, ~May 2027 (2026 round closed 4–6 May 2026)
**Fallback venue:** Methods in Ecology and Evolution / Ecological Informatics (rolling)

## Problem

Every production bioacoustic classifier — BirdNET, Perch 2.0, the BirdSet baselines — treats a
recording as context-free audio. A clip goes in, a species posterior comes out, and the facts that
the clip was recorded in Nørrebro on 14 November are discarded. Yet those facts are always
available (passive recorders log time and GPS) and highly informative: a Redwing in Denmark in
November is expected; the same acoustic posterior in June is not.

Nobody has measured what that context is worth, at scale, with controls. The obvious reason is
cost: answering it properly needs a full factorial over prior construction, fusion method, encoder
and seed — hundreds of runs trained to convergence, not three hand-picked configs.

We have both halves: a national occurrence knowledge graph (birdology: DOF/DOFbasen via GBIF) and
24×8 H100 GPUs.

## Contribution

1. **A spatiotemporal occurrence prior**, released CC BY 4.0 — `P(species | H3 cell, ISO week)`,
   built from eBird EOD (global) and DOF (Denmark, dense case study), with effort correction,
   hierarchical smoothing, and explicit leakage guards.
2. **A comparison of five fusion methods** for combining that prior with a frozen acoustic
   encoder, from training-free Bayesian rescoring to FiLM conditioning.
3. **A factorial study** (~300–400 runs × 5 seeds) giving every reported number a mean and spread
   — the part that needs the GPUs.
4. **The rarity trade-off curve**: priors are expected to improve mean performance while degrading
   vagrant detection. We quantify that, rather than reporting a single headline number.

## Non-goals

- **Not a new audio dataset.** BirdSet (ICLR 2025, DBD-research-group) already covers that ground.
  We inherit its audio, splits and metrics wholesale. A Danish-subset audio benchmark would be a
  strict subset of BirdSet and redundant.
- **Not a new encoder.** Perch 2.0 and BirdNET are used frozen, as baselines to build on.
- **No audio redistribution.** Xeno-Canto recordings carry per-recording CC licences, many NC/ND.
  BirdSet already hosts the audio; we point at them and release only derived artifacts.
- **Not multi-node distributed training.** Every run is single-GPU. See "Fabric" below.

## Prior art

| Work | What it is | Relation |
|---|---|---|
| BirdSet (ICLR 2025) | Xeno-Canto focal train + 7 soundscape test sets, baselines | Substrate: we use its splits and metrics |
| Perch 2.0 (2025) | Supervised bioacoustics foundation model, 14 597 species | Frozen encoder / baseline |
| BirdNET | Incumbent field tool; has a crude location filter | Frozen encoder / baseline; its filter is a method to beat |
| BirdCLEF | Annual soundscape competition | Related task framing |

The gap all four share: context-free inference.

## Section 1 — The prior

### Definition

`P(species | cell, week)` — for each species in the label set, the probability of presence at a
given place and time of year.

### Resolution

- **Space:** H3 hexagons, resolution 4 (~1 770 km², ~25 km edge) as default. Resolutions 3 and 5
  also built, so grid coarseness is a swept axis rather than a guess. Equal-area cells matter:
  the data spans 0°–60°+ latitude, where lat/lon cells vary in area by 2×.
- **Time:** 52 ISO weeks, circular (week 52 adjacent to week 1). Not months — spring migration
  moves faster than a month-wide bin can express.

### Effort correction

Raw occurrence counts measure observers, not birds: Copenhagen and Aarhus look species-rich, west
Jutland looks empty, and both are artifacts of where birders live.

We use **target-group background**: a species' share of *all bird records* in the same cell-week,
rather than absolute counts. Observer effort appears in numerator and denominator and cancels to
first order.

**Stated limitation:** this corrects for effort *density* but not observer *preference* — rarities
are reported disproportionately. Checklist-level effort variables (duration, distance,
complete-checklist flag) would allow proper detectability modelling; they are in the eBird EBD,
not in EOD. See "Data access" below.

### Sparsity and smoothing

Most (cell, week, species) combinations are empty, and empty means "nobody looked" far more often
than "absent". Three-level hierarchical backoff:

1. cell-week estimate, shrunk toward
2. the cell's annual profile, shrunk toward
3. the species' national/continental profile

with shrinkage weight set by the observation count behind each level. Plus smoothing across the 6
adjacent hexes and ±1 week. Both bandwidths are swept axes.

### Leakage guards

Xeno-Canto is itself a GBIF publisher. If a test recording's own occurrence record is in the prior,
the prior trivially knows the answer.

1. Every Xeno-Canto-sourced occurrence is excluded from the prior.
2. Any record in the same (cell, week) as a test recording from the same contributor is dropped.

Both enforced in code with a dedicated test, and stated explicitly in the paper. This is the first
thing a sharp reviewer will probe.

### Two instantiations

- **Global** (EOD, ~2×10⁹ records) — covers BirdSet's American and Hawaiian test sets.
- **Denmark** (DOF + EOD's Danish share) — ~2 orders of magnitude denser per km². Enables the
  question: how does prior quality scale with occurrence density? A result in its own right.

### Data access

| Source | Route | Licence | Lead time |
|---|---|---|---|
| eBird EOD | GBIF dataset `4fa7b334-ce0d-4e88-aaae-2e0c138d049e` | CC BY 4.0 | none |
| eBird EBD | Cornell request form | restricted redistribution | days–weeks |
| DOF/DOFbasen | GBIF dataset `95db4db8-f762-11e1-a439-00145eb45e9a` | CC BY | none |

EOD is the dependency; the EBD request is filed in parallel as a **bonus, not a prerequisite**.
Both GBIF routes reuse the existing `src/birdology/ingestion/gbif_dof.py` client.

### Prerequisite: re-pull DOF proportionally

The current `output/birdology.ttl` **cannot be used to build the prior**. Measured:

```
observations:          100 117
observed species:          508  (232 singletons; only 169 with ≥50 obs)
locations:               8 058
observers:               1 613
years:            2025: 24 996, 2026: 25 202
months:  01–07: 4166 each, 08: 4239, 09: 4299, 10–12: 4166 each
```

That month histogram is flat to the record because `build_graph.py` batches DOF by (month, year)
to give every calendar month an equal share — correct for a browsable graph, fatal here. It has
normalised out precisely the temporal signal the paper is about. Building a prior from this file
would measure our own sampling quota, and a reviewer who plots the histogram sinks the paper.

**Action:** a separate proportional extract over a long window (DOFbasen goes back decades),
preserving unquota'd counts. The app keeps its balanced graph for browsing; the paper gets its own
extract. Two artifacts, one source. This is rate-limited by GBIF, not GPUs, so it starts first and
runs while everything else is built.

**Consequences of the current 2-year window** (to be designed around, not discovered in review):
no year-held-out split is possible; 8 058 locations across Denmark puts the spatial prior at
region×week resolution, not site×week.

## Section 2 — Fusion methods

A frozen encoder produces an acoustic posterior over species for a 5 s window; the prior supplies
a probability vector for that recording's (cell, week).

| Method | Cost | Description |
|---|---|---|
| Bayesian rescoring | training-free | posterior × prior ÷ training marginal, temperature τ on prior exponent |
| Learned logit offset | 1–10³ params | add λ·log prior to logits; λ global, then per-species |
| Late-fusion head | minutes | small MLP over [acoustic embedding ‖ prior vector], on cached embeddings |
| FiLM conditioning | GPU-hours | feature-wise modulation of encoder activations by a learned (cell, week) embedding |
| Context token | GPU-hours | context embedding as an extra transformer input token |

Bayesian rescoring matters disproportionately: it runs on a frozen model with no gradient steps,
so it is the method a practitioner could adopt tomorrow, and it is roughly what BirdNET's location
filter does crudely. Beating that cleanly is a concrete contribution.

FiLM and context-token are the only arms requiring encoder weight updates — they are what justifies
real GPU time.

### Controls

Run in **every** cell. This is where credibility lives.

1. **Uniform prior** — no context.
2. **Species-marginal-frequency prior** — no space, no time. *The* check: is the gain just label
   frequency exploitation? A reviewer will demand this one.
3. **Spatially-shuffled prior** — cells permuted.
4. **Time-shuffled prior** — weeks permuted.

A gain that survives all four is a real spatiotemporal effect.

## Section 3 — The factorial

The sweep's selling point is **error bars, not scale**. Most bioacoustics papers report one run per
config. Every number here carries mean ± std over 5 seeds, and every grid cell is trained to
convergence.

### Phase A — embedding extraction

Both frozen encoders over the whole BirdSet corpus, once; cache the embeddings. Embarrassingly
parallel: shard recordings across 192 GPUs, zero communication.

This is the real hardware stress test — 192 GPUs decoding audio, running inference and writing
storage for hours. Expected pain point: 24 nodes pulling a multi-hundred-GB corpus off one Synology
NFS box. Mitigation: stage each shard to node-local `/raid/scratch` first (5.3 GB/s measured vs
317 MB/s NFS write / 239 MB/s read), compute, write only compact embeddings back to shared storage.
**The NFS behaviour under 24-node load is itself a deliverable** for the cluster acceptance side.

### Phase B — the sweep

Cheap arms (rescoring, logit offset, MLP head) run on cached embeddings in CPU-minutes, so the
prior-resolution / smoothing-bandwidth / temperature axes get swept densely almost for free.

Expensive arms are genuine fine-tunes. The GPU budget below counts **only** the two
encoder-updating methods (FiLM, context token); the three cheap arms ride along on cached
embeddings and are not part of this multiplication:

```
2 encoders × 2 encoder-updating methods × 3 H3 resolutions × 2 prior sources × 5 seeds = 120 runs
+ control conditions re-run on the same grid                        ≈ 300–400 runs total
```

At ~3 GPU-hours each: roughly 1.5 days of the full machine.

### Scheduling

Slurm job array, `--gres=gpu:1`, 192 concurrent tasks, one results row per task — the same pattern
as the existing `gpu-scale-sweep.csv` harness. Deliberately 1 GPU per task so a flaky node kills
one cell, not the sweep.

### Fabric

Every phase is single-GPU-per-task or single-node, so **none of this blocks on the unfitted NDR
optics**. corespark's InfiniBand is not yet up (192 optics unfitted, 2 ports/node showing "signal
not detected", no subnet-manager decision; multi-node NCCL currently falls back to TCP over the
management NIC). Intra-node is at hardware limits (NV18 mesh, 469–480 GB/s bus bandwidth) and
irrelevant to us. Designing the sweep as embarrassingly parallel is what makes it runnable today —
and, conveniently, it is also the experiment nobody without 192 GPUs can run.

## Section 4 — Evaluation protocol

### Splits

Inherit BirdSet's exactly: XCL/XCM focal recordings for training, held-out soundscape test sets
(HSN, NES, PER, POW, SSW, SNE, UHH) for evaluation. Not renegotiating them is the point — identical
splits give direct comparability with the published ICLR 2025 table. Any deviation is flagged
loudly.

**The train/test shift is the mechanism.** Focal recordings (one bird, close mic) train; soundscapes
(passive recorders, overlapping species, noise) test. Context should help most exactly there,
because a soundscape's location and date are known and reliable while its audio is hard. This
yields a falsifiable prediction: the effect is larger on soundscape tests than on held-out focal
recordings. Reported either way.

Note BirdSet's soundscapes are all Americas + Hawaii — a Denmark prior applies to none of them.
Hence: global EOD prior for BirdSet evaluation, DOF for the density case study.

### Metrics

cmAP primary (BirdSet's headline, so numbers line up), plus AUROC and top-1. All stratified three
ways:

1. by species frequency decile,
2. by whether the species is expected in that (cell, week) under the prior,
3. by test set.

Stratum (1) is where the vagrant trade-off becomes visible.

### Statistics

5 seeds per cell; mean ± std. Paired tests across shared seeds when claiming method A beats method
B. No bolded-largest-number tables without a test behind them. Multiple-comparison correction on
headline claims, given a few hundred configs — better pre-empted than raised in review.

### Reproduction gate (hard)

Before any prior is added, reproduce BirdSet's published baselines within tolerance on their splits
with their checkpoints. If we cannot, nothing downstream is trustworthy. Doubles as the cluster's
correctness check: results disagreeing with published ones indicate a stack problem, not a finding.

### Released artifacts

- Prior tensors: global + Denmark × 3 resolutions, CC BY 4.0
- Prior construction code, including leakage guards and their tests
- Embedding caches
- All checkpoints
- Full results table, every cell
- `fetch.py` for anything not redistributable

## Section 5 — Cluster execution

**Cluster:** `ssh ubuntu@100.69.23.11` (slurm-login-01.infra.faic.dk), partition `gpu`,
24 × DGX (dgx160–171, 176–187), 8×H100 80GB each = 192 GPUs, 224 CPU + 2 TB RAM per node,
MaxTime 2-00:00:00, driver 595.91.07. Queue empty.

### Storage (resolved)

Jobs do not run as the login user: login is `uid=1000(ubuntu)` group `hpcbuild`; inside a job it is
`uid=1000(nvidia)` group `nvidia`. NFS maps by id, so from a compute node `/srv/home`,
`/shared/apps/cache` and `/raid` are all unwritable and `$HOME` is unreadable (Slurm chdirs to
`/tmp`).

`/shared` is **not** itself an NFS mount — nodes mount `/shared/apps` and `/srv/home` separately, so
a directory created at `/shared/ML` lands on the login node's local disk and is invisible to jobs.

**Resolution:** `/shared/apps/ML`, owned `ubuntu:ubuntu`, mode 2775 (setgid), created and verified
writable from a job on dgx160. No sysadmin ticket needed.

| Path | Role | Measured |
|---|---|---|
| `/shared/apps/ML` | durable: code, priors, checkpoints, results | 317 MB/s w, 239 MB/s r |
| `/raid/scratch` (node-local, 28 TB) | staging: audio shards, temp | 5.3 GB/s |
| `/tmp` | job scratch | node-local, wiped |

### PyTorch (open)

No PyTorch module is installed: `/shared/apps/modules/all/ai/PyTorch/` holds no modulefile and
`/shared/apps/software/PyTorch` does not exist. Build 2241
(`2.9.1-foss-2025b-CUDA-13.4.1`, on dgx160) **compiled clean** — `pip install` exited 0 after 40
minutes — then EasyBuild ran the test suite for 9h23m and aborted on 144 failures against a max of
10, so nothing installed. Staging prefix was `/raid/scratch/nvidia-eb-2241/...`, node-local.

Two routes, both to be taken:

1. **Container (immediate):** NGC PyTorch via enroot or Apptainer. Both binaries verified present
   on dgx160 — the earlier inventory suggesting enroot exists only on dgx160–168 must be
   re-checked across all 24 nodes before relying on it.
2. **Module (clean stack):** re-run the build with the test step relaxed or known-failing tests
   excluded. The compile is proven; the tests are the blocker.

Also confirmed on compute nodes: `libcuda.so.1` present, PyPI reachable (so a venv on
`/shared/apps/ML` is viable). No torchaudio / torchvision / librosa / soundfile easyconfigs exist
anywhere — FFmpeg and LLVM do. Survivable: mel spectrograms are `torch.stft` plus a filterbank
matmul, and doing them on-GPU beats torchaudio-on-CPU. But the audio decode path is a decision, not
an assumption.

### Xeno-Canto API

v2 is retired (404 as of 2025-10-10); v3 requires a per-account key passed as `key=` query param
(currently 401). Free, but a prerequisite. Only needed for metadata reconciliation, since BirdSet
hosts the audio.

### Phase order

1. Storage (done) + PyTorch route settled + XC key + EOD/DOF proportional pull started
2. Reproduce BirdSet baselines on one node — **hard correctness gate**
3. Build priors (global + Denmark × 3 resolutions), leakage tests green
4. Phase A: embedding extraction, 192-way, NFS behaviour measured
5. Phase B: sweep as job array
6. Analysis, artifact packaging, paper

## Open questions

- Audio decode path: on-GPU STFT vs FFmpeg-based CPU decode — decide during Phase A design.
- Whether the EBD arrives in time to add a detectability-modelling arm.
- Denmark case study depth: how far back the proportional DOF pull should reach.
