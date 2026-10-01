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

1. **A spatiotemporal occurrence prior** — `P(species | H3 cell, ISO week)`, global (EOD)
   released CC BY 4.0; Denmark (DOF) released CC BY-NC 4.0 (MEASURED 2026-10-01: the GBIF
   download API returns BY-NC for the DOF archive, not the CC BY this table first claimed —
   licence follows the source),
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

**Stated limitation — the probability floor erases the absence distinction at scale.**
A non-zero floor (1e-6) keeps an unobserved cell-week from collapsing to a hard zero, so
"nobody looked" stays distinguishable from "looked and did not find". That distinction holds
only while the global denominator stays below `1/floor`. Above it — i.e. once a region
accumulates more than ~1e6 bird records, which is the normal GBIF regime — a species
*surveyed and not found* becomes bit-identical to a species *never recorded anywhere*.
Verified empirically: at 1e6 records in a cell, a species with one sighting elsewhere scores
exactly 1e-6 in a surveyed week where it was absent, and an unrecorded species also scores
exactly 1e-6. The floor is still necessary — a hard zero would make the prior veto a correct
detection outright — but the paper must state the threshold rather than claim the
distinction holds everywhere. Report it as a known boundary of the method, and if a reviewer
asks what the prior says about a surveyed absence, the honest answer is "below `1/floor`
records, less than the cell profile; above it, indistinguishable from unrecorded".

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
| DOF/DOFbasen | GBIF dataset `95db4db8-f762-11e1-a439-00145eb45e9a` | CC BY-NC 4.0 (measured from download 10.15468/dl.3j4czw; was wrongly listed CC BY) | none |

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
parallel: shard recordings across the working fleet (160 GPUs today, 192 once dgx184–187 are
remediated), zero communication.

This is the real hardware stress test — the whole fleet decoding audio, running inference and writing
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

Slurm job array, `--gres=gpu:1`, up to 160 concurrent tasks (192 after remediation), one results
row per task — the same pattern
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
for evaluation. BirdSet's seven test sets are HSN, NBP, NES, PER, SSW, SNE, UHH (this spec first
wrote POW here — POW is BirdSet's VALIDATION set, Table 5). Prior-fusion evaluation runs on SIX:
{HSN, NES, PER, SSW, SNE, UHH}. NBP is excluded — MEASURED 2026-10-01: all 539 NBP test_5s
filenames carry no recording date, and a prior conditioned on ISO week is undefined without one
(POW's 4,560 filenames have the same defect). Not renegotiating the splits is the point — identical
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

**Fleet state 2026-09-30:** 21 nodes up, **4 down — dgx169, dgx177, dgx179, dgx182** — so
**168 GPUs usable**, not the 160 recorded earlier. The remediation this section previously
anticipated for dgx184–187 has landed; all four are up. Node availability drifts, so re-read
it with `sinfo -N` rather than trusting this line.

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
| `/mnt/weka` | **shared, fast — the Phase A corpus belongs here** | 41.6 GB/s r, 27.9 GB/s w (dgx171 only, 2026-09-30) |
| `/shared/apps/ML` | durable: code, priors, checkpoints, results | 0.54 GB/s r, 0.51 GB/s w |
| `/raid/scratch` (node-local, 28 TB) | staging: audio shards, temp | 101.6 GB/s r, 48.1 GB/s w |
| `/tmp` | job scratch | node-local, wiped |

### WEKA (new 2026-09-30, changes the Phase A plan)

A 1.0 PB WEKA filesystem (`10.11.5.11/delta-fs`) is mounted at `/mnt/weka`. **Today it is on
dgx171 ONLY** — verified node by node across all 21 up nodes; the other 20 report no mount, and
the login node has none. It is expected fleet-wide soon.

`/mnt/weka` arrives `root:root` 775, so jobs (running as `nvidia`) cannot write to it.
`/mnt/weka/ML` was created `nvidia:nvidia` mode 2775, mirroring the `/shared/apps/ML`
convention, using the passwordless sudo available inside a job.

Measured on dgx171 with fio 3.41, all `--direct=1`, 16 jobs, 1 MB blocks. Every figure was
re-run at a **640 GB working set** (16 × 40 G) that cannot be served from the node's 2 TB of
RAM, and all four reproduced within ~1% of the 32 GB run — so these are storage numbers, not
page-cache numbers:

| | WEKA | NFS (`/shared/apps`) | node-local raid0 |
|---|---|---|---|
| read, 16 jobs | **41.6 GB/s** | 0.54 GB/s | 101.6 GB/s |
| write, 16 jobs | **27.9 GB/s** | 0.51 GB/s | 48.1 GB/s |
| 4k randread | 789k IOPS | 54k IOPS | 3.37M IOPS |

`/raid` is raid0 over 8 NVMe (`/proc/mdstat`), i.e. ~12.7 GB/s per drive — consistent, and the
reason the node-local figure is genuinely that high rather than a measurement artifact.

**Consequence for Phase A — the staging design becomes unnecessary.** The stage-to-`/raid`
mitigation exists solely because NFS is flat at ~535 MB/s from 1 reader to 16, with latency
going 31 ms → 507 ms: a saturated box that cannot feed 24 nodes. WEKA scales instead
(8 → 41.6 GB/s from 1 to 16 jobs) and is shared, so shards need no per-node copy. Node-local
RAID stays ~2.4× faster in raw throughput, so staging remains an option for a
re-read-heavy arm, but it is no longer required.

Two things this does NOT establish, both of which matter before committing the plan:

- **Aggregate behaviour under 24-node load is unmeasured.** This is one client on one node.
  The multi-node number is the one that decides Phase A, and it can only be taken once the
  mount is fleet-wide.
- **The "NFS behaviour under 24-node load" deliverable** (below) is obsoleted rather than
  satisfied if the corpus moves to WEKA. It needs restating as a WEKA scaling measurement,
  not quietly dropped — it was a cluster-acceptance deliverable, not a project one.

### Container runtime (resolved: Apptainer, 20 of 24 nodes)

**Decision: Apptainer SIF, not pyxis/enroot.** Built and verified.

`/shared/apps/ML/containers/pytorch.sif` — 9.8 GB, from `nvcr.io/nvidia/pytorch:25.08-py3`,
built by `code/build_sif.sh` (job 2359, 4m07s). Verified inside the image on an H100:

```
torch 2.8.0a0+34c6371d24.nv25.08   cuda 13.0   avail True
bf16 8192^3 matmul: 786.3 TFLOP/s
on-GPU stft ok (64, 513, 501)      numpy 1.26.4   scipy 1.15.3
```

The on-GPU STFT check settles the audio decode question: `torch.stft` in the container works, so
no torchaudio/librosa module is needed for mel spectrograms.

**Why not pyxis/enroot.** Pyxis derives its enroot paths from the passwd `HOME` (`/home/ubuntu`),
which is unreadable inside a job, and it ignores `--export`, so every `--container-image` start
dies on `mkdir: Permission denied`. Fixing it requires editing `/etc/enroot/enroot.conf`
fleet-wide — a privileged change, deliberately not made. Apptainer honours `APPTAINER_CACHEDIR` /
`APPTAINER_TMPDIR` / `HOME` from the environment, so it needs no such change. Every job script
must set those three to node-local paths.

**Fleet status** (job 2362, all 24 nodes, `code/fleet_check.sh`):

| Nodes | apptainer | Result |
|---|---|---|
| dgx160, 162–171, 176–183 (19) | system binary | OK, 716–745 TFLOP/s bf16 4096³ — tight spread, no slow node |
| dgx161 | system binary | task launch timed out (23 of 24 started); transient, re-test |
| dgx184–187 (4) | module only | **FAILED** |

dgx184–187 have no system apptainer, enroot or pyxis. The module binary is not setuid
(`-rwxrwxr-x`), so it cannot mount a SIF directly and falls back to sandbox extraction, which
needs unprivileged user namespaces:

```
FATAL: while extracting pytorch.sif: root filesystem extraction failed:
       Failed to create container process: Operation not permitted
```

They also lack `squashfuse`. Remediation is a sysadmin task (install the apptainer package +
`squashfuse`, or set `kernel.apparmor_restrict_unprivileged_userns=0`) — **not a blocker**:
20 nodes × 8 = **160 GPUs** is the working fleet, and the sweep is embarrassingly parallel, so it
simply uses fewer array slots. Treat 184–187 as a remediation ticket to file, not a dependency.

**Observed I/O ceiling, for free.** The fleet check took 10m33s, almost entirely spent with 24
nodes pulling the same 9.8 GB SIF over one NFS link (~235 GB through a 239 MB/s pipe). This is
exactly the Phase A bottleneck the spec predicts, arriving early and for free. It confirms the
mitigation: stage the SIF and audio shards to node-local `/raid/scratch` (5.3 GB/s) once per node,
never per task.

### PyTorch module (deferred, not needed)

No PyTorch module is installed: `/shared/apps/modules/all/ai/PyTorch/` holds no modulefile and
`/shared/apps/software/PyTorch` does not exist. Build 2241
(`2.9.1-foss-2025b-CUDA-13.4.1`, on dgx160) **compiled clean** — `pip install` exited 0 after 40
minutes — then EasyBuild ran the test suite for 9h23m and aborted on 144 failures against a max of
10, so nothing installed. Staging prefix was `/raid/scratch/nvidia-eb-2241/...`, node-local.

The container covers all project needs, so the module is deferred. If a clean module stack is
wanted later, re-run the build with the test step relaxed or known-failing tests excluded — the
compile is proven; the tests are the blocker.

Also confirmed on compute nodes: `libcuda.so.1` present, PyPI reachable (so a venv on
`/shared/apps/ML` is viable if the container ever needs supplementing). No torchaudio /
torchvision / librosa / soundfile easyconfigs exist anywhere — moot now that on-GPU STFT is
verified.

### Xeno-Canto API

v2 is retired (404 as of 2025-10-10); v3 requires a per-account key passed as `key=` query param
(currently 401). Free, but a prerequisite. Only needed for metadata reconciliation, since BirdSet
hosts the audio.

### Phase order

1. Storage (done) + container (done: Apptainer SIF, 20/24 nodes) + XC key + EOD/DOF proportional
   pull started
2. Reproduce BirdSet baselines on one node — **hard correctness gate**
3. Build priors (global + Denmark × 3 resolutions), leakage tests green
4. Phase A: embedding extraction, fleet-wide, NFS behaviour measured
5. Phase B: sweep as job array
6. Analysis, artifact packaging, paper

## Open questions

- Whether the EBD arrives in time to add a detectability-modelling arm.
- Denmark case study depth: how far back the proportional DOF pull should reach.
