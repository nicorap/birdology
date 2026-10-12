#!/usr/bin/env bash
#SBATCH --job-name=phase-c-train
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=384G
#SBATCH --time=06:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_c/logs/train_%A_%a.out
#
# Phase C fleet runner: ONE (arm, model, resolution, seed, prior_variant)
# cell per array task, one GPU each. Each task trains ONCE and then loops
# evaluation over every test set in its TEST_SETS field (finding I2:
# training is test-set-independent, so a per-test-set checkpoint dir
# would retrain every cell 6x — ~1080 GPU-h vs the spec's ~600). One
# results row per test set. Twins are their own tasks with --arm twin —
# the pairing happens in analysis by (model, resolution, seed), never
# inside one job.
#
#   sbatch --array=0-N scripts/cluster/phase_c_train.sbatch CELLS_TSV
#
# CELLS_TSV: one cell per line, tab-separated:
#   arm  model  checkpoint  revision  resolution  seed  prior_variant  test_sets  shards_glob
# where prior_variant is one of full|marginal|shuffle_space|shuffle_time
# (finding I1 — EXACTLY the npz suffixes build_prior.py writes; the 135
# control runs are rows with a non-full variant) and test_sets is a
# comma-separated list, e.g. HSN,NES,PER,SSW,SNE,UHH.
# SLURM_ARRAY_TASK_ID indexes the line. Same exclusivity ruling as
# phase_a_embed.sbatch: re-submitting into the same OUT with a CHANGED
# cells file is forbidden; point a changed grid at a fresh OUT.
#
# EVAL TABLES ARE CONTEXT-JOINED, NEVER RAW METADATA (finding C1): the
# raw BirdSet metadata parquets carry NO cell/week columns, so a
# conditioned eval fed them would silently run present=0 throughout and
# the conditioned−twin headline would measure training-side noise only.
# The tables under /srv/home/rapnic/birdology/data/phase_c/eval_ctx/ are produced by
# scripts/phase_c_build_eval_ctx.py (test-set label metadata joined with
# H3 cell + ISO week, phase_a's own derivation); build them BEFORE
# launching the array. phase_c_run refuses an all-null table for
# conditioned arms.
#
# NO CONTAINER: the run uses the phase_c venv (apps/venvs/phase_c, python
# 3.14 + torch) directly. Silent failures guarded here, all measured:
#   --gres=gpu:1            without it, torch.cuda.is_available() is False
#   --device cuda           explicit: never a silent CPU fallback (job 5730's
#                           twins ran 2h43m on CPU and looked like GPU runs)
#   home visibility guard   an unmounted /srv/home is an EMPTY DIRECTORY --
#                           0 files found, exit 0
# Plus the Phase C staging ruling: ALL shard I/O goes to /raid/scratch
# FIRST (measured: 5.3 GB/s local vs ~300 MB/s shared WEKA at contention)
# -- a training loop reading crops off WEKA would starve every GPU on the
# node, silently.
set -euo pipefail

CELLS=${1:?usage: sbatch --array=0-N phase_c_train.sbatch CELLS_TSV}

REPO=/srv/home/rapnic/birdology/apps/birdology
OUT=/srv/home/rapnic/birdology/data/phase_c/results
CKPT_ROOT=/srv/home/rapnic/birdology/data/phase_c/checkpoints
PRIOR_DIR=${PRIOR_DIR:-/srv/home/rapnic/birdology/data/prior/eod}
BRIDGE=/srv/home/rapnic/birdology/data/phase_a/code_to_gbif.json
PRIOR_DOI=${PRIOR_DOI:?export PRIOR_DOI -- a row that cannot cite its prior is refused}
GIT_SHA=${GIT_SHA:?export GIT_SHA — /srv/home/rapnic/birdology/apps/birdology is an rsync copy, not a git repo}

# Fail loudly rather than writing to a local directory of the same name.
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }
[ -f "$CELLS" ] || { echo "FATAL: no cells file at $CELLS"; exit 2; }

IDX=${SLURM_ARRAY_TASK_ID:-0}
LINE=$(sed -n "$((IDX + 1))p" "$CELLS")
[ -n "$LINE" ] || { echo "task $IDX: no line $((IDX + 1)) in $CELLS, nothing to do"; exit 0; }
IFS=$'\t' read -r ARM MODEL CHECKPOINT REVISION RESOLUTION SEED PRIOR_VARIANT TEST_SETS SHARDS_GLOB <<< "$LINE"

case "$PRIOR_VARIANT" in
    full|marginal|shuffle_space|shuffle_time) ;;
    *) echo "FATAL: unknown prior_variant '$PRIOR_VARIANT' (finding I1: must be an npz suffix build_prior.py writes)"; exit 2 ;;
esac

mkdir -p "$OUT" /srv/home/rapnic/birdology/data/phase_c/logs
echo "host=$(hostname -s) task=$IDX arm=$ARM model=$MODEL r=$RESOLUTION seed=$SEED variant=$PRIOR_VARIANT tests=$TEST_SETS"

# ---- /raid/scratch staging (the measured I/O ruling) -------------------
# Stage this task's shards to node-local scratch BEFORE any decode; the
# trap reclaims the space on every exit path (preemption included).
STAGE=/raid/scratch/phase_c_${SLURM_JOB_ID:-manual}_${IDX}
mkdir -p "$STAGE/audio"
trap 'rm -rf "$STAGE"' EXIT
N_STAGED=0
# Train shards (XCM) + POW val + this cell's eval test sets, all into one
# root: test-set tars extract flat filenames, XCM extracts data/xeno-canto/...
# -- measured, no collisions. Extraction runs 8-way parallel.
ALL_TARS=$(ls $SHARDS_GLOB 2>/dev/null)
# XCM fill: 9,847 manifest recordings absent from the XCM tars, sourced from
# XCL (byte-identical, measured) and re-archived at their manifest paths.
ALL_TARS="$ALL_TARS $(ls /srv/home/rapnic/birdology/data/birdset/XCM_fill/*.tar.gz 2>/dev/null)"
for TS in POW $(echo "$TEST_SETS" | tr ',' ' '); do
    ALL_TARS="$ALL_TARS $(ls /srv/home/rapnic/birdology/data/birdset/$TS/$TS/*.tar.gz 2>/dev/null)"
done
N_STAGED=$(echo $ALL_TARS | wc -w)
echo $ALL_TARS | tr ' ' '\n' | xargs -P 8 -I{} tar -xzf {} -C "$STAGE/audio" || { echo "FATAL: tar extraction failed"; exit 2; }
[ "$N_STAGED" -gt 0 ] || { echo "FATAL: shard glob matched nothing: $SHARDS_GLOB"; exit 2; }
echo "staged $N_STAGED shard(s) to $STAGE/audio"

# Pre-flight: every filepath in the val + eval tables must exist on disk
# NOW, not at the end of epoch 1 hours from now.
/srv/home/rapnic/birdology/apps/venvs/phase_c/bin/python - "$STAGE/audio" "$RESOLUTION" "$TEST_SETS" <<'PYEOF' || { echo "FATAL: pre-flight failed"; exit 2; }
import sys, os, pyarrow.parquet as pq
root, res, sets = sys.argv[1], sys.argv[2], sys.argv[3].split(",")
paths = {"TRAIN(XCM)": f"/srv/home/rapnic/birdology/data/phase_a/XCM_r{res}_manifest.parquet",
         "POW(val)": f"/srv/home/rapnic/birdology/data/phase_c/eval_ctx/POW_r{res}_eval_ctx.parquet"}
paths.update({ts: f"/srv/home/rapnic/birdology/data/phase_c/eval_ctx/{ts}_r{res}_eval_ctx.parquet" for ts in sets})
bad = 0
for name, p in paths.items():
    fps = pq.read_table(p, columns=["filepath"]).column("filepath").to_pylist()
    miss = [f for f in fps if not os.path.exists(os.path.join(root, f))]
    print(f"PREFLIGHT {name}: {len(fps)-len(miss)}/{len(fps)} present")
    if miss:
        bad += 1
        print("  missing e.g.:", miss[:3])
sys.exit(1 if bad else 0)
PYEOF


# CKPT_DIR keyed WITHOUT test_set (finding I2): training depends only on
# (arm, model, resolution, seed [, prior_variant for conditioned runs]) —
# the test set only changes WHICH eval table is read after training.
CKPT_DIR=$CKPT_ROOT/${ARM}_${MODEL}_r${RESOLUTION}_s${SEED}_${PRIOR_VARIANT}
mkdir -p "$CKPT_DIR"

# The venv does not carry this repo: PYTHONPATH it, and redirect $HOME to
# /tmp (python and HF touch it; keep job state out of the home).
env HOME=/tmp PYTHONPATH=$REPO/src \
        HF_HOME=/srv/home/rapnic/birdology/apps/models/hf \
    /srv/home/rapnic/birdology/apps/venvs/phase_c/bin/python $REPO/scripts/phase_c_run.py \
        --arm "$ARM" --model "$MODEL" \
        --checkpoint "$CHECKPOINT" --checkpoint-revision "$REVISION" \
        --resolution "$RESOLUTION" --seed "$SEED" --test-sets "$TEST_SETS" \
        --manifest "/srv/home/rapnic/birdology/data/phase_a/XCM_r${RESOLUTION}_manifest.parquet" \
        --val-metadata "/srv/home/rapnic/birdology/data/phase_c/eval_ctx/POW_r${RESOLUTION}_eval_ctx.parquet" \
        --eval-metadata-template "/srv/home/rapnic/birdology/data/phase_c/eval_ctx/{test_set}_r${RESOLUTION}_eval_ctx.parquet" \
        --audio-root "$STAGE/audio" \
        --prior "$PRIOR_DIR/${PRIOR_PREFIX:-prior}_r${RESOLUTION}_${PRIOR_VARIANT}.npz" \
        --prior-doi "$PRIOR_DOI" \
        --prior-variant "$PRIOR_VARIANT" \
        --bridge "$BRIDGE" \
        --context-source "${PRIOR_SOURCE:-eod}_r${RESOLUTION}_${PRIOR_VARIANT}" \
        --git-sha "$GIT_SHA" \
        --ckpt-dir "$CKPT_DIR" \
        --out "$OUT" --device cuda --num-workers 8 --log-every 50
