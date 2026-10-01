#!/usr/bin/env bash
#SBATCH --job-name=phase-c-train
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=96G
#SBATCH --time=06:00:00
#SBATCH --output=/mnt/weka/ML/phase_c/logs/train_%A_%a.out
#
# Phase C fleet runner: ONE (arm, model, resolution, seed, test_set) cell
# per array task, one GPU each (the grid is 195 runs x ~3 GPU-h; time is
# 6 h for headroom, mem 96G for the XCL-class checkpoints + staging).
# Twins are their own tasks with --arm twin — the pairing happens in
# analysis by (model, resolution, seed), never inside one job.
#
#   sbatch --array=0-N scripts/cluster/phase_c_train.sbatch CELLS_TSV
#
# CELLS_TSV: one cell per line, tab-separated:
#   arm  model  checkpoint  revision  resolution  seed  test_set  shards_glob
# SLURM_ARRAY_TASK_ID indexes the line. Same exclusivity ruling as
# phase_a_embed.sbatch: re-submitting into the same OUT with a CHANGED
# cells file is forbidden; point a changed grid at a fresh OUT.
#
# THREE things here are silent failures if omitted, all measured
# (phase_a_embed.sbatch's ledger, inherited verbatim):
#   --gres=gpu:1            without it, torch.cuda.is_available() is False
#                           INSIDE the container even with --nv
#   --nv                    without it, the container has no CUDA driver
#   --bind /mnt/weka        without it, /mnt/weka is an EMPTY DIRECTORY in
#                           the container -- 0 files found, exit 0
# Plus the Phase C staging ruling: ALL shard I/O goes to /raid/scratch
# FIRST (measured: 5.3 GB/s local vs ~300 MB/s shared WEKA at contention)
# -- a training loop reading crops off WEKA would starve every GPU on the
# node, silently.
set -euo pipefail

CELLS=${1:?usage: sbatch --array=0-N phase_c_train.sbatch CELLS_TSV}

REPO=/shared/apps/ML/birdology
SIF=/shared/apps/ML/containers/pytorch.sif
OUT=/mnt/weka/ML/phase_c/results
CKPT_ROOT=/mnt/weka/ML/phase_c/checkpoints
PRIOR_DIR=/mnt/weka/ML/prior/eod
BRIDGE=/mnt/weka/ML/phase_a/code_to_gbif.json
PRIOR_DOI=${PRIOR_DOI:?export PRIOR_DOI -- a row that cannot cite its prior is refused}
GIT_SHA=$(git -C "$REPO" rev-parse HEAD)

# Fail loudly rather than writing to a local directory of the same name.
mountpoint -q /mnt/weka || { echo "FATAL: /mnt/weka is not a mountpoint on $(hostname -s)"; exit 2; }
[ -f "$CELLS" ] || { echo "FATAL: no cells file at $CELLS"; exit 2; }

IDX=${SLURM_ARRAY_TASK_ID:-0}
LINE=$(sed -n "$((IDX + 1))p" "$CELLS")
[ -n "$LINE" ] || { echo "task $IDX: no line $((IDX + 1)) in $CELLS, nothing to do"; exit 0; }
IFS=$'\t' read -r ARM MODEL CHECKPOINT REVISION RESOLUTION SEED TEST_SET SHARDS_GLOB <<< "$LINE"

mkdir -p "$OUT" /mnt/weka/ML/phase_c/logs
echo "host=$(hostname -s) task=$IDX arm=$ARM model=$MODEL r=$RESOLUTION seed=$SEED test=$TEST_SET"

# ---- /raid/scratch staging (the measured I/O ruling) -------------------
# Stage this task's shards to node-local scratch BEFORE any decode; the
# trap reclaims the space on every exit path (preemption included).
STAGE=/raid/scratch/phase_c_${SLURM_JOB_ID:-manual}_${IDX}
mkdir -p "$STAGE/audio"
trap 'rm -rf "$STAGE"' EXIT
N_STAGED=0
for shard in $SHARDS_GLOB; do
    tar -xzf "$shard" -C "$STAGE/audio"
    N_STAGED=$((N_STAGED + 1))
done
[ "$N_STAGED" -gt 0 ] || { echo "FATAL: shard glob matched nothing: $SHARDS_GLOB"; exit 2; }
echo "staged $N_STAGED shard(s) to $STAGE/audio"

CKPT_DIR=$CKPT_ROOT/${ARM}_${MODEL}_r${RESOLUTION}_s${SEED}_${TEST_SET}
mkdir -p "$CKPT_DIR"

# The SIF does NOT carry this repo: bind-mount it, PYTHONPATH it, and
# redirect $HOME (jobs run with an unreadable $HOME; python touches it).
apptainer exec --nv --bind /mnt/weka:/mnt/weka,/raid/scratch:/raid/scratch,"$REPO":/opt/birdology,/shared/apps/ML:/shared/apps/ML "$SIF" \
    env HOME=/tmp PYTHONPATH=/opt/birdology/src \
        HF_HOME=/shared/apps/ML/models/hf \
    python3 /opt/birdology/scripts/phase_c_run.py \
        --arm "$ARM" --model "$MODEL" \
        --checkpoint "$CHECKPOINT" --checkpoint-revision "$REVISION" \
        --resolution "$RESOLUTION" --seed "$SEED" --test-set "$TEST_SET" \
        --manifest "/mnt/weka/ML/phase_a/XCM_r${RESOLUTION}_manifest.parquet" \
        --val-metadata "/mnt/weka/ML/phase_a/POW_r${RESOLUTION}_manifest.parquet" \
        --eval-metadata "/mnt/weka/ML/birdset/${TEST_SET}/${TEST_SET}/${TEST_SET}_metadata_test_5s.parquet" \
        --audio-root "$STAGE/audio" \
        --prior "$PRIOR_DIR/prior_r${RESOLUTION}_full.npz" \
        --prior-doi "$PRIOR_DOI" \
        --bridge "$BRIDGE" \
        --context-source "eod_r${RESOLUTION}_full" \
        --git-sha "$GIT_SHA" \
        --ckpt-dir "$CKPT_DIR" \
        --out "$OUT"
