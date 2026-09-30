#!/usr/bin/env bash
#SBATCH --job-name=phase-a-embed
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=/mnt/weka/ML/phase_a/logs/embed_%A_%a.out
#
# One array task per shard, one GPU each. Deliberately 1 GPU per task so a
# flaky node kills one shard rather than the run.
#
#   sbatch --array=0-181 scripts/cluster/phase_a_embed.sbatch XCM 4
#
# FIRST SUBMISSION on a fresh tree: run `mkdir -p /mnt/weka/ML/phase_a/logs`
# beforehand (`make phase-a-manifest` does it) -- Slurm opens the
# #SBATCH --output path BEFORE this script runs, so the in-job mkdir below
# is too late for the very first array.
#
# THREE things here are silent failures if omitted, all measured:
#   --gres=gpu:1            without it, torch.cuda.is_available() is False
#                           INSIDE the container even with --nv
#   --nv                    without it, the container has no CUDA driver
#   --bind /mnt/weka        without it, /mnt/weka is an EMPTY DIRECTORY in the
#                           container -- the shard loop finds 0 files, exit 0
# WEKA is fleet-wide (verified across all 20 schedulable nodes, 2026-09-30),
# so the array spreads over the whole fleet; the mountpoint guard below still
# protects against any node that silently loses the mount -- with 20 nodes
# mounting independently, one silently losing it fabricates an empty corpus
# on that node only.
#
# ONE ARRAY TASK OWNS ONE SHARD, EXCLUSIVELY: SLURM_ARRAY_TASK_ID indexes the
# SORTED shard list below. Re-submitting into the same OUT with a DIFFERENT
# shard list (files added or removed under $CORPUS) is FORBIDDEN -- the
# index->shard mapping shifts and two generations of outputs would merge
# silently. Point a changed corpus at a FRESH OUT instead. Re-submitting the
# SAME list is safe: embed_cli refuses (exit 2) a shard whose output is
# already complete unless --force is passed.
set -euo pipefail

CONFIG=${1:?usage: sbatch ... phase_a_embed.sbatch CONFIG RESOLUTION [SPLIT] [ENCODER]}
RESOLUTION=${2:-4}
# SPLIT: for configs shipping several metadata parquets (HSN: train/test/test_5s).
# It is part of the manifest artifact name and of OUT, matching the driver.
SPLIT=${3:-}
# ENCODER: melsummary (pytorch.sif) or perch/birdnet (tf.sif, golden-gated
# 2026-09-30 at exact-zero diff vs the official tooling).
ENCODER=${4:-melsummary}
# WITH_LOGITS: "logits" as $5 adds the species-posterior column (perch: raw
# uncalibrated logits + metadata flag; birdnet: dense sigmoid scores).
WITH_LOGITS=${5:-}

SUFFIX=${SPLIT:+_${SPLIT}}
CORPUS=/mnt/weka/ML/birdset/${CONFIG}/${CONFIG}
# The encoder is part of OUT: caches from different encoders must never
# share a directory (indistinguishable rows, and shard_is_complete would
# skip real work).
OUT=/mnt/weka/ML/phase_a/embeddings/${CONFIG}${SUFFIX}_${ENCODER}
MANIFEST=/mnt/weka/ML/phase_a/${CONFIG}${SUFFIX}_r${RESOLUTION}_manifest.parquet
# tf.sif for the TF encoders; pytorch.sif for the checkpoint-free stand-in.
case "$ENCODER" in
    perch|birdnet) SIF=/shared/apps/ML/containers/tf.sif ;;
    *)             SIF=/shared/apps/ML/containers/pytorch.sif ;;
esac
# The SIF does NOT carry this repo (verified on the fleet, 2026-09-30): the
# code must be bind-mounted in and put on PYTHONPATH, and $HOME redirected --
# jobs run with an unreadable $HOME, and python's site machinery touches it.
REPO=/shared/apps/ML/birdology

# Fail loudly rather than writing to a local directory of the same name.
mountpoint -q /mnt/weka || { echo "FATAL: /mnt/weka is not a mountpoint on $(hostname -s)"; exit 2; }
[ -f "$MANIFEST" ] || { echo "FATAL: no manifest at $MANIFEST"; exit 2; }

mkdir -p "$OUT" /mnt/weka/ML/phase_a/logs

if [ -n "$SPLIT" ]; then
    # Split-sharded configs name shards WITHOUT the split's underscores:
    # metadata is HSN_metadata_test_5s.parquet but shards are
    # HSN_test5s_shard_*.tar.gz (measured on WEKA, 2026-09-30 -- BirdSet's own
    # inconsistency). tr -d '_' maps test_5s->test5s, train->train, test->test.
    SHARD_TAG=$(echo "$SPLIT" | tr -d '_')
    mapfile -t SHARDS < <(ls "$CORPUS/${CONFIG}_${SHARD_TAG}_shard_"*.tar.gz | sort)
else
    mapfile -t SHARDS < <(ls "$CORPUS"/*.tar.gz | sort)
fi
N=${#SHARDS[@]}
[ "$N" -gt 0 ] || { echo "FATAL: no shards under $CORPUS"; exit 2; }

IDX=${SLURM_ARRAY_TASK_ID:-0}
# Exactly one task (0) checks the operator-supplied --array range against the
# real shard count: an under-provisioned array (200 shards, --array=0-181)
# leaves shards 182+ silently un-embedded. WARN, do not fail -- a deliberate
# partial run (re-submitting a range) is legitimate; task 0's log is the signal.
ARRAY_TASKS=$(( ${SLURM_ARRAY_TASK_MAX:-0} + 1 ))
if [ "$IDX" -eq 0 ] && [ "$ARRAY_TASKS" -lt "$N" ]; then
    echo "WARNING: array covers $ARRAY_TASKS of $N shards -- shards $ARRAY_TASKS..$((N-1)) will NOT be embedded"
fi
[ "$IDX" -lt "$N" ] || { echo "task $IDX >= $N shards, nothing to do"; exit 0; }
SHARD=${SHARDS[$IDX]}

echo "host=$(hostname -s) task=$IDX/$N shard=$(basename "$SHARD")"

LOGITS_FLAG=""
[ "$WITH_LOGITS" = "logits" ] && LOGITS_FLAG="--with-logits"

apptainer exec --nv --bind /mnt/weka:/mnt/weka,"$REPO":/opt/birdology,/shared/apps/ML:/shared/apps/ML "$SIF" \
    env HOME=/tmp PYTHONPATH=/opt/birdology/src \
        KAGGLEHUB_CACHE=/shared/apps/ML/models/kagglehub \
        BIRDNET_APP_DATA=/shared/apps/ML/models/birdnet \
        TF_FORCE_GPU_ALLOW_GROWTH=true \
    python3 -m birdology.phase_a.embed_cli \
        --shard "$SHARD" --manifest "$MANIFEST" --out "$OUT" \
        --encoder "$ENCODER" $LOGITS_FLAG
