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

CONFIG=${1:?usage: sbatch ... phase_a_embed.sbatch CONFIG RESOLUTION}
RESOLUTION=${2:-4}

CORPUS=/mnt/weka/ML/birdset/${CONFIG}/${CONFIG}
OUT=/mnt/weka/ML/phase_a/embeddings/${CONFIG}
MANIFEST=/mnt/weka/ML/phase_a/${CONFIG}_r${RESOLUTION}_manifest.parquet
SIF=/shared/apps/ML/containers/pytorch.sif

# Fail loudly rather than writing to a local directory of the same name.
mountpoint -q /mnt/weka || { echo "FATAL: /mnt/weka is not a mountpoint on $(hostname -s)"; exit 2; }
[ -f "$MANIFEST" ] || { echo "FATAL: no manifest at $MANIFEST"; exit 2; }

mkdir -p "$OUT" /mnt/weka/ML/phase_a/logs

mapfile -t SHARDS < <(ls "$CORPUS"/*.tar.gz | sort)
N=${#SHARDS[@]}
[ "$N" -gt 0 ] || { echo "FATAL: no shards under $CORPUS"; exit 2; }

IDX=${SLURM_ARRAY_TASK_ID:-0}
[ "$IDX" -lt "$N" ] || { echo "task $IDX >= $N shards, nothing to do"; exit 0; }
SHARD=${SHARDS[$IDX]}

echo "host=$(hostname -s) task=$IDX/$N shard=$(basename "$SHARD")"

apptainer exec --nv --bind /mnt/weka:/mnt/weka "$SIF" \
    python3 -m birdology.phase_a.embed_cli \
        --shard "$SHARD" --manifest "$MANIFEST" --out "$OUT"
