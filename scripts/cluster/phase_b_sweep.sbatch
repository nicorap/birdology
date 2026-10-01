#!/bin/bash
#SBATCH --job-name=phase-b-sweep
#SBATCH --partition=gpu
#SBATCH --time=12:00:00
#SBATCH --mem=192G
#SBATCH --cpus-per-task=32
#SBATCH --output=/faic/weka/ML/phase_b/logs/sweep_%A_%a.out
#
# Phase B cheap-arm sweep: CPU-ONLY work submitted array-wide so the whole
# fleet carries it (user directive 2026-10-01: use as many nodes as possible
# for the CPU work). Deliberately NO --gres=gpu — these tasks reserve cores
# and memory only; the GPUs stay free for the FiLM/context-token plan.
#
# Usage: sbatch --array=0-N phase_b_sweep.sbatch <ENCODERS> <PRIOR_DIR>
#   array size MUST equal the --n-shards below; shards are fit-group-aligned
#   (shard_cells_grouped), so one task owns all six test sets of each fit.
set -uo pipefail
mountpoint -q /faic/weka || { echo "FATAL: no /faic/weka"; exit 2; }

N_SHARDS=${N_SHARDS:-48}
IDX=${SLURM_ARRAY_TASK_ID:-0}
PRIOR_DIR=${2:-/faic/weka/ML/prior/dof}
OUT=/faic/weka/ML/phase_b/results

mkdir -p "$OUT" /faic/weka/ML/phase_b/logs
export HOME=/tmp PYTHONPATH=/shared/apps/ML/birdology/src
echo "host=$(hostname -s) shard=$IDX/$N_SHARDS $(date -Is 2>/dev/null || date)"

exec /shared/apps/ML/venvs/hf/bin/python \
  /shared/apps/ML/birdology/scripts/phase_b_sweep.py \
  --grid-shard "$IDX" --n-shards "$N_SHARDS" \
  --train-cache "/faic/weka/ML/phase_a/embeddings/XCL_{encoder}" \
  --eval-cache "/faic/weka/ML/phase_a/embeddings/{test_set}_test_5s_{encoder}" \
  --eval-labels "/faic/weka/ML/birdset/{test_set}/{test_set}/{test_set}_metadata_test_5s.parquet" \
  --prior "$PRIOR_DIR/prior_r{resolution}_full.npz" \
  --labels "/shared/apps/ML/models/{encoder}_labels.txt" \
  --bridge /faic/weka/ML/phase_a/code_to_gbif.json \
  --out "$OUT"
