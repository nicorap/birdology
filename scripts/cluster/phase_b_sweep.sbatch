#!/bin/bash
#SBATCH --job-name=phase-b-sweep
#SBATCH --partition=gpu
#SBATCH --time=12:00:00
#SBATCH --mem=192G
#SBATCH --cpus-per-task=32
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_b/logs/sweep_%A_%a.out
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
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }

N_SHARDS=${N_SHARDS:-48}
IDX=${SLURM_ARRAY_TASK_ID:-0}
PRIOR_DIR=${2:-/srv/home/rapnic/birdology/data/prior/dof}
OUT=/srv/home/rapnic/birdology/data/phase_b/results

mkdir -p "$OUT" /srv/home/rapnic/birdology/data/phase_b/logs
export HOME=/tmp PYTHONPATH=/srv/home/rapnic/birdology/apps/birdology/src
echo "host=$(hostname -s) shard=$IDX/$N_SHARDS $(date -Is 2>/dev/null || date)"

exec /srv/home/rapnic/birdology/apps/venvs/hf/bin/python \
  /srv/home/rapnic/birdology/apps/birdology/scripts/phase_b_sweep.py \
  --grid-shard "$IDX" --n-shards "$N_SHARDS" \
  --train-cache "/srv/home/rapnic/birdology/data/phase_a/embeddings/XCL_{encoder}" \
  --eval-cache "/srv/home/rapnic/birdology/data/phase_a/embeddings/{test_set}_test_5s_{encoder}" \
  --eval-labels "/srv/home/rapnic/birdology/data/birdset/{test_set}/{test_set}/{test_set}_metadata_test_5s.parquet" \
  --train-locations /srv/home/rapnic/birdology/data/phase_a/XCL_r4_manifest.parquet \
  --eval-locations "/srv/home/rapnic/birdology/data/phase_a/{test_set}_test_5s_r4_manifest.parquet" \
  --prior "$PRIOR_DIR/prior_r{resolution}_full.npz" \
  --labels "/srv/home/rapnic/birdology/apps/models/{encoder}_labels.txt" \
  --bridge /srv/home/rapnic/birdology/data/phase_a/code_to_gbif.json \
  --out "$OUT"
