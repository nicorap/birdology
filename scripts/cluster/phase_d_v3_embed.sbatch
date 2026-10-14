#!/usr/bin/env bash
#SBATCH --job-name=phase-d-v3-embed
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/phase_d_v3_embed_%A_%a.out
#
# Per-window BirdNET+ V3 embeddings (the BirdNET Live app's model), one array
# task per sorted tar shard of SRC (scripts/phase_d_v3_embed.py). Runs in the
# apps/venvs/onnx venv (onnxruntime-gpu). Throttle big corpora (%8): each task
# streams its shard off the home NFS.
#
#   export SRC=data/birdset/DKX/DKX OUT=data/phase_d/v3/DKX [MANIFEST=... MAX_WINDOWS=100]
#   sbatch --array=0-6 scripts/cluster/phase_d_v3_embed.sbatch
set -euo pipefail
: "${SRC:?export SRC}" "${OUT:?export OUT}"
B=/srv/home/rapnic/birdology
cd "$B"
mapfile -t SHARDS < <(ls "$SRC"/*.tar.gz | sort)
IDX=${SLURM_ARRAY_TASK_ID:-0}
[ "$IDX" -lt "${#SHARDS[@]}" ] || { echo "task $IDX >= ${#SHARDS[@]} shards, nothing to do"; exit 0; }
mkdir -p "$OUT" 2>/dev/null || [ -d "$OUT" ]
echo "host=$(hostname -s) task=$IDX shard=$(basename "${SHARDS[$IDX]}") out=$OUT"
PYTHONPATH=$B/apps/birdology/src exec $B/apps/venvs/onnx/bin/python -P $B/apps/birdology/scripts/phase_d_v3_embed.py \
    --shard "${SHARDS[$IDX]}" --out "$OUT" ${MANIFEST:+--manifest "$MANIFEST"} ${MAX_WINDOWS:+--max-windows "$MAX_WINDOWS"}
