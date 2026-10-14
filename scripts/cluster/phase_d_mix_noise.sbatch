#!/usr/bin/env bash
#SBATCH --job-name=phase-d-mix
#SBATCH --partition=gpu
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/phase_d_mix_%A_%a.out
#
# One array task per clean shard (CPU only; no --gres). SRC, NAME, FOLDS and
# SNRS are exported in the submitting shell.
#   export SRC=data/birdset/DKX/DKX NAME=DKXN FOLDS="5" SNRS="20 10 5 0"
#   sbatch --array=0-6 scripts/cluster/phase_d_mix_noise.sbatch
set -euo pipefail
: "${SRC:?}" "${NAME:?}" "${FOLDS:?}" "${SNRS:?}"
B=/srv/home/rapnic/birdology
cd "$B"
mapfile -t SHARDS < <(ls "$SRC"/*.tar.gz | sort)
IDX=${SLURM_ARRAY_TASK_ID:-0}
[ "$IDX" -lt "${#SHARDS[@]}" ] || { echo "task $IDX >= ${#SHARDS[@]} shards"; exit 0; }
PYTHONPATH=$B/apps/birdology/src exec $B/apps/venvs/phase_c/bin/python -P $B/apps/birdology/scripts/phase_d_mix_noise.py \
    --src "$SRC" --name "$NAME" --shard "$(basename "${SHARDS[$IDX]}")" --folds $FOLDS --snrs $SNRS
