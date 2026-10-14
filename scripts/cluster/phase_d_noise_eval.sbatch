#!/usr/bin/env bash
#SBATCH --job-name=phase-d-noise-eval
#SBATCH --partition=gpu
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/phase_d_noise_eval_%j.out
#   export HEADS="data/phase_d/dk_head_v3/head.pt"; sbatch scripts/cluster/phase_d_noise_eval.sbatch
set -euo pipefail
: "${HEADS:?export HEADS}"
B=/srv/home/rapnic/birdology
cd "$B"
PYTHONPATH=$B/apps/birdology/src exec $B/apps/venvs/phase_c/bin/python -P $B/apps/birdology/scripts/phase_d_noise_eval.py \
    --heads $HEADS --out data/phase_d/noise_curve.csv
