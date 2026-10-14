#!/usr/bin/env bash
#SBATCH --job-name=phase-d-head
#SBATCH --partition=gpu
#SBATCH --cpus-per-task=32
#SBATCH --mem=160G
#SBATCH --time=06:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/phase_d_head_%j.out
#
# CPU job (no --gres): the head trains in minutes on 32 cores. Loading the
# European XCL caches with their 6522-wide score rows needs the memory.
#   export HEAD_OUT=data/phase_d/dk_head_v3; sbatch scripts/cluster/phase_d_train_head.sbatch
# Optional NOISY: noise-augmented caches of the same recordings, e.g.
#   export NOISY=data/phase_a/embeddings/XCL_eu_noisy_birdnet
set -euo pipefail
: "${HEAD_OUT:?export HEAD_OUT}"
B=/srv/home/rapnic/birdology
cd "$B"
PYTHONPATH=$B/apps/birdology/src exec $B/apps/venvs/phase_c/bin/python -P $B/apps/birdology/scripts/phase_d_train_head.py \
    --out "$HEAD_OUT" --extra-cache data/phase_a/embeddings/XCL_eu_undated_fill_birdnet \
    ${NOISY:+--noisy-cache $NOISY}
