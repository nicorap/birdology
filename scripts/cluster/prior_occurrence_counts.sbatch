#!/bin/bash
#SBATCH --job-name=prior-occ
#SBATCH --partition=gpu
#SBATCH --cpus-per-task=32
#SBATCH --mem=256G
#SBATCH --time=08:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/prior/logs/prior_occ_%j.out
# Typed occurrence table + guarded counts from the EOD Parquet, checked against
# the eod_q counts checkpoints (jobs 6860/6866/7276, built from the zip).
set -uo pipefail
B=/srv/home/rapnic/birdology
export HOME=/tmp PYTHONPATH=$B/apps/birdology/src
cd $B
exec $B/apps/venvs/hf/bin/python -u $B/apps/birdology/scripts/prior_occurrence_counts.py \
    --parquet data/prior/parquet/eod --occ data/prior/occurrences/eod --resolutions 3 4 5 \
    --test-split data/prior/splits_eval.csv --split-contributor-mode placeholder \
    --workers 32 --compare data/prior/eod_q --source eod "$@"
