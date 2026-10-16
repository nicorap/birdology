#!/bin/bash
#SBATCH --job-name=prior-occ
#SBATCH --partition=gpu
#SBATCH --cpus-per-task=32
#SBATCH --mem=256G
#SBATCH --time=08:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/prior/logs/prior_occ_%j.out
# Typed occurrence table + guarded counts from the EOD Parquet, checked against
# the eod_q counts checkpoints (jobs 6860/6866/7276, built from the zip).
#
# Scratch goes to NFS, not /tmp: /tmp on the DGX nodes is tmpfs, charged to
# the job's memory, and an OOM-killed run never reached its cleanup (jobs
# 12556/12580/12584 left 16.6 GB in dgx160's RAM). /raid is root-only. A failed
# mkdir must stop the job: Python would silently fall back to /tmp. The trap
# removes the scratch whatever happens to Python.
set -uo pipefail
B=/srv/home/rapnic/birdology
export HOME=/tmp PYTHONPATH=$B/apps/birdology/src
export TMPDIR=$B/data/prior/tmp/$SLURM_JOB_ID
mkdir -p "$TMPDIR" || { echo "FATAL: cannot create $TMPDIR"; exit 1; }
trap 'rm -rf "$TMPDIR"' EXIT
( while sleep 10; do
    echo "MEM $(date -u +%H:%M:%S) rss_GB=$(ps -u $USER -o rss= | awk '{s+=$1} END {printf "%.1f", s/1048576}') top=$(ps -u $USER -o rss=,pid= --sort=-rss | head -3 | awk '{printf "%.1fG/%s ", $1/1048576, $2}')"
  done ) &
cd $B
$B/apps/venvs/hf/bin/python -u $B/apps/birdology/scripts/prior_occurrence_counts.py \
    --parquet data/prior/parquet/eod --occ data/prior/occurrences/eod --resolutions 3 4 5 \
    --test-split data/prior/splits_eval.csv --split-contributor-mode placeholder \
    --workers 32 --compare data/prior/eod_q --source eod "$@"
