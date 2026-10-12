#!/bin/bash
#SBATCH --job-name=prior-eod-q
#SBATCH --partition=gpu
#SBATCH --time=2-00:00:00
#SBATCH --mem=1500G
#SBATCH --cpus-per-task=8
#SBATCH --exclude=dgx177
#SBATCH --output=/srv/home/rapnic/birdology/data/prior/logs/prior_eod_q_r%a_%A.out
# Restricted EOD prior (a60b5a0): --query-set builds only on the queried
# (cell, week) contexts + the exact one-step smoothing halo, bit-identical to
# the full grid there. --sequential parse (9e3d0ce); this first run writes
# the guarded-counts checkpoint, so later iterations use --resume-counts and
# skip the ~6.5h parse + guards. One resolution per task: --array=3,4,5.
set -uo pipefail
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }
export HOME=/tmp PYTHONPATH=/srv/home/rapnic/birdology/apps/birdology/src
R=${SLURM_ARRAY_TASK_ID:?run as --array=3,4,5}
OUT=/srv/home/rapnic/birdology/data/prior/eod_q
MODE=${MODE:---sequential}
memsample() {
    while sleep 300; do
        echo "MEMSAMPLE $(date -u +%H:%M) node_used_GB=$(free -g | awk '/Mem:/{print $3}')"
    done
}
memsample &
exec /srv/home/rapnic/birdology/apps/venvs/hf/bin/python /srv/home/rapnic/birdology/apps/birdology/scripts/build_prior.py \
    --archive /srv/home/rapnic/birdology/data/prior/archives/eod.zip --source eod --resolutions "$R" \
    --out "$OUT" --gbif-doi 10.15468/dl.smqz7f \
    --test-split /srv/home/rapnic/birdology/data/prior/splits_eval.csv --split-contributor-mode placeholder \
    "$MODE" --query-set "$OUT/query_set.parquet"
