#!/bin/bash
#SBATCH --job-name=prior-parquet
#SBATCH --partition=gpu
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --time=08:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/prior/logs/prior_parquet_%j.out
# One-time GBIF archive -> Parquet (raw text) for the prior and rarity flagging.
# CPU only: the zip inflates on one core (~307 MB/s, ~52 min floor for eod.zip).
#
#   sbatch prior_archive_parquet.sbatch data/prior/archives/eod.zip data/prior/parquet/eod --expect-rows 1775515330
#   sbatch prior_archive_parquet.sbatch data/prior/profile/eod_slice100M.zip data/prior/parquet/eod_slice100M --verify
set -uo pipefail
B=/srv/home/rapnic/birdology
ARCHIVE=${1:?archive}; OUT=${2:?out dir}; shift 2
export HOME=/tmp PYTHONPATH=$B/apps/birdology/src
cd $B
exec $B/apps/venvs/hf/bin/python -u $B/apps/birdology/scripts/prior_archive_to_parquet.py \
    --archive "$ARCHIVE" --out "$OUT" "$@"
