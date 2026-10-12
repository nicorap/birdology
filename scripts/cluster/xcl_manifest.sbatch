#!/bin/bash
#SBATCH --job-name=xcl-manifest
#SBATCH --partition=gpu
#SBATCH --time=08:00:00
#SBATCH --mem=48G
#SBATCH --cpus-per-task=8
#SBATCH --exclude=dgx177
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/xcl_manifest_%j.out

# XCL dating sweep + manifest build. Resumable: XCL_dates_progress.json
# checkpoints per species; rerunning this job skips everything fetched.
# The key travels ONLY as a file path (ps/env leak rule).
set -u
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }

export HOME=/tmp
export PYTHONPATH=/srv/home/rapnic/birdology/apps/birdology/src

exec /srv/home/rapnic/birdology/apps/venvs/hf/bin/python \
  /srv/home/rapnic/birdology/apps/birdology/scripts/phase_a_manifest.py \
  --corpus /srv/home/rapnic/birdology/data/birdset \
  --config XCL \
  --resolution 4 \
  --key-file /srv/home/rapnic/birdology/apps/secrets/xc_key \
  --species-map /srv/home/rapnic/birdology/data/phase_a/ebird_code_to_sci.json \
  --out /srv/home/rapnic/birdology/data/phase_a
