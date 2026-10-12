#!/bin/bash
#SBATCH --job-name=prior-dof
#SBATCH --partition=gpu
#SBATCH --time=12:00:00
#SBATCH --mem=256G
#SBATCH --cpus-per-task=32
#SBATCH --exclude=dgx177
#SBATCH --output=/srv/home/rapnic/birdology/data/prior/logs/prior_dof_%j.out
set -uo pipefail
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }
export HOME=/tmp PYTHONPATH=/srv/home/rapnic/birdology/apps/birdology/src
exec /srv/home/rapnic/birdology/apps/venvs/hf/bin/python /srv/home/rapnic/birdology/apps/birdology/scripts/build_prior.py   --archive /srv/home/rapnic/birdology/data/prior/archives/dof.zip   --source dof   --resolutions 3 4 5   --out /srv/home/rapnic/birdology/data/prior/dof   --gbif-doi 10.15468/dl.3j4czw \
  --no-test-split
