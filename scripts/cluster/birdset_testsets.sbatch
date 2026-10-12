#!/bin/bash
#SBATCH --job-name=birdset-tests
#SBATCH --partition=gpu
#SBATCH --time=08:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=16
#SBATCH --exclude=dgx177
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/birdset_tests_%j.out
set -uo pipefail
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }
for CONFIG in NES PER POW SSW SNE UHH; do
  echo "=== $CONFIG $(date -Is) ==="
  DEST=/srv/home/rapnic/birdology/data/birdset CONFIG=$CONFIG bash /srv/home/rapnic/birdology/apps/birdology/scripts/cluster/birdset_fetch.sh || echo "FAILED: $CONFIG"
done
echo "=== ALL DONE $(date -Is) ==="
ls /srv/home/rapnic/birdology/data/birdset/
