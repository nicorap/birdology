#!/bin/bash
#SBATCH --job-name=testset-manifests
#SBATCH --partition=gpu
#SBATCH --time=02:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8
#SBATCH --exclude=dgx177
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/testset_manifests_%j.out
set -uo pipefail
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }
export HOME=/tmp PYTHONPATH=/srv/home/rapnic/birdology/apps/birdology/src
for CONFIG in NES PER POW SSW SNE UHH; do
  echo "=== $CONFIG $(date -u +%H:%M:%S) ==="
  /srv/home/rapnic/birdology/apps/venvs/hf/bin/python /srv/home/rapnic/birdology/apps/birdology/scripts/phase_a_manifest.py     --corpus /srv/home/rapnic/birdology/data/birdset --config $CONFIG --split test_5s     --resolution 4 --key-file /srv/home/rapnic/birdology/apps/secrets/xc_key     --out /srv/home/rapnic/birdology/data/phase_a || echo "FAILED: $CONFIG"
done
echo "=== ALL DONE ==="
ls -la /srv/home/rapnic/birdology/data/phase_a/*_test_5s_r4_manifest.parquet
