#!/bin/bash
#SBATCH --job-name=prior-eod
#SBATCH --partition=gpu
#SBATCH --time=12:00:00
#SBATCH --mem=1500G
#SBATCH --cpus-per-task=96
#SBATCH --exclude=dgx177
#SBATCH --output=/srv/home/rapnic/birdology/data/prior/logs/prior_eod_%j.out
set -uo pipefail
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }
export HOME=/tmp PYTHONPATH=/srv/home/rapnic/birdology/apps/birdology/src
# Parent-RSS sampler (af758ac's named unmeasured risk: parent merge
# memory at 1.76B rows). One line per minute into the job log.
( while sleep 60; do echo "MEMSAMPLE $(date -u +%H:%M) node_used_GB=$(free -g | awk '/Mem:/{print $3}') top_rss_GB=$(ps -eo rss --sort=-rss | sed -n 2p | awk '{printf "%.1f", $1/1048576}')"; done ) &
/srv/home/rapnic/birdology/apps/venvs/hf/bin/python /srv/home/rapnic/birdology/apps/birdology/scripts/build_prior.py   --archive /srv/home/rapnic/birdology/data/prior/archives/eod.zip   --source eod   --resolutions 3 4 5   --out /srv/home/rapnic/birdology/data/prior/eod   --gbif-doi 10.15468/dl.smqz7f --test-split /srv/home/rapnic/birdology/data/prior/splits_eval.csv --split-contributor-mode placeholder --workers 90
  
