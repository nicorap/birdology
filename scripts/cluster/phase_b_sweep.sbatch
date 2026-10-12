#!/bin/bash
#SBATCH --job-name=phase-b-sweep
#SBATCH --partition=gpu
#SBATCH --time=2-00:00:00
#SBATCH --mem=480G
#SBATCH --cpus-per-task=32
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_b/logs/sweep_%A_%a.out
#
# Phase B cheap-arm sweep: CPU-ONLY work submitted array-wide so the whole
# fleet carries it (user directive 2026-10-01: use as many nodes as possible
# for the CPU work). Deliberately NO --gres=gpu -- these tasks reserve cores
# and memory only; the GPUs stay free for the FiLM/context-token plan.
#
# Usage: [env ...] sbatch --array=0-N phase_b_sweep.sbatch
#   array size MUST equal N_SHARDS; shards are fit-group-aligned
#   (shard_cells_grouped), so one task owns all six test sets of each fit.
# Env (defaults = the DOF sweep, unchanged):
#   PRIOR_TEMPLATE  prior npz with {resolution}
#   OUT             results dir -- ONE PER PRIOR SOURCE: config_hash does not
#                   include the prior, so two sources in one dir would skip
#                   each other's cells as "existing"
#   GIT_SHA         provenance (the deploy is a git-archive copy, no .git)
#   EXTRA           extra sweep args, e.g. "--resolutions 3 4"
# 2-day limit: 10 of 48 DOF shards hit the old 24h limit (job 5626).
set -uo pipefail
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }

N_SHARDS=${N_SHARDS:-48}
IDX=${SLURM_ARRAY_TASK_ID:-0}
# NOT ${PRIOR_TEMPLATE:-...{resolution}...}: the `}` of `{resolution}` closes
# the parameter expansion early and glues `_full.npz}` onto the value (jobs
# 6959/6960: KeyError 'resolution_full' / "Single '}' encountered").
DEFAULT_PRIOR='/srv/home/rapnic/birdology/data/prior/dof/prior_dof_r{resolution}_full.npz'
PRIOR_TEMPLATE=${PRIOR_TEMPLATE:-$DEFAULT_PRIOR}
case "$PRIOR_TEMPLATE" in
    *'{resolution}'*.npz) ;;
    *) echo "FATAL: PRIOR_TEMPLATE must contain {resolution} and end in .npz: $PRIOR_TEMPLATE"; exit 2 ;;
esac
OUT=${OUT:-/srv/home/rapnic/birdology/data/phase_b/results}
GIT_SHA=${GIT_SHA:?export GIT_SHA -- /srv/home/rapnic/birdology/apps/birdology is a git-archive copy}
EXTRA=${EXTRA:-}

mkdir -p "$OUT" /srv/home/rapnic/birdology/data/phase_b/logs
export HOME=/tmp PYTHONPATH=/srv/home/rapnic/birdology/apps/birdology/src
echo "host=$(hostname -s) shard=$IDX/$N_SHARDS prior=$PRIOR_TEMPLATE out=$OUT sha=$GIT_SHA extra=[$EXTRA] $(date -Is)"

# shellcheck disable=SC2086  # EXTRA is deliberately word-split
exec /srv/home/rapnic/birdology/apps/venvs/hf/bin/python \
  /srv/home/rapnic/birdology/apps/birdology/scripts/phase_b_sweep.py \
  --grid-shard "$IDX" --n-shards "$N_SHARDS" \
  --train-cache "/srv/home/rapnic/birdology/data/phase_a/embeddings/XCL_{encoder}" \
  --eval-cache "/srv/home/rapnic/birdology/data/phase_a/embeddings/{test_set}_test_5s_{encoder}" \
  --eval-labels "/srv/home/rapnic/birdology/data/birdset/{test_set}/{test_set}/{test_set}_metadata_test_5s.parquet" \
  --prior "$PRIOR_TEMPLATE" \
  --labels "/srv/home/rapnic/birdology/apps/models/{encoder}_labels.txt" \
  --bridge /srv/home/rapnic/birdology/data/phase_a/code_to_gbif.json \
  --train-locations /srv/home/rapnic/birdology/data/phase_a/XCL_r4_manifest.parquet \
  --eval-locations "/srv/home/rapnic/birdology/data/phase_a/{test_set}_test_5s_r4_manifest.parquet" \
  --out "$OUT" --git-sha "$GIT_SHA" $EXTRA
