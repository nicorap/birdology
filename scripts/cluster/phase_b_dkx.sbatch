#!/bin/bash
#SBATCH --job-name=phase-b-dkx
#SBATCH --partition=gpu
#SBATCH --cpus-per-task=32
#SBATCH --mem=480G   # as phase_b_sweep: the XCL train cache (Perch logits ~30 GB) is loaded per cell; 256G OOMed (jobs 9526/9527)
#SBATCH --time=1-00:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_b/logs/dkx_%A_%a.out
#
# Phase B cheap arms on DKX, the held-out Danish Xeno-canto test set
# (scripts/build_dkx_testset.py), against a DOF prior. CPU work: no --gres.
# It is on the gpu partition only because cpu is a single 8-core VM.
#
#   PRIOR_TEMPLATE=.../prior/dof_pre2025/prior_dof_r{resolution}_full.npz \
#   OUT=.../phase_b/results_dkx_dof_pre2025 GIT_SHA=<deployed sha> \
#   sbatch --array=0-$((N_SHARDS-1)) scripts/cluster/phase_b_dkx.sbatch
#
# Differences from phase_b_sweep.sbatch, all forced by DKX's layout:
#   - eval caches/labels/manifest use split "focal", not "test_5s";
#   - the unit is one recording, not a 5 s segment (cmAP not comparable with BirdSet);
#   - PRIOR_TEMPLATE and OUT are REQUIRED: config_hash does not include the prior,
#     so a shared OUT would let one prior's cells skip another's as "existing";
#   - the MLP late-fusion arm is excluded (degenerate cmAP on BirdSet, unresolved).
set -uo pipefail
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }

N_SHARDS=${N_SHARDS:-8}
IDX=${SLURM_ARRAY_TASK_ID:-0}
PRIOR_TEMPLATE=${PRIOR_TEMPLATE:?export PRIOR_TEMPLATE, e.g. .../prior_dof_r\{resolution\}_full.npz}
case "$PRIOR_TEMPLATE" in
    *'{resolution}'*.npz) ;;
    *) echo "FATAL: PRIOR_TEMPLATE must contain {resolution} and end in .npz: $PRIOR_TEMPLATE"; exit 2 ;;
esac
OUT=${OUT:?export OUT -- one results dir per prior}
GIT_SHA=${GIT_SHA:?export GIT_SHA -- the deploy is a git-archive copy}
D=$BIRDOLOGY_ROOT/data

mkdir -p "$OUT" "$D/phase_b/logs"
export HOME=/tmp PYTHONPATH=$BIRDOLOGY_ROOT/apps/birdology/src
echo "host=$(hostname -s) shard=$IDX/$N_SHARDS prior=$PRIOR_TEMPLATE out=$OUT sha=$GIT_SHA $(date -Is)"

exec $BIRDOLOGY_ROOT/apps/venvs/hf/bin/python \
  $BIRDOLOGY_ROOT/apps/birdology/scripts/phase_b_sweep.py \
  --grid-shard "$IDX" --n-shards "$N_SHARDS" \
  --test-sets DKX --methods rescoring offset_global offset_per_species \
  --train-cache "$D/phase_a/embeddings/XCL_{encoder}" \
  --eval-cache "$D/phase_a/embeddings/{test_set}_focal_{encoder}" \
  --eval-labels "$D/birdset/{test_set}/{test_set}/{test_set}_metadata_focal.parquet" \
  --prior "$PRIOR_TEMPLATE" \
  --labels "$BIRDOLOGY_ROOT/apps/models/{encoder}_labels.txt" \
  --bridge "$D/phase_a/code_to_gbif.json" \
  --train-locations "$D/phase_a/XCL_r4_manifest.parquet" \
  --eval-locations "$D/phase_a/{test_set}_focal_r4_manifest.parquet" \
  --out "$OUT" --git-sha "$GIT_SHA"
