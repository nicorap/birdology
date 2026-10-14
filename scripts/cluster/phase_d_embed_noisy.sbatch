#!/usr/bin/env bash
#SBATCH --job-name=phase-d-embed-noisy
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/phase_d_embed_noisy_%A_%a.out
#
# Noise-augmented BirdNET embedding: every manifest recording in the shard is
# mixed in memory with bird-free ESC-50 noise (folds 1-4, SNR drawn per
# recording in 0-20 dB) before encoding. One array task per sorted shard.
# Throttle the array: each task streams a ~5 GB XCL shard off the home NFS,
# and 16 of them plus other traffic stalled the server on 2026-10-14.
#
#   export CORPUS=/srv/home/rapnic/birdology/data/birdset/XCL/XCL \
#          MANIFEST=/srv/home/rapnic/birdology/data/phase_a/XCL_eu_all_manifest.parquet \
#          OUT=/srv/home/rapnic/birdology/data/phase_a/embeddings/XCL_eu_noisy_birdnet
#   sbatch --array=0-97%8 scripts/cluster/phase_d_embed_noisy.sbatch
set -euo pipefail
: "${CORPUS:?export CORPUS}" "${MANIFEST:?export MANIFEST}" "${OUT:?export OUT}"
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
SIF=$BIRDOLOGY_ROOT/apps/containers/tf.sif
REPO=$BIRDOLOGY_ROOT/apps/birdology
[ -f "$MANIFEST" ] || { echo "FATAL: no manifest at $MANIFEST"; exit 2; }
mkdir -p "$OUT" 2>/dev/null || [ -d "$OUT" ]
mapfile -t SHARDS < <(ls "$CORPUS"/*.tar.gz | sort)
IDX=${SLURM_ARRAY_TASK_ID:-0}
[ "$IDX" -lt "${#SHARDS[@]}" ] || { echo "task $IDX >= ${#SHARDS[@]} shards, nothing to do"; exit 0; }
SHARD=${SHARDS[$IDX]}
echo "host=$(hostname -s) task=$IDX shard=$(basename "$SHARD") out=$OUT"
apptainer exec --nv --bind "$BIRDOLOGY_ROOT":"$BIRDOLOGY_ROOT","$REPO":/opt/birdology "$SIF" \
    env HOME=/tmp PYTHONPATH=/opt/birdology/src \
        KAGGLEHUB_CACHE=$BIRDOLOGY_ROOT/apps/models/kagglehub \
        BIRDNET_APP_DATA=$BIRDOLOGY_ROOT/apps/models/birdnet \
        TF_FORCE_GPU_ALLOW_GROWTH=true \
    python3 -m birdology.phase_d.embed_noisy_cli \
        --shard "$SHARD" --manifest "$MANIFEST" --out "$OUT" --root "$BIRDOLOGY_ROOT" --folds 1 2 3 4
