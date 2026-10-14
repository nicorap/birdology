#!/usr/bin/env bash
#SBATCH --job-name=phase-d-embed
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/phase_d_embed_%A_%a.out
#
# BirdNET-embed (with scores) every shard under CORPUS against MANIFEST into
# OUT -- one array task per sorted shard. phase_a_embed.sbatch derives all
# three from a BirdSet config name; Phase D's sets (the XCL undated fill, the
# ESC noise clips, noisy DKX) do not follow that naming, so they are passed in.
# Export them in the submitting shell: `sbatch --export=ALL,VAR=x` holds the
# job ("user env retrieval failed requeued held") on this cluster.
#
#   export CORPUS=.../data/birdset/DKXN/snr10/DKXN_snr10 \
#          MANIFEST=.../data/phase_a/DKX_focal_r4_manifest.parquet \
#          OUT=.../data/phase_a/embeddings/DKXN_snr10_birdnet
#   sbatch --array=0-6 scripts/cluster/phase_d_embed.sbatch
#
# A shard holding none of the manifest's rows makes embed_cli raise its
# zero-overlap guard: for a fill manifest that is a harmless, expected FAILED.
set -euo pipefail
: "${CORPUS:?export CORPUS}" "${MANIFEST:?export MANIFEST}" "${OUT:?export OUT}"
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
SIF=$BIRDOLOGY_ROOT/apps/containers/tf.sif
REPO=$BIRDOLOGY_ROOT/apps/birdology
[ -f "$MANIFEST" ] || { echo "FATAL: no manifest at $MANIFEST"; exit 2; }
# Many tasks starting together on one node can race creating OUT on NFS
# (measured: 8 tasks failed with "mkdir: Already exists"); tolerate that.
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
    python3 -m birdology.phase_a.embed_cli \
        --shard "$SHARD" --manifest "$MANIFEST" --out "$OUT" --encoder birdnet --with-logits
