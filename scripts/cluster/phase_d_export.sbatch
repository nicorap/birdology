#!/usr/bin/env bash
#SBATCH --job-name=phase-d-export
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=/srv/home/rapnic/birdology/data/phase_a/logs/phase_d_export_%j.out
#
# Export BirdNET 2.4 + a Danish head as one SavedModel / TFLite, then check it
# on DKX audio (scripts/phase_d_export.py). Three steps in two environments:
# head.pt -> arrays in the phase_c venv (torch), build + check in tf.sif.
#
#   export HEAD=data/phase_d/dk_head_v5/head.pt OUT=data/phase_d/export_v5
#   sbatch scripts/cluster/phase_d_export.sbatch
set -euo pipefail
: "${HEAD:?export HEAD}" "${OUT:?export OUT}"
B=/srv/home/rapnic/birdology
SIF=$B/apps/containers/tf.sif
REPO=$B/apps/birdology
cd "$B"
mkdir -p "$OUT"
PYTHONPATH=$REPO/src $B/apps/venvs/phase_c/bin/python -P $REPO/scripts/phase_d_export.py \
    arrays --head "$HEAD" --out "$OUT/head_arrays.npz"
run_tf() {
    apptainer exec --nv --bind "$B":"$B","$REPO":/opt/birdology "$SIF" \
        env HOME=/tmp PYTHONPATH=/opt/birdology/src TF_FORCE_GPU_ALLOW_GROWTH=true \
        python3 /opt/birdology/scripts/phase_d_export.py "$@"
}
run_tf build --arrays "$OUT/head_arrays.npz" --out "$OUT"
run_tf check --export "$OUT"
