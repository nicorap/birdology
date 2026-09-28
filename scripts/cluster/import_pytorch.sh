#!/bin/bash
#SBATCH --job-name=ml-import-torch
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --chdir=/tmp
#SBATCH --output=/shared/apps/ML/logs/import-%j.out
#SBATCH --error=/shared/apps/ML/logs/import-%j.out

# Jobs run as uid 1000 name 'nvidia' with an unreadable $HOME, so every enroot
# path must be redirected to node-local scratch or the import dies on mkdir.
export HOME=/raid/scratch/mlenroot
export XDG_CACHE_HOME=$HOME/.cache
export XDG_DATA_HOME=$HOME/.local/share
export XDG_RUNTIME_DIR=$HOME/run
export ENROOT_CACHE_PATH=$HOME/cache
export ENROOT_DATA_PATH=$HOME/data
export ENROOT_RUNTIME_PATH=$HOME/runtime
export ENROOT_TEMP_PATH=$HOME/tmp
export ENROOT_SQUASH_OPTIONS='-comp lz4 -noD'
mkdir -p "$ENROOT_CACHE_PATH" "$ENROOT_DATA_PATH" "$ENROOT_RUNTIME_PATH" \
         "$ENROOT_TEMP_PATH" "$XDG_CACHE_HOME" "$XDG_DATA_HOME" "$XDG_RUNTIME_DIR"

IMAGE="${1:-nvidia/pytorch:25.08-py3}"
OUT=/shared/apps/ML/containers/pytorch.sqsh
STAGE="$ENROOT_TEMP_PATH/pytorch.sqsh"

echo "host=$(hostname -s) image=$IMAGE"
echo "=== import (staging to node-local $STAGE) ==="
time enroot import -o "$STAGE" "docker://nvcr.io#${IMAGE}" || { echo IMPORT_FAILED; exit 1; }
ls -lh "$STAGE"

echo "=== copy to shared ==="
time cp "$STAGE" "$OUT.tmp" && mv "$OUT.tmp" "$OUT"
ls -lh "$OUT"
echo IMPORT_OK
