#!/usr/bin/env bash
# Task 6.1 -- build tf.sif and stock the weights depot. Runs as ONE srun job.
#
# tf.sif: TF 2.20.0-gpu base + perch-hoplite + birdnet + our I/O deps.
#   - TF >= 2.20 is REQUIRED by the Perch v2 GPU SavedModel (Kaggle model card).
#   - birdnet's GPU path is the ProtoBuf backend (TFLite is CPU-only), which
#     needs full TF -- same base serves both.
# Weights: downloaded ONCE to /shared/apps/ML/models (shared, node-agnostic);
#   KAGGLEHUB_CACHE and BIRDNET_APP_DATA point there at runtime.
set -uo pipefail

CONTAINERS=/shared/apps/ML/containers
MODELS=/shared/apps/ML/models
DEF=/shared/apps/ML/tf.def
TMP=${TMPDIR:-/raid/scratch}/tf_sif_build.$$

echo "=== 1. write the def file ==="
cat > "$DEF" <<'DEF'
Bootstrap: docker
From: tensorflow/tensorflow:2.20.0-gpu

%post
    apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
        libsndfile1 ffmpeg && rm -rf /var/lib/apt/lists/*
    # tensorflow[and-cuda] OVERWRITES the base TF with one whose CUDA libs
    # come as nvidia-*-cu12 pip wheels in site-packages -- the docker-image
    # CUDA layout is not fully exposed under apptainer and TF saw ZERO GPUs
    # (measured: libcuda loads, built_with_cuda True, dlopen of the cu-libs
    # fails, GPUs []). The wheel layout is found via python paths, so it
    # survives the containerization.
    pip install --no-cache-dir 'tensorflow[and-cuda]==2.20.0'
    pip install --no-cache-dir \
        perch-hoplite birdnet soundfile pyarrow h3 librosa
%environment
    export TF_FORCE_GPU_ALLOW_GROWTH=true
    export TF_CPP_MIN_LOG_LEVEL=1
DEF

echo "=== 2. build (node-local tmp; WEKA/NFS would be slow for the sandbox) ==="
mkdir -p "$TMP" "$CONTAINERS" "$MODELS"
export APPTAINER_TMPDIR="$TMP"
apptainer build --force "$TMP/tf.sif" "$DEF"
rc=$?
if [ $rc -ne 0 ]; then echo "BUILD FAILED rc=$rc"; rm -rf "$TMP"; exit $rc; fi
mv "$TMP/tf.sif" "$CONTAINERS/tf.sif"
rm -rf "$TMP"
ls -la "$CONTAINERS/tf.sif"

echo "=== 3. weights depot ==="
# Perch v2 via kagglehub (anonymous for public models); BirdNET via its own
# Zenodo downloader. HOME must be writable (job HOME is not) for both.
apptainer exec --bind /shared/apps/ML:/shared/apps/ML "$CONTAINERS/tf.sif" \
  env HOME=/tmp \
      KAGGLEHUB_CACHE="$MODELS/kagglehub" \
      BIRDNET_APP_DATA="$MODELS/birdnet" \
  python3 - <<'PYEOF'
import os
print("--- perch v2 download ---")
import kagglehub
p = kagglehub.model_download("google/bird-vocalization-classifier/tensorFlow2/perch_v2")
print("perch_v2 at:", p)
import pathlib
total = sum(f.stat().st_size for f in pathlib.Path(p).rglob("*") if f.is_file())
print(f"perch_v2 size: {total/1e9:.2f} GB")

print("--- birdnet v2.4 pb download ---")
import birdnet
m = birdnet.load("acoustic", "2.4", "pb")
print("birdnet loaded:", type(m).__name__)
PYEOF
rc=$?
[ $rc -ne 0 ] && { echo "WEIGHTS FAILED rc=$rc"; exit $rc; }

echo "=== 4. GPU verification: both models embed a 1 s sine ==="
# A REAL FILE, not a heredoc: birdnet's spawn workers re-execute the main
# module via runpy, and a <stdin> main kills every worker with
# FileNotFoundError(/tmp/<stdin>). Measured 2026-09-30.
apptainer exec --nv --bind /shared/apps/ML:/shared/apps/ML "$CONTAINERS/tf.sif" \
  env HOME=/tmp \
      KAGGLEHUB_CACHE="$MODELS/kagglehub" \
      BIRDNET_APP_DATA="$MODELS/birdnet" \
      TF_FORCE_GPU_ALLOW_GROWTH=true \
  python3 /shared/apps/ML/verify_encoders.py
rc=$?
[ $rc -ne 0 ] && { echo "VERIFY FAILED rc=$rc"; exit $rc; }
echo "ALL DONE"
