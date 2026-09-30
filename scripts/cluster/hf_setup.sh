#!/usr/bin/env bash
# One-time setup: a venv with the HF stack, on /shared/apps/ML so it is visible
# from every node (not on /mnt/weka, which today exists only on dgx171).
#
# The Apptainer image has soundfile but NOT huggingface_hub or datasets, so the
# download tooling has to come from PyPI. PyPI is reachable from compute nodes.
set -euo pipefail

VENV=/shared/apps/ML/venvs/hf
mkdir -p "$(dirname "$VENV")"

if [ -d "$VENV" ]; then
  echo "venv already present at $VENV"
else
  python3 -m venv "$VENV"
fi

"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet 'huggingface_hub[hf_transfer]>=0.26' 'datasets>=3.0'

echo "--- versions ---"
"$VENV/bin/python" -c "
import huggingface_hub, datasets
print('huggingface_hub', huggingface_hub.__version__)
print('datasets', datasets.__version__)
"
echo "venv ready: $VENV"
