#!/bin/bash
#SBATCH --job-name=ml-sif
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=256G
#SBATCH --time=03:00:00
#SBATCH --chdir=/tmp
#SBATCH --output=/srv/home/rapnic/birdology/apps/logs/sif-%j.out
#SBATCH --error=/srv/home/rapnic/birdology/apps/logs/sif-%j.out

# Apptainer route. Pyxis/enroot cannot be used: pyxis derives its enroot paths
# from the passwd HOME (/home/ubuntu), which is unreadable inside a job, and it
# ignores --export, so container start always fails on "mkdir: Permission denied".
# Fixing that needs /etc/enroot/enroot.conf edited fleet-wide. Apptainer instead
# honours APPTAINER_* env vars, so it needs no privileged change.
#
# /tmp on the DGX nodes is a ~1 TB tmpfs (RAM-backed), fine for staging a 20 GB
# image on a 2 TB node.

set -euo pipefail
export APPTAINER_CACHEDIR=/tmp/appt/cache
export APPTAINER_TMPDIR=/tmp/appt/tmp
export HOME=/tmp/appt/home
mkdir -p "$APPTAINER_CACHEDIR" "$APPTAINER_TMPDIR" "$HOME"

APPT=$(command -v apptainer || ls -d /shared/apps/software/Apptainer/*/bin/apptainer 2>/dev/null | head -1)
IMAGE="${1:-docker://nvcr.io/nvidia/pytorch:25.08-py3}"
STAGE=/tmp/appt/pytorch.sif
OUT=/srv/home/rapnic/birdology/apps/containers/pytorch.sif

echo "host=$(hostname -s) apptainer=$APPT image=$IMAGE"
"$APPT" --version

echo "=== build SIF (staging to node-local $STAGE) ==="
time "$APPT" build --force "$STAGE" "$IMAGE"
ls -lh "$STAGE"

echo "=== verify GPU + torch inside the image ==="
"$APPT" exec --nv "$STAGE" python -c "
import torch, time
print('torch', torch.__version__, 'cuda', torch.version.cuda, 'avail', torch.cuda.is_available())
print('device', torch.cuda.get_device_name(0))
a = torch.randn(8192, 8192, device='cuda', dtype=torch.bfloat16)
b = torch.randn(8192, 8192, device='cuda', dtype=torch.bfloat16)
for _ in range(3): c = a @ b
torch.cuda.synchronize(); t = time.time()
n = 20
for _ in range(n): c = a @ b
torch.cuda.synchronize()
dt = (time.time() - t) / n
print(f'bf16 8192^3 matmul: {2*8192**3/dt/1e12:.1f} TFLOP/s')
x = torch.randn(64, 5*32000, device='cuda')
w = torch.hann_window(1024, device='cuda')
s = torch.stft(x, n_fft=1024, hop_length=320, window=w, return_complex=True)
print('on-GPU stft ok', tuple(s.shape))
import numpy, scipy; print('numpy', numpy.__version__, 'scipy', scipy.__version__)
"

echo "=== publish to shared ==="
time cp "$STAGE" "$OUT.tmp" && mv "$OUT.tmp" "$OUT"
ls -lh "$OUT"
echo SIF_OK
