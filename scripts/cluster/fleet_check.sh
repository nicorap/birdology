#!/bin/bash
#SBATCH --job-name=ml-fleet
#SBATCH --partition=gpu
#SBATCH --nodes=24
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=00:25:00
#SBATCH --chdir=/tmp
#SBATCH --output=/shared/apps/ML/logs/fleet-%j.out
#SBATCH --error=/shared/apps/ML/logs/fleet-%j.out

# Fleet-wide check: does the SIF start and see a GPU on every node, including
# dgx184-187 which have no system apptainer/enroot/pyxis (module binary only)?
export APPTAINER_CACHEDIR=/tmp/appt/cache
export APPTAINER_TMPDIR=/tmp/appt/tmp
export HOME=/tmp/appt/home
mkdir -p "$APPTAINER_CACHEDIR" "$APPTAINER_TMPDIR" "$HOME"

APPT=$(command -v apptainer || ls -d /shared/apps/software/Apptainer/*/bin/apptainer 2>/dev/null | head -1)
SIF=/shared/apps/ML/containers/pytorch.sif

srun --export=ALL bash -c '
  export APPTAINER_CACHEDIR=/tmp/appt/cache APPTAINER_TMPDIR=/tmp/appt/tmp HOME=/tmp/appt/home
  mkdir -p $APPTAINER_CACHEDIR $APPTAINER_TMPDIR $HOME
  A=$(command -v apptainer || ls -d /shared/apps/software/Apptainer/*/bin/apptainer 2>/dev/null | head -1)
  SRC=$([ -n "$(command -v apptainer)" ] && echo system || echo module)
  OUT=$("$A" exec --nv '"$SIF"' python -c "
import torch,time
a=torch.randn(4096,4096,device=\"cuda\",dtype=torch.bfloat16)
b=torch.randn(4096,4096,device=\"cuda\",dtype=torch.bfloat16)
for _ in range(3): c=a@b
torch.cuda.synchronize(); t=time.time()
for _ in range(10): c=a@b
torch.cuda.synchronize()
print(f\"{torch.cuda.get_device_name(0)}|{2*4096**3/((time.time()-t)/10)/1e12:.0f}\")
" 2>/dev/null | tail -1)
  printf "%s appt=%s %s\n" "$(hostname -s)" "$SRC" "${OUT:-FAILED}"
' 2>&1 | sort

echo FLEET_DONE
