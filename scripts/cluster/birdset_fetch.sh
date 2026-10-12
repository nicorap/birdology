#!/usr/bin/env bash
# Timed pilot download of ONE BirdSet config into WEKA.
#
# THE PATH THAT WORKS, and why the two obvious ones do not:
#   1. load_dataset(...)            -> RuntimeError: "Dataset scripts are no
#      longer supported, but found BirdSet.py". datasets 5.0.1 removed script
#      loading, so BirdSet's builder cannot run at all on a current stack.
#   2. snapshot_download(main)      -> 262 KB: the main revision holds only 7
#      files (loader script + docs). No audio.
# The audio and metadata live on the repo's separate `data` BRANCH (516 files),
# which snapshot_download CAN fetch with revision="data". That sidesteps the
# builder entirely -- we take the bytes and do our own decoding, which is what
# Phase A wants anyway (frozen encoders over raw audio, no HF Arrow layer).
#
# HSN deliberately, not XCL: 5.92 GB vs 484 GB. Answers "does this work and how
# fast" in minutes, and the baseline correctness gate needs HSN regardless.
#
# Jobs run as uid nvidia with $HOME unreadable, so HF_HOME and HOME must both
# point somewhere writable or huggingface_hub dies reading ~/.cache/.../token.
set -uo pipefail

VENV=/srv/home/rapnic/birdology/apps/venvs/hf
DEST=${DEST:-/srv/home/rapnic/birdology/data/birdset}
CONFIG=${CONFIG:-HSN}

[ -d "$VENV" ] || { echo "venv missing: $VENV"; exit 2; }
mkdir -p "$DEST/.hf" || { echo "cannot write $DEST"; exit 2; }

export HF_HOME="$DEST/.hf"
export HOME=/tmp

echo "host=$(hostname -s) user=$(id -un) $(date -Is)"
echo "dest=$DEST config=$CONFIG revision=data"
df -h "$DEST" | tail -1
echo

"$VENV/bin/python" - "$CONFIG" "$DEST" <<'PY'
import sys, time
from pathlib import Path
from huggingface_hub import snapshot_download, list_repo_files

config, dest = sys.argv[1], Path(sys.argv[2])
target = dest / config

files = list_repo_files("DBD-research-group/BirdSet", repo_type="dataset", revision="data")
mine = [f for f in files if f.startswith(f"{config}/")]
print(f"{config}: {len(mine)} files on the data branch")
for f in mine[:6]:
    print("   ", f)
if not mine:
    print("FAILED: no files matched"); raise SystemExit(1)

t0 = time.time()
path = snapshot_download(
    repo_id="DBD-research-group/BirdSet",
    repo_type="dataset",
    revision="data",
    local_dir=str(target),
    allow_patterns=[f"{config}/*"],
    max_workers=16,
)
dt = time.time() - t0

got = [f for f in Path(path).rglob("*") if f.is_file() and ".cache" not in f.parts]
total = sum(f.stat().st_size for f in got)
gb = total / 1e9
print()
print(f"files={len(got)}  GB={gb:.2f}  elapsed={dt:.0f}s  throughput={gb*1000/dt:.1f} MB/s")
if gb > 0 and dt > 0:
    rate = gb / (dt / 3600)
    print(f"sustained={rate:.1f} GB/h")
    print(f"projected XCL (484 GB) ~= {484/rate:.1f} h single-node at this rate")
    print(f"projected XCM ( 89 GB) ~= {89/rate:.1f} h")
ext = {}
for f in got:
    ext[f.suffix] = ext.get(f.suffix, 0) + 1
print("by extension:", dict(sorted(ext.items(), key=lambda kv: -kv[1])[:8]))
PY
rc=$?

echo
echo "--- on disk ---"
du -sh "$DEST/$CONFIG" 2>/dev/null
echo "exit=$rc"
