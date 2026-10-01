#!/bin/bash
#SBATCH --job-name=wekabench
#SBATCH --partition=gpu
#SBATCH --time=00:45:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=32
#SBATCH --output=/shared/apps/ML/logs/wekabench_%A_%a.out
#
# WEKA performance benchmark — run after the 2026-10-01 reconfig to measure
# what the re-mounted client actually delivers from the DGX nodes.
#
# Usage:
#   Single node:     sbatch /shared/apps/ML/wekabench.sbatch
#   Fleet aggregate: sbatch --array=0-11 /shared/apps/ML/wekabench.sbatch
#     (every task runs the same phases simultaneously on distinct files;
#      aggregate = sum of per-task MB/s in the same phase window)
#
# Baselines measured BEFORE the reconfig (2026-09-30/10-01, possibly the
# misconfigured client — that is the point of comparison):
#   - 196 concurrent array tasks sustained ~330 MB/s aggregate writes
#   - embed array 4494: 6.5 TB read / 5.3 TB write over ~5 h
#   - /raid/scratch local staging: ~5.3 GB/s (the node-local anchor)
# WEKA on NDR fabric should deliver GB/s-class per node, not hundreds of MB/s.
set -uo pipefail
mountpoint -q /mnt/weka || { echo "FATAL: /mnt/weka not mounted on $(hostname -s)"; exit 2; }

IDX=${SLURM_ARRAY_TASK_ID:-solo}
HOST=$(hostname -s)
D=/mnt/weka/ML/bench/${SLURM_JOB_ID}_${IDX}_${HOST}
mkdir -p "$D" /shared/apps/ML/logs
trap 'rm -rf "$D"' EXIT

echo "=== wekabench $HOST task=$IDX job=${SLURM_JOB_ID} $(date -u +%FT%TZ) ==="
echo "--- mount options ---"
grep -m1 ' /mnt/weka ' /proc/mounts || mount | grep -m1 weka
command -v weka >/dev/null 2>&1 && weka status 2>/dev/null | head -5

bw() { # bw <bytes> <seconds>
  python3 -c "print(f'{$1/$2/1e6:.0f} MB/s ({$1/1e9:.1f} GB in {$2:.1f}s)')"
}

# Phase 1: single-stream sequential write, 16 GB, direct-ish via conv=fsync
echo "--- P1 seq write 1x16GB ---"
S=$(date +%s.%N)
dd if=/dev/zero of="$D/w1" bs=16M count=1000 conv=fsync 2>/dev/null
E=$(date +%s.%N); bw $((16*1000*1024*1024)) "$(echo "$E-$S" | bc)"

# Phase 2: 8-stream parallel sequential write, 8x8 GB
echo "--- P2 seq write 8x8GB parallel ---"
S=$(date +%s.%N)
for i in $(seq 1 8); do dd if=/dev/zero of="$D/w8_$i" bs=16M count=500 conv=fsync 2>/dev/null & done; wait
E=$(date +%s.%N); bw $((8*500*16*1024*1024)) "$(echo "$E-$S" | bc)"

# Phase 3: single-stream sequential read (drop nothing — WEKA is remote; re-read w1)
echo "--- P3 seq read 1x16GB ---"
S=$(date +%s.%N)
dd if="$D/w1" of=/dev/null bs=16M 2>/dev/null
E=$(date +%s.%N); bw $((16*1000*1024*1024)) "$(echo "$E-$S" | bc)"

# Phase 4: 8-stream parallel read
echo "--- P4 seq read 8x8GB parallel ---"
S=$(date +%s.%N)
for i in $(seq 1 8); do dd if="$D/w8_$i" of=/dev/null bs=16M 2>/dev/null & done; wait
E=$(date +%s.%N); bw $((8*500*16*1024*1024)) "$(echo "$E-$S" | bc)"

# Phase 5: small-file metadata (create+stat+delete 2000 files) — the parquet
# shard pattern: many small opens hurt most when a client is misconfigured
echo "--- P5 metadata 2000 x 64KB create/stat/delete ---"
S=$(date +%s.%N)
mkdir -p "$D/meta"
python3 - "$D/meta" <<'EOF'
import sys, os
d = sys.argv[1]
buf = os.urandom(65536)
for i in range(2000):
    p = f"{d}/f{i:05d}"
    with open(p, "wb") as f: f.write(buf)
for i in range(2000):
    os.stat(f"{d}/f{i:05d}")
for i in range(2000):
    os.unlink(f"{d}/f{i:05d}")
EOF
E=$(date +%s.%N)
python3 -c "print(f'{6000/($E-$S):.0f} ops/s ({$E-$S:.1f}s for 6000 ops)')"

# Phase 6: fio random 4k if available (apptainer image carries it on some nodes)
if command -v fio >/dev/null 2>&1; then
  echo "--- P6 fio randread 4k QD32 30s ---"
  fio --name=rr --directory="$D" --rw=randread --bs=4k --size=4G --iodepth=32 \
      --direct=1 --runtime=30 --time_based --group_reporting --output-format=terse \
      2>/dev/null | awk -F';' '{print $8" KB/s read, "$9" iops"}' || echo "fio failed"
else
  echo "--- P6 skipped (no fio) ---"
fi

echo "=== done $HOST $(date -u +%FT%TZ) ==="
