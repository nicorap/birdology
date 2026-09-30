#!/usr/bin/env bash
# Verification pass: is the RAID number honest, and does WEKA hold up at a
# working-set size that cannot be served from cache?
#
# Why: the first run reported raid read at ~100 GB/s over 16 jobs x 2G = 32 GB.
# That is far above any plausible NVMe array, and 32 GB fits trivially in 2 TB
# of RAM, so O_DIRECT was probably not reaching the device. Re-run with a
# working set far larger than any cache could hide, same flags, same node.
set -u

report() {  # <label> <rw> <json>
  python3 -c "
import json,sys
raw=sys.stdin.read()
try: d=json.loads(raw)
except Exception: print('$1: FAILED (no json)'); raise SystemExit
j=d['jobs'][0]
k='read' if 'read' in '$2' else 'write'
s=j[k]
print(f\"$1: {s['bw_bytes']/1e6:9.0f} MB/s  {s['iops']:10.0f} IOPS  lat {s['clat_ns']['mean']/1e6:7.2f} ms\")
"
}

echo "host=$(hostname -s) user=$(id -un) $(date -Is)"
echo "--- array composition ---"
head -4 /proc/mdstat
echo "--- nvme count ---"
ls /dev/nvme*n1 2>/dev/null | wc -l
echo

# 16 jobs x 40G = 640 GB working set. Cannot be cached in 2 TB alongside
# everything else, and each job's file alone (40G) exceeds any readahead.
for spec in "raid:/raid/scratch/_v" "weka:/mnt/weka/ML/_v"; do
  name=${spec%%:*}; dir=${spec#*:}
  mkdir -p "$dir" || { echo "$name: cannot mkdir"; continue; }
  echo "=== $name  16 jobs x 40G = 640 GB working set, O_DIRECT"
  fio --name=t --directory="$dir" --rw=read --bs=1M --size=40G \
      --numjobs=16 --iodepth=16 --direct=1 --ioengine=libaio \
      --group_reporting --runtime=60 --time_based --ramp_time=10 \
      --output-format=json 2>/dev/null | report "$name read 640G" read
  fio --name=t --directory="$dir" --rw=write --bs=1M --size=40G \
      --numjobs=16 --iodepth=16 --direct=1 --ioengine=libaio \
      --group_reporting --runtime=60 --time_based --ramp_time=10 \
      --output-format=json 2>/dev/null | report "$name write 640G" write
  rm -rf "$dir"
  echo
done
echo done
