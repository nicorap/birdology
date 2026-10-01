#!/usr/bin/env bash
# WEKA vs NFS vs node-local RAID on dgx171 — the only node with /faic/weka today.
#
# Aimed at Phase A of the bioacoustics sweep, whose real pattern is:
#   read a large audio corpus (many medium files, concurrent readers)
#   write compact embeddings back (sequential, modest volume)
#
# All tests --direct=1: the node has 2 TB RAM, so the page cache would otherwise
# report memory bandwidth and call it storage. Same node and same fio build for
# every target, so the comparison is internally valid even if absolute numbers
# differ from a vendor sheet.
set -u

WEKA=${WEKA_DIR:-/faic/weka/ML/bench}
NFS=/shared/apps/ML/_bench
RAID=/raid/scratch/_bench

RUNTIME=${RUNTIME:-20}
SIZE=${SIZE:-2G}

echo "host=$(hostname -s)  user=$(id -un)  $(date -Is)"
echo "fio=$(fio --version)"
echo

run_one() {
  local dir="$1" name="$2" rw="$3" bs="$4" jobs="$5" iodepth="$6"
  mkdir -p "$dir" 2>/dev/null || { printf '%-8s %-22s SKIP (cannot mkdir)\n' "$name" "$rw/$bs/j$jobs"; return; }
  local out
  out=$(fio --name=t --directory="$dir" --rw="$rw" --bs="$bs" \
            --size="$SIZE" --numjobs="$jobs" --iodepth="$iodepth" \
            --direct=1 --ioengine=libaio --group_reporting \
            --runtime="$RUNTIME" --time_based --ramp_time=3 \
            --output-format=json 2>/dev/null)
  if [ -z "$out" ]; then
    printf '%-8s %-22s FAILED\n' "$name" "$rw/$bs/j$jobs"; return
  fi
  python3 - "$rw" <<PY
import json,sys
d=json.loads('''$out''')
j=d["jobs"][0]; rw=sys.argv[1]
k="read" if "read" in rw else "write"
bw=j[k]["bw_bytes"]/1e6; iops=j[k]["iops"]; lat=j[k]["clat_ns"]["mean"]/1e6
print(f"%-8s %-22s {bw:10.1f} MB/s {iops:12.0f} IOPS {lat:9.2f} ms" % ("$name","$rw/$bs/j$jobs"))
PY
  rm -f "$dir"/t.* 2>/dev/null
}

for target in "weka:$WEKA" "nfs:$NFS" "raid:$RAID"; do
  name=${target%%:*}; dir=${target#*:}
  echo "=== $name  ($dir)"
  run_one "$dir" "$name" write     1M  1  16
  run_one "$dir" "$name" write     1M 16  16
  run_one "$dir" "$name" read      1M  1  16
  run_one "$dir" "$name" read      1M 16  16
  run_one "$dir" "$name" randread  1M 16  16   # ~ audio-file-sized reads, concurrent
  run_one "$dir" "$name" randread  4k 16  32   # metadata / small-read floor
  echo
done

echo "cleanup"
rm -rf "$NFS" "$RAID" 2>/dev/null
rm -rf "$WEKA" 2>/dev/null
echo done
