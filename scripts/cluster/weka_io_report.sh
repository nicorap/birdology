#!/usr/bin/env bash
# weka_io_report.sh — per-job disk I/O attribution from Slurm accounting.
#
# Tier 1 of the WEKA performance record (see AGENTS.md / cluster acceptance
# notes): sacct's MaxDiskRead/MaxDiskWrite come from /proc/<pid>/io sampled by
# slurmstepd, so this is free, retroactive, and needs no WEKA credentials.
#
# KNOWN LIMITS (why this is "tier 1", not the whole story):
#   * byte counts are ALL disk I/O of the batch step, not WEKA-specific —
#     /raid/scratch staging and container image reads are included;
#   * no time resolution: aggregate rate = bytes / task-walltime, which mixes
#     GPU compute time into the denominator. Peak/burst rates need
#     cluster-side `weka stats` (read-only account pending) or prolog/epilog
#     /proc/self/io snapshots (tier 3).
#
# Usage:
#   ./weka_io_report.sh 4493,4494,4715          # explicit job ids
#   ./weka_io_report.sh --since 2026-09-30      # every job since a date
#
# Measured baseline (2026-09-30 night, the XCL production run): 11.3 TB read
# + 7.1 TB written through /mnt/weka by 396 tasks, zero I/O errors.
set -euo pipefail

if [ "${1:-}" = "--since" ]; then
    [ -n "${2:-}" ] || { echo "usage: $0 --since YYYY-MM-DD | $0 <jobids>"; exit 2; }
    SACCT_ARGS=(-S "$2" --allusers)
else
    [ -n "${1:-}" ] || { echo "usage: $0 --since YYYY-MM-DD | $0 <jobids>"; exit 2; }
    SACCT_ARGS=(-j "$1")
fi

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
sacct "${SACCT_ARGS[@]}" -n \
    --format=JobID%20,JobName%16,State%12,Elapsed,MaxDiskRead,MaxDiskWrite,NodeList%14 \
    > "$TMP" || true   # parent rows carry the real JobName; .batch rows the I/O

python3 - "$TMP" <<'PYEOF'
import sys
from collections import defaultdict

def to_mb(x: str) -> float:
    if x.endswith("M"): return float(x[:-1])
    if x.endswith("G"): return float(x[:-1]) * 1024
    if x.endswith("K"): return float(x[:-1]) / 1024
    try:
        return float(x) / 1e6
    except ValueError:
        return 0.0

rows = []
names = {}
for line in open(sys.argv[1]):
    f = line.split()
    if len(f) < 4:
        continue
    jid, name = f[0], f[1]
    if ".batch" not in jid and ".extern" not in jid:
        names[jid.split("_")[0]] = name   # parent row: real JobName
        continue
    if ".batch" not in jid or len(f) < 6:
        continue
    base = jid.replace(".batch", "")
    elapsed = f[3]
    rd = to_mb(f[4]) if len(f) > 4 else 0.0
    wr = to_mb(f[5]) if len(f) > 5 else 0.0
    t = elapsed.split("-")
    hms = t[-1].split(":")
    if len(hms) != 3:
        continue
    h, m, s = map(int, hms)
    secs = (int(t[0]) * 86400 if len(t) > 1 else 0) + h * 3600 + m * 60 + s
    rows.append((base.split("_")[0], rd, wr, secs))

if not rows:
    print("no .batch steps found — check job ids / date range")
    sys.exit(0)

agg = defaultdict(lambda: [0, 0.0, 0.0, 0])
for arr, rd, wr, secs in rows:
    a = agg[arr]
    a[0] += 1
    a[1] += rd
    a[2] += wr
    a[3] += secs

print("%8s %-16s %5s %9s %9s %9s %12s"
      % ("job", "name", "tasks", "read_GB", "write_GB", "task_hrs", "agg_rd_MBps"))
tot_r = tot_w = 0.0
for arr in sorted(agg):
    n, rd, wr, secs = agg[arr]
    tot_r += rd
    tot_w += wr
    name = names.get(arr, "?")
    print("%8s %-16s %5d %9.1f %9.1f %9.2f %12.1f"
          % (arr, name[:16], n, rd / 1024, wr / 1024, secs / 3600, rd / max(secs, 1)))
print("TOTAL read %.1f GB, write %.1f GB" % (tot_r / 1024, tot_w / 1024))
PYEOF
