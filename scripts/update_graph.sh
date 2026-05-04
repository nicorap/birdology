#!/usr/bin/env bash
# Daily update job: rebuild the eBird+DOF graph, then run the reasoner.
# Reads EBIRD_API_KEY (and optional DOF_MAX) from the environment.
set -euo pipefail

DOF_MAX="${DOF_MAX:-50000}"
OUTPUT_DIR="${OUTPUT_DIR:-/app/output}"

cd /app

echo "[$(date -Iseconds)] building graph (dof-max=${DOF_MAX})"
python scripts/build_graph.py --dof-max "${DOF_MAX}"

echo "[$(date -Iseconds)] running reasoner"
python scripts/reason.py

echo "[$(date -Iseconds)] update complete"
ls -lh "${OUTPUT_DIR}"/*.ttl
