#!/usr/bin/env bash
# Daily incremental refresh: run the FULL pipeline with an incremental fetch
# (build --update → enrich → reason → reload). Delegates to refresh.py so the
# graph the web server loads is always enriched + reasoned.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

uv run python scripts/refresh.py --incremental
