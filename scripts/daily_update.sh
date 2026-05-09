#!/usr/bin/env bash
# Daily incremental update: fetch new eBird observations and reload web server.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="$PROJECT_DIR/logs/daily_update.log"
mkdir -p "$PROJECT_DIR/logs"

echo "[$(date -Iseconds)] Starting daily update…" >> "$LOG"

cd "$PROJECT_DIR"

# Incremental update (eBird last N days since lastFetchDate)
uv run python scripts/build_graph.py --update --dof-max 5000 >> "$LOG" 2>&1

echo "[$(date -Iseconds)] Update complete." >> "$LOG"

# Reload web server if running (sends SIGHUP to Flask to reload gracefully)
WEB_PID=$(pgrep -f "python.*web_chat.py" | head -1 || true)
if [ -n "$WEB_PID" ]; then
    echo "[$(date -Iseconds)] Restarting web server (PID $WEB_PID)…" >> "$LOG"
    kill "$WEB_PID"
    sleep 2
    nohup uv run python scripts/web_chat.py >> "$LOG" 2>&1 &
    echo "[$(date -Iseconds)] Web server restarted." >> "$LOG"
fi
