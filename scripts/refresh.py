"""Orchestrate the full Birdology graph refresh: build → enrich → reason →
(optional wiki reindex) → reload web server.

The step sequence is computed as pure data (`build_plan`) and executed by a
separate runner (`run_plan`), so ordering and flag handling are unit-testable
without spawning real subprocesses.

Usage:
    uv run python scripts/refresh.py                 # full rebuild + reload
    uv run python scripts/refresh.py --incremental   # fast obs-only update
    uv run python scripts/refresh.py --reindex       # also rebuild wiki index (needs Ollama)
    uv run python scripts/refresh.py --dry-run       # print the plan, run nothing

Cron suggestion:
    # daily fast update at 04:00
    0 4 * * *  cd /path/to/birdology && uv run python scripts/refresh.py --incremental
    # weekly full refresh (with wiki reindex) Sunday 03:00
    0 3 * * 0  cd /path/to/birdology && uv run python scripts/refresh.py --reindex
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_LOG_PATH = _PROJECT_DIR / "logs" / "refresh.log"


@dataclass(frozen=True)
class Step:
    name: str
    argv: list[str] = field(default_factory=list)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Refresh the Birdology knowledge graph.")
    p.add_argument("--incremental", action="store_true",
                   help="Fast observations-only update (build_graph --update).")
    p.add_argument("--dof-max", type=int, default=20000,
                   help="Max DOF occurrences to fetch (default: 20000).")
    p.add_argument("--reindex", action="store_true",
                   help="Also rebuild the Wikipedia RAG index (needs Ollama).")
    p.add_argument("--no-reload", action="store_true",
                   help="Do not reload the running web server afterwards.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the resolved step plan without executing.")
    return p.parse_args(argv)


def build_plan(args: argparse.Namespace) -> list[Step]:
    build = ["uv", "run", "python", "scripts/build_graph.py"]
    if args.incremental:
        build.append("--update")
    build += ["--dof-max", str(args.dof_max)]

    plan = [
        Step("build_graph", build),
        Step("enrich_dbpedia",
             ["uv", "run", "python", "scripts/enrich_dbpedia.py",
              "--input", "output/birdology.ttl"]),
        Step("reason", ["uv", "run", "python", "scripts/reason.py"]),
    ]
    if args.reindex:
        plan.append(Step("build_wiki_index",
                         ["uv", "run", "python", "scripts/build_wiki_index.py",
                          "--input", "output/birdology.ttl", "--observed-only"]))
    if not args.no_reload:
        plan.append(Step("reload_web"))
    return plan


def log(msg: str) -> None:
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line)
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _LOG_PATH.open("a") as fh:
        fh.write(line + "\n")


def _find_web_pid(pgrep=subprocess.run) -> int | None:
    """Find the PID of the running web_chat.py process, or None if not running."""
    result = pgrep(["pgrep", "-f", "web_chat.py"], capture_output=True, text=True)
    pids = [line for line in result.stdout.split() if line.strip()]
    return int(pids[0]) if pids else None


def _spawn_web(popen=subprocess.Popen) -> None:
    """Spawn the web_chat.py server against the reasoned graph."""
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log_fh = _LOG_PATH.open("a")
    popen(
        ["uv", "run", "python", "scripts/web_chat.py",
         "--input", "output/birdology_reasoned.ttl"],
        cwd=str(_PROJECT_DIR), stdout=log_fh, stderr=log_fh,
        start_new_session=True,
    )


def reload_web_server() -> None:
    """Reload the running web server so it picks up the fresh reasoned graph.

    Fully implemented in Task 3.
    """
    pid = _find_web_pid()
    if pid is None:
        log("No running web server — skipping reload.")
        return
    log(f"Reloading web server (pid {pid}) on the reasoned graph…")
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
    except ProcessLookupError:
        log(f"Web server (pid {pid}) already gone; launching a fresh one.")
    _spawn_web()
    log("Web server relaunched.")


def run_plan(plan, *, runner=subprocess.run, reloader=reload_web_server) -> None:
    for step in plan:
        log(f"→ {step.name}")
        if step.name == "reload_web":
            reloader()
        else:
            runner(step.argv, check=True)
        log(f"✓ {step.name}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    plan = build_plan(args)
    if args.dry_run:
        for step in plan:
            print(f"{step.name}: {' '.join(step.argv)}")
        return 0
    log(f"Refresh started ({'incremental' if args.incremental else 'full'}).")
    try:
        run_plan(plan)
    except Exception as e:
        log(f"✗ Refresh failed: {e}")
        return 1
    log("Refresh complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
