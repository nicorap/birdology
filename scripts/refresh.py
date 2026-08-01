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
import re
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_LOG_PATH = _PROJECT_DIR / "logs" / "refresh.log"


def _default_port() -> int:
    return int(os.environ.get("BIRDOLOGY_PORT", "5000"))


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
    p.add_argument("--port", type=int, default=None,
                   help="Port for the relaunched web server. Default: reuse the port "
                        "the running server is on, else $BIRDOLOGY_PORT, else 5000.")
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


def _running_web_port(ps=subprocess.run) -> int | None:
    """Port the running web_chat.py process was started on, or None.

    Reads the port back off the process's own command line. Uses `ps` rather than
    `pgrep -a`: the -a flag is Linux-only and macOS pgrep silently prints bare PIDs.
    """
    pid = _find_web_pid()
    if pid is None:
        return None
    result = ps(["ps", "-o", "args=", "-p", str(pid)], capture_output=True, text=True)
    # Both spellings argparse accepts: "--port 5001" and "--port=5001". Matching only
    # the space form sent us back to the 5000 default — the AirPlay-occupied port this
    # whole code path exists to avoid.
    match = re.search(r"--port[=\s]+(\d+)", result.stdout or "")
    return int(match.group(1)) if match else None


def _wait_for_port(
    port: int,
    host: str = "127.0.0.1",
    attempts: int = 20,
    delay: float = 0.5,
    connect=socket.create_connection,
    sleep=time.sleep,
) -> bool:
    """Poll until something is accepting connections on *port*, or give up.

    The server takes a moment to bind (uv start-up + graph load), so a single probe
    would false-alarm; and Popen returning tells us only that the process started, not
    that it came up.
    """
    for attempt in range(attempts):
        try:
            sock = connect((host, port), timeout=1)
        except OSError:
            if attempt < attempts - 1:
                sleep(delay)
            continue
        sock.close()
        return True
    return False


def _spawn_web(port: int | None = None, popen=subprocess.Popen) -> None:
    """Spawn the web_chat.py server against the reasoned graph."""
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log_fh = _LOG_PATH.open("a")
    argv = ["uv", "run", "python", "scripts/web_chat.py",
            "--input", "output/birdology_reasoned.ttl"]
    if port is not None:
        argv += ["--port", str(port)]
    popen(
        argv,
        cwd=str(_PROJECT_DIR), stdout=log_fh, stderr=log_fh,
        start_new_session=True,
    )


def reload_web_server(port: int | None = None) -> None:
    """Reload the running web server so it picks up the fresh reasoned graph.

    Relaunches on the port the server was already serving on, so a refresh never
    silently moves it (web_chat's own default of 5000 is unusable on macOS,
    where AirPlay Receiver holds that port).
    """
    pid = _find_web_pid()
    if pid is None:
        log("No running web server — skipping reload.")
        return
    target_port = port or _running_web_port() or _default_port()
    log(f"Reloading web server (pid {pid}) on the reasoned graph, port {target_port}…")
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
    except ProcessLookupError:
        log(f"Web server (pid {pid}) already gone; launching a fresh one.")
    _spawn_web(port=target_port)

    # Popen returning means the process started, not that it bound the port. Without
    # this check a server that died on startup (port taken, missing graph) was still
    # reported as a successful relaunch, and the refresh looked clean while the site
    # was down.
    if not _wait_for_port(target_port):
        msg = (
            f"Web server did not come up on port {target_port} — the new process "
            f"started but never accepted a connection. See {_LOG_PATH}."
        )
        log(f"✗ {msg}")
        raise RuntimeError(msg)
    log(f"Web server relaunched on port {target_port}.")


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
        run_plan(plan, reloader=partial(reload_web_server, args.port))
    except Exception as e:
        log(f"✗ Refresh failed: {e}")
        return 1
    log("Refresh complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
