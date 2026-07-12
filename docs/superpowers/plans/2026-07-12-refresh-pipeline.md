# Refresh Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single testable Python orchestrator (`scripts/refresh.py`) that runs the complete graph pipeline — build → enrich → reason → (optional reindex) → reload — so updates never again silently drop enrichment or reasoning.

**Architecture:** The step sequence is computed as pure data (`build_plan(args) -> list[Step]`), and a separate runner executes it. This split makes the ordering and flag handling unit-testable without touching the network or spawning real subprocesses. A thin CLI wraps both.

**Tech Stack:** Python 3.13 stdlib only (`argparse`, `subprocess`, `dataclasses`, `pathlib`, `datetime`), pytest, `uv run` for invocation.

## Global Constraints

- All Python is run via `uv run python ...` (project venv). Copy this verbatim into every step's argv for pipeline steps.
- TDD: write the failing test first, watch it fail, implement minimally, watch it pass, commit.
- Commit after each task. End every commit message with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
- Stdlib only — do not add dependencies.
- Default `--dof-max` is `20000`.
- Tests import the script via: `sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))` then `import refresh`.
- Pipeline step argv (exact, used across tasks):
  - build (full): `["uv","run","python","scripts/build_graph.py","--dof-max","<N>"]`
  - build (incremental): `["uv","run","python","scripts/build_graph.py","--update","--dof-max","<N>"]`
  - enrich: `["uv","run","python","scripts/enrich_dbpedia.py","--input","output/birdology.ttl"]`
  - reason: `["uv","run","python","scripts/reason.py"]`
  - reindex: `["uv","run","python","scripts/build_wiki_index.py","--input","output/birdology.ttl","--observed-only"]`
  - reload: a `Step` named `"reload_web"` with `argv == []` (the runner special-cases it; see Task 3).

---

## File Structure

- Create `scripts/refresh.py` — orchestrator: `Step`, `build_plan`, `parse_args`, `run_plan`, `reload_web_server`, `log`, `main`.
- Create `tests/test_refresh.py` — unit tests for planning + runner (no network, mocked subprocess).
- Modify `scripts/daily_update.sh` — call `refresh.py --incremental` instead of a bare build.

---

### Task 1: Planning core — `Step`, `parse_args`, `build_plan`

**Files:**
- Create: `scripts/refresh.py`
- Test: `tests/test_refresh.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Step: name: str; argv: list[str]`
  - `parse_args(argv: list[str] | None = None) -> argparse.Namespace` with attrs
    `incremental: bool`, `dof_max: int` (default 20000), `reindex: bool`,
    `no_reload: bool`, `dry_run: bool`.
  - `build_plan(args: argparse.Namespace) -> list[Step]`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_refresh.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import refresh


def _names(plan):
    return [s.name for s in plan]


def test_default_plan_is_full_build_enrich_reason_reload():
    plan = refresh.build_plan(refresh.parse_args([]))
    assert _names(plan) == ["build_graph", "enrich_dbpedia", "reason", "reload_web"]
    build = plan[0].argv
    assert "--update" not in build
    assert build[-2:] == ["--dof-max", "20000"]


def test_incremental_uses_update_flag():
    plan = refresh.build_plan(refresh.parse_args(["--incremental"]))
    assert "--update" in plan[0].argv


def test_custom_dof_max_passed_through():
    plan = refresh.build_plan(refresh.parse_args(["--dof-max", "500"]))
    assert plan[0].argv[-2:] == ["--dof-max", "500"]


def test_reindex_adds_wiki_step_before_reload():
    plan = refresh.build_plan(refresh.parse_args(["--reindex"]))
    assert _names(plan) == [
        "build_graph", "enrich_dbpedia", "reason", "build_wiki_index", "reload_web",
    ]


def test_no_reload_omits_reload_step():
    plan = refresh.build_plan(refresh.parse_args(["--no-reload"]))
    assert "reload_web" not in _names(plan)


def test_reload_step_has_empty_argv():
    plan = refresh.build_plan(refresh.parse_args([]))
    reload_step = [s for s in plan if s.name == "reload_web"][0]
    assert reload_step.argv == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_refresh.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'refresh'` (or `AttributeError`).

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/refresh.py
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
from dataclasses import dataclass, field


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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_refresh.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/refresh.py tests/test_refresh.py
git commit -m "refresh: add Step + build_plan planning core

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Runner + logging + `main`/`--dry-run`

**Files:**
- Modify: `scripts/refresh.py`
- Test: `tests/test_refresh.py`

**Interfaces:**
- Consumes: `Step`, `build_plan`, `parse_args` (Task 1).
- Produces:
  - `log(msg: str) -> None` — timestamped line to stdout and `logs/refresh.log`.
  - `run_plan(plan: list[Step], *, runner=subprocess.run, reloader=reload_web_server) -> None`
    — executes each step in order; subprocess steps call `runner(step.argv, check=True)`;
    the `reload_web` step calls `reloader()`. Raises on first failure (does not
    continue).
  - `main(argv: list[str] | None = None) -> int` — parses args, builds plan; with
    `--dry-run` prints `f"{name}: {' '.join(argv)}"` per step and returns 0 without
    running; otherwise calls `run_plan` and returns 0.
  - Note: `reload_web_server` is defined as a stub in this task (`pass`) and
    fully implemented in Task 3, so `run_plan` is testable now.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_refresh.py
import subprocess
import pytest


def test_run_plan_executes_steps_in_order():
    calls = []
    plan = [refresh.Step("a", ["x"]), refresh.Step("b", ["y"]), refresh.Step("reload_web")]
    refresh.run_plan(
        plan,
        runner=lambda argv, check: calls.append(("run", argv)),
        reloader=lambda: calls.append(("reload", None)),
    )
    assert calls == [("run", ["x"]), ("run", ["y"]), ("reload", None)]


def test_run_plan_aborts_on_first_failure():
    calls = []

    def boom(argv, check):
        calls.append(argv)
        raise subprocess.CalledProcessError(1, argv)

    plan = [refresh.Step("a", ["x"]), refresh.Step("b", ["y"])]
    with pytest.raises(subprocess.CalledProcessError):
        refresh.run_plan(plan, runner=boom, reloader=lambda: None)
    assert calls == [["x"]]  # stopped after the first step failed


def test_main_dry_run_prints_plan_and_runs_nothing(capsys):
    ran = []
    rc = refresh.main(["--dry-run"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "build_graph:" in out
    assert "reason:" in out
    assert "reload_web:" in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_refresh.py -v`
Expected: FAIL — `AttributeError: module 'refresh' has no attribute 'run_plan'`.

- [ ] **Step 3: Write minimal implementation**

Add these imports at the top of `scripts/refresh.py` (extend the existing import block):

```python
import subprocess
import sys
from datetime import datetime
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_LOG_PATH = _PROJECT_DIR / "logs" / "refresh.log"
```

Add these functions to `scripts/refresh.py` (below `build_plan`):

```python
def log(msg: str) -> None:
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line)
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _LOG_PATH.open("a") as fh:
        fh.write(line + "\n")


def reload_web_server() -> None:
    """Reload the running web server so it picks up the fresh reasoned graph.

    Fully implemented in Task 3.
    """
    pass


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
    run_plan(plan)
    log("Refresh complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_refresh.py -v`
Expected: PASS (9 tests total).

- [ ] **Step 5: Commit**

```bash
git add scripts/refresh.py tests/test_refresh.py
git commit -m "refresh: add runner, logging, and CLI main with --dry-run

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `reload_web_server` implementation

**Files:**
- Modify: `scripts/refresh.py`
- Test: `tests/test_refresh.py`

**Interfaces:**
- Consumes: `log` (Task 2).
- Produces: `reload_web_server()` — finds a running `web_chat.py` process; if
  present, terminates it and relaunches it against `output/birdology_reasoned.ttl`.
  Uses `subprocess`-level helpers that are injected for testing:
  `_find_web_pid(pgrep=subprocess.run) -> int | None` and
  `_spawn_web(popen=subprocess.Popen) -> None`.

Rationale: splitting the pgrep/kill/spawn into small injectable helpers keeps
`reload_web_server` testable (assert "no server → no spawn/kill" and
"server present → kill then spawn") without real processes.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_refresh.py
def test_reload_noop_when_no_server(monkeypatch):
    events = []
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: None)
    monkeypatch.setattr(refresh, "_spawn_web", lambda: events.append("spawn"))
    monkeypatch.setattr(refresh.os, "kill", lambda pid, sig: events.append(("kill", pid)))
    refresh.reload_web_server()
    assert events == []  # nothing to reload


def test_reload_kills_then_spawns_when_server_running(monkeypatch):
    events = []
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: 4242)
    monkeypatch.setattr(refresh, "_spawn_web", lambda: events.append("spawn"))
    monkeypatch.setattr(refresh.os, "kill", lambda pid, sig: events.append(("kill", pid)))
    monkeypatch.setattr(refresh.time, "sleep", lambda s: None)
    refresh.reload_web_server()
    assert events == [("kill", 4242), "spawn"]  # kill before spawn
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_refresh.py -k reload -v`
Expected: FAIL — `AttributeError: module 'refresh' has no attribute '_find_web_pid'`.

- [ ] **Step 3: Write minimal implementation**

Add to the import block in `scripts/refresh.py`:

```python
import os
import signal
import time
```

Replace the stub `reload_web_server` with:

```python
def _find_web_pid(pgrep=subprocess.run) -> int | None:
    result = pgrep(["pgrep", "-f", "web_chat.py"], capture_output=True, text=True)
    pids = [line for line in result.stdout.split() if line.strip()]
    return int(pids[0]) if pids else None


def _spawn_web(popen=subprocess.Popen) -> None:
    log_fh = _LOG_PATH.open("a")
    popen(
        ["uv", "run", "python", "scripts/web_chat.py",
         "--input", "output/birdology_reasoned.ttl"],
        cwd=str(_PROJECT_DIR), stdout=log_fh, stderr=log_fh,
        start_new_session=True,
    )


def reload_web_server() -> None:
    pid = _find_web_pid()
    if pid is None:
        log("No running web server — skipping reload.")
        return
    log(f"Reloading web server (pid {pid}) on the reasoned graph…")
    os.kill(pid, signal.SIGTERM)
    time.sleep(2)
    _spawn_web()
    log("Web server relaunched.")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_refresh.py -v`
Expected: PASS (11 tests total).

- [ ] **Step 5: Commit**

```bash
git add scripts/refresh.py tests/test_refresh.py
git commit -m "refresh: implement web-server reload on the reasoned graph

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Rewire `daily_update.sh` + verify end-to-end dry run

**Files:**
- Modify: `scripts/daily_update.sh`

**Interfaces:**
- Consumes: `scripts/refresh.py` CLI (Tasks 1–3).

- [ ] **Step 1: Rewrite `daily_update.sh`**

Replace the entire contents of `scripts/daily_update.sh` with:

```bash
#!/usr/bin/env bash
# Daily incremental refresh: run the FULL pipeline with an incremental fetch
# (build --update → enrich → reason → reload). Delegates to refresh.py so the
# graph the web server loads is always enriched + reasoned.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

uv run python scripts/refresh.py --incremental
```

- [ ] **Step 2: Verify the dry-run plan is correct**

Run: `uv run python scripts/refresh.py --incremental --dry-run`
Expected output contains, in order:
```
build_graph: uv run python scripts/build_graph.py --update --dof-max 20000
enrich_dbpedia: uv run python scripts/enrich_dbpedia.py --input output/birdology.ttl
reason: uv run python scripts/reason.py
reload_web:
```

- [ ] **Step 3: Run the full test suite**

Run: `uv run pytest tests/ -q`
Expected: all pass (existing suite + 11 new refresh tests), no failures.

- [ ] **Step 4: Commit**

```bash
git add scripts/daily_update.sh
git commit -m "refresh: route daily_update.sh through the full refresh pipeline

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage** (Feature 2 of the design doc):
- Full-pipeline orchestrator build→enrich→reason→reindex→reload → Task 1 (`build_plan`) + Task 2 (`run_plan`).
- `--incremental`, `--dof-max`, `--reindex`, `--no-reload`, `--dry-run` → Task 1 (`parse_args`) + tests.
- Testable step plan (assert steps/order/args) → Task 1 tests.
- Runner runs in order + aborts on failure → Task 2 tests.
- Reload the *reasoned* graph → Task 3 (`_spawn_web` uses `birdology_reasoned.ttl`).
- Rewrite `daily_update.sh` to `refresh.py --incremental` → Task 4.
- Cron docs → `refresh.py` module docstring (Task 1).
- Logging to `logs/refresh.log` → Task 2 (`log`).

**Placeholder scan:** none — every step has full code/commands.

**Type consistency:** `Step(name, argv)`, `build_plan(args)`, `run_plan(plan, *, runner, reloader)`, `reload_web_server()`, `_find_web_pid`, `_spawn_web`, `log`, `main` are used consistently across tasks. The Task 2 `reload_web_server` stub is replaced (not redefined alongside) in Task 3.

**Out of scope (per spec):** week-level forecasts, Wikidata/Elton enrichment, incremental enrichment — none included. Correct.
