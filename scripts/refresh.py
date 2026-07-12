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
