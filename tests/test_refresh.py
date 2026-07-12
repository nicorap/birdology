import subprocess
import sys
from pathlib import Path

import pytest

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


def test_run_plan_executes_steps_in_order(monkeypatch, tmp_path):
    monkeypatch.setattr(refresh, "_LOG_PATH", tmp_path / "refresh.log")
    calls = []
    plan = [refresh.Step("a", ["x"]), refresh.Step("b", ["y"]), refresh.Step("reload_web")]
    refresh.run_plan(
        plan,
        runner=lambda argv, check: calls.append(("run", argv)),
        reloader=lambda: calls.append(("reload", None)),
    )
    assert calls == [("run", ["x"]), ("run", ["y"]), ("reload", None)]


def test_run_plan_aborts_on_first_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(refresh, "_LOG_PATH", tmp_path / "refresh.log")
    calls = []

    def boom(argv, check):
        calls.append(argv)
        raise subprocess.CalledProcessError(1, argv)

    plan = [refresh.Step("a", ["x"]), refresh.Step("b", ["y"])]
    with pytest.raises(subprocess.CalledProcessError):
        refresh.run_plan(plan, runner=boom, reloader=lambda: None)
    assert calls == [["x"]]  # stopped after the first step failed


def test_main_dry_run_prints_plan_and_runs_nothing(capsys):
    rc = refresh.main(["--dry-run"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "build_graph:" in out
    assert "reason:" in out
    assert "reload_web:" in out
