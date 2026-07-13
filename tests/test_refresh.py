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


def test_reload_noop_when_no_server(monkeypatch):
    events = []
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: None)
    monkeypatch.setattr(refresh, "_spawn_web", lambda **kw: events.append("spawn"))
    monkeypatch.setattr(refresh.os, "kill", lambda pid, sig: events.append(("kill", pid)))
    refresh.reload_web_server()
    assert events == []  # nothing to reload


def test_reload_kills_then_spawns_when_server_running(monkeypatch):
    events = []
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: 4242)
    monkeypatch.setattr(refresh, "_running_web_port", lambda: None)
    monkeypatch.setattr(refresh, "_spawn_web", lambda **kw: events.append("spawn"))
    monkeypatch.setattr(refresh.os, "kill", lambda pid, sig: events.append(("kill", pid)))
    monkeypatch.setattr(refresh.time, "sleep", lambda s: None)
    refresh.reload_web_server()
    assert events == [("kill", 4242), "spawn"]  # kill before spawn


def test_spawn_web_targets_reasoned_graph(monkeypatch, tmp_path):
    monkeypatch.setattr(refresh, "_LOG_PATH", tmp_path / "refresh.log")
    captured = {}
    def fake_popen(argv, **kwargs):
        captured["argv"] = argv
        return None
    refresh._spawn_web(popen=fake_popen)
    assert "--input" in captured["argv"]
    assert "output/birdology_reasoned.ttl" in captured["argv"]


# ── port handling (regression: refresh relaunched the server on hardcoded 5000,
#    which is taken by AirPlay Receiver on macOS, so the server never came back) ──

def test_spawn_web_passes_port(monkeypatch, tmp_path):
    monkeypatch.setattr(refresh, "_LOG_PATH", tmp_path / "refresh.log")
    captured = {}
    def fake_popen(argv, **kwargs):
        captured["argv"] = argv
        return None
    refresh._spawn_web(port=8080, popen=fake_popen)
    argv = captured["argv"]
    assert "--port" in argv
    assert argv[argv.index("--port") + 1] == "8080"


def test_running_web_port_parsed_from_process_cmdline(monkeypatch):
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: 60409)
    def fake_ps(argv, **kwargs):
        assert argv[0] == "ps", "must use ps: macOS pgrep ignores -a and prints bare PIDs"
        class R:
            stdout = "uv run python scripts/web_chat.py --port 8080 --input output/x.ttl\n"
        return R()
    assert refresh._running_web_port(ps=fake_ps) == 8080


def test_running_web_port_none_when_no_server(monkeypatch):
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: None)
    assert refresh._running_web_port() is None


def test_running_web_port_none_when_port_not_in_cmdline(monkeypatch):
    """Server started without --port is on web_chat's own default, not ours to guess."""
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: 60409)
    def fake_ps(argv, **kwargs):
        class R:
            stdout = "uv run python scripts/web_chat.py --input output/x.ttl\n"
        return R()
    assert refresh._running_web_port(ps=fake_ps) is None


def test_reload_reuses_running_server_port(monkeypatch):
    """A refresh must not move the server to a different port behind the user's back."""
    captured = {}
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: 4242)
    monkeypatch.setattr(refresh, "_running_web_port", lambda: 8080)
    monkeypatch.setattr(refresh, "_spawn_web", lambda **kw: captured.update(kw))
    monkeypatch.setattr(refresh.os, "kill", lambda pid, sig: None)
    monkeypatch.setattr(refresh.time, "sleep", lambda s: None)
    refresh.reload_web_server()
    assert captured["port"] == 8080


def test_explicit_port_overrides_running_port(monkeypatch):
    captured = {}
    monkeypatch.setattr(refresh, "_find_web_pid", lambda: 4242)
    monkeypatch.setattr(refresh, "_running_web_port", lambda: 8080)
    monkeypatch.setattr(refresh, "_spawn_web", lambda **kw: captured.update(kw))
    monkeypatch.setattr(refresh.os, "kill", lambda pid, sig: None)
    monkeypatch.setattr(refresh.time, "sleep", lambda s: None)
    refresh.reload_web_server(port=9999)
    assert captured["port"] == 9999


def test_main_returns_nonzero_on_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(refresh, "_LOG_PATH", tmp_path / "refresh.log")
    def boom(plan):
        raise RuntimeError("step failed")
    monkeypatch.setattr(refresh, "run_plan", boom)
    rc = refresh.main([])
    assert rc == 1
