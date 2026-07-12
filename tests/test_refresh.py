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
