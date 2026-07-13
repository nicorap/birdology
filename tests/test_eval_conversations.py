"""Offline unit tests for the eval harness. No live server, LLM, or network."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import eval_chat
from eval_chat import TestCase, check_answer


def _case(**kw) -> TestCase:
    kw.setdefault("id", "t")
    kw.setdefault("category", "c")
    kw.setdefault("question", "q")
    return TestCase(**kw)


def test_check_answer_passes_when_expected_tool_called():
    spec = _case(expected_tools=["find_species"])
    assert check_answer(spec, ["find_species"], "Le Rouge-gorge.") == []


def test_check_answer_flags_missing_expected_tool():
    spec = _case(expected_tools=["where_seen"])
    failures = check_answer(spec, ["find_species"], "answer")
    assert any("where_seen" in f for f in failures)


def test_check_answer_flags_forbidden_tool():
    spec = _case(forbidden_tools=["compare_seasonal"])
    failures = check_answer(spec, ["compare_seasonal"], "answer")
    assert any("Forbidden tool" in f for f in failures)


def test_check_answer_must_contain_is_case_insensitive():
    spec = _case(must_contain=["ROUGE-GORGE"])
    assert check_answer(spec, [], "le rouge-gorge familier") == []


def test_check_answer_must_contain_any_needs_only_one():
    spec = _case(must_contain_any=["![", "photo"])
    assert check_answer(spec, [], "voici une photo") == []


def test_check_answer_flags_must_not_contain():
    spec = _case(must_not_contain=["pas d'information"])
    failures = check_answer(spec, [], "Je n'ai pas d'information sur la localité")
    assert any("Forbidden phrase" in f for f in failures)


def test_check_answer_short_circuits_on_infra_error():
    spec = _case(expected_tools=["find_species"])
    failures = check_answer(spec, [], "Erreur serveur: /api/embeddings failed")
    assert failures == ["INFRA: Ollama embeddings crashed (nomic-embed-text unavailable)"]


# --- Multi-turn conversation tests ---

from eval_chat import Conversation, Turn, run_conversation


class FakePost:
    """Stands in for HTTP. Records calls; replays scripted (answer, tool_calls)."""

    def __init__(self, scripted):
        self.scripted = list(scripted)
        self.calls = []          # (session_id, question)

    def __call__(self, base_url, session_id, question):
        self.calls.append((session_id, question))
        return self.scripted.pop(0)


def test_all_turns_share_one_session():
    """The shared session is the whole point — it is what exercises history."""
    conv = Conversation(id="c", category="conversation_regression", turns=[
        Turn("premiere question"),
        Turn("deuxieme question"),
        Turn("troisieme question"),
    ])
    post = FakePost([("a", []), ("b", []), ("c", [])])
    run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    session_ids = {sid for sid, _ in post.calls}
    assert len(session_ids) == 1, f"turns used {len(session_ids)} sessions, must share one"
    assert [q for _, q in post.calls] == [
        "premiere question", "deuxieme question", "troisieme question",
    ]


def test_per_turn_assertions_are_evaluated():
    conv = Conversation(id="c", category="conversation_regression", turns=[
        Turn("setup"),                                     # asserts nothing
        Turn("ou a-t-il ete vu ?", expected_tools=["where_seen"]),
    ])
    post = FakePost([
        ("bla", [{"name": "find_species", "args": {}}]),
        ("Egå Engsø", [{"name": "where_seen", "args": {}}]),
    ])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    assert result.turns[0].passed          # setup turn has no assertions
    assert result.turns[1].passed
    assert result.passed


def test_failing_turn_does_not_abort_the_conversation():
    """Whether the assistant RECOVERS is exactly what we want to measure."""
    conv = Conversation(id="c", category="conversation_regression", turns=[
        Turn("t1", expected_tools=["where_seen"]),   # will fail
        Turn("t2", expected_tools=["find_species"]),  # must still run, and pass
    ])
    post = FakePost([("nope", []), ("photo", [{"name": "find_species", "args": {}}])])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    assert len(result.turns) == 2, "later turns must still run after a failure"
    assert not result.turns[0].passed
    assert result.turns[1].passed
    assert not result.passed, "conversation fails if any turn fails"


def test_turn_results_are_numbered_from_one():
    conv = Conversation(id="c", category="x", turns=[Turn("a"), Turn("b")])
    post = FakePost([("1", []), ("2", [])])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)
    assert [t.index for t in result.turns] == [1, 2]


def test_http_error_fails_that_turn_but_later_turns_still_run():
    def boom(base_url, session_id, question):
        if question == "t1":
            raise RuntimeError("connection reset")
        return ("ok", [{"name": "find_species", "args": {}}])

    conv = Conversation(id="c", category="x", turns=[
        Turn("t1"), Turn("t2", expected_tools=["find_species"]),
    ])
    result = run_conversation(conv, "http://x", delay=0, post=boom, sleep=lambda s: None)

    assert not result.turns[0].passed
    assert any("connection reset" in f for f in result.turns[0].failures)
    assert result.turns[1].passed


# --- Tests for CONVERSATIONS data structure ---

from eval_chat import CONVERSATIONS


def test_conversations_have_unique_ids():
    ids = [c.id for c in CONVERSATIONS]
    assert len(ids) == len(set(ids))


def test_both_conversation_categories_present():
    cats = {c.category for c in CONVERSATIONS}
    assert "conversation_regression" in cats
    assert "conversation_robustness" in cats


def test_every_conversation_is_multi_turn_and_within_budget():
    for c in CONVERSATIONS:
        assert 2 <= len(c.turns) <= 5, f"{c.id} has {len(c.turns)} turns"


def test_every_conversation_asserts_something():
    """A conversation with no assertions anywhere would silently always pass."""
    for c in CONVERSATIONS:
        asserted = any(
            t.expected_tools or t.forbidden_tools or t.must_contain
            or t.must_contain_any or t.must_not_contain
            for t in c.turns
        )
        assert asserted, f"{c.id} asserts nothing"


def test_regression_conversations_cover_the_known_bugs():
    ids = {c.id for c in CONVERSATIONS}
    assert {"conv_photo_after_nearby", "conv_pronoun_where_seen",
            "conv_refusal_inertia", "conv_no_invented_species"} <= ids


# ---------------------------------------------------------------------------
# Tool-argument capture (fixes the finding: expected_tools only checks NAMES,
# so a conversation could pass while calling the right tool with stale/wrong
# arguments — e.g. where_to_watch(Copenhagen) when Aarhus was asked).
# ---------------------------------------------------------------------------

def test_post_message_returns_tool_call_dicts_with_args(monkeypatch):
    """Confirms /api/chat's tool_calls carry 'args' alongside 'name' (see web_chat.py
    tool_calls_log entries), and that _post_message passes them through untouched."""

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "answer": "Le Rouge-gorge familier.",
                "tool_calls": [{"name": "find_species", "args": {"name": "Rouge-gorge"}}],
            }

    def fake_post(url, json, timeout):
        return FakeResponse()

    monkeypatch.setattr(eval_chat.requests, "post", fake_post)

    answer, tool_calls = eval_chat._post_message("http://x", "sess1", "Parle-moi du Rouge-gorge")
    assert answer == "Le Rouge-gorge familier."
    assert tool_calls == [{"name": "find_species", "args": {"name": "Rouge-gorge"}}]


def test_check_answer_passes_when_expected_tool_args_all_match():
    spec = Turn("q", expected_tools=["where_to_watch"],
                expected_tool_args={"lat": 56.16, "lon": 10.20})
    details = [{"name": "where_to_watch", "args": {"lat": 56.1629, "lon": 10.2039}}]
    assert check_answer(spec, ["where_to_watch"], "answer", tool_call_details=details) == []


def test_check_answer_flags_wrong_tool_args_same_tool_name_wrong_location():
    """THE bug: right tool name, but stale Copenhagen coords instead of the new Aarhus ones."""
    spec = Turn("q", expected_tools=["where_to_watch"],
                expected_tool_args={"lat": 56.16, "lon": 10.20})
    details = [{"name": "where_to_watch", "args": {"lat": 55.6761, "lon": 12.5683}}]  # Copenhagen
    failures = check_answer(spec, ["where_to_watch"], "answer", tool_call_details=details)
    assert failures, "must fail: where_to_watch was called with stale Copenhagen coordinates"


def test_check_answer_flags_wrong_tool_args_wrong_month():
    """THE bug: right tool name, but month=3 (March) instead of the requested June."""
    spec = Turn("q", expected_tools=["observations_by_month"], expected_tool_args={"month": 6})
    details = [{"name": "observations_by_month", "args": {"month": 3}}]
    failures = check_answer(spec, ["observations_by_month"], "answer", tool_call_details=details)
    assert failures, "must fail: month=3 does not satisfy the month=6 (June) requirement"


def test_check_answer_numeric_tolerance_within_bound_passes():
    spec = Turn("q", expected_tool_args={"lat": 56.16})
    details = [{"name": "t", "args": {"lat": 56.16 + 0.5}}]  # exactly at the tolerance boundary
    assert check_answer(spec, ["t"], "answer", tool_call_details=details) == []


def test_check_answer_numeric_tolerance_just_outside_bound_fails():
    spec = Turn("q", expected_tool_args={"lat": 56.16})
    details = [{"name": "t", "args": {"lat": 56.16 + 0.51}}]
    failures = check_answer(spec, ["t"], "answer", tool_call_details=details)
    assert failures, "0.51 is outside the 0.5 tolerance"


def test_check_answer_string_arg_is_case_insensitive_substring():
    spec = Turn("q", expected_tool_args={"name": "Rouge-queue"})
    details = [{"name": "find_species", "args": {"name": "rouge-queue noir"}}]
    assert check_answer(spec, ["find_species"], "answer", tool_call_details=details) == []


def test_check_answer_expected_tool_args_needs_at_least_one_fully_matching_call():
    """Multiple tool calls in a turn — only one needs to match ALL expected args."""
    spec = Turn("q", expected_tool_args={"lat": 56.16, "lon": 10.20})
    details = [
        {"name": "where_to_watch", "args": {"lat": 55.68, "lon": 12.57}},   # Copenhagen, wrong
        {"name": "where_to_watch", "args": {"lat": 56.1629, "lon": 10.2039}},  # Aarhus, right
    ]
    assert check_answer(spec, ["where_to_watch", "where_to_watch"], "answer",
                         tool_call_details=details) == []


def test_check_answer_expected_tool_args_fails_when_no_details_given():
    """If a Turn sets expected_tool_args but the caller forgot to pass tool_call_details,
    the check must not silently pass."""
    spec = Turn("q", expected_tool_args={"month": 6})
    failures = check_answer(spec, ["observations_by_month"], "answer")
    assert failures


def test_check_answer_no_expected_tool_args_skips_the_check():
    """Default (empty) expected_tool_args must not affect ordinary single-turn behaviour."""
    spec = Turn("q", expected_tools=["find_species"])
    assert check_answer(spec, ["find_species"], "answer") == []


def test_check_answer_testcase_without_expected_tool_args_field_still_works():
    """TestCase has no expected_tool_args attribute at all; getattr fallback must not crash."""
    spec = TestCase(id="t", category="c", question="q", expected_tools=["find_species"])
    assert not hasattr(spec, "expected_tool_args")
    assert check_answer(spec, ["find_species"], "Le Rouge-gorge.") == []


def test_run_conversation_records_tool_call_details_and_names():
    conv = Conversation(id="c", category="x", turns=[
        Turn("q1", expected_tools=["find_species"]),
    ])
    details = [{"name": "find_species", "args": {"name": "Rouge-gorge"}}]
    post = FakePost([("answer", details)])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    assert result.turns[0].tool_calls == ["find_species"]
    assert result.turns[0].tool_call_details == details


def test_run_conversation_fails_turn_when_tool_called_with_wrong_args():
    """Reproduces the exact bug the finding describes: where_to_watch is called again for
    the Aarhus turn, but with the same stale Copenhagen coordinates as turn 1."""
    conv = Conversation(id="conv_location_carryover_repro", category="conversation_robustness", turns=[
        Turn("Où observer des oiseaux près de Copenhague ?", expected_tools=["where_to_watch"]),
        Turn("et à Aarhus ?", expected_tools=["where_to_watch"],
             expected_tool_args={"lat": 56.16, "lon": 10.20}),
    ])
    post = FakePost([
        ("Copenhague: ...", [{"name": "where_to_watch", "args": {"lat": 55.6761, "lon": 12.5683}}]),
        ("Aarhus: ...", [{"name": "where_to_watch", "args": {"lat": 55.6761, "lon": 12.5683}}]),  # BUG
    ])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    assert not result.turns[1].passed, (
        "must fail: where_to_watch reused Copenhagen coordinates for the Aarhus turn"
    )


def test_run_conversation_passes_turn_when_tool_called_with_correct_args():
    """Same shape as the repro above, but the model actually re-runs the tool for Aarhus."""
    conv = Conversation(id="conv_location_carryover_repro_ok", category="conversation_robustness", turns=[
        Turn("Où observer des oiseaux près de Copenhague ?", expected_tools=["where_to_watch"]),
        Turn("et à Aarhus ?", expected_tools=["where_to_watch"],
             expected_tool_args={"lat": 56.16, "lon": 10.20}),
    ])
    post = FakePost([
        ("Copenhague: ...", [{"name": "where_to_watch", "args": {"lat": 55.6761, "lon": 12.5683}}]),
        ("Aarhus: ...", [{"name": "where_to_watch", "args": {"lat": 56.1629, "lon": 10.2039}}]),
    ])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    assert result.turns[1].passed


def test_robustness_conversations_have_expected_tool_args_pinned():
    by_id = {c.id: c for c in CONVERSATIONS}
    assert by_id["conv_location_carryover"].turns[1].expected_tool_args == {"lat": 56.16, "lon": 10.20}
    assert by_id["conv_month_carryover"].turns[1].expected_tool_args == {"month": 6}
    assert by_id["conv_topic_switch_and_back"].turns[2].expected_tool_args == {"name": "Rouge-gorge"}
    assert by_id["conv_user_correction"].turns[1].expected_tool_args == {"name": "Rouge-queue"}
