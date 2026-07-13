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
    post = FakePost([("bla", ["find_species"]), ("Egå Engsø", ["where_seen"])])
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
    post = FakePost([("nope", []), ("photo", ["find_species"])])
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
        return ("ok", ["find_species"])

    conv = Conversation(id="c", category="x", turns=[
        Turn("t1"), Turn("t2", expected_tools=["find_species"]),
    ])
    result = run_conversation(conv, "http://x", delay=0, post=boom, sleep=lambda s: None)

    assert not result.turns[0].passed
    assert any("connection reset" in f for f in result.turns[0].failures)
    assert result.turns[1].passed
