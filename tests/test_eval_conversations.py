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
