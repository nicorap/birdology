"""Tests for web chat session handling — no live LLM or network calls.

Regression context: after the graph was rebuilt and the `where_seen` tool was
added, existing sessions kept replaying the assistant's *pre-fix* refusals
("je n'ai pas d'information sur la localité") and never called the new tool.
A system-prompt rule was not enough to overcome that in-context precedent, so
sessions are now invalidated when the server's capabilities change.
"""
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

pytest.importorskip("flask")

import web_chat


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "sessions.db"
    web_chat._init_session_db(path)
    return path


def _history(db_path, session_id):
    with sqlite3.connect(db_path) as con:
        row = con.execute(
            "SELECT messages FROM sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
    return json.loads(row[0]) if row else None


def _poison(db_path, session_id):
    """Seed a session that already contains a stale 'I have no data' refusal."""
    msgs = [
        {"role": "system", "content": "old system prompt"},
        {"role": "user", "content": "ou a-t-il ete vu ?"},
        {"role": "assistant",
         "content": "Je n'ai pas d'information sur la localité (source: graphe Birdology)."},
    ]
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT OR REPLACE INTO sessions (session_id, messages, last_seen, capabilities)"
            " VALUES (?,?,?,?)",
            (session_id, json.dumps(msgs), 9e9, "stale-fingerprint"),
        )
    return msgs


def test_capability_fingerprint_is_stable_across_calls():
    assert web_chat._capability_fingerprint() == web_chat._capability_fingerprint()


def test_capability_fingerprint_changes_when_tools_change(monkeypatch):
    before = web_chat._capability_fingerprint()
    monkeypatch.setattr(
        web_chat, "TOOLS_OPENAI",
        list(web_chat.TOOLS_OPENAI) + [{"function": {"name": "brand_new_tool"}}],
    )
    assert web_chat._capability_fingerprint() != before, (
        "adding a tool must invalidate sessions — otherwise old refusals get replayed "
        "and the new tool is never called"
    )


def test_stale_session_drops_history(db):
    """The core regression: a session built before the tool existed must not
    carry its old refusals into the new capability set."""
    _poison(db, "sess_stale")
    msgs = web_chat._get_session("sess_stale")

    assert len(msgs) == 1 and msgs[0]["role"] == "system"
    assert not any("pas d'information" in str(m.get("content", "")) for m in msgs)


def test_fresh_session_history_is_preserved(db):
    """A session created under the current capabilities keeps its history."""
    web_chat._get_session("sess_live")
    msgs = web_chat._get_session("sess_live")
    msgs.append({"role": "user", "content": "hello"})
    web_chat._save_session("sess_live", msgs)

    reloaded = web_chat._get_session("sess_live")
    assert any(m.get("content") == "hello" for m in reloaded)


def test_session_stamped_with_current_fingerprint(db):
    web_chat._get_session("sess_new")
    with sqlite3.connect(db) as con:
        stored = con.execute(
            "SELECT capabilities FROM sessions WHERE session_id = ?", ("sess_new",)
        ).fetchone()[0]
    assert stored == web_chat._capability_fingerprint()


def test_legacy_db_without_capabilities_column_is_migrated(tmp_path):
    """Existing sessions.db files predate the capabilities column."""
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as con:
        con.execute(
            "CREATE TABLE sessions (session_id TEXT PRIMARY KEY, messages TEXT NOT NULL,"
            " last_seen REAL NOT NULL)"
        )
        con.execute(
            "INSERT INTO sessions VALUES (?,?,?)",
            ("old", json.dumps([{"role": "system", "content": "x"},
                                {"role": "assistant", "content": "Je n'ai pas d'information"}]),
             9e9),
        )

    web_chat._init_session_db(path)  # must not raise
    msgs = web_chat._get_session("old")
    assert len(msgs) == 1 and msgs[0]["role"] == "system"  # legacy history dropped


# ── the fingerprint must cover every capability, not just tool NAMES ──────────
# Regression: after the Wikipedia index was rebuilt (179 -> 276 species) and
# search_wikipedia gained a required scientific_name argument, the fingerprint
# did not change — tool NAMES were identical and the graph file was untouched.
# Sessions kept their history and the assistant went on replaying "l'index
# Wikipedia ne contient pas cette espèce" without ever calling the tool again.

def test_fingerprint_changes_when_a_tool_schema_changes(monkeypatch):
    """A tool can gain a required argument without changing its name. That is a
    capability change and must invalidate sessions."""
    before = web_chat._capability_fingerprint()

    tools = [
        {"function": {"name": t["function"]["name"],
                      "parameters": dict(t["function"].get("parameters", {}))}}
        for t in web_chat.TOOLS_OPENAI
    ]
    tools[0]["function"]["parameters"] = {"required": ["a_brand_new_required_arg"]}
    monkeypatch.setattr(web_chat, "TOOLS_OPENAI", tools)

    assert web_chat._capability_fingerprint() != before, (
        "a changed tool schema must invalidate sessions — otherwise the model "
        "keeps replaying refusals from before the tool could answer"
    )


def test_fingerprint_changes_when_the_wiki_index_changes(monkeypatch, tmp_path):
    """The Wikipedia index is a data source like the graph. Rebuilding it makes
    every prior 'not indexed' answer stale."""
    idx = tmp_path / "chroma.sqlite3"
    idx.write_bytes(b"old index")
    monkeypatch.setattr(web_chat, "_WIKI_INDEX_DB", idx)
    before = web_chat._capability_fingerprint()

    idx.write_bytes(b"a much larger rebuilt index with more species")

    assert web_chat._capability_fingerprint() != before, (
        "rebuilding the wiki index must invalidate sessions"
    )
