#!/usr/bin/env python3
"""
Birdology Graph-RAG — web chat interface.

Serves a browser-based chat UI backed by the Birdology knowledge graph.

Usage:
    python scripts/web_chat.py
    python scripts/web_chat.py --port 8080
    python scripts/web_chat.py --input output/birdology_reasoned.ttl
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, request, jsonify, Response

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.graph import load_graph
from chat import (
    SYSTEM_PROMPT,
    TOOLS_OPENAI,
    _run_tool,
)

load_dotenv()

DEFAULT_TTL = Path(__file__).parent.parent / "output" / "birdology.ttl"
STATIC_DIR = Path(__file__).parent / "static"

app = Flask(__name__)
GRAPH = None  # loaded at startup

# ── Session storage (in-memory) ──────────────────────────────────────────────
# Each session stores its message history so the LLM has conversation context.
# Sessions expire after 2 hours of inactivity.

_sessions: dict[str, dict] = {}  # session_id -> {"messages": [...], "last_seen": float}
_sessions_lock = threading.Lock()
_SESSION_TTL = 7200  # 2 hours


_MAX_HISTORY = 40  # keep last N messages (+ system prompt) to avoid bloating LLM context


def _get_session(session_id: str) -> list[dict]:
    """Return the message list for a session, creating it if needed."""
    with _sessions_lock:
        if session_id not in _sessions:
            _sessions[session_id] = {
                "messages": [{"role": "system", "content": SYSTEM_PROMPT}],
                "last_seen": time.time(),
            }
        sess = _sessions[session_id]
        sess["last_seen"] = time.time()
        # Trim old messages (keep system prompt + last N)
        msgs = sess["messages"]
        if len(msgs) > _MAX_HISTORY + 1:
            sess["messages"] = msgs[:1] + msgs[-(  _MAX_HISTORY):]
        # Prune expired sessions while we're here
        now = time.time()
        expired = [k for k, v in _sessions.items() if now - v["last_seen"] > _SESSION_TTL]
        for k in expired:
            del _sessions[k]
        return sess["messages"]


def _extract_thumbnails(tool_result: str, out: list) -> None:
    """Parse tool JSON results and collect (name, thumbnail_url) pairs."""
    try:
        data = json.loads(tool_result)
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(data, list):
        return
    for item in data:
        if not isinstance(item, dict):
            continue
        thumb = item.get("thumbnail")
        if thumb and thumb.startswith("http"):
            name = (item.get("commonNameEn")
                    or item.get("commonNameFr")
                    or item.get("scientificName")
                    or "")
            # Avoid duplicates
            if not any(u == thumb for _, u in out):
                out.append((name, thumb))


@app.route("/")
def index():
    html = (STATIC_DIR / "chat.html").read_text(encoding="utf-8")
    return Response(html, content_type="text/html; charset=utf-8",
                    headers={"Cache-Control": "no-cache, no-store"})


@app.route("/api/chat", methods=["POST"])
def api_chat():
    data = request.get_json()
    question = data.get("message", "").strip()
    session_id = data.get("session_id", "default")
    if not question:
        return jsonify({"answer": "Posez une question."}), 400

    base_url = os.getenv("LLM_BASE_URL", "http://localhost:11434/v1")
    api_key = os.getenv("LLM_API_KEY", "ollama")
    model = os.getenv("LLM_MODEL", "mistral")

    from openai import OpenAI

    client = OpenAI(base_url=base_url, api_key=api_key)

    # Session stores only user/assistant text messages (no tool intermediaries).
    # We build the full request messages from session history + current turn.
    history = _get_session(session_id)
    history.append({"role": "user", "content": question})

    # Working messages for this request (includes tool calls, not persisted)
    messages = list(history)

    tool_calls_log = []
    thumbnails_seen = []  # collect thumbnail URLs from tool results
    max_rounds = 5

    try:
        for _ in range(max_rounds):
            # Retry up to 2 times on transient LLM API errors
            last_err = None
            for attempt in range(3):
                try:
                    response = client.chat.completions.create(
                        model=model,
                        messages=messages,
                        tools=TOOLS_OPENAI,
                    )
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    if attempt < 2:
                        time.sleep(2)
            if last_err:
                raise last_err
            msg = response.choices[0].message

            if not msg.tool_calls:
                answer = msg.content or ""
                # Append photo gallery only for thumbnails NOT already in the answer
                if thumbnails_seen:
                    already = answer  # check against raw answer text
                    new_thumbs = [
                        (name, url) for name, url in thumbnails_seen[:4]
                        if url not in already
                    ]
                    if new_thumbs:
                        gallery = "\n\n"
                        for name, url in new_thumbs:
                            gallery += f'<bird-img name="{name}" src="{url}">\n'
                        answer += gallery
                # Persist only the final assistant text to session history
                history.append({"role": "assistant", "content": answer})
                return jsonify({
                    "answer": answer,
                    "tool_calls": tool_calls_log,
                })

            # Tool calls — add to working messages but NOT to session history
            messages.append(msg.model_dump(exclude_none=True))

            for tc in msg.tool_calls:
                fn_name = tc.function.name
                try:
                    fn_args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    fn_args = {}

                tool_calls_log.append({"name": fn_name, "args": fn_args})
                result = _run_tool(fn_name, fn_args, GRAPH)

                # Extract thumbnails from tool results
                _extract_thumbnails(result, thumbnails_seen)

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })

        answer = "(max tool rounds reached)"
        history.append({"role": "assistant", "content": answer})
        return jsonify({
            "answer": answer,
            "tool_calls": tool_calls_log,
        })

    except Exception as e:
        # Remove the user message we just added if the request failed
        if history and history[-1].get("role") == "user":
            history.pop()
        return jsonify({"answer": f"Erreur serveur: {e}"}), 200


@app.route("/api/reset", methods=["POST"])
def api_reset():
    """Clear conversation history for a session."""
    data = request.get_json() or {}
    session_id = data.get("session_id", "default")
    with _sessions_lock:
        _sessions.pop(session_id, None)
    return jsonify({"status": "ok"})


def main():
    global GRAPH

    parser = argparse.ArgumentParser(description="Birdology Graph-RAG web chat")
    parser.add_argument(
        "--input",
        default=str(DEFAULT_TTL),
        metavar="PATH",
        help="Turtle graph file (default: output/birdology.ttl)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port to serve on (default: 5000)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    args = parser.parse_args()

    ttl_path = Path(args.input)
    if not ttl_path.exists():
        print(f"Error: graph file not found: {ttl_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading graph from {ttl_path} …")
    GRAPH = load_graph(ttl_path)
    print(f"\nBirdology Graph-RAG web UI")
    print(f"  http://localhost:{args.port}\n")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
