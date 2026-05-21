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
import re
import sqlite3
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, request, jsonify, Response

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.graph import load_graph
from birdology.queries import observations_for_map
from chat import (
    SYSTEM_PROMPT,
    TOOLS_OPENAI,
    _run_tool,
)

_PANEL_CACHE: dict[str, object] = {}

load_dotenv()

DEFAULT_TTL = Path(__file__).parent.parent / "output" / "birdology.ttl"
STATIC_DIR = Path(__file__).parent / "static"

app = Flask(__name__)
GRAPH = None  # loaded at startup

# ── Session storage (SQLite) ──────────────────────────────────────────────────
# Each session stores its message history so the LLM has conversation context.
# Sessions expire after 2 hours of inactivity.
# DB path is set at startup via _init_session_db().

_sessions_lock = threading.Lock()
_SESSION_TTL = 7200  # 2 hours
_MAX_HISTORY = 40    # keep last N messages (+ system prompt) to avoid bloating LLM context
_SESSION_DB: Path | None = None  # set by _init_session_db()


def _init_session_db(db_path: Path) -> None:
    global _SESSION_DB
    _SESSION_DB = db_path
    with sqlite3.connect(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                messages   TEXT NOT NULL,
                last_seen  REAL NOT NULL
            )
        """)


def _db() -> sqlite3.Connection:
    return sqlite3.connect(_SESSION_DB)


def _get_session(session_id: str) -> list[dict]:
    """Return the mutable message list for a session, creating it if needed.

    The returned list is a live Python object backed by SQLite; callers must
    call _save_session() after appending to persist changes.
    """
    import datetime
    with _sessions_lock:
        now = time.time()
        with _db() as con:
            # Prune expired sessions
            con.execute("DELETE FROM sessions WHERE last_seen < ?", (now - _SESSION_TTL,))

            row = con.execute(
                "SELECT messages FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()

            if row is None:
                today = datetime.date.today()
                system = SYSTEM_PROMPT + (
                    f"\n\n## Current date\nToday is {today.strftime('%A %d %B %Y')} "
                    f"(month {today.month}). Use this when the user says 'today', 'tomorrow', "
                    f"'this month', etc. — never assume a different month."
                )
                msgs: list[dict] = [{"role": "system", "content": system}]
            else:
                msgs = json.loads(row[0])
                # Refresh system prompt date on each new day
                import datetime as _dt
                today = _dt.date.today()
                new_system = SYSTEM_PROMPT + (
                    f"\n\n## Current date\nToday is {today.strftime('%A %d %B %Y')} "
                    f"(month {today.month}). Use this when the user says 'today', 'tomorrow', "
                    f"'this month', etc. — never assume a different month."
                )
                if msgs and msgs[0]["role"] == "system":
                    msgs[0]["content"] = new_system

            # Trim to MAX_HISTORY (keep system prompt + last N)
            if len(msgs) > _MAX_HISTORY + 1:
                msgs = msgs[:1] + msgs[-_MAX_HISTORY:]

            con.execute(
                "INSERT OR REPLACE INTO sessions (session_id, messages, last_seen) VALUES (?,?,?)",
                (session_id, json.dumps(msgs, ensure_ascii=False), now),
            )

        return msgs


def _save_session(session_id: str, msgs: list[dict]) -> None:
    """Persist the message list for a session after modification."""
    with _sessions_lock:
        with _db() as con:
            con.execute(
                "UPDATE sessions SET messages = ?, last_seen = ? WHERE session_id = ?",
                (json.dumps(msgs, ensure_ascii=False), time.time(), session_id),
            )


def _extract_phenology_tag(tool_result: str, out: list) -> None:
    """Extract the <bird-phenology> tag from a phenology tool result."""
    try:
        data = json.loads(tool_result)
    except (json.JSONDecodeError, TypeError):
        return
    if isinstance(data, dict):
        tag = data.get("phenologyTag")
        if tag:
            out.append(tag)


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
            thumb = _WIDTH_RE.sub("", thumb)  # strip ?width=NNN from DBpedia URLs
            name = (item.get("commonNameEn")
                    or item.get("commonNameFr")
                    or item.get("scientificName")
                    or "")
            # Avoid duplicates
            if not any(u == thumb for _, u in out):
                out.append((name, thumb))


# Supports parentheses inside URLs (e.g. DBpedia FilePath with species names)
_IMG_RE = re.compile(r'!\[([^\]]*)\]\(((?:[^()]|\([^()]*\))+)\)')

_WIDTH_RE = re.compile(r'\?width=\d+$')

# Matches padding section headers the LLM adds despite instructions not to
_PADDING_RE = re.compile(
    r'\n+\*{0,2}(?:Remarque(?:\s+finale)?|À noter|À surveiller|À vérifier'
    r'|Photo\s+de\s+r[eé]f[eé]rence|Conseils?|À\s+[eé]viter'
    r'|[ÉE]quipement(?:\s+recommand[eé])?|Que\s+faire[^?\n]*\??'
    r'|À\s+ne\s+pas\s+manquer|Esp[eè]ces?\s+[àa]\s+cocher)\*{0,2}'
    r'[^\n]*(?:\n(?!\n).+)*',
    re.IGNORECASE,
)

_EMOJI_RE = re.compile('[\U0001F300-\U0001F9FF\U00002702-\U000027B0]')

# Hallucinated "Observation récente / Observations récentes" lines in where_to_watch responses
_OBS_RECENTE_RE = re.compile(
    r'\n[ \t]*(?:\*\s*)?(?:\*\*)?Observations?\s+r[eé]cente?s?(?:\*\*)?\s*:?[^\n]+',
    re.IGNORECASE,
)

# Trailing offer sentences ("Si vous souhaitez...", "N'hésitez pas...", etc.)
_OFFER_RE = re.compile(
    r'\n*(?:Si vous (souhaitez|voulez|avez)|N\'h[eé]sitez pas|'
    r'Je peux (approfondir|vous en dire|vous donner)|'
    r'Voulez-vous (que je|plus de|des d[eé]tails)|'
    r'Dites-moi si vous)[^\n]*\.?$',
    re.IGNORECASE | re.MULTILINE,
)


def _normalize_img_url(url: str) -> str:
    """Strip Wikimedia ?width=NNN suffix for comparison."""
    return _WIDTH_RE.sub("", url)


def _strip_hallucinated_images(text: str, allowed_urls: set) -> str:
    """Remove markdown images whose URL was not returned by a tool."""
    normalized = {_normalize_img_url(u) for u in allowed_urls}

    def _keep(m):
        url = m.group(2)
        return m.group(0) if url in allowed_urls or _normalize_img_url(url) in normalized else ""

    return _IMG_RE.sub(_keep, text)


def _strip_padding_sections(text: str) -> str:
    """Remove forbidden editorial sections, hallucinated obs lines, and trailing offers."""
    text = _PADDING_RE.sub("", text)
    text = _OBS_RECENTE_RE.sub("", text)
    text = _OFFER_RE.sub("", text)
    return text.rstrip()


def _strip_emojis(text: str) -> str:
    """Remove emoji characters the LLM inserts despite prompt rules."""
    return _EMOJI_RE.sub("", text)


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
        return jsonify({"answer": "Please enter a question."}), 400

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
    phenology_tags = []   # collect <bird-phenology> tags from phenology tool results
    wiki_calls = 0  # enforce at-most-once for search_wikipedia
    max_rounds = 8

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
                # Strip any image the LLM invented — only tool-returned URLs allowed
                allowed_urls = {url for _, url in thumbnails_seen}
                answer = _strip_hallucinated_images(answer, allowed_urls)
                answer = _strip_padding_sections(answer)
                answer = _strip_emojis(answer)
                # Append photo gallery only for thumbnails NOT already in the answer
                if thumbnails_seen:
                    new_thumbs = [
                        (name, url) for name, url in thumbnails_seen[:4]
                        if _normalize_img_url(url) not in answer and url not in answer
                    ]
                    if new_thumbs:
                        gallery = "\n\n"
                        for name, url in new_thumbs:
                            gallery += f'<bird-img name="{name}" src="{url}">\n'
                        answer += gallery
                # Append phenology chart if one was collected
                if phenology_tags:
                    answer += "\n\n" + phenology_tags[0]
                # Persist only the final assistant text to session history
                history.append({"role": "assistant", "content": answer})
                _save_session(session_id, history)
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

                if fn_name == "search_wikipedia":
                    wiki_calls += 1
                    if wiki_calls > 1:
                        result = "search_wikipedia already called once. Do not call it again."
                    else:
                        tool_calls_log.append({"name": fn_name, "args": fn_args})
                        result = _run_tool(fn_name, fn_args, GRAPH)
                else:
                    tool_calls_log.append({"name": fn_name, "args": fn_args})
                    result = _run_tool(fn_name, fn_args, GRAPH)

                # Extract thumbnails and phenology tags from tool results
                _extract_thumbnails(result, thumbnails_seen)
                _extract_phenology_tag(result, phenology_tags)

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })

        answer = "(max tool rounds reached)"
        history.append({"role": "assistant", "content": answer})
        _save_session(session_id, history)
        return jsonify({
            "answer": answer,
            "tool_calls": tool_calls_log,
        })

    except Exception as e:
        # Remove the user message we just added if the request failed
        if history and history[-1].get("role") == "user":
            history.pop()
            _save_session(session_id, history)
        return jsonify({"answer": f"Erreur serveur: {e}"}), 200


@app.route("/api/chat/stream", methods=["POST"])
def api_chat_stream():
    data = request.get_json()
    question = data.get("message", "").strip()
    session_id = data.get("session_id", "default")
    if not question:
        return jsonify({"answer": "Please enter a question."}), 400

    base_url = os.getenv("LLM_BASE_URL", "http://localhost:11434/v1")
    api_key = os.getenv("LLM_API_KEY", "ollama")
    model = os.getenv("LLM_MODEL", "mistral")

    from openai import OpenAI
    client = OpenAI(base_url=base_url, api_key=api_key)

    def _sse(event: str, payload: object) -> str:
        return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    def _llm_call(msgs: list, stream: bool):
        last_err = None
        for attempt in range(3):
            try:
                r = client.chat.completions.create(
                    model=model, messages=msgs, tools=TOOLS_OPENAI, stream=stream,
                )
                return r
            except Exception as e:
                last_err = e
                if attempt < 2:
                    time.sleep(2)
        raise last_err

    def _run_tool_event(fn_name, fn_args, wiki_calls_ref):
        """Run a tool and return (result, wiki_calls, should_log)."""
        if fn_name == "search_wikipedia":
            wiki_calls_ref[0] += 1
            if wiki_calls_ref[0] > 1:
                return "search_wikipedia already called once. Do not call it again.", False
            return _run_tool(fn_name, fn_args, GRAPH), True
        return _run_tool(fn_name, fn_args, GRAPH), True

    def generate():
        history = _get_session(session_id)
        history.append({"role": "user", "content": question})
        messages = list(history)
        tool_calls_log = []
        thumbnails_seen = []
        phenology_tags = []
        wiki_calls = [0]  # mutable ref
        max_rounds = 8

        try:
            for _ in range(max_rounds):
                # Use non-streaming for tool rounds to avoid Ollama multi-tool
                # streaming bugs (concatenated names, wrong indices).
                response = _llm_call(messages, stream=False)
                msg = response.choices[0].message

                if not msg.tool_calls:
                    # Final answer — emit word by word for progressive display.
                    # We already have the full text (non-streaming call), so we
                    # split and pace it to feel natural in the UI.
                    text = msg.content or ""
                    # Strip any image the LLM invented — only tool-returned URLs allowed
                    allowed_urls = {url for _, url in thumbnails_seen}
                    text = _strip_hallucinated_images(text, allowed_urls)
                    text = _strip_padding_sections(text)
                    text = _strip_emojis(text)
                    if thumbnails_seen:
                        new_thumbs = [
                            (tname, url) for tname, url in thumbnails_seen[:4]
                            if _normalize_img_url(url) not in text and url not in text
                        ]
                        if new_thumbs:
                            gallery = "\n\n"
                            for tname, url in new_thumbs:
                                gallery += f'<bird-img name="{tname}" src="{url}">\n'
                            text += gallery
                    if phenology_tags:
                        text += "\n\n" + phenology_tags[0]
                    import re as _re
                    for chunk in _re.split(r'(\s+)', text):
                        if chunk:
                            yield _sse("token", {"text": chunk})
                            time.sleep(0.03)  # ~30ms per word — readable pace
                    history.append({"role": "assistant", "content": text})
                    _save_session(session_id, history)
                    yield _sse("done", {"tool_calls": tool_calls_log})
                    return

                # Tool round — emit tool events as each tool runs
                messages.append(msg.model_dump(exclude_none=True))
                for tc in msg.tool_calls:
                    fn_name = tc.function.name
                    try:
                        fn_args = json.loads(tc.function.arguments)
                    except json.JSONDecodeError:
                        fn_args = {}

                    result, should_log = _run_tool_event(fn_name, fn_args, wiki_calls)
                    if should_log:
                        tool_calls_log.append({"name": fn_name, "args": fn_args})
                        yield _sse("tool", {"name": fn_name, "args": fn_args})

                    _extract_thumbnails(result, thumbnails_seen)
                    _extract_phenology_tag(result, phenology_tags)
                    messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

            answer = "(max tool rounds reached)"
            history.append({"role": "assistant", "content": answer})
            _save_session(session_id, history)
            yield _sse("token", {"text": answer})
            yield _sse("done", {"tool_calls": tool_calls_log})

        except Exception as e:
            if history and history[-1].get("role") == "user":
                history.pop()
                _save_session(session_id, history)
            yield _sse("error", {"message": str(e)})

    return Response(
        generate(),
        content_type="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@app.route("/api/observations")
def api_observations():
    if "observations" not in _PANEL_CACHE:
        rows = observations_for_map(GRAPH)
        out = []
        for r in rows:
            try:
                lat = float(r.get("lat", ""))
                lon = float(r.get("lon", ""))
            except (TypeError, ValueError):
                continue
            if r.get("atypicalReason"):
                continue  # atypical observations not shown on map
            out.append({
                "scientificName": r.get("scientificName", ""),
                "commonName": r.get("commonNameFr") or r.get("commonNameEn") or r.get("commonNameDa") or "",
                "lat": lat,
                "lon": lon,
                "date": r.get("date", ""),
                "locality": r.get("locality", ""),
                "count": r.get("count", ""),
            })
        _PANEL_CACHE["observations"] = out
    return jsonify(_PANEL_CACHE["observations"])


@app.route("/api/species")
def api_species():
    if "species" not in _PANEL_CACHE:
        q = """
PREFIX bird: <https://birdology.org/ontology/>
PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
SELECT ?species ?scientificName ?commonNameEn ?commonNameFr ?commonNameDa ?thumbnail
       (COUNT(?obs) AS ?obsCount)
WHERE {
    ?species a bird:Species ;
             dwc:scientificName ?scientificName ;
             bird:hasObservation ?obs .
    FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
    OPTIONAL { ?species bird:thumbnailUrl ?thumbnail }
    OPTIONAL { ?species bird:commonNameEn ?commonNameEn }
    OPTIONAL { ?species bird:commonNameFr ?commonNameFr }
    OPTIONAL { ?species bird:commonNameDa ?commonNameDa }
}
GROUP BY ?species ?scientificName ?commonNameEn ?commonNameFr ?commonNameDa ?thumbnail
ORDER BY DESC(?obsCount)
LIMIT 30
"""
        results = GRAPH.query(q)
        out = []
        for row in results:
            d = {str(var): str(row[var]) for var in results.vars if row[var] is not None}
            out.append({
                "scientificName": d.get("scientificName", ""),
                "commonName": d.get("commonNameFr") or d.get("commonNameEn") or d.get("commonNameDa") or "",
                "thumbnail": d.get("thumbnail", ""),
                "obsCount": int(d.get("obsCount", "0") or 0),
            })
        _PANEL_CACHE["species"] = out
    return jsonify(_PANEL_CACHE["species"])


@app.route("/api/dashboard")
def api_dashboard():
    """Return dashboard data: rare species (eBird live) and hotspots."""
    rare_rows = _dashboard_rare()
    hotspot_rows = _dashboard_hotspots()
    return jsonify({
        "rare": rare_rows,
        "hotspots": hotspot_rows,
    })


def _dashboard_rare() -> list[dict]:
    """Fetch recent notable observations from eBird live API, cross-reference
    with IUCN status from the graph. Falls back to graph data if no API key."""
    api_key = os.getenv("EBIRD_API_KEY", "")
    if not api_key:
        return _dashboard_rare_fallback()

    try:
        from birdology.ingestion.ebird import fetch_recent_denmark
        raw = fetch_recent_denmark(api_key, days=14)
    except Exception:
        return _dashboard_rare_fallback()

    # Build a sciName → IUCN status lookup from the graph
    iucn_q = """
    PREFIX bird: <https://birdology.org/ontology/>
    PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
    SELECT ?scientificName ?status ?commonNameFr WHERE {
        ?sp a bird:Species ;
            dwc:scientificName ?scientificName ;
            bird:conservationStatus ?status .
        FILTER(?status IN ("CR", "EN", "VU", "NT"))
        OPTIONAL { ?sp bird:commonNameFr ?commonNameFr }
    }
    """
    iucn_map = {}  # sciName → {status, commonNameFr}
    for row in GRAPH.query(iucn_q):
        iucn_map[str(row.scientificName)] = {
            "status": str(row.status),
            "commonNameFr": str(row.commonNameFr) if row.commonNameFr else None,
        }

    iucn_rank = {"CR": 0, "EN": 1, "VU": 2, "NT": 3}
    results = []
    seen = set()
    for obs in raw:
        sci = obs.get("sciName", "")
        info = iucn_map.get(sci)
        if not info or sci in seen:
            continue
        seen.add(sci)
        results.append({
            "scientificName": sci,
            "commonNameEn": obs.get("comName"),
            "commonNameFr": info.get("commonNameFr"),
            "status": info["status"],
            "date": obs.get("obsDt"),
            "locality": obs.get("locName"),
            "lat": obs.get("lat"),
            "lon": obs.get("lng"),
        })

    results.sort(key=lambda r: (iucn_rank.get(r["status"], 99), r["scientificName"]))
    return results[:12]


def _dashboard_rare_fallback() -> list[dict]:
    """Graph-only fallback for rare species when eBird API is unavailable."""
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
    SELECT DISTINCT ?species ?scientificName ?commonNameEn ?commonNameFr ?status ?date ?locality
    WHERE {
        ?species a bird:Species ;
                 dwc:scientificName ?scientificName ;
                 bird:conservationStatus ?status ;
                 bird:hasObservation ?obs .
        FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
        FILTER(?status IN ("CR", "EN", "VU", "NT"))
        ?obs bird:observedOn ?date .
        OPTIONAL { ?obs bird:observedAt ?loc . ?loc bird:locality ?locality }
        OPTIONAL { ?species bird:commonNameEn ?commonNameEn }
        OPTIONAL { ?species bird:commonNameFr ?commonNameFr }
    }
    ORDER BY ?status DESC(?date)
    LIMIT 20
    """
    rows = []
    seen = set()
    for row in GRAPH.query(q):
        sp = str(row.species)
        if sp in seen:
            continue
        seen.add(sp)
        rows.append({
            "scientificName": str(row.scientificName),
            "commonNameEn": str(row.commonNameEn) if row.commonNameEn else None,
            "commonNameFr": str(row.commonNameFr) if row.commonNameFr else None,
            "status": str(row.status),
            "date": str(row.date) if row.date else None,
            "locality": str(row.locality) if row.locality else None,
        })
    return rows


def _dashboard_hotspots() -> list[dict]:
    """Compute hotspots on-the-fly from observation counts per location.

    Returns up to 30 locations with lat/lon for the Leaflet map.
    """
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    SELECT ?loc ?locality ?lat ?lon (COUNT(?obs) AS ?n) WHERE {
        ?obs bird:observedAt ?loc .
        ?loc a bird:Location ;
             bird:latitude ?lat ;
             bird:longitude ?lon .
        OPTIONAL { ?loc bird:locality ?locality }
    }
    GROUP BY ?loc ?locality ?lat ?lon
    HAVING (COUNT(?obs) >= 5)
    ORDER BY DESC(?n)
    LIMIT 30
    """
    rows = []
    for row in GRAPH.query(q):
        lat = str(row.lat) if row.lat else None
        lon = str(row.lon) if row.lon else None
        if lat and lon:
            rows.append({
                "locality": str(row.locality) if row.locality else None,
                "lat": lat,
                "lon": lon,
                "count": int(str(row.n)),
            })
    return rows


@app.route("/api/weekend")
def api_weekend():
    """Suggest where to go and what to see this weekend.

    Combines: nearest hotspots + species present this month + migration status.
    Accepts ?lat=...&lon=... query params (user geolocation); defaults to
    Copenhagen center (55.6761, 12.5683) if not provided.
    """
    import datetime
    import math

    try:
        user_lat = float(request.args.get("lat", 55.6761))
        user_lon = float(request.args.get("lon", 12.5683))
    except (TypeError, ValueError):
        user_lat, user_lon = 55.6761, 12.5683

    month = datetime.date.today().month

    # 1) Get all hotspot locations with coords (low threshold to include sparse areas)
    hotspot_q = """
    PREFIX bird: <https://birdology.org/ontology/>
    SELECT ?loc ?locality ?lat ?lon (COUNT(?obs) AS ?n) WHERE {
        ?obs bird:observedAt ?loc .
        ?loc a bird:Location ;
             bird:latitude ?lat ;
             bird:longitude ?lon .
        OPTIONAL { ?loc bird:locality ?locality }
    }
    GROUP BY ?loc ?locality ?lat ?lon
    ORDER BY DESC(?n)
    LIMIT 100
    """
    hotspots = []
    for row in GRAPH.query(hotspot_q):
        lat = row.lat
        lon = row.lon
        if lat is None or lon is None:
            continue
        try:
            flat, flon = float(str(lat)), float(str(lon))
        except (TypeError, ValueError):
            continue
        dlat = math.radians(flat - user_lat)
        dlon = math.radians(flon - user_lon)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(user_lat))
             * math.cos(math.radians(flat))
             * math.sin(dlon / 2) ** 2)
        dist_km = 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        hotspots.append({
            "loc": str(row.loc),
            "locality": str(row.locality) if row.locality else None,
            "lat": flat,
            "lon": flon,
            "count": int(str(row.n)),
            "dist_km": round(dist_km, 1),
        })

    hotspots.sort(key=lambda h: h["dist_km"])
    top_hotspots = hotspots[:5]

    if not top_hotspots:
        return jsonify({"spots": []})

    # 2) For each hotspot, find species observed there that are present this month
    spots = []
    for hs in top_hotspots:
        species_q = f"""
        PREFIX bird: <https://birdology.org/ontology/>
        PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
        SELECT DISTINCT ?species ?scientificName ?commonNameFr ?commonNameEn
                        ?migStatus ?status ?thumbnail
        WHERE {{
            ?species a bird:Species ;
                     dwc:scientificName ?scientificName ;
                     bird:hasObservation ?obs ;
                     bird:typicallyPresentInMonth {month} .
            ?obs bird:observedAt <{hs["loc"]}> .
            FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
            OPTIONAL {{ ?species bird:commonNameFr      ?commonNameFr }}
            OPTIONAL {{ ?species bird:commonNameEn      ?commonNameEn }}
            OPTIONAL {{ ?species bird:migrationStatus   ?migStatus }}
            OPTIONAL {{ ?species bird:conservationStatus ?status }}
            OPTIONAL {{ ?species bird:thumbnailUrl      ?thumbnail }}
        }}
        ORDER BY ?migStatus ?scientificName
        """
        birds = []
        seen = set()
        for row in GRAPH.query(species_q):
            sci = str(row.scientificName)
            if sci in seen:
                continue
            seen.add(sci)
            birds.append({
                "scientificName": sci,
                "commonNameFr": str(row.commonNameFr) if row.commonNameFr else None,
                "commonNameEn": str(row.commonNameEn) if row.commonNameEn else None,
                "migrationStatus": str(row.migStatus) if row.migStatus else None,
                "conservationStatus": str(row.status) if row.status else None,
                "thumbnail": str(row.thumbnail) if row.thumbnail else None,
            })

        # If no typicallyPresentInMonth data, fall back to all species at this location
        if not birds:
            fallback_q = f"""
            PREFIX bird: <https://birdology.org/ontology/>
            PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
            SELECT DISTINCT ?species ?scientificName ?commonNameFr ?commonNameEn
                            ?migStatus ?status ?thumbnail
            WHERE {{
                ?species a bird:Species ;
                         dwc:scientificName ?scientificName ;
                         bird:hasObservation ?obs .
                ?obs bird:observedAt <{hs["loc"]}> .
                FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
                OPTIONAL {{ ?species bird:commonNameFr      ?commonNameFr }}
                OPTIONAL {{ ?species bird:commonNameEn      ?commonNameEn }}
                OPTIONAL {{ ?species bird:migrationStatus   ?migStatus }}
                OPTIONAL {{ ?species bird:conservationStatus ?status }}
                OPTIONAL {{ ?species bird:thumbnailUrl      ?thumbnail }}
            }}
            ORDER BY ?scientificName
            """
            for row in GRAPH.query(fallback_q):
                sci = str(row.scientificName)
                if sci in seen:
                    continue
                seen.add(sci)
                birds.append({
                    "scientificName": sci,
                    "commonNameFr": str(row.commonNameFr) if row.commonNameFr else None,
                    "commonNameEn": str(row.commonNameEn) if row.commonNameEn else None,
                    "migrationStatus": str(row.migStatus) if row.migStatus else None,
                    "conservationStatus": str(row.status) if row.status else None,
                    "thumbnail": str(row.thumbnail) if row.thumbnail else None,
                })

        spots.append({
            "locality": hs["locality"],
            "lat": hs["lat"],
            "lon": hs["lon"],
            "dist_km": hs["dist_km"],
            "obs_count": hs["count"],
            "species": birds[:15],
        })

    return jsonify({"month": month, "spots": spots})


@app.route("/api/reset", methods=["POST"])
def api_reset():
    """Clear conversation history for a session."""
    data = request.get_json() or {}
    session_id = data.get("session_id", "default")
    with _sessions_lock:
        with _db() as con:
            con.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
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

    db_path = ttl_path.parent / "sessions.db"
    _init_session_db(db_path)
    print(f"Session DB: {db_path}")

    print(f"Loading graph from {ttl_path} …")
    GRAPH = load_graph(ttl_path)
    print(f"\nBirdology Graph-RAG web UI")
    print(f"  http://localhost:{args.port}\n")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
