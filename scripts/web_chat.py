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
            OPTIONAL {{ ?species bird:thumbnail         ?thumbnail }}
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
                OPTIONAL {{ ?species bird:thumbnail         ?thumbnail }}
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
