"""
Integration tests for the Birdology web chat API.

These tests send real HTTP requests to a running web_chat.py server.
They are automatically skipped if the server is not available on localhost:5000.

Run manually:
    uv run pytest tests/test_chat_integration.py -v

The server must be started separately:
    uv run python scripts/web_chat.py --input output/birdology_reasoned.ttl
"""
import json
import pytest
import requests

_BASE = "http://localhost:5000"
_TIMEOUT = 120  # seconds — LLM calls can be slow


def _server_available() -> bool:
    try:
        requests.get(f"{_BASE}/api/dashboard", timeout=3)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _server_available(),
    reason="web_chat.py server not running on localhost:5000",
)


def _chat(message: str, session_id: str) -> dict:
    """POST to /api/chat and return the parsed JSON response."""
    r = requests.post(
        f"{_BASE}/api/chat",
        json={"message": message, "session_id": session_id},
        timeout=_TIMEOUT,
    )
    assert r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}"
    data = r.json()
    assert "answer" in data, f"No 'answer' in response: {data}"
    return data


def _tool_names(data: dict) -> list[str]:
    return [t["name"] for t in data.get("tool_calls", [])]


def _tool_args(data: dict, tool_name: str) -> dict:
    for t in data.get("tool_calls", []):
        if t["name"] == tool_name:
            return t.get("args", {})
    return {}


# ── Tool routing tests ────────────────────────────────────────────────────────

class TestToolRouting:
    """Verify the LLM calls the right tool for each type of question."""

    def test_where_to_watch_uses_correct_tool(self):
        """'où observer demain' → where_to_watch"""
        data = _chat("où observer demain ?", "int_where1")
        assert "where_to_watch" in _tool_names(data), \
            f"Expected where_to_watch, got: {_tool_names(data)}"

    def test_where_to_watch_uses_current_month(self):
        """where_to_watch should use current month (May = 5)"""
        import datetime
        data = _chat("où observer ce week-end ?", "int_where2")
        if "where_to_watch" in _tool_names(data):
            args = _tool_args(data, "where_to_watch")
            month = args.get("month")
            if month is not None:
                assert month == datetime.date.today().month, \
                    f"Expected month {datetime.date.today().month}, got {month}"

    def test_recent_ebird_uses_source_filter(self):
        """'observations cette semaine' → recent_observations with source=ebird"""
        data = _chat("observations de cette semaine au Danemark ?", "int_ebird1")
        tools = _tool_names(data)
        assert "recent_observations" in tools or "live_observations" in tools, \
            f"Expected recent_observations or live_observations, got: {tools}"
        if "recent_observations" in tools:
            args = _tool_args(data, "recent_observations")
            assert args.get("source") in ("ebird", None), \
                f"Expected source=ebird, got {args.get('source')}"

    def test_species_question_calls_find_species_first(self):
        """Any species question should call find_species first."""
        data = _chat("parle-moi du rouge-gorge", "int_sp1")
        tools = _tool_names(data)
        assert "find_species" in tools, \
            f"Expected find_species in tools, got: {tools}"
        # find_species should be first
        assert tools[0] == "find_species", \
            f"find_species should be first, got: {tools}"

    def test_location_query_uses_locality_param(self):
        """'observations à Brøndby Strand' → recent_observations with locality param."""
        data = _chat("quelles observations à Brøndby Strand ?", "int_loc1")
        tools = _tool_names(data)
        assert "recent_observations" in tools, \
            f"Expected recent_observations, got: {tools}"
        args = _tool_args(data, "recent_observations")
        assert args.get("locality"), \
            f"Expected locality param, got args: {args}"
        assert "species" not in args or not args.get("species"), \
            f"Location name passed as species: {args}"

    def test_find_species_passes_exact_name(self):
        """find_species should receive the name as given, not translated."""
        data = _chat("observations de l'huitrier pie ?", "int_sp2")
        if "find_species" in _tool_names(data):
            args = _tool_args(data, "find_species")
            name = args.get("name", "").lower()
            assert "huitrier" in name or "ostralegus" in name, \
                f"find_species called with unexpected name: {args.get('name')}"

    def test_month_question_uses_observations_by_month(self):
        """'quels oiseaux en mars' → observations_by_month"""
        data = _chat("quels oiseaux peut-on voir en mars au Danemark ?", "int_month1")
        tools = _tool_names(data)
        assert "observations_by_month" in tools or "currently_present" in tools, \
            f"Expected observations_by_month or currently_present, got: {tools}"
        if "observations_by_month" in tools:
            args = _tool_args(data, "observations_by_month")
            assert args.get("month") == 3, f"Expected month=3, got {args.get('month')}"

    def test_wikipedia_called_for_behavior(self):
        """'comportement du rouge-gorge' → search_wikipedia called"""
        data = _chat("décris le comportement du rouge-gorge", "int_wiki1")
        tools = _tool_names(data)
        assert "search_wikipedia" in tools, \
            f"Expected search_wikipedia in tools, got: {tools}"

    def test_wikipedia_called_at_most_once(self):
        """search_wikipedia should never be called more than once per question."""
        data = _chat("habitat et régime alimentaire de la mésange bleue", "int_wiki2")
        wiki_calls = [t for t in _tool_names(data) if t == "search_wikipedia"]
        assert len(wiki_calls) <= 1, \
            f"search_wikipedia called {len(wiki_calls)} times, expected at most 1"


# ── Anti-hallucination tests ──────────────────────────────────────────────────

class TestAntiHallucination:
    """Verify the LLM doesn't add unwanted editorial sections."""

    FORBIDDEN_SECTIONS = [
        "À vérifier", "A vérifier", "Recommandations", "Précautions",
        "Espèces absentes", "Espèces confondues", "À surveiller",
        "identification tips",
    ]

    def test_live_obs_no_padding(self):
        """Live observations response should not contain editorial sections."""
        data = _chat("observations des 30 derniers jours au Danemark ?", "int_pad1")
        answer = data["answer"]
        for forbidden in self.FORBIDDEN_SECTIONS:
            assert forbidden not in answer, \
                f"Response contains forbidden section '{forbidden}'"

    def test_where_to_watch_no_padding(self):
        """where_to_watch response should not contain editorial sections."""
        data = _chat("où aller observer des oiseaux ce week-end ?", "int_pad2")
        answer = data["answer"]
        for forbidden in self.FORBIDDEN_SECTIONS:
            assert forbidden not in answer, \
                f"Response contains forbidden section '{forbidden}'"

    def test_response_not_empty(self):
        """Every question should return a non-empty answer."""
        data = _chat("combien d'espèces dans le graphe ?", "int_empty1")
        assert len(data["answer"].strip()) > 20, "Answer is too short"


# ── API endpoint tests ────────────────────────────────────────────────────────

class TestApiEndpoints:
    """Smoke tests for non-chat API endpoints."""

    def test_dashboard_returns_hotspots(self):
        r = requests.get(f"{_BASE}/api/dashboard", timeout=10)
        assert r.status_code == 200
        data = r.json()
        assert "hotspots" in data
        assert isinstance(data["hotspots"], list)

    def test_dashboard_returns_rare(self):
        r = requests.get(f"{_BASE}/api/dashboard", timeout=10)
        data = r.json()
        assert "rare" in data
        assert isinstance(data["rare"], list)

    def test_observations_returns_list(self):
        r = requests.get(f"{_BASE}/api/observations", timeout=30)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) > 1000, f"Expected >1000 observations, got {len(data)}"

    def test_observations_have_required_fields(self):
        r = requests.get(f"{_BASE}/api/observations", timeout=30)
        data = r.json()
        sample = data[:5]
        for obs in sample:
            assert "lat" in obs, f"Missing 'lat' in {obs}"
            assert "lon" in obs, f"Missing 'lon' in {obs}"

    def test_weekend_endpoint(self):
        r = requests.get(f"{_BASE}/api/weekend", timeout=15)
        assert r.status_code == 200
        data = r.json()
        assert "spots" in data or "month" in data

    def test_reset_endpoint(self):
        r = requests.post(
            f"{_BASE}/api/reset",
            json={"session_id": "int_reset_test"},
            timeout=5,
        )
        assert r.status_code == 200


# ── Streaming endpoint tests ──────────────────────────────────────────────────

def _stream_chat(message: str, session_id: str) -> dict:
    """POST to /api/chat/stream and collect all SSE events.

    Returns:
        {
            "answer": str,          # concatenated token texts
            "tools": list[dict],    # tool events [{name, args}, ...]
            "done": bool,           # True if done event received
            "error": str | None,    # error message if error event received
        }
    """
    result = {"answer": "", "tools": [], "done": False, "error": None}
    r = requests.post(
        f"{_BASE}/api/chat/stream",
        json={"message": message, "session_id": session_id},
        stream=True,
        timeout=_TIMEOUT,
    )
    assert r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}"

    current_event = ""
    for raw_line in r.iter_lines(decode_unicode=True):
        if raw_line.startswith("event: "):
            current_event = raw_line[7:].strip()
        elif raw_line.startswith("data: "):
            try:
                payload = json.loads(raw_line[6:])
            except json.JSONDecodeError:
                continue
            if current_event == "token":
                result["answer"] += payload.get("text", "")
            elif current_event == "tool":
                result["tools"].append({"name": payload.get("name"), "args": payload.get("args", {})})
            elif current_event == "done":
                result["done"] = True
            elif current_event == "error":
                result["error"] = payload.get("message", "unknown error")
            current_event = ""
        elif raw_line == "":
            current_event = ""

    return result


class TestStreaming:
    """Integration tests for the /api/chat/stream SSE endpoint."""

    def test_stream_returns_200_with_correct_content_type(self):
        r = requests.post(
            f"{_BASE}/api/chat/stream",
            json={"message": "combien d'espèces ?", "session_id": "str_ct"},
            stream=True,
            timeout=_TIMEOUT,
        )
        assert r.status_code == 200
        assert "text/event-stream" in r.headers.get("Content-Type", "")

    def test_stream_emits_done_event(self):
        data = _stream_chat("combien d'espèces dans le graphe ?", "str_done1")
        assert data["done"], "Stream did not emit a 'done' event"

    def test_stream_answer_not_empty(self):
        data = _stream_chat("combien d'espèces dans le graphe ?", "str_ans1")
        assert len(data["answer"].strip()) > 20, \
            f"Streamed answer is too short: {repr(data['answer'])}"

    def test_stream_no_error(self):
        data = _stream_chat("combien d'espèces dans le graphe ?", "str_err1")
        assert data["error"] is None, f"Stream returned error: {data['error']}"

    def test_stream_emits_tool_events(self):
        """A question that requires tools should emit at least one tool event."""
        data = _stream_chat("parle-moi du rouge-gorge", "str_tool1")
        assert len(data["tools"]) >= 1, \
            f"Expected at least one tool event, got: {data['tools']}"

    def test_stream_tool_events_before_tokens(self):
        """When both tool and token events are present, tools must come first."""
        r = requests.post(
            f"{_BASE}/api/chat/stream",
            json={"message": "parle-moi du rouge-gorge", "session_id": "str_order1"},
            stream=True,
            timeout=_TIMEOUT,
        )
        events = []
        current_event = ""
        for raw_line in r.iter_lines(decode_unicode=True):
            if raw_line.startswith("event: "):
                current_event = raw_line[7:].strip()
            elif raw_line.startswith("data: "):
                events.append(current_event)
                current_event = ""
            elif raw_line == "":
                current_event = ""

        tool_idx = next((i for i, e in enumerate(events) if e == "tool"), None)
        token_idx = next((i for i, e in enumerate(events) if e == "token"), None)
        assert tool_idx is not None, "No tool event found"
        # If the LLM returned no text (empty answer), skip ordering check
        if token_idx is not None:
            assert tool_idx < token_idx, \
                f"First tool event (idx {tool_idx}) should come before first token (idx {token_idx})"

    def test_stream_tokens_form_valid_text(self):
        """Concatenated tokens should form a readable sentence."""
        data = _stream_chat("combien d'espèces dans le graphe ?", "str_tok1")
        # Should contain at least one digit (species count) and some letters
        import re
        assert re.search(r'\d', data["answer"]), "Answer contains no number"
        assert re.search(r'[a-zA-ZÀ-ÿ]{3,}', data["answer"]), "Answer contains no words"

    def test_stream_wikipedia_at_most_once(self):
        """search_wikipedia should appear at most once in tool events."""
        data = _stream_chat("habitat et comportement de la mésange bleue", "str_wiki1")
        wiki_calls = [t for t in data["tools"] if t["name"] == "search_wikipedia"]
        assert len(wiki_calls) <= 1, \
            f"search_wikipedia called {len(wiki_calls)} times in stream, expected at most 1"

    def test_stream_empty_message_returns_400(self):
        r = requests.post(
            f"{_BASE}/api/chat/stream",
            json={"message": "", "session_id": "str_empty1"},
            timeout=5,
        )
        assert r.status_code == 400
