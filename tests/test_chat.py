"""Tests for the Graph-RAG chat tool execution — no API key needed."""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD

from birdology.namespaces import BIRD, DWC, LOC, OBS, TAXON

# Import the tool runner and formatting from chat.py
from chat import TOOLS, SYSTEM_PROMPT, _run_tool, _fmt


# ── Fixture graph ────────────────────────────────────────────────────────────

def _make_graph() -> Graph:
    """Small graph with two species and one observation each."""
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)

    ROBIN = TAXON["species/robi"]
    WOODP = TAXON["species/euwoo1"]
    OBS_R = OBS["obs_robin"]
    LOC_R = LOC["loc_cph"]

    for uri in [ROBIN, WOODP]:
        g.add((uri, RDF.type, BIRD.Species))

    # Robin
    g.add((ROBIN, DWC.scientificName, Literal("Erithacus rubecula")))
    g.add((ROBIN, BIRD.commonNameEn, Literal("European Robin")))
    g.add((ROBIN, BIRD.commonNameDa, Literal("Rødhals")))
    g.add((ROBIN, BIRD.commonNameFr, Literal("Rouge-gorge familier")))
    g.add((ROBIN, BIRD.eBirdCode, Literal("eurrob1")))
    g.add((ROBIN, DWC.family, Literal("Muscicapidae")))
    g.add((ROBIN, DWC.order, Literal("Passeriformes")))

    # Woodpecker
    g.add((WOODP, DWC.scientificName, Literal("Dendrocopos major")))
    g.add((WOODP, BIRD.commonNameEn, Literal("Great Spotted Woodpecker")))
    g.add((WOODP, BIRD.commonNameDa, Literal("Stor Flagspætte")))
    g.add((WOODP, BIRD.eBirdCode, Literal("euwoo1")))
    g.add((WOODP, DWC.family, Literal("Picidae")))
    g.add((WOODP, DWC.order, Literal("Piciformes")))

    # Observation for Robin near Copenhagen
    g.add((ROBIN, BIRD.hasObservation, OBS_R))
    g.add((OBS_R, RDF.type, BIRD.Observation))
    g.add((OBS_R, BIRD.observedOn, Literal("2024-03-15", datatype=XSD.date)))
    g.add((OBS_R, BIRD.individualCount, Literal(3, datatype=XSD.integer)))
    g.add((OBS_R, BIRD.observedAt, LOC_R))
    g.add((LOC_R, BIRD.latitude, Literal("55.6918", datatype=XSD.decimal)))
    g.add((LOC_R, BIRD.longitude, Literal("12.5559", datatype=XSD.decimal)))
    g.add((LOC_R, BIRD.locality, Literal("Assistens Kirkegård")))

    return g


# ── Tool definitions ─────────────────────────────────────────────────────────

def test_all_tools_have_required_fields():
    """Every tool must have name, description, and input_schema."""
    for tool in TOOLS:
        assert "name" in tool
        assert "description" in tool
        assert "input_schema" in tool
        assert tool["input_schema"]["type"] == "object"


def test_tool_names_unique():
    names = [t["name"] for t in TOOLS]
    assert len(names) == len(set(names))


def test_system_prompt_not_empty():
    assert len(SYSTEM_PROMPT) > 100


# ── _fmt helper ──────────────────────────────────────────────────────────────

def test_fmt_empty():
    assert _fmt([]) == "No results found."


def test_fmt_truncation():
    rows = [{"x": i} for i in range(30)]
    out = _fmt(rows, limit=5)
    assert "25 more results" in out


def test_fmt_json_valid():
    rows = [{"name": "Rødhals", "sci": "Erithacus rubecula"}]
    out = _fmt(rows)
    parsed = json.loads(out)
    assert parsed[0]["name"] == "Rødhals"


# ── Tool execution with graph ───────────────────────────────────────────────

def test_find_species():
    g = _make_graph()
    result = _run_tool("find_species", {"name": "Robin"}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1
    assert any("Erithacus" in r.get("scientificName", "") for r in parsed)


def test_find_species_danish():
    g = _make_graph()
    result = _run_tool("find_species", {"name": "Rødhals"}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1


def test_find_species_no_match():
    g = _make_graph()
    result = _run_tool("find_species", {"name": "Flamingo"}, g)
    assert result == "No results found."


def test_species_by_family():
    g = _make_graph()
    result = _run_tool("species_by_family", {"family": "Muscicapidae"}, g)
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert "Erithacus" in parsed[0]["scientificName"]


def test_species_by_order():
    g = _make_graph()
    result = _run_tool("species_by_order", {"order": "Passeriformes"}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1


def test_recent_observations_filtered():
    g = _make_graph()
    result = _run_tool("recent_observations", {"species": "Robin"}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1
    assert any("2024-03-15" in r.get("date", "") for r in parsed)


def test_recent_observations_no_filter():
    g = _make_graph()
    result = _run_tool("recent_observations", {}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1


def test_nearby_birds_default():
    g = _make_graph()
    result = _run_tool("nearby_birds", {}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1
    assert any("Erithacus" in r.get("scientificName", "") for r in parsed)


def test_nearby_birds_custom_location():
    g = _make_graph()
    # Far away from the observation — should find nothing
    result = _run_tool("nearby_birds", {"lat": 40.0, "lon": -74.0, "radius_km": 1.0}, g)
    assert result == "No results found."


def test_taxonomy_summary():
    g = _make_graph()
    result = _run_tool("taxonomy_summary", {}, g)
    parsed = json.loads(result)
    assert parsed["species"] == 2
    assert parsed["observations"] == 1


def test_currently_present_no_data():
    """Without reasoner data, currently_present returns empty."""
    g = _make_graph()
    result = _run_tool("currently_present", {"month": 3}, g)
    assert result == "No results found."


def test_live_observations_no_key(monkeypatch):
    """live_observations returns an error message when EBIRD_API_KEY is unset."""
    monkeypatch.delenv("EBIRD_API_KEY", raising=False)
    g = _make_graph()
    result = _run_tool("live_observations", {"days": 7}, g)
    assert "EBIRD_API_KEY" in result


def test_live_observations_mock(monkeypatch):
    """live_observations formats eBird API results correctly."""
    monkeypatch.setenv("EBIRD_API_KEY", "fake_key")
    fake_data = [
        {"comName": "Smew", "sciName": "Mergellus albellus", "obsDt": "2026-04-19",
         "locName": "Tivoli", "lat": 55.67, "lng": 12.57, "howMany": 2},
    ]
    import chat
    monkeypatch.setattr(chat, "fetch_recent_denmark", lambda key, days: fake_data)
    g = _make_graph()
    result = _run_tool("live_observations", {"days": 7}, g)
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert parsed[0]["species"] == "Smew"
    assert parsed[0]["date"] == "2026-04-19"


def test_unknown_tool():
    g = _make_graph()
    result = _run_tool("nonexistent", {}, g)
    assert "Unknown tool" in result
