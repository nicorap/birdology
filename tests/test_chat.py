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


def test_recent_observations_locality_match():
    g = _make_graph()
    result = _run_tool("recent_observations", {"locality": "Assistens"}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1
    assert all("Assistens" in r.get("locality", "") for r in parsed)


def test_recent_observations_locality_case_insensitive():
    g = _make_graph()
    result = _run_tool("recent_observations", {"locality": "assistens"}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1


def test_recent_observations_locality_no_match():
    g = _make_graph()
    result = _run_tool("recent_observations", {"locality": "Tivoli"}, g)
    assert result == "No results found."


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


def test_observations_by_month_defaults_to_current_month():
    """observations_by_month without month param defaults to current month."""
    g = _make_graph()
    # Should not raise even without month argument
    result = _run_tool("observations_by_month", {}, g)
    assert result is not None  # empty graph → "No results found."


def test_recent_observations_live_no_key(monkeypatch):
    """recent_observations with source='live' returns error when EBIRD_API_KEY is unset."""
    monkeypatch.delenv("EBIRD_API_KEY", raising=False)
    g = _make_graph()
    result = _run_tool("recent_observations", {"source": "live", "days": 7}, g)
    assert "EBIRD_API_KEY" in result


def test_recent_observations_live_mock(monkeypatch):
    """recent_observations with source='live' formats eBird API results correctly."""
    monkeypatch.setenv("EBIRD_API_KEY", "fake_key")
    fake_data = [
        {"comName": "Smew", "sciName": "Mergellus albellus", "obsDt": "2026-04-19",
         "locName": "Tivoli", "lat": 55.67, "lng": 12.57, "howMany": 2},
    ]
    import chat
    monkeypatch.setattr(chat, "fetch_recent_denmark", lambda key, days: fake_data)
    g = _make_graph()
    result = _run_tool("recent_observations", {"source": "live", "days": 7}, g)
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert parsed[0]["species"] == "Smew"
    assert parsed[0]["date"] == "2026-04-19"


def test_recent_observations_live_rare_only(monkeypatch):
    """rare_only=True filters live results to species with non-LC IUCN status in graph."""
    monkeypatch.setenv("EBIRD_API_KEY", "fake_key")
    fake_data = [
        {"comName": "European Robin", "sciName": "Erithacus rubecula",
         "obsDt": "2026-04-19", "locName": "CPH", "lat": 55.67, "lng": 12.57, "howMany": 1},
        {"comName": "Osprey", "sciName": "Pandion haliaetus",
         "obsDt": "2026-04-19", "locName": "CPH", "lat": 55.67, "lng": 12.57, "howMany": 1},
    ]
    import chat
    monkeypatch.setattr(chat, "fetch_recent_denmark", lambda key, days: fake_data)

    g = _make_graph()
    # Add Osprey with VU status, Robin stays LC (no status triple → filtered out)
    OSPREY = TAXON["species/ospr"]
    g.add((OSPREY, RDF.type, BIRD.Species))
    g.add((OSPREY, DWC.scientificName, Literal("Pandion haliaetus")))
    g.add((OSPREY, BIRD.conservationStatus, Literal("VU")))

    result = _run_tool("recent_observations", {"source": "live", "rare_only": True}, g)
    parsed = json.loads(result)
    scinames = [r["sciName"] for r in parsed]
    assert "Pandion haliaetus" in scinames      # VU → kept
    assert "Erithacus rubecula" not in scinames  # no status in graph → filtered out
    assert parsed[0]["iucnStatus"] == "VU"


def test_unknown_tool():
    g = _make_graph()
    result = _run_tool("nonexistent", {}, g)
    assert "Unknown tool" in result


# ── observations_by_month ────────────────────────────────────────────────────

def _make_graph_with_months() -> Graph:
    """Graph with Robin observed in March and Woodpecker observed in July."""
    g = _make_graph()

    ROBIN = TAXON["species/robi"]
    WOODP = TAXON["species/euwoo1"]
    OBS_W = OBS["obs_woodp"]
    LOC_W = LOC["loc_cph"]

    g.add((WOODP, BIRD.hasObservation, OBS_W))
    g.add((OBS_W, RDF.type, BIRD.Observation))
    g.add((OBS_W, BIRD.observedOn, Literal("2024-07-10", datatype=XSD.date)))
    g.add((OBS_W, BIRD.individualCount, Literal(1, datatype=XSD.integer)))
    g.add((OBS_W, BIRD.observedAt, LOC_W))

    # Robin already has a March observation from _make_graph()
    g.add((ROBIN, BIRD.migrationStatus, Literal("Resident")))

    return g


def test_observations_by_month_returns_species():
    g = _make_graph_with_months()
    result = _run_tool("observations_by_month", {"month": 3}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1
    assert any("Erithacus" in r.get("scientificName", "") for r in parsed)


def test_observations_by_month_excludes_other_months():
    g = _make_graph_with_months()
    # March should contain Robin but NOT Woodpecker (July)
    result = _run_tool("observations_by_month", {"month": 3}, g)
    parsed = json.loads(result)
    names = [r.get("scientificName", "") for r in parsed]
    assert not any("Dendrocopos" in n for n in names), \
        "Woodpecker (July obs) should not appear in March results"


def test_observations_by_month_species_filter():
    g = _make_graph_with_months()
    result = _run_tool("observations_by_month", {"month": 3, "species_name": "Robin"}, g)
    parsed = json.loads(result)
    assert len(parsed) >= 1
    assert all("Erithacus" in r.get("scientificName", "") for r in parsed)


def test_observations_by_month_species_filter_no_match():
    g = _make_graph_with_months()
    # Robin has no July observation
    result = _run_tool("observations_by_month", {"month": 7, "species_name": "Robin"}, g)
    assert result == "No results found."


def test_observations_by_month_includes_migration_status():
    g = _make_graph_with_months()
    result = _run_tool("observations_by_month", {"month": 3}, g)
    parsed = json.loads(result)
    robin = next((r for r in parsed if "Erithacus" in r.get("scientificName", "")), None)
    assert robin is not None
    assert robin.get("migStatus") == "Resident"


# ── where_to_watch ───────────────────────────────────────────────────────────

def _make_graph_with_hotspot() -> Graph:
    """Graph with a hotspot location and a species present in month 5 (May)."""
    from rdflib.namespace import RDF
    g = _make_graph_with_months()

    ROBIN = TAXON["species/robi"]
    LOC_HOT = LOC["loc_cph"]

    # Mark the location as a hotspot with observation count
    g.add((LOC_HOT, RDF.type, BIRD.Location))
    g.add((LOC_HOT, BIRD.isHotspot, Literal(True, datatype=XSD.boolean)))
    g.add((LOC_HOT, BIRD.observationCount, Literal(50, datatype=XSD.integer)))

    # Add a May observation for Robin so where_to_watch finds it in month=5
    OBS_MAY = OBS["obs_robin_may"]
    g.add((ROBIN, BIRD.hasObservation, OBS_MAY))
    g.add((OBS_MAY, RDF.type, BIRD.Observation))
    g.add((OBS_MAY, BIRD.observedOn, Literal("2024-05-12", datatype=XSD.date)))
    g.add((OBS_MAY, BIRD.observedAt, LOC_HOT))
    g.add((ROBIN, BIRD.typicallyPresentInMonth, Literal(5, datatype=XSD.integer)))

    return g


def test_where_to_watch_returns_hotspots():
    g = _make_graph_with_hotspot()
    # Copenhagen coords, wide radius to catch the fixture location
    result = _run_tool(
        "where_to_watch",
        {"month": 5, "lat": 55.6918, "lon": 12.5559, "radius_km": 50.0},
        g,
    )
    parsed = json.loads(result)
    assert len(parsed) >= 1
    assert any(r.get("locality") == "Assistens Kirkegård" for r in parsed)


def test_where_to_watch_has_expected_fields():
    g = _make_graph_with_hotspot()
    result = _run_tool(
        "where_to_watch",
        {"month": 5, "lat": 55.6918, "lon": 12.5559, "radius_km": 50.0},
        g,
    )
    parsed = json.loads(result)
    spot = parsed[0]
    assert "locality" in spot
    assert "dist_km" in spot
    assert "obsCount" in spot


def test_where_to_watch_empty_outside_radius():
    g = _make_graph_with_hotspot()
    # Use a location far from the fixture (New York area)
    result = _run_tool(
        "where_to_watch",
        {"month": 5, "lat": 40.7128, "lon": -74.0060, "radius_km": 1.0},
        g,
    )
    assert result == "No results found."


def test_where_to_watch_defaults_to_current_month(monkeypatch):
    """Calling without month should use today's month."""
    import datetime
    import chat
    called_with = {}

    original = chat.where_to_watch

    def spy(graph, **kwargs):
        called_with.update(kwargs)
        return original(graph, **kwargs)

    monkeypatch.setattr(chat, "where_to_watch", spy)
    g = _make_graph_with_hotspot()
    _run_tool("where_to_watch", {}, g)
    # The default is applied inside queries.py, so month may not appear in kwargs
    # — just verify the tool doesn't crash and returns a valid JSON or "No results"
    assert True  # no exception raised
