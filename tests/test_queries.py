"""Tests for SPARQL query functions — no live API or file I/O."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, XSD

from birdology.namespaces import BIRD, DWC, OBS, LOC, TAXON
from birdology.queries import (
    find_species_by_name,
    list_danish_species,
    nearby_watch,
    recent_danish_observations,
    species_by_family,
    species_by_order,
    taxonomy_summary,
)


# ── Minimal graph fixture ─────────────────────────────────────────────────────

def _make_graph() -> Graph:
    """
    Build a small but realistic graph with two species, one observation each.

    Species:
      - Erithacus rubecula (Robin / Rødhals / Rouge-gorge) — LC — Passeriformes / Muscicapidae
      - Dendrocopos major (Great Spotted Woodpecker) — LC — Piciformes / Picidae
    """
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)

    ROBIN_URI = TAXON["species/robi"]
    WOODP_URI = TAXON["species/euwoo1"]
    OBS_ROBIN = OBS["obs_robin"]
    OBS_WOODP = OBS["obs_woodp"]
    LOC_CPH   = LOC["loc_cph"]
    LOC_AARHUS = LOC["loc_aarhus"]

    GENUS_ERITHACUS  = TAXON["genus/Erithacus"]
    GENUS_DENDROCOPOS = TAXON["genus/Dendrocopos"]
    FAM_MUSC  = TAXON["family/Muscicapidae"]
    FAM_PIC   = TAXON["family/Picidae"]
    ORD_PASS  = TAXON["order/Passeriformes"]
    ORD_PIC   = TAXON["order/Piciformes"]

    # ── Taxonomy hierarchy ────────────────────────────────────────────────────
    for uri, cls in [
        (ORD_PASS, BIRD.Order), (ORD_PIC, BIRD.Order),
        (FAM_MUSC, BIRD.Family), (FAM_PIC, BIRD.Family),
        (GENUS_ERITHACUS, BIRD.Genus), (GENUS_DENDROCOPOS, BIRD.Genus),
        (ROBIN_URI, BIRD.Species), (WOODP_URI, BIRD.Species),
    ]:
        g.add((uri, RDF.type, cls))

    g.add((GENUS_ERITHACUS,   BIRD.parentTaxon, FAM_MUSC))
    g.add((GENUS_DENDROCOPOS, BIRD.parentTaxon, FAM_PIC))
    g.add((FAM_MUSC, BIRD.parentTaxon, ORD_PASS))
    g.add((FAM_PIC,  BIRD.parentTaxon, ORD_PIC))
    g.add((ROBIN_URI, BIRD.parentTaxon, GENUS_ERITHACUS))
    g.add((WOODP_URI, BIRD.parentTaxon, GENUS_DENDROCOPOS))

    # ── Species properties ────────────────────────────────────────────────────
    g.add((ROBIN_URI, DWC.scientificName, Literal("Erithacus rubecula")))
    g.add((ROBIN_URI, DWC.family,         Literal("Muscicapidae")))
    g.add((ROBIN_URI, DWC.order,          Literal("Passeriformes")))
    g.add((ROBIN_URI, BIRD.commonNameEn,  Literal("European Robin", lang="en")))
    g.add((ROBIN_URI, BIRD.commonNameDa,  Literal("Rødhals", lang="da")))
    g.add((ROBIN_URI, BIRD.commonNameFr,  Literal("Rouge-gorge familier", lang="fr")))
    g.add((ROBIN_URI, BIRD.eBirdCode,     Literal("robi")))
    g.add((ROBIN_URI, BIRD.conservationStatus, Literal("LC")))

    g.add((WOODP_URI, DWC.scientificName, Literal("Dendrocopos major")))
    g.add((WOODP_URI, DWC.family,         Literal("Picidae")))
    g.add((WOODP_URI, DWC.order,          Literal("Piciformes")))
    g.add((WOODP_URI, BIRD.commonNameEn,  Literal("Great Spotted Woodpecker", lang="en")))
    g.add((WOODP_URI, BIRD.commonNameDa,  Literal("Stor Flagspætte", lang="da")))
    g.add((WOODP_URI, BIRD.eBirdCode,     Literal("euwoo1")))
    g.add((WOODP_URI, BIRD.conservationStatus, Literal("LC")))

    # ── Observations ──────────────────────────────────────────────────────────
    # Robin near Assistens Kirkegård (lat=55.694, lon=12.554)
    g.add((OBS_ROBIN, RDF.type, BIRD.Observation))
    g.add((OBS_ROBIN, BIRD.observedOn,      Literal("2024-04-10", datatype=XSD.date)))
    g.add((OBS_ROBIN, BIRD.individualCount, Literal(2, datatype=XSD.integer)))
    g.add((OBS_ROBIN, BIRD.observedAt, LOC_CPH))
    g.add((ROBIN_URI, BIRD.hasObservation, OBS_ROBIN))

    g.add((LOC_CPH, RDF.type,       BIRD.Location))
    g.add((LOC_CPH, BIRD.latitude,  Literal(55.694, datatype=XSD.decimal)))
    g.add((LOC_CPH, BIRD.longitude, Literal(12.554, datatype=XSD.decimal)))
    g.add((LOC_CPH, BIRD.locality,  Literal("Nørrebro, Copenhagen")))

    # Woodpecker in Aarhus (far from Assistens)
    g.add((OBS_WOODP, RDF.type, BIRD.Observation))
    g.add((OBS_WOODP, BIRD.observedOn,      Literal("2024-03-20", datatype=XSD.date)))
    g.add((OBS_WOODP, BIRD.individualCount, Literal(1, datatype=XSD.integer)))
    g.add((OBS_WOODP, BIRD.observedAt, LOC_AARHUS))
    g.add((WOODP_URI, BIRD.hasObservation, OBS_WOODP))

    g.add((LOC_AARHUS, RDF.type,       BIRD.Location))
    g.add((LOC_AARHUS, BIRD.latitude,  Literal(56.163, datatype=XSD.decimal)))
    g.add((LOC_AARHUS, BIRD.longitude, Literal(10.204, datatype=XSD.decimal)))
    g.add((LOC_AARHUS, BIRD.locality,  Literal("Aarhus")))

    return g


# ── find_species_by_name ──────────────────────────────────────────────────────

def test_find_by_scientific_name():
    g = _make_graph()
    rows = find_species_by_name(g, "Erithacus")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_find_by_english_name():
    g = _make_graph()
    rows = find_species_by_name(g, "Robin")
    assert any(r["scientificName"] == "Erithacus rubecula" for r in rows)


def test_find_by_danish_name():
    g = _make_graph()
    rows = find_species_by_name(g, "Rødhals")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_find_by_french_name():
    g = _make_graph()
    rows = find_species_by_name(g, "Rouge-gorge")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_find_returns_empty_for_unknown():
    g = _make_graph()
    assert find_species_by_name(g, "Zyxopteryx phantastica") == []


# ── list_danish_species ───────────────────────────────────────────────────────

def test_list_danish_species_count():
    g = _make_graph()
    rows = list_danish_species(g)
    # Both species have observations in the fixture
    assert len(rows) == 2


def test_list_danish_species_has_names():
    g = _make_graph()
    rows = list_danish_species(g)
    sci_names = {r["scientificName"] for r in rows}
    assert "Erithacus rubecula" in sci_names
    assert "Dendrocopos major" in sci_names


# ── recent_danish_observations ────────────────────────────────────────────────

def test_recent_obs_no_filter_returns_all():
    g = _make_graph()
    rows = recent_danish_observations(g)
    assert len(rows) == 2


def test_recent_obs_filter_by_scientific_name():
    g = _make_graph()
    rows = recent_danish_observations(g, "Erithacus")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_recent_obs_filter_by_danish_name():
    g = _make_graph()
    rows = recent_danish_observations(g, "Rødhals")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_recent_obs_filter_by_french_name():
    g = _make_graph()
    rows = recent_danish_observations(g, "Rouge-gorge")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_recent_obs_filter_no_match():
    g = _make_graph()
    assert recent_danish_observations(g, "Flamingo") == []


def test_recent_obs_sorted_newest_first():
    g = _make_graph()
    rows = recent_danish_observations(g)
    dates = [r["date"] for r in rows]
    assert dates == sorted(dates, reverse=True)


# ── species_by_family / species_by_order ─────────────────────────────────────

def test_species_by_family():
    g = _make_graph()
    rows = species_by_family(g, "Muscicapidae")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_species_by_order():
    g = _make_graph()
    rows = species_by_order(g, "Piciformes")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Dendrocopos major"


def test_species_by_family_no_match():
    g = _make_graph()
    assert species_by_family(g, "Accipitridae") == []


# ── nearby_watch ─────────────────────────────────────────────────────────────

def test_nearby_watch_finds_robin_near_assistens():
    """Robin observation (55.694, 12.554) is ~0.3 km from Assistens (55.6918, 12.5559)."""
    g = _make_graph()
    rows = nearby_watch(g, lat=55.6918, lon=12.5559, radius_km=2.0)
    sci_names = {r["scientificName"] for r in rows}
    assert "Erithacus rubecula" in sci_names


def test_nearby_watch_excludes_distant_woodpecker():
    """Woodpecker in Aarhus (~170 km away) must not appear in a 2 km search."""
    g = _make_graph()
    rows = nearby_watch(g, lat=55.6918, lon=12.5559, radius_km=2.0)
    sci_names = {r["scientificName"] for r in rows}
    assert "Dendrocopos major" not in sci_names


def test_nearby_watch_wider_radius_still_excludes_aarhus():
    """Even 50 km radius from Copenhagen should not include Aarhus (~170 km)."""
    g = _make_graph()
    rows = nearby_watch(g, lat=55.6918, lon=12.5559, radius_km=50.0)
    sci_names = {r["scientificName"] for r in rows}
    assert "Dendrocopos major" not in sci_names


# ── taxonomy_summary ──────────────────────────────────────────────────────────

def test_taxonomy_summary_counts():
    g = _make_graph()
    summary = taxonomy_summary(g)
    assert summary["species"] == 2
    assert summary["observations"] == 2
    assert summary["orders"] == 2
    assert summary["families"] == 2
