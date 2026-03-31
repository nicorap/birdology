"""Tests for ingestion modules — no live API calls."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib.namespace import OWL, RDF

from birdology.ingestion.ebird import add_danish_names, taxonomy_to_rdf
from birdology.ingestion.gbif_dof import occurrences_to_rdf
from birdology.namespaces import BIRD, DWC


# ── Fixtures ──────────────────────────────────────────────────────────────────

EBIRD_RECORDS = [
    {
        "speciesCode": "robi",
        "category": "species",
        "sciName": "Erithacus rubecula",
        "comName": "European Robin",
        "order": "Passeriformes",
        "familySciName": "Muscicapidae",
        "familyComName": "Old World Flycatchers",
        "taxonOrder": 32500.0,
    },
    {
        "speciesCode": "euwoo1",
        "category": "species",
        "sciName": "Dendrocopos major",
        "comName": "Great Spotted Woodpecker",
        "order": "Piciformes",
        "familySciName": "Picidae",
        "familyComName": "Woodpeckers",
        "taxonOrder": 28000.0,
    },
    # Non-species records should be skipped for Species nodes
    {
        "speciesCode": "",
        "category": "spuh",
        "sciName": "Erithacus sp.",
        "comName": "robin sp.",
        "order": "Passeriformes",
        "familySciName": "Muscicapidae",
        "familyComName": "Old World Flycatchers",
        "taxonOrder": 32501.0,
    },
]

DANISH_RECORDS = [
    {
        "speciesCode": "robi",
        "category": "species",
        "comName": "Rødhals",
    }
]

GBIF_OCCURRENCES = [
    {
        "key": 12345,
        "scientificName": "Erithacus rubecula",
        "eventDate": "2024-04-15",
        "individualCount": 3,
        "decimalLatitude": 55.6761,
        "decimalLongitude": 12.5683,
        "locality": "Copenhagen",
        "recordedBy": "Lars Hansen",
        "speciesKey": 9876543,
    },
    {
        "key": 12346,
        "scientificName": "Dendrocopos major",
        "eventDate": "2024-03-10",
        "decimalLatitude": 56.1629,
        "decimalLongitude": 10.2039,
        "speciesKey": 1234567,
    },
]


# ── eBird ingestion tests ─────────────────────────────────────────────────────

def test_taxonomy_to_rdf_species_count():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    species = list(g.subjects(RDF.type, BIRD.Species))
    assert len(species) == 2  # spuh record should be skipped


def test_taxonomy_to_rdf_scientific_name():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    sci_names = set(str(o) for o in g.objects(None, DWC.scientificName))
    assert "Erithacus rubecula" in sci_names


def test_taxonomy_to_rdf_creates_order_and_family():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    orders = list(g.subjects(RDF.type, BIRD.Order))
    families = list(g.subjects(RDF.type, BIRD.Family))
    assert len(orders) == 2  # Passeriformes + Piciformes
    assert len(families) == 2  # Muscicapidae + Picidae


def test_taxonomy_to_rdf_ebird_code():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    codes = set(str(o) for o in g.objects(None, BIRD.eBirdCode))
    assert "robi" in codes


def test_taxonomy_to_rdf_owl_sameAs():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    same_as = list(g.subject_objects(OWL.sameAs))
    assert len(same_as) == 2


def test_add_danish_names():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    add_danish_names(g, DANISH_RECORDS)
    da_names = set(str(o) for o in g.objects(None, BIRD.commonNameDa))
    assert "Rødhals" in da_names


# ── GBIF/DOF ingestion tests ──────────────────────────────────────────────────

def test_occurrences_to_rdf_observation_count():
    g = occurrences_to_rdf(GBIF_OCCURRENCES)
    obs = list(g.subjects(RDF.type, BIRD.Observation))
    assert len(obs) == 2


def test_occurrences_to_rdf_location_created():
    g = occurrences_to_rdf(GBIF_OCCURRENCES)
    locs = list(g.subjects(RDF.type, BIRD.Location))
    assert len(locs) == 2


def test_occurrences_to_rdf_owl_sameAs_to_gbif():
    g = occurrences_to_rdf(GBIF_OCCURRENCES)
    same_as = list(g.subject_objects(OWL.sameAs))
    assert len(same_as) == 2


def test_occurrences_to_rdf_species_linked():
    g = occurrences_to_rdf(GBIF_OCCURRENCES)
    obs_links = list(g.subject_objects(BIRD.hasObservation))
    assert len(obs_links) == 2


def test_occurrences_use_ebird_species_when_index_provided():
    """DOF observations should link to the eBird species node, not a fallback URI."""
    ebird_g = taxonomy_to_rdf(EBIRD_RECORDS)
    from birdology.ingestion.ebird import _species_uri
    ebird_robin_uri = _species_uri("robi")

    from rdflib import URIRef
    sci_index = {"Erithacus rubecula": ebird_robin_uri}

    dof_g = occurrences_to_rdf(GBIF_OCCURRENCES, sci_name_index=sci_index)
    # The robin observation should link back to the eBird species node
    subjects = set(dof_g.subjects(BIRD.hasObservation, None))
    assert ebird_robin_uri in subjects
