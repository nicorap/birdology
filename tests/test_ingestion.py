"""Tests for ingestion modules — no live API calls."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib.namespace import OWL, RDF, XSD

from birdology.ingestion.ebird import (
    add_danish_names,
    add_localized_names,
    ebird_observations_to_rdf,
    fetch_recent_denmark,
    fetch_taxonomy,
    taxonomy_to_rdf,
)
from birdology.ingestion.gbif_dof import occurrences_to_rdf
from birdology.namespaces import BIRD, DWC, OBS


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

EBIRD_OBS_RECORDS = [
    {
        "speciesCode": "robi",
        "sciName": "Erithacus rubecula",
        "comName": "European Robin",
        "obsDt": "2026-05-09 08:30",
        "howMany": 2,
        "subId": "S123",
        "lat": 55.6200,
        "lng": 12.4386,
        "locName": "Brøndby Strand",
    },
    {
        "speciesCode": "euwoo1",
        "sciName": "Dendrocopos major",
        "comName": "Great Spotted Woodpecker",
        "obsDt": "2026-05-09",
        "howMany": None,
        "subId": "S124",
        "lat": 55.71,
        "lng": 12.51,
        "locName": "Utterslev Mose",
    },
    # Record missing required fields — should be skipped
    {
        "speciesCode": "",
        "sciName": "",
        "obsDt": "2026-05-09",
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


# ── fetch_taxonomy / fetch_recent_denmark (mocked HTTP) ──────────────────────

def test_fetch_taxonomy_calls_correct_endpoint(monkeypatch):
    mock_resp = MagicMock()
    mock_resp.json.return_value = EBIRD_RECORDS
    mock_resp.raise_for_status = MagicMock()
    with patch("birdology.ingestion.ebird.requests.get", return_value=mock_resp) as mock_get:
        result = fetch_taxonomy("fake_key", locale="en")
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert "taxonomy/ebird" in args[0]
    assert kwargs["headers"]["x-ebirdapitoken"] == "fake_key"
    assert result == EBIRD_RECORDS


def test_fetch_taxonomy_raises_on_http_error(monkeypatch):
    import requests as _req
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = _req.HTTPError("401")
    with patch("birdology.ingestion.ebird.requests.get", return_value=mock_resp):
        try:
            fetch_taxonomy("bad_key")
            assert False, "Should have raised"
        except _req.HTTPError:
            pass


def test_fetch_recent_denmark_calls_dk_endpoint(monkeypatch):
    mock_resp = MagicMock()
    mock_resp.json.return_value = []
    mock_resp.raise_for_status = MagicMock()
    with patch("birdology.ingestion.ebird.requests.get", return_value=mock_resp) as mock_get:
        fetch_recent_denmark("fake_key", days=7)
    args, _ = mock_get.call_args
    assert "/DK/recent" in args[0]


def test_fetch_recent_denmark_caps_days_at_30(monkeypatch):
    mock_resp = MagicMock()
    mock_resp.json.return_value = []
    mock_resp.raise_for_status = MagicMock()
    with patch("birdology.ingestion.ebird.requests.get", return_value=mock_resp) as mock_get:
        fetch_recent_denmark("fake_key", days=60)
    _, kwargs = mock_get.call_args
    assert kwargs["params"]["back"] == 30


# ── ebird_observations_to_rdf ─────────────────────────────────────────────────

def test_ebird_obs_to_rdf_observation_count():
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS)
    obs = list(g.subjects(RDF.type, BIRD.Observation))
    assert len(obs) == 2  # third record (empty fields) skipped


def test_ebird_obs_to_rdf_date_parsed():
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS)
    dates = set(str(o) for o in g.objects(None, BIRD.observedOn))
    assert "2026-05-09" in dates


def test_ebird_obs_to_rdf_individual_count():
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS)
    counts = [int(o) for o in g.objects(None, BIRD.individualCount)]
    assert 2 in counts


def test_ebird_obs_to_rdf_location_created():
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS)
    locs = list(g.subjects(RDF.type, BIRD.Location))
    assert len(locs) == 2


def test_ebird_obs_to_rdf_locality_name():
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS)
    localities = set(str(o) for o in g.objects(None, BIRD.locality))
    assert "Brøndby Strand" in localities


def test_ebird_obs_to_rdf_species_linked():
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS)
    links = list(g.subject_objects(BIRD.hasObservation))
    assert len(links) == 2


def test_ebird_obs_to_rdf_ebird_code_on_obs():
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS)
    codes = set(str(o) for o in g.objects(None, BIRD.eBirdCode))
    assert "robi" in codes


def test_ebird_obs_to_rdf_sci_name_index_overrides_uri():
    """When sci_name_index provided, observation links to the indexed species URI."""
    from rdflib import URIRef
    custom_uri = URIRef("https://birdology.org/taxon/species/robi")
    index = {"Erithacus rubecula": custom_uri}
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS, sci_name_index=index)
    subjects = set(g.subjects(BIRD.hasObservation, None))
    assert custom_uri in subjects


def test_ebird_obs_to_rdf_missing_howmany_ok():
    """Records with howMany=None should not crash and skip individualCount."""
    g = ebird_observations_to_rdf(EBIRD_OBS_RECORDS)
    # Woodpecker has howMany=None — it should still create an observation
    obs_codes = set(str(o) for o in g.objects(None, BIRD.eBirdCode))
    assert "euwoo1" in obs_codes


# ── add_localized_names ───────────────────────────────────────────────────────

def test_add_localized_names_french():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    fr_records = [{"speciesCode": "robi", "category": "species", "comName": "Rouge-gorge familier"}]
    add_localized_names(g, fr_records, "fr")
    fr_names = set(str(o) for o in g.objects(None, BIRD.commonNameFr))
    assert "Rouge-gorge familier" in fr_names


def test_add_localized_names_unsupported_locale():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    try:
        add_localized_names(g, [], "zh")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_add_localized_names_skips_non_species():
    g = taxonomy_to_rdf(EBIRD_RECORDS)
    non_species = [{"speciesCode": "robi", "category": "spuh", "comName": "should-be-skipped"}]
    add_localized_names(g, non_species, "fr")
    fr_names = list(g.objects(None, BIRD.commonNameFr))
    assert all("should-be-skipped" not in str(n) for n in fr_names)


# ── cross-source linking ──────────────────────────────────────────────────────

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
