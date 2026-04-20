"""Tests for the IUCN Red List enrichment module — no network required."""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD

from birdology.namespaces import BIRD, DWC, TAXON
from birdology.ingestion.iucn import (
    enrich_graph,
    fetch_iucn_data,
    fetch_species_assessment,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _make_graph() -> Graph:
    """Small graph with two species."""
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)

    sp1 = TAXON["species/eurrob1"]
    g.add((sp1, RDF.type, BIRD.Species))
    g.add((sp1, DWC.scientificName, Literal("Erithacus rubecula")))

    sp2 = TAXON["species/eurbla"]
    g.add((sp2, RDF.type, BIRD.Species))
    g.add((sp2, DWC.scientificName, Literal("Turdus merula")))

    return g


FAKE_LOOKUP_RESPONSE = {
    "taxon": {
        "sis_id": 22709675,
        "scientific_name": "Erithacus rubecula",
        "genus_name": "Erithacus",
        "species_name": "rubecula",
    },
    "assessments": [
        {
            "year_published": "2021",
            "latest": True,
            "assessment_id": 166290100,
        },
        {
            "year_published": "2016",
            "latest": False,
            "assessment_id": 100000001,
        },
    ],
}

FAKE_ASSESSMENT_RESPONSE = {
    "assessment_id": 166290100,
    "red_list_category": {"code": "LC", "description": {"en": "Least Concern"}},
    "population_trend": {"code": "1", "description": {"en": "Stable"}},
    "supplementary_info": {
        "movement_patterns": "Full Migrant",
        "population_size": "130000000-300000000",
    },
    "habitats": [
        {
            "description": {"en": "Forest - Boreal"},
            "suitability": "Suitable",
            "season": "Breeding Season",
            "majorImportance": "Yes",
        },
        {
            "description": {"en": "Shrubland - Mediterranean-type Shrubby Vegetation"},
            "suitability": "Suitable",
            "season": "Non-Breeding Season",
            "majorImportance": "No",
        },
    ],
    "threats": [
        {
            "description": {"en": "Hunting & trapping terrestrial animals"},
            "scope": "Minority (<50%)",
            "severity": "Unknown",
            "timing": "Ongoing",
        },
    ],
    "systems": [
        {"description": {"en": "Terrestrial"}, "code": "0"},
    ],
    "conservation_actions": [],
}


# ── Tests: fetch_species_assessment ──────────────────────────────────────────


class TestFetchSpeciesAssessment:
    @patch("birdology.ingestion.iucn._get")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_basic_fetch(self, mock_get):
        mock_get.side_effect = [FAKE_LOOKUP_RESPONSE, FAKE_ASSESSMENT_RESPONSE]

        result = fetch_species_assessment("Erithacus rubecula", "fake_token")

        assert result is not None
        assert result["category"] == "LC"
        assert result["population_trend"] == "Stable"

    @patch("birdology.ingestion.iucn._get")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_habitats_parsed(self, mock_get):
        mock_get.side_effect = [FAKE_LOOKUP_RESPONSE, FAKE_ASSESSMENT_RESPONSE]

        result = fetch_species_assessment("Erithacus rubecula", "fake_token")

        assert len(result["habitats"]) == 2
        assert result["habitats"][0]["name"] == "Forest - Boreal"
        assert result["habitats"][0]["major"] is True
        assert result["habitats"][1]["major"] is False

    @patch("birdology.ingestion.iucn._get")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_threats_parsed(self, mock_get):
        mock_get.side_effect = [FAKE_LOOKUP_RESPONSE, FAKE_ASSESSMENT_RESPONSE]

        result = fetch_species_assessment("Erithacus rubecula", "fake_token")

        assert len(result["threats"]) == 1
        assert "Hunting" in result["threats"][0]["name"]
        assert result["threats"][0]["timing"] == "Ongoing"

    @patch("birdology.ingestion.iucn._get")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_movement_pattern(self, mock_get):
        mock_get.side_effect = [FAKE_LOOKUP_RESPONSE, FAKE_ASSESSMENT_RESPONSE]

        result = fetch_species_assessment("Erithacus rubecula", "fake_token")

        assert result["movement"] == "Full Migrant"

    @patch("birdology.ingestion.iucn._get")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_systems_parsed(self, mock_get):
        mock_get.side_effect = [FAKE_LOOKUP_RESPONSE, FAKE_ASSESSMENT_RESPONSE]

        result = fetch_species_assessment("Erithacus rubecula", "fake_token")

        assert "Terrestrial" in result["systems"]

    @patch("birdology.ingestion.iucn._get")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_species_not_found(self, mock_get):
        mock_get.return_value = None

        result = fetch_species_assessment("Nonexistent bird", "fake_token")

        assert result is None

    @patch("birdology.ingestion.iucn._get")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_no_assessments(self, mock_get):
        mock_get.return_value = {"taxon": {}, "assessments": []}

        result = fetch_species_assessment("Erithacus rubecula", "fake_token")

        assert result is None

    def test_single_word_name_rejected(self):
        result = fetch_species_assessment("Erithacus", "fake_token")
        assert result is None

    @patch("birdology.ingestion.iucn._get")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_picks_latest_assessment(self, mock_get):
        """Should use the assessment marked as latest=True."""
        mock_get.side_effect = [FAKE_LOOKUP_RESPONSE, FAKE_ASSESSMENT_RESPONSE]

        fetch_species_assessment("Erithacus rubecula", "fake_token")

        # Second call should be for the latest assessment ID (166290100)
        calls = mock_get.call_args_list
        assert calls[1][0][0] == "/assessment/166290100"


# ── Tests: fetch_iucn_data ───────────────────────────────────────────────────


class TestFetchIucnData:
    @patch("birdology.ingestion.iucn.fetch_species_assessment")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_batch_fetch(self, mock_fetch):
        mock_fetch.side_effect = [
            {"category": "LC", "population_trend": "Stable", "habitats": [],
             "threats": [], "movement": None, "systems": []},
            None,  # second species not found
        ]

        results = fetch_iucn_data(["Erithacus rubecula", "Unknown bird"], "token")

        assert len(results) == 1
        assert "Erithacus rubecula" in results

    @patch("birdology.ingestion.iucn.fetch_species_assessment")
    @patch("birdology.ingestion.iucn._DELAY", 0)
    def test_empty_list(self, mock_fetch):
        results = fetch_iucn_data([], "token")
        assert len(results) == 0
        mock_fetch.assert_not_called()


# ── Tests: enrich_graph ──────────────────────────────────────────────────────


class TestEnrichGraph:
    def _sample_data(self):
        return {
            "Erithacus rubecula": {
                "category": "LC",
                "population_trend": "Stable",
                "habitats": [
                    {"name": "Forest - Boreal", "suitability": "Suitable",
                     "season": "Breeding Season", "major": True},
                ],
                "threats": [
                    {"name": "Hunting & trapping", "scope": "Minority",
                     "severity": "Unknown", "timing": "Ongoing"},
                ],
                "movement": "Full Migrant",
                "systems": ["Terrestrial"],
            },
        }

    def test_triples_added(self):
        g = _make_graph()
        initial = len(g)
        added = enrich_graph(g, self._sample_data())

        assert added > 0
        assert len(g) > initial

    def test_category_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        cats = [str(c) for c in g.objects(sp, BIRD.iucnCategory)]
        assert "LC" in cats

    def test_population_trend_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        trends = [str(t) for t in g.objects(sp, BIRD.populationTrend)]
        assert "Stable" in trends

    def test_habitat_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        habitats = [str(h) for h in g.objects(sp, BIRD.iucnHabitat)]
        assert "Forest - Boreal" in habitats

    def test_threat_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        threats = [str(t) for t in g.objects(sp, BIRD.threat)]
        assert any("Hunting" in t for t in threats)

    def test_movement_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        mvts = [str(m) for m in g.objects(sp, BIRD.movementPattern)]
        assert "Full Migrant" in mvts

    def test_system_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        systems = [str(s) for s in g.objects(sp, BIRD.system)]
        assert "Terrestrial" in systems

    def test_unknown_species_skipped(self):
        g = _make_graph()
        data = {
            "Unknown species": {
                "category": "EN", "population_trend": "Decreasing",
                "habitats": [], "threats": [], "movement": None, "systems": [],
            }
        }
        initial = len(g)
        added = enrich_graph(g, data)

        assert added == 0
        assert len(g) == initial

    def test_count_accuracy(self):
        """Added count should match actual new triples."""
        g = _make_graph()
        initial = len(g)
        added = enrich_graph(g, self._sample_data())

        assert len(g) - initial == added

    def test_no_data_no_triples(self):
        """Empty data dict should not add anything."""
        g = _make_graph()
        initial = len(g)
        added = enrich_graph(g, {})

        assert added == 0
        assert len(g) == initial
