"""Tests for the Wikidata enrichment module — no network required."""
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, XSD

from birdology.namespaces import BIRD, DWC, TAXON
from birdology.ingestion.wikidata import enrich_graph, fetch_wikidata_traits, _val


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_graph() -> Graph:
    """Small graph with two species."""
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)

    sp1 = TAXON["species/eurrob1"]
    g.add((sp1, RDF.type, BIRD.Species))
    g.add((sp1, DWC.scientificName, Literal("Erithacus rubecula")))
    g.add((sp1, BIRD.commonNameEn, Literal("European Robin")))

    sp2 = TAXON["species/eurbla"]
    g.add((sp2, RDF.type, BIRD.Species))
    g.add((sp2, DWC.scientificName, Literal("Turdus merula")))
    g.add((sp2, BIRD.commonNameEn, Literal("Eurasian Blackbird")))

    return g


FAKE_WIKIDATA_RESPONSE = {
    "results": {
        "bindings": [
            {
                "name": {"type": "literal", "value": "Erithacus rubecula"},
                "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q25334"},
                "itemLabel": {"type": "literal", "value": "European robin"},
                "wingspan": {"type": "literal", "value": "21.0", "datatype": "http://www.w3.org/2001/XMLSchema#decimal"},
                "diel": {"type": "uri", "value": "http://www.wikidata.org/entity/Q2095477"},
                "dielLabel": {"type": "literal", "value": "diurnality"},
                "gbifID": {"type": "literal", "value": "2492462"},
                "ebirdID": {"type": "literal", "value": "eurrob1"},
                "range": {"type": "uri", "value": "http://www.wikidata.org/entity/Q15"},
                "rangeLabel": {"type": "literal", "value": "Africa"},
            },
            {
                "name": {"type": "literal", "value": "Turdus merula"},
                "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q25234"},
                "itemLabel": {"type": "literal", "value": "common blackbird"},
                "wingspan": {"type": "literal", "value": "36.0", "datatype": "http://www.w3.org/2001/XMLSchema#decimal"},
                "mass": {"type": "literal", "value": "95.0", "datatype": "http://www.w3.org/2001/XMLSchema#decimal"},
                "diel": {"type": "uri", "value": "http://www.wikidata.org/entity/Q2095477"},
                "dielLabel": {"type": "literal", "value": "diurnality"},
                "gbifID": {"type": "literal", "value": "2490719"},
                "ebirdID": {"type": "literal", "value": "eurbla"},
                "habitatLabel": {"type": "literal", "value": "forest"},
                "habitat": {"type": "uri", "value": "http://www.wikidata.org/entity/Q4421"},
            },
        ]
    }
}


# ── Tests ────────────────────────────────────────────────────────────────────


class TestValHelper:
    def test_val_present(self):
        binding = {"name": {"type": "literal", "value": "hello"}}
        assert _val(binding, "name") == "hello"

    def test_val_missing(self):
        assert _val({}, "name") is None

    def test_val_none_value(self):
        assert _val({"x": None}, "x") is None


class TestFetchWikidataTraits:
    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_basic_fetch(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]

        traits = fetch_wikidata_traits(["Erithacus rubecula", "Turdus merula"])

        assert len(traits) == 2
        assert "Erithacus rubecula" in traits
        assert "Turdus merula" in traits

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_wingspan_parsed(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Erithacus rubecula"])

        assert traits["Erithacus rubecula"]["wingspan_mm"] == 21.0

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_mass_parsed(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Turdus merula"])

        assert traits["Turdus merula"]["mass_g"] == 95.0

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_diel_cycle(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Erithacus rubecula"])

        assert traits["Erithacus rubecula"]["diel"] == "diurnality"

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_habitat(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Turdus merula"])

        assert "forest" in traits["Turdus merula"]["habitats"]

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_ranges(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Erithacus rubecula"])

        assert "Africa" in traits["Erithacus rubecula"]["ranges"]

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_wikidata_id(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Erithacus rubecula"])

        assert traits["Erithacus rubecula"]["wikidata_id"] == "Q25334"

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_cross_links(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Erithacus rubecula"])

        assert traits["Erithacus rubecula"]["gbif_id"] == "2492462"
        assert traits["Erithacus rubecula"]["ebird_id"] == "eurrob1"

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_empty_result(self, mock_query):
        mock_query.return_value = []
        traits = fetch_wikidata_traits(["Nonexistent species"])

        assert len(traits) == 0

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_api_error_skips_batch(self, mock_query):
        mock_query.side_effect = Exception("503 Service Unavailable")
        traits = fetch_wikidata_traits(["Erithacus rubecula"])

        assert len(traits) == 0  # graceful degradation


class TestEnrichGraph:
    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_triples_added(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Erithacus rubecula", "Turdus merula"])

        g = _make_graph()
        initial_count = len(g)
        added = enrich_graph(g, traits)

        assert added > 0
        assert len(g) > initial_count

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_wingspan_triple(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Erithacus rubecula"])

        g = _make_graph()
        enrich_graph(g, traits)

        sp = TAXON["species/eurrob1"]
        wingspans = list(g.objects(sp, BIRD.wingspanMm))
        assert len(wingspans) == 1
        assert float(wingspans[0]) == 21.0

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_mass_triple(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Turdus merula"])

        g = _make_graph()
        enrich_graph(g, traits)

        sp = TAXON["species/eurbla"]
        masses = list(g.objects(sp, BIRD.massGrams))
        assert len(masses) == 1
        assert float(masses[0]) == 95.0

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_habitat_triple(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Turdus merula"])

        g = _make_graph()
        enrich_graph(g, traits)

        sp = TAXON["species/eurbla"]
        habitats = [str(h) for h in g.objects(sp, BIRD.habitat)]
        assert "forest" in habitats

    @patch("birdology.ingestion.wikidata._query_wikidata")
    @patch("birdology.ingestion.wikidata._DELAY", 0)
    def test_owl_sameas_wikidata(self, mock_query):
        mock_query.return_value = FAKE_WIKIDATA_RESPONSE["results"]["bindings"]
        traits = fetch_wikidata_traits(["Erithacus rubecula"])

        g = _make_graph()
        enrich_graph(g, traits)

        sp = TAXON["species/eurrob1"]
        sameas = [str(u) for u in g.objects(sp, OWL.sameAs)]
        assert "http://www.wikidata.org/entity/Q25334" in sameas

    def test_unknown_species_skipped(self):
        """Species not in the graph should not add triples."""
        traits = {
            "Unknown bird": {
                "label": "Unknown",
                "habitats": ["forest"],
                "iucn_status": None,
                "ranges": ["Europe"],
                "diel": "diurnality",
                "mass_g": 50.0,
                "wingspan_mm": 30.0,
                "gbif_id": "12345",
                "ebird_id": "unkn1",
                "wikidata_id": "Q99999",
            }
        }

        g = _make_graph()
        initial_count = len(g)
        added = enrich_graph(g, traits)

        assert added == 0
        assert len(g) == initial_count
