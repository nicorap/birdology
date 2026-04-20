"""Tests for the DBpedia enrichment module — no network required."""
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF

from birdology.namespaces import BIRD, DWC, TAXON
from birdology.ingestion.dbpedia import (
    _name_to_dbpedia_slug,
    enrich_graph,
    fetch_dbpedia_data,
)


def _make_graph() -> Graph:
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)

    sp1 = TAXON["species/eurrob1"]
    g.add((sp1, RDF.type, BIRD.Species))
    g.add((sp1, DWC.scientificName, Literal("Erithacus rubecula")))
    g.add((sp1, BIRD.commonNameEn, Literal("European Robin")))

    sp2 = TAXON["species/mallar3"]
    g.add((sp2, RDF.type, BIRD.Species))
    g.add((sp2, DWC.scientificName, Literal("Anas platyrhynchos")))
    g.add((sp2, BIRD.commonNameEn, Literal("Mallard")))

    return g


FAKE_BINDINGS = [
    {
        "item": {"type": "uri", "value": "http://dbpedia.org/resource/European_robin"},
        "thumb": {"type": "uri", "value": "http://commons.wikimedia.org/wiki/Special:FilePath/Erithacus_rubecula.jpg?width=300"},
        "rangeMap": {"type": "literal", "value": "ErithacusRubeculaIUCN.svg"},
    },
    {
        "item": {"type": "uri", "value": "http://dbpedia.org/resource/Mallard"},
        "thumb": {"type": "uri", "value": "http://commons.wikimedia.org/wiki/Special:FilePath/Mallard.jpg?width=300"},
    },
]


class TestNameToSlug:
    def test_two_words(self):
        assert _name_to_dbpedia_slug("European Robin") == "European_robin"

    def test_single_word(self):
        assert _name_to_dbpedia_slug("Mallard") == "Mallard"

    def test_three_words(self):
        assert _name_to_dbpedia_slug("White-tailed Eagle") == "White-tailed_eagle"

    def test_hyphenated(self):
        assert _name_to_dbpedia_slug("Black-headed Gull") == "Black-headed_gull"

    def test_empty(self):
        assert _name_to_dbpedia_slug("") == ""

    def test_preserves_first_word_case(self):
        assert _name_to_dbpedia_slug("Great Crested Grebe") == "Great_crested_grebe"


class TestFetchDbpediaData:
    @patch("birdology.ingestion.dbpedia._query_dbpedia")
    @patch("birdology.ingestion.dbpedia._DELAY", 0)
    def test_basic_fetch(self, mock_query):
        mock_query.return_value = FAKE_BINDINGS

        pairs = [
            ("Erithacus rubecula", "European Robin"),
            ("Anas platyrhynchos", "Mallard"),
        ]
        results = fetch_dbpedia_data(pairs)

        assert len(results) == 2
        assert "Erithacus rubecula" in results
        assert "Anas platyrhynchos" in results

    @patch("birdology.ingestion.dbpedia._query_dbpedia")
    @patch("birdology.ingestion.dbpedia._DELAY", 0)
    def test_thumbnail_extracted(self, mock_query):
        mock_query.return_value = FAKE_BINDINGS

        results = fetch_dbpedia_data([("Erithacus rubecula", "European Robin")])

        assert "Erithacus_rubecula.jpg" in results["Erithacus rubecula"]["thumbnail"]

    @patch("birdology.ingestion.dbpedia._query_dbpedia")
    @patch("birdology.ingestion.dbpedia._DELAY", 0)
    def test_range_map_converted_to_url(self, mock_query):
        mock_query.return_value = FAKE_BINDINGS

        results = fetch_dbpedia_data([("Erithacus rubecula", "European Robin")])

        rmap = results["Erithacus rubecula"]["range_map"]
        assert rmap.startswith("http://commons.wikimedia.org/")
        assert "ErithacusRubeculaIUCN.svg" in rmap

    @patch("birdology.ingestion.dbpedia._query_dbpedia")
    @patch("birdology.ingestion.dbpedia._DELAY", 0)
    def test_dbpedia_uri(self, mock_query):
        mock_query.return_value = FAKE_BINDINGS

        results = fetch_dbpedia_data([("Erithacus rubecula", "European Robin")])

        assert results["Erithacus rubecula"]["dbpedia_uri"] == "http://dbpedia.org/resource/European_robin"

    @patch("birdology.ingestion.dbpedia._query_dbpedia")
    @patch("birdology.ingestion.dbpedia._DELAY", 0)
    def test_api_error_skips_batch(self, mock_query):
        mock_query.side_effect = Exception("503")

        results = fetch_dbpedia_data([("Erithacus rubecula", "European Robin")])

        assert len(results) == 0

    @patch("birdology.ingestion.dbpedia._query_dbpedia")
    @patch("birdology.ingestion.dbpedia._DELAY", 0)
    def test_empty_input(self, mock_query):
        results = fetch_dbpedia_data([])
        assert len(results) == 0
        mock_query.assert_not_called()


class TestEnrichGraph:
    def _sample_data(self):
        return {
            "Erithacus rubecula": {
                "thumbnail": "http://commons.wikimedia.org/wiki/Special:FilePath/Robin.jpg?width=300",
                "range_map": "http://commons.wikimedia.org/wiki/Special:FilePath/RobinRange.svg",
                "dbpedia_uri": "http://dbpedia.org/resource/European_robin",
            },
        }

    def test_triples_added(self):
        g = _make_graph()
        initial = len(g)
        added = enrich_graph(g, self._sample_data())

        assert added == 3  # thumbnail + range_map + sameAs
        assert len(g) == initial + 3

    def test_thumbnail_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        thumbs = [str(t) for t in g.objects(sp, BIRD.thumbnailUrl)]
        assert any("Robin.jpg" in t for t in thumbs)

    def test_range_map_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        maps = [str(m) for m in g.objects(sp, BIRD.rangeMapUrl)]
        assert any("RobinRange.svg" in m for m in maps)

    def test_sameas_triple(self):
        g = _make_graph()
        enrich_graph(g, self._sample_data())

        sp = TAXON["species/eurrob1"]
        sameas = [str(u) for u in g.objects(sp, OWL.sameAs)]
        assert "http://dbpedia.org/resource/European_robin" in sameas

    def test_unknown_species_skipped(self):
        g = _make_graph()
        data = {"Unknown bird": {
            "thumbnail": "http://x.com/img.jpg",
            "range_map": None,
            "dbpedia_uri": "http://dbpedia.org/resource/X",
        }}
        added = enrich_graph(g, data)
        assert added == 0
