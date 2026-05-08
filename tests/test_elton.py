"""Tests for the EltonTraits enrichment pipeline."""
from __future__ import annotations

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD

from birdology.ingestion.elton import (
    _build_sci_index,
    _pct,
    elton_to_rdf,
    enrich_graph,
    load_elton_traits,
)
from birdology.namespaces import BIRD, DWC, TAXON

# ── Fixtures ──────────────────────────────────────────────────────────────────

_SAMPLE_RECORDS = [
    {
        "Scientific": "Erithacus rubecula",
        "Diet.Inv": "95.0",
        "Diet.Vend": "2.0",
        "Diet.Vect": "0.0",
        "Diet.Vfish": "0.0",
        "Diet.Vunk": "0.0",
        "Diet.Scav": "0.0",
        "Diet.Fruit": "2.0",
        "Diet.Nect": "0.0",
        "Diet.Seed": "1.0",
        "Diet.PlantO": "0.0",
        "Diet.5Cat": "Invertebrate",
        "ForStrat.watbelowsurf": "0.0",
        "ForStrat.wataroundsurf": "0.0",
        "ForStrat.ground": "60.0",
        "ForStrat.understory": "20.0",
        "ForStrat.midhigh": "10.0",
        "ForStrat.canopy": "10.0",
        "ForStrat.aerial": "0.0",
        "Nocturnal": "0",
    },
    {
        "Scientific": "Larus argentatus",
        "Diet.Inv": "20.0",
        "Diet.Vend": "5.0",
        "Diet.Vect": "5.0",
        "Diet.Vfish": "40.0",
        "Diet.Vunk": "0.0",
        "Diet.Scav": "20.0",
        "Diet.Fruit": "5.0",
        "Diet.Nect": "0.0",
        "Diet.Seed": "5.0",
        "Diet.PlantO": "0.0",
        "Diet.5Cat": "VertFishScav",
        "ForStrat.watbelowsurf": "10.0",
        "ForStrat.wataroundsurf": "30.0",
        "ForStrat.ground": "30.0",
        "ForStrat.understory": "0.0",
        "ForStrat.midhigh": "0.0",
        "ForStrat.canopy": "0.0",
        "ForStrat.aerial": "30.0",
        "Nocturnal": "0",
    },
    {
        "Scientific": "Bubo bubo",
        "Diet.Inv": "5.0",
        "Diet.Vend": "70.0",
        "Diet.Vect": "10.0",
        "Diet.Vfish": "5.0",
        "Diet.Vunk": "5.0",
        "Diet.Scav": "5.0",
        "Diet.Fruit": "0.0",
        "Diet.Nect": "0.0",
        "Diet.Seed": "0.0",
        "Diet.PlantO": "0.0",
        "Diet.5Cat": "VertFishScav",
        "ForStrat.watbelowsurf": "0.0",
        "ForStrat.wataroundsurf": "0.0",
        "ForStrat.ground": "10.0",
        "ForStrat.understory": "10.0",
        "ForStrat.midhigh": "30.0",
        "ForStrat.canopy": "50.0",
        "ForStrat.aerial": "0.0",
        "Nocturnal": "1",
    },
]


def _make_graph_with_species(*sci_names: str) -> tuple[Graph, dict[str, URIRef]]:
    """Build a minimal graph containing species nodes and return (g, sci_index)."""
    g = Graph()
    index: dict[str, URIRef] = {}
    for name in sci_names:
        slug = name.replace(" ", "_").lower()
        uri = TAXON[f"species/{slug}"]
        g.add((uri, RDF.type, BIRD.Species))
        g.add((uri, DWC.scientificName, Literal(name)))
        index[name] = uri
    return g, index


# ── Unit tests ────────────────────────────────────────────────────────────────

class TestPct:
    def test_valid(self):
        assert _pct({"x": "42.5"}, "x") == 42.5

    def test_zero(self):
        assert _pct({"x": "0.0"}, "x") == 0.0

    def test_missing_key(self):
        assert _pct({}, "x") is None

    def test_empty_string(self):
        assert _pct({"x": ""}, "x") is None

    def test_non_numeric(self):
        assert _pct({"x": "NA"}, "x") is None


class TestBuildSciIndex:
    def test_index_keys(self):
        idx = _build_sci_index(_SAMPLE_RECORDS)
        assert "Erithacus rubecula" in idx
        assert "Larus argentatus" in idx

    def test_trinomial_truncated(self):
        recs = [{"Scientific": "Parus major major", "Diet.Inv": "50"}]
        idx = _build_sci_index(recs)
        assert "Parus major" in idx

    def test_empty_name_skipped(self):
        recs = [{"Scientific": "", "Diet.Inv": "50"}]
        idx = _build_sci_index(recs)
        assert idx == {}


class TestEltonToRdf:
    def test_matched_species_get_triples(self):
        _, sci_index = _make_graph_with_species("Erithacus rubecula")
        g, matched, unmatched = elton_to_rdf(_SAMPLE_RECORDS, sci_name_index=sci_index)
        assert matched == 1
        sp_uri = sci_index["Erithacus rubecula"]
        assert (sp_uri, BIRD.dietInvertPct, Literal(95.0, datatype=XSD.decimal)) in g
        assert (sp_uri, BIRD.dietCategory, Literal("Invertebrate")) in g

    def test_unmatched_species_counted(self):
        _, sci_index = _make_graph_with_species("Corvus cornix")  # not in sample
        _, matched, unmatched = elton_to_rdf(_SAMPLE_RECORDS, sci_name_index=sci_index)
        assert matched == 0
        assert unmatched == 1

    def test_vertebrate_columns_summed(self):
        _, sci_index = _make_graph_with_species("Larus argentatus")
        g, _, _ = elton_to_rdf(_SAMPLE_RECORDS, sci_name_index=sci_index)
        sp_uri = sci_index["Larus argentatus"]
        # 5 + 5 + 40 + 0 = 50
        assert (sp_uri, BIRD.dietVertPct, Literal(50.0, datatype=XSD.decimal)) in g

    def test_foraging_water_summed(self):
        _, sci_index = _make_graph_with_species("Larus argentatus")
        g, _, _ = elton_to_rdf(_SAMPLE_RECORDS, sci_name_index=sci_index)
        sp_uri = sci_index["Larus argentatus"]
        # below (10) + around (30) = 40
        assert (sp_uri, BIRD.foragingWater, Literal(40.0, datatype=XSD.decimal)) in g

    def test_foraging_canopy_summed(self):
        _, sci_index = _make_graph_with_species("Bubo bubo")
        g, _, _ = elton_to_rdf(_SAMPLE_RECORDS, sci_name_index=sci_index)
        sp_uri = sci_index["Bubo bubo"]
        # understory (10) + midhigh (30) + canopy (50) = 90
        assert (sp_uri, BIRD.foragingCanopy, Literal(90.0, datatype=XSD.decimal)) in g

    def test_nocturnal_true(self):
        _, sci_index = _make_graph_with_species("Bubo bubo")
        g, _, _ = elton_to_rdf(_SAMPLE_RECORDS, sci_name_index=sci_index)
        sp_uri = sci_index["Bubo bubo"]
        assert (sp_uri, BIRD.nocturnal, Literal(True, datatype=XSD.boolean)) in g

    def test_nocturnal_false(self):
        _, sci_index = _make_graph_with_species("Erithacus rubecula")
        g, _, _ = elton_to_rdf(_SAMPLE_RECORDS, sci_name_index=sci_index)
        sp_uri = sci_index["Erithacus rubecula"]
        assert (sp_uri, BIRD.nocturnal, Literal(False, datatype=XSD.boolean)) in g

    def test_empty_sci_name_index(self):
        g, matched, unmatched = elton_to_rdf(_SAMPLE_RECORDS, sci_name_index={})
        assert matched == 0
        assert len(g) == 0


class TestEnrichGraph:
    def test_triples_added_to_graph(self):
        g, _ = _make_graph_with_species("Erithacus rubecula", "Larus argentatus")
        before = len(g)
        matched = enrich_graph(g, _SAMPLE_RECORDS)
        assert matched == 2
        assert len(g) > before

    def test_idempotent(self):
        """Running enrichment twice should not duplicate triples."""
        g, _ = _make_graph_with_species("Erithacus rubecula")
        enrich_graph(g, _SAMPLE_RECORDS)
        after_first = len(g)
        enrich_graph(g, _SAMPLE_RECORDS)
        assert len(g) == after_first
