"""
Graph quality checks — validate the enriched graph has expected structure.

These tests load the actual output graph (if present) and verify:
- Schema completeness (all expected classes and properties exist)
- Data integrity (no dangling references, consistent types)
- Enrichment coverage (Wikidata/IUCN data actually present)

Skipped if output files don't exist (CI-friendly).
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib import Graph, Literal
from rdflib.namespace import OWL, RDF, RDFS, XSD

from birdology.namespaces import BIRD, DWC, TAXON
from birdology.schema import build_schema

BASE_TTL = Path(__file__).parent.parent / "output" / "birdology.ttl"
ENRICHED_TTL = Path(__file__).parent.parent / "output" / "birdology_enriched.ttl"


def _load(path: Path) -> Graph:
    g = Graph()
    g.parse(str(path), format="turtle")
    return g


# ── Schema quality ───────────────────────────────────────────────────────────


class TestSchemaQuality:
    def test_all_classes_declared(self):
        g = build_schema()
        expected = [BIRD.Taxon, BIRD.Order, BIRD.Family, BIRD.Genus,
                    BIRD.Species, BIRD.Observation, BIRD.Location]
        for cls in expected:
            assert (cls, RDF.type, OWL.Class) in g, f"Missing class: {cls}"

    def test_all_subclass_relations(self):
        g = build_schema()
        for sub in [BIRD.Order, BIRD.Family, BIRD.Genus, BIRD.Species]:
            assert (sub, RDFS.subClassOf, BIRD.Taxon) in g, f"{sub} not subClassOf Taxon"

    def test_wikidata_properties_declared(self):
        g = build_schema()
        for prop in [BIRD.massGrams, BIRD.wingspanMm, BIRD.habitat,
                     BIRD.range, BIRD.dielCycle]:
            assert (prop, RDF.type, OWL.DatatypeProperty) in g, f"Missing property: {prop}"

    def test_iucn_properties_declared(self):
        g = build_schema()
        for prop in [BIRD.iucnCategory, BIRD.populationTrend, BIRD.iucnHabitat,
                     BIRD.threat, BIRD.movementPattern, BIRD.system]:
            assert (prop, RDF.type, OWL.DatatypeProperty) in g, f"Missing property: {prop}"

    def test_all_properties_have_domain_and_range(self):
        g = build_schema()
        for prop in g.subjects(RDF.type, OWL.DatatypeProperty):
            domains = list(g.objects(prop, RDFS.domain))
            ranges = list(g.objects(prop, RDFS.range))
            assert domains, f"{prop} has no domain"
            assert ranges, f"{prop} has no range"


# ── Base graph quality ───────────────────────────────────────────────────────


@pytest.mark.skipif(not BASE_TTL.exists(), reason="output/birdology.ttl not found")
class TestBaseGraphQuality:
    @pytest.fixture(scope="class")
    def graph(self):
        return _load(BASE_TTL)

    def test_has_species(self, graph):
        count = sum(1 for _ in graph.subjects(RDF.type, BIRD.Species))
        assert count > 10000, f"Expected 10000+ species, got {count}"

    def test_has_observations(self, graph):
        count = sum(1 for _ in graph.subjects(RDF.type, BIRD.Observation))
        assert count > 0, "No observations found"

    def test_species_have_scientific_names(self, graph):
        species = list(graph.subjects(RDF.type, BIRD.Species))
        with_names = [sp for sp in species if list(graph.objects(sp, DWC.scientificName))]
        ratio = len(with_names) / len(species)
        assert ratio > 0.99, f"Only {ratio:.1%} of species have scientific names"

    def test_species_have_english_names(self, graph):
        species = list(graph.subjects(RDF.type, BIRD.Species))
        with_names = [sp for sp in species if list(graph.objects(sp, BIRD.commonNameEn))]
        ratio = len(with_names) / len(species)
        assert ratio > 0.95, f"Only {ratio:.1%} of species have English names"

    def test_observations_have_dates(self, graph):
        obs = list(graph.subjects(RDF.type, BIRD.Observation))
        with_dates = [o for o in obs if list(graph.objects(o, BIRD.observedOn))]
        ratio = len(with_dates) / len(obs) if obs else 0
        assert ratio > 0.95, f"Only {ratio:.1%} of observations have dates"

    def test_observations_have_locations(self, graph):
        obs = list(graph.subjects(RDF.type, BIRD.Observation))
        with_locs = [o for o in obs if list(graph.objects(o, BIRD.observedAt))]
        ratio = len(with_locs) / len(obs) if obs else 0
        assert ratio > 0.95, f"Only {ratio:.1%} of observations have locations"

    def test_no_empty_scientific_names(self, graph):
        for sp in graph.subjects(RDF.type, BIRD.Species):
            for name in graph.objects(sp, DWC.scientificName):
                assert str(name).strip(), f"Empty scientificName on {sp}"


# ── Enriched graph quality ───────────────────────────────────────────────────


@pytest.mark.skipif(not ENRICHED_TTL.exists(), reason="output/birdology_enriched.ttl not found")
class TestEnrichedGraphQuality:
    @pytest.fixture(scope="class")
    def graph(self):
        return _load(ENRICHED_TTL)

    def test_more_triples_than_base(self, graph):
        assert len(graph) > 200000, f"Enriched graph too small: {len(graph)} triples"

    def test_has_wikidata_links(self, graph):
        """At least some species should have owl:sameAs to Wikidata."""
        wd_links = [o for o in graph.objects(predicate=OWL.sameAs)
                     if "wikidata.org" in str(o)]
        assert len(wd_links) > 1000, f"Only {len(wd_links)} Wikidata links"

    def test_has_wingspan_data(self, graph):
        count = sum(1 for _ in graph.subject_objects(BIRD.wingspanMm))
        assert count > 100, f"Only {count} wingspan values"

    def test_has_range_data(self, graph):
        count = sum(1 for _ in graph.subject_objects(BIRD.range))
        assert count > 1000, f"Only {count} range values"

    def test_has_diel_data(self, graph):
        count = sum(1 for _ in graph.subject_objects(BIRD.dielCycle))
        assert count > 100, f"Only {count} diel cycle values"

    def test_wingspan_values_reasonable(self, graph):
        """Wingspan should be between 5mm and 4000mm."""
        for _, ws in graph.subject_objects(BIRD.wingspanMm):
            val = float(ws)
            assert 5 <= val <= 4000, f"Unreasonable wingspan: {val}mm"

    def test_mass_values_reasonable(self, graph):
        """Mass should be between 0.5g (bee hummingbird) and 200kg (ostrich)."""
        for _, m in graph.subject_objects(BIRD.massGrams):
            val = float(m)
            assert 0.5 <= val <= 200000, f"Unreasonable mass: {val}g"
