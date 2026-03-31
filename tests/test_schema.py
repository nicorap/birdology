"""Tests for the OWL schema graph."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib.namespace import OWL, RDF, RDFS

from birdology.namespaces import BIRD, DWC
from birdology.schema import build_schema


def test_schema_builds_without_error():
    g = build_schema()
    assert len(g) > 0


def test_core_classes_exist():
    g = build_schema()
    for cls in [BIRD.Order, BIRD.Family, BIRD.Genus, BIRD.Species,
                BIRD.Observation, BIRD.Location]:
        assert (cls, RDF.type, OWL.Class) in g, f"{cls} not declared as owl:Class"


def test_species_subclass_of_taxon():
    g = build_schema()
    assert (BIRD.Species, RDFS.subClassOf, BIRD.Taxon) in g


def test_key_properties_exist():
    g = build_schema()
    for prop in [BIRD.eBirdCode, BIRD.commonNameEn, BIRD.commonNameDa,
                 BIRD.parentTaxon, BIRD.hasObservation, BIRD.observedAt,
                 BIRD.observedOn, BIRD.latitude, BIRD.longitude,
                 DWC.scientificName]:
        exists = (prop, RDF.type, OWL.DatatypeProperty) in g or \
                 (prop, RDF.type, OWL.ObjectProperty) in g
        assert exists, f"{prop} not declared in schema"
