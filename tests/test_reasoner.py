"""Tests for the parallel reasoner inference rules."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib import Graph, Literal
from rdflib.namespace import OWL, RDF, RDFS, XSD

from birdology.namespaces import BIRD, DWC, TAXON


# ── Helpers that mirror the reasoner rules ────────────────────────────────────
# Import the private functions directly for unit-testing each rule in isolation.

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from reason import (
    _materialise_transitive_parent_taxon,
    _materialise_subclass_types,
    _materialise_domain_types,
    _materialise_same_as,
)


# ── Rule 1: transitive parentTaxon ────────────────────────────────────────────

def _chain_graph() -> tuple[Graph, object, object, object, object]:
    """Species → Genus → Family → Order — returns (g, sp, genus, fam, ord)."""
    g = Graph()
    sp    = TAXON["species/robi"]
    genus = TAXON["genus/Erithacus"]
    fam   = TAXON["family/Muscicapidae"]
    order = TAXON["order/Passeriformes"]

    g.add((sp,    RDF.type, BIRD.Species))
    g.add((genus, RDF.type, BIRD.Genus))
    g.add((fam,   RDF.type, BIRD.Family))
    g.add((order, RDF.type, BIRD.Order))

    g.add((sp,    BIRD.parentTaxon, genus))
    g.add((genus, BIRD.parentTaxon, fam))
    g.add((fam,   BIRD.parentTaxon, order))
    return g, sp, genus, fam, order


def test_transitive_closure_adds_species_to_family():
    g, sp, _, fam, _ = _chain_graph()
    n = _materialise_transitive_parent_taxon(g, n_workers=1)
    assert n > 0
    assert (sp, BIRD.parentTaxon, fam) in g


def test_transitive_closure_adds_species_to_order():
    g, sp, _, _, order = _chain_graph()
    _materialise_transitive_parent_taxon(g, n_workers=1)
    assert (sp, BIRD.parentTaxon, order) in g


def test_transitive_closure_does_not_add_self_loop():
    g, sp, genus, fam, order = _chain_graph()
    _materialise_transitive_parent_taxon(g, n_workers=1)
    assert (sp, BIRD.parentTaxon, sp) not in g


def test_transitive_closure_idempotent():
    g, _, _, _, _ = _chain_graph()
    n1 = _materialise_transitive_parent_taxon(g, n_workers=1)
    n2 = _materialise_transitive_parent_taxon(g, n_workers=1)
    assert n1 > 0
    assert n2 == 0  # second run adds nothing


def test_transitive_closure_parallel_same_as_sequential():
    g1, _, _, _, _ = _chain_graph()
    g2, _, _, _, _ = _chain_graph()
    _materialise_transitive_parent_taxon(g1, n_workers=1)
    _materialise_transitive_parent_taxon(g2, n_workers=4)
    assert set(g1) == set(g2)


# ── Rule 2: subClass type propagation ────────────────────────────────────────

def test_subclass_propagation_types_species_as_taxon():
    g = Graph()
    g.add((BIRD.Species, RDFS.subClassOf, BIRD.Taxon))
    sp = TAXON["species/robi"]
    g.add((sp, RDF.type, BIRD.Species))

    n = _materialise_subclass_types(g)
    assert n > 0
    assert (sp, RDF.type, BIRD.Taxon) in g


def test_subclass_propagation_transitive():
    """Order ⊆ Taxon ← inferred via Species ⊆ Order (contrived but tests fixpoint)."""
    g = Graph()
    g.add((BIRD.Species, RDFS.subClassOf, BIRD.Order))
    g.add((BIRD.Order,   RDFS.subClassOf, BIRD.Taxon))
    sp = TAXON["species/robi"]
    g.add((sp, RDF.type, BIRD.Species))

    _materialise_subclass_types(g)
    assert (sp, RDF.type, BIRD.Taxon) in g


def test_subclass_propagation_idempotent():
    g = Graph()
    g.add((BIRD.Species, RDFS.subClassOf, BIRD.Taxon))
    sp = TAXON["species/robi"]
    g.add((sp, RDF.type, BIRD.Species))
    n1 = _materialise_subclass_types(g)
    n2 = _materialise_subclass_types(g)
    assert n2 == 0


# ── Rule 3: domain inference ──────────────────────────────────────────────────

def test_domain_inference_types_subject():
    g = Graph()
    g.add((BIRD.eBirdCode, RDF.type,      OWL.DatatypeProperty))
    g.add((BIRD.eBirdCode, RDFS.domain,   BIRD.Species))
    sp = TAXON["species/robi"]
    g.add((sp, BIRD.eBirdCode, Literal("robi")))

    n = _materialise_domain_types(g)
    assert n > 0
    assert (sp, RDF.type, BIRD.Species) in g


def test_domain_inference_skips_already_typed():
    g = Graph()
    g.add((BIRD.eBirdCode, RDF.type,    OWL.DatatypeProperty))
    g.add((BIRD.eBirdCode, RDFS.domain, BIRD.Species))
    sp = TAXON["species/robi"]
    g.add((sp, RDF.type,      BIRD.Species))
    g.add((sp, BIRD.eBirdCode, Literal("robi")))

    n = _materialise_domain_types(g)
    assert n == 0


# ── Rule 4: owl:sameAs closure ────────────────────────────────────────────────

def test_sameAs_copies_properties_bidirectionally():
    from rdflib import URIRef
    g = Graph()
    a = URIRef("https://birdology.org/taxon/species/robi")
    b = URIRef("https://ebird.org/species/robi")
    g.add((a, OWL.sameAs, b))
    g.add((b, DWC.scientificName, Literal("Erithacus rubecula")))

    n = _materialise_same_as(g)
    assert n > 0
    assert (a, DWC.scientificName, Literal("Erithacus rubecula")) in g


def test_sameAs_symmetric():
    from rdflib import URIRef
    g = Graph()
    a = URIRef("https://birdology.org/taxon/species/robi")
    b = URIRef("https://ebird.org/species/robi")
    g.add((a, OWL.sameAs, b))
    g.add((a, BIRD.eBirdCode, Literal("robi")))

    _materialise_same_as(g)
    assert (b, BIRD.eBirdCode, Literal("robi")) in g


def test_sameAs_does_not_copy_sameAs_itself():
    from rdflib import URIRef
    g = Graph()
    a = URIRef("https://birdology.org/taxon/species/robi")
    b = URIRef("https://ebird.org/species/robi")
    g.add((a, OWL.sameAs, b))
    before = len(g)
    _materialise_same_as(g)
    # No new sameAs triples should be created
    new_sameAs = list(g.subject_objects(OWL.sameAs))
    assert len(new_sameAs) == 1
