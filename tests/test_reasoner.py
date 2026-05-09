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
    _materialise_co_occurrence,
    _materialise_population_trend,
    _materialise_hotspots,
    _materialise_atypical_observations,
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


# ── Rule 6: co-occurrence ────────────────────────────────────────────────────

from birdology.namespaces import OBS, LOC


def _co_occurrence_graph() -> Graph:
    """Two species observed at the same (location, date) on 3 events."""
    g = Graph()
    sp_a = TAXON["species/robi"]
    sp_b = TAXON["species/meru"]
    sp_c = TAXON["species/pama"]  # only 1 shared event with sp_a
    g.add((sp_a, RDF.type, BIRD.Species))
    g.add((sp_b, RDF.type, BIRD.Species))
    g.add((sp_c, RDF.type, BIRD.Species))

    for i in range(3):
        loc = LOC[f"loc{i}"]
        date = Literal(f"2024-06-{10 + i:02d}", datatype=XSD.date)
        # sp_a and sp_b always together
        for sp in [sp_a, sp_b]:
            obs = OBS[f"{sp.split('/')[-1]}_obs{i}"]
            g.add((sp, BIRD.hasObservation, obs))
            g.add((obs, BIRD.observedAt, loc))
            g.add((obs, BIRD.observedOn, date))
        # sp_c only on first event
        if i == 0:
            obs_c = OBS[f"pama_obs{i}"]
            g.add((sp_c, BIRD.hasObservation, obs_c))
            g.add((obs_c, BIRD.observedAt, loc))
            g.add((obs_c, BIRD.observedOn, date))
    return g


def test_co_occurrence_links_frequent_pairs():
    g = _co_occurrence_graph()
    n = _materialise_co_occurrence(g, threshold=3)
    sp_a = TAXON["species/robi"]
    sp_b = TAXON["species/meru"]
    assert n >= 2  # symmetric pair
    assert (sp_a, BIRD.frequentlyCoOccursWith, sp_b) in g
    assert (sp_b, BIRD.frequentlyCoOccursWith, sp_a) in g


def test_co_occurrence_skips_infrequent_pairs():
    g = _co_occurrence_graph()
    _materialise_co_occurrence(g, threshold=3)
    sp_a = TAXON["species/robi"]
    sp_c = TAXON["species/pama"]
    # sp_a and sp_c only share 1 event — below threshold
    assert (sp_a, BIRD.frequentlyCoOccursWith, sp_c) not in g


def test_co_occurrence_idempotent():
    g = _co_occurrence_graph()
    n1 = _materialise_co_occurrence(g, threshold=3)
    n2 = _materialise_co_occurrence(g, threshold=3)
    assert n1 > 0
    assert n2 == 0


# ── Rule 7: local population trend ──────────────────────────────────────────

def _trend_graph(yearly_counts: dict[int, int]) -> tuple[Graph, object]:
    """Build a graph with observations spread across years."""
    g = Graph()
    sp = TAXON["species/trend_bird"]
    g.add((sp, RDF.type, BIRD.Species))
    obs_idx = 0
    for year, count in yearly_counts.items():
        for j in range(count):
            obs = OBS[f"trend_obs{obs_idx}"]
            loc = LOC["trend_loc"]
            g.add((sp, BIRD.hasObservation, obs))
            g.add((obs, BIRD.observedOn, Literal(f"{year}-06-15", datatype=XSD.date)))
            g.add((obs, BIRD.observedAt, loc))
            obs_idx += 1
    return g, sp


def test_trend_increasing():
    # Strong upward trend: 2, 4, 8, 16
    g, sp = _trend_graph({2020: 2, 2021: 4, 2022: 8, 2023: 16})
    n = _materialise_population_trend(g, min_years=3)
    assert n == 1
    assert (sp, BIRD.populationTrendLocal, Literal("Increasing")) in g


def test_trend_decreasing():
    # Strong downward trend: 20, 10, 5, 2
    g, sp = _trend_graph({2020: 20, 2021: 10, 2022: 5, 2023: 2})
    n = _materialise_population_trend(g, min_years=3)
    assert n == 1
    assert (sp, BIRD.populationTrendLocal, Literal("Decreasing")) in g


def test_trend_stable():
    # Flat: 10, 10, 10, 10
    g, sp = _trend_graph({2020: 10, 2021: 10, 2022: 10, 2023: 10})
    n = _materialise_population_trend(g, min_years=3)
    assert n == 1
    assert (sp, BIRD.populationTrendLocal, Literal("Stable")) in g


def test_trend_skips_insufficient_years():
    g, sp = _trend_graph({2020: 5, 2021: 10})
    n = _materialise_population_trend(g, min_years=3)
    assert n == 0
    assert (sp, BIRD.populationTrendLocal, None) not in g


def test_trend_idempotent():
    g, sp = _trend_graph({2020: 2, 2021: 4, 2022: 8, 2023: 16})
    n1 = _materialise_population_trend(g, min_years=3)
    n2 = _materialise_population_trend(g, min_years=3)
    assert n1 > 0
    assert n2 == 0


# ── Rule 8: hotspot locations ────────────────────────────────────────────────

def _hotspot_graph(n_obs: int) -> tuple[Graph, object]:
    """Build a graph with n_obs observations at the same location."""
    g = Graph()
    loc = LOC["hotspot_loc"]
    g.add((loc, RDF.type, BIRD.Location))
    sp = TAXON["species/robi"]
    g.add((sp, RDF.type, BIRD.Species))
    for i in range(n_obs):
        obs = OBS[f"hot_obs{i}"]
        g.add((obs, RDF.type, BIRD.Observation))
        g.add((obs, BIRD.observedAt, loc))
        g.add((sp, BIRD.hasObservation, obs))
    return g, loc


def test_hotspot_marks_busy_location():
    g, loc = _hotspot_graph(25)
    n = _materialise_hotspots(g, threshold=20)
    assert n >= 2
    assert (loc, BIRD.isHotspot, Literal(True)) in g


def test_hotspot_skips_quiet_location():
    g, loc = _hotspot_graph(5)
    n = _materialise_hotspots(g, threshold=20)
    assert n == 0
    assert (loc, BIRD.isHotspot, Literal(True)) not in g


def test_hotspot_idempotent():
    g, loc = _hotspot_graph(25)
    n1 = _materialise_hotspots(g, threshold=20)
    n2 = _materialise_hotspots(g, threshold=20)
    assert n1 > 0
    assert n2 == 0


# ── Rule 9: atypical observations ─────────────────────────────────────────────

def _atypical_graph(obs_month: int, typical_months: list[int], total_obs: int):
    """Build a minimal graph with one species and one observation."""
    g = Graph()
    sp = TAXON["species/atyptest"]
    obs = OBS["atyptest_obs"]
    loc = LOC["atyptest_loc"]

    g.add((sp, RDF.type, BIRD.Species))
    g.add((sp, DWC.scientificName, Literal("Testus atypicus")))
    g.add((obs, RDF.type, BIRD.Observation))
    g.add((obs, DWC.scientificName, Literal("Testus atypicus")))
    g.add((obs, BIRD.observedOn, Literal(f"2026-{obs_month:02d}-15", datatype=XSD.date)))
    g.add((obs, BIRD.observedAt, loc))
    g.add((sp, BIRD.hasObservation, obs))
    g.add((loc, RDF.type, BIRD.Location))

    for m in typical_months:
        g.add((sp, BIRD.typicallyPresentInMonth, Literal(m, datatype=XSD.integer)))

    # Add extra observations to simulate total_obs count
    for i in range(total_obs - 1):
        extra_obs = OBS[f"atyptest_extra{i}"]
        g.add((extra_obs, RDF.type, BIRD.Observation))
        g.add((extra_obs, DWC.scientificName, Literal("Testus atypicus")))
        g.add((extra_obs, BIRD.observedAt, loc))
        g.add((extra_obs, BIRD.observedOn, Literal("2025-06-01", datatype=XSD.date)))

    return g, sp, obs


class TestAtypicalObservations:
    def test_out_of_season_tagged(self):
        # Observed in January, typical months are May-August
        g, sp, obs = _atypical_graph(obs_month=1, typical_months=[5, 6, 7, 8], total_obs=20)
        n = _materialise_atypical_observations(g)
        assert n == 1
        reasons = list(g.objects(obs, BIRD.atypicalReason))
        assert len(reasons) == 1
        assert "hors saison" in str(reasons[0])

    def test_in_season_not_tagged(self):
        # Observed in June, typical months include June — not atypical
        g, sp, obs = _atypical_graph(obs_month=6, typical_months=[5, 6, 7, 8], total_obs=20)
        n = _materialise_atypical_observations(g)
        assert n == 0
        assert list(g.objects(obs, BIRD.atypicalReason)) == []

    def test_very_rare_locally_tagged(self):
        # Only 3 total observations in graph — very rare locally (< 5 threshold)
        g, sp, obs = _atypical_graph(obs_month=6, typical_months=[6], total_obs=3)
        n = _materialise_atypical_observations(g)
        # At least the target obs is tagged (extra obs may also be tagged)
        assert n >= 1
        reasons = list(g.objects(obs, BIRD.atypicalReason))
        assert len(reasons) == 1
        assert "rare" in str(reasons[0])

    def test_both_reasons_combined(self):
        # Out of season AND very rare locally
        g, sp, obs = _atypical_graph(obs_month=1, typical_months=[5, 6, 7, 8], total_obs=3)
        n = _materialise_atypical_observations(g)
        assert n >= 1
        reason = str(list(g.objects(obs, BIRD.atypicalReason))[0])
        assert "hors saison" in reason
        assert "rare" in reason

    def test_no_typical_months_not_tagged(self):
        # No typicallyPresentInMonth data → can't be out of season
        g, sp, obs = _atypical_graph(obs_month=1, typical_months=[], total_obs=20)
        n = _materialise_atypical_observations(g)
        assert n == 0

    def test_idempotent(self):
        g, sp, obs = _atypical_graph(obs_month=1, typical_months=[5, 6, 7, 8], total_obs=20)
        n1 = _materialise_atypical_observations(g)
        n2 = _materialise_atypical_observations(g)
        assert n1 == 1
        assert n2 == 0
