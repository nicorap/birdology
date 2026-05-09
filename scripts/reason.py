#!/usr/bin/env python3
"""
Materialise inferred facts for the Birdology knowledge graph
using parallel Python workers.

Nine inference rules are applied:

  1. Transitive parentTaxon closure  (parallelised with ProcessPoolExecutor)
       Species → Genus → Family → Order
     becomes direct links:  Species → Family  and  Species → Order,
     enabling flat "give me all species in Passeriformes" queries without
     multi-hop traversal.

  2. SubClass propagation
       bird:Species rdfs:subClassOf bird:Taxon
     → every species individual is also typed bird:Taxon.

  3. Domain inference
       bird:eBirdCode rdfs:domain bird:Species
     → if ?x bird:eBirdCode ?y  then  ?x rdf:type bird:Species.

  4. owl:sameAs closure
       properties on GBIF/eBird IRIs propagate to the canonical node.

Usage
-----
    python scripts/reason.py
    python scripts/reason.py --input  output/birdology.ttl \\
                              --output output/birdology_reasoned.ttl
    python scripts/reason.py --workers 8   # default: cpu_count
"""
import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from collections import defaultdict

from birdology.graph import load_graph_rdflib as load_graph, save_graph
from birdology.migration import infer_migration_status
from birdology.namespaces import BIRD

DWC_SCI = URIRef("http://rs.tdwg.org/dwc/terms/scientificName")
PARENT_TAXON = str(BIRD.parentTaxon)


def _count_by_class(g: Graph) -> dict[str, int]:
    classes = {
        "Order":       BIRD.Order,
        "Family":      BIRD.Family,
        "Genus":       BIRD.Genus,
        "Species":     BIRD.Species,
        "Taxon":       BIRD.Taxon,
        "Observation": BIRD.Observation,
        "Location":    BIRD.Location,
    }
    return {k: sum(1 for _ in g.subjects(RDF.type, v)) for k, v in classes.items()}


# ── Rule 1: Parallel transitive parentTaxon closure ──────────────────────────

def _ancestors_for_batch(args: tuple[list[str], dict[str, list[str]]]) -> list[tuple[str, str]]:
    """Worker function: compute all transitive ancestors for a batch of nodes.

    Returns a list of (node_str, ancestor_str) pairs that are *new* (not direct).
    Runs in a subprocess — must not import rdflib Graph objects.
    """
    nodes, parent_map = args
    new_pairs: list[tuple[str, str]] = []
    for node in nodes:
        direct = set(parent_map.get(node, []))
        visited: set[str] = set()
        queue = list(direct)
        while queue:
            anc = queue.pop()
            if anc in visited:
                continue
            visited.add(anc)
            if anc not in direct:          # transitive-only (not already direct)
                new_pairs.append((node, anc))
            queue.extend(parent_map.get(anc, []))
    return new_pairs


def _materialise_transitive_parent_taxon(g: Graph, n_workers: int) -> int:
    """Add direct parentTaxon links for all transitive ancestors.

    Uses subprocess workers when available, but falls back to a sequential path
    when only one worker is requested or multiprocessing is unavailable in the
    current runtime.
    """
    pt = URIRef(PARENT_TAXON)

    # Serialise the parentTaxon graph as plain dicts (picklable for subprocess)
    parent_map: dict[str, list[str]] = {}
    for s, o in g.subject_objects(pt):
        parent_map.setdefault(str(s), []).append(str(o))

    nodes = list(parent_map.keys())
    if not nodes:
        return 0

    # Split into batches
    batch_size = max(1, len(nodes) // n_workers)
    batches = [
        (nodes[i : i + batch_size], parent_map)
        for i in range(0, len(nodes), batch_size)
    ]

    new_pairs: list[tuple[str, str]] = []
    if n_workers <= 1:
        for batch in batches:
            new_pairs.extend(_ancestors_for_batch(batch))
    else:
        try:
            with ProcessPoolExecutor(max_workers=n_workers) as pool:
                futures = {pool.submit(_ancestors_for_batch, b): b for b in batches}
                for fut in as_completed(futures):
                    new_pairs.extend(fut.result())
        except (NotImplementedError, PermissionError):
            for batch in batches:
                new_pairs.extend(_ancestors_for_batch(batch))

    added = 0
    for s_str, o_str in new_pairs:
        triple = (URIRef(s_str), pt, URIRef(o_str))
        if triple not in g:
            g.add(triple)
            added += 1
    return added


# ── Rule 2: SubClass propagation ─────────────────────────────────────────────

def _materialise_subclass_types(g: Graph) -> int:
    subclass_map: dict[URIRef, list[URIRef]] = {}
    for sub, sup in g.subject_objects(RDFS.subClassOf):
        subclass_map.setdefault(sub, []).append(sup)

    added = 0
    changed = True
    while changed:
        changed = False
        for ind in list(g.subjects(RDF.type, None)):
            for typ in list(g.objects(ind, RDF.type)):
                for sup in subclass_map.get(typ, []):
                    if (ind, RDF.type, sup) not in g:
                        g.add((ind, RDF.type, sup))
                        added += 1
                        changed = True
    return added


# ── Rule 3: Domain inference ─────────────────────────────────────────────────

def _materialise_domain_types(g: Graph) -> int:
    domain_map: dict[URIRef, URIRef] = {}
    for prop in list(g.subjects(RDF.type, OWL.DatatypeProperty)) + \
                list(g.subjects(RDF.type, OWL.ObjectProperty)):
        domain = g.value(prop, RDFS.domain)
        if domain:
            domain_map[prop] = domain

    added = 0
    for prop, domain_cls in domain_map.items():
        for subj in g.subjects(prop, None):
            if (subj, RDF.type, domain_cls) not in g:
                g.add((subj, RDF.type, domain_cls))
                added += 1
    return added


# ── Rule 4: owl:sameAs closure ────────────────────────────────────────────────

def _materialise_same_as(g: Graph) -> int:
    pairs = list(g.subject_objects(OWL.sameAs))
    added = 0
    for a, b in pairs:
        for p, o in list(g.predicate_objects(b)):
            if p == OWL.sameAs:
                continue
            if (a, p, o) not in g:
                g.add((a, p, o))
                added += 1
        for p, o in list(g.predicate_objects(a)):
            if p == OWL.sameAs:
                continue
            if (b, p, o) not in g:
                g.add((b, p, o))
                added += 1
    return added


# ── Rule 6: Co-occurrence ────────────────────────────────────────────────────

_CO_OCCUR_THRESHOLD = 3  # minimum shared (location, date) pairs to link two species


def _materialise_co_occurrence(g: Graph, threshold: int = _CO_OCCUR_THRESHOLD) -> int:
    """Link species that are frequently observed at the same location and date.

    Two species get a symmetric ``bird:frequentlyCoOccursWith`` link when they
    share at least *threshold* distinct (location, date) observation events.
    """
    # Collect (location, date) → set of species
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    SELECT ?species ?loc ?date WHERE {
        ?species a bird:Species ;
                 bird:hasObservation ?obs .
        FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
        ?obs bird:observedAt ?loc ;
             bird:observedOn ?date .
    }
    """
    events: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in g.query(q):
        events[(str(row.loc), str(row.date))].add(str(row.species))

    # Count co-occurrences per species pair
    pair_count: dict[tuple[str, str], int] = defaultdict(int)
    for species_set in events.values():
        species_list = sorted(species_set)
        for i, a in enumerate(species_list):
            for b in species_list[i + 1:]:
                pair_count[(a, b)] += 1

    # Add triples for pairs above threshold
    prop = BIRD.frequentlyCoOccursWith
    added = 0
    for (a_str, b_str), count in pair_count.items():
        if count >= threshold:
            a, b = URIRef(a_str), URIRef(b_str)
            if (a, prop, b) not in g:
                g.add((a, prop, b))
                added += 1
            if (b, prop, a) not in g:
                g.add((b, prop, a))
                added += 1
    return added


# ── Rule 7: Local population trend ──────────────────────────────────────────

_TREND_MIN_YEARS = 3  # need at least this many distinct years to infer a trend


def _materialise_population_trend(g: Graph, min_years: int = _TREND_MIN_YEARS) -> int:
    """Classify each species' local population trend from observation counts per year.

    Uses simple linear regression on (year, observation_count).
    A slope significantly positive → Increasing, negative → Decreasing, else Stable.
    Only species with observations spanning at least *min_years* are classified.
    """
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    SELECT ?species ?date WHERE {
        ?species a bird:Species ;
                 bird:hasObservation ?obs .
        FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
        ?obs bird:observedOn ?date .
        FILTER NOT EXISTS { ?species bird:populationTrendLocal ?any }
    }
    """
    # Collect year → count per species
    yearly: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for row in g.query(q):
        try:
            year = int(str(row.date)[:4])
            yearly[str(row.species)][year] += 1
        except (IndexError, ValueError):
            pass

    added = 0
    for sp_str, year_counts in yearly.items():
        if len(year_counts) < min_years:
            continue
        # Simple linear regression: slope of obs count vs year
        years = sorted(year_counts)
        n = len(years)
        sum_x = sum(years)
        sum_y = sum(year_counts[y] for y in years)
        sum_xy = sum(y * year_counts[y] for y in years)
        sum_x2 = sum(y * y for y in years)
        denom = n * sum_x2 - sum_x * sum_x
        if denom == 0:
            continue
        slope = (n * sum_xy - sum_x * sum_y) / denom
        mean_y = sum_y / n
        # Normalise slope relative to mean count to get a proportional change
        if mean_y > 0:
            relative_slope = slope / mean_y
        else:
            continue
        # Threshold: >10% relative change per year = significant
        if relative_slope > 0.10:
            trend = "Increasing"
        elif relative_slope < -0.10:
            trend = "Decreasing"
        else:
            trend = "Stable"
        g.add((URIRef(sp_str), BIRD.populationTrendLocal, Literal(trend)))
        added += 1
    return added



# ── Rule 8: Hotspot locations ────────────────────────────────────────────────

_HOTSPOT_THRESHOLD = 20  # minimum observations at a location to be a hotspot


def _materialise_hotspots(g: Graph, threshold: int = _HOTSPOT_THRESHOLD) -> int:
    """Mark locations with many observations as hotspots.

    A location gets bird:isHotspot true and bird:observationCount N
    when it has at least *threshold* observations linked to it.
    """
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    SELECT ?loc (COUNT(?obs) AS ?n) WHERE {
        ?obs bird:observedAt ?loc .
        ?loc a bird:Location .
        FILTER NOT EXISTS { ?loc bird:isHotspot ?any }
    }
    GROUP BY ?loc
    """
    from rdflib.namespace import XSD as _XSD
    added = 0
    for row in g.query(q):
        count = int(str(row.n))
        loc = URIRef(str(row.loc))
        if count >= threshold:
            g.add((loc, BIRD.isHotspot, Literal(True)))
            g.add((loc, BIRD.observationCount, Literal(count, datatype=_XSD.integer)))
            added += 2
    return added


# ── Rule 9: Atypical observations ────────────────────────────────────────────

_RARE_LOCAL_THRESHOLD = 5  # species with fewer obs than this = "very rare locally"


def _materialise_atypical_observations(g: Graph) -> int:
    """Tag observations as atypical when:
      - species observed outside its typical months (out of season)
      - species has very few historical observations in Denmark (very rare locally)
    Adds bird:atypicalReason to the observation node.
    """
    from rdflib.namespace import XSD as _XSD

    # Build: species → set of typical months
    typical_months: dict[str, set[int]] = {}
    for sp, _, month in g.triples((None, BIRD.typicallyPresentInMonth, None)):
        typical_months.setdefault(str(sp), set()).add(int(str(month)))

    # Build: species → total obs count in Denmark
    obs_count: dict[str, int] = {}
    q_count = """
    PREFIX bird: <https://birdology.org/ontology/>
    PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
    SELECT ?sp (COUNT(?obs) AS ?n) WHERE {
        ?obs a bird:Observation ;
             bird:hasSpecies ?sp .
    }
    GROUP BY ?sp
    """
    # Also accept scientificName-based linking
    q_count2 = """
    PREFIX bird: <https://birdology.org/ontology/>
    PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
    SELECT ?sci (COUNT(?obs) AS ?n) WHERE {
        ?obs a bird:Observation ;
             dwc:scientificName ?sci .
    }
    GROUP BY ?sci
    """
    for row in g.query(q_count2):
        obs_count[str(row.sci)] = int(str(row.n))

    # Find observations with a date and a linked species
    q_obs = """
    PREFIX bird: <https://birdology.org/ontology/>
    PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
    SELECT ?obs ?sp ?sci ?date WHERE {
        ?obs a bird:Observation ;
             bird:observedOn ?date ;
             dwc:scientificName ?sci .
        FILTER NOT EXISTS { ?obs bird:atypicalReason ?any }
        OPTIONAL { ?sp a bird:Species ; dwc:scientificName ?sci }
    }
    """
    added = 0
    for row in g.query(q_obs):
        obs_uri = URIRef(str(row.obs))
        sci = str(row.sci).split("(")[0].strip()  # strip author from e.g. "Parus major (L.)"
        date_str = str(row.date)
        try:
            month = int(date_str[5:7])
        except (ValueError, IndexError):
            continue

        reasons = []

        # Check out-of-season
        sp_uri = str(row.sp) if row.sp else None
        months = typical_months.get(sp_uri, set()) if sp_uri else set()
        if months and month not in months:
            typical_str = ", ".join(str(m) for m in sorted(months))
            reasons.append(f"hors saison (typique: mois {typical_str})")

        # Check very rare locally (match on first two words of sciName)
        binomial = " ".join(sci.split()[:2])
        count = obs_count.get(sci, obs_count.get(binomial, 0))
        if 0 < count < _RARE_LOCAL_THRESHOLD:
            reasons.append(f"très rare localement ({count} obs. au Danemark)")

        if reasons:
            g.add((obs_uri, BIRD.atypicalReason, Literal(" | ".join(reasons), datatype=_XSD.string)))
            added += 1

    return added


# ── Main ──────────────────────────────────────────────────────────────────────

def run_reasoner(input_path: str, output_path: str, n_workers: int) -> None:
    abs_input  = Path(input_path).resolve()
    abs_output = Path(output_path).resolve()
    abs_output.parent.mkdir(parents=True, exist_ok=True)

    print(f"Loading {abs_input}…")
    g = load_graph(str(abs_input))
    print(f"  {len(g):,} asserted triples.")
    print(f"  Parallel workers: {n_workers}")

    before = _count_by_class(g)

    print(f"Rule 1 — transitive parentTaxon closure  ({n_workers} workers)…")
    n1 = _materialise_transitive_parent_taxon(g, n_workers)
    print(f"  +{n1:,} triples")

    print("Rule 2 — subClass type propagation…")
    n2 = _materialise_subclass_types(g)
    print(f"  +{n2:,} triples")

    print("Rule 3 — domain inference…")
    n3 = _materialise_domain_types(g)
    print(f"  +{n3:,} triples")

    print("Rule 4 — owl:sameAs closure…")
    n4 = _materialise_same_as(g)
    print(f"  +{n4:,} triples")

    print("Rule 5 — migration status from observation months…")
    n5 = infer_migration_status(g)
    print(f"  +{n5:,} triples")

    print("Rule 6 — co-occurrence links…")
    n6 = _materialise_co_occurrence(g)
    print(f"  +{n6:,} triples")

    print("Rule 7 — local population trends…")
    n7 = _materialise_population_trend(g)
    print(f"  +{n7:,} triples")

    print("Rule 8 — hotspot locations…")
    n8 = _materialise_hotspots(g)
    print(f"  +{n8:,} triples")

    print("Rule 9 — atypical observations…")
    n9 = _materialise_atypical_observations(g)
    print(f"  +{n9:,} triples")

    total_new = n1 + n2 + n3 + n4 + n5 + n6 + n7 + n8 + n9
    print(f"\nTotal inferred: +{total_new:,} triples  ({len(g):,} total)")

    # ── Report class counts before / after ──────────────────────────────────
    print("\nType assertions before / after reasoning:")
    after = _count_by_class(g)
    for cls in sorted(after):
        n_before = before.get(cls, 0)
        n_after  = after[cls]
        diff = n_after - n_before
        suffix = f"  (+{diff:,} inferred)" if diff > 0 else ""
        print(f"  {cls:<14}: {n_before:>6,}  →  {n_after:>6,}{suffix}")

    # ── Spot-check transitive parentTaxon ────────────────────────────────────
    sample = next(iter(g.subjects(RDF.type, BIRD.Species)), None)
    if sample:
        ancestors = list(g.objects(sample, BIRD.parentTaxon))
        sci = g.value(sample, DWC_SCI)
        print(f"\nTransitive parentTaxon ancestors of '{sci or sample}':")
        for a in ancestors:
            a_type = str(g.value(a, RDF.type) or "").split("/")[-1]
            a_sci  = g.value(a, DWC_SCI)
            print(f"  [{a_type}]  {a_sci or a}")

    # ── Save ─────────────────────────────────────────────────────────────────
    save_graph(g, abs_output)
    print(f"\nReasoned graph → {abs_output}")
    print("Query it with:")
    print(f"  python scripts/query_graph.py --input {abs_output} --summary")


def main():
    cpu = os.cpu_count() or 4
    parser = argparse.ArgumentParser(
        description="Materialise inferred facts in the Birdology graph (parallel Python rules)."
    )
    parser.add_argument("--input",   default="output/birdology.ttl")
    parser.add_argument("--output",  default="output/birdology_reasoned.ttl")
    parser.add_argument("--workers", type=int, default=cpu,
                        help=f"Parallel workers for transitive closure (default: {cpu})")
    args = parser.parse_args()
    run_reasoner(args.input, args.output, args.workers)


if __name__ == "__main__":
    main()
