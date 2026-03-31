#!/usr/bin/env python3
"""
Materialise inferred facts for the Birdology knowledge graph
using parallel Python workers.

Four inference rules are applied:

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

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from birdology.graph import load_graph, save_graph
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
    """Add direct parentTaxon links for all transitive ancestors, in parallel."""
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
    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(_ancestors_for_batch, b): b for b in batches}
        for fut in as_completed(futures):
            new_pairs.extend(fut.result())

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

    total_new = n1 + n2 + n3 + n4
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
