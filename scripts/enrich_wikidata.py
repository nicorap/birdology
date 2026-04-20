#!/usr/bin/env python3
"""
Enrich the Birdology knowledge graph with Wikidata traits.

Queries the Wikidata SPARQL endpoint for wingspan, mass, habitat,
IUCN status, range, diel cycle, and cross-links for all species in
the graph.

Usage:
    python scripts/enrich_wikidata.py
    python scripts/enrich_wikidata.py --input output/birdology_reasoned.ttl
    python scripts/enrich_wikidata.py --output output/birdology_enriched.ttl
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib.namespace import RDF

from birdology.graph import load_graph_rdflib, save_graph
from birdology.ingestion.wikidata import enrich_graph, fetch_wikidata_traits
from birdology.namespaces import BIRD, DWC


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich Birdology graph with Wikidata traits")
    parser.add_argument(
        "--input",
        default="output/birdology.ttl",
        metavar="PATH",
        help="Input Turtle graph (default: output/birdology.ttl)",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Output Turtle file (default: overwrite input)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path

    if not input_path.exists():
        print(f"Error: {input_path} not found", file=sys.stderr)
        sys.exit(1)

    # Load with rdflib (not Oxigraph) so we can add triples
    print(f"Loading graph from {input_path} …")
    g = load_graph_rdflib(input_path)

    # Collect all scientific names from species nodes
    sci_names = []
    for sp in g.subjects(RDF.type, BIRD.Species):
        for name in g.objects(sp, DWC.scientificName):
            sci_names.append(str(name))

    print(f"Found {len(sci_names):,} species to enrich.")

    # Query Wikidata
    traits = fetch_wikidata_traits(sci_names)
    print(f"Wikidata returned data for {len(traits):,} species.")

    # Count what we got
    stats = {
        "habitat": sum(1 for t in traits.values() if t["habitats"]),
        "mass": sum(1 for t in traits.values() if t["mass_g"] is not None),
        "wingspan": sum(1 for t in traits.values() if t["wingspan_mm"] is not None),
        "diel": sum(1 for t in traits.values() if t["diel"]),
        "ranges": sum(1 for t in traits.values() if t["ranges"]),
        "wikidata_id": sum(1 for t in traits.values() if t["wikidata_id"]),
    }
    print("Data coverage:")
    for key, count in stats.items():
        print(f"  {key}: {count:,} species")

    # Enrich the graph
    added = enrich_graph(g, traits)
    print(f"\nAdded {added:,} triples to the graph.")
    print(f"Graph now has {len(g):,} triples total.")

    # Save
    save_graph(g, output_path)


if __name__ == "__main__":
    main()
