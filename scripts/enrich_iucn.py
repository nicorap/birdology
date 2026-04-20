#!/usr/bin/env python3
"""
Enrich the Birdology knowledge graph with IUCN Red List data.

Queries the IUCN Red List API v4 for conservation status, population trends,
habitats, threats, and movement patterns for species in the graph.

Requires IUCN_API_TOKEN in .env (free registration at
https://api.iucnredlist.org/users/sign_up).

Usage:
    python scripts/enrich_iucn.py
    python scripts/enrich_iucn.py --input output/birdology_enriched.ttl
    python scripts/enrich_iucn.py --limit 100   # test with first 100 species
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib.namespace import RDF

from birdology.graph import load_graph_rdflib, save_graph
from birdology.ingestion.iucn import enrich_graph, fetch_iucn_data
from birdology.namespaces import BIRD, DWC

load_dotenv()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich Birdology graph with IUCN Red List data")
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
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Only process first N species (0 = all, default: 0)",
    )
    args = parser.parse_args()

    token = os.getenv("IUCN_API_TOKEN", "")
    if not token:
        print(
            "Error: IUCN_API_TOKEN not set.\n"
            "Register for free at https://api.iucnredlist.org/users/sign_up\n"
            "Then add IUCN_API_TOKEN=your_token to .env",
            file=sys.stderr,
        )
        sys.exit(1)

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path

    if not input_path.exists():
        print(f"Error: {input_path} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Loading graph from {input_path} …")
    g = load_graph_rdflib(input_path)

    # Collect scientific names
    sci_names = []
    for sp in g.subjects(RDF.type, BIRD.Species):
        for name in g.objects(sp, DWC.scientificName):
            sci_names.append(str(name))

    if args.limit > 0:
        sci_names = sci_names[:args.limit]

    print(f"Querying IUCN Red List for {len(sci_names):,} species…")
    iucn_data = fetch_iucn_data(sci_names, token)
    print(f"IUCN returned data for {len(iucn_data):,} species.")

    # Stats
    stats = {
        "category": sum(1 for d in iucn_data.values() if d["category"]),
        "population_trend": sum(1 for d in iucn_data.values() if d["population_trend"]),
        "habitats": sum(1 for d in iucn_data.values() if d["habitats"]),
        "threats": sum(1 for d in iucn_data.values() if d["threats"]),
        "movement": sum(1 for d in iucn_data.values() if d["movement"]),
    }
    print("Data coverage:")
    for key, count in stats.items():
        print(f"  {key}: {count:,} species")

    added = enrich_graph(g, iucn_data)
    print(f"\nAdded {added:,} triples to the graph.")
    print(f"Graph now has {len(g):,} triples total.")

    save_graph(g, output_path)


if __name__ == "__main__":
    main()
