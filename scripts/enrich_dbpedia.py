#!/usr/bin/env python3
"""
Enrich the Birdology knowledge graph with DBpedia data.

Adds thumbnails (Wikimedia Commons photos), range maps, and
owl:sameAs links to DBpedia for species in the graph.

No API key required — uses the public DBpedia SPARQL endpoint.

Usage:
    python scripts/enrich_dbpedia.py
    python scripts/enrich_dbpedia.py --input output/birdology_enriched.ttl
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib.namespace import RDF

from birdology.graph import load_graph_rdflib, save_graph
from birdology.ingestion.dbpedia import enrich_graph, fetch_dbpedia_data
from birdology.namespaces import BIRD, DWC


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich Birdology graph with DBpedia data")
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

    print(f"Loading graph from {input_path} …")
    g = load_graph_rdflib(input_path)

    # Collect (scientific_name, english_name) pairs
    name_pairs = []
    for sp in g.subjects(RDF.type, BIRD.Species):
        sci_names = list(g.objects(sp, DWC.scientificName))
        en_names = list(g.objects(sp, BIRD.commonNameEn))
        if sci_names and en_names:
            name_pairs.append((str(sci_names[0]), str(en_names[0])))

    print(f"Found {len(name_pairs):,} species with English names to look up.")

    # Query DBpedia
    dbpedia_data = fetch_dbpedia_data(name_pairs)
    print(f"DBpedia returned data for {len(dbpedia_data):,} species.")

    stats = {
        "thumbnail": sum(1 for d in dbpedia_data.values() if d["thumbnail"]),
        "range_map": sum(1 for d in dbpedia_data.values() if d["range_map"]),
        "dbpedia_link": sum(1 for d in dbpedia_data.values() if d["dbpedia_uri"]),
    }
    print("Data coverage:")
    for key, count in stats.items():
        print(f"  {key}: {count:,} species")

    added = enrich_graph(g, dbpedia_data)
    print(f"\nAdded {added:,} triples to the graph.")
    print(f"Graph now has {len(g):,} triples total.")

    save_graph(g, output_path)


if __name__ == "__main__":
    main()
