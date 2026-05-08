#!/usr/bin/env python3
"""
Enrich the Birdology knowledge graph with EltonTraits diet and foraging data.

Downloads the EltonTraits 1.0 bird dataset (CC0) from figshare if not already
cached in data/EltonTraits_birds.txt, then adds diet percentages and foraging-
strata properties to matching species nodes.

Properties added
----------------
bird:dietInvertPct, bird:dietVertPct, bird:dietFruitPct, bird:dietNectarPct,
bird:dietSeedPct, bird:dietScavPct, bird:dietCategory,
bird:foragingGround, bird:foragingCanopy, bird:foragingAerial,
bird:foragingWater, bird:nocturnal

Usage
-----
    python scripts/enrich_elton.py
    python scripts/enrich_elton.py --input output/birdology_enriched.ttl
    python scripts/enrich_elton.py --elton data/EltonTraits_birds.txt  # local file
    python scripts/enrich_elton.py --download-only                     # just cache the file
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.graph import load_graph_rdflib, save_graph
from birdology.ingestion.elton import download_elton_traits, enrich_graph, load_elton_traits


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich Birdology graph with EltonTraits diet & foraging data"
    )
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
        "--elton",
        default=None,
        metavar="PATH",
        help="Path to local EltonTraits bird file (default: data/EltonTraits_birds.txt, "
             "auto-downloaded if missing)",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Just download/verify the EltonTraits file and exit",
    )
    args = parser.parse_args()

    elton_path = Path(args.elton) if args.elton else None

    if args.download_only:
        path = download_elton_traits(elton_path)
        print(f"EltonTraits file ready at {path}")
        return

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path

    if not input_path.exists():
        print(f"Error: {input_path} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Loading graph from {input_path} …")
    g = load_graph_rdflib(input_path)
    before = len(g)

    print("Loading EltonTraits …")
    records = load_elton_traits(elton_path)

    print("Enriching graph …")
    enrich_graph(g, records)

    print(f"Triples: {before:,} → {len(g):,} (+{len(g) - before:,})")
    print(f"Saving to {output_path} …")
    save_graph(g, output_path)
    print("Done.")


if __name__ == "__main__":
    main()
