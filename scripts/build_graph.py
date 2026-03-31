#!/usr/bin/env python3
"""
Build the Birdology knowledge graph and save it to output/birdology.ttl.

Usage
-----
    python scripts/build_graph.py [--dof-max N] [--no-danish]

Options
-------
--dof-max N     Maximum DOFbasen occurrence records to fetch (default: 5000)
--no-danish     Skip fetching Danish common names (saves one API call)
"""
import argparse
import sys
from pathlib import Path

# Make the src package importable when running from the repo root
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
import os

load_dotenv()

from birdology.graph import build_graph, save_graph  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Build the Birdology RDF knowledge graph.")
    parser.add_argument("--dof-max", type=int, default=5000, metavar="N",
                        help="Max DOFbasen occurrences to fetch (default: 5000)")
    parser.add_argument("--output", default="output/birdology.ttl",
                        help="Output Turtle file path (default: output/birdology.ttl)")
    args = parser.parse_args()

    api_key = os.getenv("EBIRD_API_KEY")
    if not api_key:
        print("ERROR: EBIRD_API_KEY is not set.")
        print("  Copy .env.example to .env and add your key from https://ebird.org/api/keygen")
        sys.exit(1)

    g = build_graph(
        ebird_key=api_key,
        dof_max_records=args.dof_max,
    )
    save_graph(g, args.output)


if __name__ == "__main__":
    main()
