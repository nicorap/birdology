#!/usr/bin/env python3
"""
Build the Wikipedia RAG index for all bird species in the knowledge graph.

Fetches English Wikipedia articles for each species, chunks them, embeds
with Ollama (nomic-embed-text) and stores in ChromaDB at data/wiki_index/.

Usage:
    python scripts/build_wiki_index.py                         # all species
    python scripts/build_wiki_index.py --limit 50              # first 50 (test)
    python scripts/build_wiki_index.py --input output/birdology_reasoned.ttl
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib.namespace import RDF

from birdology.graph import load_graph_rdflib
from birdology.namespaces import BIRD, DWC
from birdology.wiki_rag import WikiRAG

DEFAULT_INPUT = Path(__file__).parent.parent / "output" / "birdology.ttl"
DEFAULT_INDEX = Path(__file__).parent.parent / "data" / "wiki_index"


def main() -> None:
    ap = argparse.ArgumentParser(description="Build Wikipedia RAG index")
    ap.add_argument("--input", default=str(DEFAULT_INPUT), help="Input graph (.ttl)")
    ap.add_argument("--index-dir", default=str(DEFAULT_INDEX), help="ChromaDB directory")
    ap.add_argument("--model", default="nomic-embed-text", help="Ollama embedding model")
    ap.add_argument("--ollama-url", default="http://localhost:11434", help="Ollama base URL")
    ap.add_argument("--limit", type=int, default=None, help="Max species to index")
    ap.add_argument("--lang", default="en", help="Wikipedia language code (default: en)")
    args = ap.parse_args()

    print(f"Loading graph from {args.input} …")
    g = load_graph_rdflib(args.input)

    species_list: list[dict] = []
    for sp in g.subjects(RDF.type, BIRD.Species):
        sci = g.value(sp, DWC.scientificName)
        if not sci:
            continue
        common_en = g.value(sp, BIRD.commonNameEn)
        species_list.append({
            "scientificName": str(sci),
            "commonName": str(common_en) if common_en else "",
        })

    if args.limit:
        species_list = species_list[: args.limit]

    print(f"Found {len(species_list)} species — building Wikipedia index …")
    print(f"  embed model : {args.model} @ {args.ollama_url}")
    print(f"  index dir   : {args.index_dir}")
    print()

    rag = WikiRAG(
        index_dir=args.index_dir,
        embed_model=args.model,
        ollama_url=args.ollama_url,
    )

    already = rag.count()
    if already:
        print(f"  (index already has {already} chunks — will skip duplicates)")

    total = rag.build_index(species_list, verbose=True)
    print(f"\nDone: {total} new chunks added  (total in index: {rag.count()})")


if __name__ == "__main__":
    main()
