#!/usr/bin/env python3
"""
Run example SPARQL queries against the saved Birdology knowledge graph.

Usage
-----
    python scripts/query_graph.py [--input output/birdology.ttl]
    python scripts/query_graph.py --species "Robin"
    python scripts/query_graph.py --family "Turdidae"
    python scripts/query_graph.py --order "Passeriformes"
    python scripts/query_graph.py --danish
    python scripts/query_graph.py --obs "Erithacus"
    python scripts/query_graph.py --cemetery           # rare birds near Assistens Kirkegård
    python scripts/query_graph.py --nearby LAT LON     # rare birds near custom coordinates
    python scripts/query_graph.py --summary
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.graph import load_graph  # noqa: E402
from birdology.queries import (  # noqa: E402
    ASSISTENS_LAT,
    ASSISTENS_LON,
    _IUCN_EMOJI,
    find_species_by_name,
    list_danish_species,
    nearby_watch,
    recent_danish_observations,
    species_by_family,
    species_by_order,
    taxonomy_summary,
)

_IUCN_LABEL = {
    "CR": "Critically Endangered",
    "EN": "Endangered",
    "VU": "Vulnerable",
    "NT": "Near Threatened",
    "LC": "Least Concern",
    "DD": "Data Deficient",
}


def _print_rows(rows: list[dict], max_rows: int = 50) -> None:
    if not rows:
        print("  (no results)")
        return
    keys = list(rows[0].keys())
    col_w = {k: max(len(k), max(len(r.get(k, "")) for r in rows[:max_rows])) for k in keys}
    header = "  " + "  ".join(k.ljust(col_w[k]) for k in keys)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for row in rows[:max_rows]:
        print("  " + "  ".join(row.get(k, "").ljust(col_w[k]) for k in keys))
    if len(rows) > max_rows:
        print(f"  … {len(rows) - max_rows} more rows")


def _print_watch(rows: list[dict], location_label: str) -> None:
    """Pretty-print nearby watch results with rarity emoji."""
    if not rows:
        print("  (no observations found nearby)")
        return
    print(f"  {'#':<3}  {'St':<4}  {'Nom français':<28}  {'Dansk navn':<24}  {'Scientific name':<28}  {'Locality'}")
    print("  " + "-" * 120)
    for i, row in enumerate(rows, 1):
        status = row.get("status", "")
        emoji = _IUCN_EMOJI.get(status, " ")
        fr = row.get("commonNameFr", "?")
        da = row.get("commonNameDa", "?")
        sci = row.get("scientificName", "?")
        locality = row.get("locality", "")
        date = row.get("date", "")
        status_str = f"{emoji}{status}" if status else "  ?"
        print(f"  {i:<3}  {status_str:<5}  {fr:<28}  {da:<24}  {sci:<28}  {locality}  {date}")


def main():
    parser = argparse.ArgumentParser(description="Query the Birdology knowledge graph.")
    parser.add_argument("--input", default="output/birdology_reasoned.ttl",
                        help="Path to the Turtle graph file")
    parser.add_argument("--species", metavar="NAME",
                        help="Find species by name (scientific or common)")
    parser.add_argument("--family", metavar="NAME", help="List species in a family")
    parser.add_argument("--order", metavar="NAME", help="List species in an order")
    parser.add_argument("--danish", action="store_true",
                        help="List all species with Danish observations")
    parser.add_argument("--obs", metavar="NAME",
                        help="Show recent Danish observations, optionally filtered by species name")
    parser.add_argument("--cemetery", action="store_true",
                        help="Show rare/cool birds observed near Assistens Kirkegård, Nørrebro")
    parser.add_argument("--nearby", nargs=2, metavar=("LAT", "LON"), type=float,
                        help="Show rare birds near custom coordinates (decimal degrees)")
    parser.add_argument("--radius", type=float, default=2.0,
                        help="Search radius in km for --cemetery / --nearby (default: 2.0)")
    parser.add_argument("--summary", action="store_true",
                        help="Print taxonomy summary counts")
    args = parser.parse_args()

    g = load_graph(args.input)

    has_action = any([args.species, args.family, args.order, args.danish,
                      args.obs is not None, args.cemetery, args.nearby])

    if args.summary or not has_action:
        print("\nTaxonomy summary:")
        for k, v in taxonomy_summary(g).items():
            print(f"  {k:>15}: {v:,}")

    if args.species:
        print(f"\nSpecies matching '{args.species}':")
        _print_rows(find_species_by_name(g, args.species))

    if args.family:
        print(f"\nSpecies in family '{args.family}':")
        _print_rows(species_by_family(g, args.family))

    if args.order:
        print(f"\nSpecies in order '{args.order}':")
        _print_rows(species_by_order(g, args.order))

    if args.danish:
        print("\nSpecies with Danish observations:")
        _print_rows(list_danish_species(g))

    if args.obs is not None:
        label = f"filtered by '{args.obs}'" if args.obs else "all"
        print(f"\nRecent Danish observations ({label}):")
        _print_rows(recent_danish_observations(g, args.obs or None))

    if args.cemetery:
        print(f"\nBirds to watch near Assistens Kirkegård, Nørrebro "
              f"(within {args.radius} km) — sorted by rarity:")
        rows = nearby_watch(g, ASSISTENS_LAT, ASSISTENS_LON, radius_km=args.radius)
        _print_watch(rows, "Assistens Kirkegård")
        if rows:
            threatened = [r for r in rows if r.get("status") in ("CR", "EN", "VU", "NT")]
            if threatened:
                print(f"\n  ⚠  {len(threatened)} threatened/near-threatened species observed nearby!")

    if args.nearby:
        lat, lon = args.nearby
        print(f"\nBirds to watch near ({lat}, {lon}) within {args.radius} km — sorted by rarity:")
        rows = nearby_watch(g, lat, lon, radius_km=args.radius)
        _print_watch(rows, f"({lat}, {lon})")


if __name__ == "__main__":
    main()
