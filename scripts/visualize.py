#!/usr/bin/env python3
"""
Visualize the Birdology knowledge graph.

Modes
-----
    python scripts/visualize.py --mode graph   # interactive HTML (pyvis) of Danish species
    python scripts/visualize.py --mode stats   # conservation status bar chart (PNG)
    python scripts/visualize.py --mode both    # both (default)

Options
-------
    --family Turdidae    Restrict graph mode to one family
    --order  Passeriformes  Restrict graph mode to one order
    --output output/     Output directory (default: output/)
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for saving files
import matplotlib.pyplot as plt
import networkx as nx
from pyvis.network import Network
from rdflib import Graph
from rdflib.namespace import RDF

from birdology.graph import load_graph
from birdology.namespaces import BIRD, DWC

# ── Colour palette ────────────────────────────────────────────────────────────

_STATUS_COLOR = {
    "CR": "#d32f2f",  # dark red
    "EN": "#f57c00",  # orange
    "VU": "#fbc02d",  # amber
    "NT": "#1976d2",  # blue
    "LC": "#388e3c",  # green
    "DD": "#9e9e9e",  # grey
    "NE": "#bdbdbd",  # light grey
    None: "#78909c",  # blue-grey (unknown)
}
_STATUS_ORDER = ["CR", "EN", "VU", "NT", "LC", "DD", "NE"]
_STATUS_LABEL = {
    "CR": "Critically Endangered",
    "EN": "Endangered",
    "VU": "Vulnerable",
    "NT": "Near Threatened",
    "LC": "Least Concern",
    "DD": "Data Deficient",
    "NE": "Not Evaluated",
}

_ORDER_COLOR = "#5c6bc0"    # indigo — order nodes
_FAMILY_COLOR = "#26a69a"   # teal  — family nodes


# ── Helpers ───────────────────────────────────────────────────────────────────

def _short(uri: str) -> str:
    return uri.split("/")[-1]


def _get_str(g: Graph, subject, predicate) -> str | None:
    val = g.value(subject, predicate)
    return str(val) if val else None


def _danish_species_uris(g: Graph) -> set:
    """Return URIs of species that have at least one observation."""
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    SELECT DISTINCT ?sp WHERE { ?sp bird:hasObservation [] }
    """
    return {str(row.sp) for row in g.query(q)}


# ── Graph visualisation (pyvis) ───────────────────────────────────────────────

def build_taxonomy_graph(
    g: Graph,
    family_filter: str | None = None,
    order_filter: str | None = None,
) -> nx.DiGraph:
    """Build a networkx DiGraph of the taxonomy for Danish species."""
    danish = _danish_species_uris(g)
    nx_g = nx.DiGraph()

    for sp_uri in g.subjects(RDF.type, BIRD.Species):
        sp_str = str(sp_uri)
        if sp_str not in danish:
            continue

        sci = _get_str(g, sp_uri, DWC.scientificName) or _short(sp_str)
        da_name = _get_str(g, sp_uri, BIRD.commonNameDa)
        en_name = _get_str(g, sp_uri, BIRD.commonNameEn)
        label = da_name or en_name or sci
        status = _get_str(g, sp_uri, BIRD.conservationStatus)
        family = _get_str(g, sp_uri, DWC.family)
        order = _get_str(g, sp_uri, DWC.order)

        if family_filter and (not family or family_filter.lower() not in family.lower()):
            continue
        if order_filter and (not order or order_filter.lower() not in order.lower()):
            continue

        nx_g.add_node(
            sp_str,
            label=label,
            title=f"{sci}<br>Status: {status or 'unknown'}<br>Family: {family or '?'}",
            color=_STATUS_COLOR.get(status, _STATUS_COLOR[None]),
            size=12,
            shape="dot",
            group="species",
        )

        # Walk parentTaxon chain: species → genus → family → order
        seen = set()
        current = sp_uri
        while True:
            parent = g.value(current, BIRD.parentTaxon)
            if parent is None or str(parent) in seen:
                break
            seen.add(str(parent))
            parent_str = str(parent)
            parent_sci = _get_str(g, parent, DWC.scientificName) or _short(parent_str)

            # Determine parent type for styling
            if (parent, RDF.type, BIRD.Order) in g:
                shape, color, size, group = "star", _ORDER_COLOR, 25, "order"
            elif (parent, RDF.type, BIRD.Family) in g:
                shape, color, size, group = "diamond", _FAMILY_COLOR, 18, "family"
            else:
                shape, color, size, group = "square", "#78909c", 14, "genus"

            if parent_str not in nx_g:
                nx_g.add_node(parent_str, label=parent_sci, color=color,
                              size=size, shape=shape, group=group,
                              title=f"{group.capitalize()}: {parent_sci}")
            nx_g.add_edge(str(current), parent_str)
            current = parent

    return nx_g


def render_html(nx_g: nx.DiGraph, output_path: Path) -> None:
    """Render the networkx graph as an interactive pyvis HTML file."""
    net = Network(height="800px", width="100%", bgcolor="#1a1a2e", font_color="white",
                  directed=False, notebook=False)
    net.from_nx(nx_g)
    net.set_options("""
    {
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.005,
          "springLength": 120,
          "springConstant": 0.08
        },
        "solver": "forceAtlas2Based",
        "stabilization": { "iterations": 150 }
      },
      "interaction": { "hover": true, "navigationButtons": true }
    }
    """)
    net.save_graph(str(output_path))
    print(f"Saved interactive graph → {output_path}")


# ── Stats chart (matplotlib) ──────────────────────────────────────────────────

def render_stats(g: Graph, output_path: Path) -> None:
    """Save a conservation status bar chart for all species with known status."""
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    SELECT ?status (COUNT(?sp) AS ?n)
    WHERE { ?sp a bird:Species ; bird:conservationStatus ?status }
    GROUP BY ?status
    ORDER BY ?status
    """
    counts: dict[str, int] = {}
    for row in g.query(q):
        counts[str(row.status)] = int(str(row.n))

    if not counts:
        print("No conservation status data in graph — try rebuilding with more DOF records.")
        return

    labels = [s for s in _STATUS_ORDER if s in counts]
    values = [counts[s] for s in labels]
    colors = [_STATUS_COLOR[s] for s in labels]
    full_labels = [f"{_STATUS_LABEL[s]}\n({s})" for s in labels]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(full_labels, values, color=colors, edgecolor="white", linewidth=0.5)
    ax.bar_label(bars, padding=3, color="white", fontsize=9)
    ax.set_facecolor("#1a1a2e")
    fig.patch.set_facecolor("#1a1a2e")
    ax.tick_params(colors="white")
    ax.spines[:].set_color("#444")
    ax.set_title("IUCN Conservation Status of Observed Species", color="white", fontsize=13, pad=12)
    ax.set_ylabel("Number of species", color="white")
    plt.tight_layout()
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved stats chart → {output_path}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Visualize the Birdology knowledge graph.")
    parser.add_argument("--input", default="output/birdology.ttl")
    parser.add_argument("--mode", choices=["graph", "stats", "both"], default="both")
    parser.add_argument("--family", help="Restrict taxonomy graph to one family")
    parser.add_argument("--order", help="Restrict taxonomy graph to one order")
    parser.add_argument("--output", default="output/", help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    g = load_graph(args.input)

    if args.mode in ("graph", "both"):
        label = args.family or args.order or "danish"
        nx_g = build_taxonomy_graph(g, family_filter=args.family, order_filter=args.order)
        n_sp = sum(1 for _, d in nx_g.nodes(data=True) if d.get("group") == "species")
        print(f"Graph: {nx_g.number_of_nodes()} nodes, {nx_g.number_of_edges()} edges "
              f"({n_sp} species)")
        if nx_g.number_of_nodes() == 0:
            print("  No matching species found — check --family / --order spelling.")
        else:
            render_html(nx_g, out_dir / f"birdology_{label}.html")

    if args.mode in ("stats", "both"):
        render_stats(g, out_dir / "birdology_stats.png")


if __name__ == "__main__":
    main()
