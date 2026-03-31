"""
Knowledge graph builder.

Orchestrates schema + ingestion into a single rdflib.Graph,
then serializes it to / loads it from Turtle.
"""
from __future__ import annotations

from pathlib import Path

from rdflib import Graph, URIRef
from rdflib.namespace import RDF

from .ingestion.ebird import (
    add_danish_names,  # noqa: F401 — kept for backwards compat
    add_localized_names,
    fetch_taxonomy,
    taxonomy_to_rdf,
)
from .ingestion.gbif_dof import fetch_dof_occurrences, occurrences_to_rdf
from .namespaces import BIRD, DWC, EBIRD, GBIF, LOC, OBS, TAXON
from .schema import build_schema


def build_graph(
    ebird_key: str,
    dof_max_records: int = 5000,
) -> Graph:
    """
    Build the full Birdology knowledge graph.

    Steps
    -----
    1. Build the OWL schema (classes + properties).
    2. Fetch eBird taxonomy (English) → convert to RDF.
    3. Fetch Danish and French common names and merge them in.
    4. Build a scientificName index and fetch DOF occurrences from GBIF.
    5. Return the merged Graph.

    Parameters
    ----------
    ebird_key:
        eBird API key (from https://ebird.org/api/keygen).
    dof_max_records:
        Maximum number of DOFbasen occurrence records to fetch.
    """
    g = Graph()
    _bind_prefixes(g)

    print("Building OWL schema…")
    g += build_schema()

    print("Fetching eBird taxonomy (English)…")
    en_records = fetch_taxonomy(ebird_key, locale="en")
    print(f"  {len(en_records):,} records received.")
    g += taxonomy_to_rdf(en_records, locale="en")

    for locale in ["da", "fr"]:
        print(f"Fetching eBird taxonomy ({locale} names)…")
        loc_records = fetch_taxonomy(ebird_key, locale=locale)
        loc_graph = Graph()
        add_localized_names(loc_graph, loc_records, locale)
        g += loc_graph

    print("Building scientific name index for cross-source linking…")
    sci_index = build_sci_name_index(g)
    print(f"  {len(sci_index):,} species indexed.")

    print(f"Fetching DOFbasen occurrences (max {dof_max_records:,})…")
    occ_records = fetch_dof_occurrences(max_records=dof_max_records)
    print(f"  {len(occ_records):,} occurrences received.")
    g += occurrences_to_rdf(occ_records, sci_name_index=sci_index)

    print(f"Graph complete — {len(g):,} triples total.")
    return g


def save_graph(g: Graph, path: str | Path) -> None:
    """Serialize the graph to a Turtle file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    g.serialize(destination=str(path), format="turtle")
    print(f"Saved → {path}  ({path.stat().st_size / 1024:.1f} KB)")


def load_graph(path: str | Path) -> Graph:
    """Load a previously saved Turtle file into a Graph."""
    path = Path(path)
    g = Graph()
    _bind_prefixes(g)
    g.parse(str(path), format="turtle")
    print(f"Loaded {path}  ({len(g):,} triples)")
    return g


def build_sci_name_index(g: Graph) -> dict[str, URIRef]:
    """Return a mapping of scientificName → species URIRef from the graph."""
    index = {}
    for sp_uri in g.subjects(RDF.type, BIRD.Species):
        for name in g.objects(sp_uri, DWC.scientificName):
            index[str(name)] = URIRef(sp_uri)
    return index


def _bind_prefixes(g: Graph) -> None:
    g.bind("bird", BIRD)
    g.bind("taxon", TAXON)
    g.bind("obs", OBS)
    g.bind("loc", LOC)
    g.bind("dwc", DWC)
    g.bind("ebird", EBIRD)
    g.bind("gbif", GBIF)
