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


def load_graph(path: str | Path) -> "Graph | OxiGraph":
    """Load a Turtle file, using a fast Oxigraph binary cache when available.

    First call for a given TTL builds an Oxigraph persistent store alongside
    it (e.g. `output/.birdology_oxi/`).  Subsequent calls load the binary
    store directly — typically 10-15× faster than re-parsing the TTL.

    Falls back to plain rdflib.Graph if pyoxigraph is not installed.
    """
    path = Path(path)
    try:
        return _load_oxigraph(path)
    except ImportError:
        pass
    g = Graph()
    _bind_prefixes(g)
    g.parse(str(path), format="turtle")
    print(f"Loaded {path}  ({len(g):,} triples)")
    return g


def _load_oxigraph(path: Path) -> "OxiGraph":
    import pyoxigraph

    cache_dir = path.parent / f".{path.stem}_oxi"
    stamp_file = cache_dir / "_stamp"
    ttl_mtime  = path.stat().st_mtime

    cache_ok = (
        cache_dir.exists()
        and stamp_file.exists()
        and float(stamp_file.read_text()) >= ttl_mtime
    )

    store = pyoxigraph.Store(str(cache_dir))
    if not cache_ok:
        store.clear()
        with open(path, "rb") as f:
            store.bulk_load(f, pyoxigraph.RdfFormat.TURTLE)
        store.optimize()
        stamp_file.write_text(str(ttl_mtime))

    oxi = OxiGraph(store)
    print(f"Loaded {path}  ({len(oxi):,} triples)")
    return oxi


class OxiGraph:
    """Thin rdflib-compatible wrapper around a pyoxigraph.Store.

    Supports `.query(sparql)` and `len()`.  The result object exposes
    `.vars` (list of str) and is iterable, with each row supporting
    both `row["varname"]` and `row.varname` access — same contract as
    rdflib query results.
    """

    def __init__(self, store) -> None:
        self._store = store

    def __len__(self) -> int:
        return len(self._store)

    def query(self, sparql: str) -> "_OxiResult":
        return _OxiResult(self._store.query(sparql))


class _OxiResult:
    def __init__(self, solutions) -> None:
        self.vars = [v.value for v in solutions.variables]
        self._data = list(solutions)

    def __iter__(self):
        for sol in self._data:
            yield _OxiRow(sol)


class _OxiRow:
    __slots__ = ("_sol",)

    def __init__(self, solution) -> None:
        self._sol = solution

    def _val(self, key: str):
        v = self._sol[key]
        return v.value if v is not None else None

    def __getitem__(self, key: str):
        return self._val(key)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._val(name)


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
