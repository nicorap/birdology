"""
DBpedia enrichment for the Birdology knowledge graph.

Queries the DBpedia SPARQL endpoint to add thumbnails, images, range maps,
and IUCN status codes to existing species nodes.

No authentication required — uses the public SPARQL endpoint.
"""
from __future__ import annotations

import re
import time

import requests
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD
from tqdm import tqdm

from ..namespaces import BIRD, DWC

_ENDPOINT = "https://dbpedia.org/sparql"
_TIMEOUT = 30
_BATCH_SIZE = 50
_DELAY = 1.0  # seconds between batches

_SPARQL_TEMPLATE = """\
SELECT ?item ?thumb ?status ?statusSystem ?rangeMap WHERE {{
  VALUES ?item {{ {uris} }}
  OPTIONAL {{ ?item <http://dbpedia.org/ontology/thumbnail> ?thumb }}
  OPTIONAL {{ ?item <http://dbpedia.org/property/status> ?status .
             FILTER(LANG(?status) = "en") }}
  OPTIONAL {{ ?item <http://dbpedia.org/property/statusSystem> ?statusSystem .
             FILTER(LANG(?statusSystem) = "en") }}
  OPTIONAL {{ ?item <http://dbpedia.org/property/rangeMap> ?rangeMap }}
}}
"""

_DBR = "http://dbpedia.org/resource/"


def _name_to_dbpedia_slug(common_name: str) -> str:
    """Convert an English common name to a DBpedia resource slug.

    DBpedia uses Wikipedia article titles: first letter capitalized,
    subsequent words lowercase, spaces replaced with underscores.
    E.g. "European Robin" -> "European_robin"
    """
    parts = common_name.strip().split()
    if not parts:
        return ""
    # First word keeps its case, rest lowercase
    slug_parts = [parts[0]] + [p.lower() for p in parts[1:]]
    return "_".join(slug_parts)


def _query_dbpedia(sparql: str) -> list[dict]:
    """Execute a SPARQL query against DBpedia and return bindings."""
    resp = requests.get(
        _ENDPOINT,
        params={"query": sparql, "format": "application/json"},
        headers={"User-Agent": "Birdology/1.0 (knowledge graph enrichment)"},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", {}).get("bindings", [])


def _val(binding: dict, key: str) -> str | None:
    b = binding.get(key)
    return b["value"] if b else None


def fetch_dbpedia_data(name_pairs: list[tuple[str, str]]) -> dict[str, dict]:
    """Query DBpedia for species data using English common names.

    Parameters
    ----------
    name_pairs : list of (scientific_name, english_common_name)

    Returns
    -------
    dict mapping scientific_name → {thumbnail, range_map, dbpedia_uri}
    """
    # Build mapping: dbpedia_slug → scientific_name
    slug_to_sci: dict[str, str] = {}
    for sci_name, en_name in name_pairs:
        slug = _name_to_dbpedia_slug(en_name)
        if slug:
            slug_to_sci[slug] = sci_name

    slugs = list(slug_to_sci.keys())
    results: dict[str, dict] = {}

    batches = [slugs[i:i + _BATCH_SIZE] for i in range(0, len(slugs), _BATCH_SIZE)]

    for batch in tqdm(batches, desc="Querying DBpedia", unit="batch"):
        uris = " ".join(f"<{_DBR}{s}>" for s in batch)
        sparql = _SPARQL_TEMPLATE.format(uris=uris)

        try:
            bindings = _query_dbpedia(sparql)
        except Exception as e:
            print(f"  Warning: DBpedia batch failed: {e}")
            time.sleep(_DELAY)
            continue

        for b in bindings:
            item_uri = _val(b, "item")
            if not item_uri:
                continue

            slug = item_uri.replace(_DBR, "")
            sci_name = slug_to_sci.get(slug)
            if not sci_name:
                continue

            if sci_name not in results:
                results[sci_name] = {
                    "thumbnail": None,
                    "range_map": None,
                    "dbpedia_uri": item_uri,
                }

            rec = results[sci_name]

            thumb = _val(b, "thumb")
            if thumb and not rec["thumbnail"]:
                rec["thumbnail"] = thumb

            rmap = _val(b, "rangeMap")
            if rmap and not rec["range_map"]:
                # Convert filename to Wikimedia Commons URL
                if not rmap.startswith("http"):
                    rmap = f"http://commons.wikimedia.org/wiki/Special:FilePath/{rmap}"
                rec["range_map"] = rmap

        time.sleep(_DELAY)

    return results


def enrich_graph(g: Graph, dbpedia_data: dict[str, dict]) -> int:
    """Add DBpedia data as triples to an existing graph.

    Returns the number of triples added.
    """
    added = 0

    # Build sci_name → species URI index
    sci_index: dict[str, URIRef] = {}
    for sp in g.subjects(RDF.type, BIRD.Species):
        for name in g.objects(sp, DWC.scientificName):
            sci_index[str(name)] = URIRef(sp)

    for sci_name, data in dbpedia_data.items():
        sp_uri = sci_index.get(sci_name)
        if not sp_uri:
            continue

        if data.get("thumbnail"):
            g.add((sp_uri, BIRD.thumbnailUrl, Literal(data["thumbnail"])))
            added += 1

        if data.get("range_map"):
            g.add((sp_uri, BIRD.rangeMapUrl, Literal(data["range_map"])))
            added += 1

        if data.get("dbpedia_uri"):
            from rdflib.namespace import OWL
            g.add((sp_uri, OWL.sameAs, URIRef(data["dbpedia_uri"])))
            added += 1

    return added
