"""
Wikidata enrichment for the Birdology knowledge graph.

Queries the Wikidata SPARQL endpoint to add traits (wingspan, mass,
IUCN status, habitat, range, diel cycle) and cross-links (Wikidata ID,
GBIF ID, eBird ID) to existing species nodes.

No authentication required — uses the public SPARQL endpoint.
"""
from __future__ import annotations

import time
import urllib.parse

import requests
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, XSD
from tqdm import tqdm

from ..namespaces import BIRD, DWC, TAXON

_ENDPOINT = "https://query.wikidata.org/sparql"
_TIMEOUT = 60
_BATCH_SIZE = 80  # Wikidata VALUES limit per query
_DELAY = 2.0  # seconds between batches (respect rate limit)

# Wikidata namespace for owl:sameAs links
WD = "http://www.wikidata.org/entity/"

_SPARQL_TEMPLATE = """\
SELECT ?name ?item ?itemLabel ?habitat ?habitatLabel
       ?status ?statusLabel ?range ?rangeLabel
       ?diel ?dielLabel ?mass ?wingspan
       ?gbifID ?ebirdID
WHERE {{
  VALUES ?name {{ {values} }}
  ?item wdt:P225 ?name .
  OPTIONAL {{ ?item wdt:P2974 ?habitat }}
  OPTIONAL {{ ?item wdt:P141 ?status }}
  OPTIONAL {{ ?item wdt:P9714 ?range }}
  OPTIONAL {{ ?item wdt:P9566 ?diel }}
  OPTIONAL {{ ?item p:P2067/psv:P2067 [ wikibase:quantityAmount ?mass ;
                                         wikibase:quantityUnit ?massUnit ] .
              FILTER(?massUnit = wd:Q11570) }}
  OPTIONAL {{ ?item p:P2050/psv:P2050 [ wikibase:quantityAmount ?wingspan ;
                                         wikibase:quantityUnit ?wsUnit ] .
              FILTER(?wsUnit = wd:Q174728) }}
  OPTIONAL {{ ?item wdt:P846 ?gbifID }}
  OPTIONAL {{ ?item wdt:P3444 ?ebirdID }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
}}
"""


def _query_wikidata(sparql: str) -> list[dict]:
    """Execute a SPARQL query against Wikidata and return bindings."""
    resp = requests.get(
        _ENDPOINT,
        params={"query": sparql, "format": "json"},
        headers={"User-Agent": "Birdology/1.0 (knowledge graph enrichment)"},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", {}).get("bindings", [])


def _val(binding: dict, key: str) -> str | None:
    """Extract value string from a SPARQL binding, or None."""
    b = binding.get(key)
    return b["value"] if b else None


def fetch_wikidata_traits(sci_names: list[str]) -> dict[str, dict]:
    """Query Wikidata for traits of species by scientific name.

    Returns a dict mapping scientific name → trait dict with keys:
        label, habitats, iucn_status, ranges, diel, mass_g, wingspan_mm,
        gbif_id, ebird_id, wikidata_id
    """
    results: dict[str, dict] = {}

    batches = [sci_names[i:i + _BATCH_SIZE] for i in range(0, len(sci_names), _BATCH_SIZE)]

    for batch in tqdm(batches, desc="Querying Wikidata", unit="batch"):
        values = " ".join(f'"{n}"' for n in batch)
        sparql = _SPARQL_TEMPLATE.format(values=values)

        try:
            bindings = _query_wikidata(sparql)
        except Exception as e:
            print(f"  Warning: Wikidata batch failed: {e}")
            time.sleep(_DELAY)
            continue

        for b in bindings:
            name = _val(b, "name")
            if not name:
                continue

            if name not in results:
                results[name] = {
                    "label": _val(b, "itemLabel"),
                    "habitats": set(),
                    "iucn_status": None,
                    "ranges": set(),
                    "diel": None,
                    "mass_g": None,
                    "wingspan_mm": None,
                    "gbif_id": None,
                    "ebird_id": None,
                    "wikidata_id": None,
                }

            rec = results[name]

            # Extract Wikidata entity ID from ?item URI
            item_uri = _val(b, "item")
            if item_uri and not rec["wikidata_id"] and "/Q" in item_uri:
                rec["wikidata_id"] = item_uri.split("/")[-1]

            habitat = _val(b, "habitatLabel")
            if habitat and not habitat.startswith("Q"):
                rec["habitats"].add(habitat)

            status = _val(b, "statusLabel")
            if status and not status.startswith("Q"):
                rec["iucn_status"] = status

            rng = _val(b, "rangeLabel")
            if rng and not rng.startswith("Q"):
                rec["ranges"].add(rng)

            diel = _val(b, "dielLabel")
            if diel and not diel.startswith("Q"):
                rec["diel"] = diel

            mass = _val(b, "mass")
            if mass:
                try:
                    rec["mass_g"] = float(mass)
                except ValueError:
                    pass

            ws = _val(b, "wingspan")
            if ws:
                try:
                    rec["wingspan_mm"] = float(ws)
                except ValueError:
                    pass

            gbif = _val(b, "gbifID")
            if gbif:
                rec["gbif_id"] = gbif

            ebird = _val(b, "ebirdID")
            if ebird:
                rec["ebird_id"] = ebird

        time.sleep(_DELAY)

    # Convert sets to sorted lists for consistency
    for rec in results.values():
        rec["habitats"] = sorted(rec["habitats"])
        rec["ranges"] = sorted(rec["ranges"])

    return results


def enrich_graph(g: Graph, traits: dict[str, dict]) -> int:
    """Add Wikidata traits as triples to an existing graph.

    Returns the number of triples added.
    """
    added = 0

    # Build sci_name → species URI index from the graph
    sci_index: dict[str, URIRef] = {}
    for sp in g.subjects(RDF.type, BIRD.Species):
        for name in g.objects(sp, DWC.scientificName):
            sci_index[str(name)] = URIRef(sp)

    for sci_name, data in traits.items():
        sp_uri = sci_index.get(sci_name)
        if not sp_uri:
            continue

        if data.get("mass_g") is not None:
            g.add((sp_uri, BIRD.massGrams, Literal(data["mass_g"], datatype=XSD.decimal)))
            added += 1

        if data.get("wingspan_mm") is not None:
            g.add((sp_uri, BIRD.wingspanMm, Literal(data["wingspan_mm"], datatype=XSD.decimal)))
            added += 1

        if data.get("diel"):
            g.add((sp_uri, BIRD.dielCycle, Literal(data["diel"])))
            added += 1

        for habitat in data.get("habitats", []):
            g.add((sp_uri, BIRD.habitat, Literal(habitat)))
            added += 1

        for rng in data.get("ranges", []):
            g.add((sp_uri, BIRD.range, Literal(rng)))
            added += 1

        if data.get("wikidata_id"):
            wd_uri = URIRef(WD + data["wikidata_id"])
            g.add((sp_uri, OWL.sameAs, wd_uri))
            added += 1

    return added
