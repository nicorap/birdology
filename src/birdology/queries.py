"""
Pre-built SPARQL queries for the Birdology knowledge graph.

Each function accepts an rdflib Graph/ConjunctiveGraph and returns a list of
result row dicts for easy consumption.
"""
from __future__ import annotations

import math

from rdflib import ConjunctiveGraph, Graph

_PREFIXES = """
PREFIX bird: <https://birdology.org/ontology/>
PREFIX taxon: <https://birdology.org/taxon/>
PREFIX obs:   <https://birdology.org/observation/>
PREFIX loc:   <https://birdology.org/location/>
PREFIX dwc:   <http://rs.tdwg.org/dwc/terms/>
PREFIX owl:   <http://www.w3.org/2002/07/owl#>
PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
"""


def _rows(results) -> list[dict]:
    return [
        {str(var): str(row[var]) for var in results.vars if row[var] is not None}
        for row in results
    ]


def find_species_by_name(g: Graph | ConjunctiveGraph, name: str) -> list[dict]:
    """Find species whose English, Danish, French, or scientific name contains *name*."""
    q = (
        _PREFIXES
        + f"""
SELECT DISTINCT ?species ?scientificName ?commonNameEn ?commonNameDa ?commonNameFr ?eBirdCode
WHERE {{
    ?species a bird:Species .
    ?species dwc:scientificName ?scientificName .
    OPTIONAL {{ ?species bird:commonNameEn  ?commonNameEn }}
    OPTIONAL {{ ?species bird:commonNameDa  ?commonNameDa }}
    OPTIONAL {{ ?species bird:commonNameFr  ?commonNameFr }}
    OPTIONAL {{ ?species bird:eBirdCode     ?eBirdCode }}
    FILTER (
        CONTAINS(LCASE(STR(?scientificName)), LCASE("{name}")) ||
        CONTAINS(LCASE(STR(?commonNameEn)),   LCASE("{name}")) ||
        CONTAINS(LCASE(STR(?commonNameDa)),   LCASE("{name}")) ||
        CONTAINS(LCASE(STR(?commonNameFr)),   LCASE("{name}"))
    )
}}
ORDER BY ?scientificName
LIMIT 50
"""
    )
    return _rows(g.query(q))


def list_danish_species(g: Graph | ConjunctiveGraph) -> list[dict]:
    """List all species that have at least one DOFbasen observation."""
    q = (
        _PREFIXES
        + """
SELECT DISTINCT ?species ?scientificName ?commonNameFr ?commonNameDa ?commonNameEn
WHERE {
    ?species a bird:Species ;
             bird:hasObservation ?obs ;
             dwc:scientificName  ?scientificName .
    OPTIONAL { ?species bird:commonNameFr  ?commonNameFr }
    OPTIONAL { ?species bird:commonNameDa  ?commonNameDa }
    OPTIONAL { ?species bird:commonNameEn  ?commonNameEn }
}
ORDER BY ?scientificName
"""
    )
    return _rows(g.query(q))


def species_by_family(g: Graph | ConjunctiveGraph, family_name: str) -> list[dict]:
    """List all species in a given family (scientific or common name match)."""
    q = (
        _PREFIXES
        + f"""
SELECT DISTINCT ?species ?scientificName ?commonNameEn ?commonNameDa ?eBirdCode
WHERE {{
    ?species a bird:Species ;
             dwc:scientificName ?scientificName ;
             dwc:family         ?family .
    OPTIONAL {{ ?species bird:commonNameEn ?commonNameEn }}
    OPTIONAL {{ ?species bird:commonNameDa ?commonNameDa }}
    OPTIONAL {{ ?species bird:eBirdCode   ?eBirdCode }}
    FILTER ( CONTAINS(LCASE(STR(?family)), LCASE("{family_name}")) )
}}
ORDER BY ?scientificName
"""
    )
    return _rows(g.query(q))


def species_by_order(g: Graph | ConjunctiveGraph, order_name: str) -> list[dict]:
    """List all species in a given order."""
    q = (
        _PREFIXES
        + f"""
SELECT DISTINCT ?species ?scientificName ?commonNameEn ?family
WHERE {{
    ?species a bird:Species ;
             dwc:scientificName ?scientificName ;
             dwc:order          ?order .
    OPTIONAL {{ ?species bird:commonNameEn ?commonNameEn }}
    OPTIONAL {{ ?species dwc:family        ?family }}
    FILTER ( CONTAINS(LCASE(STR(?order)), LCASE("{order_name}")) )
}}
ORDER BY ?family ?scientificName
"""
    )
    return _rows(g.query(q))


def recent_danish_observations(
    g: Graph | ConjunctiveGraph, species_name: str | None = None
) -> list[dict]:
    """
    List DOFbasen observations, optionally filtered by species name.

    Returns rows sorted by date descending.
    """
    species_filter = ""
    if species_name:
        species_filter = f'FILTER ( CONTAINS(LCASE(STR(?scientificName)), LCASE("{species_name}")) )'

    q = (
        _PREFIXES
        + f"""
SELECT ?scientificName ?date ?count ?locality ?lat ?lon
WHERE {{
    ?species a bird:Species ;
             bird:hasObservation ?obs ;
             dwc:scientificName  ?scientificName .
    ?obs bird:observedOn ?date .
    OPTIONAL {{ ?obs bird:individualCount ?count }}
    OPTIONAL {{
        ?obs bird:observedAt ?loc .
        OPTIONAL {{ ?loc bird:locality  ?locality }}
        OPTIONAL {{ ?loc bird:latitude  ?lat }}
        OPTIONAL {{ ?loc bird:longitude ?lon }}
    }}
    {species_filter}
}}
ORDER BY DESC(?date)
LIMIT 100
"""
    )
    return _rows(g.query(q))


# Assistens Kirkegård, Nørrebro, Copenhagen
ASSISTENS_LAT = 55.6918
ASSISTENS_LON = 12.5559

# IUCN rank for sorting (lower = more threatened)
_IUCN_RANK = {"CR": 0, "EN": 1, "VU": 2, "NT": 3, "LC": 4, "DD": 5, "NE": 6}
_IUCN_EMOJI = {"CR": "🔴", "EN": "🟠", "VU": "🟡", "NT": "🔵", "LC": "🟢", "DD": "⚪", "NE": "⚫"}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def nearby_watch(
    g: Graph | ConjunctiveGraph,
    lat: float = ASSISTENS_LAT,
    lon: float = ASSISTENS_LON,
    radius_km: float = 2.0,
) -> list[dict]:
    """
    Find bird species observed within *radius_km* of the given coordinates,
    sorted by rarity (IUCN: CR → EN → VU → NT → LC → unknown).

    Defaults to Assistens Kirkegård, Nørrebro.
    """
    # Bounding box pre-filter (1° lat ≈ 111 km, 1° lon ≈ 111*cos(lat) km)
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * math.cos(math.radians(lat)))
    lat_min, lat_max = lat - dlat, lat + dlat
    lon_min, lon_max = lon - dlon, lon + dlon

    q = (
        _PREFIXES
        + f"""
SELECT DISTINCT ?species ?scientificName ?commonNameFr ?commonNameDa ?commonNameEn
                ?status ?date ?count ?locality ?lat ?lon
WHERE {{
    ?species a bird:Species ;
             bird:hasObservation ?obs ;
             dwc:scientificName  ?scientificName .
    OPTIONAL {{ ?species bird:commonNameFr      ?commonNameFr }}
    OPTIONAL {{ ?species bird:commonNameDa      ?commonNameDa }}
    OPTIONAL {{ ?species bird:commonNameEn      ?commonNameEn }}
    OPTIONAL {{ ?species bird:conservationStatus ?status }}
    ?obs bird:observedAt ?loc .
    ?loc bird:latitude   ?lat ;
         bird:longitude  ?lon .
    OPTIONAL {{ ?obs bird:observedOn       ?date }}
    OPTIONAL {{ ?obs bird:individualCount  ?count }}
    OPTIONAL {{ ?loc bird:locality         ?locality }}
    FILTER ( xsd:decimal(?lat) >= {lat_min} && xsd:decimal(?lat) <= {lat_max} &&
             xsd:decimal(?lon) >= {lon_min} && xsd:decimal(?lon) <= {lon_max} )
}}
ORDER BY ?scientificName
"""
    )

    # Fine-grained haversine filter + deduplicate to best observation per species
    candidates: dict[str, dict] = {}
    result = g.query(q)
    vars_ = [str(v) for v in result.vars]
    for row in result:
        try:
            rlat, rlon = float(str(row.lat)), float(str(row.lon))
        except (TypeError, ValueError):
            continue
        if _haversine_km(lat, lon, rlat, rlon) > radius_km:
            continue
        sci = str(row.scientificName)
        existing = candidates.get(sci)
        # Keep the entry with the most threatened status
        new_rank = _IUCN_RANK.get(str(row.status or ""), 999)
        old_rank = _IUCN_RANK.get(str(existing.get("status", "") if existing else ""), 999)
        if existing is None or new_rank < old_rank:
            candidates[sci] = {v: str(row[v]) for v in vars_ if row[v] is not None}

    results = list(candidates.values())
    results.sort(key=lambda r: (_IUCN_RANK.get(r.get("status", ""), 999), r.get("scientificName", "")))
    return results


def taxonomy_summary(g: Graph | ConjunctiveGraph) -> dict:
    """Return counts of Orders, Families, Genera, Species, and Observations."""
    counts = {}
    for label, cls in [
        ("orders", "bird:Order"),
        ("families", "bird:Family"),
        ("genera", "bird:Genus"),
        ("species", "bird:Species"),
        ("observations", "bird:Observation"),
    ]:
        q = _PREFIXES + f"SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE {{ ?x a {cls} }}"
        rows = list(g.query(q))
        counts[label] = int(rows[0]["n"]) if rows else 0
    return counts
