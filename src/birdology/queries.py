"""
Pre-built SPARQL queries for the Birdology knowledge graph.

Each function accepts an rdflib Graph/ConjunctiveGraph and returns a list of
result row dicts for easy consumption.
"""
from __future__ import annotations

import math
import unicodedata

from rdflib import ConjunctiveGraph, Graph


_SPECIAL_FOLD = str.maketrans({
    "ø": "o", "Ø": "o",
    "æ": "ae", "Æ": "ae",
    "å": "a", "Å": "a",
    "ß": "ss",
    "œ": "oe", "Œ": "oe",
    "ð": "d", "Ð": "d",
    "þ": "th", "Þ": "th",
})


def _accent_fold(s: str) -> str:
    """Case- and accent-insensitive fold: 'Rødhals'→'rodhals', 'mésange'→'mesange'.

    1. Map special characters that don't decompose (ø→o, æ→ae, å→a, ß→ss …).
    2. NFD-decompose and strip combining diacritics (é→e, à→a …).
    """
    s = s.translate(_SPECIAL_FOLD)
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii").lower()


def _name_matches(name: str, query: str) -> bool:
    """Case- and accent-insensitive substring match."""
    q_fold = _accent_fold(query)
    return (
        query.lower() in name.lower()
        or q_fold in _accent_fold(name)
    )


# SPARQL REPLACE chain to strip common Latin accents from a string expression.
# Applied to the stored value so unaccented user queries still match.
# Regex-REPLACE chains to normalise stored values in SPARQL.
# Covers French (é/è/ê/ë→e, …), Danish (ø→o, æ→ae, å→a), and common others.
_ACCENT_REGEX_REPLACEMENTS = [
    ("[éèêë]", "e"),
    ("[àâä]",  "a"),
    ("[ùûü]",  "u"),
    ("[îï]",   "i"),
    ("[ôö]",   "o"),
    ("ç",      "c"),
    ("ø",      "o"),   # Danish
    ("å",      "a"),   # Danish
    ("æ",      "ae"),  # Danish (1→2 chars is fine in SPARQL REPLACE)
]


def _sparql_fold(expr: str) -> str:
    """Wrap a SPARQL string expression with regex-REPLACE chains to strip accents."""
    for pattern, replacement in _ACCENT_REGEX_REPLACEMENTS:
        expr = f"REPLACE({expr}, '{pattern}', '{replacement}')"
    return f"LCASE({expr})"


def _sparql_name_filter(*var_exprs: str, query: str) -> str:
    """Build a SPARQL FILTER that matches *query* against any of the given
    variable expressions, both with the original string (covers queries that
    already use accents) and after accent-stripping on the stored side (covers
    accent-free input like 'mesange' matching 'Mésange').
    """
    q_exact  = query.lower()
    q_folded = _accent_fold(query)
    clauses = []
    for expr in var_exprs:
        # Exact case-insensitive match (fast path for already-accented queries)
        clauses.append(f'CONTAINS(LCASE(STR({expr})), "{q_exact}")')
        # Accent-stripped match: strip accents from the stored value, compare
        # with the folded query.  Always included so 'mesange' matches 'Mésange'.
        clauses.append(f'CONTAINS({_sparql_fold(f"STR({expr})")}, "{q_folded}")')
    return "FILTER (\n        " + " ||\n        ".join(clauses) + "\n    )"

_PREFIXES = """
PREFIX bird: <https://birdology.org/ontology/>
PREFIX taxon: <https://birdology.org/taxon/>
PREFIX obs:   <https://birdology.org/observation/>
PREFIX loc:   <https://birdology.org/location/>
PREFIX dwc:   <http://rs.tdwg.org/dwc/terms/>
PREFIX owl:   <http://www.w3.org/2002/07/owl#>
PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
"""

# The owl:sameAs closure copies hasObservation to external IRIs (ebird:, gbif:, …).
# Restrict observation queries to canonical taxon: species nodes to avoid
# re-counting each observation 3–4× and triggering large joins.
_CANONICAL_SPECIES = 'FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))'


def _rows(results) -> list[dict]:
    return [
        {str(var): str(row[var]) for var in results.vars if row[var] is not None}
        for row in results
    ]


def find_species_by_name(g: Graph | ConjunctiveGraph, name: str) -> list[dict]:
    """Find species whose English, Danish, French, or scientific name contains *name*.

    Comparison is case- and accent-insensitive ('mesange' matches 'Mésange').
    """
    name_filter = _sparql_name_filter(
        "?scientificName", "?commonNameEn", "?commonNameDa", "?commonNameFr",
        query=name,
    )
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
    {name_filter}
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
    FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
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
    if species_name:
        # Step 1: find matching species URIs (cheap — no observation join).
        name_filter = _sparql_name_filter(
            "?scientificName", "?commonNameEn", "?commonNameDa", "?commonNameFr",
            query=species_name,
        )
        species_q = (
            _PREFIXES
            + f"""
SELECT DISTINCT ?species ?scientificName ?commonNameEn ?commonNameDa ?commonNameFr
WHERE {{
    ?species a bird:Species ;
             dwc:scientificName ?scientificName .
    {_CANONICAL_SPECIES}
    OPTIONAL {{ ?species bird:commonNameEn ?commonNameEn }}
    OPTIONAL {{ ?species bird:commonNameDa ?commonNameDa }}
    OPTIONAL {{ ?species bird:commonNameFr ?commonNameFr }}
    {name_filter}
}}
"""
        )
        sp_rows = _rows(g.query(species_q))
        if not sp_rows:
            return []
        sp_info = {r["species"]: r for r in sp_rows}
        values = "VALUES ?species { " + " ".join(f"<{uri}>" for uri in sp_info) + " }"
    else:
        sp_info = {}
        values = _CANONICAL_SPECIES.replace("FILTER", "").strip("()").strip()
        values = _CANONICAL_SPECIES  # keep as FILTER in the main query

    if species_name:
        # Step 2: fetch observations only for the matched species.
        obs_q = (
            _PREFIXES
            + f"""
SELECT ?species ?date ?count ?locality ?lat ?lon
WHERE {{
    {values}
    ?species bird:hasObservation ?obs .
    ?obs bird:observedOn ?date .
    OPTIONAL {{ ?obs bird:individualCount ?count }}
    OPTIONAL {{
        ?obs bird:observedAt ?loc .
        OPTIONAL {{ ?loc bird:locality  ?locality }}
        OPTIONAL {{ ?loc bird:latitude  ?lat }}
        OPTIONAL {{ ?loc bird:longitude ?lon }}
    }}
}}
ORDER BY DESC(?date)
LIMIT 100
"""
        )
        obs_rows = _rows(g.query(obs_q))
        # Merge species name info back in
        return [
            {**sp_info.get(r["species"], {}), **r}
            for r in obs_rows
        ]

    # No filter — get recent dates first (fast), then enrich with names.
    q_dates = (
        _PREFIXES
        + f"""
SELECT ?species ?date ?count ?locality ?lat ?lon
WHERE {{
    ?species a bird:Species ;
             bird:hasObservation ?obs .
    {_CANONICAL_SPECIES}
    ?obs bird:observedOn ?date .
    OPTIONAL {{ ?obs bird:individualCount ?count }}
    OPTIONAL {{
        ?obs bird:observedAt ?loc .
        OPTIONAL {{ ?loc bird:locality  ?locality }}
        OPTIONAL {{ ?loc bird:latitude  ?lat }}
        OPTIONAL {{ ?loc bird:longitude ?lon }}
    }}
}}
ORDER BY DESC(?date)
LIMIT 100
"""
    )
    obs_rows = _rows(g.query(q_dates))
    if not obs_rows:
        return []

    values = "VALUES ?species { " + " ".join(f"<{r['species']}>" for r in obs_rows) + " }"
    q_names = (
        _PREFIXES
        + f"""
SELECT DISTINCT ?species ?scientificName ?commonNameEn ?commonNameDa ?commonNameFr
WHERE {{
    {values}
    ?species dwc:scientificName ?scientificName .
    OPTIONAL {{ ?species bird:commonNameEn ?commonNameEn }}
    OPTIONAL {{ ?species bird:commonNameDa ?commonNameDa }}
    OPTIONAL {{ ?species bird:commonNameFr ?commonNameFr }}
}}
"""
    )
    sp_info = {r["species"]: r for r in _rows(g.query(q_names))}
    return [{**sp_info.get(r["species"], {}), **r} for r in obs_rows]


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

    # Step 1: pull locations + dates in a compact query (no species name OPTIONALs yet).
    # Bounding-box pre-filter is cheap; exact haversine done in Python.
    q_obs = (
        _PREFIXES
        + f"""
SELECT ?species ?lat ?lon ?date ?count ?locality
WHERE {{
    ?species a bird:Species ;
             bird:hasObservation ?obs .
    {_CANONICAL_SPECIES}
    ?obs bird:observedAt ?loc .
    ?loc bird:latitude  ?lat ;
         bird:longitude ?lon .
    OPTIONAL {{ ?obs bird:observedOn      ?date }}
    OPTIONAL {{ ?obs bird:individualCount ?count }}
    OPTIONAL {{ ?loc bird:locality        ?locality }}
    FILTER ( xsd:decimal(?lat) >= {lat_min} && xsd:decimal(?lat) <= {lat_max} &&
             xsd:decimal(?lon) >= {lon_min} && xsd:decimal(?lon) <= {lon_max} )
}}
"""
    )

    # Haversine filter → keep the most recent observation per species URI
    best_obs: dict[str, dict] = {}   # species URI → obs dict
    result = g.query(q_obs)
    vars_ = [str(v) for v in result.vars]
    for row in result:
        try:
            rlat, rlon = float(str(row.lat)), float(str(row.lon))
        except (TypeError, ValueError):
            continue
        if _haversine_km(lat, lon, rlat, rlon) > radius_km:
            continue
        sp_uri = str(row.species)
        obs_dict = {v: str(row[v]) for v in vars_ if row[v] is not None}
        existing = best_obs.get(sp_uri)
        # Prefer the most recent observation
        if existing is None or obs_dict.get("date", "") > existing.get("date", ""):
            best_obs[sp_uri] = obs_dict

    if not best_obs:
        return []

    # Step 2: fetch species names + IUCN status for the matched species URIs.
    values = "VALUES ?species { " + " ".join(f"<{uri}>" for uri in best_obs) + " }"
    q_names = (
        _PREFIXES
        + f"""
SELECT ?species ?scientificName ?commonNameFr ?commonNameDa ?commonNameEn ?status
WHERE {{
    {values}
    ?species dwc:scientificName ?scientificName .
    OPTIONAL {{ ?species bird:commonNameFr      ?commonNameFr }}
    OPTIONAL {{ ?species bird:commonNameDa      ?commonNameDa }}
    OPTIONAL {{ ?species bird:commonNameEn      ?commonNameEn }}
    OPTIONAL {{ ?species bird:conservationStatus ?status }}
}}
"""
    )
    sp_info = {r["species"]: r for r in _rows(g.query(q_names))}

    # Merge and sort by IUCN rarity then name
    merged = []
    for sp_uri, obs in best_obs.items():
        row = {**sp_info.get(sp_uri, {}), **obs}
        merged.append(row)
    merged.sort(key=lambda r: (_IUCN_RANK.get(r.get("status", ""), 999), r.get("scientificName", "")))
    return merged


def currently_present(
    g: Graph | ConjunctiveGraph,
    month: int | None = None,
) -> list[dict]:
    """Species typically present in Denmark in *month* (default: current month).

    Requires the graph to have been through the reasoner (Rule 5) so that
    bird:typicallyPresentInMonth triples exist.  Falls back gracefully to
    species that have any observation if no migration data is found.

    Returns rows sorted by migration status then scientific name.
    """
    import datetime
    if month is None:
        month = datetime.date.today().month

    q = (
        _PREFIXES
        + f"""
SELECT DISTINCT ?species ?scientificName ?commonNameDa ?commonNameFr ?commonNameEn
                ?eBirdCode ?status ?migStatus
WHERE {{
    ?species a bird:Species ;
             dwc:scientificName ?scientificName ;
             bird:typicallyPresentInMonth {month} .
    {_CANONICAL_SPECIES}
    OPTIONAL {{ ?species bird:commonNameDa      ?commonNameDa }}
    OPTIONAL {{ ?species bird:commonNameFr      ?commonNameFr }}
    OPTIONAL {{ ?species bird:commonNameEn      ?commonNameEn }}
    OPTIONAL {{ ?species bird:eBirdCode         ?eBirdCode }}
    OPTIONAL {{ ?species bird:conservationStatus ?status }}
    OPTIONAL {{ ?species bird:migrationStatus   ?migStatus }}
}}
ORDER BY ?migStatus ?scientificName
"""
    )
    return _rows(g.query(q))


def observations_for_map(
    g: Graph | ConjunctiveGraph,
    species_filter: str | None = None,
    family_filter: str | None = None,
    order_filter: str | None = None,
) -> list[dict]:
    """Return all observations with coordinates for map rendering.

    Each row contains: scientificName, commonNameEn, commonNameDa, commonNameFr, status,
    lat, lon, date (optional), count (optional), locality (optional).

    Optional filters narrow by species name (any language/scientific),
    family string, or order string.
    """
    family_clause = ""
    if family_filter:
        family_clause = f'?species dwc:family ?fam . FILTER(CONTAINS(LCASE(STR(?fam)), "{family_filter.lower()}"))'
    order_clause = ""
    if order_filter:
        order_clause = f'?species dwc:order ?ord . FILTER(CONTAINS(LCASE(STR(?ord)), "{order_filter.lower()}"))'

    q = (
        _PREFIXES
        + f"""
SELECT ?species ?scientificName ?commonNameEn ?commonNameDa ?commonNameFr ?status
       ?lat ?lon ?date ?count ?locality
WHERE {{
    ?species a bird:Species ;
             dwc:scientificName ?scientificName ;
             bird:hasObservation ?obs .
    {_CANONICAL_SPECIES}
    ?obs bird:observedAt ?loc .
    ?loc bird:latitude  ?lat ;
         bird:longitude ?lon .
    OPTIONAL {{ ?species bird:commonNameEn      ?commonNameEn }}
    OPTIONAL {{ ?species bird:commonNameDa      ?commonNameDa }}
    OPTIONAL {{ ?species bird:commonNameFr      ?commonNameFr }}
    OPTIONAL {{ ?species bird:conservationStatus ?status }}
    OPTIONAL {{ ?obs bird:observedOn      ?date }}
    OPTIONAL {{ ?obs bird:individualCount ?count }}
    OPTIONAL {{ ?loc bird:locality        ?locality }}
    {family_clause}
    {order_clause}
}}
ORDER BY ?scientificName DESC(?date)
"""
    )
    rows = _rows(g.query(q))

    if species_filter:
        rows = [
            r for r in rows
            if _name_matches(r.get("scientificName", ""), species_filter)
            or _name_matches(r.get("commonNameEn", ""), species_filter)
            or _name_matches(r.get("commonNameDa", ""), species_filter)
        ]
    return rows


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
