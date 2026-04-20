"""
IUCN Red List enrichment for the Birdology knowledge graph.

Queries the IUCN Red List API v4 to add conservation status, population
trends, habitats, and threats to existing species nodes.

Requires a free API token — register at https://api.iucnredlist.org/users/sign_up
Set the token in .env as IUCN_API_TOKEN.
"""
from __future__ import annotations

import time

import requests
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD
from tqdm import tqdm

from ..namespaces import BIRD, DWC

_BASE = "https://api.iucnredlist.org/api/v4"
_TIMEOUT = 30
_DELAY = 0.5  # seconds between requests (respect rate limit)


def _get(path: str, token: str, params: dict | None = None) -> dict | None:
    """Make an authenticated GET request to the IUCN API v4."""
    try:
        resp = requests.get(
            f"{_BASE}{path}",
            headers={
                "Authorization": token,
                "User-Agent": "Birdology/1.0 (knowledge graph enrichment)",
            },
            params=params or {},
            timeout=_TIMEOUT,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        return None


def fetch_species_assessment(sci_name: str, token: str) -> dict | None:
    """Look up a species by scientific name and fetch its latest assessment.

    Returns a dict with keys: category, population_trend, habitats, threats,
    movement, systems, or None if not found.
    """
    parts = sci_name.strip().split()
    if len(parts) < 2:
        return None

    genus, species = parts[0], parts[1]

    # Step 1: look up species to get assessment ID
    data = _get("/taxa/scientific_name", token,
                params={"genus_name": genus, "species_name": species})
    if not data:
        return None

    # Find the latest assessment
    assessments = data.get("assessments", [])
    assessment_id = None
    for a in assessments:
        if a.get("latest"):
            assessment_id = a.get("assessment_id")
            break
    if not assessment_id and assessments:
        assessment_id = assessments[0].get("assessment_id")
    if not assessment_id:
        return None

    # Step 2: fetch full assessment
    time.sleep(_DELAY)
    assessment = _get(f"/assessment/{assessment_id}", token)
    if not assessment:
        return None

    result = {
        "category": None,
        "population_trend": None,
        "habitats": [],
        "threats": [],
        "movement": None,
        "systems": [],
    }

    cat = assessment.get("red_list_category", {})
    result["category"] = cat.get("code")

    trend = assessment.get("population_trend", {})
    result["population_trend"] = trend.get("description", {}).get("en")

    for h in assessment.get("habitats", []):
        desc = h.get("description", {}).get("en", "")
        if desc:
            result["habitats"].append({
                "name": desc,
                "suitability": h.get("suitability"),
                "season": h.get("season"),
                "major": h.get("majorImportance") == "Yes",
            })

    for t in assessment.get("threats", []):
        desc = t.get("description", {}).get("en", "")
        if desc:
            result["threats"].append({
                "name": desc,
                "scope": t.get("scope"),
                "severity": t.get("severity"),
                "timing": t.get("timing"),
            })

    supp = assessment.get("supplementary_info", {})
    if supp.get("movement_patterns"):
        result["movement"] = supp["movement_patterns"]

    for s in assessment.get("systems", []):
        desc = s.get("description", {}).get("en", "")
        if desc:
            result["systems"].append(desc)

    return result


def fetch_iucn_data(sci_names: list[str], token: str) -> dict[str, dict]:
    """Fetch IUCN assessments for a list of species by scientific name.

    Returns a dict mapping scientific name → assessment dict.
    """
    results: dict[str, dict] = {}

    for name in tqdm(sci_names, desc="Querying IUCN", unit="sp"):
        data = fetch_species_assessment(name, token)
        if data:
            results[name] = data
        time.sleep(_DELAY)

    return results


def enrich_graph(g: Graph, iucn_data: dict[str, dict]) -> int:
    """Add IUCN data as triples to an existing graph.

    Returns the number of triples added.
    """
    added = 0

    # Build sci_name → species URI index
    sci_index: dict[str, URIRef] = {}
    for sp in g.subjects(RDF.type, BIRD.Species):
        for name in g.objects(sp, DWC.scientificName):
            sci_index[str(name)] = URIRef(sp)

    for sci_name, data in iucn_data.items():
        sp_uri = sci_index.get(sci_name)
        if not sp_uri:
            continue

        if data.get("category"):
            g.add((sp_uri, BIRD.iucnCategory, Literal(data["category"])))
            added += 1

        if data.get("population_trend"):
            g.add((sp_uri, BIRD.populationTrend, Literal(data["population_trend"])))
            added += 1

        if data.get("movement"):
            g.add((sp_uri, BIRD.movementPattern, Literal(data["movement"])))
            added += 1

        for h in data.get("habitats", []):
            g.add((sp_uri, BIRD.iucnHabitat, Literal(h["name"])))
            added += 1

        for t in data.get("threats", []):
            g.add((sp_uri, BIRD.threat, Literal(t["name"])))
            added += 1

        for s in data.get("systems", []):
            g.add((sp_uri, BIRD.system, Literal(s)))
            added += 1

    return added
