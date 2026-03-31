"""
DOFbasen ingestion via the GBIF public REST API.

DOF (Dansk Ornitologisk Forening / BirdLife Denmark) publishes their observation
data weekly to GBIF as a Darwin Core Archive.

Dataset: https://www.gbif.org/dataset/95db4db8-f762-11e1-a439-00145eb45e9a
GBIF API: https://api.gbif.org/v1/  (no auth required for reads)
"""
from __future__ import annotations

import hashlib
import re
from datetime import date

import requests
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, XSD
from tqdm import tqdm

from ..namespaces import BIRD, DWC, GBIF, LOC, OBS, TAXON

_GBIF_BASE = "https://api.gbif.org/v1"
_DOF_DATASET_KEY = "95db4db8-f762-11e1-a439-00145eb45e9a"
_TIMEOUT = 30
_PAGE_SIZE = 300  # GBIF max per request

# IUCN Red List categories ordered from most to least threatened
IUCN_RANK = {"CR": 0, "EN": 1, "VU": 2, "NT": 3, "LC": 4, "DD": 5, "NE": 6}
IUCN_LABELS = {
    "EX": "Extinct",
    "EW": "Extinct in the Wild",
    "CR": "Critically Endangered",
    "EN": "Endangered",
    "VU": "Vulnerable",
    "NT": "Near Threatened",
    "LC": "Least Concern",
    "DD": "Data Deficient",
    "NE": "Not Evaluated",
}


_GBIF_OFFSET_CAP = 9_700   # GBIF hard-caps offset at ~10 000; stay safely under

def _fetch_year(year: int, remaining: int) -> list[dict]:
    """Fetch up to *remaining* occurrences for a single calendar year."""
    results: list[dict] = []
    offset = 0
    while len(results) < remaining:
        limit = min(_PAGE_SIZE, remaining - len(results))
        params = {
            "datasetKey": _DOF_DATASET_KEY,
            "limit": limit,
            "offset": offset,
            "basisOfRecord": "HUMAN_OBSERVATION",
            "year": year,
        }
        resp = requests.get(
            f"{_GBIF_BASE}/occurrence/search", params=params, timeout=_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("results", [])
        results.extend(batch)
        offset += len(batch)
        if data.get("endOfRecords", True) or not batch or offset >= _GBIF_OFFSET_CAP:
            break
    return results


def fetch_dof_occurrences(max_records: int = 5000) -> list[dict]:
    """
    Fetch occurrence records from the DOF GBIF dataset.

    The GBIF occurrence search API caps the offset at ~10 000 per query, so
    fetching more than that requires batching by year.  We iterate recent years
    newest-first until *max_records* is reached.

    max_records caps total records fetched (default 5 000; use a higher value
    like 50 000 to get a broader sample — each year contributes up to ~9 700).
    """
    from datetime import date as _date
    current_year = _date.today().year
    years = list(range(current_year, current_year - 30, -1))  # up to 30 years back

    results: list[dict] = []
    seen_keys: set = set()

    with tqdm(total=max_records, unit="rec", desc="Fetching DOFbasen") as pbar:
        for year in years:
            if len(results) >= max_records:
                break
            batch = _fetch_year(year, max_records - len(results))
            for rec in batch:
                key = rec.get("key") or rec.get("gbifID")
                if key and key not in seen_keys:
                    seen_keys.add(key)
                    results.append(rec)
            pbar.update(len(batch))

    return results[:max_records]


def _obs_uri(gbif_key: int | str) -> URIRef:
    return OBS[str(gbif_key)]


def _loc_uri(lat: float, lon: float) -> URIRef:
    # Round to ~10 m precision for deduplication
    key = f"{lat:.4f}_{lon:.4f}".replace("-", "N").replace(".", "p")
    return LOC[key]


def _species_uri_by_name(sci_name: str) -> URIRef:
    """Fallback species URI when no eBird match is found — slug of scientific name."""
    slug = re.sub(r"[^A-Za-z0-9]", "_", sci_name).strip("_")
    return TAXON[f"species/sci/{slug}"]


def _stable_obs_id(rec: dict) -> str:
    """Produce a stable URI fragment for an occurrence record."""
    key = str(rec.get("key") or rec.get("gbifID") or "")
    if key:
        return key
    raw = f"{rec.get('scientificName')}{rec.get('eventDate')}{rec.get('decimalLatitude')}{rec.get('decimalLongitude')}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def occurrences_to_rdf(
    records: list[dict],
    sci_name_index: dict[str, URIRef] | None = None,
) -> Graph:
    """
    Convert GBIF occurrence records from DOFbasen to RDF.

    Creates Observation and Location instances and links them to Species nodes
    via bird:hasObservation.  Extracts IUCN conservation status when present.

    Parameters
    ----------
    records:
        Raw GBIF occurrence dicts.
    sci_name_index:
        Optional mapping of scientificName → species URIRef built from the
        eBird graph (see graph.build_sci_name_index).  When provided, DOF
        observations are attached directly to the matching eBird species node.
        When absent, a fallback URI is minted from the scientific name.
    """
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)
    g.bind("obs", OBS)
    g.bind("loc", LOC)
    g.bind("gbif", GBIF)

    # Track which species have had their conservation status set (avoid overwriting
    # a more specific status with a less specific one from a later record).
    iucn_written: dict[URIRef, int] = {}

    for rec in tqdm(records, desc="Converting to RDF", unit="rec", leave=False):
        sci_name = (rec.get("scientificName") or "").strip()
        if not sci_name:
            continue

        obs_id = _stable_obs_id(rec)
        obs_uri = _obs_uri(obs_id)

        # GBIF returns "Genus species (Author, Year)" — extract binomial for matching
        binomial = " ".join(sci_name.split()[:2])

        # Match to eBird species node when possible; otherwise mint a fallback URI.
        if sci_name_index and binomial in sci_name_index:
            sp_uri = sci_name_index[binomial]
        else:
            sp_uri = _species_uri_by_name(sci_name)
            g.add((sp_uri, RDF.type, BIRD.Species))
            g.add((sp_uri, DWC.scientificName, Literal(sci_name)))

        # ── IUCN conservation status ─────────────────────────────────────────
        iucn = (rec.get("iucnRedListCategory") or "").strip().upper()
        if iucn and iucn in IUCN_RANK:
            current_rank = iucn_written.get(sp_uri, 999)
            if IUCN_RANK[iucn] < current_rank:
                # Remove any previously written (worse) status
                g.remove((sp_uri, BIRD.conservationStatus, None))
                g.add((sp_uri, BIRD.conservationStatus, Literal(iucn)))
                iucn_written[sp_uri] = IUCN_RANK[iucn]

        # ── Observation ──────────────────────────────────────────────────────
        g.add((obs_uri, RDF.type, BIRD.Observation))
        g.add((obs_uri, DWC.scientificName, Literal(sci_name)))

        event_date = rec.get("eventDate") or rec.get("dateIdentified")
        if event_date:
            try:
                d = date.fromisoformat(str(event_date)[:10])
                g.add((obs_uri, BIRD.observedOn, Literal(d, datatype=XSD.date)))
            except ValueError:
                pass

        count = rec.get("individualCount")
        if count is not None:
            try:
                g.add((obs_uri, BIRD.individualCount, Literal(int(count), datatype=XSD.integer)))
            except (TypeError, ValueError):
                pass

        recorder = rec.get("recordedBy")
        if recorder:
            g.add((obs_uri, BIRD.recordedBy, Literal(str(recorder))))

        gbif_key = rec.get("key") or rec.get("gbifID")
        if gbif_key:
            g.add((obs_uri, BIRD.gbifKey, Literal(int(gbif_key), datatype=XSD.integer)))

        # ── Location ─────────────────────────────────────────────────────────
        lat = rec.get("decimalLatitude")
        lon = rec.get("decimalLongitude")
        if lat is not None and lon is not None:
            try:
                lat, lon = float(lat), float(lon)
                loc_uri = _loc_uri(lat, lon)
                g.add((loc_uri, RDF.type, BIRD.Location))
                g.add((loc_uri, BIRD.latitude, Literal(lat, datatype=XSD.decimal)))
                g.add((loc_uri, BIRD.longitude, Literal(lon, datatype=XSD.decimal)))
                locality = rec.get("locality") or rec.get("verbatimLocality")
                if locality:
                    g.add((loc_uri, BIRD.locality, Literal(str(locality))))
                g.add((obs_uri, BIRD.observedAt, loc_uri))
            except (TypeError, ValueError):
                pass

        # ── Link observation to species ───────────────────────────────────────
        g.add((sp_uri, BIRD.hasObservation, obs_uri))

        # ── owl:sameAs to GBIF species IRI ──────────────────────────────────
        gbif_species_key = rec.get("speciesKey") or rec.get("taxonKey")
        if gbif_species_key:
            g.add((sp_uri, OWL.sameAs, GBIF[str(gbif_species_key)]))

    return g
