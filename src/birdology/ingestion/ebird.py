"""
eBird taxonomy ingestion.

Fetches the eBird/Clements taxonomy via the eBird API v2 and converts it to
RDF triples using the Birdology ontology.

API docs: https://documenter.getpostman.com/view/664302/S1ENwy59
API key:  https://ebird.org/api/keygen
"""
from __future__ import annotations

import re

import requests
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, XSD
from tqdm import tqdm

from ..namespaces import BIRD, DWC, EBIRD, TAXON

_EBIRD_BASE = "https://api.ebird.org/v2"
_TIMEOUT = 30


def fetch_taxonomy(api_key: str, locale: str = "en") -> list[dict]:
    """Return the full eBird taxonomy as a list of dicts (one per taxon)."""
    resp = requests.get(
        f"{_EBIRD_BASE}/ref/taxonomy/ebird",
        params={"fmt": "json", "locale": locale},
        headers={"x-ebirdapitoken": api_key},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _slug(text: str) -> str:
    """Convert arbitrary text to a safe URI fragment."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", text).strip("_")


def _order_uri(order_name: str) -> URIRef:
    return TAXON[f"order/{_slug(order_name)}"]


def _family_uri(family_sci_name: str) -> URIRef:
    return TAXON[f"family/{_slug(family_sci_name)}"]


def _genus_uri(genus: str) -> URIRef:
    return TAXON[f"genus/{_slug(genus)}"]


def _species_uri(species_code: str) -> URIRef:
    return TAXON[f"species/{species_code}"]


def taxonomy_to_rdf(records: list[dict], locale: str = "en") -> Graph:
    """
    Convert eBird taxonomy records to an RDF graph.

    Creates Order, Family, Genus, and Species instances linked via
    bird:parentTaxon. Only records with category == 'species' produce
    Species nodes; all others are skipped for instance creation but their
    enclosing Order/Family nodes are still created.
    """
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)
    g.bind("ebird", EBIRD)
    g.bind("taxon", TAXON)

    common_name_prop = BIRD.commonNameEn if locale == "en" else BIRD.commonNameDa

    seen_orders: set[str] = set()
    seen_families: set[str] = set()
    seen_genera: set[str] = set()

    for rec in tqdm(records, desc=f"Converting taxonomy ({locale})", unit="rec", leave=False):
        order_name = rec.get("order", "")
        family_sci = rec.get("familySciName", "")
        family_com = rec.get("familyComName", "")
        sci_name = rec.get("sciName") or rec.get("scientificName", "")
        com_name = rec.get("comName", "")
        species_code = rec.get("speciesCode", "")
        category = rec.get("category", "")
        taxon_order = rec.get("taxonOrder")

        # ── Order ────────────────────────────────────────────────────────────
        if order_name and order_name not in seen_orders:
            o_uri = _order_uri(order_name)
            g.add((o_uri, RDF.type, BIRD.Order))
            g.add((o_uri, DWC.scientificName, Literal(order_name)))
            g.add((o_uri, BIRD.commonNameEn, Literal(order_name)))
            seen_orders.add(order_name)

        # ── Family ───────────────────────────────────────────────────────────
        if family_sci and family_sci not in seen_families:
            f_uri = _family_uri(family_sci)
            g.add((f_uri, RDF.type, BIRD.Family))
            g.add((f_uri, DWC.scientificName, Literal(family_sci)))
            if family_com:
                g.add((f_uri, BIRD.commonNameEn, Literal(family_com)))
            if order_name:
                g.add((f_uri, BIRD.parentTaxon, _order_uri(order_name)))
            seen_families.add(family_sci)

        # Only create Genus + Species instances for true species records
        if category != "species":
            continue

        # ── Genus ────────────────────────────────────────────────────────────
        genus = sci_name.split()[0] if sci_name else ""
        if genus and genus not in seen_genera:
            g_uri = _genus_uri(genus)
            g.add((g_uri, RDF.type, BIRD.Genus))
            g.add((g_uri, DWC.genus, Literal(genus)))
            g.add((g_uri, DWC.scientificName, Literal(genus)))
            if family_sci:
                g.add((g_uri, BIRD.parentTaxon, _family_uri(family_sci)))
            seen_genera.add(genus)

        # ── Species ──────────────────────────────────────────────────────────
        if not species_code:
            continue

        sp_uri = _species_uri(species_code)
        g.add((sp_uri, RDF.type, BIRD.Species))
        g.add((sp_uri, BIRD.eBirdCode, Literal(species_code)))
        g.add((sp_uri, DWC.scientificName, Literal(sci_name)))
        g.add((sp_uri, common_name_prop, Literal(com_name)))
        if genus:
            g.add((sp_uri, BIRD.parentTaxon, _genus_uri(genus)))
            g.add((sp_uri, DWC.genus, Literal(genus)))
        if family_sci:
            g.add((sp_uri, DWC.family, Literal(family_sci)))
        if order_name:
            g.add((sp_uri, DWC.order, Literal(order_name)))
        if taxon_order is not None:
            g.add((sp_uri, BIRD.taxonOrder, Literal(float(taxon_order), datatype=XSD.decimal)))

        # Link eBird external IRI via owl:sameAs
        g.add((sp_uri, OWL.sameAs, EBIRD[species_code]))

    return g


_LOCALE_PROP = {
    "da": BIRD.commonNameDa,
    "fr": BIRD.commonNameFr,
    "en": BIRD.commonNameEn,
}


def add_localized_names(g: Graph, records: list[dict], locale: str) -> None:
    """Merge common names for *locale* into an existing graph (in-place)."""
    prop = _LOCALE_PROP.get(locale)
    if prop is None:
        raise ValueError(f"Unsupported locale '{locale}'. Supported: {list(_LOCALE_PROP)}")
    for rec in records:
        if rec.get("category") != "species":
            continue
        code = rec.get("speciesCode", "")
        name = rec.get("comName", "")
        if code and name:
            g.add((_species_uri(code), prop, Literal(name, lang=locale)))


# Keep backwards-compatible alias
def add_danish_names(g: Graph, records: list[dict]) -> None:
    add_localized_names(g, records, "da")
