"""
OWL schema for the Birdology knowledge graph.

Defines the ontology: classes, object properties, and datatype properties.
Call build_schema() to get an rdflib.Graph with all declarations.
"""
from rdflib import Graph, Literal
from rdflib.namespace import OWL, RDF, RDFS, XSD

from .namespaces import BIRD, DWC, TAXON  # noqa: F401


def build_schema() -> Graph:
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)

    # ── Classes ───────────────────────────────────────────────────────────────

    classes = [
        (BIRD.Taxon,        "Taxon",       "An abstract taxonomic unit."),
        (BIRD.Order,        "Order",       "A taxonomic order (e.g. Passeriformes)."),
        (BIRD.Family,       "Family",      "A taxonomic family (e.g. Turdidae)."),
        (BIRD.Genus,        "Genus",       "A taxonomic genus."),
        (BIRD.Species,      "Species",     "A bird species."),
        (BIRD.Observation,  "Observation", "A recorded sighting of a species at a place and time."),
        (BIRD.Location,     "Location",    "A geographic point where an observation was made."),
    ]
    for uri, label, comment in classes:
        g.add((uri, RDF.type, OWL.Class))
        g.add((uri, RDFS.label, Literal(label)))
        g.add((uri, RDFS.comment, Literal(comment)))

    # Taxonomic hierarchy via rdfs:subClassOf
    for sub, sup in [
        (BIRD.Order,   BIRD.Taxon),
        (BIRD.Family,  BIRD.Taxon),
        (BIRD.Genus,   BIRD.Taxon),
        (BIRD.Species, BIRD.Taxon),
    ]:
        g.add((sub, RDFS.subClassOf, sup))

    # ── Object Properties ─────────────────────────────────────────────────────

    obj_props = [
        (BIRD.parentTaxon,    "parentTaxon",    BIRD.Taxon,        BIRD.Taxon,
         "Links a taxon to its parent in the taxonomic hierarchy."),
        (BIRD.hasObservation, "hasObservation", BIRD.Species,      BIRD.Observation,
         "Links a species to one of its recorded observations."),
        (BIRD.observedAt,     "observedAt",     BIRD.Observation,  BIRD.Location,
         "The location where an observation was made."),
    ]
    for uri, label, domain, range_, comment in obj_props:
        g.add((uri, RDF.type, OWL.ObjectProperty))
        g.add((uri, RDFS.label, Literal(label)))
        g.add((uri, RDFS.domain, domain))
        g.add((uri, RDFS.range, range_))
        g.add((uri, RDFS.comment, Literal(comment)))

    # ── Datatype Properties ───────────────────────────────────────────────────

    dt_props = [
        # Taxonomy identifiers
        (BIRD.eBirdCode,          "eBirdCode",          BIRD.Species,      XSD.string,
         "The eBird alphanumeric species code (e.g. 'robi')."),
        (BIRD.gbifKey,            "gbifKey",            BIRD.Taxon,        XSD.integer,
         "The GBIF numeric taxon key."),
        (BIRD.taxonOrder,         "taxonOrder",         BIRD.Taxon,        XSD.decimal,
         "Numeric sort order within the eBird/Clements taxonomy."),

        # Names (use DWC terms where appropriate, BIRD terms for language variants)
        (DWC.scientificName,      "scientificName",     BIRD.Taxon,        XSD.string,
         "The full scientific name of the taxon."),
        (DWC.order,               "order",              BIRD.Taxon,        XSD.string,
         "The scientific name of the taxonomic order."),
        (DWC.family,              "family",             BIRD.Taxon,        XSD.string,
         "The scientific name of the taxonomic family."),
        (DWC.genus,               "genus",              BIRD.Taxon,        XSD.string,
         "The scientific name of the genus."),
        (BIRD.commonNameEn,       "commonNameEn",       BIRD.Taxon,        XSD.string,
         "English common name."),
        (BIRD.commonNameDa,       "commonNameDa",       BIRD.Taxon,        XSD.string,
         "Danish common name (dansk navn)."),
        (BIRD.commonNameFr,       "commonNameFr",       BIRD.Taxon,        XSD.string,
         "French common name (nom commun français)."),

        # Ecology / traits
        (BIRD.conservationStatus, "conservationStatus", BIRD.Species,      XSD.string,
         "IUCN Red List status (e.g. LC, NT, VU, EN, CR)."),

        # Observation fields
        (BIRD.observedOn,         "observedOn",         BIRD.Observation,  XSD.date,
         "ISO 8601 date of the observation."),
        (BIRD.individualCount,    "individualCount",    BIRD.Observation,  XSD.integer,
         "Number of individuals recorded."),
        (BIRD.recordedBy,         "recordedBy",         BIRD.Observation,  XSD.string,
         "Name of the observer."),

        # Location fields
        (BIRD.latitude,           "latitude",           BIRD.Location,     XSD.decimal,
         "WGS84 decimal latitude."),
        (BIRD.longitude,          "longitude",          BIRD.Location,     XSD.decimal,
         "WGS84 decimal longitude."),
        (BIRD.locality,           "locality",           BIRD.Location,     XSD.string,
         "Human-readable locality name."),
    ]
    for uri, label, domain, range_, comment in dt_props:
        g.add((uri, RDF.type, OWL.DatatypeProperty))
        g.add((uri, RDFS.label, Literal(label)))
        g.add((uri, RDFS.domain, domain))
        g.add((uri, RDFS.range, range_))
        g.add((uri, RDFS.comment, Literal(comment)))

    return g
