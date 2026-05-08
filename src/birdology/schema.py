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
        (BIRD.GraphMeta,    "GraphMeta",   "Metadata about the knowledge graph itself."),
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
        (BIRD.frequentlyCoOccursWith, "frequentlyCoOccursWith", BIRD.Species, BIRD.Species,
         "Links two species frequently observed at the same location and date."),
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
        (BIRD.conservationStatus,    "conservationStatus",    BIRD.Species, XSD.string,
         "IUCN Red List status (e.g. LC, NT, VU, EN, CR)."),
        (BIRD.migrationStatus,       "migrationStatus",       BIRD.Species, XSD.string,
         "Inferred migratory behaviour in Denmark: Resident, SummerVisitor, WinterVisitor, PassageMigrant, PartialMigrant."),
        (BIRD.typicallyPresentInMonth, "typicallyPresentInMonth", BIRD.Species, XSD.integer,
         "Calendar month (1–12) when the species is typically present in Denmark, derived from observation records."),

        # Wikidata traits
        (BIRD.massGrams,      "massGrams",      BIRD.Species, XSD.decimal,
         "Body mass in grams (from Wikidata P2067)."),
        (BIRD.wingspanMm,     "wingspanMm",     BIRD.Species, XSD.decimal,
         "Wingspan in millimetres (from Wikidata P2050)."),
        (BIRD.habitat,        "habitat",        BIRD.Species, XSD.string,
         "Habitat type (from Wikidata P2974), e.g. 'forest', 'wetland'."),
        (BIRD.range,          "range",          BIRD.Species, XSD.string,
         "Geographic range (from Wikidata P9714), e.g. 'Europe', 'Africa'."),
        (BIRD.dielCycle,      "dielCycle",      BIRD.Species, XSD.string,
         "Diel activity pattern: 'diurnality', 'nocturnality' (from Wikidata P9566)."),

        # DBpedia data
        (BIRD.thumbnailUrl,   "thumbnailUrl",   BIRD.Species, XSD.anyURI,
         "Wikimedia Commons thumbnail URL (from DBpedia)."),
        (BIRD.rangeMapUrl,    "rangeMapUrl",    BIRD.Species, XSD.anyURI,
         "Wikimedia Commons range map URL (from DBpedia)."),

        # IUCN Red List traits
        (BIRD.iucnCategory,   "iucnCategory",   BIRD.Species, XSD.string,
         "IUCN Red List category code (LC/NT/VU/EN/CR/EW/EX/DD/NE) from IUCN API v4."),
        (BIRD.populationTrend, "populationTrend", BIRD.Species, XSD.string,
         "Population trend: Increasing, Stable, Decreasing, Unknown (from IUCN)."),
        (BIRD.iucnHabitat,    "iucnHabitat",    BIRD.Species, XSD.string,
         "IUCN habitat classification (e.g. 'Forest - Boreal', 'Marine Neritic')."),
        (BIRD.threat,         "threat",         BIRD.Species, XSD.string,
         "Threat description from IUCN (e.g. 'Hunting & trapping', 'Habitat loss')."),
        (BIRD.movementPattern, "movementPattern", BIRD.Species, XSD.string,
         "Movement pattern: Full Migrant, Altitudinal Migrant, Nomadic, Sedentary (from IUCN)."),
        (BIRD.system,         "system",         BIRD.Species, XSD.string,
         "Ecological system: Terrestrial, Freshwater, Marine (from IUCN)."),
        (BIRD.populationTrendLocal, "populationTrendLocal", BIRD.Species, XSD.string,
         "Local population trend inferred from observation counts per year: Increasing, Decreasing, or Stable."),

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
        (BIRD.isHotspot,          "isHotspot",          BIRD.Location,     XSD.boolean,
         "True if this location has an unusually high number of observations."),
        (BIRD.observationCount,   "observationCount",   BIRD.Location,     XSD.integer,
         "Total number of observations recorded at this location."),

        # EltonTraits — diet percentages
        (BIRD.dietInvertPct,   "dietInvertPct",   BIRD.Species, XSD.decimal,
         "% diet invertebrates (EltonTraits 1.0)."),
        (BIRD.dietVertPct,     "dietVertPct",     BIRD.Species, XSD.decimal,
         "% diet vertebrates incl. fish (EltonTraits 1.0)."),
        (BIRD.dietFruitPct,    "dietFruitPct",    BIRD.Species, XSD.decimal,
         "% diet fruit (EltonTraits 1.0)."),
        (BIRD.dietNectarPct,   "dietNectarPct",   BIRD.Species, XSD.decimal,
         "% diet nectar (EltonTraits 1.0)."),
        (BIRD.dietSeedPct,     "dietSeedPct",     BIRD.Species, XSD.decimal,
         "% diet seeds (EltonTraits 1.0)."),
        (BIRD.dietScavPct,     "dietScavPct",     BIRD.Species, XSD.decimal,
         "% diet carrion/scavenging (EltonTraits 1.0)."),
        (BIRD.dietCategory,    "dietCategory",    BIRD.Species, XSD.string,
         "5-class diet label: Invertebrate | PlantSeed | FruiNect | VertFishScav | Omnivore (EltonTraits 1.0)."),

        # EltonTraits — foraging strata (% of foraging time)
        (BIRD.foragingGround,  "foragingGround",  BIRD.Species, XSD.decimal,
         "% foraging on/near ground (EltonTraits 1.0)."),
        (BIRD.foragingCanopy,  "foragingCanopy",  BIRD.Species, XSD.decimal,
         "% foraging in understory+mid+canopy (EltonTraits 1.0)."),
        (BIRD.foragingAerial,  "foragingAerial",  BIRD.Species, XSD.decimal,
         "% foraging in flight (EltonTraits 1.0)."),
        (BIRD.foragingWater,   "foragingWater",   BIRD.Species, XSD.decimal,
         "% foraging in/around water (EltonTraits 1.0)."),
        (BIRD.nocturnal,       "nocturnal",       BIRD.Species, XSD.boolean,
         "True if primarily nocturnal (EltonTraits 1.0)."),

        # Graph metadata
        (BIRD.lastFetchDate,      "lastFetchDate",      BIRD.GraphMeta,    XSD.date,
         "Date of the most recent DOFbasen data fetch."),
    ]
    for uri, label, domain, range_, comment in dt_props:
        g.add((uri, RDF.type, OWL.DatatypeProperty))
        g.add((uri, RDFS.label, Literal(label)))
        g.add((uri, RDFS.domain, domain))
        g.add((uri, RDFS.range, range_))
        g.add((uri, RDFS.comment, Literal(comment)))

    return g
