from rdflib import Namespace
from rdflib.namespace import RDF, RDFS, OWL, XSD, SKOS  # noqa: F401 — re-exported for convenience

# Birdology ontology — classes and properties
BIRD = Namespace("https://birdology.org/ontology/")

# Birdology instance namespaces
TAXON = Namespace("https://birdology.org/taxon/")
OBS = Namespace("https://birdology.org/observation/")
LOC = Namespace("https://birdology.org/location/")

# Darwin Core — standard biodiversity vocabulary
DWC = Namespace("http://rs.tdwg.org/dwc/terms/")

# External data sources
EBIRD = Namespace("https://ebird.org/species/")
GBIF = Namespace("https://www.gbif.org/species/")
