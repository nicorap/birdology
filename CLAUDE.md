# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests (no API key needed)
pytest tests/

# Build the knowledge graph (requires EBIRD_API_KEY in .env)
python scripts/build_graph.py --dof-max 5000

# Build with fewer DOF records (faster for development)
python scripts/build_graph.py --dof-max 500 --no-danish

# Query the saved graph
python scripts/query_graph.py --summary
python scripts/query_graph.py --species "Robin"
python scripts/query_graph.py --family "Turdidae"
python scripts/query_graph.py --danish
python scripts/query_graph.py --obs "Erithacus"

# Rare/cool birds near Assistens Kirkegård, Nørrebro (hardcoded)
python scripts/query_graph.py --cemetery
python scripts/query_graph.py --cemetery --radius 5.0   # wider search

# Birds near any coordinates
python scripts/query_graph.py --nearby 55.6918 12.5559

# Visualize (interactive HTML + stats PNG → output/)
python scripts/visualize.py
python scripts/visualize.py --mode graph --family "Turdidae"
python scripts/visualize.py --mode stats
```

## Architecture

The project builds an OWL/RDF knowledge graph of birds and saves it as a Turtle file (`output/birdology.ttl`).

### Package layout (`src/birdology/`)

| File | Role |
|------|------|
| `namespaces.py` | All `rdflib.Namespace` objects. Import from here; never hardcode URIs elsewhere. |
| `schema.py` | `build_schema()` — declares OWL classes and properties into a `Graph`. The single source of truth for the ontology shape. |
| `graph.py` | `build_graph(ebird_key)` orchestrates schema + ingestion → `ConjunctiveGraph`. `save_graph` / `load_graph` handle Turtle I/O. |
| `queries.py` | Reusable SPARQL functions that take a graph and return `list[dict]`. |
| `ingestion/ebird.py` | Calls eBird API v2 (`/ref/taxonomy/ebird`), converts records → RDF via `taxonomy_to_rdf()`. |
| `ingestion/gbif_dof.py` | Calls GBIF API for DOFbasen dataset (key `95db4db8`), converts occurrences → RDF via `occurrences_to_rdf()`. |

### Key design decisions

**Namespace split**: Instance IRIs use `TAXON:`, `OBS:`, `LOC:` namespaces; ontology terms use `BIRD:`. Darwin Core (`DWC:`) is used for standard biodiversity terms (`scientificName`, `family`, `order`, `genus`).

**Cross-source linking**: eBird species get `owl:sameAs <https://ebird.org/species/{code}>`. DOF occurrences get `owl:sameAs <https://www.gbif.org/species/{gbifKey}>`. Species from both sources are connected through their shared `dwc:scientificName`.

**Species URI scheme**: eBird species → `taxon:species/{eBirdCode}`. DOF occurrence → species node at `taxon:species/sci/{slug_of_scientificName}`. When eBird data is loaded first, the DOF occurrences link to a separate node; a SPARQL reasoner or `owl:sameAs` closure unifies them.

**DOF access**: DOFbasen has no public REST API. Data is accessed via the GBIF public API (no auth required) using dataset key `95db4db8-f762-11e1-a439-00145eb45e9a`.

## External APIs

| API | Auth | Key env var |
|-----|------|-------------|
| eBird API v2 (`api.ebird.org/v2`) | `x-ebirdapitoken` header | `EBIRD_API_KEY` |
| GBIF (`api.gbif.org/v1`) | None (public) | — |

Get an eBird key at https://ebird.org/api/keygen (free, requires an eBird account).
