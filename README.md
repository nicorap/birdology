# Birdology

A bird ontology and knowledge graph built with Python, RDFLib, and OWL.

Combines the global eBird/Clements taxonomy (11 000+ species) with Danish field
observations from DOFbasen via GBIF, and runs an OWL 2 DL reasoner (HermiT) to
materialise inferred facts across the graph.

## What's in the graph

| Layer | Source | Content |
|-------|--------|---------|
| **Taxonomy** | eBird API v2 | 45 orders → 251 families → 2 377 genera → 11 167 species |
| **Names** | eBird API v2 | English, Danish, and French common names per species |
| **Observations** | DOFbasen via GBIF | Sightings in Denmark with date, GPS, locality, observer |
| **Conservation** | GBIF occurrence data | IUCN Red List status per observed species |
| **Links** | `owl:sameAs` | eBird species IRIs ↔ GBIF species IRIs |

## Quick start

```bash
# 1. Install dependencies (Python 3.11+, Java required for reasoner)
pip install -r requirements.txt

# 2. Get a free eBird API key → https://ebird.org/api/keygen
cp .env.example .env
# edit .env and set EBIRD_API_KEY=...

# 3. Build the graph (DOF observations batched by year, up to ~9 700/year)
python scripts/build_graph.py --dof-max 5000    # quick demo
python scripts/build_graph.py --dof-max 50000   # ~5 years of Danish sightings

# 4. Query
python scripts/query_graph.py --summary
python scripts/query_graph.py --cemetery        # birds near Assistens Kirkegård, Nørrebro
python scripts/query_graph.py --species "Rouge-gorge"
python scripts/query_graph.py --family "Turdidae"

# 5. Visualise
python scripts/visualize.py                     # → output/birdology_danish.html + stats.png

# 6. Run reasoner (pure Python, parallelised — no Java required)
python scripts/reason.py                        # → output/birdology_reasoned.ttl
python scripts/reason.py --workers 8            # explicit parallelism
python scripts/query_graph.py --input output/birdology_reasoned.ttl --summary

# 7. Graph-RAG chat (natural language Q&A over the knowledge graph)
python scripts/chat.py                          # Ollama + mistral (default)
python scripts/chat.py --backend anthropic      # Claude (needs ANTHROPIC_API_KEY)
python scripts/chat.py --ask "Quels oiseaux près de Copenhague ?"

# 8. Web chat UI (browser-based, with session history)
python scripts/web_chat.py                      # → http://localhost:5000
python scripts/web_chat.py --port 8080
```

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/build_graph.py` | Fetch eBird taxonomy + DOFbasen observations, save `output/birdology.ttl` |
| `scripts/query_graph.py` | SPARQL queries: species lookup, family/order drill-down, Danish sightings, cemetery watch |
| `scripts/visualize.py` | Interactive pyvis HTML graph + IUCN stats PNG |
| `scripts/reason.py` | Parallel pure-Python reasoner → materialised `output/birdology_reasoned.ttl` |
| `scripts/chat.py` | Graph-RAG chat — LLM queries the graph via tool-calling (Ollama / Anthropic) |
| `scripts/web_chat.py` | Flask web UI for the Graph-RAG chat with session history |

## Query examples

```bash
# Find species by name (English, Danish, French, or scientific)
python scripts/query_graph.py --species "Vindrossel"
python scripts/query_graph.py --species "Grive mauvis"
python scripts/query_graph.py --species "Turdus iliacus"

# Taxonomy browsing
python scripts/query_graph.py --order "Passeriformes"
python scripts/query_graph.py --family "Anatidae"

# Recent Danish observations
python scripts/query_graph.py --obs ""          # all
python scripts/query_graph.py --obs "Larus"     # filtered

# Rare birds near Assistens Kirkegård, Nørrebro (sorted by IUCN status)
python scripts/query_graph.py --cemetery
python scripts/query_graph.py --cemetery --radius 5.0

# Birds near any location
python scripts/query_graph.py --nearby 55.6761 12.5683

# Visualise a specific family
python scripts/visualize.py --mode graph --family "Turdidae"
python scripts/visualize.py --mode stats
```

## Tests

```bash
pytest tests/          # 55 tests, no API key or network required
```

| Test file | Coverage |
|-----------|----------|
| `test_schema.py` | OWL class/property declarations |
| `test_ingestion.py` | eBird + DOF RDF conversion, cross-source linking |
| `test_queries.py` | All SPARQL functions with an in-memory graph fixture |
| `test_reasoner.py` | Each inference rule: correctness, idempotency, parallel vs sequential |
| `test_gbif_batching.py` | Year-batching, offset-cap enforcement, deduplication (mocked HTTP) |
| `test_chat.py` | Graph-RAG tool definitions, formatting, and all 8 tools against in-memory graph |

## What the reasoner adds

Running `scripts/reason.py` materialises:

- **Transitive `bird:parentTaxon`** — direct species → order links inferred from the
  species → genus → family → order chain. After reasoning you can find all species
  in an order with a single triple pattern instead of a multi-hop traversal.
- **SubClass propagation** — every `bird:Species` instance is also typed `bird:Taxon`.
- **Domain inference** — nodes with `bird:eBirdCode` are typed `bird:Species` even
  if the type triple is missing.
- **`owl:sameAs` closure** — properties on GBIF IRIs propagate to eBird IRIs.

## Graph-RAG chat

The chat interface lets you query the knowledge graph in natural language. The LLM
has 8 tools that execute SPARQL queries and live API calls:

| Tool | Description |
|------|-------------|
| `find_species` | Search by name (English, Danish, French, scientific) |
| `species_by_family` | List species in a taxonomic family |
| `species_by_order` | List species in a taxonomic order |
| `recent_observations` | DOFbasen observations from the graph |
| `nearby_birds` | Species observed near GPS coordinates |
| `currently_present` | Species typically present in a given month |
| `taxonomy_summary` | Graph statistics (orders, families, species, observations) |
| `live_observations` | **Real-time** eBird sightings in Denmark (last 1–30 days) |

The web UI (`scripts/web_chat.py`) adds conversation history per session, automatic
retry on transient API errors, and a "new conversation" button.

Configure the LLM backend via `.env`:
```bash
LLM_BASE_URL=https://ollama.com/v1   # or http://localhost:11434/v1 for local
LLM_API_KEY=your_key
LLM_MODEL=mistral-large-3:675b
```

## Output files

| File | Description |
|------|-------------|
| `output/birdology.ttl` | Main knowledge graph (Turtle, ~8 MB) |
| `output/birdology_reasoned.ttl` | Graph with HermiT-inferred triples |
| `output/birdology_danish.html` | Interactive taxonomy browser (open in browser) |
| `output/birdology_stats.png` | IUCN conservation status distribution |

## Ontology namespaces

| Prefix | URI | Used for |
|--------|-----|---------|
| `bird:` | `https://birdology.org/ontology/` | Classes and properties |
| `taxon:` | `https://birdology.org/taxon/` | Taxon instances |
| `obs:` | `https://birdology.org/observation/` | Observation instances |
| `loc:` | `https://birdology.org/location/` | Location instances |
| `dwc:` | `http://rs.tdwg.org/dwc/terms/` | Darwin Core standard terms |
| `ebird:` | `https://ebird.org/species/` | eBird external species IRIs |
| `gbif:` | `https://www.gbif.org/species/` | GBIF external species IRIs |

## Data sources

- **eBird API v2** — Cornell Lab of Ornithology. Free API key at <https://ebird.org/api/keygen>
- **DOFbasen** — Dansk Ornitologisk Forening / BirdLife Denmark, accessed via GBIF
  dataset [`95db4db8`](https://www.gbif.org/dataset/95db4db8-f762-11e1-a439-00145eb45e9a)
