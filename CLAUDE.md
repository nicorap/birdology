# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (uv reads pyproject.toml; creates .venv automatically)
uv sync                              # core deps only
uv sync --extra server               # core + Flask/openai/anthropic
uv sync --extra server --extra gui --extra viz   # everything (GUI + viz)

# Run any command in the project venv
uv run pytest tests/
uv run python scripts/build_graph.py --dof-max 5000

# Add a new dependency
uv add rdflib                        # to [project.dependencies]
uv add --optional server flask       # to a specific extra
uv add --dev pytest                  # to dev group

# Run a single test file or test function
uv run pytest tests/test_queries.py
uv run pytest tests/test_ingestion.py::test_cross_source_linking

# Build the knowledge graph (requires EBIRD_API_KEY in .env)
python scripts/build_graph.py --dof-max 5000

# Build with fewer DOF records (faster for development)
python scripts/build_graph.py --dof-max 500

# Get more observations — DOF is batched by year (up to ~9 700/year)
python scripts/build_graph.py --dof-max 50000

# Incremental update (fetch only new observations since last build)
python scripts/build_graph.py --update --dof-max 5000

# Query the saved graph
python scripts/query_graph.py --summary
python scripts/query_graph.py --species "Robin"
python scripts/query_graph.py --family "Turdidae"
python scripts/query_graph.py --danish
python scripts/query_graph.py --obs "Erithacus"  # also accepts Danish/French/English names
python scripts/query_graph.py --obs "Rødhals"
python scripts/query_graph.py --order "Passeriformes"

# Run reasoner (pure-Python parallel rules — seconds, not minutes)
python scripts/reason.py
python scripts/reason.py --workers 4   # default: cpu_count
python scripts/query_graph.py --input output/birdology_reasoned.ttl --summary

# Enrichment pipelines (run after build_graph.py)
python scripts/enrich_wikidata.py   # Wikidata descriptions & links
python scripts/enrich_dbpedia.py    # DBpedia linked data
python scripts/enrich_iucn.py       # IUCN Red List conservation status
python scripts/enrich_elton.py      # EltonTraits diet & foraging strata (CC0)
python scripts/enrich_elton.py --download-only   # just cache data/EltonTraits_birds.txt
python scripts/enrich_elton.py --elton data/EltonTraits_birds.txt  # local file

# Rare/cool birds near Assistens Kirkegård, Nørrebro (hardcoded)
python scripts/query_graph.py --cemetery
python scripts/query_graph.py --cemetery --radius 5.0   # wider search

# Birds near any coordinates
python scripts/query_graph.py --nearby 55.6918 12.5559

# Visualize (interactive HTML + stats PNG → output/)
python scripts/visualize.py
python scripts/visualize.py --mode graph --family "Turdidae"
python scripts/visualize.py --mode stats
python scripts/visualize.py --mode map
python scripts/visualize.py --mode all   # graph + stats + map

# Graph-RAG chat — ask questions in natural language
python scripts/chat.py                                          # Ollama + mistral (default)
python scripts/chat.py --model llama3                           # Ollama + different model
python scripts/chat.py --backend anthropic                      # Claude (needs ANTHROPIC_API_KEY)
python scripts/chat.py --input output/birdology_reasoned.ttl    # richer migration data

# Graph-RAG web chat (opens in browser at http://localhost:5000)
python scripts/web_chat.py
python scripts/web_chat.py --port 8080
python scripts/web_chat.py --input output/birdology_reasoned.ttl

# Desktop dashboard (PySide6 GUI — wraps all CLI features)
python scripts/dashboard.py

# Evaluate the chat system (server must be running)
python scripts/eval_chat.py --url http://localhost:8080                      # 21 single-turn cases
python scripts/eval_chat.py --url http://localhost:8080 --suite conversations # multi-turn cases
python scripts/eval_chat.py --url http://localhost:8080 --suite all --judge   # everything + LLM judge
```

## Makefile — prefer these for everyday work

```bash
make            # list all targets
make serve      # web chat on PORT (default 8080; 5000 is taken by AirPlay on macOS)
make update     # incremental: new observations only, then enrich + reason
make rebuild    # full: graph + enrich + reason + wiki reindex
make reindex    # Wikipedia index alone (needs Ollama + nomic-embed-text)
make test       # offline suite
make eval-conversations   # multi-turn eval (needs a running server)
make months     # observations per calendar month — should be roughly flat
```

**Three artifacts must stay in step:** `output/birdology.ttl` (the graph),
`output/birdology_reasoned.ttl` (what the server loads), and `data/wiki_index/`
(built from the graph's **observed** species). `make rebuild` always reindexes,
because the index is derived from the graph — when the graph held only January
observations, the index silently excluded every summer migrant for months.

`make update` does not reindex; run `make rebuild` weekly so the index keeps up.
**Nothing is scheduled by default** — `scripts/daily_update.sh` exists but no cron
or launchd entry invokes it.

## Architecture

The project builds an OWL/RDF knowledge graph of birds and saves it as a Turtle file (`output/birdology.ttl`).

### Package layout (`src/birdology/`)

| File | Role |
|------|------|
| `namespaces.py` | All `rdflib.Namespace` objects. Import from here; never hardcode URIs elsewhere. |
| `schema.py` | `build_schema()` — declares OWL classes and properties into a `Graph`. The single source of truth for the ontology shape. Includes `GraphMeta` class for metadata tracking. |
| `graph.py` | `build_graph(ebird_key)` orchestrates schema + ingestion → plain `Graph`. `update_graph()` does incremental refresh (fetches only records since `lastFetchDate` metadata). `save_graph` / `load_graph` handle Turtle I/O. |
| `queries.py` | 14 reusable SPARQL functions (`find_species_by_name`, `species_by_family`, `species_by_order`, `nearby_watch`, `currently_present`, `taxonomy_summary`, etc.) that take a graph and return `list[dict]`. |
| `migration.py` | `infer_migration_status()` — classifies each observed species as Resident/SummerVisitor/WinterVisitor/PassageMigrant/PartialMigrant from DOF month data; adds `bird:migrationStatus` and `bird:typicallyPresentInMonth` triples. |
| `ingestion/ebird.py` | Calls eBird API v2 (`/ref/taxonomy/ebird`), converts records → RDF via `taxonomy_to_rdf()`. |
| `ingestion/gbif_dof.py` | Calls GBIF API for DOFbasen dataset (key `95db4db8`), converts occurrences → RDF via `occurrences_to_rdf()`. |
| `ingestion/wikidata.py` | Wikidata SPARQL enrichment — descriptions and external links. |
| `ingestion/dbpedia.py` | DBpedia linked data enrichment. |
| `ingestion/iucn.py` | IUCN Red List conservation status enrichment. |
| `ingestion/elton.py` | EltonTraits 1.0 enrichment — diet percentages and foraging strata. Downloads `data/EltonTraits_birds.txt` from figshare (CC0) on first run. |

### Key design decisions

**Namespace split**: Instance IRIs use `TAXON:`, `OBS:`, `LOC:` namespaces; ontology terms use `BIRD:`. Darwin Core (`DWC:`) is used for standard biodiversity terms (`scientificName`, `family`, `order`, `genus`).

**Cross-source linking**: eBird species get `owl:sameAs <https://ebird.org/species/{code}>`. DOF occurrences get `owl:sameAs <https://www.gbif.org/species/{gbifKey}>`. Species from both sources are connected through their shared `dwc:scientificName`.

**Species URI scheme**: eBird species → `taxon:species/{eBirdCode}`. DOF occurrence → species node at `taxon:species/sci/{slug_of_scientificName}`. When eBird data is loaded first, the DOF occurrences link to a separate node; a SPARQL reasoner or `owl:sameAs` closure unifies them.

**DOF access**: DOFbasen has no public REST API. Data is accessed via the GBIF public API (no auth required) using dataset key `95db4db8-f762-11e1-a439-00145eb45e9a`.

**GBIF offset cap**: The GBIF occurrence search API silently rejects offsets > 10 000. `fetch_dof_occurrences` works around this by iterating year-by-year (newest first), fetching up to `_GBIF_OFFSET_CAP` (9 700) records per year.

**Reasoner**: `scripts/reason.py` applies 8 inference rules in pure Python using `concurrent.futures.ProcessPoolExecutor` for the transitive `parentTaxon` closure (the expensive rule). No Java/OWL reasoner required — runs in seconds on the full 11k-species graph. Rules: (1) transitive parentTaxon closure, (2) SubClass propagation, (3) domain inference, (4) `owl:sameAs` closure, (5) migration status from observation months, (6) co-occurrence linking (species sharing 3+ location/date events get `bird:frequentlyCoOccursWith`), (7) local population trend via linear regression on yearly counts (`bird:populationTrendLocal`), (8) hotspot detection (`bird:isHotspot` + `bird:observationCount` on high-activity locations).

**Graph metadata**: Graphs store a metadata node (`taxon:_meta`) with `bird:lastFetchDate`, enabling incremental updates via `update_graph()` / `--update` flag.

**Graph-RAG chat**: Tool-based architecture (not retrieval-based). The LLM gets 8 tools that execute SPARQL queries and live eBird API calls. Backends: Ollama (OpenAI-compatible, default) and Anthropic Claude API. Web UI uses Flask with in-memory session history (2-hour TTL). Web chat also exposes `GET /api/dashboard` (rare species + hotspots) and `GET /api/weekend` (birding recommendations).

**LLM configuration** via `.env`: `LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL`.

## Tests

```
tests/test_schema.py        — OWL class/property declarations
tests/test_ingestion.py     — eBird + DOF RDF conversion, cross-source linking
tests/test_queries.py       — all SPARQL query functions with an in-memory fixture graph
tests/test_reasoner.py      — each inference rule in isolation (idempotency, correctness)
tests/test_gbif_batching.py — year-batching, offset-cap, deduplication (mocked HTTP)
tests/test_chat.py          — Graph-RAG tool definitions, formatting, all 8 tools
tests/test_eval_conversations.py — eval harness: check_answer + run_conversation (offline, mocked HTTP)
tests/test_wikidata.py      — Wikidata enrichment
tests/test_dbpedia.py       — DBpedia enrichment
tests/test_iucn.py          — IUCN enrichment
tests/test_graph_quality.py — graph validation
```

## External APIs

| API | Auth | Key env var |
|-----|------|-------------|
| eBird API v2 (`api.ebird.org/v2`) | `x-ebirdapitoken` header | `EBIRD_API_KEY` |
| GBIF (`api.gbif.org/v1`) | None (public) | — |
| Wikidata SPARQL (`query.wikidata.org`) | None (public) | — |

Get an eBird key at https://ebird.org/api/keygen (free, requires an eBird account).

## CI

GitHub Actions (`.github/workflows/tests.yml`): runs `pytest -q` on Python 3.11 / Ubuntu on push to main and PRs.
