#!/usr/bin/env python3
"""
Birdology Graph-RAG — conversational interface over the knowledge graph.

Supports two backends:
  - Ollama (default, local, free) — uses the OpenAI-compatible API
  - Anthropic Claude          — requires ANTHROPIC_API_KEY

Usage:
    python scripts/chat.py                                  # Ollama + mistral
    python scripts/chat.py --model llama3                   # Ollama + different model
    python scripts/chat.py --backend anthropic              # Claude (needs API key)
    python scripts/chat.py --input output/birdology_reasoned.ttl
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.graph import load_graph
from birdology.ingestion.ebird import (
    fetch_recent_denmark,
    fetch_recent_geo,
    fetch_nearby_hotspots,
    fetch_hotspot_species,
)
from birdology.queries import (
    currently_present,
    find_species_by_name,
    migration_timing,
    nearby_watch,
    observations_by_month,
    recent_danish_observations,
    species_by_family,
    where_to_watch,
    species_by_order,
    taxonomy_summary,
)

load_dotenv()

DEFAULT_TTL = Path(__file__).parent.parent / "output" / "birdology.ttl"
_WIKI_INDEX_DIR = Path(__file__).parent.parent / "data" / "wiki_index"

# Lazy singleton — only loaded when the tool is first called
_wiki_rag = None


def _get_wiki_rag():
    global _wiki_rag
    if _wiki_rag is None:
        from birdology.wiki_rag import WikiRAG
        _wiki_rag = WikiRAG(index_dir=_WIKI_INDEX_DIR)
    return _wiki_rag

# ---------------------------------------------------------------------------
# Tool definitions (OpenAI function-calling format — works for both backends)
# ---------------------------------------------------------------------------

TOOLS_OPENAI = [
    {
        "type": "function",
        "function": {
            "name": "find_species",
            "description": (
                "Search for bird species by name. Accepts English, Danish, French, or "
                "scientific names. Case- and accent-insensitive substring match. "
                "Returns up to 50 matches with names in all languages and eBird code."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name to search for (any language or scientific name)",
                    },
                },
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "species_by_family",
            "description": (
                "List all species in a given bird family. "
                "Use scientific family names (e.g. 'Turdidae', 'Anatidae', 'Paridae')."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "family": {
                        "type": "string",
                        "description": "Family name, scientific (e.g. 'Turdidae')",
                    },
                },
                "required": ["family"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "species_by_order",
            "description": (
                "List all species in a given taxonomic order. "
                "Use scientific order names (e.g. 'Passeriformes', 'Anseriformes')."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "order": {
                        "type": "string",
                        "description": "Order name, scientific (e.g. 'Passeriformes')",
                    },
                },
                "required": ["order"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recent_observations",
            "description": (
                "List recent bird observations from the graph (DOF historical + eBird last 30 days), "
                "sorted by date descending. Optionally filter by species name (any language), "
                "locality name, date range, and/or source. Returns date, locality, GPS, and count."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "species": {
                        "type": "string",
                        "description": (
                            "Optional species name filter (English, Danish, French, or scientific). "
                            "Omit to get all recent observations."
                        ),
                    },
                    "locality": {
                        "type": "string",
                        "description": (
                            "Optional location name filter (partial match, case-insensitive). "
                            "Use this when the user asks about observations at a specific place, "
                            "e.g. 'Brøndby Strand', 'Utterslev Mose', 'Tivoli'. "
                            "Do NOT pass a location name as the 'species' parameter."
                        ),
                    },
                    "date_from": {
                        "type": "string",
                        "description": (
                            "Start date filter, ISO format YYYY-MM-DD (inclusive). "
                            "Use for 'ce mois-ci' (first day of current month), "
                            "'cette année', 'depuis janvier', etc."
                        ),
                    },
                    "date_to": {
                        "type": "string",
                        "description": (
                            "End date filter, ISO format YYYY-MM-DD (inclusive). "
                            "Combine with date_from for a date range."
                        ),
                    },
                    "source": {
                        "type": "string",
                        "enum": ["all", "ebird", "dof"],
                        "description": (
                            "Filter by data source: 'ebird' = only eBird observations (last 30 days), "
                            "'dof' = only DOFbasen historical data, 'all' = both (default)."
                        ),
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "nearby_birds",
            "description": (
                "Find bird species observed near a geographic location, sorted by IUCN rarity "
                "(CR > EN > VU > NT > LC). Defaults to Assistens Kirkegaard, Norrebro, Copenhagen "
                "if no coordinates are provided."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "lat": {
                        "type": "number",
                        "description": "Latitude in decimal degrees (e.g. 55.6918)",
                    },
                    "lon": {
                        "type": "number",
                        "description": "Longitude in decimal degrees (e.g. 12.5559)",
                    },
                    "radius_km": {
                        "type": "number",
                        "description": "Search radius in kilometres (default: 2.0)",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "currently_present",
            "description": (
                "List bird species typically present in Denmark in a given month, based on "
                "historical DOF observations. Requires the reasoned graph for best results. "
                "Defaults to the current month if not specified. "
                "Results include migration status (Resident, SummerVisitor, WinterVisitor, etc.)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "month": {
                        "type": "integer",
                        "description": "Month number 1-12. Omit for current month.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "live_observations",
            "description": (
                "Fetch LIVE recent bird observations from eBird (last 1-30 days). "
                "This queries the eBird API in real-time, so results are up-to-date. "
                "Without coordinates, returns observations for all of Denmark. "
                "With lat/lon, returns observations within radius_km of that point — "
                "use this when the user mentions a specific city or location."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days back to search (1-30, default: 14)",
                    },
                    "lat": {
                        "type": "number",
                        "description": "Latitude for geo search (e.g. 55.6761 for Copenhagen)",
                    },
                    "lon": {
                        "type": "number",
                        "description": "Longitude for geo search (e.g. 12.5683 for Copenhagen)",
                    },
                    "radius_km": {
                        "type": "integer",
                        "description": "Search radius in km when lat/lon provided (default: 25, max: 50)",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "observations_by_month",
            "description": (
                "Return bird species historically observed in Denmark during a specific month, "
                "based on DOF data. Use this for questions like 'quels oiseaux en mars ?', "
                "'which birds can I see in winter ?', 'oiseaux de printemps'. "
                "Returns species sorted by observation count with migration status."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "month": {
                        "type": "integer",
                        "description": "Month number (1=January, 12=December)",
                    },
                    "species_name": {
                        "type": "string",
                        "description": "Optional: filter by species name to check if it's present in that month",
                    },
                },
                "required": ["month"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "where_to_watch",
            "description": (
                "Return the best birdwatching hotspots near given coordinates, with the species "
                "expected to be present in a given month. Use this for questions like "
                "'où observer demain ?', 'où aller observer ce week-end ?', "
                "'meilleurs endroits pour observer des oiseaux près de Copenhague'. "
                "Returns top hotspots with distance and list of expected species."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "month": {
                        "type": "integer",
                        "description": "Month number (1-12). Defaults to current month.",
                    },
                    "lat": {
                        "type": "number",
                        "description": "Latitude of the search center (default: Copenhagen 55.676)",
                    },
                    "lon": {
                        "type": "number",
                        "description": "Longitude of the search center (default: Copenhagen 12.568)",
                    },
                    "radius_km": {
                        "type": "number",
                        "description": "Search radius in km (default: 30)",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_seasonal",
            "description": (
                "Compare what is being seen RIGHT NOW in Denmark (live eBird) against what is "
                "historically expected for the current month (DOF graph data). "
                "Returns three structured lists: species seen this week that are unexpected for "
                "the month (potential rarities), species expected for the month that are absent "
                "from live data, and species present in both (normal). "
                "Use this tool ONLY when the user asks about rarities, unexpected species, or an "
                "explicit comparison between live and historical data: "
                "'qu'est-ce qu'on voit en ce moment', 'qu'observe-t-on cette semaine', "
                "'quelles raretés', 'quoi d'inattendu', 'surprises de la semaine'. "
                "Do NOT use it for possibility/availability/news questions: 'peut voir', "
                "'peut observer', 'il y a à voir', 'quoi de neuf', 'quelles nouvelles', "
                "'que s'est-il passé' — for those, call live_observations and "
                "observations_by_month separately."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Days back for live eBird data (1-14, default: 7)",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "taxonomy_summary",
            "description": (
                "Return overall statistics for the knowledge graph: number of orders, families, "
                "genera, species, and observation records."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "live_hotspots",
            "description": (
                "Find active birdwatching hotspots near coordinates using live eBird data. "
                "Returns the top hotspots with their recent species lists. "
                "Use this when where_to_watch returns few results (large radius or sparse area), "
                "or when the user asks for spots beyond 50 km where graph data is limited. "
                "radius_km can be up to 200 km. days = how many days back to look (1-14)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "lat": {"type": "number", "description": "Latitude"},
                    "lon": {"type": "number", "description": "Longitude"},
                    "radius_km": {
                        "type": "integer",
                        "description": "Search radius in km (max 200, default 50)",
                    },
                    "days": {
                        "type": "integer",
                        "description": "How many days back to include (1-14, default 7)",
                    },
                },
                "required": ["lat", "lon"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "migration_timing",
            "description": (
                "Return arrival and departure timing for a bird species in Denmark, "
                "based on DOF historical observation data. "
                "Use this for questions like 'quand arrive l'hirondelle ?', "
                "'when does the cuckoo arrive?', 'hvornår ankommer storkene?', "
                "'quand part le rouge-gorge ?', 'depuis quand le martinet est là ?'. "
                "Returns: presence months, typical arrival month, typical departure month, "
                "earliest ever recorded date, and yearly first/last observation dates."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "species_name": {
                        "type": "string",
                        "description": "Species name in any language or scientific name",
                    },
                },
                "required": ["species_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_wikipedia",
            "description": (
                "Search Wikipedia articles about bird species for detailed information on "
                "behavior, habitat, diet, courtship, song, breeding, migration, and ecology. "
                "Use this when the user asks about how a bird behaves, where it lives, "
                "what it eats, how it sings, or any natural history question."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "Natural language query, e.g. 'European Robin courtship behavior', "
                            "'Barn Swallow migration route', 'Great Tit song description'"
                        ),
                    },
                },
                "required": ["query"],
            },
        },
    },
]

# Anthropic format (converted from OpenAI format)
TOOLS_ANTHROPIC = [
    {
        "name": t["function"]["name"],
        "description": t["function"]["description"],
        "input_schema": t["function"]["parameters"],
    }
    for t in TOOLS_OPENAI
]

# Re-export for tests
TOOLS = TOOLS_ANTHROPIC

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are Birdology, an ornithologist assistant powered by a knowledge graph (OWL/RDF).

## STRICT RULES — follow these exactly

1. **ONLY report data returned by your tools.** Never invent dates, statistics, population \
trends, observations, or localities. **If a tool returns no data, say exactly that** \
("I don't have this information in my data." / "Je n'ai pas cette information dans mes données." — match the user's language) and stop — do NOT fill in from general \
knowledge, do NOT suggest alternatives, do NOT describe the species from memory. \
If you are unsure which tool to use or whether you have the right data, say so and ask the \
user to clarify rather than guessing.
2. **Cite your sources.** After each fact, add the source in parentheses: \
(source: graphe Birdology), (source: eBird live), etc.
3. **Be concise.** Answer in 1-3 short paragraphs or a table. Never write essays, projections, \
or advice sections. **For `compare_seasonal` results: list at most 5 species per category \
(unexpected / normal), picking the most notable ones (rarest, highest count, or atypical). \
Do not dump the full list.**
4. **Flag atypical observations.** If a tool result contains an `atypicalReason` field, \
mention it explicitly: e.g. "(⚠ atypique : hors-saison)" or "(⚠ atypique : espèce très rare \
localement)". Do not omit this information.
5. **No padding.** Do NOT add identification tips, confusion species, validation advice, \
"what to watch for", or any editorial commentary that was not asked. Do NOT invent sections \
like "Recommandations", "À vérifier", "À noter", "À surveiller", "Remarque", "Remarque finale". \
Just report the tool results.
6. **No emojis** in your responses unless the user uses them.
7. **Show photos ONLY from tool results.** If a tool returns a `thumbnail` field, include it \
as a markdown image: `![species name](url)`. Show max 3 photos per response. \
**NEVER write a markdown image (`![...]()`) from memory or general knowledge** — only use \
URLs that appear literally in a tool result.
8. **Links**: when referencing a species URI or eBird page, use markdown links: \
`[text](url)`.
9. **When comparing live vs historical data**, only list species as "expected but absent" if \
they appear in the tool result for `observations_by_month` or `currently_present`. \
Do NOT add species from memory.

## Data available in the graph
- eBird/Clements taxonomy: 45 orders, 251 families, ~11 000 species
- Names in English, Danish, French
- Danish field observations from DOFbasen (GPS, dates, counts)
- IUCN conservation status (CR/EN/VU/NT/LC)
- Migration status: Resident, SummerVisitor, WinterVisitor, PassageMigrant, PartialMigrant
- Wikidata traits: mass (g), wingspan (mm), habitat, range, diel cycle
- DBpedia: thumbnails (photos), range maps, owl:sameAs links
- Live eBird data (1-30 days) via `live_observations`
- Wikipedia articles (behavior, habitat, song, breeding, ecology) via `search_wikipedia`

## Tool usage
- **ALWAYS call `find_species` first** when the user mentions a bird name (in any language). \
Never guess the scientific name or English name from memory — always verify via tool. \
**When calling `search_wikipedia` after `find_species`, always use the `commonNameEn` field \
returned by `find_species` as the query — never translate the French/Danish name yourself.**
- The `find_species` tool accepts French, Danish, English, and scientific names. \
Pass the exact name the user gave — do not translate it yourself before searching.
- If `find_species` returns no match, say so. Do NOT substitute a different species.
- For questions about observations, use `recent_observations`. \
**Always use `source="all"` by default** — omitting eBird data causes missing results. \
Only use `source="ebird"` or `source="dof"` when the user explicitly asks for one source. \
**When the user mentions a place name** (e.g. "Brøndby Strand", "Utterslev Mose"), pass it as \
`locality` — NEVER as `species`. Location names are never species names. \
**When the user mentions a time period**, pass `date_from` and/or `date_to` in YYYY-MM-DD format. \
Today's date is in the system context — use it to compute relative dates precisely: \
- "ce mois-ci" → `date_from` = first day of current month, `date_to` = today \
- "cette année" → `date_from` = January 1st of current year, `date_to` = today \
- "en avril" (no year) → `date_from="YYYY-04-01"`, `date_to="YYYY-04-30"` using current year \
- "en avril 2024" → `date_from="2024-04-01"`, `date_to="2024-04-30"` \
- "les 15 derniers jours" → `date_from` = today minus 15 days, `date_to` = today \
- "l'hiver dernier" → `date_from="YYYY-12-01"`, `date_to="YYYY-02-28"` (adjust year) \
- "au printemps" → `date_from="YYYY-03-01"`, `date_to="YYYY-05-31"`
- **Use `compare_seasonal`** ONLY when the user explicitly asks for rarities, unexpected species, \
or a comparison: "qu'est-ce qu'on voit en ce moment" / "qu'observe-t-on cette semaine" / \
"quelles raretés" / "quoi d'inattendu" / "surprises". \
It returns unexpected_in_live (potential rarities), expected_but_absent, and normal_present. \
Report at most 5 species per category — do NOT dump the full list.
- **When the user asks what one CAN or MIGHT see, what there IS to see, or what is NEW in \
observations** ("qu'est-ce qu'on **peut** voir", "que peut-on observer", \
"qu'est-ce qu'**il y a** à voir", "**quoi de neuf** dans les observations", \
"que s'est-il passé", "que peut-on observer cette semaine"), \
call **both** `live_observations` (days=7) **and** \
`observations_by_month` (current month, NOT `currently_present`) to give live + historical context. \
Report only what the tools return — do NOT add species from memory.
- Use `live_observations` for recent live sightings, or when following up on a specific species \
in a live context (e.g. "pas d'aigle royal ?" after a live mention → `live_observations`). \
**When the user mentions a city or place** (e.g. "près de Copenhague", "autour d'Aarhus"), \
pass `lat`/`lon` coordinates for that place to `live_observations` — do NOT omit them.
- **ALWAYS call `search_wikipedia`** when the user asks about behavior (comportement), habitat, \
song (chant), courtship, diet (régime alimentaire), or ecology of a specific species — \
**call it even if you did NOT call `find_species` first** (e.g. "décris le comportement du \
rouge-gorge" → call `search_wikipedia("European Robin")` directly). \
Call it at most once per question — if it returns no results, say the Wikipedia index does not \
yet cover this species, and answer from your own knowledge if you can, clearly labeling it as such. \
**Always write `search_wikipedia` queries in English** (use the English common name or scientific \
name). The Wikipedia index is in English — French queries reduce recall.
- **Use `migration_timing`** for questions about when a species arrives or departs: \
"quand arrive l'hirondelle ?", "when does the cuckoo arrive?", "hvornår ankommer storkene?", \
"quand part le martinet ?", "depuis quand est là le rouge-gorge ?". \
Always call `find_species` first to resolve the name, then call `migration_timing` with the \
resolved scientific name. Present the result as: arrival month, departure month, months present, \
and (if available) the earliest date ever recorded and a table of yearly first/last dates.
- Use `observations_by_month` for questions about which birds are present in a specific month \
("oiseaux en mars", "birds in winter", etc.).
- Use `where_to_watch` for questions about where to go birdwatching ("où observer demain ?", \
"meilleurs endroits près de Copenhague"). Pass `month` and coordinates if the user provides them. \
**If the user says "près de chez moi", "near me", or similar without giving a place name or \
coordinates, ask them for their location before calling any tool.** \
**When the user says "un peu plus loin" or "encore plus loin" after a previous `where_to_watch` \
call, use `live_hotspots` with a larger radius (double the previous, e.g. 50→100→200 km). \
`live_hotspots` uses live eBird data and covers any distance — prefer it over `where_to_watch` \
for radius > 50 km. Always call a tool — never answer from memory.** \
**CRITICAL: only list locations and species that `where_to_watch` actually returned. \
Never add locations or species from your own knowledge — the tool enforces the radius. \
Never invent "Observation récente" details — only mention dates/counts explicitly present \
in the tool result. If the tool returns no recent observations for a site, say nothing.**
- **Use `nearby_birds`** when the user asks for **rare or threatened species near a location** \
("espèces rares près de Copenhague", "oiseaux menacés autour d'Aarhus"). \
It sorts by IUCN conservation status (CR > EN > VU > NT > LC). \
Do NOT use `live_observations` for rarity-based queries — it has no IUCN filter. \
**If no location is given, ask the user where they are — do NOT default to Copenhagen.**
- Use graph tools (`species_by_family`, `currently_present`, etc.) for taxonomy and historical data.

## Response language
**Always reply in the same language the user wrote in.** Detect automatically:
- French question → French answer
- English question → English answer
- Danish question → Danish answer
Do NOT mix languages in the same response. For every species mentioned, always include:
- scientific name (*italics*)
- French common name if available
- English common name if available
- Danish common name if available
Format: **Common name** (*Genus species* · fr: Nom français · en: English name · da: Dansk navn)
Omit a language tag only if that name is genuinely missing from the tool result.

## Examples of correct responses

**User:** "Qu'est-ce qu'on **peut** voir en ce moment au Danemark ?" / "que peut-on observer cette semaine ?" / "qu'est-ce qu'**il y a** à voir en ce moment ?" / "quoi de neuf dans les observations ?"
→ tools: live_observations({"days": 7}), observations_by_month({"month": <current_month>})
→ Correct response:
"Voici ce qu'on observe actuellement (source: eBird live) : [liste live]
Espèces typiques pour [mois] (source: graphe Birdology) : [liste DOF]"

**User:** "Qu'est-ce qu'on **voit** en ce moment au Danemark ?" / "quelles raretés cette semaine ?" / "quoi d'inattendu ?" / "surprises de la semaine ?"
→ tool: compare_seasonal({"days": 7})
→ Correct response:
"Voici le bilan de cette semaine au Danemark (source: eBird live + DOF mai) :
**Inattendus pour mai (raretés potentielles) :** [liste exacte du champ unexpected_in_live]
**Présents normalement :** [liste exacte du champ normal_present]"

**User:** "Quelles observations dans les 30 derniers jours ?" / "observations récentes au Danemark ?" / "qu'a-t-on vu cette semaine ?"
→ tool: live_observations({"days": 30}) or live_observations({"days": 7})
→ Correct response:
"Voici les espèces récemment observées au Danemark (source: eBird live) :
| Espèce | Localité | Date | N |
|--------|----------|------|---|
| Pygargue à queue blanche | Tranekær | 2026-05-09 | 1 |
| Oie rieuse | Gundsømagle Sø | 2026-05-08 | 93 |
Limité aux 15 premières observations retournées par l'outil."

**User:** "Parle-moi du rouge-gorge" / "décris le comportement du rouge-gorge" / "que mange la mésange bleue ?" / "comment chante le merle ?"
→ tools: find_species("rouge-gorge"), search_wikipedia("European Robin")
  [For behavior/diet/song questions: search_wikipedia is MANDATORY even without find_species]
→ Correct response:
"Le Rouge-gorge familier (*Erithacus rubecula*, Rødhals) est un résident permanent au Danemark, \
observé 1 243 fois dans le graphe (source: graphe Birdology). Il fréquente les forêts, haies \
et jardins, chante dès l'aube et est territorial (source: Wikipedia)."

❌ NEVER add sections titled "Précautions", "À vérifier", "Recommandations", "Espèces absentes", \
"Espèces confondues", "À surveiller", "À noter", "Remarque", "Conseil", identification tips, \
validation advice, or any commentary not explicitly requested by the user. \
**If a tool returns no data or an empty list, say "Je n'ai pas de données pour cette requête." \
and stop — do NOT fill in from memory, do NOT suggest alternatives, do NOT list places or \
species you know about.** Report only what the tools returned."""

# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------


def _fmt(rows: list[dict], limit: int = 25) -> str:
    """Serialise query results as compact JSON for the LLM."""
    if not rows:
        return "No results found."
    shown = rows[:limit]
    out = json.dumps(shown, ensure_ascii=False, indent=2)
    if len(rows) > limit:
        out += f"\n\n… and {len(rows) - limit} more results not shown."
    return out


def _run_tool(name: str, inputs: dict, graph) -> str:
    if name == "find_species":
        return _fmt(find_species_by_name(graph, inputs["name"]))

    if name == "species_by_family":
        return _fmt(species_by_family(graph, inputs["family"]))

    if name == "species_by_order":
        return _fmt(species_by_order(graph, inputs["order"]))

    if name == "recent_observations":
        ebird_only = inputs.get("source", "all") == "ebird"
        return _fmt(recent_danish_observations(
            graph,
            inputs.get("species"),
            ebird_only=ebird_only,
            locality=inputs.get("locality"),
            date_from=inputs.get("date_from"),
            date_to=inputs.get("date_to"),
        ))

    if name == "nearby_birds":
        kwargs: dict = {}
        if "lat" in inputs:
            kwargs["lat"] = float(inputs["lat"])
        if "lon" in inputs:
            kwargs["lon"] = float(inputs["lon"])
        if "radius_km" in inputs:
            kwargs["radius_km"] = float(inputs["radius_km"])
        return _fmt(nearby_watch(graph, **kwargs))

    if name == "currently_present":
        return _fmt(currently_present(graph, inputs.get("month")))

    if name == "live_observations":
        api_key = os.getenv("EBIRD_API_KEY", "")
        if not api_key:
            return "Error: EBIRD_API_KEY not set in .env — cannot query eBird live API."
        days = min(max(int(inputs.get("days", 14)), 1), 30)
        lat = inputs.get("lat")
        lon = inputs.get("lon")
        radius_km = int(inputs.get("radius_km", 25))
        try:
            if lat is not None and lon is not None:
                raw = fetch_recent_geo(api_key, float(lat), float(lon),
                                       days=days, radius_km=radius_km)
            else:
                raw = fetch_recent_denmark(api_key, days=days)
        except Exception as e:
            return f"eBird API error: {e}"
        results = [
            {
                "species": r.get("comName", ""),
                "sciName": r.get("sciName", ""),
                "date": r.get("obsDt", ""),
                "location": r.get("locName", ""),
                "lat": r.get("lat"),
                "lon": r.get("lng"),
                "count": r.get("howMany", "X"),
            }
            for r in raw
        ]
        return _fmt(results, limit=15)

    if name == "compare_seasonal":
        import datetime as _dt
        api_key = os.getenv("EBIRD_API_KEY", "")
        if not api_key:
            return "Error: EBIRD_API_KEY not set — cannot query eBird live API."
        days = min(max(int(inputs.get("days", 7)), 1), 14)
        month = _dt.date.today().month
        # Fetch live data
        try:
            raw_live = fetch_recent_denmark(api_key, days=days)
        except Exception as e:
            return f"eBird API error: {e}"
        live_sci = {r["sciName"] for r in raw_live if r.get("sciName")}
        live_by_sci = {}
        for r in raw_live:
            sci = r.get("sciName", "")
            if sci and sci not in live_by_sci:
                live_by_sci[sci] = {
                    "species": r.get("comName", ""),
                    "sciName": sci,
                    "location": r.get("locName", ""),
                    "date": r.get("obsDt", ""),
                    "count": r.get("howMany", "X"),
                }
        # Fetch historical monthly data
        historical = observations_by_month(graph, month)
        hist_sci = {r["sciName"] for r in historical if r.get("sciName")}
        # Compute diff
        unexpected = [live_by_sci[s] for s in live_sci - hist_sci if s in live_by_sci]
        normal = [live_by_sci[s] for s in live_sci & hist_sci if s in live_by_sci]
        result = {
            "month": month,
            "days_checked": days,
            "live_species_count": len(live_sci),
            "unexpected_in_live": unexpected[:5],    # in live, not in history → potential rarities
            "normal_present": normal[:5],            # in both → business as usual
        }
        return json.dumps(result, ensure_ascii=False, indent=2)

    if name == "migration_timing":
        result = migration_timing(graph, inputs["species_name"])
        if not result:
            return json.dumps({"error": "Species not found or no timing data available"})
        return json.dumps(result, ensure_ascii=False, indent=2)

    if name == "observations_by_month":
        month = int(inputs["month"])
        species_name = inputs.get("species_name")
        return _fmt(observations_by_month(graph, month, species_name))

    if name == "where_to_watch":
        kwargs: dict = {}
        if "month" in inputs:
            kwargs["month"] = int(inputs["month"])
        if "lat" in inputs:
            kwargs["lat"] = float(inputs["lat"])
        if "lon" in inputs:
            kwargs["lon"] = float(inputs["lon"])
        if "radius_km" in inputs:
            kwargs["radius_km"] = float(inputs["radius_km"])
        return _fmt(where_to_watch(graph, **kwargs), limit=10)

    if name == "live_hotspots":
        api_key = os.getenv("EBIRD_API_KEY", "")
        if not api_key:
            return json.dumps({"error": "EBIRD_API_KEY not set"})
        lat = float(inputs["lat"])
        lon = float(inputs["lon"])
        radius_km = min(int(inputs.get("radius_km", 50)), 200)
        days = min(int(inputs.get("days", 7)), 14)
        hotspots = fetch_nearby_hotspots(api_key, lat, lon, radius_km)
        # Sort by most recently active, take top 8
        hotspots = sorted(
            hotspots,
            key=lambda h: h.get("latestObsDt") or "",
            reverse=True,
        )[:8]
        results = []
        for h in hotspots:
            try:
                species = fetch_hotspot_species(api_key, h["locId"], days)
            except Exception:
                species = []
            results.append({
                "name": h.get("locName", ""),
                "locId": h.get("locId", ""),
                "lat": h.get("lat"),
                "lon": h.get("lng"),
                "latestObsDate": h.get("latestObsDt"),
                "allTimeSpeciesCount": h.get("numSpeciesAllTime"),
                "recentSpecies": [
                    {
                        "commonName": s.get("comName", ""),
                        "scientificName": s.get("sciName", ""),
                        "count": s.get("howMany"),
                        "date": s.get("obsDt", ""),
                    }
                    for s in species[:20]
                ],
            })
        return json.dumps(results, ensure_ascii=False, indent=2)

    if name == "taxonomy_summary":
        return json.dumps(taxonomy_summary(graph), ensure_ascii=False, indent=2)

    if name == "search_wikipedia":
        rag = _get_wiki_rag()
        if not rag.is_built():
            return (
                "Wikipedia index not built yet. "
                "Run: python scripts/build_wiki_index.py"
            )
        results = rag.search(inputs["query"], n_results=4)
        if not results:
            return "No Wikipedia results found for this query."
        return json.dumps(results, ensure_ascii=False, indent=2)

    return f"Unknown tool: {name}"


# ---------------------------------------------------------------------------
# Ollama backend (OpenAI-compatible API)
# ---------------------------------------------------------------------------

def _ask_ollama(graph, model: str, base_url: str, question: str, api_key: str = "ollama") -> str:
    """Single-shot: ask one question and return the final answer text."""
    from openai import OpenAI

    client = OpenAI(base_url=base_url, api_key=api_key)
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": question},
    ]

    max_tool_rounds = 5
    for _ in range(max_tool_rounds):
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOLS_OPENAI,
        )
        msg = response.choices[0].message
        messages.append(msg.model_dump(exclude_none=True))

        if not msg.tool_calls:
            return msg.content or ""

        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                fn_args = {}
            print(f"  [tool: {fn_name}({json.dumps(fn_args, ensure_ascii=False)})]")
            result = _run_tool(fn_name, fn_args, graph)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    return "(max tool rounds reached)"


def _chat_ollama(graph, model: str, base_url: str, api_key: str = "ollama") -> None:
    from openai import OpenAI

    client = OpenAI(base_url=base_url, api_key=api_key)
    messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]

    print(f"Birdology Graph-RAG  [Ollama — {model}]")
    print(f"Endpoint: {base_url}")
    print("Type 'quit' to exit.\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not user_input:
            continue
        if user_input.lower() in {"quit", "exit", "bye", "q"}:
            print("Bye!")
            break

        messages.append({"role": "user", "content": user_input})

        # Agentic loop
        max_tool_rounds = 5
        for _ in range(max_tool_rounds):
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                tools=TOOLS_OPENAI,
            )

            choice = response.choices[0]
            msg = choice.message

            # Append assistant message
            messages.append(msg.model_dump(exclude_none=True))

            if not msg.tool_calls:
                # Final text response
                if msg.content:
                    print(f"\nAssistant: {msg.content}\n")
                break

            # Execute tool calls
            for tc in msg.tool_calls:
                fn_name = tc.function.name
                try:
                    fn_args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    fn_args = {}

                print(f"  [tool: {fn_name}({json.dumps(fn_args, ensure_ascii=False)})]")
                result = _run_tool(fn_name, fn_args, graph)

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
        else:
            print("\nAssistant: (max tool rounds reached)\n")


# ---------------------------------------------------------------------------
# Anthropic backend
# ---------------------------------------------------------------------------

def _chat_anthropic(graph, model: str) -> None:
    import anthropic

    client = anthropic.Anthropic()
    messages: list[dict] = []

    print(f"Birdology Graph-RAG  [Anthropic — {model}]")
    print("Type 'quit' to exit.\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not user_input:
            continue
        if user_input.lower() in {"quit", "exit", "bye", "q"}:
            print("Bye!")
            break

        messages.append({"role": "user", "content": user_input})

        while True:
            with client.messages.stream(
                model=model,
                max_tokens=4096,
                thinking={"type": "adaptive"},
                system=SYSTEM_PROMPT,
                tools=TOOLS_ANTHROPIC,
                messages=messages,
            ) as stream:
                response = stream.get_final_message()

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                for block in response.content:
                    if block.type == "text":
                        print(f"\nAssistant: {block.text}\n")
                break

            if response.stop_reason == "tool_use":
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        print(
                            f"  [tool: {block.name}"
                            f"({json.dumps(block.input, ensure_ascii=False)})]"
                        )
                        result = _run_tool(block.name, block.input, graph)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })
                messages.append({"role": "user", "content": tool_results})
            else:
                for block in response.content:
                    if block.type == "text":
                        print(f"\nAssistant: {block.text}\n")
                break


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Birdology Graph-RAG — chat with the knowledge graph"
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_TTL),
        metavar="PATH",
        help="Turtle graph file to load (default: output/birdology.ttl)",
    )
    parser.add_argument(
        "--backend",
        choices=["ollama", "anthropic"],
        default="ollama",
        help="LLM backend (default: ollama)",
    )
    parser.add_argument(
        "--model",
        default=None,
        metavar="MODEL",
        help="Model name (default: mistral for Ollama, claude-opus-4-6 for Anthropic)",
    )
    parser.add_argument(
        "--ollama-url",
        default=os.getenv("LLM_BASE_URL", "http://localhost:11434/v1"),
        metavar="URL",
        help="Ollama API base URL (default: LLM_BASE_URL env var or http://localhost:11434/v1)",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        metavar="KEY",
        help="API key for the OpenAI-compatible endpoint (default: 'ollama' for local, or LLM_API_KEY env var)",
    )
    parser.add_argument(
        "--ask",
        default=None,
        metavar="QUESTION",
        help="Single-shot mode: ask one question and exit (non-interactive)",
    )
    args = parser.parse_args()

    # Resolve model default per backend
    if args.model is None:
        env_model = os.getenv("LLM_MODEL")
        if args.backend == "ollama" and env_model:
            args.model = env_model
        elif args.backend == "ollama":
            args.model = "mistral"
        else:
            args.model = "claude-opus-4-6"

    # Resolve API key: explicit flag > env var > "ollama" default
    if args.api_key is None:
        args.api_key = os.getenv("LLM_API_KEY", "ollama")

    if args.backend == "anthropic" and not os.getenv("ANTHROPIC_API_KEY"):
        print(
            "Error: ANTHROPIC_API_KEY is not set.\n"
            "Add it to your .env file or export it in your shell.\n"
            "Or use --backend ollama (default) for local inference.",
            file=sys.stderr,
        )
        sys.exit(1)

    ttl_path = Path(args.input)
    if not ttl_path.exists():
        print(f"Error: graph file not found: {ttl_path}", file=sys.stderr)
        print(
            "Build it first with: python scripts/build_graph.py --dof-max 5000",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Loading graph from {ttl_path} …")
    graph = load_graph(ttl_path)
    print()

    if args.ask:
        if args.backend == "ollama":
            answer = _ask_ollama(graph, args.model, args.ollama_url, args.ask, args.api_key)
        else:
            print("Single-shot mode not supported for Anthropic backend yet.")
            sys.exit(1)
        print(answer)
    elif args.backend == "ollama":
        _chat_ollama(graph, args.model, args.ollama_url, args.api_key)
    else:
        _chat_anthropic(graph, args.model)


if __name__ == "__main__":
    main()
