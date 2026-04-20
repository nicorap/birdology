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
from birdology.ingestion.ebird import fetch_recent_denmark
from birdology.queries import (
    currently_present,
    find_species_by_name,
    nearby_watch,
    recent_danish_observations,
    species_by_family,
    species_by_order,
    taxonomy_summary,
)

load_dotenv()

DEFAULT_TTL = Path(__file__).parent.parent / "output" / "birdology.ttl"

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
                "List recent bird observations from DOFbasen (Danish field observations), "
                "sorted by date descending. Optionally filter by species name (any language). "
                "Returns date, locality, GPS, and individual count."
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
                "Fetch LIVE recent bird observations from eBird for Denmark (last 1-30 days). "
                "This queries the eBird API in real-time, so results are up-to-date (unlike the "
                "graph data which may be weeks/months old). Returns species seen recently with "
                "observation date, location name, coordinates, and count."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days back to search (1-30, default: 14)",
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
trends, or observations. If a tool returns no data, say so — do NOT fill in from general knowledge.
2. **Cite your sources.** After each fact, add the source in parentheses: \
(source: graphe Birdology), (source: eBird live), etc.
3. **Be concise.** Answer in 1-3 short paragraphs. Use a table only when comparing multiple \
species. Never write essays, projections, or advice sections.
4. **No emojis** in your responses unless the user uses them.
5. **Show photos** when available: if the tool returns a `thumbnail` field, include it as \
a markdown image: `![species name](url)`. Show max 3 photos per response.
6. **Links**: when referencing a species URI or eBird page, use markdown links: \
`[text](url)`.

## Data available in the graph
- eBird/Clements taxonomy: 45 orders, 251 families, ~11 000 species
- Names in English, Danish, French
- Danish field observations from DOFbasen (GPS, dates, counts)
- IUCN conservation status (CR/EN/VU/NT/LC)
- Migration status: Resident, SummerVisitor, WinterVisitor, PassageMigrant, PartialMigrant
- Wikidata traits: mass (g), wingspan (mm), habitat, range, diel cycle
- DBpedia: thumbnails (photos), range maps, owl:sameAs links
- Live eBird data (1-30 days) via `live_observations`

## Tool usage
- Use `live_observations` for real-time data (what's being seen NOW).
- Use graph tools for taxonomy, historical observations, species info.
- Always query tools first. Never answer from memory alone.
- Combine tools when needed (e.g. find_species + recent_observations).

## Response language
Answer in the user's language. Include scientific name + Danish name when relevant."""

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
        return _fmt(recent_danish_observations(graph, inputs.get("species")))

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
        days = inputs.get("days", 14)
        try:
            raw = fetch_recent_denmark(api_key, days=min(max(days, 1), 30))
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
        return _fmt(results, limit=40)

    if name == "taxonomy_summary":
        return json.dumps(taxonomy_summary(graph), ensure_ascii=False, indent=2)

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
