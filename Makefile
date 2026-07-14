# Birdology — common tasks.
#
# The pipeline that feeds the app has three artifacts, and they must stay in step:
#
#   output/birdology.ttl           the graph (eBird taxonomy + DOF observations)
#   output/birdology_reasoned.ttl  the graph after inference — what the server loads
#   data/wiki_index/               the Wikipedia index, built from the graph's
#                                  OBSERVED species
#
# The wiki index is derived from the graph, so `rebuild` always reindexes. Letting
# them drift is not a hypothetical: the index was once built from a graph that held
# only January observations, so every summer migrant was missing from it for months.
#
# Run `make` with no target for help.

SHELL := /bin/bash

# Port 5000 is taken by the AirPlay Receiver on macOS, so default to 8080.
PORT     ?= 8080
GRAPH    ?= output/birdology_reasoned.ttl
DOF_MAX  ?= 50000

.DEFAULT_GOAL := help

## ─── Setup ──────────────────────────────────────────────────────────────────

.PHONY: install
install: ## Install all dependencies (server + gui + viz) into .venv
	uv sync --extra server --extra gui --extra viz

## ─── Data pipeline ──────────────────────────────────────────────────────────

.PHONY: update
update: ## Incremental: fetch only new observations, then enrich + reason (fast, daily)
	uv run python scripts/refresh.py --incremental --port $(PORT)

.PHONY: rebuild
rebuild: ## Full: refetch the graph, enrich, reason, AND rebuild the wiki index
	uv run python scripts/refresh.py --dof-max $(DOF_MAX) --reindex --port $(PORT)

.PHONY: reindex
reindex: ## Rebuild the Wikipedia index alone (needs Ollama + nomic-embed-text)
	uv run python scripts/build_wiki_index.py --input $(GRAPH) --observed-only

.PHONY: reason
reason: ## Re-run the reasoner over the existing graph
	uv run python scripts/reason.py

## ─── Run ────────────────────────────────────────────────────────────────────

.PHONY: serve
serve: ## Start the web chat against the reasoned graph (PORT=8080 by default)
	uv run python scripts/web_chat.py --port $(PORT) --input $(GRAPH)

.PHONY: stop
stop: ## Stop the running web chat server
	@pkill -f web_chat.py && echo "stopped" || echo "not running"

## ─── Checks ─────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run the offline test suite (no network, no server, no LLM)
	uv run pytest -q

.PHONY: eval
eval: ## Single-turn eval against a running server (21 cases)
	uv run python scripts/eval_chat.py --url http://localhost:$(PORT)

.PHONY: eval-conversations
eval-conversations: ## Multi-turn eval — catches the bugs single-turn cases miss
	uv run python scripts/eval_chat.py --url http://localhost:$(PORT) --suite conversations

.PHONY: eval-all
eval-all: ## Every eval case, single- and multi-turn
	uv run python scripts/eval_chat.py --url http://localhost:$(PORT) --suite all

## ─── Inspect ────────────────────────────────────────────────────────────────

.PHONY: summary
summary: ## Print a summary of the reasoned graph
	uv run python scripts/query_graph.py --input $(GRAPH) --summary

.PHONY: months
months: ## Observations per calendar month — should be roughly flat
	@grep -ao 'bird:observedOn "[0-9]\{4\}-[0-9]\{2\}' output/birdology.ttl \
		| sed 's/.*-\([0-9][0-9]\)$$/\1/' | sort | uniq -c | sort -k2

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
