# Migration Calendar & Refresh Pipeline — Design

Date: 2026-07-12
Branch: `feat/migration-calendar-and-refresh`

## Context & motivation

DOFbasen's own web interface is dated. The north-star goal of Birdology's web UI
is to be a **beautiful, modern window onto DOF bird data**. This spec covers two
independent pieces of that next iteration:

1. **Migration calendar + forecasts** — a showcase view that makes the migration
   data (already inferred into the graph) beautiful and scannable.
2. **Refresh pipeline** — a testable orchestrator that runs the *complete* graph
   pipeline, fixing the fragility that currently loses enrichment and reasoning
   on updates.

Both are primarily for the maintainer's own use as a Copenhagen/Nørrebro birder.

---

## Feature 1 — Migration Calendar + Forecasts

### Goal

A dedicated, full-screen, beautiful view of *when* each observed species is
present in Denmark across the year, sorted so the spring-arrival / autumn-
departure migration wave is visible at a glance, with a "this week" forecast
strip on top.

Per-species phenology already exists as an inline SVG chart in chat answers
(`<bird-phenology>` tag). This feature is the **browsable multi-species view**
plus **forecasts** — it does not re-invent the single-species chart.

### Data source

All required data already lives in `output/birdology_reasoned.ttl`:
- `bird:migrationStatus` (Resident / SummerVisitor / WinterVisitor /
  PassageMigrant / PartialMigrant)
- `bird:typicallyPresentInMonth` (integer month triples 1–12)
- `bird:thumbnailUrl` (from DBpedia enrichment)
- `dwc:scientificName`, common names

Scope: **observed species only** (those with `bird:hasObservation`) — the ~198
species that actually occur in the DOF data.

### Backend: `GET /api/migration-calendar`

New Flask route in `scripts/web_chat.py`, backed by a new query function in
`src/birdology/queries.py` (e.g. `migration_calendar(graph) -> list[dict]`).

Returns one row per observed species:

```json
{
  "commonName": "Common Swift",
  "scientificName": "Apus apus",
  "thumbnail": "http://.../Apus_apus.jpg",
  "migrationStatus": "SummerVisitor",
  "months": [false,false,false,false,true,true,true,true,true,false,false,false],
  "arrivalMonth": 5,
  "departureMonth": 9
}
```

Notes:
- `months` is a 12-element boolean array indexed Jan..Dec.
- Cross-source note: a species may have `typicallyPresentInMonth` on an
  `owl:sameAs`-equivalent node; the query must collect months across the
  species' sameAs cluster (or the reasoned graph's unified node) so rows aren't
  split or empty.

### Forecast / arrival–departure logic (month granularity, no new API calls)

Computed from the `months` array:
- **arrivalMonth** = first present month immediately preceded by an absent month
  (cyclically). Residents (all 12 present) → `null`.
- **departureMonth** = last present month immediately followed by an absent
  month (cyclically). Residents → `null`.
- Winter visitors whose window wraps the year boundary (e.g. Oct–Mar) must be
  handled with cyclic (mod-12) reasoning, not naive first/last index.

"This week" strip (client-side, from the rows + current month `m`):
- **↑ Arriving** — `arrivalMonth ∈ {m, m+1}`
- **● Peaking** — present in `m`, non-resident, and not in arrival/departure edge
- **↓ Leaving soon** — `departureMonth ∈ {m, m+1}`

### Frontend (all in `scripts/static/chat.html`, no framework)

- A **📅 Migration calendar** button in the header (mirrors the existing
  "What to watch this weekend?" button).
- Clicking it opens a **full-screen overlay section** (hidden by default,
  toggled via a JS handler); a close control returns to the dashboard.
- Layout inside the overlay:
  - **Forecast strip** across the top: Arriving / Peaking / Leaving soon.
  - **Grid**: rows sorted by `arrivalMonth` ascending → the diagonal wave;
    residents grouped as a solid block at the bottom.
  - Each row: **thumbnail** + common name (+ scientific) + a 12-cell month bar.
    Filled cells colored by `migrationStatus` (reuse existing status palette /
    `migIcons`). A vertical highlight line marks the current month down the grid.
- **Interaction**: clicking a row calls the existing `askAbout(name)` hook and
  closes the overlay (asks the chat about that species).

### Testing

- `tests/test_queries.py`: `migration_calendar()` against a fixture graph
  covering each edge case — resident (arrival/departure null), summer visitor
  (contiguous window), winter visitor (wrap-around Oct–Mar), passage migrant
  (two short windows), and a species with sameAs-split month triples.
- Endpoint smoke test (existing integration-test pattern, skipped without a
  running server): `/api/migration-calendar` returns 200 and well-formed rows.

---

## Feature 2 — Refresh Pipeline

### Goal

A single **testable Python orchestrator** that runs the complete, correct graph
pipeline in order — so an update never again silently drops enrichment or
reasoning. Replaces the incomplete `daily_update.sh` logic (which only builds,
skips enrichment + reasoning, and reloads the un-reasoned graph).

### `scripts/refresh.py`

Runs these steps in order, each logged with timestamps and per-step status,
fail-fast on error:

1. `build_graph.py` — full rebuild by default; `--incremental` maps to
   `--update` (fast, observations-only).
2. `enrich_dbpedia.py` — thumbnails + range maps (the step lost today).
3. `reason.py` → `output/birdology_reasoned.ttl`.
4. `build_wiki_index.py --observed-only` — **only when `--reindex` is passed**
   (needs Ollama; slow).
5. Reload the running web server so it picks up the fresh **reasoned** graph
   (fixes today's bug where the daily script reloaded the un-reasoned file).

### Flags

- `--incremental` — obs-only `--update` instead of full rebuild.
- `--dof-max N` — passed through to `build_graph.py` (default 20000).
- `--reindex` — include the Ollama wiki-index rebuild (default off).
- `--no-reload` — skip the web-server reload.
- `--dry-run` — print the resolved step plan without executing.

### Design for testability

- The step plan is data: a function `build_plan(args) -> list[Step]` where each
  `Step` has a name and a command (argv list). The runner executes the plan.
- This separation lets unit tests assert **which steps run, in what order, with
  what arguments** for each flag combination — without invoking the real
  pipeline (no network, no subprocess). The runner is mocked in tests.
- Logging goes to `logs/refresh.log` (timestamped) and stdout.

### Wire-up

- Rewrite `daily_update.sh` to call `python scripts/refresh.py --incremental`
  (so "daily" = the full correct pipeline, just with an incremental fetch).
- Document a weekly full `refresh.py` cron entry (full rebuild + `--reindex`)
  in the script docstring / README.

### Testing

- `tests/test_refresh.py`:
  - `build_plan()` produces the expected ordered steps for: default (full),
    `--incremental`, `--reindex`, `--no-reload`, and combinations.
  - `--incremental` yields `build_graph … --update`; default yields a full
    build with `--dof-max`.
  - `--reindex` includes the wiki-index step; default omits it.
  - The runner invokes steps in order and aborts on first failure (mocked
    subprocess).

---

## Out of scope (YAGNI)

- Week-level GBIF arrival-window precision (month granularity is enough).
- Wikidata descriptions / Elton diet enrichment in the refresh pipeline (the
  current graph never had them; not needed for these features).
- Incremental *enrichment* (dbpedia re-runs over the whole graph; acceptable at
  this scale).
- Any change to the per-species inline `<bird-phenology>` chart.

## Dependencies / ordering

Feature 1 depends on the migration data from `feat/gbif-migration-and-alerts`
(this branch is based on it). The two features are otherwise independent and can
be implemented in either order.
