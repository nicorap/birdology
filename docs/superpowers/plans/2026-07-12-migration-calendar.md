# Migration Calendar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A full-screen "migration calendar" view — observed species as rows sorted by arrival month (the spring-in/autumn-out wave), each with a 12-month presence bar colored by migration status, plus a "this week" arriving/peaking/leaving forecast strip.

**Architecture:** A pure query function (`migration_calendar`) + a pure cyclic arrival/departure helper in `queries.py` produce per-species rows; a thin Flask route serves them as JSON; a self-contained overlay in `chat.html` (mirroring the existing `#weekend-panel` toggle pattern) renders the grid and forecasts. All month/forecast logic is month-granularity — no new external API calls.

**Tech Stack:** Python 3.13, rdflib SPARQL, Flask, pytest; vanilla HTML/CSS/JS (no framework) in `scripts/static/chat.html`.

## Global Constraints

- Run everything via `uv run` (e.g. `uv run pytest tests/test_queries.py -v`).
- TDD for the Python tasks (Tasks 1–2): failing test first, verify fail, implement, verify pass, commit.
- Commit after each task. End every commit message with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
- Instance IRIs use the `TAXON`/`OBS` namespaces; ontology terms use `BIRD`; Darwin Core uses `DWC`. Import namespaces from `birdology.namespaces` — never hardcode URIs in Python.
- Observed-species scope: a species must have `bird:hasObservation` and a `taxon/species/` IRI.
- Endpoint row shape (exact keys), one per species:
  `{"commonName": str, "scientificName": str, "thumbnail": str, "migrationStatus": str, "months": [12 bools Jan..Dec], "arrivalMonth": int|None, "departureMonth": int|None}`
- Arrival/departure rule (cyclic, month granularity):
  - `arrivalMonth` = smallest month `m∈1..12` with `m` present and `(m-1 mod 12)` absent; `None` if all 12 present or none.
  - `departureMonth` = smallest month `m∈1..12` with `m` present and `(m+1 mod 12)` absent; `None` if all 12 present or none.
- The migration status → cell color map (frontend, dark theme):
  `Resident #7dcea0`, `SummerVisitor #e8c46a`, `WinterVisitor #6a9ec0`, `PassageMigrant #5fb0a0`, `PartialMigrant #b58ad0`, fallback `#4a5568`.

---

## File Structure

- Modify `src/birdology/queries.py` — add `_month_bools`, `_arrival_departure`, `migration_calendar`.
- Modify `tests/test_queries.py` — add tests for the three new functions.
- Modify `scripts/web_chat.py` — add `GET /api/migration-calendar` route (cached, like `/api/species`).
- Modify `scripts/static/chat.html` — header button, overlay panel CSS/HTML, grid + forecast JS.

---

### Task 1: Backend data — `migration_calendar` query + helpers

**Files:**
- Modify: `src/birdology/queries.py`
- Test: `tests/test_queries.py`

**Interfaces:**
- Produces:
  - `_month_bools(months: set[int]) -> list[bool]` — 12-element list, index 0 = January.
  - `_arrival_departure(months: set[int]) -> tuple[int | None, int | None]` — per the Global Constraints rule.
  - `migration_calendar(g) -> list[dict]` — one row per observed, classified species (grouped by `scientificName` so `owl:sameAs`-split nodes merge), each row exactly the Global-Constraints shape.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_queries.py` (near the other query tests; the file already imports `Graph`, `Literal`, and the `birdology.namespaces` symbols — if a needed name like `TAXON`, `BIRD`, `DWC`, `RDF`, or `XSD` is not already imported at the top of the file, add it to the existing imports):

```python
from birdology.queries import _arrival_departure, _month_bools, migration_calendar


def test_arrival_departure_resident_is_none():
    assert _arrival_departure(set(range(1, 13))) == (None, None)


def test_arrival_departure_empty_is_none():
    assert _arrival_departure(set()) == (None, None)


def test_arrival_departure_summer_visitor():
    # Apr-Sep present
    assert _arrival_departure({4, 5, 6, 7, 8, 9}) == (4, 9)


def test_arrival_departure_winter_wraps_year():
    # Oct-Mar present (wraps December→January)
    assert _arrival_departure({10, 11, 12, 1, 2, 3}) == (10, 3)


def test_arrival_departure_passage_two_windows_picks_earliest_edges():
    # spring (Mar-May) + autumn (Sep-Oct)
    assert _arrival_departure({3, 4, 5, 9, 10}) == (3, 5)


def test_month_bools_indexes_january_at_zero():
    bools = _month_bools({1, 12})
    assert bools[0] is True and bools[11] is True
    assert bools[1] is False
    assert len(bools) == 12


def _calendar_fixture_graph():
    from rdflib import Graph, Literal
    from rdflib.namespace import RDF, XSD
    from birdology.namespaces import BIRD, DWC, TAXON
    g = Graph()

    def add_species(local, sci, status, months, common_en):
        sp = TAXON["species/" + local]
        g.add((sp, RDF.type, BIRD.Species))
        g.add((sp, DWC.scientificName, Literal(sci)))
        g.add((sp, BIRD.commonNameEn, Literal(common_en)))
        g.add((sp, BIRD.migrationStatus, Literal(status)))
        g.add((sp, BIRD.hasObservation, TAXON["obs/" + local]))
        for m in months:
            g.add((sp, BIRD.typicallyPresentInMonth, Literal(m, datatype=XSD.integer)))
        return sp

    add_species("robi", "Erithacus rubecula", "Resident", range(1, 13), "Robin")
    add_species("swift", "Apus apus", "SummerVisitor", [5, 6, 7, 8], "Common Swift")
    return g


def test_migration_calendar_row_shape_and_values():
    g = _calendar_fixture_graph()
    rows = {r["scientificName"]: r for r in migration_calendar(g)}
    assert set(rows) == {"Erithacus rubecula", "Apus apus"}

    swift = rows["Apus apus"]
    assert swift["commonName"] == "Common Swift"
    assert swift["migrationStatus"] == "SummerVisitor"
    assert swift["months"] == [False, False, False, False, True, True, True, True, False, False, False, False]
    assert swift["arrivalMonth"] == 5
    assert swift["departureMonth"] == 8
    assert set(swift) == {"commonName", "scientificName", "thumbnail", "migrationStatus", "months", "arrivalMonth", "departureMonth"}

    robin = rows["Erithacus rubecula"]
    assert robin["arrivalMonth"] is None and robin["departureMonth"] is None


def test_migration_calendar_merges_sameas_split_months():
    # Two nodes with the SAME scientificName, months split across them → one merged row.
    from rdflib import Graph, Literal
    from rdflib.namespace import RDF, XSD
    from birdology.namespaces import BIRD, DWC, TAXON
    g = Graph()
    for local, months in [("gree1", [10, 11, 12]), ("gree2", [1, 2, 3])]:
        sp = TAXON["species/" + local]
        g.add((sp, RDF.type, BIRD.Species))
        g.add((sp, DWC.scientificName, Literal("Chloris chloris")))
        g.add((sp, BIRD.migrationStatus, Literal("WinterVisitor")))
        g.add((sp, BIRD.hasObservation, TAXON["obs/" + local]))
        for m in months:
            g.add((sp, BIRD.typicallyPresentInMonth, Literal(m, datatype=XSD.integer)))
    rows = migration_calendar(g)
    assert len(rows) == 1
    assert rows[0]["months"] == [True, True, True, False, False, False, False, False, False, True, True, True]
    assert rows[0]["arrivalMonth"] == 10 and rows[0]["departureMonth"] == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_queries.py -k "arrival or month_bools or migration_calendar" -v`
Expected: FAIL — `ImportError: cannot import name '_arrival_departure'` (or `migration_calendar`).

- [ ] **Step 3: Write minimal implementation**

Add to `src/birdology/queries.py`. The module already defines `_PREFIXES` and a `_rows` helper and imports `Graph`; follow those patterns. Add:

```python
def _month_bools(months: set[int]) -> list[bool]:
    """12-element presence list, index 0 = January."""
    return [m in months for m in range(1, 13)]


def _arrival_departure(months: set[int]) -> tuple[int | None, int | None]:
    """Cyclic month-granularity arrival/departure edges.

    arrival = smallest present month whose previous month (cyclic) is absent;
    departure = smallest present month whose next month (cyclic) is absent.
    Residents (all 12) and empties have neither.
    """
    if not months or len(months) == 12:
        return (None, None)
    prev_absent = [m for m in range(1, 13) if m in months and ((m - 2) % 12 + 1) not in months]
    next_absent = [m for m in range(1, 13) if m in months and (m % 12 + 1) not in months]
    arrival = min(prev_absent) if prev_absent else None
    departure = min(next_absent) if next_absent else None
    return (arrival, departure)


def migration_calendar(g: "Graph") -> list[dict]:
    """One row per observed, classified species: month-presence + arrival/departure.

    Grouped by scientificName so owl:sameAs-split nodes merge into a single row.
    """
    q = (
        _PREFIXES
        + """
SELECT ?scientificName ?commonNameEn ?commonNameFr ?commonNameDa
       ?migrationStatus ?thumbnail ?month
WHERE {
    ?species a bird:Species ;
             dwc:scientificName    ?scientificName ;
             bird:hasObservation   ?obs ;
             bird:migrationStatus  ?migrationStatus .
    FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
    OPTIONAL { ?species bird:typicallyPresentInMonth ?month }
    OPTIONAL {
        { ?species bird:thumbnailUrl ?thumbnail }
        UNION
        { ?species owl:sameAs ?alt . ?alt bird:thumbnailUrl ?thumbnail }
    }
    OPTIONAL { ?species bird:commonNameEn ?commonNameEn }
    OPTIONAL { ?species bird:commonNameFr ?commonNameFr }
    OPTIONAL { ?species bird:commonNameDa ?commonNameDa }
}
"""
    )
    grouped: dict[str, dict] = {}
    for row in g.query(q):
        sci = str(row.scientificName)
        entry = grouped.setdefault(sci, {
            "scientificName": sci,
            "commonName": "",
            "thumbnail": "",
            "migrationStatus": str(row.migrationStatus),
            "months": set(),
        })
        if row.month is not None:
            entry["months"].add(int(str(row.month)))
        if not entry["thumbnail"] and row.thumbnail is not None:
            entry["thumbnail"] = str(row.thumbnail)
        if not entry["commonName"]:
            name = row.commonNameFr or row.commonNameEn or row.commonNameDa
            if name is not None:
                entry["commonName"] = str(name)

    out = []
    for entry in grouped.values():
        months = entry["months"]
        arrival, departure = _arrival_departure(months)
        out.append({
            "commonName": entry["commonName"] or entry["scientificName"],
            "scientificName": entry["scientificName"],
            "thumbnail": entry["thumbnail"],
            "migrationStatus": entry["migrationStatus"],
            "months": _month_bools(months),
            "arrivalMonth": arrival,
            "departureMonth": departure,
        })
    return out
```

Note: if `_PREFIXES` does not already declare the `owl:` prefix, add `PREFIX owl: <http://www.w3.org/2002/07/owl#>` to the query string (check `_PREFIXES` first; `/api/species` in `web_chat.py` relies on the same `owl:` prefix, so mirror whatever it does).

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_queries.py -k "arrival or month_bools or migration_calendar" -v`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add src/birdology/queries.py tests/test_queries.py
git commit -m "calendar: add migration_calendar query + arrival/departure helpers

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Flask route `GET /api/migration-calendar`

**Files:**
- Modify: `scripts/web_chat.py`

**Interfaces:**
- Consumes: `migration_calendar` (Task 1).
- Produces: `GET /api/migration-calendar` → JSON list of the Task-1 row dicts, cached in `_PANEL_CACHE`.

- [ ] **Step 1: Add the import**

`web_chat.py` already imports query functions from `birdology.queries` (e.g. `nearby_watch`). Add `migration_calendar` to that existing import statement. Do not add a second import line.

- [ ] **Step 2: Add the route**

Add next to the other panel routes (e.g. right after `api_species`, near `scripts/web_chat.py:557`):

```python
@app.route("/api/migration-calendar")
def api_migration_calendar():
    if "migration_calendar" not in _PANEL_CACHE:
        _PANEL_CACHE["migration_calendar"] = migration_calendar(GRAPH)
    return jsonify(_PANEL_CACHE["migration_calendar"])
```

- [ ] **Step 3: Verify against the real graph**

Start the server (port 5000 is taken by macOS AirPlay; use 5055):
```bash
uv run python scripts/web_chat.py --port 5055 --input output/birdology_reasoned.ttl &
```
Wait for "Running on", then:
```bash
curl -s http://localhost:5055/api/migration-calendar | uv run python -c "import sys,json; d=json.load(sys.stdin); print('rows:', len(d)); r=[x for x in d if x['migrationStatus']=='SummerVisitor'][0]; print(r['commonName'], r['months'], r['arrivalMonth'], r['departureMonth'])"
```
Expected: `rows:` ~190+, and a SummerVisitor row whose `months` are True across spring/summer with a non-null `arrivalMonth`. Then stop the server (`kill %1` or the PID).

- [ ] **Step 4: Run the query test suite (no regressions)**

Run: `uv run pytest tests/test_queries.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/web_chat.py
git commit -m "calendar: add /api/migration-calendar route

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Frontend — calendar overlay in `chat.html`

**Files:**
- Modify: `scripts/static/chat.html`

**Interfaces:**
- Consumes: `GET /api/migration-calendar` (Task 2); existing globals `escapeHtml`, `escapeAttr`, `askAbout`, `monthNames`.

This task has no JS unit-test harness in the repo; it is verified by running the server and loading the page (Step 5).

- [ ] **Step 1: Add the header button**

In `scripts/static/chat.html`, in the `<header>` (near `scripts/static/chat.html:461`, the `#weekend-btn` line), add a button before it:

```html
  <button id="calendar-btn">📅 Migration calendar</button>
```

- [ ] **Step 2: Add the CSS**

Add near the `#weekend-panel` styles (around `scripts/static/chat.html:185`). This defines the full-screen overlay, forecast strip, and grid:

```css
  #calendar-btn {
    background: #2d5a3d; color: #7dcea0; border: none; border-radius: 10px;
    padding: 8px 14px; font-size: 13px; cursor: pointer; margin-right: 8px;
  }
  #calendar-btn:hover { background: #3a7a50; }

  #calendar-panel {
    position: fixed; inset: 0; z-index: 2000; background: #0f1117;
    display: none; flex-direction: column; padding: 18px 22px; overflow: hidden;
  }
  #calendar-panel.visible { display: flex; }
  #calendar-panel .cal-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
  #calendar-panel h2 { font-size: 18px; color: #7dcea0; }
  .cal-close {
    background: transparent; border: 1px solid #3a3d4a; color: #a0a8c0;
    padding: 4px 12px; border-radius: 6px; cursor: pointer; font-size: 13px;
  }
  .cal-close:hover { border-color: #7dcea0; color: #7dcea0; }

  #cal-forecast { display: flex; gap: 18px; flex-wrap: wrap; margin-bottom: 14px; font-size: 12px; }
  .cal-forecast-group { background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 8px; padding: 8px 12px; }
  .cal-forecast-group .cal-fc-title { color: #7dcea0; font-weight: 600; margin-bottom: 4px; }
  .cal-forecast-group .cal-fc-names { color: #c0c8d8; }

  #cal-grid-wrap { flex: 1; overflow-y: auto; }
  .cal-grid { width: 100%; border-collapse: collapse; }
  .cal-grid th { position: sticky; top: 0; background: #0f1117; color: #8a94a8; font-size: 11px; font-weight: 500; padding: 4px 2px; text-align: center; }
  .cal-grid th.cal-name-col { text-align: left; padding-left: 8px; }
  .cal-row { cursor: pointer; }
  .cal-row:hover td { background: #171a22; }
  .cal-name { display: flex; align-items: center; gap: 8px; padding: 3px 8px; white-space: nowrap; }
  .cal-name img { width: 26px; height: 26px; border-radius: 4px; object-fit: cover; background: #252830; }
  .cal-name .cal-nm { font-size: 13px; color: #e0e0e0; }
  .cal-name .cal-sci { font-size: 11px; color: #7a8496; font-style: italic; }
  .cal-cell { width: 26px; height: 20px; border-radius: 3px; }
  .cal-cell.on { }
  td.cal-current { box-shadow: inset 1px 0 0 #7dcea0, inset -1px 0 0 #7dcea0; }
```

- [ ] **Step 3: Add the panel HTML**

Add just before the closing `</body>` (near `scripts/static/chat.html:1162`):

```html
<div id="calendar-panel">
  <div class="cal-head">
    <h2>📅 Migration calendar <span id="cal-count" style="color:#6a7a8a;font-size:13px;font-weight:400"></span></h2>
    <button class="cal-close" id="cal-close-btn">✕ Close</button>
  </div>
  <div id="cal-forecast"></div>
  <div id="cal-grid-wrap"><div class="panel-loading">Loading…</div></div>
</div>
```

- [ ] **Step 4: Add the JS**

Add inside the main `<script>` block, before `userInput.focus();` (near `scripts/static/chat.html:1160`):

```javascript
// ── Migration calendar overlay ───────────────────────────────────────────
var migColors = {
  'Resident': '#7dcea0', 'SummerVisitor': '#e8c46a', 'WinterVisitor': '#6a9ec0',
  'PassageMigrant': '#5fb0a0', 'PartialMigrant': '#b58ad0'
};
var calLoaded = false;

function cyclic(m, delta) { return ((m - 1 + delta) % 12 + 12) % 12 + 1; }

function renderCalendar(rows) {
  var now = new Date().getMonth() + 1;
  // Forecast: arriving / peaking / leaving this or next month.
  var arriving = [], leaving = [], peaking = [];
  rows.forEach(function(r) {
    var soon = [now, cyclic(now, 1)];
    if (r.arrivalMonth && soon.indexOf(r.arrivalMonth) !== -1) arriving.push(r.commonName);
    else if (r.departureMonth && soon.indexOf(r.departureMonth) !== -1) leaving.push(r.commonName);
    else if (r.migrationStatus !== 'Resident' && r.months[now - 1]) peaking.push(r.commonName);
  });
  function group(title, names) {
    if (!names.length) return '';
    return '<div class="cal-forecast-group"><div class="cal-fc-title">' + title + ' (' + names.length + ')</div>'
      + '<div class="cal-fc-names">' + names.slice(0, 6).map(escapeHtml).join(', ') + (names.length > 6 ? '…' : '') + '</div></div>';
  }
  document.getElementById('cal-forecast').innerHTML =
    group('↑ Arriving', arriving) + group('● Peaking', peaking) + group('↓ Leaving soon', leaving);

  // Sort: non-residents by arrival month, residents block at the bottom.
  var sorted = rows.slice().sort(function(a, b) {
    var aa = a.arrivalMonth || 99, bb = b.arrivalMonth || 99;
    if (aa !== bb) return aa - bb;
    return a.commonName.localeCompare(b.commonName);
  });

  var letters = ['J','F','M','A','M','J','J','A','S','O','N','D'];
  var html = '<table class="cal-grid"><thead><tr><th class="cal-name-col">Species</th>';
  for (var i = 0; i < 12; i++) html += '<th' + (i + 1 === now ? ' class="cal-current"' : '') + '>' + letters[i] + '</th>';
  html += '</tr></thead><tbody>';
  sorted.forEach(function(r) {
    var color = migColors[r.migrationStatus] || '#4a5568';
    var thumb = r.thumbnail
      ? '<img src="' + escapeAttr(r.thumbnail) + '" alt="" loading="lazy" onerror="this.style.visibility=\'hidden\'">'
      : '<img alt="" style="visibility:hidden">';
    html += '<tr class="cal-row" onclick="openCalSpecies(\'' + escapeAttr(r.commonName) + '\')">';
    html += '<td class="cal-name">' + thumb + '<span class="cal-nm">' + escapeHtml(r.commonName)
      + '</span> <span class="cal-sci">' + escapeHtml(r.scientificName) + '</span></td>';
    for (var m = 0; m < 12; m++) {
      var cur = (m + 1 === now) ? ' cal-current' : '';
      var cell = r.months[m]
        ? '<div class="cal-cell on" style="background:' + color + '"></div>'
        : '<div class="cal-cell"></div>';
      html += '<td class="' + cur.trim() + '">' + cell + '</td>';
    }
    html += '</tr>';
  });
  html += '</tbody></table>';
  document.getElementById('cal-grid-wrap').innerHTML = html;
  document.getElementById('cal-count').textContent = rows.length + ' species';
}

function openCalSpecies(name) {
  document.getElementById('calendar-panel').classList.remove('visible');
  askAbout(name);
}

function openCalendar() {
  document.getElementById('calendar-panel').classList.add('visible');
  if (calLoaded) return;
  fetch('/api/migration-calendar').then(function(r) { return r.json(); }).then(function(rows) {
    calLoaded = true;
    renderCalendar(rows);
  }).catch(function(e) {
    document.getElementById('cal-grid-wrap').innerHTML = '<div class="panel-loading">Error loading calendar</div>';
    console.error(e);
  });
}

document.getElementById('calendar-btn').addEventListener('click', openCalendar);
document.getElementById('cal-close-btn').addEventListener('click', function() {
  document.getElementById('calendar-panel').classList.remove('visible');
});
```

- [ ] **Step 5: Verify in the running app**

```bash
uv run python scripts/web_chat.py --port 5055 --input output/birdology_reasoned.ttl &
```
Wait for "Running on", then:
- `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5055/` → `200`.
- `curl -s http://localhost:5055/ | grep -c 'id="calendar-btn"'` → `1`.
- Open http://localhost:5055 in a browser, click **📅 Migration calendar**: the overlay opens, the forecast strip shows arriving/peaking/leaving for the current month, and the grid shows species rows sorted into the arrival-wave with month bars colored by status and the current month column highlighted. Clicking a row closes the overlay and asks the chat about that species. Close button hides the overlay.

Then stop the server.

- [ ] **Step 6: Commit**

```bash
git add scripts/static/chat.html
git commit -m "calendar: add full-screen migration calendar overlay with forecast strip

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage** (Feature 1 of the design doc):
- `GET /api/migration-calendar` returning the exact row shape → Task 1 (`migration_calendar`) + Task 2 (route).
- Observed-species scope, sameAs month-merge → Task 1 (group by scientificName; `test_migration_calendar_merges_sameas_split_months`).
- Cyclic arrival/departure incl. winter wrap-around → Task 1 (`_arrival_departure`; `test_arrival_departure_winter_wraps_year`).
- Full-screen view via header button, mirroring the weekend panel → Task 3 (`#calendar-btn`, `#calendar-panel.visible`).
- Rows sorted by arrival month (residents at bottom), thumbnail + name + 12 month cells colored by status, current-month highlight → Task 3 (`renderCalendar`).
- Forecast strip (arriving / peaking / leaving this or next month) → Task 3 (`renderCalendar` forecast section).
- Click row → `askAbout` + close overlay → Task 3 (`openCalSpecies`).
- Tests: arrival/departure edge cases + query row shape + sameAs merge → Task 1.

**Placeholder scan:** none — full code/commands in every step.

**Type consistency:** `_month_bools`, `_arrival_departure`, `migration_calendar` names and the row-dict keys are identical across Task 1 (Python), Task 2 (route passes rows through unchanged), and Task 3 (JS reads `commonName`, `scientificName`, `thumbnail`, `migrationStatus`, `months`, `arrivalMonth`, `departureMonth`). Frontend `migColors` keys match the migration status strings produced by `migration.py`.

**Out of scope (per spec):** week-level arrival precision, changes to the inline `<bird-phenology>` chart, Wikidata/Elton enrichment — none included.
```
