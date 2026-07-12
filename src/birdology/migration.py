"""
Migration status inference for the Birdology knowledge graph.

For each observed species we ask the GBIF occurrence facet API in which months
that species has been recorded in the DOF dataset, then infer:
  - bird:migrationStatus  — one of: Resident, SummerVisitor, WinterVisitor,
                             PassageMigrant, PartialMigrant
  - bird:typicallyPresentInMonth  — one integer triple per month (1–12) the
                                    species is typically present in Denmark

Classification rules
--------------------
  Coverage ≥ 9 months   → Resident
  Both summer and winter → PartialMigrant
  Summer only            → SummerVisitor   (breeds here, winters elsewhere)
  Winter only            → WinterVisitor   (breeds elsewhere, winters here)
  Spring/fall only       → PassageMigrant  (passes through, neither breeds nor winters)

"Summer" (breeding core) = June–July.
"Winter" (wintering core) = December–February.

These labels apply to presence *in Denmark* specifically — a species that
breeds in Denmark and migrates south for winter is a SummerVisitor regardless
of where it goes.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import XSD
from urllib3.util.retry import Retry

from .ingestion.gbif_dof import _DOF_DATASET_KEY, _GBIF_BASE, _TIMEOUT
from .namespaces import BIRD

# GBIF rate-limits burst concurrency with HTTP 429. Keep the worker pool small
# and back off on 429/5xx (respecting Retry-After) so bulk runs don't silently
# degrade every rate-limited lookup into "Unknown".
_DEFAULT_WORKERS = 4


def _make_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1.0,                      # 1s, 2s, 4s, 8s, 16s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    session.mount("https://", adapter)
    return session


# Shared across worker threads (requests.Session is thread-safe for requests).
_SESSION = _make_session()

# Core breeding months in Denmark (birds present here must be nesting/summering)
_BREEDING = {6, 7}          # June–July
# Core wintering months (birds present here must be wintering)
_WINTERING = {12, 1, 2}     # December–February

# A month only counts as "typical presence" if it holds at least this share of
# the species' total observations. Without it, a handful of vagrant/stray
# records (e.g. a lone over-wintering Barn Swallow) would inflate month coverage
# and misclassify clear migrants as Resident.
_MONTH_PRESENCE_FRAC = 0.02


def _classify(months: set[int]) -> str:
    if not months:
        return "Unknown"
    breeds_here  = bool(months & _BREEDING)
    winters_here = bool(months & _WINTERING)
    if len(months) >= 9:
        return "Resident"
    if breeds_here and winters_here:
        return "PartialMigrant"
    if breeds_here:
        return "SummerVisitor"
    if winters_here:
        return "WinterVisitor"
    return "PassageMigrant"   # present only in passage months (spring/autumn)


def _gbif_months_for_species(scientific_name: str) -> set[int]:
    """Return the set of months (1–12) in which *scientific_name* has been
    observed in the DOF dataset, via the GBIF occurrence facet API.

    Uses ``facet=month`` with ``limit=0`` so GBIF returns per-month occurrence
    counts without transferring any records. Months contributing less than
    ``_MONTH_PRESENCE_FRAC`` of the species' total observations are dropped as
    vagrant noise. Returns an empty set on any network/parse error so a single
    flaky lookup never aborts the whole run.
    """
    params = {
        "datasetKey": _DOF_DATASET_KEY,
        "scientificName": scientific_name,
        "facet": "month",
        "facetLimit": 12,
        "limit": 0,
    }
    try:
        resp = _SESSION.get(
            f"{_GBIF_BASE}/occurrence/search", params=params, timeout=_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError):
        return set()

    counts: dict[int, int] = {}
    for facet in data.get("facets", []):
        if facet.get("field") != "MONTH":
            continue
        for entry in facet.get("counts", []):
            try:
                month = int(entry["name"])
                n = int(entry["count"])
            except (KeyError, ValueError, TypeError):
                continue
            if 1 <= month <= 12:
                counts[month] = counts.get(month, 0) + n

    total = sum(counts.values())
    if total == 0:
        return set()
    threshold = total * _MONTH_PRESENCE_FRAC
    return {month for month, n in counts.items() if n >= threshold}


def _months_from_graph(g: Graph, species: URIRef) -> set[int]:
    """Fallback: months this species was observed, taken from the observation
    dates already ingested into *g*.

    Used when the GBIF name lookup returns nothing — most often because the
    graph's (eBird) scientific name is a synonym GBIF indexes under a different
    name (e.g. Chloris chloris vs Carduelis chloris). Also covers GBIF outages.
    """
    months: set[int] = set()
    for obs in g.objects(species, BIRD.hasObservation):
        for observed_on in g.objects(obs, BIRD.observedOn):
            try:
                months.add(int(str(observed_on)[5:7]))
            except (ValueError, IndexError):
                continue
    return months


def infer_migration_status(g: Graph, max_workers: int | None = _DEFAULT_WORKERS) -> int:
    """Analyse per-species observation months and add migration triples to *g*.

    For every ``bird:Species`` that has a ``dwc:scientificName`` but no
    ``bird:migrationStatus`` yet, fetch its observed months from GBIF (in
    parallel across *max_workers* threads — the work is HTTP-bound), classify
    it, and add ``bird:migrationStatus`` + ``bird:typicallyPresentInMonth``.

    Returns the number of new triples added.
    """
    # Scope to species actually observed in Denmark — classifying migration
    # status for a taxon never recorded here is meaningless, and it keeps the
    # GBIF lookups to the ~hundreds of observed species rather than the ~11k in
    # the full eBird taxonomy.
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    PREFIX dwc: <http://rs.tdwg.org/dwc/terms/>
    SELECT DISTINCT ?species ?sci WHERE {
        ?species a bird:Species ;
                 dwc:scientificName ?sci ;
                 bird:hasObservation ?obs .
        FILTER NOT EXISTS { ?species bird:migrationStatus ?any }
    }
    """
    targets = [(URIRef(str(row.species)), str(row.sci)) for row in g.query(q)]
    if not targets:
        return 0

    def _fetch(item: tuple[URIRef, str]) -> tuple[URIRef, set[int]]:
        species, sci = item
        # Look up the fetcher via the module global at call time so tests can
        # monkeypatch birdology.migration._gbif_months_for_species.
        return species, _gbif_months_for_species(sci)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(_fetch, targets))

    added = 0
    for species, months in results:
        if not months:
            # GBIF name lookup came back empty — fall back to the observation
            # months already in the graph (handles synonyms / GBIF outages).
            months = _months_from_graph(g, species)
        status = _classify(months)
        g.add((species, BIRD.migrationStatus, Literal(status)))
        added += 1
        for m in sorted(months):
            g.add((species, BIRD.typicallyPresentInMonth, Literal(m, datatype=XSD.integer)))
            added += 1

    return added


def is_likely_present(migration_status: str, month: int) -> bool:
    """Return True if a species with *migration_status* is likely in Denmark in *month*."""
    if migration_status in ("Resident", "PartialMigrant"):
        return True
    if migration_status == "SummerVisitor":
        return month in {4, 5, 6, 7, 8, 9}    # Apr–Sep
    if migration_status == "WinterVisitor":
        return month in {10, 11, 12, 1, 2, 3}  # Oct–Mar
    if migration_status == "PassageMigrant":
        return month in {3, 4, 5, 9, 10}        # spring + autumn passage
    return True   # Unknown — assume present


def migration_label(status: str) -> str:
    """Human-readable label + emoji for a migration status string."""
    return {
        "Resident":       "🏠 Resident",
        "SummerVisitor":  "☀️  Summer visitor",
        "WinterVisitor":  "❄️  Winter visitor",
        "PassageMigrant": "🌊 Passage migrant",
        "PartialMigrant": "↔️  Partial migrant",
        "Unknown":        "❓ Unknown",
    }.get(status, status)
