"""
Migration status inference for the Birdology knowledge graph.

Analyses the months in which each species has been observed (from DOF records)
and infers:
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

"Summer" = April–August (breeding season).
"Winter"  = November–March.

These labels apply to presence *in Denmark* specifically — a species that
breeds in Denmark and migrates south for winter is a SummerVisitor regardless
of where it goes.
"""
from __future__ import annotations

from collections import defaultdict

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import XSD

from .namespaces import BIRD

# Core breeding months in Denmark (birds present here must be nesting/summering)
_BREEDING = {5, 6, 7}       # May–July
# Core wintering months (birds present here must be wintering)
_WINTERING = {12, 1, 2}     # December–February


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


def infer_migration_status(g: Graph) -> int:
    """Analyse observation dates and add migration triples to *g*.

    Returns the number of new triples added.
    Skips species that already have bird:migrationStatus set.
    """
    q = """
    PREFIX bird: <https://birdology.org/ontology/>
    SELECT ?species ?date WHERE {
        ?species a bird:Species ;
                 bird:hasObservation ?obs .
        ?obs bird:observedOn ?date .
        FILTER(STRSTARTS(STR(?species), "https://birdology.org/taxon/species/"))
        FILTER NOT EXISTS { ?species bird:migrationStatus ?any }
    }
    """
    months_by_species: dict[str, set[int]] = defaultdict(set)
    for row in g.query(q):
        try:
            month = int(str(row.date)[5:7])
            months_by_species[str(row.species)].add(month)
        except (IndexError, ValueError):
            pass

    added = 0
    for sp_str, months in months_by_species.items():
        sp     = URIRef(sp_str)
        status = _classify(months)
        g.add((sp, BIRD.migrationStatus, Literal(status)))
        added += 1
        for m in months:
            g.add((sp, BIRD.typicallyPresentInMonth, Literal(m, datatype=XSD.integer)))
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

