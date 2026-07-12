"""Tests for migration.py — pure functions only, no live HTTP."""
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD

from birdology.migration import (
    _classify,
    _gbif_months_for_species,
    infer_migration_status,
    is_likely_present,
    migration_label,
)
from birdology.namespaces import BIRD, DWC, TAXON


# ── _classify ─────────────────────────────────────────────────────────────────

def test_classify_resident_many_months():
    assert _classify({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) == "Resident"


def test_classify_resident_year_round():
    assert _classify(set(range(1, 13))) == "Resident"


def test_classify_partial_migrant_breeds_and_winters_few_months():
    # breeds (June/July) + winters (Dec–Feb) but fewer than 9 months total
    assert _classify({6, 7, 12, 1, 2}) == "PartialMigrant"


def test_classify_summer_visitor():
    assert _classify({5, 6, 7, 8}) == "SummerVisitor"


def test_classify_winter_visitor():
    assert _classify({11, 12, 1, 2}) == "WinterVisitor"


def test_classify_passage_migrant():
    # Spring only, no breeding months (June/July) and no wintering
    assert _classify({4, 5}) == "PassageMigrant"


def test_classify_empty_months():
    assert _classify(set()) == "Unknown"


# ── _gbif_months_for_species (vagrant-record filtering) ─────────────────────────

def _fake_facet_response(month_counts: dict[int, int]):
    """Fake requests.Response exposing a GBIF month-facet payload."""
    counts = [{"name": str(m), "count": n} for m, n in month_counts.items()]

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"facets": [{"field": "MONTH", "counts": counts}]}

    return _Resp()


def test_gbif_months_filters_vagrant_records():
    # Strong summer presence + a couple of stray winter records → summer only.
    month_counts = {4: 50, 5: 200, 6: 300, 7: 280, 8: 150, 9: 40, 1: 2, 12: 3}
    with patch("birdology.migration._SESSION.get",
               return_value=_fake_facet_response(month_counts)):
        months = _gbif_months_for_species("Hirundo rustica")
    assert months == {4, 5, 6, 7, 8, 9}
    assert _classify(months) == "SummerVisitor"


def test_gbif_months_keeps_year_round_presence():
    month_counts = {m: 100 for m in range(1, 13)}
    with patch("birdology.migration._SESSION.get",
               return_value=_fake_facet_response(month_counts)):
        months = _gbif_months_for_species("Erithacus rubecula")
    assert months == set(range(1, 13))


def test_gbif_months_empty_when_no_records():
    with patch("birdology.migration._SESSION.get",
               return_value=_fake_facet_response({})):
        assert _gbif_months_for_species("Nonexistent species") == set()


def test_gbif_months_empty_on_network_error():
    import requests
    with patch("birdology.migration._SESSION.get",
               side_effect=requests.exceptions.ConnectionError("boom")):
        assert _gbif_months_for_species("Erithacus rubecula") == set()


# ── is_likely_present ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("month", range(1, 13))
def test_resident_present_every_month(month):
    assert is_likely_present("Resident", month) is True


@pytest.mark.parametrize("month", range(1, 13))
def test_partial_migrant_present_every_month(month):
    assert is_likely_present("PartialMigrant", month) is True


@pytest.mark.parametrize("month", [4, 5, 6, 7, 8, 9])
def test_summer_visitor_present_in_summer(month):
    assert is_likely_present("SummerVisitor", month) is True


@pytest.mark.parametrize("month", [1, 2, 3, 10, 11, 12])
def test_summer_visitor_absent_in_winter(month):
    assert is_likely_present("SummerVisitor", month) is False


@pytest.mark.parametrize("month", [10, 11, 12, 1, 2, 3])
def test_winter_visitor_present_in_winter(month):
    assert is_likely_present("WinterVisitor", month) is True


@pytest.mark.parametrize("month", [4, 5, 6, 7, 8, 9])
def test_winter_visitor_absent_in_summer(month):
    assert is_likely_present("WinterVisitor", month) is False


@pytest.mark.parametrize("month", [3, 4, 5, 9, 10])
def test_passage_migrant_present_in_passage(month):
    assert is_likely_present("PassageMigrant", month) is True


@pytest.mark.parametrize("month", [1, 2, 6, 7, 8, 11, 12])
def test_passage_migrant_absent_off_passage(month):
    assert is_likely_present("PassageMigrant", month) is False


def test_unknown_status_assumed_present():
    assert is_likely_present("Unknown", 6) is True
    assert is_likely_present("Unknown", 1) is True


# ── migration_label ───────────────────────────────────────────────────────────

def test_migration_label_resident():
    label = migration_label("Resident")
    assert "Resident" in label


def test_migration_label_summer_visitor():
    label = migration_label("SummerVisitor")
    assert "Summer" in label


def test_migration_label_winter_visitor():
    label = migration_label("WinterVisitor")
    assert "Winter" in label


def test_migration_label_passage_migrant():
    label = migration_label("PassageMigrant")
    assert "Passage" in label


def test_migration_label_partial_migrant():
    label = migration_label("PartialMigrant")
    assert "Partial" in label


def test_migration_label_unknown():
    label = migration_label("Unknown")
    assert label  # non-empty


def test_migration_label_fallback():
    # Unrecognised status returns the status string itself
    assert migration_label("Extinct") == "Extinct"


# ── infer_migration_status ────────────────────────────────────────────────────

def _make_graph_for_migration() -> Graph:
    """Minimal graph with one observed species and no migration status yet."""
    g = Graph()
    sp = TAXON["species/robi"]
    obs = TAXON["obs/1"]
    g.add((sp, RDF.type, BIRD.Species))
    g.add((sp, DWC.scientificName, Literal("Erithacus rubecula")))
    g.add((sp, BIRD.hasObservation, obs))
    return g


def test_infer_migration_status_adds_triples():
    g = _make_graph_for_migration()
    # Simulate GBIF returning June + July → SummerVisitor
    with patch("birdology.migration._gbif_months_for_species", return_value={6, 7}):
        added = infer_migration_status(g, max_workers=1)

    assert added > 0
    statuses = list(g.objects(TAXON["species/robi"], BIRD.migrationStatus))
    assert len(statuses) == 1
    assert str(statuses[0]) == "SummerVisitor"


def test_infer_migration_status_adds_month_triples():
    g = _make_graph_for_migration()
    with patch("birdology.migration._gbif_months_for_species", return_value={6, 7, 8}):
        infer_migration_status(g, max_workers=1)

    months = {int(str(m)) for m in g.objects(TAXON["species/robi"], BIRD.typicallyPresentInMonth)}
    assert months == {6, 7, 8}


def test_infer_migration_status_skips_already_classified():
    g = _make_graph_for_migration()
    g.add((TAXON["species/robi"], BIRD.migrationStatus, Literal("Resident")))

    with patch("birdology.migration._gbif_months_for_species") as mock_fn:
        added = infer_migration_status(g, max_workers=1)

    mock_fn.assert_not_called()
    assert added == 0


def test_infer_migration_status_handles_empty_months():
    g = _make_graph_for_migration()
    with patch("birdology.migration._gbif_months_for_species", return_value=set()):
        added = infer_migration_status(g, max_workers=1)

    assert added > 0
    statuses = list(g.objects(TAXON["species/robi"], BIRD.migrationStatus))
    assert str(statuses[0]) == "Unknown"


def test_infer_migration_status_falls_back_to_graph_months():
    # GBIF name lookup returns nothing (e.g. synonym mismatch), but the graph
    # has observation dates → classify from those instead of "Unknown".
    g = Graph()
    sp = TAXON["species/gree"]
    g.add((sp, RDF.type, BIRD.Species))
    g.add((sp, DWC.scientificName, Literal("Chloris chloris")))
    for i, month in enumerate((6, 7, 8)):
        obs = TAXON[f"obs/{i}"]
        g.add((sp, BIRD.hasObservation, obs))
        g.add((obs, BIRD.observedOn, Literal(f"2025-{month:02d}-15")))

    with patch("birdology.migration._gbif_months_for_species", return_value=set()):
        infer_migration_status(g, max_workers=1)

    statuses = list(g.objects(sp, BIRD.migrationStatus))
    assert str(statuses[0]) == "SummerVisitor"
    months = {int(str(m)) for m in g.objects(sp, BIRD.typicallyPresentInMonth)}
    assert months == {6, 7, 8}
