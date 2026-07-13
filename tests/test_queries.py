"""Tests for SPARQL query functions — no live API or file I/O."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, XSD

from birdology.namespaces import BIRD, DWC, OBS, LOC, TAXON
from birdology.queries import (
    _arrival_departure,
    _month_bools,
    find_species_by_name,
    list_danish_species,
    migration_calendar,
    migration_timing,
    nearby_watch,
    observation_locations,
    phenology,
    recent_danish_observations,
    species_by_family,
    species_by_order,
    taxonomy_summary,
)


# ── Minimal graph fixture ─────────────────────────────────────────────────────

def _make_graph() -> Graph:
    """
    Build a small but realistic graph with two species, one observation each.

    Species:
      - Erithacus rubecula (Robin / Rødhals / Rouge-gorge) — LC — Passeriformes / Muscicapidae
      - Dendrocopos major (Great Spotted Woodpecker) — LC — Piciformes / Picidae
    """
    g = Graph()
    g.bind("bird", BIRD)
    g.bind("dwc", DWC)

    ROBIN_URI = TAXON["species/robi"]
    WOODP_URI = TAXON["species/euwoo1"]
    OBS_ROBIN = OBS["obs_robin"]
    OBS_WOODP = OBS["obs_woodp"]
    LOC_CPH   = LOC["loc_cph"]
    LOC_AARHUS = LOC["loc_aarhus"]

    GENUS_ERITHACUS  = TAXON["genus/Erithacus"]
    GENUS_DENDROCOPOS = TAXON["genus/Dendrocopos"]
    FAM_MUSC  = TAXON["family/Muscicapidae"]
    FAM_PIC   = TAXON["family/Picidae"]
    ORD_PASS  = TAXON["order/Passeriformes"]
    ORD_PIC   = TAXON["order/Piciformes"]

    # ── Taxonomy hierarchy ────────────────────────────────────────────────────
    for uri, cls in [
        (ORD_PASS, BIRD.Order), (ORD_PIC, BIRD.Order),
        (FAM_MUSC, BIRD.Family), (FAM_PIC, BIRD.Family),
        (GENUS_ERITHACUS, BIRD.Genus), (GENUS_DENDROCOPOS, BIRD.Genus),
        (ROBIN_URI, BIRD.Species), (WOODP_URI, BIRD.Species),
    ]:
        g.add((uri, RDF.type, cls))

    g.add((GENUS_ERITHACUS,   BIRD.parentTaxon, FAM_MUSC))
    g.add((GENUS_DENDROCOPOS, BIRD.parentTaxon, FAM_PIC))
    g.add((FAM_MUSC, BIRD.parentTaxon, ORD_PASS))
    g.add((FAM_PIC,  BIRD.parentTaxon, ORD_PIC))
    g.add((ROBIN_URI, BIRD.parentTaxon, GENUS_ERITHACUS))
    g.add((WOODP_URI, BIRD.parentTaxon, GENUS_DENDROCOPOS))

    # ── Species properties ────────────────────────────────────────────────────
    g.add((ROBIN_URI, DWC.scientificName, Literal("Erithacus rubecula")))
    g.add((ROBIN_URI, DWC.family,         Literal("Muscicapidae")))
    g.add((ROBIN_URI, DWC.order,          Literal("Passeriformes")))
    g.add((ROBIN_URI, BIRD.commonNameEn,  Literal("European Robin", lang="en")))
    g.add((ROBIN_URI, BIRD.commonNameDa,  Literal("Rødhals", lang="da")))
    g.add((ROBIN_URI, BIRD.commonNameFr,  Literal("Rouge-gorge familier", lang="fr")))
    g.add((ROBIN_URI, BIRD.eBirdCode,     Literal("robi")))
    g.add((ROBIN_URI, BIRD.conservationStatus, Literal("LC")))
    g.add((ROBIN_URI, BIRD.thumbnailUrl,  Literal("https://example.org/robin.jpg?width=300")))

    g.add((WOODP_URI, DWC.scientificName, Literal("Dendrocopos major")))
    g.add((WOODP_URI, DWC.family,         Literal("Picidae")))
    g.add((WOODP_URI, DWC.order,          Literal("Piciformes")))
    g.add((WOODP_URI, BIRD.commonNameEn,  Literal("Great Spotted Woodpecker", lang="en")))
    g.add((WOODP_URI, BIRD.commonNameDa,  Literal("Stor Flagspætte", lang="da")))
    g.add((WOODP_URI, BIRD.eBirdCode,     Literal("euwoo1")))
    g.add((WOODP_URI, BIRD.conservationStatus, Literal("LC")))

    # ── Observations ──────────────────────────────────────────────────────────
    # Robin near Assistens Kirkegård (lat=55.694, lon=12.554)
    g.add((OBS_ROBIN, RDF.type, BIRD.Observation))
    g.add((OBS_ROBIN, BIRD.observedOn,      Literal("2024-04-10", datatype=XSD.date)))
    g.add((OBS_ROBIN, BIRD.individualCount, Literal(2, datatype=XSD.integer)))
    g.add((OBS_ROBIN, BIRD.observedAt, LOC_CPH))
    g.add((ROBIN_URI, BIRD.hasObservation, OBS_ROBIN))

    g.add((LOC_CPH, RDF.type,       BIRD.Location))
    g.add((LOC_CPH, BIRD.latitude,  Literal(55.694, datatype=XSD.decimal)))
    g.add((LOC_CPH, BIRD.longitude, Literal(12.554, datatype=XSD.decimal)))
    g.add((LOC_CPH, BIRD.locality,  Literal("Nørrebro, Copenhagen")))

    # Woodpecker in Aarhus (far from Assistens)
    g.add((OBS_WOODP, RDF.type, BIRD.Observation))
    g.add((OBS_WOODP, BIRD.observedOn,      Literal("2024-03-20", datatype=XSD.date)))
    g.add((OBS_WOODP, BIRD.individualCount, Literal(1, datatype=XSD.integer)))
    g.add((OBS_WOODP, BIRD.observedAt, LOC_AARHUS))
    g.add((WOODP_URI, BIRD.hasObservation, OBS_WOODP))

    g.add((LOC_AARHUS, RDF.type,       BIRD.Location))
    g.add((LOC_AARHUS, BIRD.latitude,  Literal(56.163, datatype=XSD.decimal)))
    g.add((LOC_AARHUS, BIRD.longitude, Literal(10.204, datatype=XSD.decimal)))
    g.add((LOC_AARHUS, BIRD.locality,  Literal("Aarhus")))

    return g


# ── find_species_by_name ──────────────────────────────────────────────────────

def test_find_by_scientific_name():
    g = _make_graph()
    rows = find_species_by_name(g, "Erithacus")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_find_by_english_name():
    g = _make_graph()
    rows = find_species_by_name(g, "Robin")
    assert any(r["scientificName"] == "Erithacus rubecula" for r in rows)


def test_find_by_danish_name():
    g = _make_graph()
    rows = find_species_by_name(g, "Rødhals")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_find_by_french_name():
    g = _make_graph()
    rows = find_species_by_name(g, "Rouge-gorge")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_find_returns_empty_for_unknown():
    g = _make_graph()
    assert find_species_by_name(g, "Zyxopteryx phantastica") == []


# ── list_danish_species ───────────────────────────────────────────────────────

def test_list_danish_species_count():
    g = _make_graph()
    rows = list_danish_species(g)
    # Both species have observations in the fixture
    assert len(rows) == 2


def test_list_danish_species_has_names():
    g = _make_graph()
    rows = list_danish_species(g)
    sci_names = {r["scientificName"] for r in rows}
    assert "Erithacus rubecula" in sci_names
    assert "Dendrocopos major" in sci_names


# ── recent_danish_observations ────────────────────────────────────────────────

def test_recent_obs_no_filter_returns_all():
    g = _make_graph()
    rows = recent_danish_observations(g)
    assert len(rows) == 2


def test_recent_obs_filter_by_scientific_name():
    g = _make_graph()
    rows = recent_danish_observations(g, "Erithacus")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_recent_obs_filter_by_danish_name():
    g = _make_graph()
    rows = recent_danish_observations(g, "Rødhals")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_recent_obs_filter_by_french_name():
    g = _make_graph()
    rows = recent_danish_observations(g, "Rouge-gorge")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_recent_obs_filter_no_match():
    g = _make_graph()
    assert recent_danish_observations(g, "Flamingo") == []


def test_recent_obs_sorted_newest_first():
    g = _make_graph()
    rows = recent_danish_observations(g)
    dates = [r["date"] for r in rows]
    assert dates == sorted(dates, reverse=True)


def test_recent_obs_date_from_excludes_older():
    g = _make_graph()
    # Robin=2024-04-10, Woodpecker=2024-03-20 — date_from=2024-04-01 should keep only Robin
    rows = recent_danish_observations(g, date_from="2024-04-01")
    sci_names = [r["scientificName"] for r in rows]
    assert "Erithacus rubecula" in sci_names
    assert "Dendrocopos major" not in sci_names


def test_recent_obs_date_to_excludes_newer():
    g = _make_graph()
    # date_to=2024-03-31 should keep only Woodpecker
    rows = recent_danish_observations(g, date_to="2024-03-31")
    sci_names = [r["scientificName"] for r in rows]
    assert "Dendrocopos major" in sci_names
    assert "Erithacus rubecula" not in sci_names


def test_recent_obs_date_range():
    g = _make_graph()
    # Range covering only April → only Robin
    rows = recent_danish_observations(g, date_from="2024-04-01", date_to="2024-04-30")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_recent_obs_date_range_no_match():
    g = _make_graph()
    rows = recent_danish_observations(g, date_from="2025-01-01", date_to="2025-12-31")
    assert rows == []


def test_recent_obs_locality_and_date():
    g = _make_graph()
    # Locality + date_from together
    rows = recent_danish_observations(g, locality="Nørrebro", date_from="2024-04-01")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


# ── species_by_family / species_by_order ─────────────────────────────────────

def test_species_by_family():
    g = _make_graph()
    rows = species_by_family(g, "Muscicapidae")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Erithacus rubecula"


def test_species_by_order():
    g = _make_graph()
    rows = species_by_order(g, "Piciformes")
    assert len(rows) == 1
    assert rows[0]["scientificName"] == "Dendrocopos major"


def test_species_by_family_no_match():
    g = _make_graph()
    assert species_by_family(g, "Accipitridae") == []


# ── nearby_watch ─────────────────────────────────────────────────────────────

def test_nearby_watch_finds_robin_near_assistens():
    """Robin observation (55.694, 12.554) is ~0.3 km from Assistens (55.6918, 12.5559)."""
    g = _make_graph()
    rows = nearby_watch(g, lat=55.6918, lon=12.5559, radius_km=2.0)
    sci_names = {r["scientificName"] for r in rows}
    assert "Erithacus rubecula" in sci_names


def test_nearby_watch_excludes_distant_woodpecker():
    """Woodpecker in Aarhus (~170 km away) must not appear in a 2 km search."""
    g = _make_graph()
    rows = nearby_watch(g, lat=55.6918, lon=12.5559, radius_km=2.0)
    sci_names = {r["scientificName"] for r in rows}
    assert "Dendrocopos major" not in sci_names


def test_nearby_watch_wider_radius_still_excludes_aarhus():
    """Even 50 km radius from Copenhagen should not include Aarhus (~170 km)."""
    g = _make_graph()
    rows = nearby_watch(g, lat=55.6918, lon=12.5559, radius_km=50.0)
    sci_names = {r["scientificName"] for r in rows}
    assert "Dendrocopos major" not in sci_names


# ── taxonomy_summary ──────────────────────────────────────────────────────────

def test_taxonomy_summary_counts():
    g = _make_graph()
    summary = taxonomy_summary(g)
    assert summary["species"] == 2
    assert summary["observations"] == 2
    assert summary["orders"] == 2
    assert summary["families"] == 2


# ── migration_timing ──────────────────────────────────────────────────────────

def _make_migration_graph() -> Graph:
    """
    Graph with Robin observed in April + September across two years,
    and Woodpecker observed year-round (January + May).
    Robin also has bird:typicallyPresentInMonth triples set (as migration.py would).
    """
    g = _make_graph()

    ROBIN_URI = TAXON["species/robi"]
    WOODP_URI = TAXON["species/euwoo1"]
    LOC_CPH = LOC["loc_cph"]

    # Robin: April (arrival) and September (departure) in two years
    for obs_id, date_str in [
        ("obs_robin_apr_2022", "2022-04-05"),
        ("obs_robin_sep_2022", "2022-09-20"),
        ("obs_robin_apr_2023", "2023-04-08"),
        ("obs_robin_sep_2023", "2023-09-18"),
    ]:
        obs = OBS[obs_id]
        g.add((obs, RDF.type, BIRD.Observation))
        g.add((obs, BIRD.observedOn, Literal(date_str, datatype=XSD.date)))
        g.add((obs, BIRD.observedAt, LOC_CPH))
        g.add((ROBIN_URI, BIRD.hasObservation, obs))

    # Robin presence months + migration status (as set by migration.py Rule 5)
    g.add((ROBIN_URI, BIRD.migrationStatus, Literal("SummerVisitor")))
    for month in [4, 9]:
        g.add((ROBIN_URI, BIRD.typicallyPresentInMonth, Literal(month, datatype=XSD.integer)))

    # Woodpecker: year-round (January + May in two years)
    for obs_id, date_str in [
        ("obs_woodp_jan_2022", "2022-01-10"),
        ("obs_woodp_may_2022", "2022-05-15"),
        ("obs_woodp_jan_2023", "2023-01-08"),
        ("obs_woodp_may_2023", "2023-05-12"),
    ]:
        obs = OBS[obs_id]
        g.add((obs, RDF.type, BIRD.Observation))
        g.add((obs, BIRD.observedOn, Literal(date_str, datatype=XSD.date)))
        g.add((obs, BIRD.observedAt, LOC_CPH))
        g.add((WOODP_URI, BIRD.hasObservation, obs))

    g.add((WOODP_URI, BIRD.migrationStatus, Literal("Resident")))
    for month in [1, 5]:
        g.add((WOODP_URI, BIRD.typicallyPresentInMonth, Literal(month, datatype=XSD.integer)))

    return g


def test_migration_timing_returns_species_names():
    g = _make_migration_graph()
    r = migration_timing(g, "Erithacus")
    assert r["scientificName"] == "Erithacus rubecula"
    assert r["commonNameEn"] == "European Robin"
    assert r["commonNameFr"] == "Rouge-gorge familier"
    assert r["commonNameDa"] == "Rødhals"


def test_migration_timing_arrival_departure_months():
    g = _make_migration_graph()
    r = migration_timing(g, "European Robin")
    # April is first month observed each year → arrival = April
    assert r["arrivalMonth"] == 4
    assert r["arrivalMonthName"] == "April"
    # September is last month observed each year → departure = September
    assert r["departureMonth"] == 9
    assert r["departureMonthName"] == "September"


def test_migration_timing_presence_months():
    g = _make_migration_graph()
    r = migration_timing(g, "Erithacus rubecula")
    assert r["presenceMonths"] == [4, 9]
    assert "April" in r["presenceMonthNames"]
    assert "September" in r["presenceMonthNames"]


def test_migration_timing_migration_status():
    g = _make_migration_graph()
    r = migration_timing(g, "Rødhals")
    assert r["migrationStatus"] == "SummerVisitor"


def test_migration_timing_earliest_latest_recorded():
    g = _make_migration_graph()
    r = migration_timing(g, "Erithacus")
    # Earliest overall = 2022-04-05 (from migration fixture)
    # Latest = 2024-04-10 (from _make_graph() base observation)
    assert r["earliestRecorded"] == "2022-04-05"
    assert r["latestRecorded"] == "2024-04-10"


def test_migration_timing_yearly_first_dates():
    g = _make_migration_graph()
    r = migration_timing(g, "Erithacus")
    years = [e["year"] for e in r["yearlyFirstDates"]]
    assert "2022" in years
    assert "2023" in years
    # First date in 2022 = April 5
    first_2022 = next(e for e in r["yearlyFirstDates"] if e["year"] == "2022")
    assert first_2022["firstDate"] == "2022-04-05"


def test_migration_timing_yearly_last_dates():
    g = _make_migration_graph()
    r = migration_timing(g, "Erithacus")
    last_2023 = next(e for e in r["yearlyLastDates"] if e["year"] == "2023")
    assert last_2023["lastDate"] == "2023-09-18"


def test_migration_timing_resident_species():
    g = _make_migration_graph()
    r = migration_timing(g, "Dendrocopos major")
    assert r["migrationStatus"] == "Resident"
    assert 1 in r["presenceMonths"]
    assert 5 in r["presenceMonths"]


def test_migration_timing_unknown_species_returns_empty():
    g = _make_migration_graph()
    r = migration_timing(g, "Nonexistent species ZZZZ")
    assert r == {}


# ── phenology ─────────────────────────────────────────────────────────────────

def test_phenology_returns_12_monthly_values():
    g = _make_migration_graph()
    r = phenology(g, "Erithacus rubecula")
    assert len(r["monthlyCounts"]) == 12
    assert len(r["monthlyTotals"]) == 12
    assert len(r["monthlyFreq"]) == 12
    assert len(r["monthlyFreqPct"]) == 12


def test_phenology_unknown_species_returns_empty():
    g = _make_migration_graph()
    assert phenology(g, "Nonexistent ZZZZ") == {}


def test_phenology_species_with_no_observations_returns_empty():
    # A species in the graph but with no hasObservation triples
    g = _make_graph()  # Woodpecker has one obs, but let's check a species added manually
    # Remove woodpecker's observation to simulate no-obs species
    WOODP_URI = TAXON["species/euwoo1"]
    OBS_WOODP = OBS["obs_woodp"]
    g.remove((WOODP_URI, BIRD.hasObservation, OBS_WOODP))
    r = phenology(g, "Dendrocopos major")
    assert r == {}


def test_phenology_counts_correct_months():
    g = _make_migration_graph()
    r = phenology(g, "Erithacus rubecula")
    # Robin is observed in April (month 4) and September (month 9)
    # monthlyCounts is 0-indexed from month 1 → index 3 = April, index 8 = September
    assert r["monthlyCounts"][3] > 0   # April
    assert r["monthlyCounts"][8] > 0   # September
    assert r["monthlyCounts"][0] == 0  # January — Robin not observed


def test_phenology_relative_frequency_removes_effort_bias():
    g = _make_migration_graph()
    r = phenology(g, "Erithacus rubecula")
    # monthlyFreq = species_obs / total_obs for that month
    # In May: woodpecker has 2 obs, robin has 0 → robin freq in May = 0
    assert r["monthlyFreq"][4] == 0.0  # May
    # In April: woodpecker has 0 obs, robin has 2 → robin freq = 1.0
    assert r["monthlyFreq"][3] == 1.0  # April (robin is the only species observed)


def test_phenology_peak_month_is_most_frequent():
    g = _make_migration_graph()
    r = phenology(g, "Erithacus rubecula")
    # Peak should be April or September (both have robin obs, April has no woodpecker)
    assert r["peakMonth"] in [4, 9]
    assert r["peakMonthName"] in ["April", "September"]


def test_phenology_freq_pct_peak_is_100():
    g = _make_migration_graph()
    r = phenology(g, "Erithacus rubecula")
    # Normalised to peak=100
    assert max(r["monthlyFreqPct"]) == 100


def test_phenology_phenology_tag_format():
    g = _make_migration_graph()
    r = phenology(g, "Erithacus rubecula")
    tag = r["phenologyTag"]
    assert tag.startswith("<bird-phenology ")
    assert 'data="' in tag
    # 12 comma-separated values
    data_part = tag.split('data="')[1].split('"')[0]
    assert len(data_part.split(",")) == 12


def test_phenology_by_french_name():
    g = _make_migration_graph()
    r = phenology(g, "Rouge-gorge familier")
    assert r["scientificName"] == "Erithacus rubecula"


def test_phenology_resident_has_flat_profile():
    g = _make_migration_graph()
    r = phenology(g, "Dendrocopos major")
    # Woodpecker observed Jan + May → both months have relative freq > 0
    # Jan: only woodpecker → freq = 1.0; May: only woodpecker → freq = 1.0
    assert r["monthlyFreq"][0] > 0   # January
    assert r["monthlyFreq"][4] > 0   # May
    assert r["monthlyFreqPct"][0] == 100 or r["monthlyFreqPct"][4] == 100


# ── migration_calendar ────────────────────────────────────────────────────────

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


# ── observation_locations ─────────────────────────────────────────────────────
# Regression: the graph stored bird:observedAt for every observation, but no
# query exposed it, so the chat assistant answered "où a-t-il été vu ?" with
# "I have no locality information" while the data sat right there.

def test_observation_locations_returns_locality_and_coords():
    g = _make_graph()
    rows = observation_locations(g, "Rødhals")
    assert len(rows) == 1
    row = rows[0]
    assert row["locality"] == "Nørrebro, Copenhagen"
    assert float(row["lat"]) == 55.694
    assert float(row["lon"]) == 12.554
    assert int(row["observationCount"]) == 1


def test_observation_locations_matches_any_language():
    g = _make_graph()
    for name in ["Erithacus rubecula", "European Robin", "Rouge-gorge", "rodhals"]:
        rows = observation_locations(g, name)
        assert rows, f"no locality rows for {name!r}"
        assert rows[0]["locality"] == "Nørrebro, Copenhagen"


def test_observation_locations_excludes_other_species():
    g = _make_graph()
    rows = observation_locations(g, "Rødhals")
    assert all(r["locality"] != "Aarhus" for r in rows)


def test_observation_locations_unknown_species_returns_empty():
    g = _make_graph()
    assert observation_locations(g, "Nonexistent bird") == []


def test_observation_locations_aggregates_repeat_visits_to_one_row():
    """Ten observations at one site is one place with count 10, not ten places."""
    g = _make_graph()
    robin = TAXON["species/robi"]
    loc = LOC["loc_cph"]
    for i in range(9):
        obs = OBS[f"obs_robin_{i}"]
        g.add((obs, RDF.type, BIRD.Observation))
        g.add((obs, BIRD.observedOn, Literal(f"2024-05-{i + 1:02d}", datatype=XSD.date)))
        g.add((obs, BIRD.observedAt, loc))
        g.add((robin, BIRD.hasObservation, obs))
    rows = observation_locations(g, "Rødhals")
    assert len(rows) == 1
    assert int(rows[0]["observationCount"]) == 10


# ── thumbnails reachable from the tools users actually go through ─────────────
# Regression: the graph holds a thumbnailUrl for ~9,800 species, but nearby_watch
# and observation_locations did not return it. After a "birds near Nørrebro"
# answer the model had no photo in context, claimed it had none for the species,
# and then invented a species ("Haliaeetus albicillatus") to offer a photo of.

def test_nearby_watch_includes_thumbnail():
    g = _make_graph()
    rows = nearby_watch(g, lat=55.694, lon=12.554, radius_km=5)
    robin = [r for r in rows if r["scientificName"] == "Erithacus rubecula"]
    assert robin, "robin should be within 5km of the fixture location"
    assert robin[0].get("thumbnail") == "https://example.org/robin.jpg?width=300"


def test_observation_locations_includes_thumbnail():
    g = _make_graph()
    rows = observation_locations(g, "Rødhals")
    assert rows
    assert rows[0].get("thumbnail") == "https://example.org/robin.jpg?width=300"


def test_thumbnail_absent_species_has_no_thumbnail_key():
    """A species with no photo must simply omit it — never a placeholder."""
    g = _make_graph()
    rows = observation_locations(g, "Dendrocopos major")
    assert rows
    assert not rows[0].get("thumbnail")


# ── observation_locations must not conflate species ───────────────────────────
# Regression: ?species was neither selected nor grouped on, and the name filter is
# a substring match, so a genus-wide query ("Carduelis") fused every matching
# species observed at one site into a single row — with one arbitrary species'
# photo (SAMPLE(?thumb)) and the sum of everyone's observation counts.

def _add_congener(g: Graph, slug: str, sci_name: str, thumb: str, obs_id: str) -> None:
    """Add a species with one observation at the shared Copenhagen fixture location."""
    sp = TAXON[f"species/{slug}"]
    obs = OBS[obs_id]
    g.add((sp, RDF.type, BIRD.Species))
    g.add((sp, DWC.scientificName, Literal(sci_name)))
    g.add((sp, BIRD.thumbnailUrl, Literal(thumb)))
    g.add((obs, RDF.type, BIRD.Observation))
    g.add((obs, BIRD.observedOn, Literal("2024-06-01", datatype=XSD.date)))
    g.add((obs, BIRD.individualCount, Literal(1, datatype=XSD.integer)))
    g.add((obs, BIRD.observedAt, LOC["loc_cph"]))
    g.add((sp, BIRD.hasObservation, obs))


def test_observation_locations_does_not_conflate_two_species_at_one_site():
    """Two Carduelis species seen at the SAME site must stay two rows.

    Fused into one row, the caller cannot tell whose photo or whose count it has —
    the row would claim 2 observations of a single, unnamed species.
    """
    g = _make_graph()
    _add_congener(g, "eurgol", "Carduelis carduelis",
                  "https://example.org/goldfinch.jpg", "obs_goldfinch")
    _add_congener(g, "comred", "Carduelis flammea",
                  "https://example.org/redpoll.jpg", "obs_redpoll")

    rows = observation_locations(g, "Carduelis")

    assert len(rows) == 2, f"the two Carduelis species were fused into {len(rows)} row(s): {rows}"
    by_name = {r["scientificName"]: r for r in rows}
    assert set(by_name) == {"Carduelis carduelis", "Carduelis flammea"}
    assert int(by_name["Carduelis carduelis"]["observationCount"]) == 1
    assert int(by_name["Carduelis flammea"]["observationCount"]) == 1
    # A species' row must never carry a different species' photo.
    assert by_name["Carduelis carduelis"]["thumbnail"] == "https://example.org/goldfinch.jpg"
    assert by_name["Carduelis flammea"]["thumbnail"] == "https://example.org/redpoll.jpg"


# ── the reasoned-graph fixture ────────────────────────────────────────────────
# _make_graph() is a raw build-time graph: no owl:sameAs closure. Production
# queries run against output/birdology_reasoned.ttl, where reason.py has copied
# every species property onto its ebird:/gbif: alias IRIs — so any OPTIONAL that
# can bind through owl:sameAs yields 1+N rows per observation. Aggregates that
# are not DISTINCT-guarded silently multiply. This fixture reproduces that.

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from reason import _materialise_same_as  # noqa: E402

EBIRD_NS = "https://ebird.org/species/"
GBIF_NS = "https://www.gbif.org/species/"


def _make_reasoned_graph() -> Graph:
    """The fixture graph after the real owl:sameAs materialisation the reasoner does."""
    g = _make_graph()
    g.add((TAXON["species/robi"], OWL.sameAs, URIRef(EBIRD_NS + "robi")))
    g.add((TAXON["species/robi"], OWL.sameAs, URIRef(GBIF_NS + "2492467")))
    _materialise_same_as(g)
    return g


def test_reasoned_fixture_really_fans_out_the_thumbnail():
    """Guard on the fixture itself: if the alias IRIs stop carrying a copy of
    bird:thumbnailUrl, this fixture no longer reproduces production and the two
    tests below would pass for the wrong reason."""
    g = _make_reasoned_graph()
    thumb_holders = set(g.subjects(BIRD.thumbnailUrl, None))
    assert URIRef(EBIRD_NS + "robi") in thumb_holders
    assert URIRef(GBIF_NS + "2492467") in thumb_holders


def test_observation_locations_counts_are_not_inflated_on_the_reasoned_graph():
    """One observation of 2 robins is 1 observation of 2 birds — on the reasoned
    graph too. The owl:sameAs fan-out gave 3 rows per observation, so SUM(?count)
    reported 6 individuals."""
    g = _make_reasoned_graph()
    rows = observation_locations(g, "Rødhals")

    assert len(rows) == 1
    row = rows[0]
    assert int(row["observationCount"]) == 1
    assert int(row["totalIndividuals"]) == 2, (
        f"individualCount=2 reported as {row['totalIndividuals']} — the sameAs fan-out "
        f"multiplied the SUM"
    )
    assert row["thumbnail"] == "https://example.org/robin.jpg?width=300"


def test_observation_locations_totals_survive_repeat_visits_on_reasoned_graph():
    """Aggregation over several observations, each fanned out by the closure."""
    g = _make_reasoned_graph()
    robin = TAXON["species/robi"]
    for i in range(3):
        obs = OBS[f"obs_robin_extra_{i}"]
        g.add((obs, RDF.type, BIRD.Observation))
        g.add((obs, BIRD.observedOn, Literal(f"2024-05-{i + 1:02d}", datatype=XSD.date)))
        g.add((obs, BIRD.individualCount, Literal(5, datatype=XSD.integer)))
        g.add((obs, BIRD.observedAt, LOC["loc_cph"]))
        g.add((robin, BIRD.hasObservation, obs))

    rows = observation_locations(g, "Rødhals")
    assert len(rows) == 1
    # 1 original observation of 2 + 3 observations of 5 = 4 observations, 17 birds.
    assert int(rows[0]["observationCount"]) == 4
    assert int(rows[0]["totalIndividuals"]) == 17
