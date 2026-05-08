"""
EltonTraits 1.0 enrichment for the Birdology knowledge graph.

Downloads (or loads from cache) the EltonTraits bird dataset and adds diet
and foraging-strata properties to species nodes.

Reference:
    Wilman H. et al. (2014). EltonTraits 1.0: Species-level foraging
    attributes of the world's birds and mammals. Ecology 95(7):2027.
    https://doi.org/10.1890/13-1917.1

Dataset (free, CC0):
    https://figshare.com/articles/dataset/EltonTraits_1_0/3559887
    Direct bird file: https://figshare.com/ndownloader/files/5631081

Properties added to bird:Species nodes
---------------------------------------
bird:dietInvertPct     — % invertebrates
bird:dietVertPct       — % vertebrates (all classes combined)
bird:dietFruitPct      — % fruit
bird:dietNectarPct     — % nectar
bird:dietSeedPct       — % seeds
bird:dietScavPct       — % carrion / scavenging
bird:dietCategory      — 5-class label: Invertebrate | PlantSeed |
                         FruiNect | VertFishScav | Omnivore
bird:foragingGround    — % foraging on/near ground
bird:foragingCanopy    — % foraging mid–high canopy
bird:foragingAerial    — % foraging in flight
bird:foragingWater     — % foraging in/around water
bird:nocturnal         — xsd:boolean
"""
from __future__ import annotations

import csv
import urllib.request
from pathlib import Path
from typing import Iterator

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import XSD

from ..namespaces import BIRD, DWC, TAXON

# ── Download constants ────────────────────────────────────────────────────────

_ELTON_URL = "https://figshare.com/ndownloader/files/5631081"
_DEFAULT_CACHE = Path(__file__).parent.parent.parent.parent / "data" / "EltonTraits_birds.txt"

# ── Column names in the EltonTraits bird file ─────────────────────────────────

_COL_TAXON = "Scientific"          # binomial scientific name
_COL_DIET_INV = "Diet.Inv"
_COL_DIET_VEND = "Diet.Vend"       # endothermic vertebrates
_COL_DIET_VECT = "Diet.Vect"       # ectothermic vertebrates
_COL_DIET_VFISH = "Diet.Vfish"
_COL_DIET_VUNK = "Diet.Vunk"       # vertebrates unknown
_COL_DIET_SCAV = "Diet.Scav"
_COL_DIET_FRUIT = "Diet.Fruit"
_COL_DIET_NECT = "Diet.Nect"
_COL_DIET_SEED = "Diet.Seed"
_COL_DIET_PLANTO = "Diet.PlantO"
_COL_DIET_5CAT = "Diet.5Cat"
_COL_FOR_WATBELOW = "ForStrat.watbelowsurf"
_COL_FOR_WATAROUND = "ForStrat.wataroundsurf"
_COL_FOR_GROUND = "ForStrat.ground"
_COL_FOR_UNDER = "ForStrat.understory"
_COL_FOR_MIDHIGH = "ForStrat.midhigh"
_COL_FOR_CANOPY = "ForStrat.canopy"
_COL_FOR_AERIAL = "ForStrat.aerial"
_COL_NOCTURNAL = "Nocturnal"


def download_elton_traits(dest: Path | None = None) -> Path:
    """Download the EltonTraits bird file to *dest* (default: data/ directory).

    Skips download if the file already exists.  Returns the path to the file.
    """
    dest = Path(dest) if dest else _DEFAULT_CACHE
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print(f"  EltonTraits cache found at {dest} — skipping download.")
        return dest
    print(f"  Downloading EltonTraits from figshare → {dest} …")
    urllib.request.urlretrieve(_ELTON_URL, dest)
    print(f"  Downloaded {dest.stat().st_size / 1024:.0f} kB.")
    return dest


def load_elton_traits(path: Path | None = None) -> list[dict]:
    """Load EltonTraits bird records from a local tab-separated file.

    Downloads automatically if *path* is not given and the default cache does
    not exist.

    Returns a list of raw dicts (one per species row).
    """
    path = Path(path) if path else _DEFAULT_CACHE
    if not path.exists():
        path = download_elton_traits(path)

    records: list[dict] = []
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            records.append(dict(row))
    print(f"  Loaded {len(records):,} EltonTraits bird records from {path.name}.")
    return records


def _pct(row: dict, col: str) -> float | None:
    """Parse a percentage column; return None if missing or non-numeric."""
    val = row.get(col, "").strip()
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _build_sci_index(records: list[dict]) -> dict[str, dict]:
    """Map binomial scientific name → EltonTraits row dict."""
    idx: dict[str, dict] = {}
    for rec in records:
        name = rec.get(_COL_TAXON, "").strip()
        if name:
            # Normalise to binomial (first two words)
            binomial = " ".join(name.split()[:2])
            idx[binomial] = rec
    return idx


def elton_to_rdf(
    records: list[dict],
    sci_name_index: dict[str, URIRef] | None = None,
) -> tuple[Graph, int, int]:
    """Convert EltonTraits records to RDF triples.

    Matches on binomial scientific name against *sci_name_index* (a mapping
    from name string → species URIRef, as returned by
    ``birdology.graph.build_sci_name_index``).

    Returns ``(graph, matched, unmatched)``.
    """
    g = Graph()
    g.bind("bird", BIRD)

    elton_idx = _build_sci_index(records)
    matched = 0
    unmatched = 0

    if sci_name_index is None:
        sci_name_index = {}

    for binomial, sp_uri in sci_name_index.items():
        row = elton_idx.get(binomial)
        if row is None:
            unmatched += 1
            continue
        matched += 1
        _add_species_triples(g, sp_uri, row)

    return g, matched, unmatched


def _add_species_triples(g: Graph, sp_uri: URIRef, row: dict) -> None:
    """Add all EltonTraits properties to *sp_uri* in *g*."""

    def add_pct(prop: URIRef, col: str) -> None:
        v = _pct(row, col)
        if v is not None:
            g.add((sp_uri, prop, Literal(round(v, 2), datatype=XSD.decimal)))

    # ── Diet percentages ─────────────────────────────────────────────────────
    add_pct(BIRD.dietInvertPct, _COL_DIET_INV)

    # Combine all vertebrate sub-categories into one
    vert_cols = [_COL_DIET_VEND, _COL_DIET_VECT, _COL_DIET_VFISH, _COL_DIET_VUNK]
    vert_vals = [_pct(row, c) for c in vert_cols]
    if any(v is not None for v in vert_vals):
        total_vert = sum(v for v in vert_vals if v is not None)
        g.add((sp_uri, BIRD.dietVertPct, Literal(round(total_vert, 2), datatype=XSD.decimal)))

    add_pct(BIRD.dietFruitPct,  _COL_DIET_FRUIT)
    add_pct(BIRD.dietNectarPct, _COL_DIET_NECT)
    add_pct(BIRD.dietSeedPct,   _COL_DIET_SEED)
    add_pct(BIRD.dietScavPct,   _COL_DIET_SCAV)

    # 5-category diet label
    cat = row.get(_COL_DIET_5CAT, "").strip()
    if cat:
        g.add((sp_uri, BIRD.dietCategory, Literal(cat)))

    # ── Foraging strata ───────────────────────────────────────────────────────
    # Water: sum below-surface + around-surface
    water_vals = [_pct(row, _COL_FOR_WATBELOW), _pct(row, _COL_FOR_WATAROUND)]
    if any(v is not None for v in water_vals):
        g.add((sp_uri, BIRD.foragingWater,
               Literal(round(sum(v for v in water_vals if v is not None), 2),
                       datatype=XSD.decimal)))

    add_pct(BIRD.foragingGround,  _COL_FOR_GROUND)

    # Canopy: sum understory + mid-high + canopy
    canopy_vals = [_pct(row, _COL_FOR_UNDER), _pct(row, _COL_FOR_MIDHIGH), _pct(row, _COL_FOR_CANOPY)]
    if any(v is not None for v in canopy_vals):
        g.add((sp_uri, BIRD.foragingCanopy,
               Literal(round(sum(v for v in canopy_vals if v is not None), 2),
                       datatype=XSD.decimal)))

    add_pct(BIRD.foragingAerial, _COL_FOR_AERIAL)

    # ── Nocturnal ─────────────────────────────────────────────────────────────
    noc = row.get(_COL_NOCTURNAL, "").strip()
    if noc in ("0", "1"):
        g.add((sp_uri, BIRD.nocturnal, Literal(noc == "1", datatype=XSD.boolean)))


def enrich_graph(g: Graph, records: list[dict]) -> int:
    """Enrich an existing graph *g* with EltonTraits data in-place.

    Returns the number of species matched.
    """
    from ..graph import build_sci_name_index

    sci_index = build_sci_name_index(g)
    elton_g, matched, unmatched = elton_to_rdf(records, sci_name_index=sci_index)
    g += elton_g
    print(f"  EltonTraits: {matched} species matched, {unmatched} not found in graph.")
    print(f"  Added {len(elton_g):,} triples.")
    return matched
