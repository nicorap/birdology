"""Map Xeno-canto / BirdSet XCL label codes onto BirdNET 2.4's label axis.

XCL labels with an older eBird taxonomy than BirdNET's axis. Codes that were
renamed or split since (brnowl, hergul, whimbr, eurjac, lirplo, norgos ...)
match no BirdNET code, so a naive ``code in birdnet_codes`` join silently drops
every recording of those species -- measured on the European XCL subset: 4,538
recordings across 42 codes, including Barn Owl, Herring Gull, Whimbrel, Jackdaw
and Goshawk.

Several of BirdNET's own codes are placeholders (``unmapped__1166`` is
Charadrius dubius), so the join has to go through scientific names. Each old
code lists the names it may carry on BirdNET's axis, first match wins. Splits
are resolved for the European population (brnowl is Western Barn Owl, Tyto
alba), which is the only population this mapping is used for.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

#: old code -> candidate scientific names on BirdNET's axis, in preference order.
SYNONYMS: dict[str, tuple[str, ...]] = {
    "brnowl": ("Tyto alba",),
    "eurjac": ("Corvus monedula", "Coloeus monedula"),
    "whimbr": ("Numenius phaeopus",),
    "lirplo": ("Charadrius dubius", "Thinornis dubius"),
    "norgos": ("Accipiter gentilis", "Astur gentilis"),
    "hergul": ("Larus argentatus",),
    "litbit1": ("Ixobrychus minutus", "Botaurus minutus"),
    "lesred1": ("Acanthis cabaret",),
    "comred": ("Acanthis flammea",),
    "hoared": ("Acanthis hornemanni",),
    "cohmar1": ("Delichon urbicum",),
    "eurnut1": ("Nucifraga caryocatactes",),
    "eurdot": ("Charadrius morinellus", "Eudromias morinellus"),
    "refblu": ("Tarsiger cyanurus",),
    "kenplo1": ("Charadrius alexandrinus",),
    "categr": ("Bubulcus ibis",),
    "easwar1": ("Curruca cantillans", "Sylvia cantillans"),
    "eursha1": ("Phalacrocorax aristotelis", "Gulosus aristotelis"),
    "rebgoo1": ("Branta ruficollis",),
    "alpswi1": ("Apus melba", "Tachymarptis melba"),
    "greshe": ("Ardenna gravis", "Puffinus gravis"),
    "grebus1": ("Otis tarda",),
    "subwar8": ("Curruca subalpina", "Sylvia subalpina", "Curruca iberiae"),
    "gubter1": ("Gelochelidon nilotica",),
    "doveki": ("Alle alle",),
    "corshe": ("Calonectris diomedea", "Calonectris borealis"),
    "soclap1": ("Vanellus gregarius",),
    "asdwar1": ("Curruca nana", "Sylvia nana"),
    "rerswa1": ("Cecropis daurica",),
    "pygcor2": ("Microcarbo pygmaeus", "Phalacrocorax pygmeus"),
    "demcra1": ("Anthropoides virgo", "Grus virgo"),
    "grwpel1": ("Pelecanus onocrotalus",),
    "bltacc1": ("Prunella atrogularis",),
    "crccou1": ("Cursorius cursor",),
}


def resolve_remap(
    codes: Sequence[str],
    sci: Sequence[str],
    synonyms: Mapping[str, Sequence[str]] = SYNONYMS,
) -> dict[str, str]:
    """Old code -> BirdNET code, for old codes that are NOT already on the axis.

    ``codes`` and ``sci`` are BirdNET's parallel label lists (code, scientific
    name). A code already present on the axis is never remapped, and a code
    none of whose names is on the axis is left out (and stays unmappable).
    """
    if len(codes) != len(sci):
        raise ValueError(f"codes ({len(codes)}) and sci ({len(sci)}) must be parallel")
    on_axis = set(codes)
    by_sci = {s.strip().lower(): c for c, s in zip(codes, sci)}
    remap = {}
    for old, names in synonyms.items():
        if old in on_axis:
            continue
        hit = next((by_sci[n.lower()] for n in names if n.lower() in by_sci), None)
        if hit is not None:
            remap[old] = hit
    return remap


def remap_code(code: str, remap: Mapping[str, str]) -> str:
    return remap.get(code, code)


def species_mask(
    codes: Sequence[str],
    remap: Mapping[str, str],
    bridge: Mapping[str, object],
    known_keys: set[int],
) -> np.ndarray:
    """Boolean mask over BirdNET's axis: species whose GBIF key is in ``known_keys``.

    ``bridge`` maps eBird code -> GBIF key (``code_to_gbif.json``); ``known_keys``
    is e.g. every species DOF has recorded in Denmark. A remapped target counts as
    known when its OLD code bridges into ``known_keys`` -- the placeholder codes
    on BirdNET's axis (``unmapped__*``) have no bridge entry of their own.
    """
    def known(c: str) -> bool:
        k = bridge.get(c)
        return k is not None and int(k) in known_keys

    col = {c: j for j, c in enumerate(codes)}
    mask = np.array([known(c) for c in codes], dtype=bool)
    for old, new in remap.items():
        if new in col and known(old):
            mask[col[new]] = True
    return mask
