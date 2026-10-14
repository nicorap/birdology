"""Old XCL codes resolve onto BirdNET's axis by scientific name."""
import numpy as np

from birdology.phase_d.taxonomy import SYNONYMS, remap_code, resolve_remap, species_mask

CODES = ["webowl1", "euhgul1", "unmapped__1166", "eurbla", "corplo"]
SCI = ["Tyto alba", "Larus argentatus", "Charadrius dubius", "Turdus merula", "Charadrius hiaticula"]


def test_renamed_codes_map_to_birdnet_codes():
    remap = resolve_remap(CODES, SCI)
    assert remap["brnowl"] == "webowl1"
    assert remap["hergul"] == "euhgul1"
    # BirdNET's placeholder codes are reachable only through the scientific name
    assert remap["lirplo"] == "unmapped__1166"


def test_codes_already_on_the_axis_are_never_remapped():
    remap = resolve_remap(CODES + ["brnowl"], SCI + ["Tyto furcata"])
    assert "brnowl" not in remap


def test_unresolvable_codes_are_left_out():
    remap = resolve_remap(CODES, SCI)
    assert "doveki" not in remap            # Alle alle is not on this toy axis
    assert remap_code("doveki", remap) == "doveki"


def test_name_match_ignores_case_and_whitespace():
    remap = resolve_remap(["x1"], ["  tyto ALBA "], {"brnowl": ("Tyto alba",)})
    assert remap == {"brnowl": "x1"}


def test_first_listed_name_wins():
    remap = resolve_remap(["a", "b"], ["Charadrius dubius", "Thinornis dubius"])
    assert remap["lirplo"] == "a"


def test_parallel_lists_are_required():
    import pytest
    with pytest.raises(ValueError):
        resolve_remap(["a", "b"], ["Tyto alba"])


def test_every_synonym_entry_lists_at_least_one_name():
    assert all(names and all(len(n.split()) == 2 for n in names) for names in SYNONYMS.values())


def test_species_mask_counts_remapped_targets_through_their_old_code():
    remap = resolve_remap(CODES, SCI)
    bridge = {"eurbla": 1, "lirplo": 2, "corplo": 3}   # no bridge entry for unmapped__1166 itself
    mask = species_mask(CODES, remap, bridge, known_keys={1, 2})
    assert mask.tolist() == [False, False, True, True, False]
    assert mask.dtype == np.bool_
