"""build_dkx_testset: the selection rules that decide what DKX evaluates on."""
from __future__ import annotations

import importlib.util
from pathlib import Path

_REPO = Path(__file__).parent.parent
_spec = importlib.util.spec_from_file_location("dkx", _REPO / "scripts/build_dkx_testset.py")
dkx = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dkx)

SCI2CODE = dkx.sci_to_code_map({"eurrob1": "Erithacus rubecula", "eurbla": "Turdus merula"})


def rec(**kw):
    base = dict(id="100", q="A", lat="55.7", lon="12.5", date="2025-05-01",
                gen="Erithacus", sp="rubecula", also=[], rec="R", uploaded="2025-05-02", file="u")
    base.update(kw)
    return base


def test_parse_date_rejects_unknown_parts():
    assert str(dkx.parse_date("2025-05-01")) == "2025-05-01"
    for raw in ("2025-00-01", "2025-05-00", "0000-00-00", "", None):
        assert dkx.parse_date(raw) is None


def test_sci_map_drops_ambiguous_names():
    m = dkx.sci_to_code_map({"a": "Foo bar", "b": "Foo bar", "c": "Baz qux"})
    assert m == {"baz qux": "c"}


def test_xcl_ids_parses_birdset_names():
    assert dkx.xcl_ids(["XC208209.ogg", "data/x/XC5.ogg", "other.ogg"]) == {208209, 5}


def test_select_counts_every_drop_and_keeps_background_labels():
    recs = [
        rec(id="1"),
        rec(id="2", q="D"),
        rec(id="3"),                                    # in XCL
        rec(id="4", lat=""),
        rec(id="5", date="2025-00-00"),
        rec(id="6", gen="Nonexistent", sp="bird"),
        rec(id="7", also=["Turdus merula", "Unknown thing", "Erithacus rubecula"]),
    ]
    kept, drops = dkx.select(recs, exclude_ids={3}, sci2code=SCI2CODE)
    assert [r["id"] for r in kept] == [1, 7]
    assert drops == {"quality": 1, "in_xcl": 1, "no_coordinates": 1, "no_date": 1,
                     "species_not_in_ebird_map": 1, "background_label_unmapped": 1}
    assert kept[1]["ebird_code_multilabel"] == ["eurrob1", "eurbla"]   # no duplicate primary
    assert kept[0]["filepath"] == "XC1.flac" and kept[0]["long"] == 12.5


def test_xc_synonyms_resolve_to_the_ebird_code():
    m = dkx.sci_to_code_map({"lirplo": "Thinornis dubius"})
    assert m["charadrius dubius"] == "lirplo"
    assert "charadrius morinellus" not in m   # only when its eBird name is in the map


def test_to_flac_refuses_audio_shorter_than_one_second():
    import io

    import numpy as np
    import pytest
    import soundfile as sf

    buf = io.BytesIO()
    sf.write(buf, np.zeros(100, dtype="float32"), 44_100, format="WAV")
    with pytest.raises(ValueError, match="refusing"):
        dkx.to_flac_32k(buf.getvalue())
    ok = io.BytesIO()
    sf.write(ok, np.zeros(44_100 * 2, dtype="float32"), 44_100, format="WAV")
    out, sr = sf.read(io.BytesIO(dkx.to_flac_32k(ok.getvalue())))
    assert sr == 32_000 and len(out) == 64_000
