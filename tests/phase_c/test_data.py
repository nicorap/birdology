"""Task 4: the training data pipeline — XCM crops with context.

Pins, in order of load-bearing-ness:

* ``convnext_frontend`` honours the model card VERBATIM (sr 32000,
  n_fft 1024, hop 320, n_mels 128, power 2.0, PowerToDB top_db 80,
  normalize mean -4.268 std 4.569) and its output contract (1, 128, 334).
  MEASURED: torchaudio's centered Spectrogram yields 501 frames for
  160,000 samples at hop 320; the frontend right-trims to 334 (zero-pads
  right when shorter) — the 334 contract is binding, the trim is the
  documented reconciliation.
* Crop offsets are a pure function of (seed, epoch, index) — Task 5's
  bit-exact resume stands on this; a shared stateful RNG would break it.
* A manifest row whose ebird_code is NOT in label2id is EXCLUDED at
  construction with a counted, named skip (Review Focus 1) — never a
  KeyError mid-epoch.
* Null (cell, week) rows pass through to the provider's absent-context
  answer: zeros + present 0.0.
* Short recordings (<5 s) zero-pad right — BirdSet's 5 s clip handling.

No network, no files: load_audio is an injected callable over synthetic
arrays, the manifest is an in-memory pyarrow table with the real phase_a
column names, the prior is a tiny real PriorTensor.
"""
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest
import torch
from scipy import sparse

from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import PriorSpec, PriorTensor

from birdology.phase_c.context import build_context_matrix
from birdology.phase_c.data import (
    CropDataset,
    PowerToDB,
    convnext_frontend,
    make_label_map,
)

SR = 32000
CROP_SAMPLES = 160000  # 5 s @ 32 kHz

# ---------------------------------------------------------------------------
# Fixtures: tiny prior (test_context's pattern), synthetic audio, manifest.
# ---------------------------------------------------------------------------

SPECIES_KEYS = np.array([111, 222], dtype=np.int64)
CELLS = ["cellA"]
BRIDGE = {"codeA": 111, "codeB": 222}


def _tiny_prior() -> PriorTensor:
    n_cols = len(CELLS) * WEEKS_PER_YEAR
    col = 0 * WEEKS_PER_YEAR + (5 - 1)
    matrix = sparse.csr_matrix(
        ([0.5, 0.25], ([0, 1], [col, col])),
        shape=(len(SPECIES_KEYS), n_cols),
        dtype=np.float64,
    )
    return PriorTensor(
        spec=PriorSpec(resolution=4, source="eod", variant="full"),
        species_keys=SPECIES_KEYS,
        cells=CELLS,
        values=matrix,
    )


@pytest.fixture()
def provider():
    return build_context_matrix(_tiny_prior(), ["codeA", "codeB"], BRIDGE)


LABEL2ID = {"codeA": 0, "codeB": 1}


def _noise(n: int, seed: int) -> np.ndarray:
    return np.random.default_rng(seed).standard_normal(n).astype(np.float32)


#: filepath -> synthetic waveform; the injected load_audio reads this.
AUDIO = {
    "a_10s.ogg": _noise(10 * SR, 1),  # longer than 5 s: random window
    "b_2s.ogg": _noise(2 * SR, 2),  # shorter than 5 s: zero-pad right
    "c_5s.ogg": _noise(5 * SR, 3),  # exactly 5 s
    "ghost_6s.ogg": _noise(6 * SR, 4),  # code not in label2id
}


def fake_load_audio(filepath: str) -> np.ndarray:
    return AUDIO[filepath]


def _manifest(rows: list[dict]) -> pa.Table:
    """In-memory table with the real phase_a column names."""
    return pa.table(
        {
            "filepath": [r["filepath"] for r in rows],
            "ebird_code": [r["ebird_code"] for r in rows],
            "cell": [r.get("cell") for r in rows],
            "week": pa.array(
                [r.get("week") for r in rows], type=pa.int64()
            ),
            "split": [r.get("split", "train") for r in rows],
        }
    )


BASE_ROWS = [
    {"filepath": "a_10s.ogg", "ebird_code": "codeA", "cell": "cellA", "week": 5},
    {"filepath": "b_2s.ogg", "ebird_code": "codeB", "cell": "cellA", "week": 5},
    {"filepath": "c_5s.ogg", "ebird_code": "codeA", "cell": None, "week": None},
]


def _dataset(rows=None, seed=0, epoch=0, provider=None):
    return CropDataset(
        _manifest(BASE_ROWS if rows is None else rows),
        fake_load_audio,
        provider,
        LABEL2ID,
        seed=seed,
        epoch=epoch,
    )


# ---------------------------------------------------------------------------
# convnext_frontend: the model card, verbatim, output (1, 128, 334).
# ---------------------------------------------------------------------------


class TestConvnextFrontend:
    def test_output_shape_for_5s_crop(self):
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 10))
        out = convnext_frontend(wave)
        assert out.shape == (1, 128, 334)

    def test_output_finite_and_float32_after_normalize(self):
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 11))
        out = convnext_frontend(wave)
        assert out.dtype == torch.float32
        assert torch.all(torch.isfinite(out))

    def test_short_wave_still_yields_334_frames(self):
        """Fewer native frames than 334: zero-pad right to the contract."""
        wave = torch.from_numpy(_noise(SR, 12))  # 1 s -> ~101 frames
        out = convnext_frontend(wave)
        assert out.shape == (1, 128, 334)

    def test_deterministic(self):
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 13))
        assert torch.equal(convnext_frontend(wave), convnext_frontend(wave))


class TestPowerToDB:
    def test_matches_librosa_formula(self):
        """10*log10(max(S, amin)) - 10*log10(max(ref, amin)), floored at
        (max - top_db) — the card's snippet, hand-computed here."""
        S = torch.tensor([[1.0, 1e-12, 100.0, 1e-4]])
        amin, top_db = 1e-10, 80.0
        log_spec = 10.0 * np.log10(np.maximum(S.numpy(), amin))
        log_spec -= 10.0 * np.log10(np.maximum(1.0, amin))
        expected = np.maximum(log_spec, log_spec.max() - top_db)
        out = PowerToDB(ref=1.0, amin=amin, top_db=top_db)(S)
        np.testing.assert_allclose(out.numpy(), expected, rtol=1e-6)

    def test_top_db_floors_the_dynamic_range(self):
        S = torch.tensor([1.0, 1e-30])
        out = PowerToDB(top_db=80.0)(S)
        assert out.max() - out.min() <= 80.0 + 1e-6


# ---------------------------------------------------------------------------
# make_label_map: invert the HF id2label convention.
# ---------------------------------------------------------------------------


class TestMakeLabelMap:
    def test_inverts_id2label(self):
        assert make_label_map({0: "codeA", 1: "codeB", 2: "codeC"}) == {
            "codeA": 0,
            "codeB": 1,
            "codeC": 2,
        }

    def test_string_int_keys_from_json_configs(self):
        """HF configs loaded from json often carry string keys."""
        assert make_label_map({"0": "codeA", "1": "codeB"}) == {
            "codeA": 0,
            "codeB": 1,
        }


# ---------------------------------------------------------------------------
# Review Focus 1: codes outside label2id are skipped, counted, named.
# ---------------------------------------------------------------------------


class TestLabelGapSkips:
    def test_rows_with_unknown_code_are_excluded_at_construction(self, provider):
        rows = BASE_ROWS + [
            {"filepath": "ghost_6s.ogg", "ebird_code": "ghostsp1",
             "cell": "cellA", "week": 5},
            {"filepath": "ghost_6s.ogg", "ebird_code": "ghostsp1",
             "cell": "cellA", "week": 6},
            {"filepath": "ghost_6s.ogg", "ebird_code": "ghostsp2",
             "cell": None, "week": None},
        ]
        ds = _dataset(rows=rows, provider=provider)
        assert len(ds) == len(BASE_ROWS)
        assert ds.skipped_codes == {"ghostsp1": 2, "ghostsp2": 1}

    def test_no_keyerror_at_iteration_time(self, provider):
        rows = BASE_ROWS + [
            {"filepath": "ghost_6s.ogg", "ebird_code": "ghostsp1",
             "cell": "cellA", "week": 5},
        ]
        ds = _dataset(rows=rows, provider=provider)
        for i in range(len(ds)):  # would KeyError if the ghost survived
            item = ds[i]
            assert item["label"] in LABEL2ID.values()

    def test_clean_manifest_has_empty_skipped_codes(self, provider):
        ds = _dataset(provider=provider)
        assert ds.skipped_codes == {}
        assert len(ds) == len(BASE_ROWS)


# ---------------------------------------------------------------------------
# Item contract: shapes, dtypes, labels, context join.
# ---------------------------------------------------------------------------


class TestItemContract:
    def test_item_shapes_and_types(self, provider):
        item = _dataset(provider=provider)[0]
        assert set(item) == {"inputs", "context", "present", "label"}
        assert item["inputs"].shape == (1, 128, 334)
        assert item["inputs"].dtype == torch.float32
        assert item["context"].shape == (2,)
        assert item["context"].dtype == torch.float32
        assert isinstance(item["present"], float)
        assert isinstance(item["label"], int)

    def test_labels_follow_label2id(self, provider):
        ds = _dataset(provider=provider)
        assert ds[0]["label"] == LABEL2ID["codeA"]
        assert ds[1]["label"] == LABEL2ID["codeB"]

    def test_row_with_context_gets_present_one(self, provider):
        item = _dataset(provider=provider)[0]  # cellA week 5
        assert item["present"] == 1.0
        expected, _ = provider.vector("cellA", 5)
        np.testing.assert_allclose(item["context"].numpy(), expected)

    def test_null_cell_week_gets_zeros_and_present_zero(self, provider):
        item = _dataset(provider=provider)[2]  # c_5s.ogg: null cell/week
        assert item["present"] == 0.0
        np.testing.assert_array_equal(
            item["context"].numpy(), np.zeros(2, dtype=np.float32)
        )

    def test_short_recording_is_right_padded_not_rejected(self, provider):
        """2 s recording: zero-pad right to 5 s (BirdSet's clip handling)."""
        item = _dataset(provider=provider)[1]
        assert item["inputs"].shape == (1, 128, 334)
        assert torch.all(torch.isfinite(item["inputs"]))


# ---------------------------------------------------------------------------
# Crop determinism: (seed, epoch, index) is the WHOLE story (Review Focus
# 5 dependency — Task 5's bit-exact resume replays these offsets).
# ---------------------------------------------------------------------------


class TestCropDeterminism:
    def test_same_seed_epoch_index_same_crop(self, provider):
        a = _dataset(seed=7, epoch=3, provider=provider)[0]
        b = _dataset(seed=7, epoch=3, provider=provider)[0]
        assert torch.equal(a["inputs"], b["inputs"])

    def test_repeated_getitem_is_stable(self, provider):
        """No shared stateful RNG: reading idx 0 twice gives the same
        crop — a shared RNG would advance and change it."""
        ds = _dataset(seed=7, epoch=3, provider=provider)
        assert torch.equal(ds[0]["inputs"], ds[0]["inputs"])

    def test_access_order_does_not_change_crops(self, provider):
        """Reading other indices first must not perturb idx 0 — the
        offset hangs only on (seed, epoch, index)."""
        ds1 = _dataset(seed=7, epoch=3, provider=provider)
        first = ds1[0]["inputs"]
        ds2 = _dataset(seed=7, epoch=3, provider=provider)
        _ = ds2[2], ds2[1]  # touch others first
        assert torch.equal(ds2[0]["inputs"], first)

    def test_epoch_change_changes_the_crop(self, provider):
        a = _dataset(seed=7, epoch=0, provider=provider)[0]
        b = _dataset(seed=7, epoch=1, provider=provider)[0]
        assert not torch.equal(a["inputs"], b["inputs"])

    def test_seed_change_changes_the_crop(self, provider):
        a = _dataset(seed=0, epoch=0, provider=provider)[0]
        b = _dataset(seed=1, epoch=0, provider=provider)[0]
        assert not torch.equal(a["inputs"], b["inputs"])

    def test_epoch_is_mutable_for_resume(self, provider):
        """Task 5 sets dataset.epoch on resume; offsets must follow."""
        ds = _dataset(seed=7, epoch=0, provider=provider)
        at0 = ds[0]["inputs"]
        ds.epoch = 1
        at1 = ds[0]["inputs"]
        ds.epoch = 0
        again = ds[0]["inputs"]
        assert not torch.equal(at0, at1)
        assert torch.equal(at0, again)

    def test_exact_5s_recording_needs_no_rng(self, provider):
        """A 5 s recording has exactly one window: identical across
        epochs."""
        a = _dataset(seed=7, epoch=0, provider=provider)[2]
        b = _dataset(seed=7, epoch=1, provider=provider)[2]
        assert torch.equal(a["inputs"], b["inputs"])
