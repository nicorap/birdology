"""Task 4: the training data pipeline — XCM crops with context.

Pins, in order of load-bearing-ness:

* ``convnext_frontend`` honours the model card's RECIPE verbatim (sr
  32000, n_fft 1024, hop 320, n_mels 128, power 2.0, PowerToDB top_db
  80, normalize mean -4.268 std 4.569) and outputs the NATURAL frame
  count: (1, 128, 501) for a 5 s / 160,000-sample crop. CONTROLLER
  RULING: the card's (1, 128, 334) example shape is internally
  inconsistent and falsified by the measured BirdSet training transform
  (transforms.py: Spectrogram(n_fft=1024, hop_length=320) on full 5 s
  clips → 501 centered frames) — the checkpoint saw 501-frame inputs,
  so no trimming, ever. The frontend also never pads: it spectrograms
  whatever wave it gets; ALL padding is CropDataset's job, in the
  waveform domain.
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

from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
import torch
from scipy import sparse

from birdology.prior.indexing import WEEKS_PER_YEAR
from birdology.prior.tensor import PriorSpec, PriorTensor

from birdology.phase_c.context import build_context_matrix
from birdology.phase_c.data import (
    AST_N_FRAMES,
    AST_NORM_MEAN,
    AST_NORM_STD,
    AST_PAD_DB,
    CropDataset,
    PowerToDB,
    ast_frontend,
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
# convnext_frontend: the card's recipe, natural frame count (1, 128, 501).
# ---------------------------------------------------------------------------

N_FRAMES_5S = 501  # 160000 // 320 + 1 (centered STFT) — measured BirdSet truth


class TestConvnextFrontend:
    def test_output_shape_for_5s_crop(self):
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 10))
        out = convnext_frontend(wave)
        assert out.shape == (1, 128, N_FRAMES_5S)

    def test_output_finite_and_float32_after_normalize(self):
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 11))
        out = convnext_frontend(wave)
        assert out.dtype == torch.float32
        assert torch.all(torch.isfinite(out))

    def test_short_wave_yields_natural_frame_count_no_padding(self):
        """The frontend NEVER pads (padding in normalized-dB space would
        inject fake energy floors); it returns the natural spectrogram of
        whatever wave it gets. 1 s @ 32 kHz → 32000//320 + 1 = 101
        frames. CropDataset owns all padding, in the waveform domain."""
        wave = torch.from_numpy(_noise(SR, 12))  # 1 s
        out = convnext_frontend(wave)
        assert out.shape == (1, 128, SR // 320 + 1)

    def test_padded_wave_equals_full_prefix_frames(self):
        """Waveform-domain padding is the one true padding path: a short
        wave zero-padded to 5 s in the WAVEFORM domain must produce the
        same leading frames as the short wave alone (up to the STFT
        boundary frames that see the pad)."""
        short = torch.from_numpy(_noise(2 * SR, 14))
        padded = torch.nn.functional.pad(short, (0, CROP_SAMPLES - 2 * SR))
        out_padded = convnext_frontend(padded)
        assert out_padded.shape == (1, 128, N_FRAMES_5S)

    def test_deterministic(self):
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 13))
        assert torch.equal(convnext_frontend(wave), convnext_frontend(wave))


# ---------------------------------------------------------------------------
# ast_frontend: BirdSet's vision pipeline + pad/truncate-to-1024 + the
# MEASURED negative-std normalization — golden-gated against the real
# BirdSet transform (tests/phase_c/make_ast_goldens.py ran BirdSet's own
# Resizer + PowerToDB sources; the .npz is the committed ground truth).
# ---------------------------------------------------------------------------

_GOLDENS_PATH = Path(__file__).parent / "ast_goldens.npz"


class TestAstFrontend:
    def test_output_shape_for_5s_crop(self):
        """(1024, 128) time×mel — AST's input_values contract: 501
        natural frames padded right to AST_N_FRAMES=1024."""
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 20))
        out = ast_frontend(wave)
        assert out.shape == (AST_N_FRAMES, 128)

    def test_output_finite_and_float32(self):
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 21))
        out = ast_frontend(wave)
        assert out.dtype == torch.float32
        assert torch.all(torch.isfinite(out))

    def test_deterministic(self):
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 22))
        assert torch.equal(ast_frontend(wave), ast_frontend(wave))

    def test_mono_only(self):
        with pytest.raises(ValueError, match="mono"):
            ast_frontend(torch.zeros(2, CROP_SAMPLES))

    def test_pad_region_is_birdset_pad_value(self):
        """BirdSet's Resizer pads the time axis with CONSTANT -80.0 dB
        (resize.py: db_scale=True → padding_value=-80.0) BEFORE
        normalization — the padded frames therefore carry
        (-80 - mean)/std exactly, not zeros."""
        wave = torch.from_numpy(_noise(CROP_SAMPLES, 23))  # 501 frames
        out = ast_frontend(wave)
        expected_pad = (AST_PAD_DB - AST_NORM_MEAN) / AST_NORM_STD
        pad_region = out[501:, :]
        assert pad_region.shape[0] == AST_N_FRAMES - 501
        torch.testing.assert_close(
            pad_region,
            torch.full_like(pad_region, expected_pad),
        )

    def test_long_wave_truncates_to_1024_frames(self):
        """>1024 natural frames: BirdSet TRUNCATES the width (resize.py
        truncate_spectrogram_width) — never interpolates."""
        wave = torch.from_numpy(_noise(11 * SR, 24))  # 1101 frames
        out = ast_frontend(wave)
        assert out.shape == (AST_N_FRAMES, 128)

    def test_normalization_std_is_negative(self):
        """The measured BirdSet quirk, pinned: std = -4.569 (NEGATIVE, as
        in BirdSet transforms.py PreprocessingConfig line 64) — naive
        (x-mean)/|std| would flip every value's sign relative to what
        the checkpoint saw."""
        assert AST_NORM_STD == -4.569
        assert AST_NORM_MEAN == -4.268

    @pytest.mark.parametrize("name", ["noise_1s", "chirp_2s", "silence_1s"])
    def test_golden_gate_matches_real_birdset_transform(self, name):
        """ast_frontend must reproduce the REAL BirdSet preprocessing
        (Spectrogram→MelScale→PowerToDB→Resizer(1024, pad -80 dB)→
        (x-(-4.268))/(-4.569)→transpose), captured offline by running
        BirdSet's own Resizer/PowerToDB sources on these exact waves
        (make_ast_goldens.py). Bit-exact on the platform that generated
        the goldens (arm64 Mac); across platforms the FFT backend differs.
        Measured on the x86 fleet (dgx160, torch 2.13+cu129): noise_1s max
        abs diff 6.7e-6 on values of order 1; chirp_2s max abs 2.7e-4 on
        values of order 7, max relative 3.8e-5 (23 of 131,072 elements).
        The gate is rtol=1e-4 + atol=1e-4: ~2.6x the worst measured
        relative spread, and orders of magnitude below anything that
        could move a logit."""
        goldens = np.load(_GOLDENS_PATH)
        waves = __import__(
            "tests.phase_c.make_ast_goldens", fromlist=["golden_waves"]
        ).golden_waves()
        out = ast_frontend(torch.from_numpy(waves[name]))
        np.testing.assert_allclose(out.numpy(), goldens[name], rtol=1e-4, atol=1e-4)


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
    def test_rows_with_unknown_code_are_excluded_at_construction(
        self, provider, caplog
    ):
        rows = BASE_ROWS + [
            {"filepath": "ghost_6s.ogg", "ebird_code": "ghostsp1",
             "cell": "cellA", "week": 5},
            {"filepath": "ghost_6s.ogg", "ebird_code": "ghostsp1",
             "cell": "cellA", "week": 6},
            {"filepath": "ghost_6s.ogg", "ebird_code": "ghostsp2",
             "cell": None, "week": None},
        ]
        with caplog.at_level("WARNING", logger="birdology.phase_c.data"):
            ds = _dataset(rows=rows, provider=provider)
        assert len(ds) == len(BASE_ROWS)
        assert ds.skipped_codes == {"ghostsp1": 2, "ghostsp2": 1}
        # The skip must be LOUD: exactly one warning naming the gaps.
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1
        assert "ghostsp1" in warnings[0].getMessage()
        assert "ghostsp2" in warnings[0].getMessage()

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
        assert item["inputs"].shape == (1, 128, N_FRAMES_5S)
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
        """2 s recording: zero-pad right to 5 s IN THE WAVEFORM DOMAIN
        (BirdSet's clip handling) — so the frontend still sees 160,000
        samples and emits the full 501 frames."""
        item = _dataset(provider=provider)[1]
        assert item["inputs"].shape == (1, 128, N_FRAMES_5S)
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


# ---------------------------------------------------------------------------
# The frontend is INJECTABLE (default: convnext_frontend) — the AST cell
# trains on ast_frontend inputs through the SAME dataset.
# ---------------------------------------------------------------------------


class TestFrontendInjection:
    def test_default_frontend_is_convnext(self, provider):
        item = _dataset(provider=provider)[0]
        assert item["inputs"].shape == (1, 128, N_FRAMES_5S)

    def test_ast_frontend_flows_through_getitem(self, provider):
        ds = CropDataset(
            _manifest(BASE_ROWS),
            fake_load_audio,
            provider,
            LABEL2ID,
            seed=0,
            epoch=0,
            frontend=ast_frontend,
        )
        item = ds[0]
        assert item["inputs"].shape == (AST_N_FRAMES, 128)
        assert item["inputs"].dtype == torch.float32

    def test_injected_frontend_sees_the_same_crop(self, provider):
        """The frontend swap changes ONLY the feature map, never the
        (seed, epoch, idx)-keyed crop underneath."""
        kwargs = dict(seed=7, epoch=3)
        ds_conv = _dataset(provider=provider, **kwargs)
        ds_ast = CropDataset(
            _manifest(BASE_ROWS),
            fake_load_audio,
            provider,
            LABEL2ID,
            frontend=ast_frontend,
            **kwargs,
        )
        conv = ds_conv[0]["inputs"]
        ast = ds_ast[0]["inputs"]
        # Same crop → the ast natural 501 frames are the conv mel,
        # re-normalized with the AST constants and transposed.
        expected = (
            (conv[0] * 4.569 + (-4.268)) - AST_NORM_MEAN
        ) / AST_NORM_STD
        torch.testing.assert_close(
            ast[:N_FRAMES_5S, :].transpose(0, 1), expected
        )
