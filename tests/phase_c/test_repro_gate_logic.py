"""Phase C repro-gate logic + run-CLI row contract + sbatch pins.

The gate (scripts/cluster/phase_c_repro_gate.py) is Phase C's hard gate
(spec §3): the FROZEN checkpoint run through OUR pipeline must land
within ±0.05 absolute cmAP of the published LT row per test set
(ConvNext and AST rows, Table 5, arXiv:2403.10380v3 — MEASURED from the
paper's pdftotext 2026-10-01; the task brief's provisional 3-decimal
rows were falsified by the measurement and the measured values win).
These tests pin the comparison logic, the boundary semantics, the
output-line contract, the zero-rows/NaN refusals, and the FiLM identity
subcommand's check; the controller runs the fleet side.

Also here (plan Task 6 Step 1):
* the run CLI's results-row provenance refusal (phase_c_run.py), and
* the sbatch lint for the three measured silent-failure constraints.

Both script modules must be IMPORT-SAFE: importing them runs no argparse
(it lives under ``__main__``) — mirroring scripts/cluster/repro_gate.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest
import torch
from transformers import ConvNextConfig, ConvNextForImageClassification

_REPO = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_REPO / "src"))
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO / "scripts" / "cluster"))

import phase_c_repro_gate
import phase_c_run
from birdology.phase_c.context import ContextEncoder
from birdology.phase_c.film import FiLMConvNext

N_CLASSES = 5


def tiny_convnext(seed: int = 0) -> ConvNextForImageClassification:
    torch.manual_seed(seed)
    config = ConvNextConfig(
        num_channels=1,
        num_stages=2,
        hidden_sizes=[8, 16],
        depths=[1, 1],
        num_labels=N_CLASSES,
        image_size=32,
    )
    return ConvNextForImageClassification(config)


# ---------------------------------------------------------------------------
# The published table — measured values pinned (a drive-by edit must fail)
# ---------------------------------------------------------------------------


def test_published_tables_carry_the_measured_values():
    """Table 5 (arXiv:2403.10380v3), LT cmAP rows, measured from the
    paper's pdftotext 2026-10-01. The brief's provisional numbers
    (ConvNext POW .340 etc.) were falsified by this measurement."""
    cnx = phase_c_repro_gate.PUBLISHED_LT_CMAP["convnext"]
    ast = phase_c_repro_gate.PUBLISHED_LT_CMAP["ast"]
    assert cnx == {
        "POW": 0.36, "PER": 0.19, "NES": 0.34, "UHH": 0.26,
        "HSN": 0.47, "NBP": 0.62, "SSW": 0.35, "SNE": 0.30,
    }
    assert ast == {
        "POW": 0.33, "PER": 0.18, "NES": 0.32, "UHH": 0.21,
        "HSN": 0.44, "NBP": 0.61, "SSW": 0.33, "SNE": 0.28,
    }


# ---------------------------------------------------------------------------
# compare_to_published: tolerance boundary, NaN, unknown keys
# ---------------------------------------------------------------------------


def test_compare_within_tolerance_passes():
    ok, published, delta = phase_c_repro_gate.compare_to_published(
        "convnext", "HSN", 0.44
    )
    assert ok is True
    assert published == pytest.approx(0.47)
    assert delta == pytest.approx(-0.03)


def test_compare_out_of_tolerance_fails():
    ok, _, delta = phase_c_repro_gate.compare_to_published(
        "convnext", "HSN", 0.30
    )
    assert ok is False
    assert delta == pytest.approx(-0.17)


@pytest.mark.parametrize(
    "model,test_set,published",
    [
        ("convnext", "HSN", 0.47),  # 0.47+0.05 carries float dust > 0.05
        ("ast", "UHH", 0.21),  # 0.21+0.05 likewise
        ("convnext", "POW", 0.36),
        ("ast", "PER", 0.18),
    ],
)
def test_boundary_delta_of_exactly_005_passes(model, test_set, published):
    """|Δ| = 0.05 is INclusive — including when float representation
    makes `published + 0.05 - published` land a few ULP above 0.05
    (e.g. 0.47 + 0.05: delta == 0.05000000000000004). A measured pass
    must never flip to FAIL on representation dust."""
    ok, _, _ = phase_c_repro_gate.compare_to_published(
        model, test_set, published + 0.05
    )
    assert ok is True
    ok_low, _, _ = phase_c_repro_gate.compare_to_published(
        model, test_set, published - 0.05
    )
    assert ok_low is True


def test_boundary_delta_of_00501_fails():
    ok, _, _ = phase_c_repro_gate.compare_to_published(
        "convnext", "POW", 0.36 + 0.0501
    )
    assert ok is False
    ok_low, _, _ = phase_c_repro_gate.compare_to_published(
        "convnext", "POW", 0.36 - 0.0501
    )
    assert ok_low is False


def test_compare_nan_cmap_is_never_ok():
    ok, published, delta = phase_c_repro_gate.compare_to_published(
        "ast", "SSW", float("nan")
    )
    assert ok is False
    assert published == pytest.approx(0.33)
    assert np.isnan(delta)


def test_compare_unknown_model_raises_naming_it():
    with pytest.raises(ValueError, match="perchzilla"):
        phase_c_repro_gate.compare_to_published("perchzilla", "POW", 0.3)


def test_compare_unknown_test_set_raises_naming_it():
    with pytest.raises(ValueError, match="XYZ"):
        phase_c_repro_gate.compare_to_published("convnext", "XYZ", 0.3)


# ---------------------------------------------------------------------------
# gate_pair_cmap: zero-rows refusal, NaN passthrough
# ---------------------------------------------------------------------------


def test_zero_eval_rows_refused_loudly():
    scores = np.zeros((0, 3), dtype=np.float32)
    y = np.zeros((0, 3), dtype=np.int64)
    with pytest.raises(ValueError, match="[Zz]ero"):
        phase_c_repro_gate.gate_pair_cmap(scores, y)


def test_gate_pair_cmap_returns_nan_when_nothing_scoreable():
    """Rows exist but no class has a positive: NaN rides through to the
    comparison, which renders it FAIL — never a crash, never a pass."""
    scores = np.random.default_rng(0).normal(size=(4, 3)).astype(np.float32)
    y = np.zeros((4, 3), dtype=np.int64)
    value, n_scored = phase_c_repro_gate.gate_pair_cmap(scores, y)
    assert np.isnan(value)
    assert n_scored == 0


# ---------------------------------------------------------------------------
# Output lines and verdict
# ---------------------------------------------------------------------------


def test_format_gate_line_contract():
    line = phase_c_repro_gate.format_gate_line("convnext", "HSN", 0.44)
    assert line.startswith("PHASE_C_REPRO convnext HSN ")
    assert "ours=0.440" in line
    assert "published=0.47" in line
    assert "delta=-0.030" in line
    assert "FAIL" not in line


def test_format_gate_line_fail_marked():
    line = phase_c_repro_gate.format_gate_line("convnext", "HSN", 0.20)
    assert line.startswith("PHASE_C_REPRO convnext HSN ")
    assert "FAIL" in line


def test_format_gate_line_nan_renders_fail_not_grammar_break():
    line = phase_c_repro_gate.format_gate_line("ast", "SSW", float("nan"))
    assert "FAIL" in line
    assert "ours=nan" in line


def test_render_report_all_pass():
    text, ok = phase_c_repro_gate.render_report(
        [("convnext", "HSN", 0.44), ("ast", "SSW", 0.33)]
    )
    assert ok is True
    assert text.splitlines()[-1] == "PHASE_C_REPRO_OK"
    assert text.count("PHASE_C_REPRO ") == 2


def test_render_report_one_miss_fails():
    text, ok = phase_c_repro_gate.render_report(
        [("convnext", "HSN", 0.44), ("ast", "SSW", 0.10)]
    )
    assert ok is False
    assert text.splitlines()[-1] == "PHASE_C_REPRO_FAIL"


def test_render_report_zero_pairs_is_vacuous_fail():
    """An empty pair list gates nothing; OK would be a vacuous pass."""
    text, ok = phase_c_repro_gate.render_report([])
    assert ok is False
    assert text.splitlines()[-1] == "PHASE_C_REPRO_FAIL"


# ---------------------------------------------------------------------------
# FiLM identity subcommand — the check runs on any checkpoint's weights
# ---------------------------------------------------------------------------


def _identity_fixtures(seed: int = 0):
    model = tiny_convnext(seed)
    torch.manual_seed(seed + 1)
    encoder = ContextEncoder(N_CLASSES, d_ctx=8)
    wrapper = FiLMConvNext(model, encoder)
    torch.manual_seed(seed + 2)
    inputs = torch.randn(2, 1, 32, 32)
    context = torch.randn(2, N_CLASSES)
    return model, wrapper, inputs, context


def test_film_identity_check_passes_at_zero_init():
    model, wrapper, inputs, context = _identity_fixtures()
    ok, max_delta = phase_c_repro_gate.film_identity_check(
        model, wrapper, inputs, context
    )
    assert ok is True
    assert max_delta == 0.0


def test_film_identity_check_fails_on_randomized_heads():
    """The mutation pin: randomly initialized γ/β heads must FAIL the
    gate — otherwise the gate is vacuous (spec §5)."""
    model, wrapper, inputs, context = _identity_fixtures()
    torch.manual_seed(99)
    torch.nn.init.normal_(wrapper.heads[0].weight)
    torch.nn.init.normal_(wrapper.heads[0].bias)
    ok, max_delta = phase_c_repro_gate.film_identity_check(
        model, wrapper, inputs, context
    )
    assert ok is False
    assert max_delta > 0.0


def test_film_identity_check_runs_on_checkpoint_loaded_weights(tmp_path):
    """The fleet hands the check a real checkpoint path; here the same
    code path runs on weights round-tripped through torch.save — the
    check is a function of the LOADED model, not of how it was built."""
    donor = tiny_convnext(seed=5)
    ckpt = tmp_path / "ckpt.pt"
    torch.save(donor.state_dict(), ckpt)

    model = tiny_convnext(seed=0)  # different init...
    model.load_state_dict(torch.load(ckpt, weights_only=True))  # ...overwritten
    torch.manual_seed(1)
    wrapper = FiLMConvNext(model, ContextEncoder(N_CLASSES, d_ctx=8))
    torch.manual_seed(2)
    ok, max_delta = phase_c_repro_gate.film_identity_check(
        model, wrapper, torch.randn(2, 1, 32, 32), torch.randn(2, N_CLASSES)
    )
    assert ok is True
    assert max_delta == 0.0


# ---------------------------------------------------------------------------
# Import safety: argparse must live under __main__ in both scripts
# ---------------------------------------------------------------------------


def test_gate_module_is_import_safe():
    assert hasattr(phase_c_repro_gate, "main")
    assert callable(phase_c_repro_gate.main)


def test_run_module_is_import_safe():
    assert hasattr(phase_c_run, "main")
    assert callable(phase_c_run.main)


# ---------------------------------------------------------------------------
# phase_c_run: the results-row provenance refusal + the one-row write
# ---------------------------------------------------------------------------


def _full_provenance() -> dict:
    return {
        "git_sha": "abc123",
        "checkpoint_revision": "deadbeefcafe",
        "prior_doi": "10.5281/zenodo.0000000",
        "prior_variant": "full",
        "manifest_path": "/srv/home/rapnic/birdology/data/phase_a/XCM_r4_manifest.parquet",
        "torch_version": "2.6.0",
        "cuda_version": "12.4",
        # Schema v3 (job 5730): the device the run actually used —
        # cuda_version above is only the torch build's CUDA.
        "device": "cuda:0",
        "device_name": "NVIDIA H100 80GB HBM3",
        "arm": "film",
        "model_id": "DBD-research-group/ConvNeXT-Base-BirdSet-XCL",
        "context_source": "eod_r4_full",
    }


def _row(provenance: dict) -> dict:
    return phase_c_run.build_row(
        provenance=provenance,
        test_set="HSN",
        resolution=4,
        seed=3,
        cmap_value=0.41,
        n_scored=42,
        val_cmap_history=[0.30, 0.33],
        epochs_ran=2,
    )


def test_row_refused_when_checkpoint_revision_missing():
    provenance = _full_provenance()
    del provenance["checkpoint_revision"]
    with pytest.raises(ValueError, match="checkpoint_revision"):
        _row(provenance)


@pytest.mark.parametrize(
    "field",
    [
        "git_sha", "checkpoint_revision", "prior_doi", "prior_variant",
        "manifest_path",
        "torch_version", "cuda_version", "device", "device_name",
        "arm", "model_id",
        "context_source",
    ],
)
def test_row_refused_when_any_provenance_field_empty(field):
    provenance = _full_provenance()
    provenance[field] = "   "
    with pytest.raises(ValueError, match=field):
        _row(provenance)


def test_row_refused_on_unknown_arm():
    provenance = _full_provenance()
    provenance["arm"] = "placebo"
    with pytest.raises(ValueError, match="placebo"):
        _row(provenance)


def test_write_run_row_writes_one_parquet_row_with_provenance(tmp_path):
    row = _row(_full_provenance())
    out = phase_c_run.write_run_row(row, tmp_path)
    assert out.exists()
    table = pq.read_table(out)
    assert table.num_rows == 1
    got = table.to_pylist()[0]
    for key, value in _full_provenance().items():
        assert got[key] == value
    assert got["test_set"] == "HSN"
    assert got["seed"] == 3
    assert got["cmap"] == pytest.approx(0.41)


# ---------------------------------------------------------------------------
# sbatch: the three measured silent-failure constraints, grepped
# ---------------------------------------------------------------------------


def test_sbatch_pins_the_three_measured_constraints():
    """Plan mandate: --gres=gpu:1 (no GPU inside the container without
    it), /raid/scratch staging (5.3 GB/s local vs ~300 MB/s shared), and
    the birdology-home bind (an unbound home is an EMPTY DIR in the
    container — 0 files, exit 0)."""
    sbatch = (
        _REPO / "scripts" / "cluster" / "phase_c_train.sbatch"
    ).read_text()
    assert "--gres=gpu:1" in sbatch
    assert "/raid/scratch" in sbatch
    assert '--bind "$BIRDOLOGY_ROOT":"$BIRDOLOGY_ROOT"' in sbatch


def test_sbatch_binds_home_and_guards_its_visibility():
    sbatch = (
        _REPO / "scripts" / "cluster" / "phase_c_train.sbatch"
    ).read_text()
    assert '[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] ||' in sbatch
    assert "--nv" in sbatch
    assert "pytorch.sif" in sbatch


# ---------------------------------------------------------------------------
# I1 (final review): the prior-variant axis. The 135 control runs need a
# PRIOR_VARIANT column; its vocabulary is the ACTUAL npz suffixes
# build_prior.py writes ({stem}_full / _marginal / _shuffle_space /
# _shuffle_time — CONTROLS' own keys), not respelled names.
# ---------------------------------------------------------------------------


def test_prior_variant_vocabulary_matches_build_prior_suffixes():
    from birdology.prior.controls import CONTROLS

    # uniform ≡ twin (spec decision 4) and separable is not a phase_c
    # control arm; the three scheduled controls + full must be spelled
    # exactly as the npz files on disk are.
    assert set(phase_c_run.PRIOR_VARIANTS) == {
        "full", "marginal", "shuffle_space", "shuffle_time"
    }
    assert {"marginal", "shuffle_space", "shuffle_time"} <= set(CONTROLS)


def test_row_carries_prior_variant():
    provenance = _full_provenance()
    provenance["prior_variant"] = "shuffle_space"
    row = _row(provenance)
    assert row["prior_variant"] == "shuffle_space"


def test_row_refused_on_unknown_prior_variant():
    provenance = _full_provenance()
    provenance["prior_variant"] = "space_shuffled"  # the respelling trap
    with pytest.raises(ValueError, match="space_shuffled"):
        _row(provenance)


def test_row_refused_when_prior_variant_missing():
    provenance = _full_provenance()
    del provenance["prior_variant"]
    with pytest.raises(ValueError, match="prior_variant"):
        _row(provenance)


def test_sbatch_derives_prior_path_from_variant_column():
    """I1: the sbatch must read PRIOR_VARIANT from the TSV and use it in
    the npz path — a hardcoded _full would make the 135 control runs
    inexpressible."""
    sbatch = (
        _REPO / "scripts" / "cluster" / "phase_c_train.sbatch"
    ).read_text()
    assert "PRIOR_VARIANT" in sbatch
    assert "prior_r${RESOLUTION}_${PRIOR_VARIANT}.npz" in sbatch
    assert "prior_r${RESOLUTION}_full.npz" not in sbatch
    assert "--prior-variant" in sbatch


def test_sbatch_trains_once_and_loops_test_sets():
    """I2: CKPT_DIR must not embed a test set, and the run must receive
    the plural --test-sets."""
    sbatch = (
        _REPO / "scripts" / "cluster" / "phase_c_train.sbatch"
    ).read_text()
    assert "--test-sets" in sbatch
    assert "${TEST_SET}" not in sbatch.split("CKPT_DIR=")[1].splitlines()[0]


def test_sbatch_eval_path_is_context_joined_not_raw_metadata():
    """C1: the eval path must be the context-joined table, never the raw
    BirdSet metadata parquet (which has no cell/week columns)."""
    sbatch = (
        _REPO / "scripts" / "cluster" / "phase_c_train.sbatch"
    ).read_text()
    assert "eval_ctx" in sbatch
    assert "_metadata_test_5s.parquet" not in sbatch


# ---------------------------------------------------------------------------
# I3 (final review): `score` subcommand — the gate's npz producer. The
# pin is the FORMAT round-trip: score writes exactly what gate reads.
# ---------------------------------------------------------------------------


def test_score_then_gate_round_trip(tmp_path, monkeypatch):
    """score writes scores+y npz from a checkpoint + eval parquet; gate
    parses that exact file. Verdict is irrelevant — format is the pin."""
    import pyarrow as pa
    import soundfile as sf

    # tiny saved model
    model = tiny_convnext()
    ckpt = tmp_path / "ckpt"
    model.save_pretrained(ckpt)

    # tiny eval parquet + audio
    audio_root = tmp_path / "audio"
    audio_root.mkdir()
    rng = np.random.default_rng(0)
    for name in ("e0.wav", "e1.wav"):
        sf.write(
            audio_root / name,
            rng.standard_normal(16000).astype(np.float32) * 0.1,
            32000,
        )
    eval_pq = tmp_path / "eval.parquet"
    pq.write_table(
        pa.table(
            {
                "filepath": pa.array(["e0.wav", "e1.wav"], pa.string()),
                "ebird_code_multilabel": pa.array(
                    [["LABEL_0"], ["LABEL_1"]], pa.list_(pa.string())
                ),
            }
        ),
        eval_pq,
    )

    npz_path = tmp_path / "scores.npz"
    rc = phase_c_repro_gate.main(
        [
            "score",
            "--model", "convnext",
            "--checkpoint", str(ckpt),
            "--eval-metadata", str(eval_pq),
            "--audio-root", str(audio_root),
            "--out", str(npz_path),
        ]
    )
    assert rc == 0
    assert npz_path.exists()

    data = np.load(npz_path)
    assert set(data.files) >= {"scores", "y"}
    assert data["scores"].shape == data["y"].shape

    # gate parses the npz score just wrote; verdict value irrelevant
    rc = phase_c_repro_gate.main(
        ["gate", "--pair", f"convnext:HSN:{npz_path}"]
    )
    assert rc in (0, 1)
