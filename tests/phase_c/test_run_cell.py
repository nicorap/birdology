"""Task 6 fix round 1: _run_cell evaluates the BEST checkpoint, not last.

THE BUG THESE TESTS WERE WRITTEN RED AGAINST
--------------------------------------------
``train_arm`` early-stops on POW-val cmAP and returns
``TrainResult.best_state_path`` — but the original ``_run_cell`` never
restored it: it handed the trainee to ``evaluate_model`` carrying the
LAST epoch's weights, which with patience-based early stopping are
≥patience epochs PAST the val best. Every fleet row would silently
report a degraded cmAP, and the frozen-checkpoint repro gate cannot
catch it (no training happens there).

The contract pinned here: before ``evaluate_model`` runs, the trainee's
state_dict must bit-equal the ``best.pt`` checkpoint (``torch.equal``
per entry) and must NOT equal the last-epoch state whenever the two
differ. The checkpoint is saved from the TRAINEE's own perspective
(``train_arm`` checkpoints ``model_or_wrapper.state_dict()`` of exactly
the object it received — the bare HF model for the twin, the wrapper
for conditioned arms), so both arms are pinned: twin AND film.

Also here: the AST cell wiring. ``_run_cell`` selects the frontend with
the model (convnext → ``convnext_frontend``, ast → ``ast_frontend``,
golden-gated in test_data.py) and guards the AST input contract against
the checkpoint's own config (max_length/num_mel_bins) at load — a
mismatch refuses by name before any training spend.

Heavy fleet dependencies (from_pretrained, the prior, the context
matrix) are monkeypatched at their source modules — ``_run_cell``
imports them lazily at call time, so source-module patches land.
Training itself is REAL (real train_arm, real optimizer steps, real
checkpoints); only ``train._validate`` is forced onto a decreasing
sequence so best (epoch 0) != last (epoch 1) by construction.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import soundfile as sf
import torch
from transformers import ConvNextConfig, ConvNextForImageClassification

_REPO = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_REPO / "src"))
sys.path.insert(0, str(_REPO / "scripts"))

import phase_c_run
from birdology.phase_c.train import _validate as _REAL_VALIDATE

N_CLASSES = 5
#: tiny_convnext's default id2label values — the model's class axis.
CLASS_CODES = [f"LABEL_{i}" for i in range(N_CLASSES)]
SR = 32000


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


class FakeProvider:
    """ContextProvider stand-in: zeros + absent, length = model axis."""

    def __init__(self, n_classes: int) -> None:
        self.n_classes = n_classes

    def vector(self, cell, week):
        return np.zeros(self.n_classes, dtype=np.float32), 0.0


def _write_manifest(path: Path, filepaths: list[str]) -> None:
    n = len(filepaths)
    pq.write_table(
        pa.table(
            {
                "filepath": pa.array(filepaths, pa.string()),
                "ebird_code": pa.array(
                    [CLASS_CODES[i % N_CLASSES] for i in range(n)],
                    pa.string(),
                ),
                "cell": pa.array([None] * n, pa.string()),
                "week": pa.array([None] * n, pa.int64()),
            }
        ),
        path,
    )


def _write_eval_metadata(path: Path, *, with_context: bool = True) -> None:
    cols: dict = {
        "filepath": pa.array(["e0.wav", "e1.wav"], pa.string()),
        "ebird_code_multilabel": pa.array(
            [["LABEL_0"], ["LABEL_1"]], pa.list_(pa.string())
        ),
    }
    if with_context:
        cols["cell"] = pa.array(["8428309ffffffff", None], pa.string())
        cols["week"] = pa.array([12, None], pa.int64())
    pq.write_table(pa.table(cols), path)


#: The tiny POW-val soundscape: multi-label, an empty window, null
#: context throughout — phase_c_build_eval_ctx.py --allow-undated's
#: output shape (filepath, ebird_code_multilabel, cell, week).
VAL_FILES = ["v0.wav", "v1.wav", "v2.wav"]
VAL_LABELS = [["LABEL_0", "LABEL_2"], [], ["LABEL_1"]]


def _write_val_soundscape(
    path: Path, labels: list[list[str]] | None = None
) -> None:
    labels = VAL_LABELS if labels is None else labels
    n = len(labels)
    pq.write_table(
        pa.table(
            {
                "filepath": pa.array(VAL_FILES[:n], pa.string()),
                "ebird_code_multilabel": pa.array(
                    labels, pa.list_(pa.string())
                ),
                "cell": pa.array([None] * n, pa.string()),
                "week": pa.array([None] * n, pa.int8()),
            }
        ),
        path,
    )


def _write_pow_manifest_shape(path: Path, filepaths: list[str]) -> None:
    """The MEASURED /faic/weka/ML/phase_a/POW_r4_manifest.parquet shape
    that killed fleet job 5679: every ebird_code == '' and cell/week
    None (POW is a soundscape set; the focal manifest has no label)."""
    n = len(filepaths)
    pq.write_table(
        pa.table(
            {
                "filepath": pa.array(filepaths, pa.string()),
                "cell": pa.array([None] * n, pa.string()),
                "week": pa.array([None] * n, pa.int64()),
                "ebird_code": pa.array([""] * n, pa.string()),
                "split": pa.array(["test_5s"] * n, pa.string()),
                "lat": pa.array([None] * n, pa.float64()),
                "lon": pa.array([None] * n, pa.float64()),
            }
        ),
        path,
    )


def _write_audio(root: Path, names: list[str]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(0)
    for name in names:
        sf.write(
            root / name,
            rng.standard_normal(16000).astype(np.float32) * 0.1,
            SR,
        )


def _cell_args(
    tmp_path: Path,
    arm: str,
    model: str = "convnext",
    test_sets: list[str] | None = None,
    eval_with_context: bool = True,
    allow_null_context: bool = False,
    prior_variant: str = "full",
):
    test_sets = test_sets or ["HSN"]
    train_files = [f"t{i}.wav" for i in range(4)]
    audio_root = tmp_path / "audio"
    _write_audio(audio_root, train_files + VAL_FILES + ["e0.wav", "e1.wav"])
    _write_manifest(tmp_path / "manifest.parquet", train_files)
    _write_val_soundscape(tmp_path / "val.parquet")
    for ts in test_sets:
        _write_eval_metadata(
            tmp_path / f"eval_{ts}.parquet", with_context=eval_with_context
        )
    (tmp_path / "bridge.json").write_text(json.dumps({}))
    return SimpleNamespace(
        arm=arm,
        model=model,
        checkpoint="tiny/checkpoint",
        checkpoint_revision="deadbeef",
        resolution=4,
        seed=0,
        test_sets=test_sets,
        manifest=str(tmp_path / "manifest.parquet"),
        val_metadata=str(tmp_path / "val.parquet"),
        eval_metadata_template=str(tmp_path / "eval_{test_set}.parquet"),
        audio_root=str(audio_root),
        prior=str(tmp_path / "prior.npz"),
        prior_doi="10.5281/zenodo.0000000",
        prior_variant=prior_variant,
        bridge=str(tmp_path / "bridge.json"),
        context_source="eod2022",
        git_sha="f" * 40,
        ckpt_dir=str(tmp_path / "ckpt"),
        out=str(tmp_path / "out"),
        d_ctx=8,
        allow_null_context=allow_null_context,
        # Fleet job 5730: the device is EXPLICIT. The Mac/CPU harness
        # says so; the fleet default is cuda (and refuses without it).
        device="cpu",
        num_workers=0,
        log_every=50,
    )


@pytest.fixture
def fleet_stubs(monkeypatch):
    """Patch the fleet-only dependencies at their SOURCE modules (lazy
    imports in _run_cell pick these up) and force best != last: real
    training, but val goes 1.0 → 0.5 so epoch 0 is best and epoch 1
    (patience=1) still runs its optimizer steps before the stop."""
    import birdology.phase_c.context as context_mod
    import birdology.phase_c.evaluate as evaluate_mod
    import birdology.phase_c.train as train_mod
    import birdology.prior.tensor as prior_mod

    monkeypatch.setattr(
        ConvNextForImageClassification,
        "from_pretrained",
        classmethod(lambda cls, *a, **k: tiny_convnext()),
    )
    monkeypatch.setattr(prior_mod, "load_prior", lambda path: object())
    # CPU-only torch carries torch.version.cuda = None; the provenance
    # refusal is not what these tests probe, so pin a value.
    monkeypatch.setattr(torch.version, "cuda", "12.1", raising=False)
    monkeypatch.setattr(
        context_mod,
        "build_context_matrix",
        lambda prior, codes, bridge: FakeProvider(len(codes)),
    )

    vals = iter([1.0, 0.5])
    monkeypatch.setattr(
        train_mod, "_validate", lambda *a, **k: next(vals)
    )

    captured: dict = {}

    def capturing_evaluate(model_or_wrapper, eval_ds, *, arm, **kwargs):
        captured["state"] = {
            k: v.detach().clone()
            for k, v in model_or_wrapper.state_dict().items()
        }
        captured["eval_ds"] = eval_ds
        rng = np.random.default_rng(1)
        return rng.standard_normal(
            (len(eval_ds), len(eval_ds.class_codes))
        ).astype(np.float32)

    monkeypatch.setattr(evaluate_mod, "evaluate_model", capturing_evaluate)
    return captured


def _states_equal(a: dict, b: dict) -> bool:
    return set(a) == set(b) and all(torch.equal(a[k], b[k]) for k in a)


@pytest.mark.parametrize("arm", ["twin", "film"])
def test_run_cell_evaluates_best_checkpoint_not_last_epoch(
    tmp_path, fleet_stubs, arm
):
    """The state_dict evaluate_model sees must be best.pt's, not the
    last epoch's — for the bare twin AND for the wrapper-perspective
    film checkpoint."""
    args = _cell_args(tmp_path, arm=arm)
    assert phase_c_run._run_cell(args) == 0

    ckpt_dir = Path(args.ckpt_dir)
    best = torch.load(ckpt_dir / "best.pt", weights_only=True)["model_state"]
    last = torch.load(ckpt_dir / "last.pt", weights_only=False)["model_state"]

    # The scenario is only probative if early stopping actually left
    # the model past its best: epoch 1 trained after the epoch-0 best.
    assert not _states_equal(best, last), (
        "test harness failure: best == last, the scenario cannot "
        "discriminate"
    )

    seen = fleet_stubs["state"]
    assert _states_equal(seen, best), (
        "evaluate_model received weights that are NOT the best "
        "checkpoint's — _run_cell must restore best_state_path before "
        "evaluating (early stopping leaves the trainee ≥patience epochs "
        "past the val best)"
    )
    assert not _states_equal(seen, last)


def test_run_cell_refuses_ast_until_frontend_is_wired(tmp_path, monkeypatch):
    """SUPERSEDED, kept as the historical RED anchor: --model ast used
    to refuse with NotImplementedError before ANY checkpoint load.
    ast_frontend is now wired and golden-gated
    (tests/phase_c/test_data.py::TestAstFrontend), so the NEW pinned
    behavior lives in TestAstCell below — this name stays only so a
    grep for the old refusal finds the story, and it now asserts the
    blanket refusal is GONE: the cell proceeds to checkpoint
    resolution, whose failure (stubbed offline here) propagates
    instead."""
    from transformers import ASTForAudioClassification

    def unresolvable(cls, *a, **k):
        raise OSError("tiny/checkpoint is not a local folder (stubbed)")

    monkeypatch.setattr(
        ASTForAudioClassification,
        "from_pretrained",
        classmethod(unresolvable),
    )
    args = _cell_args(tmp_path, arm="twin", model="ast")
    with pytest.raises(OSError, match="tiny/checkpoint"):
        phase_c_run._run_cell(args)


# ---------------------------------------------------------------------------
# The AST cell: ast_frontend wired end-to-end, input-contract guard at load.
# ---------------------------------------------------------------------------


def tiny_ast(
    seed: int = 0, *, max_length: int = 1024, num_mel_bins: int = 128
):
    from transformers import ASTConfig, ASTForAudioClassification

    torch.manual_seed(seed)
    config = ASTConfig(
        hidden_size=24,
        num_hidden_layers=2,
        num_attention_heads=2,
        intermediate_size=32,
        patch_size=16,
        frequency_stride=10,
        time_stride=10,
        max_length=max_length,
        num_mel_bins=num_mel_bins,
        num_labels=N_CLASSES,
    )
    return ASTForAudioClassification(config)


class TestAstCell:
    def test_ast_twin_runs_with_ast_frontend(
        self, tmp_path, fleet_stubs, monkeypatch
    ):
        """--model ast trains and writes a row; both the train dataset
        and the eval dataset carry ast_frontend — never the convnext
        mel."""
        from transformers import ASTForAudioClassification

        from birdology.phase_c.data import ast_frontend

        monkeypatch.setattr(
            ASTForAudioClassification,
            "from_pretrained",
            classmethod(lambda cls, *a, **k: tiny_ast()),
        )
        args = _cell_args(tmp_path, arm="twin", model="ast")
        assert phase_c_run._run_cell(args) == 0

        files = sorted(Path(args.out).glob("*.parquet"))
        assert len(files) == 1
        row = pq.read_table(files[0]).to_pylist()[0]
        assert row["model_id"] == "tiny/checkpoint"

        eval_ds = fleet_stubs["eval_ds"]
        assert eval_ds.frontend is ast_frontend, (
            "the eval dataset was built with a frontend other than "
            "ast_frontend — AST would be scored on convnext mels"
        )

    @pytest.mark.parametrize(
        "bad_kwargs, fragment",
        [
            ({"max_length": 512}, "max_length"),
            ({"num_mel_bins": 64}, "num_mel_bins"),
        ],
    )
    def test_ast_refuses_on_input_contract_mismatch(
        self, tmp_path, fleet_stubs, monkeypatch, bad_kwargs, fragment
    ):
        """The checkpoint's own config is the input contract:
        ast_frontend emits (1024, 128), so a checkpoint expecting a
        different (max_length, num_mel_bins) must refuse BY NAME at
        load — before any training spend — not shape-crash mid-epoch."""
        from transformers import ASTForAudioClassification

        monkeypatch.setattr(
            ASTForAudioClassification,
            "from_pretrained",
            classmethod(lambda cls, *a, **k: tiny_ast(**bad_kwargs)),
        )
        args = _cell_args(tmp_path, arm="twin", model="ast")
        with pytest.raises(ValueError, match=fragment):
            phase_c_run._run_cell(args)
        # Refused before training: no checkpoints, no rows.
        assert not list(Path(args.out).glob("*.parquet"))
        assert not (Path(args.ckpt_dir) / "best.pt").exists()


# ---------------------------------------------------------------------------
# I2 (final review): one invocation trains ONCE and evaluates on EVERY
# requested test set — one row per test set. Retraining per test set
# would 6x the training budget (~1080 GPU-h vs the spec's ~600).
# ---------------------------------------------------------------------------


def test_two_test_sets_train_once_two_rows(tmp_path, fleet_stubs, monkeypatch):
    import birdology.phase_c.train as train_mod

    calls = {"train": 0}
    real_train_arm = train_mod.train_arm

    def counting_train_arm(*a, **k):
        calls["train"] += 1
        return real_train_arm(*a, **k)

    monkeypatch.setattr(train_mod, "train_arm", counting_train_arm)

    args = _cell_args(tmp_path, arm="twin", test_sets=["HSN", "SSW"])
    assert phase_c_run._run_cell(args) == 0

    assert calls["train"] == 1, (
        f"train_arm ran {calls['train']} times for 2 test sets — the "
        "cell must train ONCE and loop eval (finding I2)"
    )
    files = sorted(Path(args.out).glob("*.parquet"))
    assert len(files) == 2
    rows = [pq.read_table(f).to_pylist()[0] for f in files]
    assert sorted(r["test_set"] for r in rows) == ["HSN", "SSW"]
    assert all(r["prior_variant"] == "full" for r in rows)


# ---------------------------------------------------------------------------
# C1 wiring: conditioned arms REQUIRE a context-joined eval table;
# --allow-null-context is the deliberate escape hatch; the twin never
# requires context.
# ---------------------------------------------------------------------------


def test_conditioned_arm_refuses_all_null_context_eval(
    tmp_path, fleet_stubs
):
    from birdology.phase_c.evaluate import AllNullContextError

    args = _cell_args(tmp_path, arm="film", eval_with_context=False)
    with pytest.raises(AllNullContextError, match="C1"):
        phase_c_run._run_cell(args)


def test_conditioned_arm_allow_null_context_escape_hatch(
    tmp_path, fleet_stubs
):
    args = _cell_args(
        tmp_path, arm="film", eval_with_context=False,
        allow_null_context=True,
    )
    assert phase_c_run._run_cell(args) == 0


def test_twin_arm_never_requires_context(tmp_path, fleet_stubs):
    args = _cell_args(tmp_path, arm="twin", eval_with_context=False)
    assert phase_c_run._run_cell(args) == 0


def test_provenance_cuda_version_is_cpu_on_gpuless_stack(monkeypatch):
    """torch.version.cuda is None on CPU-only builds — that is REAL provenance
    (the run had no CUDA) and is recorded as "cpu", never "" (which would trip
    build_row's refusal and lock GPU-less probes out of writing rows).
    Measured: the 2026-10-01 Mac end-to-end run refused after 49 minutes of
    successful training at exactly this seam."""
    import argparse

    import torch

    import phase_c_run as pcr

    monkeypatch.setattr(torch.version, "cuda", None)
    args = argparse.Namespace(
        git_sha="abc1234",
        prior_doi="10.1/x",
        prior_variant="full",
        manifest="m.parquet",
        checkpoint="org/model",
        context_source="p.npz",
        arm="twin",
    )
    prov = pcr._stack_provenance(
        args, resolved_revision="deadbeef", device=torch.device("cpu")
    )
    assert prov["cuda_version"] == "cpu"
    for v in prov.values():
        assert isinstance(v, str) and v.strip()


def test_provenance_cuda_version_passes_through_when_present(monkeypatch):
    import argparse

    import torch

    import phase_c_run as pcr

    monkeypatch.setattr(torch.version, "cuda", "12.8")
    args = argparse.Namespace(
        git_sha="abc1234", prior_doi="10.1/x", prior_variant="full",
        manifest="m.parquet", checkpoint="org/model",
        context_source="p.npz", arm="film",
    )
    prov = pcr._stack_provenance(args, "deadbeef", device=torch.device("cpu"))
    assert prov["cuda_version"] == "12.8"


# ---------------------------------------------------------------------------
# Fleet job 5679 (all 10 twin cells): `ValMetricError: val set is empty`.
# POW is a multi-label SOUNDSCAPE set; the val set was built as the focal
# single-label CropDataset from POW_r4_manifest.parquet, whose every row
# has ebird_code == '' — so CropDataset dropped all 4,560 rows. Spec §2:
# POW-val goes through the soundscape path (build_eval_dataset over an
# ebird_code_multilabel table, null context) and is scored multi-hot.
# ---------------------------------------------------------------------------


@pytest.fixture
def capture_train_arm(monkeypatch):
    """Spy on train_arm's val_ds (real training underneath)."""
    import birdology.phase_c.train as train_mod

    seen: dict = {}
    real = train_mod.train_arm

    def spy(trainee, train_ds, val_ds, cfg, ckpt_dir):
        seen["val_ds"] = val_ds
        seen["train_ds"] = train_ds
        return real(trainee, train_ds, val_ds, cfg, ckpt_dir)

    monkeypatch.setattr(train_mod, "train_arm", spy)
    return seen


def test_old_crop_dataset_path_drops_every_pow_manifest_row(tmp_path):
    """The bug, reproduced: the POW manifest shape fed to CropDataset
    (the OLD val wiring) yields an EMPTY val set — every row's
    ebird_code is '' and '' is on no model axis."""
    from birdology.phase_c.data import CropDataset

    path = tmp_path / "POW_r4_manifest.parquet"
    _write_pow_manifest_shape(path, VAL_FILES)
    ds = CropDataset(
        pq.read_table(path),
        lambda f: np.zeros(SR, dtype=np.float32),
        FakeProvider(N_CLASSES),
        {code: i for i, code in enumerate(CLASS_CODES)},
    )
    assert len(ds) == 0
    assert ds.skipped_codes == {"": len(VAL_FILES)}


def test_val_is_the_multilabel_soundscape_eval_dataset(
    tmp_path, fleet_stubs, capture_train_arm
):
    """The NEW path: --val-metadata (a soundscape table) becomes an
    EvalDataset with every row kept and multi-hot y on the model axis
    — incl. the empty window's all-zero row."""
    from birdology.phase_c.evaluate import EvalDataset

    args = _cell_args(tmp_path, arm="twin")
    assert phase_c_run._run_cell(args) == 0

    val_ds = capture_train_arm["val_ds"]
    assert isinstance(val_ds, EvalDataset)
    assert len(val_ds) == len(VAL_FILES)
    assert val_ds.y.tolist() == [
        [1, 0, 1, 0, 0],
        [0, 0, 0, 0, 0],
        [0, 1, 0, 0, 0],
    ]
    # null context is SPEC'D for POW: the twin never requires it, and
    # neither may the conditioned arms' val.


def test_conditioned_arm_val_accepts_all_null_context(
    tmp_path, fleet_stubs, capture_train_arm
):
    """POW is all-undated (spec §1/§2): even a conditioned arm that
    REQUIRES context at eval must run POW-val on the null path."""
    args = _cell_args(tmp_path, arm="film")  # eval table has context
    assert phase_c_run._run_cell(args) == 0
    assert len(capture_train_arm["val_ds"]) == len(VAL_FILES)


def test_pow_manifest_as_val_metadata_refuses_by_name(tmp_path, fleet_stubs):
    """Pointing --val-metadata at the focal POW manifest (no
    ebird_code_multilabel) refuses by name, before training, pointing at
    the builder — never a bare KeyError, never an empty val set."""
    args = _cell_args(tmp_path, arm="twin")
    _write_pow_manifest_shape(Path(args.val_metadata), VAL_FILES)
    with pytest.raises(ValueError, match="ebird_code_multilabel") as exc:
        phase_c_run._run_cell(args)
    assert "phase_c_build_eval_ctx" in str(exc.value)
    assert args.val_metadata in str(exc.value)
    assert not (Path(args.ckpt_dir) / "best.pt").exists()


def test_val_table_with_no_positive_on_axis_refuses_naming_file(
    tmp_path, fleet_stubs
):
    """A val set with no positive on the model axis cannot rank: refuse
    with ValMetricError naming the file (labels off-axis count as none)."""
    from birdology.phase_c.train import ValMetricError

    args = _cell_args(tmp_path, arm="twin")
    _write_val_soundscape(
        Path(args.val_metadata), [[], ["NOT_ON_AXIS"], []]
    )
    with pytest.raises(ValMetricError, match="positive") as exc:
        phase_c_run._run_cell(args)
    assert args.val_metadata in str(exc.value)
    assert not (Path(args.ckpt_dir) / "best.pt").exists()


def test_empty_val_table_refuses_naming_file(tmp_path, fleet_stubs):
    from birdology.phase_c.train import ValMetricError

    args = _cell_args(tmp_path, arm="twin")
    _write_val_soundscape(Path(args.val_metadata), [])
    with pytest.raises(ValMetricError, match="empty") as exc:
        phase_c_run._run_cell(args)
    assert args.val_metadata in str(exc.value)


def test_ast_frontend_reaches_val_and_eval_datasets(
    tmp_path, fleet_stubs, capture_train_arm, monkeypatch
):
    """model=ast: a SPY wrapping ast_frontend is the frontend of the val
    dataset AND of the eval dataset, and is actually invoked on val
    audio during real validation."""
    import birdology.phase_c.data as data_mod
    import birdology.phase_c.train as train_mod
    from transformers import ASTForAudioClassification

    calls = {"n": 0}
    real_ast_frontend = data_mod.ast_frontend

    def spy_frontend(wave):
        calls["n"] += 1
        return real_ast_frontend(wave)

    monkeypatch.setattr(data_mod, "ast_frontend", spy_frontend)
    monkeypatch.setattr(
        ASTForAudioClassification,
        "from_pretrained",
        classmethod(lambda cls, *a, **k: tiny_ast()),
    )
    monkeypatch.setattr(train_mod, "_validate", _REAL_VALIDATE)

    args = _cell_args(tmp_path, arm="twin", model="ast")
    assert phase_c_run._run_cell(args) == 0

    assert capture_train_arm["val_ds"].frontend is spy_frontend
    assert capture_train_arm["train_ds"].frontend is spy_frontend
    assert fleet_stubs["eval_ds"].frontend is spy_frontend
    # train (4 rows x epochs) + val (3 rows x epochs): the spy ran.
    assert calls["n"] >= len(VAL_FILES)


def test_twin_end_to_end_with_real_multilabel_validation(
    tmp_path, fleet_stubs, monkeypatch
):
    """The job-5679 cell shape, end to end: twin, soundscape POW-val
    scored by the REAL _validate (multi-hot cmAP), rows written with a
    finite val history."""
    import birdology.phase_c.train as train_mod

    monkeypatch.setattr(train_mod, "_validate", _REAL_VALIDATE)
    args = _cell_args(tmp_path, arm="twin", test_sets=["HSN", "SSW"])
    assert phase_c_run._run_cell(args) == 0

    files = sorted(Path(args.out).glob("*.parquet"))
    assert len(files) == 2
    for f in files:
        row = pq.read_table(f).to_pylist()[0]
        hist = row["val_cmap_history"]
        assert 1 <= len(hist) <= 2
        assert all(np.isfinite(v) and 0.0 < v <= 1.0 for v in hist)


# ---------------------------------------------------------------------------
# Fleet job 5730: 10 twin cells sat 2h43m at ~1300% CPU with the GPU at 0%
# util — _run_cell never moved the trainee off the CPU, and train_arm /
# evaluate_model inherit the device from the parameters. The device is now
# EXPLICIT (--device, default cuda) and a cuda request on a GPU-less stack
# REFUSES before any heavy work — never a silent CPU fallback.
# ---------------------------------------------------------------------------


class TestExplicitDevice:
    def test_cuda_requested_but_unavailable_refuses_before_any_loading(
        self, tmp_path, fleet_stubs, monkeypatch
    ):
        """No checkpoint load, no dataset construction, no training: the
        refusal is the FIRST thing _run_cell does, and it names the two
        measured causes (missing --gres, CPU-only torch build)."""
        import birdology.phase_c.data as data_mod

        monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
        touched: list[str] = []
        monkeypatch.setattr(
            ConvNextForImageClassification,
            "from_pretrained",
            classmethod(lambda cls, *a, **k: touched.append("load")),
        )

        class _NoCrop:
            def __init__(self, *a, **k):
                touched.append("CropDataset")

        monkeypatch.setattr(data_mod, "CropDataset", _NoCrop)

        args = _cell_args(tmp_path, arm="twin")
        args.device = "cuda"
        with pytest.raises(RuntimeError) as exc:
            phase_c_run._run_cell(args)
        msg = str(exc.value)
        assert "--gres" in msg
        assert "CPU-only" in msg
        assert "--device cpu" in msg
        assert touched == [], f"heavy work ran before the refusal: {touched}"
        assert not (Path(args.ckpt_dir) / "best.pt").exists()
        assert not list(Path(args.out).glob("*.parquet"))

    def test_cuda_index_beyond_device_count_refuses(self, monkeypatch):
        monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
        monkeypatch.setattr(torch.cuda, "device_count", lambda: 1)
        with pytest.raises(RuntimeError, match="cuda:3"):
            phase_c_run._resolve_device("cuda:3")

    def test_cpu_resolves_to_cpu(self):
        assert phase_c_run._resolve_device("cpu") == torch.device("cpu")

    def test_trainee_is_moved_to_the_parsed_device_before_training(
        self, tmp_path, fleet_stubs, monkeypatch
    ):
        """Module.to is called on the TRAINEE with the parsed device, and
        that happens before train_arm sees it (spy on both)."""
        import birdology.phase_c.train as train_mod

        events: list = []
        real_to = torch.nn.Module.to

        def spy_to(self, *a, **k):
            events.append(("to", type(self).__name__, a, k))
            return real_to(self, *a, **k)

        monkeypatch.setattr(torch.nn.Module, "to", spy_to)
        real_train_arm = train_mod.train_arm

        def spy_train_arm(trainee, *a, **k):
            events.append(("train_arm", type(trainee).__name__))
            return real_train_arm(trainee, *a, **k)

        monkeypatch.setattr(train_mod, "train_arm", spy_train_arm)

        args = _cell_args(tmp_path, arm="film")
        assert phase_c_run._run_cell(args) == 0
        to_calls = [e for e in events if e[0] == "to"]
        assert ("to", "FiLMConvNext", (torch.device("cpu"),), {}) in to_calls
        first_to = events.index(
            ("to", "FiLMConvNext", (torch.device("cpu"),), {})
        )
        assert first_to < events.index(("train_arm", "FiLMConvNext"))

    def test_device_mismatch_after_move_refuses(
        self, tmp_path, fleet_stubs, monkeypatch
    ):
        """If .to() silently left the parameters elsewhere (the job-5730
        shape), the post-move assertion refuses before training."""
        monkeypatch.setattr(
            torch.nn.Module, "to", lambda self, *a, **k: self
        )
        monkeypatch.setattr(
            phase_c_run, "_resolve_device", lambda spec: torch.device("meta")
        )
        args = _cell_args(tmp_path, arm="twin")
        with pytest.raises(RuntimeError, match="meta"):
            phase_c_run._run_cell(args)
        assert not (Path(args.ckpt_dir) / "best.pt").exists()

    @pytest.mark.skipif(
        not torch.cuda.is_available(), reason="needs a real CUDA device"
    )
    def test_real_cuda_cell_trains_on_gpu_and_says_so(
        self, tmp_path, fleet_stubs
    ):
        args = _cell_args(tmp_path, arm="twin")
        args.device = "cuda"
        assert phase_c_run._run_cell(args) == 0
        row = pq.read_table(next(Path(args.out).glob("*.parquet"))).to_pylist()[0]
        assert row["device"].startswith("cuda:")
        assert row["device_name"] == torch.cuda.get_device_name(0)


# ---------------------------------------------------------------------------
# Honest provenance (job 5730): cuda_version is the torch BUILD's CUDA
# (12.9 on the fleet SIF even when the run sat on CPU). The row now names
# the device the run ACTUALLY used, so a CPU run can never produce a row
# indistinguishable from a GPU run.
# ---------------------------------------------------------------------------


def _prov_args(arm="twin"):
    import argparse

    return argparse.Namespace(
        git_sha="abc1234", prior_doi="10.1/x", prior_variant="full",
        manifest="m.parquet", checkpoint="org/model",
        context_source="p.npz", arm=arm,
    )


class TestDeviceProvenance:
    def test_cpu_run_on_a_cuda_build_says_cpu(self, monkeypatch):
        """The job-5730 lie: CUDA build (12.9), CPU device. The row must
        carry device='cpu' next to the build's cuda_version."""
        monkeypatch.setattr(torch.version, "cuda", "12.9")
        prov = phase_c_run._stack_provenance(
            _prov_args(), "deadbeef", device=torch.device("cpu")
        )
        assert prov["cuda_version"] == "12.9"
        assert prov["device"] == "cpu"
        assert prov["device_name"].startswith("cpu")

    def test_cuda_run_records_index_and_gpu_name(self, monkeypatch):
        monkeypatch.setattr(
            torch.cuda, "get_device_name", lambda d=None: "NVIDIA H100 80GB HBM3"
        )
        prov = phase_c_run._stack_provenance(
            _prov_args(), "deadbeef", device=torch.device("cuda", 0)
        )
        assert prov["device"] == "cuda:0"
        assert prov["device_name"] == "NVIDIA H100 80GB HBM3"

    @pytest.mark.parametrize("fname", ["device", "device_name"])
    def test_row_refuses_without_device_provenance(self, fname):
        prov = phase_c_run._stack_provenance(
            _prov_args(), "deadbeef", device=torch.device("cpu")
        )
        prov[fname] = " "
        with pytest.raises(ValueError, match=fname):
            phase_c_run.build_row(
                provenance=prov, test_set="HSN", resolution=4, seed=0,
                cmap_value=0.5, n_scored=3, val_cmap_history=[0.5],
                epochs_ran=1,
            )

    def test_schema_version_bumped_and_device_columns_present(self):
        names = phase_c_run._RESULTS_SCHEMA.names
        assert "device" in names and "device_name" in names
        assert "cuda_version" in names  # build info, kept
        assert phase_c_run.RESULTS_SCHEMA_VERSION == "3"
        assert phase_c_run._RESULTS_SCHEMA.metadata[
            b"phase_c_schema_version"
        ] == b"3"

    def test_run_cell_row_carries_the_device_actually_used(
        self, tmp_path, fleet_stubs
    ):
        """fleet_stubs pins torch.version.cuda='12.1' (a CUDA BUILD); the
        cell ran on CPU, and the row must say so."""
        args = _cell_args(tmp_path, arm="twin")
        assert phase_c_run._run_cell(args) == 0
        row = pq.read_table(
            next(Path(args.out).glob("*.parquet"))
        ).to_pylist()[0]
        assert row["cuda_version"] == "12.1"
        assert row["device"] == "cpu"
        assert row["device_name"].startswith("cpu")


# ---------------------------------------------------------------------------
# The CLI surface for the job-5730 fix: --device (default cuda),
# --num-workers (default 8 on cuda, 0 on cpu), --log-every (default 50),
# and logging configured so birdology INFO lines reach STDOUT (the Slurm
# .out file) — the fleet log had none.
# ---------------------------------------------------------------------------

_REQUIRED_ARGV = [
    "--arm", "twin", "--model", "convnext", "--checkpoint", "x",
    "--checkpoint-revision", "r", "--resolution", "4", "--seed", "0",
    "--test-sets", "HSN", "--manifest", "m", "--val-metadata", "v",
    "--eval-metadata-template", "{test_set}", "--audio-root", "a",
    "--prior", "p", "--prior-doi", "d", "--prior-variant", "full",
    "--bridge", "b", "--context-source", "c", "--git-sha", "g",
    "--ckpt-dir", "k", "--out", "o",
]


@pytest.fixture
def restore_birdology_logger():
    import logging

    lg = logging.getLogger("birdology")
    saved = (list(lg.handlers), lg.level, lg.propagate)
    yield lg
    lg.handlers[:] = saved[0]
    lg.setLevel(saved[1])
    lg.propagate = saved[2]


@pytest.fixture
def parsed(monkeypatch, restore_birdology_logger):
    """main(argv) with _run_cell replaced by a recorder."""
    seen: dict = {}

    def fake_run_cell(args):
        seen["args"] = args
        return 0

    monkeypatch.setattr(phase_c_run, "_run_cell", fake_run_cell)

    def run(*extra):
        assert phase_c_run.main(_REQUIRED_ARGV + list(extra)) == 0
        return seen["args"]

    return run


class TestCli:
    def test_device_defaults_to_cuda(self, parsed):
        assert parsed().device == "cuda"

    def test_num_workers_default_is_8_on_cuda(self, parsed):
        assert parsed().num_workers == 8

    def test_num_workers_default_is_0_on_cpu(self, parsed):
        assert parsed("--device", "cpu").num_workers == 0

    def test_num_workers_explicit_wins(self, parsed):
        assert parsed("--device", "cpu", "--num-workers", "3").num_workers == 3

    def test_log_every_default_and_override(self, parsed):
        assert parsed().log_every == 50
        assert parsed("--log-every", "7").log_every == 7

    def test_main_routes_birdology_info_to_stdout(
        self, monkeypatch, capsys, restore_birdology_logger
    ):
        import logging

        def run_cell_that_logs(args):
            logging.getLogger("birdology.phase_c.train").info("HEARTBEAT 42")
            return 0

        monkeypatch.setattr(phase_c_run, "_run_cell", run_cell_that_logs)
        assert phase_c_run.main(_REQUIRED_ARGV + ["--device", "cpu"]) == 0
        assert "HEARTBEAT 42" in capsys.readouterr().out

    def test_logging_config_is_idempotent(self, restore_birdology_logger):
        phase_c_run._configure_logging()
        n = len(restore_birdology_logger.handlers)
        phase_c_run._configure_logging()
        assert len(restore_birdology_logger.handlers) == n


def test_run_cell_threads_num_workers_and_log_every_into_training(
    tmp_path, fleet_stubs, capture_train_arm, monkeypatch
):
    import birdology.phase_c.evaluate as evaluate_mod
    import birdology.phase_c.train as train_mod

    seen: dict = {}
    real = train_mod.train_arm

    def spy(trainee, train_ds, val_ds, cfg, ckpt_dir):
        seen["cfg"] = cfg
        return real(trainee, train_ds, val_ds, cfg, ckpt_dir)

    monkeypatch.setattr(train_mod, "train_arm", spy)
    fake_eval = evaluate_mod.evaluate_model

    def eval_spy(model, ds, *, arm, **kw):
        seen["eval_kw"] = kw
        return fake_eval(model, ds, arm=arm, **kw)

    monkeypatch.setattr(evaluate_mod, "evaluate_model", eval_spy)
    args = _cell_args(tmp_path, arm="twin")
    args.num_workers = 0
    args.log_every = 3
    assert phase_c_run._run_cell(args) == 0
    assert seen["cfg"].log_every == 3
    assert seen["cfg"].num_workers == 0
    assert seen["eval_kw"]["num_workers"] == 0
