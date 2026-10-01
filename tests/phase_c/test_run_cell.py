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

Also here: the loud AST refusal. ``_run_cell`` hard-wires
``convnext_frontend``; feeding its (1, 128, 501) mel into AST would
shape-crash or garbage-score at GPU cost, so ``--model ast`` must
refuse by name until an ``ast_frontend`` is wired and golden-gated
(task-6-report concern 1).

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
    val_files = ["v0.wav", "v1.wav"]
    audio_root = tmp_path / "audio"
    _write_audio(audio_root, train_files + val_files + ["e0.wav", "e1.wav"])
    _write_manifest(tmp_path / "manifest.parquet", train_files)
    _write_manifest(tmp_path / "val.parquet", val_files)
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


def test_run_cell_refuses_ast_until_frontend_is_wired(tmp_path):
    """--model ast must refuse LOUDLY: the hard-wired convnext_frontend
    (1, 128, 501) mel is not AST's input contract (BirdSet AST uses
    mel→resize-1024 with sign-flip std), and the gap must not be
    discovered at GPU cost."""
    args = _cell_args(tmp_path, arm="twin", model="ast")
    with pytest.raises(NotImplementedError, match="ast_frontend not yet wired"):
        phase_c_run._run_cell(args)


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
