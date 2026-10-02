"""Task 5: the training loop — twins, two-group optimizer, loud val, resume.

The contract under test (plan Task 5, spec §1 Trainability + §2 val):

* ``TrainConfig(arm, seed, lr=1e-4, backbone_lr_scale=0.1, max_epochs=2,
  patience=1, batch_size=64)`` — ``arm`` ∈ {"film", "token", "twin"},
  anything else refused at construction.
* ``build_optimizer(model_or_wrapper, cfg)`` — AdamW. Conditioned arms:
  TWO param groups, backbone (the wrapper's inner ``model``) at
  ``lr*backbone_lr_scale``, conditioning modules (everything on the
  wrapper that is NOT the inner model) at full ``lr``. Twin: ONE group,
  all params at ``lr*backbone_lr_scale`` — the controller's ruling: the
  twin's backbone trains at the IDENTICAL rate as the conditioned runs'
  backbone group, so the twin differs ONLY by absence of conditioning.
* ``train_arm(model_or_wrapper, train_ds, val_ds, cfg, ckpt_dir)`` →
  ``TrainResult(val_cmap_history, best_state_path, epochs_ran, resumed)``.
* Batch order is a pure function of (cfg.seed, epoch): twin and
  conditioned runs with the same seed see the SAME index order (pinned
  via recording datasets).
* The loop SETS ``train_ds.epoch`` each epoch — crop offsets are
  (seed, epoch, idx)-keyed in data.py and the trainer owns the epoch dial.
* Early stop: ``patience`` epochs without val improvement, ``max_epochs``
  cap.
* Review Focus 4: a val set yielding NaN/empty cmAP ABORTS with
  ``ValMetricError`` — never trains to max silently, never checkpoints
  the poisoned epoch.
* Review Focus 5: resume after preemption restores model + optimizer +
  epoch + RNG; a resumed run is BIT-EQUAL (torch.equal on every
  state_dict entry) to an uninterrupted run.

Tiny models from configs (test_film's pattern), 32-row synthetic
datasets honouring CropDataset's item contract (including the PYTHON
float ``present`` that default collate widens to float64 — the trainer
must cast). CPU only, no network.
"""
from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest
import torch
from torch import nn
from transformers import (
    ASTConfig,
    ASTForAudioClassification,
    ConvNextConfig,
    ConvNextForImageClassification,
)

from birdology.phase_c.context import ContextEncoder
from birdology.phase_c.film import FiLMConvNext
from birdology.phase_c.token import TokenAST
from tests.phase_c._loader_fixtures import (
    FixtureProvider,
    SyntheticDataset,
    fixture_audio,
)
from birdology.phase_c.train import (
    TrainConfig,
    ValMetricError,
    build_optimizer,
    train_arm,
)

N_CLASSES = 5
D_CTX = 8
CONVNEXT_SHAPE = (1, 32, 32)
AST_SHAPE = (24, 16)

#: Drop-path rate for the RNG-consuming model variants. The default tiny
#: models consume ZERO global torch RNG per training step (no dropout, no
#: drop_path), which makes RNG-restore and generator-isolation tests
#: vacuously green. Stochastic depth draws ``torch.rand`` from the GLOBAL
#: generator every training forward, so tests built with this rate are
#: actually load-bearing on the RNG save point and on shuffle isolation.
#: (With depths=[1,1] HF linspaces the rate over layers as [0.0, 0.3] —
#: the second stage's layer is the active consumer.)
DROP_PATH = 0.3


@pytest.fixture(autouse=True)
def _deterministic():
    """Pin determinism for the bit-equality assertions in this module."""
    prev = torch.are_deterministic_algorithms_enabled()
    torch.use_deterministic_algorithms(True)
    yield
    torch.use_deterministic_algorithms(prev)


# ---------------------------------------------------------------------------
# Tiny models (test_film's config pattern) and synthetic datasets.
# ---------------------------------------------------------------------------


def tiny_convnext(
    drop_path_rate: float = 0.0,
) -> ConvNextForImageClassification:
    config = ConvNextConfig(
        num_channels=1,
        num_stages=2,
        hidden_sizes=[8, 16],
        depths=[1, 1],
        num_labels=N_CLASSES,
        image_size=32,
        drop_path_rate=drop_path_rate,
    )
    return ConvNextForImageClassification(config)


def tiny_ast(n_layers: int = 2) -> ASTForAudioClassification:
    config = ASTConfig(
        hidden_size=16,
        num_hidden_layers=n_layers,
        num_attention_heads=2,
        intermediate_size=32,
        num_labels=N_CLASSES,
        max_length=24,
        num_mel_bins=16,
    )
    return ASTForAudioClassification(config)


def film_convnext(seed: int = 0, drop_path_rate: float = 0.0) -> FiLMConvNext:
    torch.manual_seed(seed)
    return FiLMConvNext(
        tiny_convnext(drop_path_rate), ContextEncoder(N_CLASSES, d_ctx=D_CTX)
    )


def token_ast(seed: int = 0) -> TokenAST:
    torch.manual_seed(seed)
    return TokenAST(tiny_ast(), ContextEncoder(N_CLASSES, d_ctx=D_CTX))


def bare_convnext(
    seed: int = 0, drop_path_rate: float = 0.0
) -> ConvNextForImageClassification:
    torch.manual_seed(seed)
    return tiny_convnext(drop_path_rate)


def _cfg(**kw) -> TrainConfig:
    defaults = dict(arm="film", seed=0, batch_size=8, max_epochs=2, patience=1)
    defaults.update(kw)
    return TrainConfig(**defaults)


# ---------------------------------------------------------------------------
# TrainConfig: the arm vocabulary and the contract's defaults.
# ---------------------------------------------------------------------------


class TestTrainConfig:
    def test_rejects_unknown_arm(self):
        with pytest.raises(ValueError, match="arm"):
            TrainConfig(arm="lstm", seed=0)

    @pytest.mark.parametrize("arm", ["film", "token", "twin"])
    def test_accepts_the_three_arms(self, arm):
        assert TrainConfig(arm=arm, seed=0).arm == arm

    def test_contract_defaults(self):
        cfg = TrainConfig(arm="film", seed=0)
        assert cfg.lr == 1e-4
        assert cfg.backbone_lr_scale == 0.1
        assert cfg.max_epochs == 2
        assert cfg.patience == 1
        assert cfg.batch_size == 64


# ---------------------------------------------------------------------------
# Optimizer param groups: backbone at lr*scale, conditioning at lr; the
# twin's single group at the SAME backbone rate (controller ruling).
# ---------------------------------------------------------------------------


class TestOptimizerGroups:
    def test_film_two_groups_backbone_scaled(self):
        wrapper = film_convnext()
        cfg = _cfg(arm="film")
        opt = build_optimizer(wrapper, cfg)
        assert isinstance(opt, torch.optim.AdamW)
        assert len(opt.param_groups) == 2
        backbone, conditioning = opt.param_groups
        assert backbone["lr"] == pytest.approx(cfg.lr * cfg.backbone_lr_scale)
        assert conditioning["lr"] == pytest.approx(cfg.lr)
        assert {id(p) for p in backbone["params"]} == {
            id(p) for p in wrapper.model.parameters()
        }
        expected_cond = {id(p) for p in wrapper.heads.parameters()} | {
            id(p) for p in wrapper.context_encoder.parameters()
        }
        assert {id(p) for p in conditioning["params"]} == expected_cond

    def test_token_two_groups_conditioning_is_wrapper_trainables(self):
        wrapper = token_ast()
        cfg = _cfg(arm="token")
        opt = build_optimizer(wrapper, cfg)
        assert len(opt.param_groups) == 2
        backbone, conditioning = opt.param_groups
        assert {id(p) for p in backbone["params"]} == {
            id(p) for p in wrapper.model.parameters()
        }
        expected_cond = (
            {id(wrapper.null_token)}
            | {id(p) for p in wrapper.project.parameters()}
            | {id(p) for p in wrapper.context_encoder.parameters()}
        )
        assert {id(p) for p in conditioning["params"]} == expected_cond

    def test_twin_single_group_at_backbone_rate(self):
        """The ruling: the twin's whole model trains at lr*backbone_lr_scale
        — identical to the conditioned runs' backbone group — so the twin
        differs ONLY by absence of conditioning."""
        model = bare_convnext()
        cfg = _cfg(arm="twin")
        opt = build_optimizer(model, cfg)
        assert len(opt.param_groups) == 1
        (group,) = opt.param_groups
        assert group["lr"] == pytest.approx(cfg.lr * cfg.backbone_lr_scale)
        assert {id(p) for p in group["params"]} == {
            id(p) for p in model.parameters()
        }

    def test_every_trainable_param_is_in_exactly_one_group(self):
        wrapper = film_convnext()
        opt = build_optimizer(wrapper, _cfg(arm="film"))
        ids = [id(p) for g in opt.param_groups for p in g["params"]]
        assert len(ids) == len(set(ids)), "a param appears in two groups"
        assert set(ids) == {id(p) for p in wrapper.parameters()}


# ---------------------------------------------------------------------------
# Twin/conditioned parity: identical seed → identical batch order.
# ---------------------------------------------------------------------------


class TestBatchOrderParity:
    def test_twin_and_film_same_seed_same_order_across_epochs(self, tmp_path):
        """Multi-epoch parity with a global-RNG consumer in ONE arm only.

        The film model carries active drop_path (consumes global torch
        RNG every training step); the twin consumes none. If the shuffle
        leaked onto the global RNG the two arms' generators would be in
        DIFFERENT states by the time the epoch-1 permutation is drawn
        (film burned extra draws during epoch 0), so epoch-1 orders would
        diverge — epoch 0 alone could never catch this, because both arms
        enter it from the same ``torch.manual_seed(cfg.seed)``. Verified
        discriminating: with ``generator=None`` (global RNG) in the
        train DataLoader, this test fails on epoch 1.
        """
        film_rec: list = []
        twin_rec: list = []
        film_train = SyntheticDataset(record=film_rec)
        twin_train = SyntheticDataset(record=twin_rec)
        film_val = SyntheticDataset(n_rows=16, seed=99)
        twin_val = SyntheticDataset(n_rows=16, seed=99)

        train_arm(
            film_convnext(seed=1, drop_path_rate=DROP_PATH),
            film_train,
            film_val,
            _cfg(arm="film", seed=7, max_epochs=2, patience=10),
            tmp_path / "film",
        )
        train_arm(
            bare_convnext(seed=1),
            twin_train,
            twin_val,
            _cfg(arm="twin", seed=7, max_epochs=2, patience=10),
            tmp_path / "twin",
        )

        orders = {}
        for epoch in (0, 1):
            film_order = [i for (e, i) in film_rec if e == epoch]
            twin_order = [i for (e, i) in twin_rec if e == epoch]
            assert film_order, f"epoch {epoch} never ran for the film arm"
            assert film_order == twin_order, (
                f"twin and conditioned run with the same seed saw DIFFERENT "
                f"batch orders in epoch {epoch} — the shuffle is reading "
                "the global RNG and the twin is no longer a controlled "
                "comparison"
            )
            # And the order is actually shuffled, not an unshuffled range.
            assert film_order != sorted(film_order)
            orders[epoch] = film_order
        # Different epochs reshuffle: the order is (seed, epoch)-keyed,
        # not frozen at epoch 0.
        assert orders[0] != orders[1]


# ---------------------------------------------------------------------------
# The loop owns the epoch dial: train_ds.epoch is SET each epoch.
# ---------------------------------------------------------------------------


class TestEpochKeying:
    def test_loop_sets_dataset_epoch_per_epoch(self, tmp_path):
        rec: list = []
        train_ds = SyntheticDataset(record=rec, epoch=123)  # stale epoch
        val_ds = SyntheticDataset(n_rows=16, seed=99)
        train_arm(
            film_convnext(),
            train_ds,
            val_ds,
            _cfg(max_epochs=2, patience=5),
            tmp_path,
        )
        epochs_seen = [e for (e, _) in rec]
        n = len(train_ds)
        assert epochs_seen[:n] == [0] * n, "epoch 0 not set before epoch 0"
        assert epochs_seen[n : 2 * n] == [1] * n, "epoch 1 not set"


# ---------------------------------------------------------------------------
# Early stop: patience non-improving epochs, max_epochs cap.
# ---------------------------------------------------------------------------


class TestEarlyStop:
    def test_stops_after_patience_without_improvement(self, tmp_path):
        """lr=0 freezes the weights → val identical every epoch → epoch 1
        cannot improve on epoch 0 → stop at 2 epochs, far below max."""
        result = train_arm(
            film_convnext(),
            SyntheticDataset(),
            SyntheticDataset(n_rows=16, seed=99),
            _cfg(lr=0.0, max_epochs=5, patience=1),
            tmp_path,
        )
        assert result.epochs_ran == 2
        assert len(result.val_cmap_history) == 2
        assert result.val_cmap_history[0] == result.val_cmap_history[1]

    def test_max_epochs_caps_the_run(self, tmp_path):
        result = train_arm(
            film_convnext(),
            SyntheticDataset(),
            SyntheticDataset(n_rows=16, seed=99),
            _cfg(max_epochs=2, patience=10),
            tmp_path,
        )
        assert result.epochs_ran == 2
        assert result.resumed is False


# ---------------------------------------------------------------------------
# Review Focus 4: NaN/empty val ABORTS loudly, never trains to max.
# ---------------------------------------------------------------------------


class _NaNTwin(nn.Module):
    """A twin whose logits are all-NaN — the diverged-training shape."""

    def __init__(self) -> None:
        super().__init__()
        self.lin = nn.Linear(4, N_CLASSES)

    def forward(self, inputs: torch.Tensor) -> SimpleNamespace:
        logits = self.lin(inputs.flatten(1)[:, :4]) * float("nan")
        return SimpleNamespace(logits=logits)


class TestLoudVal:
    def test_empty_val_set_raises_named_error(self, tmp_path):
        with pytest.raises(ValMetricError, match="empty"):
            train_arm(
                film_convnext(),
                SyntheticDataset(),
                SyntheticDataset(n_rows=0),
                _cfg(),
                tmp_path,
            )

    def test_nan_cmap_aborts_before_checkpointing(self, tmp_path, monkeypatch):
        """A NaN metric must raise IMMEDIATELY — and the poisoned epoch is
        never checkpointed, so a requeue cannot resume into garbage."""
        import birdology.phase_c.train as train_mod

        monkeypatch.setattr(train_mod, "cmap", lambda s, y: (float("nan"), 0))
        with pytest.raises(ValMetricError):
            train_arm(
                film_convnext(),
                SyntheticDataset(),
                SyntheticDataset(n_rows=16, seed=99),
                _cfg(max_epochs=3),
                tmp_path,
            )
        assert not (tmp_path / "last.pt").exists(), (
            "the NaN-val epoch was checkpointed — a requeued job would "
            "resume into a poisoned state"
        )

    def test_nan_logits_abort(self, tmp_path):
        with pytest.raises(ValMetricError):
            train_arm(
                _NaNTwin(),
                SyntheticDataset(),
                SyntheticDataset(n_rows=16, seed=99),
                _cfg(arm="twin"),
                tmp_path,
            )


# ---------------------------------------------------------------------------
# POW-val is a MULTI-LABEL soundscape set (fleet job 5679): _validate
# scores multi-hot ground truth from the val item's "labels" row — zero
# or more species per 5 s window, all-zero rows legitimate.
# ---------------------------------------------------------------------------


class _MultiHotValDataset(torch.utils.data.Dataset):
    """EvalDataset's item contract: inputs/context/present + a multi-hot
    ``labels`` row on the model axis (NO single-int ``label``)."""

    def __init__(self, y: np.ndarray, seed: int = 4) -> None:
        self.y = y
        self.seed = seed

    def __len__(self) -> int:
        return self.y.shape[0]

    def __getitem__(self, idx: int) -> dict:
        rng = np.random.default_rng((self.seed, idx))
        return {
            "inputs": torch.from_numpy(
                rng.standard_normal(CONVNEXT_SHAPE).astype(np.float32)
            ),
            "context": torch.zeros(N_CLASSES),
            "present": 0.0,
            "labels": torch.from_numpy(self.y[idx]),
        }


class TestMultiHotValidate:
    def test_validate_scores_multi_hot_with_all_zero_row_sklearn_exact(self):
        """Multi-label windows (2 species), a single-species window and an
        EMPTY window (all-zero row): _validate's cmAP equals sklearn's
        per-class AP mean over the hand-built matrix (classes with no
        positive excluded — cmap's n_scored rule)."""
        from sklearn.metrics import average_precision_score
        from torch.utils.data import DataLoader

        from birdology.phase_c.train import _validate

        y = np.array(
            [
                [1, 1, 0, 0, 0],  # two species in one window
                [0, 0, 0, 0, 0],  # empty soundscape window
                [0, 1, 0, 1, 0],
                [1, 0, 0, 0, 0],
                [0, 0, 0, 0, 0],  # another empty window
                [0, 1, 0, 1, 0],
            ],
            dtype=np.int64,
        )
        ds = _MultiHotValDataset(y)
        model = bare_convnext(seed=2)
        cfg = _cfg(arm="twin", batch_size=4)  # 2 batches: concat path

        got = _validate(
            model, DataLoader(ds, batch_size=4), cfg, torch.device("cpu")
        )

        model.eval()
        with torch.no_grad():
            scores = model(
                torch.stack([ds[i]["inputs"] for i in range(len(ds))])
            ).logits.numpy()
        aps = [
            average_precision_score(y[:, j], scores[:, j])
            for j in range(N_CLASSES)
            if y[:, j].any()
        ]
        assert len(aps) == 3
        assert got == pytest.approx(float(np.mean(aps)), abs=1e-12)


# ---------------------------------------------------------------------------
# Review Focus 5: preemption resume is bit-exact — and provably so.
#
# All resume tests use drop_path-active models: stochastic depth draws from
# the GLOBAL torch RNG every training step, so the checkpoint's RNG save
# point is load-bearing — a wrong save/restore point breaks bit-equality
# instead of passing vacuously (the plain tiny models consume zero RNG).
# ---------------------------------------------------------------------------


def _resume_train(model, cfg, ckpt_dir):
    """One train_arm call on the shared synthetic data used by TestResume."""
    return train_arm(
        model,
        SyntheticDataset(),
        SyntheticDataset(n_rows=16, seed=99),
        cfg,
        ckpt_dir,
    )


def _differing_keys(model_a: nn.Module, model_b: nn.Module) -> list[str]:
    a, b = model_a.state_dict(), model_b.state_dict()
    assert a.keys() == b.keys()
    return [k for k in a if not torch.equal(a[k], b[k])]


class TestResume:
    #: The run seed (batch order, global RNG at fresh start) and the two
    #: init seeds. Leg 2 constructs a FRESH model from INIT_SEED_OTHER —
    #: a different init than the reference — so the checkpoint is the
    #: ONLY path back to the uninterrupted run's weights: if model-state
    #: restore (or the RNG restore, with drop_path active) were broken,
    #: bit-equality could not hold by accident of object reuse.
    RUN_SEED = 3
    INIT_SEED = 5
    INIT_SEED_OTHER = 11

    def _straight(self, tmp_path):
        """The uninterrupted 2-epoch reference run."""
        model = film_convnext(seed=self.INIT_SEED, drop_path_rate=DROP_PATH)
        result = _resume_train(
            model,
            _cfg(arm="film", seed=self.RUN_SEED, max_epochs=2),
            tmp_path / "straight",
        )
        return model, result

    def _leg1(self, tmp_path):
        """Epoch 0 of the preempted run; the model object then 'dies'."""
        model = film_convnext(seed=self.INIT_SEED, drop_path_rate=DROP_PATH)
        result = _resume_train(
            model,
            _cfg(arm="film", seed=self.RUN_SEED, max_epochs=1),
            tmp_path / "preempt",
        )
        return result

    def test_resumed_fresh_model_bit_equal_to_uninterrupted(self, tmp_path):
        """Train 2 epochs straight vs: train 1 epoch, DIE (the model
        object is gone), construct a FRESH model from a DIFFERENT init
        seed, re-call train_arm on the same ckpt_dir. Every state_dict
        entry must be torch.equal — which can only happen if the
        checkpoint's model, optimizer AND RNG states were all restored
        (drop_path consumes global RNG each step, so a wrong RNG save
        point alone breaks this). The two companion tests below prove
        this assertion can fail.
        """
        straight, res_straight = self._straight(tmp_path)
        res_leg1 = self._leg1(tmp_path)

        # A fresh process: new model, different init — nothing survives
        # from leg 1 except ckpt_dir.
        fresh = film_convnext(
            seed=self.INIT_SEED_OTHER, drop_path_rate=DROP_PATH
        )
        assert _differing_keys(fresh, straight), (
            "the fresh leg-2 model accidentally matches the reference "
            "init — the test would prove nothing"
        )
        res_leg2 = _resume_train(
            fresh,
            _cfg(arm="film", seed=self.RUN_SEED, max_epochs=2),
            tmp_path / "preempt",
        )

        assert res_straight.resumed is False
        assert res_leg1.resumed is False
        assert res_leg2.resumed is True
        assert res_leg1.epochs_ran == 1
        assert res_leg2.epochs_ran == res_straight.epochs_ran
        assert res_leg2.val_cmap_history == res_straight.val_cmap_history

        diffs = _differing_keys(straight, fresh)
        assert not diffs, (
            f"state_dict entries {diffs[:5]!r} differ after resume — the "
            "resumed run is NOT bit-equal to the uninterrupted one"
        )

    def test_without_the_checkpoint_the_fresh_model_diverges(self, tmp_path):
        """Discriminating-power control for the headline test: the SAME
        fresh model (different init seed) trained in an EMPTY ckpt_dir —
        identical config, data and run seed, no resume — must NOT end
        bit-equal to the reference. So when the headline test observes
        equality, the checkpoint restore is the only possible cause; the
        assertion is shown capable of failing.
        """
        straight, _ = self._straight(tmp_path)
        fresh = film_convnext(
            seed=self.INIT_SEED_OTHER, drop_path_rate=DROP_PATH
        )
        result = _resume_train(
            fresh,
            _cfg(arm="film", seed=self.RUN_SEED, max_epochs=2),
            tmp_path / "no-ckpt",  # empty: nothing to resume from
        )
        assert result.resumed is False
        assert _differing_keys(straight, fresh), (
            "a differently-initialised, never-resumed run ended bit-equal "
            "to the reference — the bit-equality assertion has no "
            "discriminating power"
        )

    def test_noop_rng_restore_breaks_bit_equality(self, tmp_path, monkeypatch):
        """Sabotage pin: with _rng_restore no-op'd, leg 2 draws its
        drop_path masks from whatever global RNG state model construction
        left behind instead of the checkpointed state — so the final
        weights must differ from the reference. Proves the RNG save
        point is genuinely load-bearing in the headline test (it was not
        before drop_path: the old tiny models consumed zero RNG and this
        sabotage passed unnoticed).
        """
        import birdology.phase_c.train as train_mod

        straight, _ = self._straight(tmp_path)
        self._leg1(tmp_path)

        monkeypatch.setattr(
            train_mod, "_rng_restore", lambda ckpt, device: None
        )
        fresh = film_convnext(
            seed=self.INIT_SEED_OTHER, drop_path_rate=DROP_PATH
        )
        _resume_train(
            fresh,
            _cfg(arm="film", seed=self.RUN_SEED, max_epochs=2),
            tmp_path / "preempt",
        )
        assert _differing_keys(straight, fresh), (
            "resume stayed bit-equal with RNG restore disabled — the "
            "resume test is not actually exercising the RNG save point"
        )

    def test_resume_past_max_epochs_is_a_silent_noop(self, tmp_path):
        """MINOR-3 pin for Task 6: resuming a checkpoint that already
        covers max_epochs trains nothing and raises nothing — it returns
        resumed=True with epochs_ran == the checkpointed epoch count and
        an unchanged val history.
        """
        model = film_convnext(seed=self.INIT_SEED, drop_path_rate=DROP_PATH)
        cfg = _cfg(arm="film", seed=self.RUN_SEED, max_epochs=1)
        first = _resume_train(model, cfg, tmp_path)
        again = _resume_train(model, cfg, tmp_path)  # must not raise
        assert again.resumed is True
        assert again.epochs_ran == first.epochs_ran == 1
        assert again.val_cmap_history == first.val_cmap_history

    def test_checkpoint_carries_the_full_resume_contract(self, tmp_path):
        train_arm(
            film_convnext(),
            SyntheticDataset(),
            SyntheticDataset(n_rows=16, seed=99),
            _cfg(max_epochs=1),
            tmp_path,
        )
        ckpt = torch.load(tmp_path / "last.pt", weights_only=False)
        for key in (
            "epoch",
            "model_state",
            "optimizer_state",
            "torch_rng",
            "numpy_rng",
            "python_rng",
            "val_history",
        ):
            assert key in ckpt, f"checkpoint missing {key!r}"
        assert ckpt["epoch"] == 0

    def test_best_state_path_loads_into_the_model(self, tmp_path):
        wrapper = film_convnext()
        result = train_arm(
            wrapper,
            SyntheticDataset(),
            SyntheticDataset(n_rows=16, seed=99),
            _cfg(max_epochs=1),
            tmp_path,
        )
        best = torch.load(result.best_state_path, weights_only=False)
        wrapper.load_state_dict(best["model_state"])  # must not raise


# ---------------------------------------------------------------------------
# Token arm smoke: the same loop drives TokenAST (and the float64 present
# from default collate is cast before the wrapper sees it).
# ---------------------------------------------------------------------------


class TestTokenArm:
    def test_token_arm_trains_one_epoch(self, tmp_path):
        result = train_arm(
            token_ast(),
            SyntheticDataset(shape=AST_SHAPE),
            SyntheticDataset(n_rows=16, seed=99, shape=AST_SHAPE),
            _cfg(arm="token", max_epochs=1),
            tmp_path,
        )
        assert result.epochs_ran == 1
        assert np.isfinite(result.val_cmap_history[0])


# ---------------------------------------------------------------------------
# Progress logging (fleet job 5730): the logs held NOTHING after "Loading
# weights" for 2.5 h, so a CPU-bound run was indistinguishable from a
# healthy one. train_arm now logs every cfg.log_every steps (epoch,
# step/total, loss, samples/s, elapsed, device) and brackets validation
# with start/end lines carrying row counts and wall time.
# ---------------------------------------------------------------------------


class TestProgressLogging:
    def _run(self, tmp_path, caplog, **cfg_kw):
        import logging

        caplog.set_level(logging.INFO, logger="birdology.phase_c")
        train_arm(
            film_convnext(),
            SyntheticDataset(n_rows=32),
            SyntheticDataset(n_rows=16, seed=99),
            _cfg(**{"max_epochs": 1, "batch_size": 8, **cfg_kw}),
            tmp_path,
        )
        return [
            r.getMessage()
            for r in caplog.records
            if r.name.startswith("birdology.phase_c")
        ]

    def test_step_lines_at_configured_cadence(self, tmp_path, caplog):
        """32 rows / batch 8 = 4 steps; log_every=2 → steps 2/4 and 4/4,
        nothing for steps 1 and 3."""
        msgs = self._run(tmp_path, caplog, log_every=2)
        steps = [m for m in msgs if m.startswith("train ")]
        assert len(steps) == 2, steps
        assert "step 2/4" in steps[0] and "step 4/4" in steps[1]
        for m in steps:
            for token in ("epoch 1/1", "loss=", "samples/s=", "elapsed=",
                          "device=cpu"):
                assert token in m, (token, m)

    def test_cadence_is_configurable(self, tmp_path, caplog):
        msgs = self._run(tmp_path, caplog, log_every=1)
        assert len([m for m in msgs if m.startswith("train ")]) == 4

    def test_default_cadence_is_50(self):
        assert TrainConfig(arm="film", seed=0).log_every == 50

    def test_validation_start_and_end_lines(self, tmp_path, caplog):
        msgs = self._run(tmp_path, caplog, log_every=100)
        start = [m for m in msgs if m.startswith("val start")]
        end = [m for m in msgs if m.startswith("val end")]
        assert len(start) == 1 and "rows=16" in start[0]
        assert len(end) == 1
        assert "cmap=" in end[0] and "wall=" in end[0] and "rows=16" in end[0]

    def test_log_every_must_be_positive(self):
        with pytest.raises(ValueError, match="log_every"):
            TrainConfig(arm="film", seed=0, log_every=0)


# ---------------------------------------------------------------------------
# DataLoader workers (fleet job 5730): with the model on the GPU, audio
# decode + the mel frontend in the main process starve it. The module
# docstring CLAIMED workers cannot change values (crops are (seed, epoch,
# idx)-keyed); these tests PROVE it on the real CropDataset + real
# convnext_frontend, and pin the loader policy (pin_memory on CUDA,
# persistent workers only where the loader is reused, own generators so
# no loader ever draws from the GLOBAL torch RNG).
#
# Everything a worker unpickles lives in tests/phase_c/_loader_fixtures.py
# (light imports): workers start in a FRESH interpreter and re-import it.
# ---------------------------------------------------------------------------


@pytest.fixture
def forkserver_workers():
    """Run worker pools under ``forkserver`` for these tests.

    Measured on the dev Mac (torch 2.14, Python 3.14): every ``spawn``
    pool costs ~10.6 s at SHUTDOWN even for a trivial dataset (workers
    miss DataLoader's 5 s join and are waited out one by one) vs 0.67 s
    under forkserver — a platform cost, not a correctness issue.
    forkserver is the STRONGER probe of value-neutrality than fork: like
    spawn, each worker is a fresh interpreter that re-imports and
    unpickles the dataset (no inherited parent memory) — and it is
    Python 3.14's Linux default, i.e. the fleet's shape.
    """
    import multiprocessing

    prev = multiprocessing.get_start_method(allow_none=True)
    multiprocessing.set_start_method("forkserver", force=True)
    yield
    multiprocessing.set_start_method(prev, force=True)

def _fixture_crop_dataset(seed: int = 0):
    import pyarrow as pa

    from birdology.phase_c.data import CropDataset, convnext_frontend

    n = 22  # not a multiple of the batch: a ragged final batch
    manifest = pa.table(
        {
            "filepath": [f"t{i}.wav" for i in range(n)],
            "ebird_code": [f"LABEL_{i % N_CLASSES}" for i in range(n)],
            "cell": [None if i % 3 == 0 else f"c{i}" for i in range(n)],
            "week": [None if i % 3 == 0 else i for i in range(n)],
        }
    )
    return CropDataset(
        manifest,
        fixture_audio,
        FixtureProvider(),
        {f"LABEL_{i}": i for i in range(N_CLASSES)},
        crop_s=0.1,  # 3200 samples: small frontend, real random offsets
        seed=seed,
        frontend=convnext_frontend,
    )


def _epoch_batches(ds, cfg, epoch):
    from birdology.phase_c.train import make_train_loader

    ds.epoch = epoch
    return list(make_train_loader(ds, cfg, epoch, torch.device("cpu")))


def _assert_batches_bit_identical(a, b):
    assert len(a) == len(b)
    for i, (x, y) in enumerate(zip(a, b)):
        assert x.keys() == y.keys()
        for k in x:
            assert x[k].dtype == y[k].dtype, (i, k, x[k].dtype, y[k].dtype)
            assert torch.equal(x[k], y[k]), f"batch {i} key {k!r} differs"


@pytest.mark.usefixtures("forkserver_workers")
class TestDataLoaderWorkers:
    def test_one_epoch_bit_identical_workers_0_vs_2(self):
        ds = _fixture_crop_dataset()
        cfg0 = _cfg(arm="film", seed=4, batch_size=4, num_workers=0)
        cfg2 = _cfg(arm="film", seed=4, batch_size=4, num_workers=2)
        b0 = _epoch_batches(ds, cfg0, epoch=0)
        b2 = _epoch_batches(ds, cfg2, epoch=0)
        _assert_batches_bit_identical(b0, b2)
        # The probe has teeth: real crops, real context, both flags, and
        # the collate's float64 `present` widening survives the workers.
        assert b0[0]["present"].dtype == torch.float64
        flags = torch.cat([b["present"] for b in b0])
        assert set(flags.tolist()) == {0.0, 1.0}
        assert b0[0]["inputs"].shape[1:] == (1, 128, 11)

    def test_epoch_dial_reaches_the_workers(self):
        """Epoch 1 with workers equals epoch 1 without — the train_ds.epoch
        the loop sets in the MAIN process reaches every worker (a
        persistent worker pool would hold a stale copy) — and differs from
        epoch 0 (crops re-drawn)."""
        ds = _fixture_crop_dataset()
        cfg0 = _cfg(arm="film", seed=4, batch_size=4, num_workers=0)
        cfg2 = _cfg(arm="film", seed=4, batch_size=4, num_workers=2)
        e1_w0 = _epoch_batches(ds, cfg0, epoch=1)
        e1_w2 = _epoch_batches(ds, cfg2, epoch=1)
        _assert_batches_bit_identical(e1_w0, e1_w2)
        e0 = _epoch_batches(ds, cfg0, epoch=0)
        assert not torch.equal(e0[0]["inputs"], e1_w0[0]["inputs"])

    def test_loaders_never_touch_the_global_torch_rng(self):
        """dropout/drop_path draw from the global RNG in the main process;
        a loader that also draws there would make training depend on
        loader bookkeeping. Every loader carries its own generator."""
        from birdology.phase_c.train import make_train_loader, make_val_loader

        ds = _fixture_crop_dataset()
        val = SyntheticDataset(n_rows=6, seed=99)
        for nw in (0, 2):
            cfg = _cfg(batch_size=4, num_workers=nw)
            torch.manual_seed(123)
            before = torch.get_rng_state()
            list(make_train_loader(ds, cfg, 0, torch.device("cpu")))
            vl = make_val_loader(val, cfg, torch.device("cpu"))
            list(vl)
            list(vl)
            assert torch.equal(before, torch.get_rng_state()), nw

    @pytest.mark.parametrize("device", ["cpu", "cuda"])
    def test_loader_policy(self, device):
        """pin_memory iff CUDA; persistent workers only on the REUSED val
        loader and only with workers (the per-epoch train loader must
        re-read train_ds.epoch, so it is never persistent)."""
        from birdology.phase_c.train import make_train_loader, make_val_loader

        dev = torch.device(device)
        ds = SyntheticDataset(n_rows=8)
        for nw in (0, 3):
            cfg = _cfg(batch_size=4, num_workers=nw)
            tl = make_train_loader(ds, cfg, 0, dev)
            vl = make_val_loader(ds, cfg, dev)
            for loader in (tl, vl):
                assert loader.num_workers == nw
                assert loader.pin_memory is (device == "cuda")
            assert tl.persistent_workers is False
            assert vl.persistent_workers is (nw > 0)

    def test_train_arm_with_workers_bit_equal_to_without(self, tmp_path):
        """End to end: the same 2-epoch film run (drop_path active, so the
        global RNG is load-bearing) with num_workers=0 and =2 ends in
        bit-identical weights and val history."""
        results, models = {}, {}
        for nw in (0, 2):
            models[nw] = film_convnext(seed=5, drop_path_rate=DROP_PATH)
            results[nw] = train_arm(
                models[nw],
                SyntheticDataset(),
                SyntheticDataset(n_rows=16, seed=99),
                _cfg(arm="film", seed=3, max_epochs=2, patience=10,
                     num_workers=nw),
                tmp_path / f"nw{nw}",
            )
        assert results[0].val_cmap_history == results[2].val_cmap_history
        assert not _differing_keys(models[0], models[2])


@pytest.mark.usefixtures("forkserver_workers")
class TestResumeWithWorkers(TestResume):
    """Review Focus 5 re-run with DataLoader workers: every TestResume
    test (headline bit-equality AND both discriminating-power controls)
    with num_workers=2 instead of 0."""

    @pytest.fixture(autouse=True)
    def _with_workers(self, monkeypatch):
        global _cfg
        base = _cfg

        def cfg_with_workers(**kw):
            kw.setdefault("num_workers", 2)
            return base(**kw)

        monkeypatch.setitem(globals(), "_cfg", cfg_with_workers)


# ---------------------------------------------------------------------------
# On CUDA, dropout/drop_path draw from the CUDA generator, not the CPU one
# _rng_snapshot used to save — a GPU resume would replay different masks.
# The checkpoint now carries the training device's CUDA RNG state. Tested
# on CPU by injecting a cuda device and stubbing torch.cuda's RNG I/O.
# ---------------------------------------------------------------------------


class TestCudaRngInResume:
    def test_snapshot_carries_cuda_rng_on_cuda(self, monkeypatch):
        from birdology.phase_c.train import _rng_snapshot

        sentinel = torch.tensor([7, 7, 7], dtype=torch.uint8)
        calls = []

        def fake_get(device=None):
            calls.append(device)
            return sentinel

        monkeypatch.setattr(torch.cuda, "get_rng_state", fake_get)
        snap = _rng_snapshot(torch.device("cuda", 1))
        assert torch.equal(snap["cuda_rng"], sentinel)
        assert calls == [torch.device("cuda", 1)]

    def test_snapshot_has_no_cuda_rng_on_cpu(self):
        from birdology.phase_c.train import _rng_snapshot

        assert "cuda_rng" not in _rng_snapshot(torch.device("cpu"))

    def test_restore_sets_cuda_rng_on_cuda(self, monkeypatch):
        from birdology.phase_c.train import _rng_restore, _rng_snapshot

        snap = _rng_snapshot(torch.device("cpu"))
        snap["cuda_rng"] = torch.tensor([1, 2], dtype=torch.uint8)
        got = {}
        monkeypatch.setattr(
            torch.cuda, "set_rng_state",
            lambda state, device=None: got.update(state=state, device=device),
        )
        _rng_restore(snap, torch.device("cuda", 0))
        assert torch.equal(got["state"], snap["cuda_rng"])
        assert got["device"] == torch.device("cuda", 0)

    def test_restore_refuses_cuda_checkpoint_without_cuda_rng(self):
        """A checkpoint written by a pre-fix GPU run cannot be resumed
        bit-exactly on CUDA — say so instead of silently diverging."""
        from birdology.phase_c.train import _rng_restore, _rng_snapshot

        snap = _rng_snapshot(torch.device("cpu"))
        with pytest.raises(RuntimeError, match="cuda_rng"):
            _rng_restore(snap, torch.device("cuda", 0))

    @pytest.mark.skipif(
        not torch.cuda.is_available(), reason="needs a real CUDA device"
    )
    def test_real_cuda_checkpoint_carries_cuda_rng(self, tmp_path):
        model = film_convnext().to("cuda")
        train_arm(
            model, SyntheticDataset(), SyntheticDataset(n_rows=16, seed=99),
            _cfg(max_epochs=1), tmp_path,
        )
        ckpt = torch.load(tmp_path / "last.pt", weights_only=False)
        assert "cuda_rng" in ckpt
