"""The head: training with early stopping, scoring onto BirdNET's axis, save/load."""
import numpy as np
import pytest

torch = pytest.importorskip("torch")

from birdology.phase_d.head import DanishHead, TrainConfig, make_head, train_head  # noqa: E402

N_AXIS, DIM = 10, 8
CLASSES = np.array([2, 5, 7])          # the head knows three of the ten axis species


CENTERS = np.random.default_rng(42).normal(0, 3, (len(CLASSES), DIM))


def _blobs(n=300, seed=0):
    rng = np.random.default_rng(seed)
    y = rng.integers(0, len(CLASSES), n)
    centers = CENTERS
    X = (centers[y] + rng.normal(0, 0.5, (n, DIM))).astype(np.float32)
    return X, y


@pytest.fixture(scope="module")
def trained():
    X, y = _blobs()
    Xv, yv = _blobs(seed=1)
    mu, sd = X.mean(0), X.std(0) + 1e-6
    z = lambda a: torch.from_numpy(((a - mu) / sd).astype(np.float32))  # noqa: E731
    res = train_head(z(X), torch.from_numpy(y), z(Xv), torch.from_numpy(yv), len(CLASSES),
                     TrainConfig(kind="linear", lr=1e-2, weight_decay=0.0, max_epochs=40))
    head = DanishHead(model=res.model, kind="linear", classes=CLASSES, n_axis=N_AXIS, mu=mu, sd=sd,
                      alpha=1.0, floor_q=1e-4, remap={"brnowl": "webowl1"})
    return res, head, Xv, yv


def test_training_separates_easy_blobs(trained):
    res, _, _, _ = trained
    assert res.val_top1 > 0.95 and res.best_epoch >= 0


def test_log_q_spreads_onto_the_full_axis_with_a_floor(trained):
    _, head, Xv, _ = trained
    lq = head.log_q(Xv[:5])
    assert lq.shape == (5, N_AXIS)
    unseen = np.setdiff1d(np.arange(N_AXIS), CLASSES)
    assert np.allclose(lq[:, unseen], np.log(1e-4))
    assert (lq[:, CLASSES] >= np.log(1e-4)).all()


def test_head_only_masks_unseen_species(trained):
    _, head, Xv, yv = trained
    X = head.head_only(Xv)
    assert np.isinf(X[:, 0]).all()
    assert (X.argmax(1) == CLASSES[yv]).mean() > 0.95


def test_fusion_is_log_s_plus_alpha_log_q(trained):
    _, head, Xv, _ = trained
    S = np.full((3, N_AXIS), 0.5)
    expected = np.log(0.5) + 0.5 * head.log_q(Xv[:3])
    assert np.allclose(head.fuse(S, Xv[:3], alpha=0.5), expected)


def test_fusion_lets_a_confident_birdnet_keep_a_species_the_head_never_saw(trained):
    _, head, Xv, _ = trained
    S = np.full((1, N_AXIS), 1e-6)
    S[0, 0] = 0.999                     # axis species 0 is unknown to the head
    assert head.fuse(S, Xv[:1], alpha=0.1).argmax() == 0


def test_save_load_round_trip(trained, tmp_path):
    _, head, Xv, _ = trained
    p = tmp_path / "head.pt"
    head.save(p)
    back = DanishHead.load(p)
    assert np.allclose(back.log_q(Xv[:4]), head.log_q(Xv[:4]))
    assert back.remap == {"brnowl": "webowl1"} and back.n_axis == N_AXIS


def test_unknown_head_kind_is_refused():
    with pytest.raises(ValueError):
        make_head("transformer", 4, 2)
