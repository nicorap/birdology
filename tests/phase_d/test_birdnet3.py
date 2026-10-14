import numpy as np
import pytest

from birdology.phase_d.birdnet3 import WINDOW, Classifier, app_week, load_labels, resample, scores, windows


def test_windows_drop_the_tail_and_pad_short_recordings():
    x = np.arange(2 * WINDOW + 100, dtype=np.float32)
    w = windows(x)
    assert w.shape == (2, WINDOW)
    np.testing.assert_array_equal(w[1], x[WINDOW:2 * WINDOW])
    short = windows(np.ones(1000, np.float32))
    assert short.shape == (1, WINDOW) and short[0, :1000].sum() == 1000 and short[0, 1000:].sum() == 0


def test_resample_to_32k():
    x = np.sin(np.linspace(0, 100, 48000)).astype(np.float32)
    assert len(resample(x, 48000)) == 32000
    assert resample(x, 32000) is not None and len(resample(x, 32000)) == 48000
    assert len(resample(np.stack([x, x], 1), 48000)) == 32000


def test_app_week_matches_geo_model_dart():
    assert app_week(1, 1) == 1
    assert app_week(1, 8) == 2
    assert app_week(1, 31) == 4          # days 29-31 stay in week 4
    assert app_week(12, 31) == 48


def test_scores_are_sigmoid_of_scaled_linear():
    rng = np.random.default_rng(0)
    clf = Classifier(W=rng.normal(size=(5, 8)).astype(np.float32), b=rng.normal(size=5).astype(np.float32), scale=2.5)
    E = rng.normal(size=(3, 8)).astype(np.float32)
    want = 1 / (1 + np.exp(-2.5 * (E @ clf.W.T + clf.b)))
    np.testing.assert_allclose(scores(clf, E), want, rtol=1e-5)


def test_load_labels_orders_by_idx(tmp_path):
    p = tmp_path / "labels.csv"
    p.write_text("idx;id;sci_name;com_name;class;order\n1;9;Parus major;Great Tit;Aves;P\n0;3;Turdus merula;Blackbird;Aves;P\n")
    assert load_labels(p) == ["Turdus merula", "Parus major"]


def test_classifier_from_onnx_recovers_the_head(tmp_path):
    onnx = pytest.importorskip("onnx")
    ort = pytest.importorskip("onnxruntime")
    from onnx import TensorProto, helper, numpy_helper

    from birdology.phase_d.birdnet3 import classifier_from_onnx

    rng = np.random.default_rng(1)
    W, b = rng.normal(size=(4, 6)).astype(np.float32), rng.normal(size=4).astype(np.float32)
    nodes = [helper.make_node("Gemm", ["emb", "W", "b"], [f"g{i}"], transB=1) for i in range(3)]
    nodes += [helper.make_node("Add", ["g0", "g1"], ["a1"]), helper.make_node("Add", ["a1", "g2"], ["a2"]),
              helper.make_node("Div", ["a2", "T"], ["d"]), helper.make_node("Sigmoid", ["d"], ["s"]),
              helper.make_node("Identity", ["s"], ["predictions"])]
    graph = helper.make_graph(
        nodes, "g", [helper.make_tensor_value_info("emb", TensorProto.FLOAT, [None, 6])],
        [helper.make_tensor_value_info("predictions", TensorProto.FLOAT, [None, 4])],
        [numpy_helper.from_array(W, "W"), numpy_helper.from_array(b, "b"), numpy_helper.from_array(np.float32(2.0), "T")])
    path = tmp_path / "m.onnx"
    onnx.save(helper.make_model(graph, opset_imports=[helper.make_opsetid("", 18)]), path)
    clf = classifier_from_onnx(path)
    assert clf.scale == pytest.approx(1.5)
    E = rng.normal(size=(2, 6)).astype(np.float32)
    ref = ort.InferenceSession(str(path)).run(None, {"emb": E})[0]
    np.testing.assert_allclose(scores(clf, E), ref, rtol=1e-5)
