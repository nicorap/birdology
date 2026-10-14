import numpy as np
import pytest

onnx = pytest.importorskip("onnx")
ort = pytest.importorskip("onnxruntime")


def _toy_app_model(path, n_in=6, n_axis=5, seed=0):
    """Shaped like the app's model: input -> embeddings_out, predictions (sigmoid)."""
    from onnx import TensorProto, helper, numpy_helper

    rng = np.random.default_rng(seed)
    W, b = rng.normal(size=(n_axis, n_in)).astype(np.float32), rng.normal(size=n_axis).astype(np.float32)
    nodes = [helper.make_node("Relu", ["input"], ["emb"]),
             helper.make_node("Identity", ["emb"], ["embeddings_out"]),
             helper.make_node("Gemm", ["emb", "W", "b"], ["lg"], transB=1),
             helper.make_node("Sigmoid", ["lg"], ["predictions"])]
    graph = helper.make_graph(
        nodes, "app", [helper.make_tensor_value_info("input", TensorProto.FLOAT, ["batch_size", n_in])],
        [helper.make_tensor_value_info("predictions", TensorProto.FLOAT, ["batch_size", n_axis]),
         helper.make_tensor_value_info("embeddings_out", TensorProto.FLOAT, ["batch_size", n_in])],
        [numpy_helper.from_array(W, "W"), numpy_helper.from_array(b, "b")])
    onnx.save(helper.make_model(graph, opset_imports=[helper.make_opsetid("", 18)], ir_version=9), path)


def _arrays(kind, n_in=6, n_axis=5, seed=1):
    rng = np.random.default_rng(seed)
    a = {"mu": rng.normal(size=n_in).astype(np.float32), "sd": rng.uniform(0.5, 2, n_in).astype(np.float32),
         "classes": np.array([0, 2, 4]), "n_axis": np.int64(n_axis), "alpha": np.float32(1.5),
         "floor_q": np.float32(1e-3), "kind": np.array(kind)}
    if kind == "mlp":
        a |= {"W1": rng.normal(size=(n_in, 4)).astype(np.float32), "b1": rng.normal(size=4).astype(np.float32),
              "W2": rng.normal(size=(4, 3)).astype(np.float32), "b2": rng.normal(size=3).astype(np.float32)}
    else:
        a |= {"W1": rng.normal(size=(n_in, 3)).astype(np.float32), "b1": rng.normal(size=3).astype(np.float32)}
    return a


@pytest.mark.parametrize("kind", ["mlp", "linear"])
@pytest.mark.parametrize("fusion", ["raw", "norm"])
def test_appended_head_matches_numpy_and_keeps_the_app_interface(tmp_path, kind, fusion):
    from birdology.phase_d.export_onnx import append_head, reference

    src, dst = tmp_path / "app.onnx", tmp_path / "dk.onnx"
    _toy_app_model(src)
    arrays = _arrays(kind)
    append_head(src, arrays, dst, fusion=fusion)
    x = np.random.default_rng(2).normal(size=(7, 6)).astype(np.float32)
    orig = ort.InferenceSession(str(src))
    new = ort.InferenceSession(str(dst))
    assert [i.name for i in new.get_inputs()] == ["input"]
    assert {"predictions", "embeddings_out", "birdnet_predictions"} == {o.name for o in new.get_outputs()}
    s, e = orig.run(["predictions", "embeddings_out"], {"input": x})
    p, s2, e2 = new.run(["predictions", "birdnet_predictions", "embeddings_out"], {"input": x})
    np.testing.assert_allclose(s2, s, rtol=1e-6)
    np.testing.assert_allclose(e2, e, rtol=1e-6)
    np.testing.assert_allclose(p, reference(s, e, arrays, fusion), rtol=1e-4, atol=1e-7)
    assert (p <= s + 1e-6).all()
    if fusion == "norm":     # the head's favourite keeps BirdNET's score
        top = np.argmax(reference(np.ones_like(s), e, arrays, fusion), 1)
        np.testing.assert_allclose(p[np.arange(7), top], s[np.arange(7), top], rtol=1e-5)
