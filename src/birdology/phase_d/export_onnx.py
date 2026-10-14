"""The BirdNET Live app's ONNX model with a Danish head appended (graph surgery).

Input and output names stay the app's own -- ``input`` (n, samples at 32 kHz),
``predictions`` (n, 9789) and ``embeddings_out`` (n, 1280) -- and the label axis
is unchanged, so the app loads the file through its existing
``model_config.json`` with no code change. The original scores stay available
as an extra output, ``birdnet_predictions``.

Per window, with s the model's score and q the head's probability on the
model's axis (``floor_q`` for species the head never saw):

    raw:   predictions = s * q**alpha
    norm:  predictions = s * (q / max_j q_j)**alpha
    cal:   predictions = sigmoid(log s + alpha log q + offset)

``cal`` (the default) ranks species and windows exactly like ``raw``, with
``offset`` (head_arrays: the head's ``meta["app_offset"]``, set by
scripts/phase_d_v3_train_head.py) chosen so the app's confidence threshold
(0.35) passes as many recordings as it does for BirdNET alone. ``norm`` also
keeps the threshold's meaning, by leaving each window's favourite at
BirdNET's score, but that lets a window where the head is unsure compete
with a clear call: on DKX it kept 0.858 top-1 of ``raw``'s 0.898.

Needs ``onnx`` (fleet: apps/venvs/onnx, apps/venvs/phase_c).
"""
from __future__ import annotations

from pathlib import Path

import numpy as np

FUSIONS = ("cal", "raw", "norm")


def append_head(model_path: Path, arrays: dict, out_path: Path, fusion: str = "cal") -> None:
    import onnx
    from onnx import TensorProto, helper, numpy_helper

    if fusion not in FUSIONS:
        raise ValueError(f"fusion must be one of {FUSIONS}")
    m = onnx.load(str(model_path))
    g = m.graph
    n_axis = int(arrays["n_axis"])
    pred_out = next(o for o in g.output if o.name == "predictions")
    if pred_out.type.tensor_type.shape.dim[1].dim_value != n_axis:
        raise ValueError(f"head is for {n_axis} species, model predicts {pred_out.type.tensor_type.shape.dim[1].dim_value}")

    # the model's own scores become 'birdnet_predictions'
    for n in g.node:
        n.output[:] = ["birdnet_predictions" if o == "predictions" else o for o in n.output]
        n.input[:] = ["birdnet_predictions" if i == "predictions" else i for i in n.input]
    pred_out.name = "birdnet_predictions"

    classes = np.asarray(arrays["classes"], np.int64)
    k = len(classes)
    gather = np.full(n_axis, k, np.int64)          # unseen species -> the floor column
    gather[classes] = np.arange(k)
    log_floor = np.float32(np.log(float(arrays["floor_q"])))
    f32 = lambda a: np.asarray(a, np.float32)  # noqa: E731
    inits = {
        "dk_mu": f32(arrays["mu"]), "dk_sd": f32(arrays["sd"]),
        "dk_W1": f32(arrays["W1"]), "dk_b1": f32(arrays["b1"]),
        "dk_floor": np.array(log_floor, np.float32), "dk_pads": np.array([0, 0, 0, 1], np.int64),
        "dk_gather": gather, "dk_alpha": np.array(float(arrays["alpha"]), np.float32),
        "dk_eps": np.array(1e-12, np.float32), "dk_zero": np.array(0.0, np.float32),
        "dk_axis1": np.array([1], np.int64),
        "dk_offset": np.array(float(arrays.get("offset", 0.0)), np.float32),
    }
    mlp = "W2" in arrays
    if mlp:
        inits |= {"dk_W2": f32(arrays["W2"]), "dk_b2": f32(arrays["b2"]),
                  "dk_half": np.array(0.5, np.float32), "dk_one": np.array(1.0, np.float32),
                  "dk_rsqrt2": np.array(1 / np.sqrt(2.0), np.float32)}
    g.initializer.extend(numpy_helper.from_array(v, k_) for k_, v in inits.items())

    N = helper.make_node
    nodes = [
        N("Sub", ["embeddings_out", "dk_mu"], ["dk_c"]),
        N("Div", ["dk_c", "dk_sd"], ["dk_z"]),
        N("Gemm", ["dk_z", "dk_W1", "dk_b1"], ["dk_h1"]),
    ]
    logits = "dk_h1"
    if mlp:                                       # exact GELU, as torch.nn.GELU()
        nodes += [
            N("Mul", ["dk_h1", "dk_rsqrt2"], ["dk_s"]), N("Erf", ["dk_s"], ["dk_e"]),
            N("Add", ["dk_e", "dk_one"], ["dk_e1"]), N("Mul", ["dk_h1", "dk_e1"], ["dk_he"]),
            N("Mul", ["dk_he", "dk_half"], ["dk_gelu"]),
            N("Gemm", ["dk_gelu", "dk_W2", "dk_b2"], ["dk_h2"]),
        ]
        logits = "dk_h2"
    nodes += [
        N("LogSoftmax", [logits], ["dk_lq0"], axis=1),
        N("Max", ["dk_lq0", "dk_floor"], ["dk_lq1"]),
        N("Pad", ["dk_lq1", "dk_pads", "dk_floor"], ["dk_lqp"], mode="constant"),
        N("Gather", ["dk_lqp", "dk_gather"], ["dk_lq"], axis=1),     # (n, n_axis) log q
    ]
    lq = "dk_lq"
    if fusion == "norm":
        nodes += [N("ReduceMax", ["dk_lq", "dk_axis1"], ["dk_lqmax"], keepdims=1),
                  N("Sub", ["dk_lq", "dk_lqmax"], ["dk_lqn"])]
        lq = "dk_lqn"
    nodes += [
        N("Max", ["birdnet_predictions", "dk_eps"], ["dk_sc"]), N("Log", ["dk_sc"], ["dk_ls"]),
        N("Mul", [lq, "dk_alpha"], ["dk_alq"]), N("Add", ["dk_ls", "dk_alq"], ["dk_lp"]),
    ]
    if fusion == "cal":
        nodes += [N("Add", ["dk_lp", "dk_offset"], ["dk_lpo"]), N("Sigmoid", ["dk_lpo"], ["predictions"])]
    else:
        nodes += [N("Min", ["dk_lp", "dk_zero"], ["dk_lpc"]), N("Exp", ["dk_lpc"], ["predictions"])]
    g.node.extend(nodes)
    out = helper.make_tensor_value_info("predictions", TensorProto.FLOAT, ["batch_size", n_axis])
    outputs = [out] + list(g.output)
    del g.output[:]
    g.output.extend(outputs)
    m.metadata_props.append(onnx.StringStringEntryProto(key="danish_head", value=(
        f"kind={arrays['kind']} classes={k} alpha={float(arrays['alpha'])} floor_q={float(arrays['floor_q'])} "
        f"fusion={fusion} offset={float(arrays.get('offset', 0.0))}")))
    onnx.checker.check_model(m)
    onnx.save(m, str(out_path))


def reference(s: np.ndarray, E: np.ndarray, arrays: dict, fusion: str = "cal") -> np.ndarray:
    """NumPy version of the appended graph, for parity checks."""
    from scipy.special import erf, expit, log_softmax

    h = (np.asarray(E, np.float64) - arrays["mu"]) / arrays["sd"] @ np.asarray(arrays["W1"], np.float64) + arrays["b1"]
    if "W2" in arrays:
        h = 0.5 * h * (1.0 + erf(h / np.sqrt(2.0))) @ np.asarray(arrays["W2"], np.float64) + arrays["b2"]
    floor = np.log(float(arrays["floor_q"]))
    lq = np.full((len(h), int(arrays["n_axis"])), floor)
    lq[:, arrays["classes"]] = np.maximum(log_softmax(h, axis=1), floor)
    if fusion == "norm":
        lq = lq - lq.max(1, keepdims=True)
    lp = np.log(np.maximum(s, 1e-12)) + float(arrays["alpha"]) * lq
    if fusion == "cal":
        return expit(lp + float(arrays.get("offset", 0.0)))
    return np.exp(np.minimum(lp, 0.0))
