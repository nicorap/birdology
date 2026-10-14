"""BirdNET 2.4 + the Danish head as one TensorFlow model (SavedModel and TFLite).

Input is BirdNET's own: float32 audio chunks of 3 s at 48 kHz, shape (n, 144000).
Output ``logits`` (n, 6522) is on BirdNET's full label axis, chosen so that

    sigmoid(logits) = s * q**alpha

where s is BirdNET's score and q the head's probability (``floor_q`` for species
the head never saw). ``__call__`` scores each 3 s chunk on its own (what
BirdNET-Analyzer does); ``recording`` takes all chunks of one recording and pools
them the way the head was trained (scores max, embeddings mean), which is the
setting the head's DKX numbers were measured in.

BirdNET-Analyzer applies a sigmoid to a custom classifier's output, so
thresholds keep their usual meaning, and the model is a drop-in replacement for
BirdNET's own: same inputs, same labels file, same species.

Needs TensorFlow (the fleet's tf.sif); import it only there.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import tensorflow as tf

#: BirdNET 2.4's chunk: 3 s at 48 kHz.
SAMPLE_RATE = 48000
CHUNK = 3 * SAMPLE_RATE


class DanishBirdNET(tf.Module):
    def __init__(self, birdnet_dir: Path, arrays: dict, scores_are_logits: bool):
        super().__init__()
        self._bn = tf.saved_model.load(str(birdnet_dir))
        self._basic = self._bn.signatures["basic"]
        self._embed = self._bn.signatures["embeddings"]
        self.scores_are_logits = scores_are_logits
        c = lambda k: tf.constant(np.asarray(arrays[k], np.float32))  # noqa: E731
        self.mu, self.sd, self.W1, self.b1 = c("mu"), c("sd"), c("W1"), c("b1")
        self.mlp = "W2" in arrays
        if self.mlp:
            self.W2, self.b2 = c("W2"), c("b2")
        self.alpha = float(arrays["alpha"])
        self.log_floor = float(np.log(float(arrays["floor_q"])))
        n_axis = int(arrays["n_axis"])
        classes = np.asarray(arrays["classes"], np.int64)
        # scatter matrix: head output j -> axis column classes[j]
        self.scatter = tf.constant(np.eye(n_axis, dtype=np.float32)[classes])
        self.seen = tf.constant(np.isin(np.arange(n_axis), classes).astype(np.float32))

    def _parts(self, inputs):
        s = self._basic(inputs=inputs)["scores"]
        log_s = tf.math.log_sigmoid(s) if self.scores_are_logits else tf.math.log(tf.clip_by_value(s, 1e-12, 1.0))
        z = (self._embed(inputs=inputs)["embeddings"] - self.mu) / self.sd
        h = tf.matmul(z, self.W1) + self.b1
        if self.mlp:
            h = tf.matmul(tf.nn.gelu(h, approximate=False), self.W2) + self.b2
        log_q_head = tf.maximum(tf.nn.log_softmax(h, axis=1), self.log_floor)
        log_q = tf.matmul(log_q_head, self.scatter) + (1.0 - self.seen) * self.log_floor
        return log_s, log_q

    @tf.function(input_signature=[tf.TensorSpec([None, CHUNK], tf.float32, name="inputs")])
    def __call__(self, inputs):
        log_s, log_q = self._parts(inputs)
        log_p = tf.minimum(log_s + self.alpha * log_q, -1e-6)
        return {"logits": log_p - tf.math.log(-tf.math.expm1(log_p))}     # logit(p) = log p - log(1 - p)

    @tf.function(input_signature=[tf.TensorSpec([None, CHUNK], tf.float32, name="inputs")])
    def recording(self, inputs):
        """All chunks of ONE recording -> (1, n_axis) logits, pooled the way the head
        was trained: BirdNET scores max over chunks, embeddings mean over chunks."""
        s = self._basic(inputs=inputs)["scores"]
        s = tf.sigmoid(s) if self.scores_are_logits else s
        log_s = tf.math.log(tf.clip_by_value(tf.reduce_max(s, axis=0, keepdims=True), 1e-12, 1.0))
        e = tf.reduce_mean(self._embed(inputs=inputs)["embeddings"], axis=0, keepdims=True)
        h = tf.matmul((e - self.mu) / self.sd, self.W1) + self.b1
        if self.mlp:
            h = tf.matmul(tf.nn.gelu(h, approximate=False), self.W2) + self.b2
        log_q_head = tf.maximum(tf.nn.log_softmax(h, axis=1), self.log_floor)
        log_q = tf.matmul(log_q_head, self.scatter) + (1.0 - self.seen) * self.log_floor
        log_p = tf.minimum(log_s + self.alpha * log_q, -1e-6)
        return {"logits": log_p - tf.math.log(-tf.math.expm1(log_p))}

    @tf.function(input_signature=[tf.TensorSpec([None, CHUNK], tf.float32, name="inputs")])
    def parts(self, inputs):
        log_s, log_q = self._parts(inputs)
        return {"log_s": log_s, "log_q": log_q}


def chunks(x: np.ndarray, min_seconds: float = 1.0) -> np.ndarray:
    """Cut 48 kHz mono audio into non-overlapping 3 s chunks; a trailing piece of
    at least ``min_seconds`` is zero-padded (BirdNET-Analyzer's default behaviour)."""
    x = np.asarray(x, np.float32)
    n_full = len(x) // CHUNK
    out = [x[i * CHUNK:(i + 1) * CHUNK] for i in range(n_full)]
    rest = x[n_full * CHUNK:]
    if len(rest) >= min_seconds * SAMPLE_RATE or not out:
        out.append(np.pad(rest, (0, CHUNK - len(rest))))
    return np.stack(out)


def to_tflite(module: DanishBirdNET, path: Path) -> None:
    conv = tf.lite.TFLiteConverter.from_concrete_functions([module.__call__.get_concrete_function()], module)
    conv.target_spec.supported_ops = [tf.lite.OpsSet.TFLITE_BUILTINS, tf.lite.OpsSet.SELECT_TF_OPS]
    Path(path).write_bytes(conv.convert())
