"""BirdNET+ V3.0 (the BirdNET Live app's model) as a per-window embedder.

The app ships ``BirdNET+_V3.0-preview3.1_Global_10K-pruned_FP16.onnx``: raw mono
32 kHz audio of any length in, ``predictions`` (n, 9789 sigmoid scores) and
``embeddings_out`` (n, 1280) out. File analysis cuts a file into non-overlapping
3 s windows and scores each one.

The species scores are a linear layer on the embedding (measured by tracing
the ONNX graph, 2026-10-14):

    predictions = sigmoid(scale * (embedding @ W.T + b))

with ``scale`` = 5 / temperature (five summed copies of the same head, then a
divide). So a cache only needs the embeddings; :func:`classifier_from_onnx`
pulls ``W``, ``b`` and ``scale`` out of the model and :func:`scores` rebuilds
the predictions, which keeps a European XCL pass at ~10 GB instead of ~60.

onnx/onnxruntime are imported lazily: the fleet has them in apps/venvs/onnx,
the dev Mac and CI do not.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

SAMPLE_RATE = 32000
WINDOW_SECONDS = 3
WINDOW = WINDOW_SECONDS * SAMPLE_RATE


def resample(x: np.ndarray, sr: int) -> np.ndarray:
    """Mono float32 at 32 kHz (polyphase, as the rest of the project does)."""
    x = np.asarray(x, np.float32)
    if x.ndim > 1:
        x = x.mean(1)
    if sr == SAMPLE_RATE:
        return x
    from scipy.signal import resample_poly

    g = np.gcd(int(sr), SAMPLE_RATE)
    return resample_poly(x, SAMPLE_RATE // g, int(sr) // g).astype(np.float32)


def windows(x: np.ndarray) -> np.ndarray:
    """(n, WINDOW) non-overlapping 3 s windows of 32 kHz audio, the app's file
    analysis at its default overlap of 0. A trailing piece shorter than a
    window is dropped, except that a recording shorter than one window is
    zero-padded to one (the app refuses such files; a test set should not
    silently lose them)."""
    x = np.asarray(x, np.float32)
    n = len(x) // WINDOW
    if n == 0:
        return np.pad(x, (0, WINDOW - len(x)))[None, :]
    return x[: n * WINDOW].reshape(n, WINDOW)


def decode_windows(payload: bytes, max_windows: int = 0) -> np.ndarray:
    """``windows(resample(audio))`` of an encoded file, keeping at most
    ``max_windows`` (0 keeps all). With a cap, only the audio the kept windows
    need (plus 1 s, so the resampling filter sees no cut) is decoded: long
    Xeno-canto files would otherwise be decoded in full for a few windows."""
    import io

    import soundfile as sf

    with sf.SoundFile(io.BytesIO(payload)) as f:
        sr = f.samplerate
        frames = -1 if not max_windows else (max_windows * WINDOW_SECONDS + 1) * sr
        x = f.read(frames, dtype="float32", always_2d=False)
    if x.size == 0:
        raise ValueError("decoded to 0 samples")
    w = windows(resample(x, sr))
    return w[:max_windows] if max_windows else w


@dataclass(frozen=True)
class Classifier:
    W: np.ndarray        # (n_species, 1280)
    b: np.ndarray        # (n_species,)
    scale: float

    def logits(self, E: np.ndarray) -> np.ndarray:
        return self.scale * (np.asarray(E, np.float32) @ self.W.T + self.b)


def scores(clf: Classifier, E: np.ndarray) -> np.ndarray:
    """The model's ``predictions`` rebuilt from cached embeddings."""
    return 1.0 / (1.0 + np.exp(-clf.logits(E)))


def classifier_from_onnx(path: Path) -> Classifier:
    """Read the final linear layer and its output scale out of the ONNX graph.

    Expects the measured structure: ``predictions`` <- Cast <- Sigmoid <- Div(c)
    <- k chained Adds of the same Gemm(embeddings, head.weight, head.bias).
    Anything else raises: a silently wrong head would mis-score every window.
    """
    import onnx
    from onnx import numpy_helper

    g = onnx.load(str(path)).graph
    prod = {o: n for n in g.node for o in n.output}
    init = {i.name: numpy_helper.to_array(i) for i in g.initializer}
    const = {n.output[0]: numpy_helper.to_array(n.attribute[0].t) for n in g.node if n.op_type == "Constant"}
    value = lambda name: init.get(name, const.get(name))  # noqa: E731

    node = prod["predictions"]
    while node.op_type in ("Cast", "Identity"):
        node = prod[node.input[0]]
    if node.op_type != "Sigmoid":
        raise ValueError(f"expected Sigmoid before predictions, found {node.op_type}")
    node = prod[node.input[0]]
    divisor = 1.0
    if node.op_type == "Div":
        divisor = float(np.asarray(value(node.input[1])).reshape(()))
        node = prod[node.input[0]]
    gemms, stack = [], [node]
    while stack:
        n = stack.pop()
        if n.op_type == "Add":
            stack += [prod[i] for i in n.input if i in prod]
        elif n.op_type == "Gemm":
            gemms.append(n)
        elif n.op_type != "Expand":            # zeros_like seed of the sum
            raise ValueError(f"unexpected {n.op_type} in the classifier sum")
    if not gemms or len({(g_.input[1], g_.input[2]) for g_ in gemms}) != 1:
        raise ValueError(f"expected copies of ONE linear head, found {len(gemms)} Gemm nodes")
    gm = gemms[0]
    attrs = {a.name: onnx.helper.get_attribute_value(a) for a in gm.attribute}
    W = np.asarray(value(gm.input[1]), np.float32)
    if not attrs.get("transB", 0):
        W = W.T
    b = np.asarray(value(gm.input[2]), np.float32)
    return Classifier(W=W, b=b, scale=float(attrs.get("alpha", 1.0)) * len(gemms) / divisor)


class Embedder:
    """onnxruntime session over the app's model; CUDA when available."""

    def __init__(self, path: Path, providers: list[str] | None = None, batch: int = 64, threads: int = 0,
                 fixed: bool = False):
        """``threads`` > 0 caps onnxruntime's CPU threads (and stops them spinning):
        by default it starts one per core of the node, ignoring the Slurm allocation.
        ``fixed`` pads every chunk to ``batch``, so the model sees one input shape."""
        import onnxruntime as ort

        if hasattr(ort, "preload_dlls"):     # CUDA/cuDNN from the nvidia-* wheels, if installed
            ort.preload_dlls()
        avail = ort.get_available_providers()
        providers = providers or [p for p in ("CUDAExecutionProvider", "CPUExecutionProvider") if p in avail]
        opts = ort.SessionOptions()
        if threads > 0:
            opts.intra_op_num_threads = threads
            opts.inter_op_num_threads = 1
            opts.add_session_config_entry("session.intra_op.allow_spinning", "0")
        self.session = ort.InferenceSession(str(path), sess_options=opts, providers=providers)
        self.batch, self.fixed = batch, fixed
        self.input = self.session.get_inputs()[0].name

    def __call__(self, W: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """(predictions, embeddings) for a (n, samples) stack of windows.

        Each chunk is padded (repeating its last window) up to a power of two,
        or to ``batch`` when ``fixed``: on CUDA, a new input shape is slow, and
        one shape per recording length ran V3 at 7 windows/s on an H100 against
        139 windows/s for a fixed batch. Bulk runs should also batch across
        recordings (scripts/phase_d_v3_embed.py)."""
        P, E = [], []
        for i in range(0, len(W), self.batch):
            w = W[i:i + self.batch]
            n = len(w)
            size = self.batch if self.fixed else min(self.batch, 1 << (n - 1).bit_length())
            if size > n:
                w = np.concatenate([w, np.repeat(w[-1:], size - n, axis=0)])
            p, e = self.session.run(["predictions", "embeddings_out"], {self.input: w})
            P.append(p[:n])
            E.append(e[:n])
        return np.concatenate(P), np.concatenate(E)


def load_labels(path: Path) -> list[str]:
    """Scientific names in model-output order, from the app's ';' labels CSV."""
    import csv

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f, delimiter=";"))
    rows.sort(key=lambda r: int(r["idx"]))
    if [int(r["idx"]) for r in rows] != list(range(len(rows))):
        raise ValueError(f"{path}: idx column is not 0..n-1")
    return [r["sci_name"] for r in rows]


def app_week(month: int, day: int) -> int:
    """The geo-model's week: 4 per month, 1-48 (geo_model.dart)."""
    return (int(month) - 1) * 4 + min(4, (int(day) - 1) // 7 + 1)


def iter_window_cache(d: Path, keep: set[str] | None = None):
    """Yield (filepaths, window index, embeddings) per cached shard of
    scripts/phase_d_v3_embed.py, rows sorted by (filepath, window). Rows with
    non-finite embeddings (all-zero windows cached before the embedder skipped
    them) are dropped."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    for f in sorted(Path(d).glob("*.parquet")):
        t = pq.read_table(f)
        if keep is not None:
            t = t.filter(pc.is_in(t.column("filepath"), value_set=pa_strings(keep)))
        if t.num_rows == 0:
            continue
        t = t.sort_by([("filepath", "ascending"), ("window", "ascending")])
        fp = np.asarray(t.column("filepath").to_pylist())
        E = np.asarray(t.column("embedding").combine_chunks().flatten(), np.float32).reshape(t.num_rows, -1)
        ok = np.isfinite(E).all(1)
        if ok.any():
            yield fp[ok], np.asarray(t.column("window").to_numpy())[ok], E[ok]


def pa_strings(values) -> "object":
    import pyarrow as pa

    return pa.array(sorted(values), pa.string())


def max_by_recording(fp: np.ndarray, X: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Max over each recording's windows; ``fp`` must be grouped (sorted)."""
    fp = np.asarray(fp)
    starts = np.flatnonzero(np.r_[True, fp[1:] != fp[:-1]])
    return fp[starts], np.maximum.reduceat(X, starts, axis=0)


def top_windows(fp: np.ndarray, score: np.ndarray, k: int) -> np.ndarray:
    """Indices of the k highest-``score`` windows of each recording (all of
    them when it has fewer); ``fp`` must be grouped."""
    fp = np.asarray(fp)
    order = np.lexsort((-np.asarray(score), fp))
    starts = np.flatnonzero(np.r_[True, fp[order][1:] != fp[order][:-1]])
    rank = np.arange(len(fp)) - np.repeat(starts, np.diff(np.r_[starts, len(fp)]))
    return np.sort(order[rank < k])
