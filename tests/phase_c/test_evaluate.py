"""Task 6: the eval runner — filepath-joined labels, segment scores.

The contract under test (plan Task 6, Review Focus 3, spec §2 Eval):

* ``build_eval_dataset(metadata, load_audio, context_provider,
  class_codes, frontend, crop_s)`` -> ``EvalDataset``: one item per
  metadata row (a 5 s test segment), labels from
  ``ebird_code_multilabel`` keyed BY FILEPATH.
* A duplicate filepath in the metadata is REFUSED, naming the offender
  (Review Focus 3): the same join hazard as phase_b — a duplicate would
  double-count its labels or silently pick one of two rows.
* Multilabel y on the MODEL's class axis in ``class_codes`` order —
  by code, never positional; codes absent from the axis are counted in
  ``unscoreable``, never a KeyError.
* Null (cell, week) rows — and metadata parquets with no cell/week
  columns at all — take the null-context path: zeros + present 0.0
  through the provider's own absent answer.
* Short segments zero-pad right in the WAVEFORM domain; long segments
  take the deterministic head (an eval segment is already a 5 s window;
  no random cropping at eval).
* ``evaluate_model(model_or_wrapper, eval_ds, arm=...)`` -> (n_rows,
  n_classes) raw logits, batch-collated exactly like training (the
  float64 ``present`` widening cast at the seam).

Tiny models from configs, in-memory tables, injected audio + frontend.
No network, no from_pretrained.
"""
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest
import torch
from transformers import ConvNextConfig, ConvNextForImageClassification

from birdology.phase_c.context import ContextEncoder
from birdology.phase_c.evaluate import build_eval_dataset, evaluate_model
from birdology.phase_c.film import FiLMConvNext

N_CLASSES = 5
CLASS_CODES = ["amecro", "blujay", "carwre", "dowwoo", "easblu"]
SR = 32000
CROP_SAMPLES = 160000


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


def tiny_frontend(wave: torch.Tensor) -> torch.Tensor:
    """Injected stand-in for convnext_frontend: (160000,) -> (1, 32, 32).

    Deterministic in the wave values so padding/cropping differences are
    visible in the output.
    """
    return wave[:1024].reshape(1, 32, 32)


class FakeProvider:
    """ContextProvider stand-in recording its calls."""

    def __init__(self, n_classes: int = N_CLASSES) -> None:
        self.n_classes = n_classes
        self.calls: list[tuple] = []

    def vector(self, cell, week):
        self.calls.append((cell, week))
        if cell is None or week is None:
            return np.zeros(self.n_classes, dtype=np.float32), 0.0
        return np.full(self.n_classes, 0.25, dtype=np.float32), 1.0


def make_metadata(
    filepaths: list[str],
    multilabels: list[list[str]],
    cells: list | None = None,
    weeks: list | None = None,
) -> pa.Table:
    cols: dict = {
        "filepath": pa.array(filepaths, pa.string()),
        "ebird_code_multilabel": pa.array(multilabels, pa.list_(pa.string())),
    }
    if cells is not None:
        cols["cell"] = pa.array(cells, pa.string())
    if weeks is not None:
        cols["week"] = pa.array(weeks, pa.int64())
    return pa.table(cols)


def sine_audio(n: int = CROP_SAMPLES, freq: float = 440.0) -> np.ndarray:
    t = np.arange(n, dtype=np.float32) / SR
    return np.sin(2 * np.pi * freq * t).astype(np.float32)


def audio_lookup(table: dict[str, np.ndarray]):
    def load(filepath: str) -> np.ndarray:
        return table[filepath]

    return load


# ---------------------------------------------------------------------------
# Review Focus 3: duplicate filepaths refused
# ---------------------------------------------------------------------------


def test_duplicate_filepath_refused_naming_it():
    """A filepath appearing twice must refuse, not double-count."""
    meta = make_metadata(
        ["a.ogg", "b.ogg", "a.ogg"],
        [["amecro"], ["blujay"], ["carwre"]],
    )
    with pytest.raises(ValueError, match="a.ogg"):
        build_eval_dataset(
            meta,
            audio_lookup({}),
            FakeProvider(),
            CLASS_CODES,
            frontend=tiny_frontend,
        )


def test_duplicate_refusal_mentions_duplicate():
    meta = make_metadata(["x.ogg", "x.ogg"], [["amecro"], ["amecro"]])
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        build_eval_dataset(
            meta,
            audio_lookup({}),
            FakeProvider(),
            CLASS_CODES,
            frontend=tiny_frontend,
        )


# ---------------------------------------------------------------------------
# Label matrix: by code, in class_codes order, gaps counted
# ---------------------------------------------------------------------------


def test_multilabel_y_on_class_axis_by_code():
    meta = make_metadata(
        ["a.ogg", "b.ogg", "c.ogg"],
        [["amecro", "carwre"], [], ["easblu"]],
    )
    audio = audio_lookup(
        {f: sine_audio() for f in ["a.ogg", "b.ogg", "c.ogg"]}
    )
    ds = build_eval_dataset(
        meta, audio, FakeProvider(), CLASS_CODES, frontend=tiny_frontend
    )
    assert ds.y.shape == (3, N_CLASSES)
    expected = np.zeros((3, N_CLASSES), dtype=np.int64)
    expected[0, CLASS_CODES.index("amecro")] = 1
    expected[0, CLASS_CODES.index("carwre")] = 1
    expected[2, CLASS_CODES.index("easblu")] = 1
    np.testing.assert_array_equal(ds.y, expected)


def test_label_matrix_is_int8_not_int64():
    """Fleet jobs 5929/5970: SSW's label matrix (205,200 x 9,736) as int64
    is ~16 GB, and every DataLoader worker unpickles its own copy -- 8
    workers OOM-killed cells at 96G and ran at the 384G ceiling. The
    matrix only ever holds 0/1: int8 is exact and 8x smaller."""
    meta = make_metadata(["a.ogg", "b.ogg"], [["amecro"], []])
    audio = audio_lookup({f: sine_audio() for f in ["a.ogg", "b.ogg"]})
    ds = build_eval_dataset(
        meta, audio, FakeProvider(), CLASS_CODES, frontend=tiny_frontend
    )
    assert ds.y.dtype == np.int8
    assert ds.y.nbytes == 2 * N_CLASSES


def test_y_columns_follow_class_codes_order_not_position():
    """Reordering class_codes reorders y columns with it — by code."""
    meta = make_metadata(["a.ogg"], [["blujay"]])
    audio = audio_lookup({"a.ogg": sine_audio()})
    shuffled = ["easblu", "blujay", "amecro", "dowwoo", "carwre"]
    ds = build_eval_dataset(
        meta, audio, FakeProvider(), shuffled, frontend=tiny_frontend
    )
    assert ds.y[0, shuffled.index("blujay")] == 1
    assert ds.y[0].sum() == 1


def test_label_code_absent_from_axis_counted_not_keyerror():
    """A metadata label the model axis lacks is counted in unscoreable."""
    meta = make_metadata(["a.ogg"], [["amecro", "zzzunknown"]])
    audio = audio_lookup({"a.ogg": sine_audio()})
    ds = build_eval_dataset(
        meta, audio, FakeProvider(), CLASS_CODES, frontend=tiny_frontend
    )
    assert ds.unscoreable == ["zzzunknown"]
    assert ds.y[0, CLASS_CODES.index("amecro")] == 1
    assert ds.y[0].sum() == 1


# ---------------------------------------------------------------------------
# Context: null rows and missing columns take the provider's absent path
# ---------------------------------------------------------------------------


def test_null_cell_week_rows_get_null_context():
    meta = make_metadata(
        ["a.ogg", "b.ogg"],
        [["amecro"], ["blujay"]],
        cells=["8428309ffffffff", None],
        weeks=[12, None],
    )
    audio = audio_lookup({"a.ogg": sine_audio(), "b.ogg": sine_audio()})
    ds = build_eval_dataset(
        meta, audio, FakeProvider(), CLASS_CODES, frontend=tiny_frontend
    )
    dated = ds[0]
    undated = ds[1]
    assert dated["present"] == 1.0
    assert undated["present"] == 0.0
    assert torch.all(undated["context"] == 0)


def test_metadata_without_cell_week_columns_is_all_null_context():
    """POW-style metadata with no cell/week columns at all: every row
    takes the null-context path, no KeyError."""
    meta = make_metadata(["a.ogg"], [["amecro"]])
    audio = audio_lookup({"a.ogg": sine_audio()})
    provider = FakeProvider()
    ds = build_eval_dataset(
        meta, audio, provider, CLASS_CODES, frontend=tiny_frontend
    )
    item = ds[0]
    assert item["present"] == 0.0
    assert provider.calls == [(None, None)]


# ---------------------------------------------------------------------------
# Segment windows: pad right, deterministic head, no random crop at eval
# ---------------------------------------------------------------------------


def test_short_segment_zero_pads_right_in_waveform_domain():
    captured: list[torch.Tensor] = []

    def capturing_frontend(wave: torch.Tensor) -> torch.Tensor:
        captured.append(wave.clone())
        return tiny_frontend(wave)

    meta = make_metadata(["short.ogg"], [["amecro"]])
    audio = audio_lookup({"short.ogg": sine_audio(1000)})
    ds = build_eval_dataset(
        meta, audio, FakeProvider(), CLASS_CODES, frontend=capturing_frontend
    )
    ds[0]
    wave = captured[0]
    assert wave.shape == (CROP_SAMPLES,)
    assert torch.all(wave[1000:] == 0)
    assert not torch.all(wave[:1000] == 0)


def test_long_segment_takes_deterministic_head():
    """Eval windows are not randomly cropped: the segment IS the window;
    anything longer takes the head, identically on every call."""
    captured: list[torch.Tensor] = []

    def capturing_frontend(wave: torch.Tensor) -> torch.Tensor:
        captured.append(wave.clone())
        return tiny_frontend(wave)

    long = sine_audio(200000)
    meta = make_metadata(["long.ogg"], [["amecro"]])
    audio = audio_lookup({"long.ogg": long})
    ds = build_eval_dataset(
        meta, audio, FakeProvider(), CLASS_CODES, frontend=capturing_frontend
    )
    ds[0]
    ds[0]
    assert captured[0].shape == (CROP_SAMPLES,)
    np.testing.assert_array_equal(captured[0].numpy(), long[:CROP_SAMPLES])
    assert torch.equal(captured[0], captured[1])


# ---------------------------------------------------------------------------
# evaluate_model: scores, dispatch, batch seam
# ---------------------------------------------------------------------------


def _three_row_dataset(provider=None):
    meta = make_metadata(
        ["a.ogg", "b.ogg", "c.ogg"],
        [["amecro"], ["blujay"], []],
        cells=["8428309ffffffff", None, "8428309ffffffff"],
        weeks=[12, None, 30],
    )
    audio = audio_lookup(
        {
            "a.ogg": sine_audio(freq=440.0),
            "b.ogg": sine_audio(freq=880.0),
            "c.ogg": sine_audio(freq=1760.0),
        }
    )
    return build_eval_dataset(
        meta,
        audio,
        provider or FakeProvider(),
        CLASS_CODES,
        frontend=tiny_frontend,
    )


def test_evaluate_model_twin_scores_match_direct_forward():
    model = tiny_convnext()
    ds = _three_row_dataset()
    scores = evaluate_model(model, ds, arm="twin", batch_size=2)
    assert scores.shape == (3, N_CLASSES)

    # Same batching as the runner (2 then 1): conv kernels are not
    # bit-stable across different batch sizes, so the bit-equality
    # claim is made against identically-shaped forwards.
    model.eval()
    with torch.no_grad():
        first = model(
            torch.stack([ds[0]["inputs"], ds[1]["inputs"]])
        ).logits.numpy()
        second = model(ds[2]["inputs"].unsqueeze(0)).logits.numpy()
    np.testing.assert_array_equal(scores, np.concatenate([first, second]))


def test_evaluate_model_wrapper_dispatch_identity_at_init():
    """The film arm goes through the wrapper contract; at zero-init the
    wrapped scores equal the bare checkpoint's (the FiLM gate), which
    proves the wrapper path actually ran with context."""
    model = tiny_convnext()
    torch.manual_seed(7)
    encoder = ContextEncoder(N_CLASSES, d_ctx=8)
    wrapper = FiLMConvNext(model, encoder)
    ds = _three_row_dataset()

    wrapped = evaluate_model(wrapper, ds, arm="film", batch_size=2)
    bare = evaluate_model(model, ds, arm="twin", batch_size=2)
    np.testing.assert_array_equal(wrapped, bare)


def test_evaluate_model_empty_dataset_returns_zero_rows():
    """An empty eval set is the GATE's refusal, not the runner's: the
    runner hands back (0, n_classes) and the gate refuses loudly."""
    meta = make_metadata([], [])
    ds = build_eval_dataset(
        meta,
        audio_lookup({}),
        FakeProvider(),
        CLASS_CODES,
        frontend=tiny_frontend,
    )
    scores = evaluate_model(tiny_convnext(), ds, arm="twin")
    assert scores.shape == (0, N_CLASSES)


# ---------------------------------------------------------------------------
# C1 (final review): require_context — a conditioned eval fed a table where
# EVERY row lacks (cell, week) would silently run present=0 throughout and
# the conditioned−twin headline would measure training-side noise only.
# ---------------------------------------------------------------------------


def test_require_context_all_null_raises_named_error():
    from birdology.phase_c.evaluate import AllNullContextError

    meta = make_metadata(["a.ogg"], [["amecro"]])  # no cell/week columns
    with pytest.raises(AllNullContextError, match="C1"):
        build_eval_dataset(
            meta,
            audio_lookup({}),
            FakeProvider(),
            CLASS_CODES,
            frontend=tiny_frontend,
            require_context=True,
        )


def test_require_context_all_null_values_also_refused():
    from birdology.phase_c.evaluate import AllNullContextError

    meta = make_metadata(
        ["a.ogg", "b.ogg"], [["amecro"], ["blujay"]],
        cells=[None, None], weeks=[None, None],
    )
    with pytest.raises(AllNullContextError, match="C1"):
        build_eval_dataset(
            meta, audio_lookup({}), FakeProvider(), CLASS_CODES,
            frontend=tiny_frontend, require_context=True,
        )


def test_require_context_partial_null_passes():
    meta = make_metadata(
        ["a.ogg", "b.ogg"], [["amecro"], ["blujay"]],
        cells=["8428309ffffffff", None], weeks=[12, None],
    )
    ds = build_eval_dataset(
        meta, audio_lookup({"a.ogg": sine_audio(), "b.ogg": sine_audio()}),
        FakeProvider(), CLASS_CODES,
        frontend=tiny_frontend, require_context=True,
    )
    assert len(ds) == 2


def test_require_context_defaults_False_for_POW_shape():
    meta = make_metadata(["a.ogg"], [["amecro"]])
    ds = build_eval_dataset(
        meta, audio_lookup({"a.ogg": sine_audio()}), FakeProvider(),
        CLASS_CODES, frontend=tiny_frontend,
    )
    assert len(ds) == 1


# ---------------------------------------------------------------------------
# POW-val reuses EvalDataset (fleet job 5679): each item carries its
# multi-hot ``labels`` row on the model axis so train._validate can score
# multi-label soundscape windows — all-zero rows for empty windows.
# ---------------------------------------------------------------------------


def test_item_carries_multi_hot_labels_row_on_model_axis():
    meta = make_metadata(
        ["a.ogg", "b.ogg", "c.ogg"],
        [["blujay", "easblu"], [], ["amecro"]],
    )
    ds = build_eval_dataset(
        meta,
        audio_lookup({f: sine_audio() for f in ["a.ogg", "b.ogg", "c.ogg"]}),
        FakeProvider(),
        CLASS_CODES,
        frontend=tiny_frontend,
    )
    rows = [ds[i]["labels"] for i in range(3)]
    for i, row in enumerate(rows):
        assert isinstance(row, torch.Tensor)
        assert torch.equal(row, torch.from_numpy(ds.y[i]))
    assert rows[0].tolist() == [0, 1, 0, 0, 1]
    assert rows[1].tolist() == [0, 0, 0, 0, 0]  # empty window: all-zero
    assert rows[2].tolist() == [1, 0, 0, 0, 0]


# ---------------------------------------------------------------------------
# Fleet job 5730: eval brackets itself with start/end log lines (rows,
# device, wall time) — silence was indistinguishable from progress.
# ---------------------------------------------------------------------------


def test_evaluate_model_logs_start_and_end(caplog):
    import logging

    import torch
    from transformers import ConvNextConfig, ConvNextForImageClassification

    from birdology.phase_c.evaluate import build_eval_dataset, evaluate_model

    torch.manual_seed(0)
    model = ConvNextForImageClassification(
        ConvNextConfig(num_channels=1, num_stages=2, hidden_sizes=[8, 16],
                       depths=[1, 1], num_labels=3, image_size=32)
    )
    meta = pa.table({
        "filepath": pa.array(["a.wav", "b.wav", "c.wav"], pa.string()),
        "ebird_code_multilabel": pa.array(
            [["LABEL_0"], [], ["LABEL_2"]], pa.list_(pa.string())
        ),
    })

    class _Prov:
        def vector(self, cell, week):
            return np.zeros(3, dtype=np.float32), 0.0

    ds = build_eval_dataset(
        meta, lambda f: np.zeros(1600, dtype=np.float32), _Prov(),
        ["LABEL_0", "LABEL_1", "LABEL_2"],
        frontend=lambda w: torch.zeros(1, 32, 32), crop_s=0.05,
    )
    caplog.set_level(logging.INFO, logger="birdology.phase_c")
    scores = evaluate_model(model, ds, arm="twin", batch_size=2)
    assert scores.shape == (3, 3)
    msgs = [r.getMessage() for r in caplog.records]
    start = [m for m in msgs if m.startswith("eval start")]
    end = [m for m in msgs if m.startswith("eval end")]
    assert len(start) == 1 and "rows=3" in start[0] and "device=cpu" in start[0]
    assert len(end) == 1 and "rows=3" in end[0] and "wall=" in end[0]
