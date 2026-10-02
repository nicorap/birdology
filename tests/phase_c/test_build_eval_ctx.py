"""The context-joined eval table builder (finding C1, part 3).

scripts/phase_c_build_eval_ctx.py joins a test-set label metadata
parquet with (cell, week) so conditioned evals see real context. The
pins here:

* cell/week values MATCH phase_a's own derivation on the same inputs —
  the builder IMPORTS h3-at-resolution + iso_week + the filename dater
  from the phase_a/prior modules, never re-implements them, and these
  tests prove it by computing the expected values through those same
  functions.
* rows undatable / uncoordinated are KEPT with null cell/week (they are
  the measured degradation path, not drops).
* the output feeds build_eval_dataset: all-null refusal fires under
  require_context=True on an undatable table; a partial-null table
  passes.
* label + filepath columns pass through untouched; configurable column
  names map onto the standard output shape.
"""
from __future__ import annotations

import sys
from pathlib import Path

import h3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_REPO = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_REPO / "src"))
sys.path.insert(0, str(_REPO / "scripts"))

import phase_c_build_eval_ctx
from birdology.phase_a.soundscape_dates import parse_filename_date
from birdology.prior.indexing import iso_week


def _meta(
    filepaths: list[str],
    lats: list,
    lons: list,
) -> pa.Table:
    return pa.table(
        {
            "filepath": pa.array(filepaths, pa.string()),
            "ebird_code_multilabel": pa.array(
                [["amecro"]] * len(filepaths), pa.list_(pa.string())
            ),
            "lat": pa.array(lats, pa.float64()),
            "long": pa.array(lons, pa.float64()),
        }
    )


def test_dated_row_gets_phase_a_exact_cell_and_week():
    name = "HSN_001_20150708_061805.ogg"
    out = phase_c_build_eval_ctx.build_eval_ctx(
        _meta([name], [55.6867], [12.57]), resolution=4
    )
    row = out.to_pylist()[0]
    # the SAME derivation phase_a uses, computed independently here
    assert row["cell"] == h3.latlng_to_cell(55.6867, 12.57, 4)
    assert row["week"] == iso_week(parse_filename_date(name))
    assert row["filepath"] == name
    assert row["ebird_code_multilabel"] == ["amecro"]


DATED = "HSN_001_20150708_061805.ogg"


def test_undatable_row_kept_with_null_cell_week():
    # A dated companion row: an ALL-null table now refuses by default.
    out = phase_c_build_eval_ctx.build_eval_ctx(
        _meta(["NODATE_station.ogg", DATED], [55.0, 55.0], [12.0, 12.0]),
        resolution=4,
    )
    assert out.num_rows == 2
    row = out.to_pylist()[0]
    assert row["cell"] is None
    assert row["week"] is None


def test_uncoordinated_row_kept_with_null_cell_week():
    out = phase_c_build_eval_ctx.build_eval_ctx(
        _meta(
            ["HSN_001_20150708_061805.ogg", "HSN_002_20150709_061805.ogg"],
            [None, 55.0],
            [None, 12.0],
        ),
        resolution=4,
    )
    assert out.num_rows == 2
    row = out.to_pylist()[0]
    assert row["cell"] is None
    assert row["week"] is None


def test_resolution_is_honoured():
    name = "HSN_001_20150708_061805.ogg"
    out = phase_c_build_eval_ctx.build_eval_ctx(
        _meta([name], [55.6867], [12.57]), resolution=6
    )
    assert out.to_pylist()[0]["cell"] == h3.latlng_to_cell(55.6867, 12.57, 6)


def test_configurable_column_names():
    t = pa.table(
        {
            "path": pa.array(["HSN_001_20150708_061805.ogg"], pa.string()),
            "labels": pa.array([["amecro"]], pa.list_(pa.string())),
            "latitude": pa.array([55.0], pa.float64()),
            "longitude": pa.array([12.0], pa.float64()),
        }
    )
    out = phase_c_build_eval_ctx.build_eval_ctx(
        t,
        resolution=4,
        filepath_col="path",
        labels_col="labels",
        lat_col="latitude",
        lon_col="longitude",
    )
    row = out.to_pylist()[0]
    assert row["filepath"] == "HSN_001_20150708_061805.ogg"
    assert row["ebird_code_multilabel"] == ["amecro"]
    assert row["cell"] == h3.latlng_to_cell(55.0, 12.0, 4)


def test_all_null_output_refused_by_require_context():
    """Round-trip: an all-undatable table built here still trips the C1
    refusal downstream."""
    import numpy as np

    from birdology.phase_c.evaluate import (
        AllNullContextError,
        build_eval_dataset,
    )

    out = phase_c_build_eval_ctx.build_eval_ctx(
        _meta(["NODATE_a.ogg", "NODATE_b.ogg"], [None, None], [None, None]),
        resolution=4,
        allow_undated=True,  # the builder's own refusal is waived here
    )

    def frontend(w):
        return w[:1024].reshape(1, 32, 32)

    class P:
        def vector(self, cell, week):
            return np.zeros(3, dtype=np.float32), 0.0

    with pytest.raises(AllNullContextError, match="C1"):
        build_eval_dataset(
            out, lambda f: None, P(), ["a", "b", "c"],
            frontend=frontend, require_context=True,
        )


def test_partial_null_output_passes_require_context():
    import numpy as np

    from birdology.phase_c.evaluate import build_eval_dataset

    out = phase_c_build_eval_ctx.build_eval_ctx(
        _meta(
            ["HSN_001_20150708_061805.ogg", "NODATE_b.ogg"],
            [55.0, None],
            [12.0, None],
        ),
        resolution=4,
    )

    def frontend(w):
        return w[:1024].reshape(1, 32, 32)

    class P:
        def vector(self, cell, week):
            return np.zeros(3, dtype=np.float32), 0.0

    ds = build_eval_dataset(
        out, lambda f: None, P(), ["a", "b", "c"],
        frontend=frontend, require_context=True,
    )
    assert len(ds) == 2


def test_cli_writes_parquet(tmp_path):
    src = tmp_path / "meta.parquet"
    pq.write_table(
        _meta(["HSN_001_20150708_061805.ogg"], [55.0], [12.0]), src
    )
    out = tmp_path / "HSN_eval_ctx.parquet"
    rc = phase_c_build_eval_ctx.main(
        ["--metadata", str(src), "--resolution", "4", "--out", str(out)]
    )
    assert rc == 0
    table = pq.read_table(out)
    assert table.num_rows == 1
    assert table.to_pylist()[0]["cell"] == h3.latlng_to_cell(55.0, 12.0, 4)


def test_module_is_import_safe():
    assert callable(phase_c_build_eval_ctx.main)


# ---------------------------------------------------------------------------
# POW-val's table (fleet job 5679): POW is all-undated, so its eval_ctx
# table is ALL null-context. That is spec'd for POW (val only, null-
# context path) — but for the six test sets an all-null table means the
# C1 join silently failed. So: refuse by default, emit only under the
# explicit --allow-undated flag.
# ---------------------------------------------------------------------------

#: POW test_5s shape: undatable filenames, multi-label incl. empty windows.
POW_NAMES = ["POW_0001_a.ogg", "POW_0002_b.ogg", "POW_0003_c.ogg"]
POW_LABELS = [["amecro", "blujay"], [], ["carwre"]]


def _pow_meta() -> pa.Table:
    return pa.table(
        {
            "filepath": pa.array(POW_NAMES, pa.string()),
            "ebird_code_multilabel": pa.array(
                POW_LABELS, pa.list_(pa.string())
            ),
            "lat": pa.array([None, 10.5, None], pa.float64()),
            "long": pa.array([None, -84.0, None], pa.float64()),
        }
    )


def test_all_undated_table_refused_by_default():
    with pytest.raises(ValueError, match="allow-undated") as exc:
        phase_c_build_eval_ctx.build_eval_ctx(_pow_meta(), resolution=4)
    assert "3" in str(exc.value)  # names the count


def test_all_undated_table_emitted_under_allow_undated():
    out = phase_c_build_eval_ctx.build_eval_ctx(
        _pow_meta(), resolution=4, allow_undated=True
    )
    assert out.num_rows == 3
    rows = out.to_pylist()
    assert [r["filepath"] for r in rows] == POW_NAMES
    assert [r["ebird_code_multilabel"] for r in rows] == POW_LABELS
    assert all(r["cell"] is None and r["week"] is None for r in rows)
    assert out.schema.metadata[b"allow_undated"] == b"true"


def test_allow_undated_output_feeds_val_without_require_context():
    """The POW table is what phase_c_run's val path consumes:
    build_eval_dataset(require_context=False) keeps every row with
    multi-hot y (the all-zero empty window included)."""
    import numpy as np

    from birdology.phase_c.evaluate import build_eval_dataset

    out = phase_c_build_eval_ctx.build_eval_ctx(
        _pow_meta(), resolution=4, allow_undated=True
    )

    class P:
        def vector(self, cell, week):
            return np.zeros(3, dtype=np.float32), 0.0

    ds = build_eval_dataset(
        out, lambda f: None, P(), ["amecro", "blujay", "carwre"],
        frontend=lambda w: w, require_context=False,
    )
    assert ds.y.tolist() == [[1, 1, 0], [0, 0, 0], [0, 0, 1]]


def test_cli_refuses_all_undated_without_flag(tmp_path):
    src = tmp_path / "POW_metadata_test_5s.parquet"
    pq.write_table(_pow_meta(), src)
    out = tmp_path / "POW_r4_eval_ctx.parquet"
    with pytest.raises(ValueError, match="allow-undated"):
        phase_c_build_eval_ctx.main(
            ["--metadata", str(src), "--resolution", "4", "--out", str(out)]
        )
    assert not out.exists()


def test_cli_allow_undated_writes_pow_table(tmp_path):
    src = tmp_path / "POW_metadata_test_5s.parquet"
    pq.write_table(_pow_meta(), src)
    out = tmp_path / "POW_r4_eval_ctx.parquet"
    rc = phase_c_build_eval_ctx.main(
        [
            "--metadata", str(src), "--resolution", "4",
            "--out", str(out), "--allow-undated",
        ]
    )
    assert rc == 0
    table = pq.read_table(out)
    assert table.column_names == [
        "filepath", "ebird_code_multilabel", "cell", "week"
    ]
    assert table.num_rows == 3
