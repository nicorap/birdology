"""phase_c_cells: the CELLS_TSV the sweep array indexes.

The sbatch reads each line positionally (9 tab fields) and refuses an
unknown prior_variant only AFTER staging; the grid's vocabulary is
pinned here against phase_c_run's own so a typo fails offline.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_REPO = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_REPO / "scripts"))


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


cells = _load("phase_c_cells", _REPO / "scripts/cluster/phase_c_cells.py")
run = _load("phase_c_run", _REPO / "scripts/phase_c_run.py")


def test_grid_counts_match_spec():
    g = cells.grid()
    arms = [c[0] for c in g]
    variants = [c[6] for c in g if c[0] != "twin"]
    assert arms.count("twin") == 10
    assert variants.count("full") == 45
    assert len(variants) - variants.count("full") == 135
    assert len(g) == 190
    assert len(set(g)) == len(g)


def test_vocabulary_agrees_with_run_cli():
    for arm, model, *_rest, variant, _tests, _glob in cells.grid():
        assert arm in run._ARMS
        assert variant in run.PRIOR_VARIANTS
        assert model in ("convnext", "ast")
    assert set(cells.VARIANTS) == set(run.PRIOR_VARIANTS)


def test_token_arm_only_on_ast():
    assert {c[1] for c in cells.grid() if c[0] == "token"} == {"ast"}


def _write_rows(d: Path, rows: list[dict]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), d / "run-x.parquet")


def test_skip_done_drops_only_fully_evaluated_cells(tmp_path):
    ckpt = cells.CHECKPOINTS["convnext"][0]
    base = dict(arm="film", model_id=ckpt, resolution=4, seed=0, prior_variant="full")
    full = [dict(base, test_set=t) for t in cells.TEST_SETS]
    partial = [dict(base, seed=1, test_set=t) for t in cells.TEST_SETS[:3]]
    _write_rows(tmp_path, full + partial)

    left = cells.pending(cells.grid(), cells.done_keys(tmp_path))
    keys = {(c[0], c[1], c[4], c[5], c[6]) for c in left}
    assert ("film", "convnext", 4, 0, "full") not in keys
    assert ("film", "convnext", 4, 1, "full") in keys  # partial: re-run
    assert len(left) == 189


def test_cli_writes_nine_tab_fields_and_refuses_overwrite(tmp_path):
    out = tmp_path / "c.tsv"
    assert cells.main(["--out", str(out)]) == 0
    lines = out.read_text().splitlines()
    assert len(lines) == 190
    assert all(len(line.split("\t")) == 9 for line in lines)
    with pytest.raises(SystemExit):
        cells.main(["--out", str(out)])
