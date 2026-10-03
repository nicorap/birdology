"""The query set: every (lat, lon, week) a consumer of the prior looks up.

Option (b) of the EOD sizing ruling: the prior is built only where it is
queried. The query set is RESOLUTION-FREE (lat, lon, week), exactly like the
test split, so each resolution derives its own cells through the SAME h3
call `indexing.assign_cells` uses -- a pre-computed cell is fixed at one
resolution and wrong at every other.
"""
import hashlib
import json
import sys
from pathlib import Path

import h3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from birdology.prior.query_set import (
    QUERY_SET_COLUMNS,
    build_query_set,
    halo_cells,
    load_query_set,
    write_query_set,
)

import build_query_set as cli

MANIFEST_SCHEMA = pa.schema(
    [
        ("filepath", pa.string()),
        ("cell", pa.string()),
        ("week", pa.int8()),
        ("ebird_code", pa.string()),
        ("split", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
    ]
)


def _manifest(path: Path, rows: list[tuple]) -> Path:
    """rows: (lat, lon, week) -- week/lat may be None."""
    cols = {name: [] for name in MANIFEST_SCHEMA.names}
    for i, (lat, lon, week) in enumerate(rows):
        cols["filepath"].append(f"f{i}.ogg")
        cols["cell"].append(
            h3.latlng_to_cell(lat, lon, 4) if lat is not None and lon is not None else None
        )
        cols["week"].append(week)
        cols["ebird_code"].append("x")
        cols["split"].append("train")
        cols["lat"].append(lat)
        cols["lon"].append(lon)
    pq.write_table(pa.table(cols, schema=MANIFEST_SCHEMA), path)
    return path


def test_the_query_set_is_the_DISTINCT_dated_located_rows_of_every_manifest(tmp_path):
    a = _manifest(
        tmp_path / "a.parquet",
        [(55.0, 12.0, 5), (55.0, 12.0, 5), (55.0, 12.0, 6), (56.0, 10.0, None)],
    )
    b = _manifest(
        tmp_path / "b.parquet",
        [(55.0, 12.0, 5), (57.0, 9.0, 52), (None, None, None)],
    )
    table, provenance = build_query_set([a, b])
    assert table.column_names == list(QUERY_SET_COLUMNS)
    got = sorted(zip(*(table.column(c).to_pylist() for c in QUERY_SET_COLUMNS)))
    assert got == [(55.0, 12.0, 5), (55.0, 12.0, 6), (57.0, 9.0, 52)]
    by_path = {s["path"]: s for s in provenance["sources"]}
    assert by_path[str(a.resolve())]["rows"] == 4
    assert by_path[str(a.resolve())]["rows_with_context"] == 3
    assert by_path[str(b.resolve())]["rows"] == 3
    assert by_path[str(b.resolve())]["rows_with_context"] == 2
    assert provenance["distinct_rows"] == 3


def test_a_null_context_manifest_contributes_nothing_and_is_still_recorded(tmp_path):
    """POW carries no dates: it must be listed (so the operator sees it was
    read) and must add no row."""
    pow_ = _manifest(tmp_path / "pow.parquet", [(55.0, 12.0, None)] * 3)
    other = _manifest(tmp_path / "x.parquet", [(55.0, 12.0, 9)])
    table, provenance = build_query_set([pow_, other])
    assert table.num_rows == 1
    assert {s["path"]: s["rows_with_context"] for s in provenance["sources"]}[
        str(pow_.resolve())
    ] == 0


@pytest.mark.parametrize("week", [0, 53])
def test_a_week_outside_1_to_52_is_REFUSED_not_folded(tmp_path, week):
    bad = _manifest(tmp_path / "bad.parquet", [(55.0, 12.0, week)])
    with pytest.raises(ValueError, match="week"):
        build_query_set([bad])


def test_a_manifest_missing_a_column_is_refused_naming_it(tmp_path):
    path = tmp_path / "m.parquet"
    pq.write_table(pa.table({"lat": [1.0], "week": pa.array([3], pa.int8())}), path)
    with pytest.raises(ValueError, match="lon"):
        build_query_set([path])


def test_write_then_load_round_trips_with_a_sha256_of_the_written_bytes(tmp_path):
    m = _manifest(tmp_path / "m.parquet", [(55.0, 12.0, 5), (56.0, 10.5, 1)])
    table, provenance = build_query_set([m])
    out = tmp_path / "q.parquet"
    written, prov_path = write_query_set(table, provenance, out)
    digest = hashlib.sha256(written.read_bytes()).hexdigest()
    recorded = json.loads(prov_path.read_text())
    assert recorded["sha256"] == digest
    assert recorded["distinct_rows"] == 2

    q = load_query_set(written)
    assert q.sha256 == digest
    assert q.path == str(written.resolve())
    assert len(q) == 2


def test_contexts_at_a_resolution_use_the_assign_cells_h3_call(tmp_path):
    """Each resolution derives its own cells: the SAME lat/lon gives a res-3
    cell at r3 and a res-5 cell at r5 -- never one reused across both."""
    m = _manifest(tmp_path / "m.parquet", [(55.6867, 12.57, 5), (55.69, 12.58, 5)])
    table, provenance = build_query_set([m])
    out, _ = write_query_set(table, provenance, tmp_path / "q.parquet")
    q = load_query_set(out)
    for res in (3, 4, 5):
        expected = {
            (h3.latlng_to_cell(55.6867, 12.57, res), 5),
            (h3.latlng_to_cell(55.69, 12.58, res), 5),
        }
        assert q.contexts_at(res) == expected


def test_load_refuses_a_file_that_is_not_a_query_set(tmp_path):
    path = tmp_path / "q.parquet"
    pq.write_table(pa.table({"lat": [1.0], "lon": [2.0]}), path)
    with pytest.raises(ValueError, match="week"):
        load_query_set(path)


def test_the_halo_is_the_query_cells_plus_their_first_ring_INTERSECTED_with_observed():
    centre = h3.latlng_to_cell(55.7, 12.5, 4)
    ring = sorted(set(h3.grid_disk(centre, 1)) - {centre})
    far = h3.latlng_to_cell(40.0, -3.0, 4)
    observed = [centre, ring[0], ring[1], far]
    got = halo_cells({centre}, observed)
    # ring[2..5] are neighbours but unobserved: off the grid, as in the full
    # build. `far` is observed but not adjacent: not needed.
    assert got == sorted([centre, ring[0], ring[1]])


def test_an_unobserved_query_cell_stays_off_the_halo_grid():
    centre = h3.latlng_to_cell(55.7, 12.5, 4)
    ring = sorted(set(h3.grid_disk(centre, 1)) - {centre})
    # The query cell itself is unobserved; its observed neighbour is NOT
    # needed by any on-grid query context, but it is harmless to include
    # and the definition (Q u ring1(Q)) n observed keeps it.
    assert halo_cells({centre}, [ring[0]]) == [ring[0]]


def test_the_cli_writes_the_query_set_and_its_provenance(tmp_path):
    m1 = _manifest(tmp_path / "XCL.parquet", [(55.0, 12.0, 5)])
    m2 = _manifest(tmp_path / "HSN.parquet", [(42.0, -76.0, 20), (55.0, 12.0, 5)])
    out = tmp_path / "out" / "query_set.parquet"
    cli.main(["--manifests", str(m1), str(m2), "--out", str(out)])
    assert out.exists()
    prov = json.loads(out.with_suffix(".provenance.json").read_text())
    assert prov["distinct_rows"] == 2
    assert prov["sha256"] == hashlib.sha256(out.read_bytes()).hexdigest()
    assert [s["path"] for s in prov["sources"]] == [
        str(m1.resolve()),
        str(m2.resolve()),
    ]


def test_smooth_on_plan_EQUALS_smooth_bit_for_bit_on_a_dense_random_disk():
    """Every cell of a radius-2 disk observed (so interior cells have six
    neighbours), random values, every week queried for three cells. The
    neighbour ORDER matters for the float sum, so this pins `grid_disk` order
    rather than any order that happens to agree on a sparse fixture."""
    import numpy as np

    from birdology.prior.query_set import plan_restriction, smooth_on_plan
    from birdology.prior.smoothing import SmoothingParams, smooth

    centre = h3.latlng_to_cell(55.7, 12.5, 4)
    cells = sorted(h3.grid_disk(centre, 2))
    rng = np.random.default_rng(7)
    species = [5, 6]
    rows = [(s, c, w) for s in species for c in cells for w in range(1, 53)]
    p = rng.uniform(1e-6, 1.0, len(rows))
    table = pa.table(
        {
            "species_key": pa.array([r[0] for r in rows], pa.int64()),
            "cell": pa.array([r[1] for r in rows], pa.string()),
            "week": pa.array([r[2] for r in rows], pa.int8()),
            "p": pa.array(p, pa.float64()),
        }
    )
    query = {(c, w) for c in (centre, cells[0], cells[-1]) for w in range(1, 53)}
    params = SmoothingParams(space_weight=0.37, time_weight=0.21)
    ref = smooth(table, params)
    ref_p = dict(zip(rows, ref.column("p").to_pylist()))
    plan = plan_restriction(query, cells)
    value_of = dict(zip(rows, p))
    support = np.array(
        [[value_of[(s, c, w)] for c, w in plan.support_points] for s in species]
    )
    got = smooth_on_plan(support, plan, params)
    for i, s in enumerate(species):
        for j, (c, w) in enumerate(plan.on_grid_contexts):
            assert got[i, j] == ref_p[(s, c, w)], (s, c, w)


@pytest.mark.parametrize("week, neighbours", [(1, (52, 2)), (52, (51, 1))])
def test_the_plan_reaches_ACROSS_the_year_wrap(week, neighbours):
    from birdology.prior.query_set import plan_restriction

    centre = h3.latlng_to_cell(55.7, 12.5, 4)
    plan = plan_restriction({(centre, week)}, [centre])
    assert plan.support_points == sorted({(centre, week), *((centre, w) for w in neighbours)})
    prev, nxt = neighbours
    assert plan.support_points[plan.prev_index[0]] == (centre, prev)
    assert plan.support_points[plan.next_index[0]] == (centre, nxt)
