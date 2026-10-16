"""Commonness shares from raw counts, and the app's name aliases."""
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.app_tiers import match_names, shares_from_counts, unmatched_names  # noqa: E402


def counts(rows):
    """rows: (species_key, cell, week, n)."""
    k, c, w, n = zip(*rows)
    return pa.table({"species_key": pa.array(k, pa.int64()), "cell": pa.array(c, pa.string()),
                     "week": pa.array(w, pa.int8()), "n": pa.array(n, pa.int64())})


def test_share_of_all_records_in_the_cell_week():
    t = counts([(1, "a", 10, 60), (2, "a", 10, 40), (9, "a", 10, 100)])   # species 9 is not exported
    cells, p = shares_from_counts(t, [1, 2], window=0, min_records=1)
    assert cells == ["a"] and p.shape == (1, 52, 2)
    assert p[0, 9, 0] == pytest.approx(0.3) and p[0, 9, 1] == pytest.approx(0.2)
    assert p[0, 10, 0] == 0.0                                            # no records that week


def test_pooling_over_neighbouring_weeks_wraps_around_the_year():
    t = counts([(1, "a", 52, 10), (9, "a", 52, 10), (9, "a", 2, 20)])
    _, p = shares_from_counts(t, [1], window=1, min_records=1)
    assert p[0, 0, 0] == pytest.approx(10 / 40)    # week 1 pools weeks 52, 1, 2
    assert p[0, 2, 0] == pytest.approx(0.0)        # week 3 pools weeks 2, 3, 4


def test_sparse_cell_weeks_fall_back_to_the_national_share():
    t = counts([(1, "a", 10, 90), (9, "a", 10, 10), (1, "b", 10, 0), (9, "b", 10, 5)])
    cells, p = shares_from_counts(t, [1], window=0, min_records=50)
    a, b = cells.index("a"), cells.index("b")
    assert p[a, 9, 0] == pytest.approx(0.9)            # 100 records: local
    assert p[b, 9, 0] == pytest.approx(90 / 105)       # 5 records: national


def test_cells_are_sorted_and_unknown_species_are_zero():
    t = counts([(9, "z", 1, 5), (9, "a", 1, 5)])
    cells, p = shares_from_counts(t, [1], window=0, min_records=1)
    assert cells == ["a", "z"] and not p.any()


def test_aliases_cover_genus_moves_and_spellings_exact_still_wins():
    app = ["Curruca communis", "Phylloscopus sibilatrix", "Zapornia parva", "Turdus merula"]
    prior = {1: "Sylvia communis", 2: "Phylloscopus sibillatrix", 3: "Porzana parva", 4: "Turdus merula",
             5: "Struthio camelus"}
    assert match_names(prior, app) == {1: "Curruca communis", 2: "Phylloscopus sibilatrix",
                                       3: "Zapornia parva", 4: "Turdus merula"}


def test_unmatched_names_lists_only_keys_without_a_match():
    names = {1: "Thinornis dubius", 2: "Struthio camelus"}
    assert unmatched_names(names, {1: "Charadrius dubius"}) == ["Struthio camelus"]
