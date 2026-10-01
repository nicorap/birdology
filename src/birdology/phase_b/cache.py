"""Read Phase A embedding caches — the single gateway for Phase B.

A cache is a directory of parquet shards written by
`birdology.phase_a.embed.embed_shard`. Two writer-side facts matter here
and are read from schema METADATA, never inferred from values:

- `schema_variant` names which columns a shard carries
  ('embedding' | 'embedding+logits').
- `perch_logits_are_raw_uncalibrated` == b"true" (perch caches only)
  declares the logits raw floats, not sigmoid probabilities. Mixing a
  flagged shard with an unflagged one in a single cache dir would put raw
  and calibrated numbers on the same class axis — that heterogeneity is
  refused loudly, not averaged over.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

#: Metadata key the perch encoder writes beside `schema_variant` (see
#: phase_a/embed.py); its PRESENCE with value b"true" is the raw signal.
RAW_LOGITS_KEY = b"perch_logits_are_raw_uncalibrated"


@dataclass(frozen=True)
class CacheView:
    """A loaded cache: concatenated shard rows plus the facts a scorer
    must not guess."""

    table: pa.Table  # concatenated shards, original columns
    logits_are_raw: bool  # True iff the perch raw flag is present=="true"
    n_classes: int  # width of the logits column (0 if absent)


def load_cache(cache_dir: Path) -> CacheView:
    """Load every parquet shard under *cache_dir* into one table.

    Raises ValueError when the dir holds no parquet files (a wrong path or
    an unfinished fleet, never a valid empty cache) and when shards
    disagree on the raw-logits flag (a heterogeneous cache is the
    cache-poisoning class — refuse, don't concatenate).
    """
    cache_dir = Path(cache_dir)
    paths = sorted(cache_dir.glob("*.parquet"))
    if not paths:
        raise ValueError(f"no parquet shards found in cache dir {cache_dir}")

    tables: list[pa.Table] = []
    flags: set[bool] = set()
    for path in paths:
        table = pq.read_table(path)
        metadata = table.schema.metadata or {}
        flags.add(metadata.get(RAW_LOGITS_KEY) == b"true")
        tables.append(table)

    if len(flags) > 1:
        raise ValueError(
            f"cache dir {cache_dir} mixes shards with and without "
            f"{RAW_LOGITS_KEY.decode()}=true -- a heterogeneous cache "
            "puts raw and calibrated logits on one class axis; refusing "
            "to concatenate"
        )

    combined = pa.concat_tables(tables, promote_options="default")
    n_classes = 0
    if "logits" in combined.column_names and combined.num_rows > 0:
        first = combined.column("logits")[0].as_py()
        n_classes = 0 if first is None else len(first)
    return CacheView(
        table=combined, logits_are_raw=flags.pop(), n_classes=n_classes
    )


def class_index(labels_path: Path) -> dict[str, int]:
    """{eBird code: class row index} from a one-label-per-line file.

    Blank lines are skipped WITHOUT consuming an index: the model head has
    no blank row. A duplicate code raises — in a dict it would silently
    shadow one of the two class rows.
    """
    index: dict[str, int] = {}
    for line in Path(labels_path).read_text(encoding="utf-8").splitlines():
        code = line.strip()
        if not code:
            continue
        if code in index:
            raise ValueError(
                f"duplicate label {code!r} in {labels_path} -- a duplicate "
                "silently shadows a class row"
            )
        index[code] = len(index)
    return index


def align_labels(
    table: pa.Table,
    class_idx: dict[str, int],
    label_col: str = "ebird_code_multilabel",
) -> tuple[np.ndarray, list[str], list[str]]:
    """Binary label matrix aligned BY CODE against *class_idx*.

    Returns ``(Y, scoreable, unscoreable)``:

    - ``scoreable``: sorted species present in both the label column's
      union and *class_idx* — Y's column order.
    - ``unscoreable``: sorted species present in the labels but absent
      from *class_idx*. Returned, never silently dropped: together with
      ``scoreable`` they cover the label union exactly.
    - ``Y``: ``(n_rows, len(scoreable))`` uint8. An empty-label row is an
      all-zero row — a negative, never a dropped row.

    The mapping is by CODE: reordering a label file changes nothing here.
    A missing *label_col* raises naming the available columns.
    """
    if label_col not in table.column_names:
        raise ValueError(
            f"label column {label_col!r} not in table; available columns: "
            f"{table.column_names}"
        )
    rows: list[list[str] | None] = table.column(label_col).to_pylist()
    union: set[str] = set()
    for labels in rows:
        union.update(labels or ())
    scoreable = sorted(code for code in union if code in class_idx)
    unscoreable = sorted(code for code in union if code not in class_idx)

    col_of = {code: j for j, code in enumerate(scoreable)}
    Y = np.zeros((len(rows), len(scoreable)), dtype=np.uint8)
    for i, labels in enumerate(rows):
        for code in labels or ():
            j = col_of.get(code)
            if j is not None:
                Y[i, j] = 1
    return Y, scoreable, unscoreable
