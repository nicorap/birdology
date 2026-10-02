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

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

#: Metadata key the perch encoder writes beside `schema_variant` (see
#: phase_a/embed.py); its PRESENCE with value b"true" is the raw signal.
RAW_LOGITS_KEY = b"perch_logits_are_raw_uncalibrated"

#: Metadata key naming which columns a shard carries (see phase_a/embed.py).
SCHEMA_VARIANT_KEY = b"schema_variant"

#: Fraction of non-finite embedding rows ABOVE which `load_cache` refuses
#: the whole cache instead of dropping the rows.
#:
#: Rationale: the measured fleet fact is 5 all-NaN rows in 488,603
#: (XCL_perch — the Perch encoder emitted NaN for a handful of degenerate
#: recordings, ~0.001%). That is encoder noise on individual recordings:
#: dropping those rows is honest (no arm can score a NaN embedding) and
#: moves no metric measurably. But MORE than 1% non-finite is a different
#: phenomenon entirely — truncated shards, a wrong dtype, a broken fleet
#: run — and silently dropping >1% of rows would shift every metric
#: denominator while looking like a successful load. The two regimes get
#: the two different responses: handfuls are dropped with one counted,
#: named warning; a broken cache is refused naming the fraction.
MAX_NONFINITE_EMBEDDING_FRACTION = 0.01

#: Cap on the filepaths NAMED in the drop warning. There are only ever a
#: handful of bad rows (see above); if the list somehow grows, the warning
#: still counts all of them but names at most this many instead of dumping
#: an unbounded list into the fleet log.
_MAX_NAMED_DROPPED = 20


@dataclass(frozen=True)
class CacheView:
    """A loaded cache: concatenated shard rows plus the facts a scorer
    must not guess."""

    table: pa.Table  # concatenated shards, original columns
    logits_are_raw: bool  # True iff the perch raw flag is present=="true"
    n_classes: int  # width of the logits column (0 if absent)


def _drop_nonfinite_embedding_rows(
    table: pa.Table, cache_dir: Path
) -> pa.Table:
    """Drop rows whose embedding contains ANY non-finite value.

    MEASURED FLEET FACT: the XCL_perch cache (488,603 rows) carries
    exactly 5 all-NaN embedding rows — the Perch encoder emitted NaN for
    a handful of degenerate recordings. Such a row cannot be scored by
    ANY arm (the NaN rides the MLP's X matrix straight into sklearn's
    'Input X contains NaN', killing the whole cell), and it cannot be
    repaired (there is no honest fill value for a missing embedding —
    zero-filling would score a fabricated point). Dropping is the honest
    treatment on BOTH paths: a dropped train row simply leaves the fit;
    a dropped eval row shrinks the metric denominator to the rows that
    actually have embeddings.

    The drop is logged as ONE counted warning naming the dropped
    filepaths (capped at ``_MAX_NAMED_DROPPED``). When MORE than
    ``MAX_NONFINITE_EMBEDDING_FRACTION`` of rows are non-finite this
    raises instead, naming the fraction — that is no longer a handful of
    bad recordings, it is a broken cache (see the constant's docstring).

    A clean table is returned UNCHANGED (same object): healthy caches
    see zero behavioral difference.
    """
    column = table.column("embedding").combine_chunks()
    offsets = np.asarray(column.offsets, dtype=np.int64)
    # zero_copy_only=False maps any null child value to NaN for float
    # types, so a null inside an embedding is caught as non-finite too.
    values = column.values.to_numpy(zero_copy_only=False)
    bad_vals = (~np.isfinite(values)).astype(np.int64)
    csum = np.concatenate(([0], np.cumsum(bad_vals)))
    bad_rows = (csum[offsets[1:]] - csum[offsets[:-1]]) > 0
    if column.null_count:
        # A null embedding CELL (whole list missing) is equally
        # unscoreable; its offsets span zero values, so catch it here.
        bad_rows |= np.asarray(column.is_null())
    if not bad_rows.any():
        return table

    n_bad = int(bad_rows.sum())
    n_total = table.num_rows
    fraction = n_bad / n_total
    bad_files = [
        table.column("filepath")[int(i)].as_py()
        for i in np.nonzero(bad_rows)[0][: _MAX_NAMED_DROPPED + 1]
    ]
    if fraction > MAX_NONFINITE_EMBEDDING_FRACTION:
        raise ValueError(
            f"cache dir {cache_dir}: {n_bad} of {n_total} rows "
            f"({fraction:.2%}) have non-finite embedding values, above "
            f"the {MAX_NONFINITE_EMBEDDING_FRACTION:.2%} threshold -- "
            "this is no longer a handful of degenerate recordings but a "
            "broken cache (truncated shards, wrong dtype, a bad fleet "
            f"run); first offenders: {bad_files[:5]}. Refusing: silently "
            "dropping this many rows would shift every metric "
            "denominator while looking like a successful load"
        )
    named = bad_files[:_MAX_NAMED_DROPPED]
    overflow = (
        f" and {n_bad - _MAX_NAMED_DROPPED} more"
        if n_bad > _MAX_NAMED_DROPPED
        else ""
    )
    logger.warning(
        "cache %s: dropping %d of %d rows whose embedding contains "
        "non-finite values (NaN/inf) -- the encoder emitted them for "
        "degenerate recordings and NO arm can score them; train-side "
        "they leave the fit, eval-side they leave the metric "
        "denominator (never zero-filled): %s%s",
        cache_dir,
        n_bad,
        n_total,
        named,
        overflow,
    )
    return table.filter(pa.array(~bad_rows))


def load_cache(cache_dir: Path) -> CacheView:
    """Load every parquet shard under *cache_dir* into one table.

    Raises ValueError when the dir holds no parquet files (a wrong path or
    an unfinished fleet, never a valid empty cache) and when shards
    disagree — on the raw-logits flag, on `schema_variant`, or on the
    logits width (a heterogeneous cache is the cache-poisoning class —
    refuse, don't concatenate). A present raw flag with any value other
    than b"true" is corruption or a foreign writer and also raises.

    Rows whose embedding contains non-finite values are DROPPED with one
    counted warning naming the filepaths; more than
    ``MAX_NONFINITE_EMBEDDING_FRACTION`` of such rows raises instead
    (see :func:`_drop_nonfinite_embedding_rows`).
    """
    cache_dir = Path(cache_dir)
    paths = sorted(cache_dir.glob("*.parquet"))
    if not paths:
        raise ValueError(f"no parquet shards found in cache dir {cache_dir}")

    tables: list[pa.Table] = []
    flags: set[bool] = set()
    variants: set[bytes | None] = set()
    widths: set[int] = set()
    for path in paths:
        table = pq.read_table(path)
        metadata = table.schema.metadata or {}
        raw_value = metadata.get(RAW_LOGITS_KEY)
        if raw_value is not None and raw_value != b"true":
            raise ValueError(
                f"shard {path} carries unrecognized "
                f"{RAW_LOGITS_KEY.decode()} value {raw_value!r} -- the "
                "phase_a writer only ever emits b'true'; this shard is "
                "corrupt or from a foreign writer"
            )
        flags.add(raw_value == b"true")
        variants.add(metadata.get(SCHEMA_VARIANT_KEY))
        if "logits" in table.column_names and table.num_rows > 0:
            first = table.column("logits")[0].as_py()
            if first is not None:
                widths.add(len(first))
        tables.append(table)

    if len(flags) > 1:
        raise ValueError(
            f"cache dir {cache_dir} mixes shards with and without "
            f"{RAW_LOGITS_KEY.decode()}=true -- a heterogeneous cache "
            "puts raw and calibrated logits on one class axis; refusing "
            "to concatenate"
        )
    if len(variants) > 1:
        raise ValueError(
            f"cache dir {cache_dir} mixes schema_variant values "
            f"{sorted(v.decode() if v else 'absent' for v in variants)} -- "
            "concatenating would null-fill logits for embedding-only rows; "
            "refusing to concatenate"
        )
    if len(widths) > 1:
        raise ValueError(
            f"cache dir {cache_dir} mixes logits widths {sorted(widths)} "
            "-- two class-axis widths means two different model heads; "
            "refusing to concatenate"
        )

    combined = pa.concat_tables(tables, promote_options="default")
    if "embedding" in combined.column_names and combined.num_rows > 0:
        combined = _drop_nonfinite_embedding_rows(combined, cache_dir)
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
