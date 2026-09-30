"""Embed one shard with a frozen encoder.

Reads audio IN PLACE from the tar: extracting XCL's 99 shards would double
485 GB of disk for no benefit, and `tarfile.extractfile` plus `soundfile` on a
BytesIO is measured to work on this corpus.

One shard is one unit of work and one output file, because the fleet runs this
as a Slurm job array. Completion is an output file whose row count matches the
shard's audio-member count -- not merely its existence, since a preempted task
leaves a short file that looks finished.

The parquet is written with an EXPLICIT schema: `pa.Table.from_pylist([])`
yields a zero-column table, so an all-failed shard would otherwise emit a
schema-less parquet that Phase B cannot read.
"""
from __future__ import annotations

import io
import json
import logging
import tarfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import soundfile as sf

_AUDIO_SUFFIXES = (".ogg", ".flac", ".wav", ".mp3")

#: The output contract with Phase B: every shard parquet carries exactly these
#: columns with exactly these dtypes, even when zero rows were embedded.
EMBEDDING_SCHEMA = pa.schema(
    [
        pa.field("filepath", pa.string()),
        pa.field("cell", pa.string()),
        pa.field("week", pa.int8()),
        pa.field("ebird_code", pa.string()),
        pa.field("split", pa.string()),
        pa.field("embedding", pa.list_(pa.float32())),
    ]
)

log = logging.getLogger(__name__)


@dataclass
class ShardResult:
    shard: str
    embedded: int
    failed: dict[str, str] = field(default_factory=dict)


def iter_shard_audio(shard: Path) -> Iterator[tuple[str, bytes]]:
    """`(member_name, raw_bytes)` for every audio member, without extracting."""
    with tarfile.open(shard, "r:*") as tf:
        for member in tf:
            if not member.isfile():
                continue
            if not member.name.lower().endswith(_AUDIO_SUFFIXES):
                continue
            handle = tf.extractfile(member)
            if handle is None:
                continue
            yield member.name, handle.read()


def _audio_member_count(shard: Path) -> int:
    with tarfile.open(shard, "r:*") as tf:
        return sum(
            1
            for m in tf
            if m.isfile() and m.name.lower().endswith(_AUDIO_SUFFIXES)
        )


def _out_paths(shard: Path, out_dir: Path) -> tuple[Path, Path]:
    stem = shard.name.split(".")[0]
    return out_dir / f"{stem}.parquet", out_dir / f"{stem}.failures.json"


def embed_shard(
    shard: Path,
    manifest: pa.Table,
    encoder: Callable[[np.ndarray, int], np.ndarray],
    *,
    out_dir: Path,
) -> ShardResult:
    """Embed every manifest-listed audio member of *shard*."""
    shard = Path(shard)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table_path, failures_path = _out_paths(shard, out_dir)

    wanted = {
        row["filepath"]: row for row in manifest.to_pylist()
    }

    rows: list[dict] = []
    failed: dict[str, str] = {}

    for name, payload in iter_shard_audio(shard):
        context = wanted.get(name)
        if context is None:
            continue
        try:
            samples, sr = sf.read(io.BytesIO(payload), dtype="float32", always_2d=False)
            if getattr(samples, "ndim", 1) > 1:
                samples = samples.mean(axis=1)
            vector = np.asarray(encoder(samples, sr), dtype=np.float32)
        except Exception as exc:  # one bad file must not lose the shard
            failed[name] = f"{type(exc).__name__}: {exc}"
            log.warning("shard %s: %s failed: %s", shard.name, name, exc)
            continue
        rows.append(
            {
                "filepath": name,
                "cell": context["cell"],
                "week": context["week"],
                "ebird_code": context["ebird_code"],
                "split": context["split"],
                "embedding": vector.tolist(),
            }
        )

    # An explicit schema: from_pylist([]) is a ZERO-COLUMN table, and a
    # schema-less parquet for an all-failed shard would break Phase B.
    pq.write_table(pa.Table.from_pylist(rows, schema=EMBEDDING_SCHEMA), table_path)
    # Written unconditionally: an absent failures file and an empty one are
    # different statements, and a consumer must be able to tell.
    failures_path.write_text(json.dumps(failed, indent=2), encoding="utf-8")

    return ShardResult(shard=shard.name, embedded=len(rows), failed=failed)


def shard_is_complete(shard: Path, out_dir: Path) -> bool:
    """True when this shard's output exists AND accounts for every member.

    Existence alone is not completion: a preempted array task leaves a short
    parquet that a re-run would otherwise skip.
    """
    table_path, failures_path = _out_paths(Path(shard), Path(out_dir))
    if not table_path.exists():
        return False
    try:
        embedded = pq.read_metadata(table_path).num_rows
    except Exception:
        return False
    failures = 0
    if failures_path.exists():
        try:
            failures = len(json.loads(failures_path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            return False
    return embedded + failures >= _audio_member_count(Path(shard))
