"""Embed one shard with a frozen encoder.

Reads audio IN PLACE from the tar: extracting XCL's 99 shards would double
485 GB of disk for no benefit, and `tarfile.extractfile` plus `soundfile` on a
BytesIO is measured to work on this corpus.

One shard is one unit of work and one output file, because the fleet runs this
as a Slurm job array. Completion means: every MANIFEST-LISTED member present
in the shard is accounted for in the outputs (embedded row or recorded
failure). It is deliberately NOT "every audio member of the tar": Task 3's
manifest drops ~2% of recordings by design (no date, no coordinates), so a
member-count contract would report every real shard permanently incomplete
and Task 5 would retry the fleet forever.

Completion is set-based -- `done ⊇ expected` on filepaths -- not count
arithmetic: a crash between the two output writes can leave mixed-generation
files where a name is double-counted (in the parquet AND the stale failures
json) while another is lost, and `2 + 1 >= 3` blesses exactly that state.
Both outputs are written atomically (tmp + os.replace), failures json FIRST:
after any crash the parquet -- the completion witness -- is the file missing,
so the shard correctly re-runs.

The parquet is written with an EXPLICIT schema: `pa.Table.from_pylist([])`
yields a zero-column table, so an all-failed shard would otherwise emit a
schema-less parquet that Phase B cannot read.
"""
from __future__ import annotations

import io
import json
import logging
import os
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
    expected: int = 0
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


def _audio_member_names(shard: Path) -> list[str]:
    with tarfile.open(shard, "r:*") as tf:
        return [
            m.name
            for m in tf
            if m.isfile() and m.name.lower().endswith(_AUDIO_SUFFIXES)
        ]


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
    """Embed every manifest-listed audio member of *shard*.

    Raises ValueError when the manifest and the shard share ZERO filepaths
    despite both being non-empty: zero overlap is a systematic key mismatch
    (a `data/` prefix change, a root move), not a valid no-op, and blessing
    it would embed zero rows fleet-wide at exit 0.
    """
    shard = Path(shard)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table_path, failures_path = _out_paths(shard, out_dir)

    wanted = {
        row["filepath"]: row for row in manifest.to_pylist()
    }

    member_names = _audio_member_names(shard)
    expected = set(wanted) & set(member_names)
    if wanted and member_names and not expected:
        raise ValueError(
            f"shard {shard.name}: ZERO overlap between manifest filepaths and "
            f"tar member names -- systematic key mismatch, refusing to bless "
            f"an empty shard. sample members: {member_names[:3]}; "
            f"sample manifest paths: {sorted(wanted)[:3]}"
        )

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
            if samples.size == 0:
                # A valid-header OGG truncated mid-stream decodes to 0 samples
                # WITHOUT raising (measured); embedding it writes a garbage
                # row that counts toward completion.
                raise ValueError("decoded to 0 samples (truncated file?)")
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

    # Both outputs are atomic (tmp + os.replace) so a preempted task can never
    # leave a torn file a reader half-trusts. Failures json goes FIRST: after
    # a crash between the two, the parquet (the completion witness) is the one
    # missing and the shard re-runs; the reverse order can leave
    # parquet-present + failures-stale, which marks an incomplete shard done.
    #
    # Written unconditionally: an absent failures file and an empty one are
    # different statements, and a consumer must be able to tell.
    failures_tmp = failures_path.with_name(failures_path.name + ".tmp")
    failures_tmp.write_text(json.dumps(failed, indent=2), encoding="utf-8")
    os.replace(failures_tmp, failures_path)

    # An explicit schema: from_pylist([]) is a ZERO-COLUMN table, and a
    # schema-less parquet for an all-failed shard would break Phase B.
    table_tmp = table_path.with_name(table_path.name + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=EMBEDDING_SCHEMA), table_tmp)
    os.replace(table_tmp, table_path)

    return ShardResult(
        shard=shard.name,
        embedded=len(rows),
        expected=len(expected),
        failed=failed,
    )


def shard_is_complete(shard: Path, manifest: pa.Table, out_dir: Path) -> bool:
    """True when every manifest-listed member of *shard* is accounted for.

    expected = {manifest filepaths} ∩ {shard audio member names};
    done     = {filepaths in the output parquet} ∪ {keys of the failures json}.
    Complete iff done ⊇ expected and both sidecars are readable.

    Set-based on purpose: count arithmetic over mixed-generation output files
    double-counts a name present in both the parquet and a stale failures
    json while losing another (2 + 1 >= 3 also holds). And measured against
    the MANIFEST, not the tar: Task 3's manifest excludes ~2% of recordings
    by design, so demanding every tar member would make every real shard
    permanently incomplete.
    """
    shard = Path(shard)
    table_path, failures_path = _out_paths(shard, Path(out_dir))
    if not table_path.exists():
        return False
    try:
        done = set(
            pq.read_table(table_path, columns=["filepath"])
            .column("filepath")
            .to_pylist()
        )
    except Exception:
        # A torn parquet -- exactly what preemption mid-write produces -- is
        # not a completion witness.
        return False
    if failures_path.exists():
        try:
            done |= set(json.loads(failures_path.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, TypeError):
            # An unreadable failures sidecar means the outputs cannot be
            # trusted as a pair; re-run the shard.
            return False
    expected = set(manifest.column("filepath").to_pylist()) & set(
        _audio_member_names(shard)
    )
    return done >= expected
