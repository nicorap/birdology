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

#: EMBEDDING_SCHEMA plus the per-recording species posterior. Written only
#: when `with_logits=True` AND the encoder actually produced logits;
#: melsummary (no classifier head) falls back to the default schema. The
#: variant is recorded in table metadata under 'schema_variant' so Phase B
#: never has to infer it from column presence.
LOGITS_SCHEMA = pa.schema(
    list(EMBEDDING_SCHEMA) + [pa.field("logits", pa.list_(pa.float32()))]
)

#: Table-metadata key naming which schema a shard parquet carries.
SCHEMA_VARIANT_KEY = b"schema_variant"

log = logging.getLogger(__name__)


@dataclass
class ShardResult:
    shard: str
    embedded: int
    expected: int = 0
    failed: dict[str, str] = field(default_factory=dict)
    schema_variant: str = "embedding"


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


def _resolve_members(
    member_names: list[str], manifest_paths: set[str]
) -> dict[str, str]:
    """Map tar member name -> canonical MANIFEST filepath.

    Measured on WEKA (2026-09-30): XCM shards 0001-0126 store members under
    full paths (`data/xeno-canto/.../XC654011.ogg`) that equal the manifest's
    `filepath` column, but shards 0127 and 0158-0182 store BARE basenames
    (`XC111167.ogg`) -- BirdSet's own packaging is inconsistent across shards.
    Exact match is tried first; a basename fallback covers the bare shards.
    The fallback is safe for this corpus because XC ids are globally unique:
    verified 85,885 manifest rows -> 85,885 distinct basenames, 0 collisions.
    An ambiguous basename (two manifest rows sharing it) is left UNRESOLVED
    rather than guessed -- a wrong join is worse than a dropped file.

    The canonical key matters: the output parquet stores the MANIFEST path,
    never the member name, so Phase B joins one key space regardless of which
    shard convention held.
    """
    by_basename: dict[str, str | None] = {}
    for p in manifest_paths:
        b = p.rsplit("/", 1)[-1]
        # Second sighting poisons the entry: ambiguous basenames stay unmapped.
        by_basename[b] = None if b in by_basename else p

    out: dict[str, str] = {}
    for name in member_names:
        if name in manifest_paths:
            out[name] = name
            continue
        hit = by_basename.get(name.rsplit("/", 1)[-1])
        if hit is not None:
            out[name] = hit
    return out


def embed_shard(
    shard: Path,
    manifest: pa.Table,
    encoder: Callable[[np.ndarray, int], "np.ndarray | tuple[np.ndarray, np.ndarray | None]"],
    *,
    out_dir: Path,
    with_logits: bool = False,
) -> ShardResult:
    """Embed every manifest-listed audio member of *shard*.

    Raises ValueError when the manifest and the shard share ZERO filepaths
    despite both being non-empty: zero overlap is a systematic key mismatch
    (a `data/` prefix change, a root move), not a valid no-op, and blessing
    it would embed zero rows fleet-wide at exit 0.

    With `with_logits=True` the encoder must return
    `(embedding, logits_or_None)` (see `load_encoder_with_logits`) and the
    parquet gains a `logits list_(float32)` column — the per-recording
    species posterior cached NOW because re-reading 581 GB of audio later to
    recover it would cost days of fleet time while the column costs ~31 GB.
    An encoder that yields logits=None for EVERY file (melsummary — no
    classifier head) falls back to the default schema; a MIXED shard (some
    rows with logits, some without) raises, because it means the encoder is
    inconsistent and half the posterior would be silently missing. Every
    logits vector in a shard must have one length: a mismatch is a
    systematic encoder bug, not a per-file failure, and is raised, not
    recorded. The variant actually written is recorded in the parquet
    metadata under 'schema_variant' ('embedding' | 'embedding+logits') so
    Phase B reads a declaration, not an inference.
    """
    shard = Path(shard)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table_path, failures_path = _out_paths(shard, out_dir)

    wanted = {
        row["filepath"]: row for row in manifest.to_pylist()
    }

    member_names = _audio_member_names(shard)
    resolved = _resolve_members(member_names, set(wanted))
    if wanted and member_names and not resolved:
        raise ValueError(
            f"shard {shard.name}: ZERO overlap between manifest filepaths and "
            f"tar member names (exact AND basename) -- systematic key "
            f"mismatch, refusing to bless an empty shard. "
            f"sample members: {member_names[:3]}; "
            f"sample manifest paths: {sorted(wanted)[:3]}"
        )

    rows: list[dict] = []
    failed: dict[str, str] = {}
    logits_width: int | None = None  # pinned by the first row that has logits

    for name, payload in iter_shard_audio(shard):
        canonical = resolved.get(name)
        if canonical is None:
            continue
        context = wanted[canonical]
        try:
            samples, sr = sf.read(io.BytesIO(payload), dtype="float32", always_2d=False)
            if getattr(samples, "ndim", 1) > 1:
                samples = samples.mean(axis=1)
            if samples.size == 0:
                # A valid-header OGG truncated mid-stream decodes to 0 samples
                # WITHOUT raising (measured); embedding it writes a garbage
                # row that counts toward completion.
                raise ValueError("decoded to 0 samples (truncated file?)")
            logits_vec: np.ndarray | None = None
            if with_logits:
                vector, logits_vec = encoder(samples, sr)
                vector = np.asarray(vector, dtype=np.float32)
            else:
                vector = np.asarray(encoder(samples, sr), dtype=np.float32)
        except Exception as exc:  # one bad file must not lose the shard
            # Keyed by the CANONICAL manifest path: completion compares
            # done against expected in manifest-path space.
            failed[canonical] = f"{type(exc).__name__}: {exc}"
            log.warning("shard %s: %s failed: %s", shard.name, name, exc)
            continue
        row = {
            "filepath": canonical,
            "cell": context["cell"],
            "week": context["week"],
            "ebird_code": context["ebird_code"],
            "split": context["split"],
            "embedding": vector.tolist(),
        }
        if with_logits and logits_vec is not None:
            logits_vec = np.asarray(logits_vec, dtype=np.float32).reshape(-1)
            if logits_width is None:
                logits_width = logits_vec.size
            elif logits_vec.size != logits_width:
                # A wrong-length posterior is a systematic encoder bug --
                # Phase B would silently misalign every class index.
                raise ValueError(
                    f"shard {shard.name}: {canonical} produced logits of "
                    f"length {logits_vec.size}, but this shard's logits "
                    f"width is pinned to {logits_width} -- inconsistent "
                    "encoder output, refusing to write a misaligned posterior"
                )
            row["logits"] = logits_vec.tolist()
        rows.append(row)

    # The variant reflects what the ENCODER produced, not what was asked:
    # melsummary under --with-logits has no classifier head and honestly
    # writes the default schema. A MIXED shard is an inconsistent encoder
    # and must not ship half a posterior. Zero embedded rows keep the
    # REQUESTED variant so an all-failed logits shard still round-trips
    # through the schema Phase B expects for that out_dir.
    rows_with_logits = sum(1 for r in rows if "logits" in r)
    if with_logits and rows and 0 < rows_with_logits < len(rows):
        raise ValueError(
            f"shard {shard.name}: encoder returned logits for "
            f"{rows_with_logits}/{len(rows)} rows -- an inconsistent "
            "posterior is worse than none; refusing to write a mixed shard"
        )
    use_logits_schema = with_logits and (rows_with_logits > 0 or not rows)
    schema = LOGITS_SCHEMA if use_logits_schema else EMBEDDING_SCHEMA
    variant = "embedding+logits" if use_logits_schema else "embedding"

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
    # Encoder-declared facts about its own output (e.g. perch's
    # 'perch_logits_are_raw_uncalibrated: true' — hoplite does NOT apply
    # the *0.97-10 calibration, measured 2026-09-30) ride in the schema
    # metadata beside the variant, so no consumer can assume calibrated
    # values from the file alone.
    metadata: dict[bytes, bytes] = {SCHEMA_VARIANT_KEY: variant.encode()}
    declared = getattr(encoder, "parquet_metadata", None)
    if declared:
        for k, v in declared.items():
            metadata[k.encode() if isinstance(k, str) else k] = (
                v.encode() if isinstance(v, str) else v
            )
    table = pa.Table.from_pylist(rows, schema=schema).replace_schema_metadata(
        metadata
    )
    table_tmp = table_path.with_name(table_path.name + ".tmp")
    pq.write_table(table, table_tmp)
    os.replace(table_tmp, table_path)

    return ShardResult(
        shard=shard.name,
        embedded=len(rows),
        expected=len(resolved),
        failed=failed,
        schema_variant=variant,
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
    manifest_paths = set(manifest.column("filepath").to_pylist())
    resolved = _resolve_members(_audio_member_names(shard), manifest_paths)
    # Expected in CANONICAL manifest-path space, matching what embed_shard
    # writes -- member names and manifest paths may differ per shard
    # convention (full-path vs bare-basename shards, both real in XCM).
    expected = set(resolved.values())
    return done >= expected
