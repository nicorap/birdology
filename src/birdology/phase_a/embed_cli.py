#!/usr/bin/env python3
"""Embed ONE shard. The entry point a Slurm array task runs in the container.

    python3 -m birdology.phase_a.embed_cli \
        --shard /faic/weka/ML/birdset/XCM/XCM/XCM_shard_0001.tar.gz \
        --manifest /faic/weka/ML/phase_a/XCM_r4_manifest.parquet \
        --out /faic/weka/ML/phase_a/embeddings/XCM

Lives in the package, not in `scripts/`, because the container has the repo on
its `PYTHONPATH` but no notion of the script directory.

Exit codes:
  0  the shard was embedded (or legitimately had nothing expected in it)
  1  every expected file failed -- a failure, not a completed task, because
     it would otherwise leave a valid empty parquet that `shard_is_complete`
     accepts on a re-run
  2  REFUSED: the output already exists and is complete and --force is absent.
     One array task owns one shard exclusively; hitting a complete shard again
     means either a benign re-submission (harmless, the refusal is loud but
     nothing is lost) or a re-submission with a shifted index->shard mapping
     (see phase_a_embed.sbatch), which must not silently merge generations.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .embed import _audio_member_names, embed_shard, shard_is_complete
from .encoders import load_encoder, load_encoder_with_logits

log = logging.getLogger("phase_a.embed")


def slice_manifest_to_shard(manifest: pa.Table, members: set[str]) -> pa.Table:
    """The manifest rows whose filepath matches a member of THIS shard.

    `embed_shard` and `shard_is_complete` iterate their manifest row-by-row
    via `to_pylist()`. XCL's manifest is ~500k rows; filtering columnar-side
    first (a pyarrow compute `is_in` — cheap) keeps the per-shard Python cost
    at the shard's ~5k rows instead of the whole corpus.

    Matches by exact path OR basename: XCM shards 0001-0126 store members
    under full manifest-equal paths, shards 0127/0158-0182 store bare
    basenames (measured on WEKA 2026-09-30 — BirdSet's packaging is
    inconsistent across shards of the same config). The slice must be a
    SUPERSET of what `_resolve_members` will match, never narrower, or the
    resolver downstream sees an artificially empty manifest.
    """
    member_basenames = {m.rsplit("/", 1)[-1] for m in members}
    basenames = pc.replace_substring_regex(
        manifest.column("filepath"), pattern=r"^.*/", replacement=""
    )
    keep = pc.or_(
        pc.is_in(
            manifest.column("filepath"),
            value_set=pa.array(sorted(members), pa.string()),
        ),
        pc.is_in(
            basenames,
            value_set=pa.array(sorted(member_basenames), pa.string()),
        ),
    )
    return manifest.filter(keep)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shard", required=True, type=Path)
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--encoder", default="perch")
    ap.add_argument(
        "--with-logits",
        action="store_true",
        help="also cache the per-recording species posterior (logits column); "
        "cheap to write now (~31 GB for XCL), absurd to recompute later "
        "(a second 581 GB pass). melsummary has no classifier head and "
        "writes the default schema even under this flag.",
    )
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    # Manifest FIRST: completion is manifest-aware (done ⊇ expected on
    # filepaths), so there is no completeness question to ask before the
    # manifest is loaded and sliced to this shard.
    manifest = pq.read_table(args.manifest)
    members = set(_audio_member_names(args.shard))
    shard_manifest = slice_manifest_to_shard(manifest, members)

    # Re-assert embed_shard's zero-overlap guard HERE: slicing first means
    # embed_shard would only ever see an already-empty manifest and its own
    # guard could never fire, blessing a systematic key mismatch (a `data/`
    # prefix change, a root move) as an empty no-op shard, fleet-wide, exit 0.
    if manifest.num_rows and members and shard_manifest.num_rows == 0:
        raise ValueError(
            f"shard {args.shard.name}: ZERO overlap between manifest filepaths "
            f"and tar member names -- systematic key mismatch, refusing to "
            f"bless an empty shard. sample members: {sorted(members)[:3]}; "
            f"sample manifest paths: "
            f"{manifest.column('filepath').to_pylist()[:3]}"
        )

    if not args.force and shard_is_complete(args.shard, shard_manifest, args.out):
        log.error(
            "%s: output under %s already complete; refusing to redo it "
            "without --force",
            args.shard.name,
            args.out,
        )
        return 2

    if args.with_logits:
        encoder = load_encoder_with_logits(args.encoder)
    else:
        encoder = load_encoder(args.encoder)
    try:
        result = embed_shard(
            args.shard,
            shard_manifest,
            encoder,
            out_dir=args.out,
            with_logits=args.with_logits,
        )
    finally:
        # The birdnet encoder holds process-lifetime sessions that own
        # child worker processes; close them deterministically after the
        # shard loop instead of leaning on atexit. melsummary/perch
        # callables have no close() — hence the hasattr guard.
        if hasattr(encoder, "close"):
            encoder.close()  # type: ignore[union-attr]

    log.info(
        "%s: embedded=%d/expected=%d failed=%d schema=%s",
        result.shard,
        result.embedded,
        result.expected,
        len(result.failed),
        result.schema_variant,
    )
    # A shard where EVERY expected file failed is a failure, not a completed
    # task: it would otherwise leave a valid empty parquet that
    # `shard_is_complete` accepts on a re-run.
    if result.embedded == 0 and result.expected > 0:
        log.error("%s: every expected file failed", result.shard)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
