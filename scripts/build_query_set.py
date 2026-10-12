#!/usr/bin/env python3
"""Build the prior's QUERY SET from the Phase A manifests.

    python scripts/build_query_set.py \\
        --manifests /srv/home/rapnic/birdology/data/phase_a/XCL_r4_manifest.parquet \\
                    /srv/home/rapnic/birdology/data/phase_a/XCM_r4_manifest.parquet \\
                    /srv/home/rapnic/birdology/data/phase_a/{HSN,NES,PER,SSW,SNE,UHH}_test_5s_r4_manifest.parquet \\
        --out output/priors/query_set.parquet

Writes ``query_set.parquet`` (distinct, sorted ``lat, lon, week`` rows) and
``query_set.provenance.json`` (every source path with its row counts, the
distinct-row count and the sha256 of the parquet). The prior build pins that
sha256 into every restricted artifact's sidecar via ``--query-set``.

RESOLUTION-FREE: the manifests' own ``cell`` column is IGNORED on purpose. It
is fixed at the manifests' resolution (r4); the prior sweeps r3/r4/r5 and
each resolution derives its own cells with the same h3 call as
``indexing.assign_cells``. See ``birdology.prior.query_set``.
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from birdology.prior.query_set import build_query_set, write_query_set

log = logging.getLogger("birdology.prior.query_set")


def main(argv: Sequence[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifests", nargs="+", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    table, provenance = build_query_set(args.manifests)
    for source in provenance["sources"]:
        log.info(
            "%s: %d row(s), %d with a (lat, lon, week) context",
            source["path"],
            source["rows"],
            source["rows_with_context"],
        )
    out, prov = write_query_set(table, provenance, args.out)
    log.info("query set: %d distinct (lat, lon, week) row(s) -> %s (%s)",
             table.num_rows, out, prov)
    print(out)


if __name__ == "__main__":
    main()
