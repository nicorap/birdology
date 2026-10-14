#!/usr/bin/env python3
"""Build DKX: a held-out Danish Xeno-canto test set, packaged like a BirdSet config.

    python scripts/build_dkx_testset.py \
        --key-file /srv/home/rapnic/birdology/apps/secrets/xc_key \
        --data /srv/home/rapnic/birdology/data --uploaded-since 2025-01-01

Why: every BirdSet test set is in the Americas, so the dense Danish (DOF) prior
was never evaluated where it applies. DKX is Danish bird recordings from
Xeno-canto, uploaded after --uploaded-since so the frozen encoders (BirdNET 2.4,
Perch 2.0; both trained on Xeno-canto) are unlikely to have seen them, minus
every recording already in BirdSet's XCL (the Phase B train cache).

Output, in the layout Phase A and Phase B already read (no code changes):
    data/birdset/DKX/DKX/DKX_metadata_focal.parquet   filepath, id, lat, long,
        ebird_code, ebird_code_multilabel, genus, date, recordist, quality, uploaded
    data/birdset/DKX/DKX/DKX_focal_shard_NNNN.tar.gz  32 kHz mono FLAC, member = filepath
    data/phase_a/DKX_focal_r4_manifest.parquet        filepath, cell, week, ...
    data/birdset/DKX/DKX/DKX_provenance.json          query, cut-off, every drop counted

The split is "focal", deliberately not "test": phase_a_manifest treats any
"_test" metadata as a soundscape and would try to date it from filenames.
The unit is one recording (Phase A pools windows), so cmAP is not comparable
with BirdSet's 5 s segment numbers.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import tarfile
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

log = logging.getLogger("dkx")

API = "https://xeno-canto.org/api/3/recordings"
SAMPLE_RATE = 32_000          # Perch refuses anything else (phase_a/encoders.py)
SHARD_SIZE = 100
QUALITIES = ("A", "B", "C")   # D/E are poor or marked unusable by the recordist


def parse_date(raw: str | None) -> date | None:
    """XC dates are YYYY-MM-DD with 00 for unknown parts; those are undated."""
    try:
        y, m, d = (int(p) for p in (raw or "").split("-"))
        return date(y, m, d)
    except (ValueError, TypeError):
        return None


#: Xeno-canto (IOC) names that eBird/Clements files under another genus. Found by
#: listing every unmapped Danish name on 2026-10-13; the rest were "Mystery mystery".
XC_TO_EBIRD_SYNONYMS = {
    "charadrius dubius": "thinornis dubius",          # Little Ringed Plover
    "charadrius morinellus": "eudromias morinellus",  # Eurasian Dotterel
    "ixobrychus minutus": "botaurus minutus",         # Little Bittern
}


def sci_to_code_map(code_to_sci: dict[str, str]) -> dict[str, str]:
    """Lower-cased scientific name -> eBird code; names shared by >1 code are dropped.

    Xeno-canto synonyms resolve through XC_TO_EBIRD_SYNONYMS when their eBird
    name is in the map.
    """
    seen: dict[str, str | None] = {}
    for code, sci in code_to_sci.items():
        key = sci.strip().lower()
        seen[key] = code if key not in seen else None
    out = {k: v for k, v in seen.items() if v is not None}
    for xc_name, ebird_name in XC_TO_EBIRD_SYNONYMS.items():
        if ebird_name in out and xc_name not in out:
            out[xc_name] = out[ebird_name]
    return out


def xcl_ids(filepaths: list[str]) -> set[int]:
    """XC ids from BirdSet filepaths such as 'XC208209.ogg'."""
    out = set()
    for fp in filepaths:
        stem = Path(fp).stem
        if stem.startswith("XC") and stem[2:].isdigit():
            out.add(int(stem[2:]))
    return out


def select(recs: list[dict], *, exclude_ids: set[int], sci2code: dict[str, str],
           qualities: tuple[str, ...] = QUALITIES) -> tuple[list[dict], Counter]:
    """Keep usable recordings and attach labels. Every drop is counted by reason."""
    kept, drops = [], Counter()
    for r in recs:
        xc = int(r["id"])
        if r.get("q") not in qualities:
            drops["quality"] += 1; continue
        if xc in exclude_ids:
            drops["in_xcl"] += 1; continue
        try:
            lat, lon = float(r["lat"]), float(r["lon"])
        except (KeyError, TypeError, ValueError):
            drops["no_coordinates"] += 1; continue
        when = parse_date(r.get("date"))
        if when is None:
            drops["no_date"] += 1; continue
        primary = sci2code.get(f"{r['gen']} {r['sp']}".strip().lower())
        if primary is None:
            drops["species_not_in_ebird_map"] += 1; continue
        labels = [primary]
        for name in r.get("also") or []:
            code = sci2code.get(str(name).strip().lower())
            if code is None:
                drops["background_label_unmapped"] += 1   # label lost, recording kept
            elif code not in labels:
                labels.append(code)
        kept.append({
            "filepath": f"XC{xc}.flac", "id": xc, "lat": lat, "long": lon,
            "ebird_code": primary, "ebird_code_multilabel": labels,
            "genus": r["gen"], "date": when.isoformat(), "recordist": r.get("rec", ""),
            "quality": r["q"], "uploaded": r.get("uploaded", ""), "url": r["file"],
        })
    return kept, drops


def fetch_all(query: str, key: str) -> list[dict]:
    recs, page = [], 1
    while True:
        url = f"{API}?" + urllib.parse.urlencode({"query": query, "key": key, "per_page": 500, "page": page})
        d = json.load(urllib.request.urlopen(url, timeout=120))
        recs += d["recordings"]
        if page >= int(d["numPages"]):
            return recs
        page += 1


def to_flac_32k(raw: bytes) -> bytes:
    import numpy as np
    import soundfile as sf
    import soxr
    audio, sr = sf.read(io.BytesIO(raw), dtype="float32", always_2d=True)
    if audio.shape[0] < sr:   # XC984018 decoded to nothing and was packaged as a 0-byte file
        raise ValueError(f"decoded audio is {audio.shape[0] / sr:.2f} s; refusing anything under 1 s")
    mono = audio.mean(axis=1)
    if sr != SAMPLE_RATE:
        mono = soxr.resample(mono, sr, SAMPLE_RATE)
    buf = io.BytesIO()
    sf.write(buf, np.clip(mono, -1, 1), SAMPLE_RATE, format="FLAC")
    return buf.getvalue()


def download(url: str, tries: int = 4) -> bytes:
    for i in range(tries):
        try:
            return urllib.request.urlopen(url, timeout=300).read()
        except Exception:  # noqa: BLE001 -- network: retry, then let the caller count it
            if i == tries - 1:
                raise
            time.sleep(5 * (i + 1))


def main(argv: list[str] | None = None) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from birdology.phase_a.manifest import build_manifest

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--key-file", type=Path, required=True)
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--uploaded-since", required=True, help="YYYY-MM-DD")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    out = args.data / "birdset" / "DKX" / "DKX"
    meta_path = out / "DKX_metadata_focal.parquet"
    if meta_path.exists():
        ap.error(f"{meta_path} exists; DKX is built once -- move it aside to rebuild")
    out.mkdir(parents=True, exist_ok=True)

    key = args.key_file.read_text().strip()
    query = f"cnt:denmark grp:birds since:{args.uploaded_since}"
    recs = fetch_all(query, key)
    log.info("%d recordings for %r", len(recs), query)

    code_to_sci = json.loads((args.data / "phase_a" / "ebird_code_to_sci.json").read_text())
    xcl = pq.read_table(args.data / "birdset/XCL/XCL/XCL_metadata.parquet", columns=["filepath"])
    kept, drops = select(recs, exclude_ids=xcl_ids(xcl.column("filepath").to_pylist()),
                         sci2code=sci_to_code_map(code_to_sci))
    log.info("selected %d, drops %s", len(kept), dict(drops))

    rows, shard, n_shard = [], None, 0
    for i, r in enumerate(kept):
        try:
            flac = to_flac_32k(download(r.pop("url")))
        except Exception as e:  # noqa: BLE001 -- one bad file must not sink the set
            drops["download_or_decode_failed"] += 1
            log.warning("XC%s: %s", r["id"], e)
            continue
        if shard is None or len(rows) % SHARD_SIZE == 0:
            if shard is not None:
                shard.close()
            n_shard += 1
            shard = tarfile.open(out / f"DKX_focal_shard_{n_shard:04d}.tar.gz", "w:gz")
        info = tarfile.TarInfo(r["filepath"]); info.size = len(flac)
        shard.addfile(info, io.BytesIO(flac))
        rows.append(r)
        if (i + 1) % 50 == 0:
            log.info("%d/%d packaged", i + 1, len(kept))
    if shard is not None:
        shard.close()

    meta = pa.Table.from_pylist(rows)
    pq.write_table(meta, meta_path)
    dates = {r["id"]: date.fromisoformat(r["date"]) for r in rows}
    manifest, mdrops = build_manifest(meta, dates=dates, resolution=4, split="focal")
    pq.write_table(manifest, args.data / "phase_a" / "DKX_focal_r4_manifest.parquet")

    prov = {"query": query, "uploaded_since": args.uploaded_since, "api_returned": len(recs),
            "kept": len(rows), "species": len({r["ebird_code"] for r in rows}),
            "recordists": len({r["recordist"] for r in rows}), "shards": n_shard,
            "drops": dict(drops), "manifest_rows": manifest.num_rows,
            "manifest_drops": {str(k): v for k, v in mdrops.items()},
            "built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (out / "DKX_provenance.json").write_text(json.dumps(prov, indent=2))
    print(json.dumps(prov, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
