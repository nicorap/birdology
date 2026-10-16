#!/usr/bin/env python3
"""Download DOF's Danish checklist and write the SU species list.

    python scripts/prior_su_list.py --out src/birdology/prior/data

Needs pdftotext (poppler). Writes su_species.csv and su_species.source.json
(URL, download date, sha256 of the PDF, count).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

from birdology.prior.su_list import SOURCE_URL, parse_checklist


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)
    with tempfile.TemporaryDirectory() as tmp:
        pdf = Path(tmp) / "danish_list.pdf"
        with urllib.request.urlopen(SOURCE_URL, timeout=60) as r:
            pdf.write_bytes(r.read())
        text = subprocess.run(["pdftotext", "-layout", str(pdf), "-"], check=True,
                              capture_output=True, text=True).stdout
        sha = hashlib.sha256(pdf.read_bytes()).hexdigest()
    species = parse_checklist(text)
    if len(species) < 150:
        print(f"FAIL: only {len(species)} SU species parsed; the checklist layout changed?")
        return 1
    args.out.mkdir(parents=True, exist_ok=True)
    with open(args.out / "su_species.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["scientific_name", "danish_name", "english_name"])
        w.writerows(species)
    (args.out / "su_species.source.json").write_text(json.dumps({
        "url": SOURCE_URL, "downloaded": dt.date.today().isoformat(), "pdf_sha256": sha,
        "count": len(species), "citation": "Olsen, K. & Danish Rarities Committee 2025. "
        "The Danish List: A Checklist of the Birds of Denmark. Dansk Orn. Foren. Tidsskr. 119: 105-112.",
    }, indent=2) + "\n")
    print(f"{len(species)} SU species -> {args.out / 'su_species.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
