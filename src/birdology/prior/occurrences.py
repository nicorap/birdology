"""Reduce a GBIF SIMPLE_CSV download to the columns a prior needs."""
from __future__ import annotations

import csv
import io
import zipfile
from datetime import date
from pathlib import Path

import pyarrow as pa

OCCURRENCE_COLUMNS = (
    "species_key",
    "scientific_name",
    "lat",
    "lon",
    "event_date",
    "publisher_key",
    "recorded_by",
)

_SCHEMA = pa.schema(
    [
        ("species_key", pa.int64()),
        ("scientific_name", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("event_date", pa.date32()),
        ("publisher_key", pa.string()),
        ("recorded_by", pa.string()),
    ]
)


def _parse_date(raw: str) -> date | None:
    """GBIF eventDate is ISO, sometimes with a time part or a range."""
    if not raw:
        return None
    head = raw.split("/", 1)[0].split("T", 1)[0]
    try:
        return date.fromisoformat(head)
    except ValueError:
        return None


def _occurrence_member(zf: zipfile.ZipFile) -> str:
    for name in zf.namelist():
        if name.endswith("occurrence.txt") or name.endswith(".csv"):
            return name
    raise ValueError("no occurrence table found in archive")


def read_occurrences(zip_path: Path) -> pa.Table:
    """Extract the prior-relevant columns, dropping unusable rows."""
    cols: dict[str, list] = {name: [] for name in OCCURRENCE_COLUMNS}

    with zipfile.ZipFile(zip_path) as zf:
        member = _occurrence_member(zf)
        with zf.open(member) as raw:
            reader = csv.DictReader(
                io.TextIOWrapper(raw, encoding="utf-8"),
                delimiter="\t",
                quoting=csv.QUOTE_NONE,
            )
            for row in reader:
                species_raw = (row.get("speciesKey") or "").strip()
                lat_raw = (row.get("decimalLatitude") or "").strip()
                lon_raw = (row.get("decimalLongitude") or "").strip()
                event = _parse_date((row.get("eventDate") or "").strip())
                if not species_raw or not lat_raw or not lon_raw or event is None:
                    continue
                try:
                    species_key = int(species_raw)
                    lat = float(lat_raw)
                    lon = float(lon_raw)
                except ValueError:
                    continue
                cols["species_key"].append(species_key)
                cols["scientific_name"].append((row.get("species") or "").strip())
                cols["lat"].append(lat)
                cols["lon"].append(lon)
                cols["event_date"].append(event)
                cols["publisher_key"].append(
                    (row.get("publishingOrgKey") or "").strip()
                )
                cols["recorded_by"].append((row.get("recordedBy") or "").strip())

    return pa.table([cols[name] for name in OCCURRENCE_COLUMNS], schema=_SCHEMA)
