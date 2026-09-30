"""Reduce a GBIF SIMPLE_CSV download to the columns a prior needs."""
from __future__ import annotations

import csv
import io
import logging
import math
import zipfile
from datetime import date
from pathlib import Path
from typing import IO

import pyarrow as pa

logger = logging.getLogger(__name__)

OCCURRENCE_COLUMNS = (
    "species_key",
    "scientific_name",
    "lat",
    "lon",
    "event_date",
    "publisher_key",
    "recorded_by",
)

#: Source columns an occurrence table must declare to be usable at all.
REQUIRED_SOURCE_COLUMNS = frozenset(
    {"speciesKey", "decimalLatitude", "decimalLongitude", "eventDate"}
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
    """GBIF eventDate is ISO, sometimes with a time part or a range.

    A range collapses to its START date; a timestamp drops its time part.
    Coarser-than-day precision (``2025-11``, ``2025``) is NOT parseable and
    yields ``None``, so such rows are dropped.
    """
    if not raw:
        return None
    head = raw.split("/", 1)[0].split("T", 1)[0]
    try:
        return date.fromisoformat(head)
    except ValueError:
        return None


def _text(raw: IO[bytes]) -> io.TextIOWrapper:
    """Wrap a zip member for ``csv``: never fail on a stray non-UTF-8 byte."""
    return io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")


def _reader(stream: io.TextIOWrapper) -> csv.DictReader:
    return csv.DictReader(stream, delimiter="\t", quoting=csv.QUOTE_NONE)


def _missing_columns(fieldnames) -> list[str]:
    return sorted(REQUIRED_SOURCE_COLUMNS - set(fieldnames or ()))


def _header_validates(zf: zipfile.ZipFile, name: str) -> bool:
    try:
        with zf.open(name) as raw:
            return not _missing_columns(_reader(_text(raw)).fieldnames)
    except (OSError, csv.Error, zipfile.BadZipFile):
        return False


def _occurrence_member(zf: zipfile.ZipFile) -> str:
    """Pick the data table, not whatever ``.csv`` happens to come first.

    Zip member order is not guaranteed, and a SIMPLE_CSV download ships
    ``citations.csv`` / ``rights.csv`` alongside ``<downloadKey>.csv``.
    """
    names = [name for name in zf.namelist() if not name.endswith("/")]
    for name in names:
        if name.endswith("occurrence.txt"):
            return name
    csvs = [name for name in names if name.endswith(".csv")]
    for name in csvs:
        if _header_validates(zf, name):
            return name
    if csvs:
        # Nothing validated: return a candidate so the caller reports which
        # columns are missing rather than a misleading "no table" error.
        return csvs[0]
    raise ValueError("no occurrence table found in archive")


def _coordinates_usable(lat: float, lon: float) -> bool:
    return (
        math.isfinite(lat)
        and math.isfinite(lon)
        and -90.0 <= lat <= 90.0
        and -180.0 <= lon <= 180.0
    )


def _optional(value: str | None) -> str | None:
    """Absent free-text fields become NULL, never the empty string.

    Downstream filters compare against these columns; under Arrow semantics a
    null never equals anything, whereas ``""`` is a real value that would match
    every row whose source field was absent.
    """
    text = (value or "").strip()
    return text or None


def read_occurrences(zip_path: Path) -> pa.Table:
    """Extract the prior-relevant columns, dropping unusable rows.

    Rows without a parseable ``speciesKey``, without finite in-range
    coordinates, or without a day-precision ``eventDate`` are dropped, and the
    per-reason totals are logged at INFO. ``eventDate`` ranges collapse to the
    START date, timestamps drop their time part, and coarse dates such as
    ``2025-11`` or ``2025`` are dropped (see :func:`_parse_date`).

    ``scientific_name``, ``publisher_key`` and ``recorded_by`` are NULL — not
    ``""`` — when the source field is absent.
    """
    cols: dict[str, list] = {name: [] for name in OCCURRENCE_COLUMNS}
    dropped = {"no_species_key": 0, "bad_coordinates": 0, "unparseable_date": 0}
    total = 0

    with zipfile.ZipFile(zip_path) as zf:
        member = _occurrence_member(zf)
        with zf.open(member) as raw:
            reader = _reader(_text(raw))
            missing = _missing_columns(reader.fieldnames)
            if missing:
                raise ValueError(
                    f"occurrence table {member} is missing columns: {missing}"
                )
            for row in reader:
                total += 1
                species_raw = (row.get("speciesKey") or "").strip()
                try:
                    species_key = int(species_raw)
                except ValueError:
                    dropped["no_species_key"] += 1
                    continue

                lat_raw = (row.get("decimalLatitude") or "").strip()
                lon_raw = (row.get("decimalLongitude") or "").strip()
                try:
                    lat = float(lat_raw)
                    lon = float(lon_raw)
                except ValueError:
                    dropped["bad_coordinates"] += 1
                    continue
                if not _coordinates_usable(lat, lon):
                    dropped["bad_coordinates"] += 1
                    continue

                event = _parse_date((row.get("eventDate") or "").strip())
                if event is None:
                    dropped["unparseable_date"] += 1
                    continue

                cols["species_key"].append(species_key)
                cols["scientific_name"].append(_optional(row.get("species")))
                cols["lat"].append(lat)
                cols["lon"].append(lon)
                cols["event_date"].append(event)
                cols["publisher_key"].append(_optional(row.get("publishingOrgKey")))
                cols["recorded_by"].append(_optional(row.get("recordedBy")))

    logger.info(
        "read_occurrences(%s): kept %d of %d rows; dropped %d "
        "(no species key: %d, bad coordinates: %d, unparseable date: %d)",
        zip_path,
        len(cols["species_key"]),
        total,
        sum(dropped.values()),
        dropped["no_species_key"],
        dropped["bad_coordinates"],
        dropped["unparseable_date"],
    )

    return pa.table([cols[name] for name in OCCURRENCE_COLUMNS], schema=_SCHEMA)
