"""Species whose Danish records must be documented to the Danish Rarities
Committee (SU), from DOF's checklist "The Danish List" (updated Sept 2025).

The checklist marks such taxa with "X" in its first column, on the species row
or on subspecies rows; "(X)" means only records from some regions, which is not
a national rarity. A species counts as SU when its own row is marked, or when it
has subspecies rows and every one of them is marked. Only Appendix 1 (categories
A, B, C: the Danish list proper) is read.
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path

SOURCE_URL = (
    "https://www.dof.dk/images/udvalg/su/dokumenter/su_listen/"
    "The_Danish_List_-_A_Checklist_of_the_Birds_of_Denmark.pdf"
)
_SPECIES = re.compile(r"^[A-Z][a-z]+ [a-z]+(-[a-z]+)?$")
_SUBSPECIES = re.compile(r"^[A-Z]\. [a-z]+\.")
_CATEGORY = re.compile(r"^[A-F][0-9.]*( [A-F][0-9.]*)*$")


@dataclass
class _Row:
    name: str
    danish: str
    english: str
    mark: str
    subspecies_marks: list[str] = field(default_factory=list)


def parse_checklist(text: str) -> list[tuple[str, str, str]]:
    """(scientific, Danish, English) names of the SU species in checklist text."""
    rows: list[_Row] = []
    for line in text.splitlines():
        if "APPENDIX 2" in line:
            break
        fields = [f for f in re.split(r"\s{2,}", line.strip()) if f]
        if not fields:
            continue
        mark = fields.pop(0) if fields[0] in ("X", "(X)") else ""
        if fields and _CATEGORY.match(fields[0]):
            fields.pop(0)
        if not fields:
            continue
        name = fields[0].replace("†", "").strip()
        if _SPECIES.match(name):
            rows.append(_Row(name, fields[1] if len(fields) > 1 else "",
                             fields[2] if len(fields) > 2 else "", mark))
        elif _SUBSPECIES.match(name) and rows:
            rows[-1].subspecies_marks.append(mark)
    return [
        (r.name, r.danish, r.english)
        for r in rows
        if r.mark == "X" or (r.subspecies_marks and all(m == "X" for m in r.subspecies_marks))
    ]


def load_su_species(path: Path) -> set[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return {row["scientific_name"] for row in csv.DictReader(f)}
