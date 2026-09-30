"""Date soundscape recordings from their filenames.

Measured on HSN: 12000/12000 filenames carry a YYYYMMDD, e.g.
`HSN_001_20150708_061805.ogg` -> 2015-07-08. The soundscape test sets are not
Xeno-Canto (HSN is Zenodo record 7525805), so the XC API cannot date them.

WHY A PLAUSIBILITY WINDOW AND NOT JUST A REGEX: `HSN_001_20150708_061805`
contains a station id AND a date, and a corpus that numbered its sites with
eight digits would parse to a valid-looking date. Every eval recording would
land in one wrong week, the prior lookup would be confidently wrong for the
whole eval set, and every control would shift with it -- so nothing in the
results table would look odd. A well-formed date is not enough; it must also be
a date a bird could have been recorded on.
"""
from __future__ import annotations

import re
from datetime import date

#: Recordings older than this are not in any BirdSet soundscape corpus, and a
#: date after today is a parse error by construction.
CORPUS_WINDOW = (date(1990, 1, 1), date.today())

_EIGHT_DIGITS = re.compile(r"(?<!\d)(\d{8})(?!\d)")


class UndatableFilename(ValueError):
    """The filename carries no plausible recording date.

    Deliberately an exception rather than a None return: a caller that forgets
    to check a None places the recording in a default week, which is the exact
    silent failure this module exists to prevent.
    """


def parse_filename_date(
    name: str, *, window: tuple[date, date] = CORPUS_WINDOW
) -> date:
    """The recording date encoded in *name*.

    Takes the LAST plausible eight-digit match, because a leading field is more
    often a site or station id (`HSN_001_20150708_...` has the id first).
    """
    lo, hi = window
    candidates = _EIGHT_DIGITS.findall(name)
    if not candidates:
        raise UndatableFilename(
            f"{name!r} contains no eight-digit group; a soundscape filename is "
            "expected to carry YYYYMMDD"
        )

    seen: list[str] = []
    for raw in reversed(candidates):
        seen.append(raw)
        try:
            parsed = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        except ValueError:
            continue
        if lo <= parsed <= hi:
            return parsed

    raise UndatableFilename(
        f"{name!r}: none of {seen} is a calendar date within the plausible "
        f"recording window {lo.isoformat()}..{hi.isoformat()} -- they are "
        "malformed or outside it; refusing rather than guessing, because a "
        "wrong date silently moves the recording to another week"
    )
