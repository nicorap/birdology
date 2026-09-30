"""Soundscape filenames carry their own date. Measured: 12000/12000 in HSN."""
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.phase_a.soundscape_dates import (
    UndatableFilename,
    parse_filename_date,
)


def test_parses_the_real_hsn_shape():
    # Measured from HSN_metadata_test.parquet.
    assert parse_filename_date("HSN_001_20150708_061805.ogg") == date(2015, 7, 8)


def test_parses_the_5s_segment_shape():
    assert parse_filename_date(
        "HSN_001_20150708_061805_000_005.ogg"
    ) == date(2015, 7, 8)


def test_rejects_a_filename_with_no_date():
    with pytest.raises(UndatableFilename):
        parse_filename_date("XC805337.ogg")


def test_rejects_an_IMPLAUSIBLE_date_that_is_well_formed():
    """Review Focus 1: the defect that would shift the whole eval set.

    A site code that parses as a valid date must be refused, not accepted.
    19010101 is a real date and not a real recording date.
    """
    with pytest.raises(UndatableFilename, match="outside"):
        parse_filename_date("SITE_19010101_0600.ogg")


def test_rejects_a_future_date():
    with pytest.raises(UndatableFilename, match="outside"):
        parse_filename_date("SITE_29991231_0600.ogg")


def test_rejects_digits_that_are_not_a_calendar_date():
    # 20151345: month 13, day 45.
    with pytest.raises(UndatableFilename):
        parse_filename_date("HSN_001_20151345_061805.ogg")


def test_a_station_id_that_looks_like_a_date_is_refused_by_the_window():
    """The specific trap: HSN_001_... has a station id AND a date."""
    # If a corpus numbered its sites 20150708 with no real date, the window is
    # what saves us -- but a 1970 site code inside the window would not be
    # caught, which is why the parser takes the LAST plausible match and the
    # manifest asserts a date spread (Task 3).
    with pytest.raises(UndatableFilename):
        parse_filename_date("SITE_12345678_0600.ogg")


def test_the_LAST_eight_digit_group_wins_over_an_in_window_station_id():
    """Kills the 'take the first match' defect.

    None of the other cases has TWO eight-digit groups, so none of them can
    tell first-match from last-match. A corpus whose station ids happen to
    parse as in-window dates (`SITE_20010101_<real date>_...`) is exactly the
    case the window cannot catch: the id is a perfectly plausible date. Only
    preferring the LAST group gets the real recording date, because the
    leading field of a soundscape filename is the site/station, not the date.
    """
    assert parse_filename_date(
        "SITE_20010101_20150708_061805.ogg"
    ) == date(2015, 7, 8)


def test_window_is_configurable_for_a_corpus_that_needs_it():
    assert parse_filename_date(
        "OLD_19500101_0600.ogg", window=(date(1940, 1, 1), date(1960, 1, 1))
    ) == date(1950, 1, 1)


def test_a_malformed_candidate_before_the_real_date_is_skipped_not_fatal():
    """Kills the continue->break mutant the review found (M-D).

    The scan runs right-to-left, so 99999999 (month 99) is tried FIRST and
    must be skipped -- not abort the scan -- so the real date behind it wins.
    """
    assert parse_filename_date(
        "HSN_001_20150708_99999999.ogg"
    ) == date(2015, 7, 8)
