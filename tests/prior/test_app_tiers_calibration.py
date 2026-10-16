"""Calibrating the tier thresholds against the SU list, breeders and swift season."""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.app_tiers import BASE_THRESHOLDS, CalibrationError, calibrate, check_su_per_species, check_tiers, force_su_rare, tiers_from_p  # noqa: E402

SPECIES = ["Turdus merula", "Parus major", "Fringilla coelebs", "Apus apus"] + [f"Rara su{i}" for i in range(10)]
SU = {f"Rara su{i}" for i in range(10)}


def _p(breeder=0.02, su=1e-5, swift_winter=1e-6):
    p = np.full((4, 52, len(SPECIES)), 1e-6)
    p[:, :, :3] = breeder
    p[:, 17:33, 3] = 0.005
    p[:, :10, 3] = p[:, 44:, 3] = swift_winter
    p[:, :, 4:] = su
    return p


def test_checks_pass_on_a_well_behaved_prior():
    checks = check_tiers(tiers_from_p(_p(), BASE_THRESHOLDS), SPECIES, SU)
    assert set(checks) == {"su_rare", "breeders_common", "swift_winter_rare"}
    assert all(c["passed"] for c in checks.values())


def test_calibrate_keeps_the_base_when_it_passes():
    thresholds, factor, checks = calibrate(_p(), SPECIES, SU)
    assert factor == 1.0 and thresholds == list(BASE_THRESHOLDS)


def test_calibrate_ignores_su_shares():
    # SU species at 1e-4 (scarce under the base) no longer drive the thresholds.
    thresholds, factor, checks = calibrate(_p(su=1e-4), SPECIES, SU)
    assert factor == 1.0 and set(checks) == {"breeders_common", "swift_winter_rare"}


def test_calibrate_scales_for_the_swift_check():
    # Swift winter p of 1e-4 is scarce under the base; a larger factor makes it rare.
    thresholds, factor, checks = calibrate(_p(swift_winter=1e-4), SPECIES, SU)
    assert factor > 1.0 and checks["swift_winter_rare"]["passed"]
    assert thresholds == pytest.approx([t * factor for t in BASE_THRESHOLDS])


def test_a_twitched_su_species_is_forced_rare():
    p = _p()
    p[1, 20, 4] = 0.02                      # one cell-week, hundreds of reports
    tiers = tiers_from_p(p, BASE_THRESHOLDS)
    assert tiers[1, 20, 4] == 5
    assert not check_su_per_species(tiers, SPECIES, SU)["passed"]
    forced_tiers, forced = force_su_rare(tiers, SPECIES, SU)
    assert forced == sorted(SU) and tiers[1, 20, 4] == 5     # input untouched
    assert (forced_tiers[:, :, 4:] == 0).all()
    assert (forced_tiers[:, :, :4] == tiers[:, :, :4]).all()
    assert check_su_per_species(forced_tiers, SPECIES, SU)["passed"]


def test_su_check_reports_the_minimum_over_species():
    tiers = tiers_from_p(_p(), BASE_THRESHOLDS)
    tiers[0, :26, 6] = 3                    # half of one species' cell-weeks of cell 0
    r = check_su_per_species(tiers, SPECIES, SU)
    assert r["worst"] == "Rara su2" and r["value"] == pytest.approx(1 - 26 / (4 * 52))
    assert not r["passed"]


def test_su_names_reach_the_app_names_through_aliases():
    species = SPECIES[:4] + [f"Rara su{i}" for i in range(9)] + ["Curruca nisoria"]
    tiers = np.full((1, 52, len(species)), 5, np.uint8)
    su = {f"Rara su{i}" for i in range(9)} | {"Sylvia nisoria"}
    forced_tiers, forced = force_su_rare(tiers, species, su)
    assert "Curruca nisoria" in forced and (forced_tiers[:, :, -1] == 0).all()


def test_calibrate_fails_loudly_when_no_factor_works():
    with pytest.raises(CalibrationError, match="breeders_common"):
        calibrate(_p(breeder=1e-5), SPECIES, SU)


def test_missing_check_species_is_an_error():
    with pytest.raises(CalibrationError, match="Apus apus"):
        check_tiers(tiers_from_p(_p(), BASE_THRESHOLDS)[:, :, :3], SPECIES[:3], SU)
