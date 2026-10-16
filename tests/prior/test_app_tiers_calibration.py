"""Calibrating the tier thresholds against the SU list, breeders and swift season."""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from birdology.prior.app_tiers import BASE_THRESHOLDS, CalibrationError, calibrate, check_tiers, tiers_from_p  # noqa: E402

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


def test_calibrate_scales_the_thresholds_when_needed():
    # SU species at 1e-4 are scarce under the base (and at factor 2.0, where p equals
    # the bound); the next factor, 2 ** (9/8) = 2.18, makes them rare.
    thresholds, factor, checks = calibrate(_p(su=1e-4), SPECIES, SU)
    assert factor > 1.0 and checks["su_rare"]["passed"]
    assert thresholds == pytest.approx([t * factor for t in BASE_THRESHOLDS])


def test_calibrate_fails_loudly_when_no_factor_works():
    with pytest.raises(CalibrationError, match="breeders_common"):
        calibrate(_p(breeder=1e-5, su=1e-5), SPECIES, SU)


def test_missing_check_species_is_an_error():
    with pytest.raises(CalibrationError, match="Apus apus"):
        check_tiers(tiers_from_p(_p(), BASE_THRESHOLDS)[:, :, :3], SPECIES[:3], SU)
