"""The app's committed test fixture is byte-identical to what this repo writes."""
import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

APP_FIXTURE = ROOT.parent / "fugle-dk" / "test" / "fixtures" / "dk_tiers"
FILES = ("dk_tiers.json", "dk_tiers.bin", "dk_raster.bin")


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_fixture_matches_the_apps_committed_files(tmp_path):
    if not APP_FIXTURE.is_dir():
        pytest.skip("fugle-dk checkout not present")
    spec = importlib.util.spec_from_file_location("prior_app_export", ROOT / "scripts" / "prior_app_export.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.fixture(tmp_path) == 0
    for name in FILES:
        assert _sha(tmp_path / name) == _sha(APP_FIXTURE / name), name
