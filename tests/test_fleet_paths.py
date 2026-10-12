"""Birdology lives under /srv/home/rapnic/birdology, off the shared ML folders.

The move (2026-10-12) was a prefix swap: /faic/weka/ML -> .../data and
/shared/apps/ML -> .../apps. A script still naming an old root would read a
stale copy, or, once the old copy is deleted, an empty directory -- exit 0.
"""
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent

_OLD_ROOTS = ("/faic/weka/ML", "/shared/apps/ML")

# Allowed to name the old roots: the move script itself (they are its
# sources) and the WEKA benchmarks, whose subject is the WEKA filesystem.
_EXEMPT = {
    "scripts/cluster/move_to_home.sh",
    "scripts/cluster/weka_bench.sh",
    "scripts/cluster/weka_io_report.sh",
    "scripts/cluster/weka_verify.sh",
    "scripts/cluster/wekabench.sbatch",
}


def _code_files():
    for top in ("scripts", "src", "tests"):
        for path in (_REPO / top).rglob("*"):
            if path.suffix in {".py", ".sh", ".sbatch"} and "__pycache__" not in path.parts:
                yield path
    yield _REPO / "Makefile"


def test_no_code_names_an_old_ml_root():
    offenders = []
    for path in _code_files():
        rel = path.relative_to(_REPO).as_posix()
        if rel in _EXEMPT or path == Path(__file__).resolve():
            continue
        text = path.read_text(errors="replace")
        offenders += [f"{rel}: {root}" for root in _OLD_ROOTS if root in text]
    assert not offenders, "old ML roots still referenced:\n" + "\n".join(offenders)
