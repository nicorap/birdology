"""Read Phase A BirdNET caches (embedding + logits per recording) into arrays."""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

#: Rough European box: the UK and northern France to the Baltics and Finland.
EUROPE = {"lat": (46.0, 71.5), "lon": (-11.0, 32.0)}


@dataclass
class Cache:
    filepaths: list[str]
    embeddings: np.ndarray       # (n, 1024) float32
    scores: np.ndarray | None    # (n, n_axis) BirdNET sigmoid scores, float32

    def take(self, idx: np.ndarray) -> "Cache":
        idx = np.asarray(idx)
        return Cache([self.filepaths[i] for i in idx], self.embeddings[idx],
                     None if self.scores is None else self.scores[idx])


def _matrix(col: pa.ChunkedArray, n: int) -> np.ndarray:
    return np.asarray(col.combine_chunks().flatten(), np.float32).reshape(n, -1)


def load_cache(dirs: Iterable[Path], keep: Sequence[str] | None = None, scores: bool = True) -> Cache:
    """Concatenate every ``*.parquet`` under ``dirs`` (sorted), optionally only
    rows whose filepath is in ``keep``. A filepath cached twice keeps its first row."""
    cols = ["filepath", "embedding"] + (["logits"] if scores else [])
    want = pa.array(sorted(set(keep))) if keep is not None else None
    F, E, S = [], [], []
    for d in dirs:
        for f in sorted(Path(d).glob("*.parquet")):
            t = pq.read_table(f, columns=cols)
            if want is not None:
                t = t.filter(pc.is_in(t.column("filepath"), value_set=want))
            if t.num_rows == 0:
                continue
            F += t.column("filepath").to_pylist()
            E.append(_matrix(t.column("embedding"), t.num_rows))
            if scores:
                S.append(_matrix(t.column("logits"), t.num_rows))
    if not F:
        raise ValueError(f"no cached rows under {list(map(str, dirs))}")
    _, first = np.unique(np.array(F), return_index=True)
    first = np.sort(first)
    return Cache([F[i] for i in first], np.concatenate(E)[first],
                 np.concatenate(S)[first] if scores else None)


def xc_id(filepath: str) -> int:
    """XC1051471.flac -> 1051471."""
    digits = "".join(ch for ch in Path(filepath).stem if ch.isdigit())
    if not digits:
        raise ValueError(f"no Xeno-canto id in {filepath!r}")
    return int(digits)


def in_box(lat: np.ndarray, lon: np.ndarray, box: dict = EUROPE) -> np.ndarray:
    lat, lon = np.asarray(lat, float), np.asarray(lon, float)
    return (lat >= box["lat"][0]) & (lat <= box["lat"][1]) & (lon >= box["lon"][0]) & (lon <= box["lon"][1])
