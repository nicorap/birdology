"""Mix bird-free background noise into recordings at fixed signal-to-noise ratios.

Used to build noisy copies of a test set (and, later, noise-augmented training
audio). Three measured pitfalls are pinned here:

* SNR is over the WHOLE recording: rms(signal) / rms(noise). Focal recordings
  have silent stretches, so during the calls the SNR is higher than nominal.
* ESC-50 pads short events with digital silence. Looping a padded clip would
  insert dead air into the noise bed, so clips are trimmed before looping and
  near-silent clips are rejected.
* libsndfile seeks back to patch the FLAC header after writing, so after
  ``sf.write(buf, ...)`` the stream position is NOT the encoded length. Reading
  ``buf.tell()`` there produced tar members holding a ~20-byte stub (the first
  noisy-DKX build). The encoded bytes are always taken with ``getvalue()``.
"""
from __future__ import annotations

import hashlib
import io
import tarfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import numpy as np
import soundfile as sf
from scipy.signal import resample_poly

#: Below this RMS a clip is treated as silence.
SILENCE_RMS = 1e-4


#: BirdNET 2.4's non-bird classes, as they appear in its scientific-name label
#: file. Listed explicitly: "Human vocal" or "Power tools" look like binomials.
NON_BIRD_LABELS = frozenset({
    "Dog", "Engine", "Environmental", "Fireworks", "Gun", "Human non-vocal",
    "Human vocal", "Human whistle", "Noise", "Power tools", "Siren",
})


def is_bird_label(name: str) -> bool:
    return name.strip() not in NON_BIRD_LABELS


def seed_for(name: str) -> int:
    """A stable per-recording seed, so every SNR of one recording gets the same noise."""
    return int(hashlib.sha1(name.encode()).hexdigest()[:8], 16)


def trim_silence(x: np.ndarray, threshold: float = SILENCE_RMS) -> np.ndarray:
    nz = np.flatnonzero(np.abs(x) > threshold)
    return x[nz[0]:nz[-1] + 1] if len(nz) else x


def mix_at_snr(x: np.ndarray, noise: np.ndarray, snr_db: float, peak: float = 0.99) -> np.ndarray:
    """x + noise scaled so rms(x)/rms(scaled noise) = 10**(snr_db/20); rescaled if it would clip."""
    if len(noise) != len(x):
        raise ValueError(f"noise length {len(noise)} != signal length {len(x)}")
    rs = np.sqrt(np.mean(np.square(x, dtype=np.float64))) + 1e-12
    rn = np.sqrt(np.mean(np.square(noise, dtype=np.float64))) + 1e-12
    y = x + noise * (rs / rn) / (10 ** (snr_db / 20))
    pk = float(np.abs(y).max()) if len(y) else 0.0
    return (y * (peak / pk) if pk > peak else y).astype(np.float32)


def flac_bytes(y: np.ndarray, sr: int) -> bytes:
    buf = io.BytesIO()
    sf.write(buf, y, sr, format="FLAC", subtype="PCM_16")
    return buf.getvalue()          # NOT buf.tell(): see the module docstring


class NoisePool:
    """A set of mono noise clips, resampled on demand and looped into noise beds."""

    def __init__(self, clips: Mapping[str, tuple[np.ndarray, int]]):
        kept = {}
        for name, (x, sr) in clips.items():
            x = np.asarray(x, np.float32)
            if x.ndim > 1:
                x = x.mean(1)
            x = x - x.mean()
            if np.sqrt(np.mean(x ** 2)) > SILENCE_RMS:
                kept[name] = (x, int(sr))
        if not kept:
            raise ValueError("noise pool is empty (every clip was silent)")
        self._clips = kept
        self.names = sorted(kept)
        self._cache: dict[tuple[str, int], np.ndarray] = {}

    @classmethod
    def from_files(cls, paths: Sequence[Path]) -> "NoisePool":
        clips = {}
        for p in paths:
            x, sr = sf.read(str(p), dtype="float32", always_2d=False)
            clips[Path(p).name] = (x, sr)
        return cls(clips)

    def clip_at(self, name: str, sr: int) -> np.ndarray:
        key = (name, int(sr))
        if key not in self._cache:
            x, s0 = self._clips[name]
            if s0 != sr:
                g = np.gcd(int(sr), s0)
                x = resample_poly(x, int(sr) // g, s0 // g).astype(np.float32)
            self._cache[key] = trim_silence(x)
        return self._cache[key]

    def bed(self, n: int, sr: int, rng: np.random.Generator) -> np.ndarray:
        """n samples of noise: random clips, trimmed, concatenated end to end."""
        out, filled = np.zeros(n, np.float32), 0
        while filled < n:
            seg = self.clip_at(self.names[rng.integers(len(self.names))], sr)
            k = min(len(seg), n - filled)
            out[filled:filled + k] = seg[:k]
            filled += k
        return out


def mix_shard(src: Path, outs: Mapping[float, Path], pool: NoisePool) -> tuple[int, list[str]]:
    """Write one noisy copy of tar shard ``src`` per SNR, same member names.

    Returns (recordings written, members skipped as undecodable or empty).
    The first written member of every SNR is decoded back and checked.
    """
    for p in outs.values():
        Path(p).parent.mkdir(parents=True, exist_ok=True)
    tars = {snr: tarfile.open(p, "w:gz") for snr, p in outs.items()}
    written, skipped = 0, []
    try:
        with tarfile.open(src) as tin:
            for m in tin:
                if not m.isfile():
                    continue
                try:
                    x, sr = sf.read(io.BytesIO(tin.extractfile(m).read()), dtype="float32", always_2d=False)
                except Exception:
                    skipped.append(m.name)
                    continue
                if x.ndim > 1:
                    x = x.mean(1)
                if len(x) == 0:
                    skipped.append(m.name)
                    continue
                noise = pool.bed(len(x), sr, np.random.default_rng(seed_for(m.name)))
                for snr, tout in tars.items():
                    data = flac_bytes(mix_at_snr(x, noise, snr), sr)
                    if written == 0:
                        back, _ = sf.read(io.BytesIO(data), dtype="float32", always_2d=False)
                        if len(back) != len(x):
                            raise RuntimeError(f"FLAC round-trip failed for {m.name}: {len(back)} != {len(x)}")
                    info = tarfile.TarInfo(m.name)
                    info.size = len(data)
                    tout.addfile(info, io.BytesIO(data))
                written += 1
    finally:
        for t in tars.values():
            t.close()
    return written, skipped
