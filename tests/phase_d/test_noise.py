"""Noise mixing: SNR arithmetic, looping, and the FLAC-in-tar round trip."""
import io
import tarfile

import numpy as np
import pytest
import soundfile as sf

from birdology.phase_d.noise import (
    NoisePool, flac_bytes, is_bird_label, mix_at_snr, mix_shard, seed_for, trim_silence,
)

SR = 32000


def _tone(seconds=1.0, f=1000.0, amp=0.3, sr=SR):
    t = np.arange(int(seconds * sr)) / sr
    return (amp * np.sin(2 * np.pi * f * t)).astype(np.float32)


def _rms(x):
    return float(np.sqrt(np.mean(np.square(x, dtype=np.float64))))


@pytest.mark.parametrize("snr", [20.0, 10.0, 0.0])
def test_mix_hits_the_requested_snr(snr):
    x = _tone(amp=0.1)
    noise = np.random.default_rng(0).normal(0, 0.05, len(x)).astype(np.float32)
    y = mix_at_snr(x, noise, snr)
    measured = 20 * np.log10(_rms(x) / _rms(y - x))
    assert measured == pytest.approx(snr, abs=0.05)


def test_mix_rescales_instead_of_clipping():
    x = _tone(amp=0.9)
    y = mix_at_snr(x, _tone(amp=0.9, f=1500.0), 0.0)
    assert np.abs(y).max() <= 0.99 + 1e-6


def test_mix_refuses_mismatched_lengths():
    with pytest.raises(ValueError):
        mix_at_snr(np.zeros(10, np.float32), np.zeros(9, np.float32), 10.0)


def test_flac_bytes_is_the_whole_stream_not_the_header():
    # regression: buf.tell() after sf.write returned a header-sized stub
    y = _tone(seconds=2.0)
    data = flac_bytes(y, SR)
    assert len(data) > 1000
    back, sr = sf.read(io.BytesIO(data), dtype="float32")
    assert sr == SR and len(back) == len(y)


def test_trim_silence_strips_digital_padding():
    x = np.r_[np.zeros(100), _tone(seconds=0.01), np.zeros(200)].astype(np.float32)
    assert len(trim_silence(x)) <= len(_tone(seconds=0.01))


def test_pool_rejects_silent_clips_and_loops_to_length():
    pool = NoisePool({"quiet": (np.zeros(SR, np.float32), SR), "hum": (_tone(seconds=0.25, f=120.0), SR)})
    assert pool.names == ["hum"]
    bed = pool.bed(SR * 3, SR, np.random.default_rng(1))
    assert len(bed) == SR * 3 and _rms(bed) > 0.1


def test_pool_resamples_to_the_recording_rate():
    pool = NoisePool({"hum": (_tone(seconds=1.0, sr=44100), 44100)})
    assert abs(len(pool.clip_at("hum", 22050)) - 22050) <= 2


def test_empty_pool_is_refused():
    with pytest.raises(ValueError):
        NoisePool({"quiet": (np.zeros(10, np.float32), SR)})


def test_seed_is_stable_per_name():
    assert seed_for("XC1.flac") == seed_for("XC1.flac") != seed_for("XC2.flac")


def test_bird_label_heuristic():
    assert is_bird_label("Turdus merula")
    for label in ("Engine", "Human vocal", "Power tools", "Siren"):
        assert not is_bird_label(label)


def test_mix_shard_writes_decodable_members_with_the_same_names(tmp_path):
    src = tmp_path / "src.tar.gz"
    with tarfile.open(src, "w:gz") as tar:
        for name in ("XC1.flac", "XC2.flac"):
            data = flac_bytes(_tone(seconds=1.5), SR)
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        bad = tarfile.TarInfo("XC3.flac")          # a zero-byte member, as in DKX
        tar.addfile(bad, io.BytesIO(b""))
    pool = NoisePool({"hum": (_tone(seconds=0.5, f=200.0), SR)})
    outs = {20.0: tmp_path / "snr20" / "s.tar.gz", 0.0: tmp_path / "snr00" / "s.tar.gz"}
    written, skipped = mix_shard(src, outs, pool)
    assert written == 2 and skipped == ["XC3.flac"]
    for snr, p in outs.items():
        with tarfile.open(p) as tar:
            assert sorted(tar.getnames()) == ["XC1.flac", "XC2.flac"]
            y, _ = sf.read(io.BytesIO(tar.extractfile("XC1.flac").read()), dtype="float32")
            assert len(y) == int(1.5 * SR)
    # the same noise draw at every SNR: louder noise, same waveform
    with tarfile.open(outs[20.0]) as a, tarfile.open(outs[0.0]) as b:
        ya, _ = sf.read(io.BytesIO(a.extractfile("XC1.flac").read()), dtype="float32")
        yb, _ = sf.read(io.BytesIO(b.extractfile("XC1.flac").read()), dtype="float32")
    x = _tone(seconds=1.5)
    assert np.corrcoef(ya - x, yb - x)[0, 1] > 0.95


def test_noisy_encoder_is_deterministic_per_recording_and_passes_through():
    from birdology.phase_d.noise import NoisyEncoder
    seen = []
    def enc(samples, sr):
        seen.append(samples.copy())
        return np.ones(3, np.float32), None
    class Closable:
        closed = False
        def __call__(self, s, sr): return enc(s, sr)
        def close(self): Closable.closed = True
    pool = NoisePool({"hum": (_tone(seconds=0.3, f=150.0), SR)})
    ne = NoisyEncoder(Closable(), pool, snr_range=(5.0, 5.0))
    x = _tone(seconds=1.0)
    out = ne(x, SR)
    assert out[1] is None and out[0].shape == (3,)
    ne(x, SR)
    assert np.array_equal(seen[0], seen[1])                       # same audio -> same noise
    measured = 20 * np.log10(_rms(x) / _rms(seen[0] - x))
    assert measured == pytest.approx(5.0, abs=0.1)
    ne.close()
    assert Closable.closed


def test_screened_clips_drops_birdy_noise_and_other_folds(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from birdology.phase_d.noise import screened_clips
    sci = ["Turdus merula", "Engine"]
    (tmp_path / "meta.csv").write_text(
        "filename,fold,target,category\n"
        "a.wav,1,0,wind\nb.wav,1,0,rain\nc.wav,5,0,engine\n")
    pq.write_table(pa.table({"filepath": ["a.wav", "b.wav", "c.wav"],
                             "logits": [[0.1, 0.9], [0.6, 0.0], [0.0, 0.0]]}), tmp_path / "s.parquet")
    keep, dropped = screened_clips(tmp_path / "meta.csv", tmp_path / "s.parquet", sci, folds=[1], bird_max=0.25)
    # a.wav: the 0.9 is the non-bird Engine class, so it stays; b.wav hears a blackbird
    assert keep == ["a.wav"] and [d[0] for d in dropped] == ["b.wav"]
