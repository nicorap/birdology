"""Golden-reference gate: OUR wrappers vs the OFFICIAL tooling, same audio.

The one test that stands between a subtly wrong window/normalization and a
528k-recording embedding cache that is indistinguishable from a correct one.
Runs ON THE CLUSTER inside tf.sif (TF + perch-hoplite + birdnet present).

ORDER MATTERS (measured 2026-09-30): BirdNET runs FIRST. The combination
perch-loaded + TWO birdnet pb sessions (reference + ours) deterministically
kills the second birdnet worker; every subset — two birdnet sessions alone,
or perch + one session — passes. Production never hits the bad cell (one
wrapper, one session per task), but the gate would. Bisect transcripts:
bn_seq_probe / bn_order_probe / bn_bs_probe / gate_min_probe.

A REAL FILE because birdnet's spawn workers re-execute the main module.
"""
import sys

import numpy as np

sys.path.insert(0, "/opt/birdology/src")

TOL = 1e-5


def _report(name, ours, ref):
    ours = np.asarray(ours, dtype=np.float32)
    ref = np.asarray(ref, dtype=np.float32)
    if ours.shape != ref.shape:
        print(f"FAIL {name}: shape {ours.shape} vs reference {ref.shape}")
        return False
    md = float(np.max(np.abs(ours - ref)))
    ok = md <= TOL
    print(f"{'ok  ' if ok else 'FAIL'} {name}: max|diff|={md:.3e} (tol {TOL})")
    return ok


def main() -> int:
    rng = np.random.default_rng(20260930)
    fails = 0

    clips = {
        "3s": (0.2 * rng.standard_normal(3 * 32000)).astype(np.float32),
        "5s": (0.2 * rng.standard_normal(5 * 32000)).astype(np.float32),
        "7s": (0.2 * rng.standard_normal(7 * 32000)).astype(np.float32),
    }

    from birdology.phase_a.encoders import load_encoder

    # ---- BirdNET FIRST (see module docstring for why) ----
    import birdnet

    official_bn = birdnet.load("acoustic", "2.4", "pb")
    ours_bn = load_encoder("birdnet")
    for name, clip in clips.items():
        res = official_bn.encode_arrays(
            [(clip, 32000)], device="GPU", n_workers=1, batch_size=1
        )
        emb = np.asarray(res.embeddings)[0]
        mask = np.asarray(res.embeddings_masked)[0]
        # numpy MaskedArray convention (measured): True = PADDING.
        valid = ~mask.any(axis=-1)
        ref = emb[valid].mean(axis=0).astype(np.float32)
        got = ours_bn(clip, 32000)
        fails += not _report(f"birdnet/{name}", got, ref)

    # ---- Perch: ours vs perch-hoplite's own embed()+pooled_embeddings ----
    import tensorflow as tf
    assert tf.config.list_physical_devices("GPU"), (
        "no GPU: the reference would fall back to perch_v2_cpu"
    )
    from perch_hoplite.zoo import model_configs

    official = model_configs.load_model_by_name("perch_v2")
    ours_perch = load_encoder("perch")
    for name, clip in clips.items():
        ref = official.embed(clip).pooled_embeddings(
            time_pooling="mean", channel_pooling="mean"
        )
        got = ours_perch(clip, 32000)
        fails += not _report(f"perch/{name}", got, ref)

    a = ours_perch(clips["5s"], 32000)
    b = ours_perch(clips["5s"], 32000)
    det = bool((np.asarray(a) == np.asarray(b)).all())
    print(f"{'ok  ' if det else 'FAIL'} perch/determinism: byte-identical={det}")
    fails += not det

    print("GOLDEN_OK" if fails == 0 else f"GOLDEN_FAILED ({fails})")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
