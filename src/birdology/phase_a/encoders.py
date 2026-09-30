"""Frozen encoders: melsummary (checkpoint-free), Perch 2.0, BirdNET V2.4.

Perch and BirdNET wrap heavyweight TF runtimes that do NOT exist on the dev
Mac or in CI, so this module keeps zero module-level TF/birdnet imports: the
runtimes are imported lazily inside the loader functions and raise ImportError
with install instructions when absent. The framing, normalization and pooling
logic — the part a subtle bug would silently poison 528k cached embeddings
with — lives here as pure-numpy functions so contract tests can pin it down
without TensorFlow.

Cited facts (2026-09-30 reconnaissance, docs/superpowers/plans/
2026-09-30-task6-real-encoders.md):
- Perch 2.0: mono float32 32 kHz, 5 s windows (160 000 samples), hop 5 s;
  recordings < 5 s are zero-CENTER-padded; per-window normalization is
  mean-subtract then peak-normalize to 0.25 (perch-hoplite's normalize_audio);
  serving returns 1536-d per window; recording-level = MEAN over windows
  (the paper's own recipe, §2.5.1).
- BirdNET V2.4: the `birdnet` pip package resamples internally (scipy FFT),
  so we pass OUR sr through; `encode_arrays` returns embeddings
  (1, n_segments, 1024) plus a boolean mask of the same shape; a segment is
  VALID when ~mask.any(axis=-1) (True marks PADDING — numpy MaskedArray
  convention, measured); recording-level = MEAN over valid segments.
  n_workers MUST be 1 under Slurm (the package spawns per-core workers
  otherwise).
"""
from __future__ import annotations

from collections.abc import Callable, Mapping

import numpy as np

Encoder = Callable[[np.ndarray, int], np.ndarray]

#: (samples, sr) -> (embedding, pooled_logits_or_None). The logits are the
#: per-recording species posterior Phase B needs; None means the encoder has
#: no classifier head (melsummary). Caching them beside the embedding costs
#: ~58 KB/recording; re-running 581 GB of audio to get them later would not.
EncoderWithLogits = Callable[
    [np.ndarray, int], "tuple[np.ndarray, np.ndarray | None]"
]

PERCH_SR = 32_000
PERCH_WINDOW_SAMPLES = 160_000  # 5 s at 32 kHz
PERCH_HOP_SAMPLES = 160_000  # hop 5 s: non-overlapping windows
PERCH_TARGET_PEAK = 0.25


def _mean_log_mel(samples: np.ndarray, sr: int, n_mels: int = 64) -> np.ndarray:
    """A real, deterministic, checkpoint-free embedding.

    Deliberately NOT a random or constant vector: a stand-in that ignores its
    input would let every downstream test pass on data that carries no signal,
    which is the failure this project keeps finding. This is a genuine (if
    weak) acoustic summary, so a fusion test that improves on it is measuring
    something.
    """
    if samples.size == 0:
        return np.zeros(n_mels, dtype=np.float32)
    spectrum = np.abs(np.fft.rfft(samples.astype(np.float64)))
    edges = np.linspace(0, spectrum.size, n_mels + 1).astype(int)
    bands = [spectrum[a:b].mean() if b > a else 0.0 for a, b in zip(edges, edges[1:])]
    return np.log1p(np.asarray(bands, dtype=np.float64)).astype(np.float32)


# ---------------------------------------------------------------------------
# Perch 2.0 — pure-numpy framing/normalization + lazy TF wrapper
# ---------------------------------------------------------------------------


def _batch_for_perch(samples: np.ndarray, sr: int) -> np.ndarray:
    """Validate and shape a mono recording into hoplite's (1, n_samples) batch.

    DELIBERATELY NO framing and NO normalization here. The real
    `batch_embed` frames (window 5 s, hop 5 s, trailing remainder dropped,
    short input padded) and normalizes (mean-subtract, peak 0.25) INTERNALLY
    — measured by reading its source on the fleet (2026-09-30). Our first
    implementation reproduced those stages faithfully (librosa-oracle
    byte-identical) and STILL failed the golden gate at ~1.2e-4: hoplite
    re-normalized our already-normalized windows, and in float32 the
    re-computed mean is not exactly 0 nor the peak exactly 0.25, so the
    inputs drifted ~1e-7 and the network amplified it. Equality with the
    reference comes from SHARING its code path, not imitating it.

    sr must be 32 kHz: the corpus is native 32 kHz, and silently accepting
    another rate would embed garbage no downstream check could catch.
    """
    if sr != PERCH_SR:
        raise ValueError(
            f"perch requires sr == {PERCH_SR} (got {sr}); the corpus is native "
            "32 kHz and resampling here would silently change the embeddings"
        )
    x = np.asarray(samples, dtype=np.float32).reshape(-1)
    if x.size == 0:
        raise ValueError("empty audio cannot be embedded")
    return x[None, :]


def _pool_perch_outputs(embeddings_array: np.ndarray) -> np.ndarray:
    """Pool hoplite batch_embed output for ONE recording to (1536,) float32.

    MEASURED on the fleet (2026-09-30, perch_v2 GPU SavedModel via
    perch-hoplite 1.0.4): `batch_embed([1, n_samples])` returns
    **(1, n_windows, 1, 1536)** — batch, time, channel, features — where
    n_windows is hoplite's OWN framing of the recording (7 s → 1, 10 s → 2).
    We pass exactly one recording per call, so batch must be 1 and channel
    singleton; the recording-level embedding is the MEAN over the time axis
    (the Perch paper's own recipe, §2.5.1 — identical to
    pooled_embeddings(time_pooling='mean', channel_pooling='mean')).
    A 2-D (n_windows, 1536) input is accepted for mocked tests. Anything
    else raises loudly rather than silently mixing recordings.

    Pure numpy so contract tests can pin the axis logic without TensorFlow.
    """
    emb = np.asarray(embeddings_array, dtype=np.float32)
    if emb.ndim == 2:
        return emb.mean(axis=0).astype(np.float32)
    if emb.ndim == 4 and emb.shape[0] == 1 and emb.shape[2] == 1:
        return emb[0, :, 0, :].mean(axis=0).astype(np.float32)
    if emb.ndim == 3 and emb.shape[1] == 1:
        # embed()-style (n_windows, 1, 1536)
        return emb[:, 0, :].mean(axis=0).astype(np.float32)
    raise ValueError(
        f"unexpected perch embeddings shape {emb.shape}; expected "
        "(1, n_windows, 1, 1536) from batch_embed of ONE recording — "
        "a batch > 1 here would silently mix recordings"
    )


def _pool_perch_logits(logits_array: np.ndarray) -> np.ndarray:
    """Pool hoplite batch_embed logits['label'] for ONE recording to (n_classes,).

    MEASURED (batch_embed source read on the fleet, 2026-09-30): the logits
    dict's 'label' entry is reshaped to ``framed_audio.shape[:2] + (n_classes,)``
    == (1, n_windows, 14795) for a single-recording batch. Recording-level
    posterior = MEAN over the time (window) axis — the same pooling direction
    as the embedding. BirdNET deliberately differs (max — BirdSet's
    convention); the asymmetry is intentional, see _max_pool_birdnet_logits.

    THESE ARE RAW, UNCALIBRATED LOGITS. MEASURED on the fleet (2026-09-30):
    hoplite's model object does NOT apply the perch_v2 calibration
    (*0.97 - 10.0). batch_embed logits equal the raw serving_default logits
    (max|diff| 9.2e-1 against the calibrated version — i.e. equals raw, not
    calibrated; silence median -1.70, nowhere near the calibrated -10). The
    reconnaissance claim "calibration is applied by the model object" was
    FALSE. Decision: the cache stores RAW model logits — calibration is a
    downstream display concern; Phase B fuses RELATIVE scores, and a fixed
    affine map cancels in a softmax. The perch encoder declares
    'perch_logits_are_raw_uncalibrated: true' in the parquet schema metadata
    (via its `parquet_metadata` attribute, written by embed_shard) so no
    consumer can assume calibrated values.

    A 2-D (n_windows, n_classes) input is accepted for mocked tests. Anything
    else raises loudly rather than squeezing/reshaping blind: a cited API
    claim is a hypothesis until measured, and a silently mis-pooled posterior
    is indistinguishable from a correct one downstream.
    """
    lg = np.asarray(logits_array, dtype=np.float32)
    if lg.ndim == 2:
        return lg.mean(axis=0).astype(np.float32)
    if lg.ndim == 3 and lg.shape[0] == 1:
        return lg[0].mean(axis=0).astype(np.float32)
    raise ValueError(
        f"unexpected perch logits shape {lg.shape}; expected "
        "(1, n_windows, n_classes) from batch_embed of ONE recording — "
        "refusing to guess axes on a surprise shape"
    )


def _load_perch_model() -> Callable[[np.ndarray], Mapping[str, np.ndarray]]:
    """Lazy-load Perch 2.0 via perch-hoplite; adapt to windows -> {'embedding'}.

    Raises ImportError with install instructions when the runtime is absent —
    never a silent fallback: an embedding cache built with the wrong encoder
    is indistinguishable from a correct one.
    """
    try:
        from perch_hoplite.zoo import model_configs
    except ImportError as exc:
        raise ImportError(
            "the perch encoder requires perch-hoplite and TensorFlow >= 2.20: "
            "pip install perch-hoplite  (on the fleet, use the tf.sif "
            "container from Task 6.1). Refusing to fall back to a stand-in."
        ) from exc
    loaded = model_configs.load_model_by_name("perch_v2")

    def infer(batch: np.ndarray) -> Mapping[str, np.ndarray]:
        # batch is (1, n_samples) RAW audio; hoplite frames and normalizes.
        return {"embedding": _pool_perch_outputs(loaded.batch_embed(batch).embeddings)}

    return infer


def make_perch_encoder(
    model: Callable[[np.ndarray], Mapping[str, np.ndarray]] | None = None,
) -> Encoder:
    """Build the Perch 2.0 encoder.

    `model` is injectable for contract tests: a callable taking a
    (1, n_samples) float32 RAW-audio batch (framing and normalization happen
    INSIDE hoplite's batch_embed — see _batch_for_perch for why imitating
    them fails the golden gate) and returning a mapping with 'embedding'
    of shape (1536,) pooled, or (n_windows, 1536) from a mock. Default None
    lazy-loads the real SavedModel (ImportError when the runtime is absent).
    """
    if model is None:
        model = _load_perch_model()

    def encode(samples: np.ndarray, sr: int) -> np.ndarray:
        batch = _batch_for_perch(samples, sr)
        # model returns the ALREADY-POOLED (1536,) recording embedding (or a
        # (n,1536) window matrix from a mock, which _pool… mean-pools).
        emb = np.asarray(model(batch)["embedding"], dtype=np.float32)
        if emb.ndim == 2:
            emb = emb.mean(axis=0)
        return emb.astype(np.float32)

    return encode


def _load_perch_model_with_logits() -> Callable[[np.ndarray], Mapping[str, np.ndarray]]:
    """Lazy-load Perch 2.0; adapt to windows -> {'embedding', 'logits'}.

    batch_embed's InferenceOutputs carries a `.logits` dict whose 'label'
    entry is the (1, n_windows, 14795) species head (MEASURED: batch_embed
    source reshapes logits to framed_audio.shape[:2] + (n_classes,)).
    These logits are RAW and UNCALIBRATED — measured, see _pool_perch_logits
    — and are cached as-is; never apply the *0.97 - 10.0 map here. A missing
    logits dict or 'label' key raises loudly: a cache silently built without
    its posterior would force the second 581 GB pass this column exists to
    avoid.
    """
    try:
        from perch_hoplite.zoo import model_configs
    except ImportError as exc:
        raise ImportError(
            "the perch encoder requires perch-hoplite and TensorFlow >= 2.20: "
            "pip install perch-hoplite  (on the fleet, use the tf.sif "
            "container from Task 6.1). Refusing to fall back to a stand-in."
        ) from exc
    loaded = model_configs.load_model_by_name("perch_v2")

    def infer(batch: np.ndarray) -> Mapping[str, np.ndarray]:
        outputs = loaded.batch_embed(batch)
        logits_dict = getattr(outputs, "logits", None)
        if not logits_dict or "label" not in logits_dict:
            have = sorted(logits_dict) if logits_dict else []
            raise ValueError(
                "perch batch_embed returned no logits['label'] "
                f"(logits keys: {have}); the with-logits encoder refuses to "
                "write an embedding-only row silently — use load_encoder("
                "'perch') if logits are genuinely not wanted"
            )
        return {
            "embedding": _pool_perch_outputs(outputs.embeddings),
            "logits": _pool_perch_logits(logits_dict["label"]),
        }

    return infer


def make_perch_encoder_with_logits(
    model: Callable[[np.ndarray], Mapping[str, np.ndarray]] | None = None,
) -> EncoderWithLogits:
    """Build the Perch 2.0 encoder that also returns pooled species logits.

    `model` is injectable for contract tests: a callable taking the
    (1, n_samples) RAW-audio batch and returning a mapping with 'embedding'
    ((1536,) pooled or (n_windows, 1536) from a mock) and 'logits'
    ((n_classes,) pooled, or (n_windows, n_classes) / (1, n_windows,
    n_classes) from a mock — pooled here with the SAME mean-over-time rule
    the real adapter uses). Default None lazy-loads the real SavedModel.
    """
    if model is None:
        model = _load_perch_model_with_logits()

    def encode2(samples: np.ndarray, sr: int) -> tuple[np.ndarray, np.ndarray]:
        batch = _batch_for_perch(samples, sr)
        out = model(batch)
        emb = np.asarray(out["embedding"], dtype=np.float32)
        if emb.ndim == 2:
            emb = emb.mean(axis=0)
        raw_logits = np.asarray(out["logits"], dtype=np.float32)
        logits = (
            raw_logits if raw_logits.ndim == 1 else _pool_perch_logits(raw_logits)
        )
        return emb.astype(np.float32), logits

    # The measured falsification (hoplite does NOT apply the perch_v2
    # calibration — see _pool_perch_logits) must travel WITH the cache:
    # embed_shard copies this declaration into the parquet schema metadata
    # so no consumer can assume calibrated values.
    encode2.parquet_metadata = {  # type: ignore[attr-defined]
        "perch_logits_are_raw_uncalibrated": "true"
    }
    return encode2


# ---------------------------------------------------------------------------
# BirdNET V2.4 — pure-numpy masked pooling + lazy wrapper
# ---------------------------------------------------------------------------


def _masked_mean(embeddings: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Mean over VALID segments of (n_segments, dim) embeddings.

    `mask` is birdnet's boolean embeddings_masked, numpy MaskedArray
    convention: **True = MASKED-OUT (padding), False = valid data**.
    MEASURED on the fleet (2026-09-30, bn_mask_probe): a lone 3 s clip's one
    real segment reads False, and in a mixed-length batch the short clip's
    padding segments read True with NaN embeddings. (The reconnaissance
    report had it inverted — "True = valid" — and the wrapper shipped with
    that inversion; a single-recording call then saw zero valid segments.)
    A segment is valid when NOT mask.any(axis=-1); padding segments are
    excluded — averaging them in would inject NaN into the embedding.
    """
    emb = np.asarray(embeddings, dtype=np.float32)
    valid = ~np.asarray(mask, dtype=bool).any(axis=-1)
    if not valid.any():
        raise ValueError("birdnet returned no valid segments for this recording")
    return emb[valid].mean(axis=0).astype(np.float32)


def _load_birdnet_model():
    """Lazy-load BirdNET V2.4 (ProtoBuf backend — the GPU path)."""
    try:
        import birdnet
    except ImportError as exc:
        raise ImportError(
            "the birdnet encoder requires the birdnet package: "
            "pip install birdnet  (on the fleet, use the tf.sif container "
            "from Task 6.1). Refusing to fall back to a stand-in."
        ) from exc
    return birdnet.load("acoustic", "2.4", "pb")


def make_birdnet_encoder(model=None, device: str = "GPU") -> Encoder:
    """Build the BirdNET V2.4 encoder.

    `model` is injectable for contract tests: an object whose
    `encode_arrays([(samples, sr)], n_workers=1, device=...)` returns a
    result with `.embeddings` (1, n_segments, 1024) and boolean
    `.embeddings_masked` of the same shape. Default None lazy-loads the
    real model.

    `device` is forwarded EXPLICITLY to every encode_arrays call: the pb
    backend selects its device at ENCODE time, not at load time, and the
    package default is not ours to trust — leaving it implicit could run
    the whole 182-shard array on CPU of an H100 node, ~6x slower with
    correct numbers, which no golden gate would catch. Default 'GPU'
    because the fleet runs GPU nodes; CPU callers pass 'CPU' explicitly.

    We pass OUR sr straight through — the package resamples internally
    (scipy FFT). n_workers is pinned to 1: under Slurm the package would
    otherwise spawn one worker per core of the whole node.
    """
    if model is None:
        model = _load_birdnet_model()

    def encode(samples: np.ndarray, sr: int) -> np.ndarray:
        x = np.asarray(samples, dtype=np.float32).reshape(-1)
        try:
            result = model.encode_arrays([(x, sr)], n_workers=1, device=device)
        except Exception as exc:
            if device != "GPU":
                raise
            # birdnet's GPU backend asserts, in its WORKER process, that
            # exactly ONE GPU is visible to TF. Both known failure modes on
            # the fleet surface here, so name them instead of re-raising a
            # bare assertion from a subprocess:
            raise RuntimeError(
                "birdnet encode_arrays failed with device='GPU'. Two known "
                "causes on the fleet: (1) TF sees ZERO GPUs inside the "
                "container — check the job ran with --nv and --gres=gpu:1 "
                "and that tf.sif has the nvidia cuda wheels installed "
                "(a known tf.sif gap); (2) TF sees MORE THAN ONE GPU — "
                "birdnet asserts exactly 1 in its worker process; restrict "
                "with CUDA_VISIBLE_DEVICES set to a single index (Slurm's "
                "--gres=gpu:1 already does this). Original error: "
                f"{exc!r}"
            ) from exc
        emb = np.asarray(result.embeddings)[0]
        mask = np.asarray(result.embeddings_masked)[0]
        return _masked_mean(emb, mask)

    return encode


def _max_pool_birdnet_logits(predictions: np.ndarray) -> np.ndarray:
    """MAX over segments of (n_segments, n_classes) DENSE scores.

    BirdSet's own convention for recording-level BirdNET scores is
    max-pooling over segments — deliberately NOT the mean used for the
    embedding (and for Perch logits). The asymmetry is intentional and
    documented in the Task 6 plan: a bird audible in one 3 s segment of a
    long recording should dominate its recording-level score, and BirdSet's
    published numbers were computed under exactly this rule.

    The input must already be DENSE and class-aligned — column j is species
    j for every row (see _dense_birdnet_scores, which verifies/repairs the
    id alignment first). Max over a sorted-by-confidence matrix would be
    meaningless.

    Strict 2-D only: predict output whose shape surprises us is refused,
    never squeezed — a cited API claim is a hypothesis until measured.
    """
    p = np.asarray(predictions, dtype=np.float32)
    if p.ndim != 2:
        raise ValueError(
            f"unexpected birdnet predictions shape {p.shape}; expected "
            "(n_segments, n_classes) for ONE recording — refusing to guess "
            "axes on a surprise shape"
        )
    if p.shape[0] == 0:
        raise ValueError("birdnet returned zero prediction segments")
    return p.max(axis=0).astype(np.float32)


def _dense_birdnet_scores(
    species_probs: np.ndarray, species_ids: np.ndarray, n_species: int
) -> np.ndarray:
    """Dense class-aligned (n_segments, n_species) scores for ONE recording.

    MEASURED (birdnet 1.1.1 pb backend, fleet 2026-09-30): predict_arrays
    with top_k=None returns .species_probs (n_inputs, n_segments, k) with
    k == n_species (6522) — ALREADY dense — and .species_ids (same shape,
    uint16) OBSERVED to be the identity permutation 0..n_species-1 per
    segment. Observed is not guaranteed: identity is VERIFIED here, and when
    ids come back permuted (e.g. sorted by confidence, which top_k<full
    output does), scores are SCATTERED into a dense vector by their ids
    instead of trusted positionally — max over a confidence-sorted matrix
    would be garbage no downstream check could catch.

    The truncation guard: k < n_species means top_k was left at the package
    default (5) somewhere upstream. Pooling a truncated vector writes a
    cache that is silently wrong for n_species - k classes — REFUSED, never
    pooled. k > n_species is an id-space mismatch and equally refused.
    """
    probs = np.asarray(species_probs, dtype=np.float32)
    ids = np.asarray(species_ids)
    if probs.ndim != 2 or ids.shape != probs.shape:
        raise ValueError(
            f"unexpected birdnet species_probs/species_ids shapes "
            f"{probs.shape}/{ids.shape}; expected matching (n_segments, k) "
            "for ONE recording — refusing to guess axes on a surprise shape"
        )
    n_segments, k = probs.shape
    if n_segments == 0:
        raise ValueError("birdnet returned zero prediction segments")
    if k != n_species:
        raise ValueError(
            f"birdnet species_probs carry k={k} classes but the model has "
            f"n_species={n_species}: a k < n_species result means "
            "predict_arrays ran with a truncating top_k (the package "
            "default is 5) and max-pooling it would write a cache silently "
            "wrong for the other classes — call with top_k=None; refusing "
            "to pool a non-dense result"
        )
    ids = ids.astype(np.int64)
    if np.array_equal(
        ids, np.broadcast_to(np.arange(n_species, dtype=np.int64), ids.shape)
    ):
        # The measured dense case: column j IS species j. No scatter needed.
        return probs
    # Permuted ids: verify each segment's ids are a permutation of
    # 0..n_species-1 (a duplicate would silently drop a score in the
    # scatter), then place every score at its own class index.
    if not np.array_equal(
        np.sort(ids, axis=1),
        np.broadcast_to(np.arange(n_species, dtype=np.int64), ids.shape),
    ):
        raise ValueError(
            "birdnet species_ids are neither the identity permutation nor a "
            "per-segment permutation of 0..n_species-1; scattering would "
            "drop or misplace scores — refusing to build a dense vector "
            "from inconsistent ids"
        )
    dense = np.empty((n_segments, n_species), dtype=np.float32)
    dense[np.arange(n_segments)[:, None], ids] = probs
    return dense


def make_birdnet_encoder_with_logits(model=None, device: str = "GPU") -> EncoderWithLogits:
    """Build the BirdNET V2.4 encoder that also returns max-pooled scores.

    `encode_arrays` does NOT return scores, so the posterior comes from
    `predict_arrays`. MEASURED (birdnet 1.1.1, pb backend, fleet
    2026-09-30) — replacing this wrapper's earlier mock-pinned
    `.predictions` contract, which DOES NOT EXIST on the real package:

        predict_arrays(inp, /, *, top_k=5, n_workers=None, batch_size=1,
                       device='CPU', apply_sigmoid=True,
                       sigmoid_sensitivity=1.0,
                       default_confidence_threshold=0.1,
                       apply_softmax=False, ...)

    returning a result with `.species_probs` (n_inputs, n_segments, k)
    float32, `.species_ids` (same shape) uint16, and `.species_list` /
    `.n_species` (6522). Called here with:

    - top_k=None — MEASURED to return the FULL n_species-wide vector
      (species_probs (1, 3, 6522)); the default top_k=5 TRUNCATES, and
      _dense_birdnet_scores refuses any k < n_species result.
    - apply_sigmoid=True — BirdNET's head emits per-class sigmoid
      PROBABILITIES in [0, 1], NOT raw logits. The column concept is
      'scores': the cache stores what the official tooling emits.
    - default_confidence_threshold=None — asking for NO thresholding.
      LOUD CAVEAT: None was probed only against the mock, not the real
      call; birdnet 1.1.1's signature defaults it to 0.1 (float). If the
      real package rejects None, the Task 6.4 pilot fails RIGHT HERE with
      birdnet's own TypeError/ValueError — adapt the wrapper then; do NOT
      pre-emptively pass a magic float that silently zeroes sub-threshold
      scores.
    - n_workers=1 (Slurm: never one worker per core), device forwarded
      exactly like encode_arrays.

    Pooling is MAX over segments (BirdSet's convention) on the DENSE
    class-aligned scores; _dense_birdnet_scores verifies species_ids are
    the identity permutation and scatters by id when they are not.

    Embeddings come from the same masked-mean path as make_birdnet_encoder.
    """
    if model is None:
        model = _load_birdnet_model()
    embed_only = make_birdnet_encoder(model=model, device=device)

    def encode2(samples: np.ndarray, sr: int) -> tuple[np.ndarray, np.ndarray]:
        emb = embed_only(samples, sr)
        if not hasattr(model, "predict_arrays"):
            raise NotImplementedError(
                "birdnet scores require the model to expose "
                "predict_arrays(inp, /, *, top_k=None, apply_sigmoid=True, "
                "n_workers=1, device=...) returning a result with "
                ".species_probs and .species_ids of shape "
                "(1, n_segments, k) plus .n_species; this birdnet build "
                f"does not have it (model type: {type(model).__name__})."
            )
        x = np.asarray(samples, dtype=np.float32).reshape(-1)
        result = model.predict_arrays(
            [(x, sr)],
            top_k=None,
            apply_sigmoid=True,
            default_confidence_threshold=None,
            n_workers=1,
            device=device,
        )
        species_probs = getattr(result, "species_probs", None)
        species_ids = getattr(result, "species_ids", None)
        if species_probs is None or species_ids is None:
            raise NotImplementedError(
                "birdnet predict_arrays returned an object without "
                ".species_probs/.species_ids "
                f"(got {type(result).__name__}); the wrapper needs the "
                "measured 1.1.1 pb result shape (1, n_segments, k) — "
                "re-measure the API on the cluster and adapt, do NOT guess."
            )
        n_species = getattr(result, "n_species", None)
        if n_species is None:
            species_list = getattr(result, "species_list", None)
            if species_list is None:
                raise NotImplementedError(
                    "birdnet predict_arrays result exposes neither "
                    ".n_species nor .species_list; without the class count "
                    "the k<n_species truncation guard cannot run — "
                    "re-measure the API, do NOT pool unguarded."
                )
            n_species = len(species_list)
        probs = np.asarray(species_probs, dtype=np.float32)
        ids = np.asarray(species_ids)
        if probs.ndim != 3 or probs.shape[0] != 1 or ids.shape != probs.shape:
            raise ValueError(
                f"unexpected birdnet species_probs/species_ids shapes "
                f"{probs.shape}/{ids.shape}; expected matching "
                "(1, n_segments, k) from predict_arrays of ONE recording — "
                "refusing to guess axes on a surprise shape"
            )
        dense = _dense_birdnet_scores(probs[0], ids[0], int(n_species))
        return emb, _max_pool_birdnet_logits(dense)

    return encode2


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_ENCODER_FACTORIES: dict[str, Callable[[], Encoder]] = {
    "melsummary": lambda: _mean_log_mel,
    "perch": make_perch_encoder,
    "birdnet": make_birdnet_encoder,
}


def _melsummary_with_logits(samples: np.ndarray, sr: int) -> tuple[np.ndarray, None]:
    """melsummary has no classifier head: logits are None, honestly."""
    return _mean_log_mel(samples, sr), None


_ENCODER2_FACTORIES: dict[str, Callable[[], EncoderWithLogits]] = {
    "melsummary": lambda: _melsummary_with_logits,
    "perch": make_perch_encoder_with_logits,
    "birdnet": make_birdnet_encoder_with_logits,
}


def load_encoder(name: str) -> Encoder:
    """Return the named frozen encoder.

    `perch` and `birdnet` lazy-import their runtimes here and raise
    ImportError with install instructions when absent — failing at load time,
    before any audio is read, rather than silently substituting a stand-in:
    an embedding cache built with the wrong encoder is indistinguishable from
    a correct one.
    """
    if name not in _ENCODER_FACTORIES:
        raise ValueError(f"unknown encoder {name!r}; have {sorted(_ENCODER_FACTORIES)}")
    return _ENCODER_FACTORIES[name]()


def load_encoder_with_logits(name: str) -> EncoderWithLogits:
    """Return the named encoder in (embedding, logits_or_None) form.

    Same loud-failure rules as load_encoder. melsummary returns (emb, None)
    — no classifier head, no fabricated posterior. perch and birdnet
    lazy-import their runtimes and raise ImportError with install
    instructions when absent.
    """
    if name not in _ENCODER2_FACTORIES:
        raise ValueError(
            f"unknown encoder {name!r}; have {sorted(_ENCODER2_FACTORIES)}"
        )
    return _ENCODER2_FACTORIES[name]()
