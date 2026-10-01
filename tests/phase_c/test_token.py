"""Context-token wrapper for AST — one token, per-row null routing, no gate.

The contract under test (plan Task 3, spec §1 context-token paragraph):

* ``TokenAST(model, context_encoder)`` — ``forward(inputs, context, present)
  -> logits``; projects d_ctx → hidden_size and inserts EXACTLY ONE token
  into the embedding output at index 2, after the CLS and distillation
  tokens.
* ``null_token`` is a learned ``nn.Parameter(hidden_size)`` substituted
  PER ROW when ``present == 0`` — mixed batches route row-by-row, and the
  gradient reaches ``null_token`` only from flag-0 rows.
* The wrapped model is never mutated (Task 2's discipline): no injected
  modules/params, inner ``state_dict`` bit-identical after a wrapper-only
  training step, inner model callable directly with hooks inert.
* There is NO identity gate — prepended tokens renormalize attention, so
  no bit-exact path exists (spec §1 / Global Constraints: "its honesty is
  the twin"). Instead the step-0 delta is pinned nonzero AND bounded.

MEASURED module tree (transformers 5.18, tiny random config — the plan's
``encoder.layer`` path was stale): ``audio_spectrogram_transformer.embeddings``
→ (B, S, H) with CLS at 0 and distillation at 1; blocks live at
``audio_spectrogram_transformer.layers[i]``; ``ASTModel.forward`` builds the
attention mask AFTER embeddings and pools indices 0/1 only, so an inserted
token at index 2 perturbs neither masking nor the pooling indices.

All models are tiny and randomly initialized from configs — no network, no
HF downloads (Global Constraints).
"""
from __future__ import annotations

import torch
from torch import nn
from transformers import ASTConfig, ASTForAudioClassification

from birdology.phase_c.context import ContextEncoder
from birdology.phase_c.token import TokenAST

N_CLASSES = 5
D_CTX = 8
HIDDEN = 16


def tiny_ast() -> ASTForAudioClassification:
    config = ASTConfig(
        hidden_size=HIDDEN,
        num_hidden_layers=2,
        num_attention_heads=2,
        intermediate_size=32,
        num_labels=N_CLASSES,
        max_length=24,
        num_mel_bins=16,
    )
    return ASTForAudioClassification(config)


def ast_inputs(batch: int = 2) -> torch.Tensor:
    return torch.randn(batch, 24, 16)


def make_wrapped(seed: int = 0) -> tuple[ASTForAudioClassification, TokenAST]:
    torch.manual_seed(seed)
    model = tiny_ast()
    model.eval()
    encoder = ContextEncoder(N_CLASSES, d_ctx=D_CTX)
    wrapped = TokenAST(model, encoder)
    wrapped.eval()
    return model, wrapped


def _capture_encoder_input(model: ASTForAudioClassification, store: list):
    """Pre-hook on the first transformer block: records the sequence it sees."""
    layer0 = model.audio_spectrogram_transformer.layers[0]

    def pre_hook(module, args, kwargs):
        store.append(args[0].detach().clone())

    return layer0.register_forward_pre_hook(pre_hook, with_kwargs=True)


def test_exactly_one_token_added_to_encoder_input():
    """The first block sees S+1 tokens under the wrapper, S bare — exactly one."""
    model, wrapped = make_wrapped()
    x = ast_inputs()
    context = torch.randn(2, N_CLASSES)
    present = torch.ones(2, 1)

    seen: list[torch.Tensor] = []
    handle = _capture_encoder_input(model, seen)
    try:
        with torch.no_grad():
            model(input_values=x)
            wrapped(x, context, present)
    finally:
        handle.remove()

    bare_seq, wrapped_seq = seen[0], seen[1]
    assert wrapped_seq.shape[1] == bare_seq.shape[1] + 1, (
        f"wrapper changed token count by {wrapped_seq.shape[1] - bare_seq.shape[1]}"
        " — the contract is EXACTLY one added token"
    )


def test_token_sits_at_index_2_after_cls_and_distillation():
    """CLS/distill (0,1) untouched; bare patches shift right by one from 2."""
    model, wrapped = make_wrapped()
    x = ast_inputs()
    context = torch.randn(2, N_CLASSES)
    present = torch.ones(2, 1)

    seen: list[torch.Tensor] = []
    handle = _capture_encoder_input(model, seen)
    try:
        with torch.no_grad():
            model(input_values=x)
            wrapped(x, context, present)
    finally:
        handle.remove()

    bare_seq, wrapped_seq = seen[0], seen[1]
    assert torch.equal(wrapped_seq[:, :2], bare_seq[:, :2]), (
        "CLS/distillation tokens (indices 0,1) were disturbed — the context "
        "token must be INSERTED at index 2, not prepended before them"
    )
    assert torch.equal(wrapped_seq[:, 3:], bare_seq[:, 2:]), (
        "patch tokens did not shift right by exactly one from index 2 — the "
        "token is not at index 2"
    )
    with torch.no_grad():
        expected = wrapped.project(wrapped.context_encoder(context, present))
    assert torch.equal(wrapped_seq[:, 2], expected), (
        "the inserted token at index 2 is not the projected context vector"
    )


def test_present_rows_use_context_absent_rows_use_null_token():
    """flag=1: two distinct contexts → different logits. flag=0: identical."""
    model, wrapped = make_wrapped()
    x = ast_inputs()
    ctx_a = torch.randn(2, N_CLASSES)
    ctx_b = torch.randn(2, N_CLASSES)

    ones = torch.ones(2, 1)
    zeros = torch.zeros(2, 1)
    with torch.no_grad():
        present_a = wrapped(x, ctx_a, ones)
        present_b = wrapped(x, ctx_b, ones)
        absent_a = wrapped(x, ctx_a, zeros)
        absent_b = wrapped(x, ctx_b, zeros)

    assert not torch.equal(present_a, present_b), (
        "two distinct contexts with flag=1 produced identical logits — the "
        "context token carries no information"
    )
    assert torch.equal(absent_a, absent_b), (
        "flag=0 output depends on the context vector — absent context must "
        "route through the learned null token, never a fabricated prior"
    )


def test_mixed_batch_routes_per_row():
    """Flags (1, 0) in ONE batch: row 0 tracks the context, row 1 does not.

    Rows of a batch are independent through attention, so each mixed-batch
    row must bit-equal the same row from an all-same-flag batch.
    """
    model, wrapped = make_wrapped()
    x = ast_inputs()
    ctx = torch.randn(2, N_CLASSES)
    mixed = torch.tensor([[1.0], [0.0]])

    with torch.no_grad():
        out_mixed = wrapped(x, ctx, mixed)
        out_all1 = wrapped(x, ctx, torch.ones(2, 1))
        out_all0 = wrapped(x, ctx, torch.zeros(2, 1))

    assert torch.equal(out_mixed[0], out_all1[0]), (
        "flag-1 row in a mixed batch differs from the all-flag-1 batch — "
        "routing is per-batch, not per-row"
    )
    assert torch.equal(out_mixed[1], out_all0[1]), (
        "flag-0 row in a mixed batch differs from the all-flag-0 batch — "
        "the null token is not routed per-row"
    )
    assert not torch.equal(out_mixed[0], out_all0[0]), (
        "flag-1 row equals the null-token output — flags are ignored"
    )


def test_gradient_reaches_null_token_only_from_flag0_rows():
    """All-flag-1 batch: null_token.grad None or zero. Flag-0 present: nonzero."""
    model, wrapped = make_wrapped()
    wrapped.train()
    x = ast_inputs()
    ctx = torch.randn(2, N_CLASSES)

    wrapped(x, ctx, torch.ones(2, 1)).sum().backward()
    grad = wrapped.null_token.grad
    assert grad is None or torch.equal(grad, torch.zeros_like(grad)), (
        "an all-flag-1 batch sent gradient into null_token — the null path "
        "is leaking into rows that HAVE context"
    )

    wrapped.zero_grad(set_to_none=True)
    wrapped(x, ctx, torch.tensor([[1.0], [0.0]])).sum().backward()
    grad = wrapped.null_token.grad
    assert grad is not None and grad.abs().sum() > 0, (
        "a flag-0 row sent no gradient into null_token — the null token "
        "cannot be learned"
    )


def test_step0_delta_nonzero_and_bounded():
    """NO identity gate — the twin is the honesty mechanism (spec §1).

    A prepended token renormalizes every attention row, so no bit-exact
    identity path exists for the token arm (Global Constraints: "The token
    arm has NO identity gate … its honesty is the twin" — every conditioned
    run is compared against an unconditioned twin with identical schedule
    and seed). What we CAN pin at step 0 is that the perturbation is real
    but tame: logits differ from the bare model, and the max absolute delta
    at random init stays under 1.0 — a wild delta would mean the token
    injection is broken, not merely un-gated.
    """
    model, wrapped = make_wrapped()
    x = ast_inputs()
    ctx = torch.randn(2, N_CLASSES)

    with torch.no_grad():
        bare = model(input_values=x).logits
        for present in (torch.ones(2, 1), torch.zeros(2, 1)):
            conditioned = wrapped(x, ctx, present)
            assert not torch.equal(conditioned, bare), (
                "token arm output equals the bare model — the token was "
                "never injected (and a bit-exact match is physically "
                "impossible with an extra attention token)"
            )
            delta = (conditioned - bare).abs().max().item()
            assert delta < 1.0, (
                f"step-0 max|Δlogits| = {delta:.3f} ≥ 1.0 — the injected "
                "token swamps the checkpoint at init instead of perturbing it"
            )


def test_wrapper_never_mutates_inner_model():
    """Task 2e's discipline, verbatim: wrapping adds nothing to the inner
    model, and a wrapper-only training step leaves its state_dict
    bit-identical."""
    torch.manual_seed(0)
    model = tiny_ast()
    modules_before = [name for name, _ in model.named_modules()]
    params_before = [name for name, _ in model.named_parameters()]
    state_before = {k: v.clone() for k, v in model.state_dict().items()}

    encoder = ContextEncoder(N_CLASSES, d_ctx=D_CTX)
    wrapped = TokenAST(model, encoder)

    assert [name for name, _ in model.named_modules()] == modules_before, (
        "wrapping injected modules INTO the wrapped model"
    )
    assert [name for name, _ in model.named_parameters()] == params_before, (
        "wrapping injected parameters INTO the wrapped model"
    )

    # One real training step on the wrapper's own params only.
    own_params = [wrapped.null_token] + list(wrapped.project.parameters())
    opt = torch.optim.SGD(own_params, lr=0.1)
    wrapped.train()
    x = ast_inputs()
    ctx = torch.randn(2, N_CLASSES)
    present = torch.tensor([[1.0], [0.0]])  # touch both paths
    wrapped(x, ctx, present).sum().backward()
    opt.step()

    state_after = model.state_dict()
    assert state_before.keys() == state_after.keys()
    for key in state_before:
        assert torch.equal(state_before[key], state_after[key]), (
            f"inner model weight {key!r} changed after a wrapper-only step — "
            "the wrapper leaked its training into the wrapped checkpoint"
        )


def test_inner_model_still_callable_directly():
    """Hooks must be inert outside wrapper.forward (the twin path calls the
    checkpoint bare)."""
    torch.manual_seed(0)
    model = tiny_ast()
    model.eval()
    x = ast_inputs()
    with torch.no_grad():
        before = model(input_values=x).logits

    wrapped = TokenAST(model, ContextEncoder(N_CLASSES, d_ctx=D_CTX))
    with torch.no_grad():
        nn.init.normal_(wrapped.project.weight, std=1.0)
        nn.init.normal_(wrapped.null_token, std=1.0)
        after_wrap = model(input_values=x).logits

    assert torch.equal(before, after_wrap), (
        "calling the inner model directly after wrapping changed its output — "
        "hooks fire outside wrapper.forward"
    )


def test_gradients_flow_to_projection_and_backbone():
    model, wrapped = make_wrapped()
    wrapped.train()
    x = ast_inputs()
    ctx = torch.randn(2, N_CLASSES)
    wrapped(x, ctx, torch.ones(2, 1)).sum().backward()

    assert wrapped.project.weight.grad is not None
    assert wrapped.project.weight.grad.abs().sum() > 0, (
        "projection weight grad identically zero — the context token is not "
        "on the computation path"
    )
    backbone_grads = [
        p.grad for p in model.parameters() if p.grad is not None and p.requires_grad
    ]
    assert backbone_grads and any(g.abs().sum() > 0 for g in backbone_grads), (
        "no backbone gradient — the wrapper detached the checkpoint"
    )
