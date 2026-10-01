"""FiLM wrappers — the step-0 identity gate is EXECUTABLE, not an intention.

The contract under test (plan Task 2, spec §1 FiLM paragraph):

* ``FiLMConvNext(model, context_encoder)`` / ``FiLMAST(model, context_encoder)``,
  both ``forward(inputs, context, present) -> logits``.
* Per-site ``Linear(d_ctx, 2*C)`` heads, weight AND bias zero-init, with
  ``γ = 1 + γ_raw`` — so at step 0 the wrapped model reproduces the wrapped
  checkpoint bit-for-bit (``torch.equal``, never allclose).
* The gate must actually bite: a mutation test randomizes a head and asserts
  the gate FAILS — a gate that passes under mutation tests nothing.
* ``present == 0`` forces identity per-row even after heads have trained.
* The wrapper never mutates the wrapped model's own modules: inner
  ``state_dict`` bit-identical after a training step on the wrapper when the
  optimizer covers only the FiLM heads; backbone training is an optimizer
  param-group decision, never a side effect of wrapping.

MEASURED module trees (transformers 5.18, tiny random configs — the plan's
``encoder.layer`` path for AST was a guess; the real tree has no ``encoder``):

* ConvNext: ``model.convnext.encoder.stages[i]`` → Tensor (B, C_i, H, W),
  C_i = config.hidden_sizes[i].
* AST: ``model.audio_spectrogram_transformer.layers[i]`` → Tensor (B, S, H),
  H = config.hidden_size.

All models here are tiny and randomly initialized from configs — no network,
no HF downloads (Global Constraints).
"""
from __future__ import annotations

import pytest
import torch
from torch import nn
from transformers import (
    ASTConfig,
    ASTForAudioClassification,
    ConvNextConfig,
    ConvNextForImageClassification,
)

from birdology.phase_c.context import ContextEncoder
from birdology.phase_c.film import FiLMAST, FiLMConvNext

N_CLASSES = 5
D_CTX = 8


def tiny_convnext() -> ConvNextForImageClassification:
    config = ConvNextConfig(
        num_channels=1,
        num_stages=2,
        hidden_sizes=[8, 16],
        depths=[1, 1],
        num_labels=N_CLASSES,
        image_size=32,
    )
    return ConvNextForImageClassification(config)


def tiny_ast(n_layers: int = 2) -> ASTForAudioClassification:
    config = ASTConfig(
        hidden_size=16,
        num_hidden_layers=n_layers,
        num_attention_heads=2,
        intermediate_size=32,
        num_labels=N_CLASSES,
        max_length=24,
        num_mel_bins=16,
    )
    return ASTForAudioClassification(config)


def convnext_inputs(batch: int = 2) -> torch.Tensor:
    return torch.randn(batch, 1, 32, 32)


def ast_inputs(batch: int = 2) -> torch.Tensor:
    return torch.randn(batch, 24, 16)


def context_batch(batch: int = 2) -> tuple[torch.Tensor, torch.Tensor]:
    return torch.randn(batch, N_CLASSES), torch.ones(batch, 1)


def _cases() -> list:
    """(name, build_model, build_inputs, bare_forward) for both backbones."""
    return [
        pytest.param(
            tiny_convnext,
            convnext_inputs,
            lambda m, x: m(pixel_values=x).logits,
            FiLMConvNext,
            id="convnext",
        ),
        pytest.param(
            tiny_ast,
            ast_inputs,
            lambda m, x: m(input_values=x).logits,
            FiLMAST,
            id="ast",
        ),
    ]


@pytest.mark.parametrize("build_model,build_inputs,bare_forward,wrapper_cls", _cases())
def test_identity_gate_bit_exact_at_init(
    build_model, build_inputs, bare_forward, wrapper_cls
):
    """At step 0 the wrapped model equals the unwrapped one to torch.equal."""
    torch.manual_seed(0)
    model = build_model()
    model.eval()
    encoder = ContextEncoder(N_CLASSES, d_ctx=D_CTX)
    wrapped = wrapper_cls(model, encoder)
    wrapped.eval()

    x = build_inputs()
    context, present = context_batch()

    with torch.no_grad():
        bare = bare_forward(model, x)
        conditioned = wrapped(x, context, present)

    assert torch.equal(conditioned, bare), (
        "FiLM wrapper is NOT identity at init — the step-0 gate is broken"
    )


@pytest.mark.parametrize("build_model,build_inputs,bare_forward,wrapper_cls", _cases())
def test_gate_fails_when_gamma_heads_randomized(
    build_model, build_inputs, bare_forward, wrapper_cls
):
    """Mutation pinned as a test: randomize a head → gate MUST fail.

    A gate that still passes after ``nn.init.normal_`` on a γ/β head is a
    gate that tests nothing.
    """
    torch.manual_seed(0)
    model = build_model()
    model.eval()
    encoder = ContextEncoder(N_CLASSES, d_ctx=D_CTX)
    wrapped = wrapper_cls(model, encoder)
    wrapped.eval()

    with torch.no_grad():
        for head in wrapped.heads:
            nn.init.normal_(head.weight, std=1.0)
            nn.init.normal_(head.bias, std=1.0)

    x = build_inputs()
    context, present = context_batch()

    with torch.no_grad():
        bare = bare_forward(model, x)
        conditioned = wrapped(x, context, present)

    assert not torch.equal(conditioned, bare), (
        "randomized FiLM heads still produced bit-identical output — the "
        "identity gate cannot detect a broken init, so it gates nothing"
    )


@pytest.mark.parametrize("build_model,build_inputs,bare_forward,wrapper_cls", _cases())
def test_gradients_flow_to_heads_and_backbone(
    build_model, build_inputs, bare_forward, wrapper_cls
):
    torch.manual_seed(0)
    model = build_model()
    encoder = ContextEncoder(N_CLASSES, d_ctx=D_CTX)
    wrapped = wrapper_cls(model, encoder)
    wrapped.train()

    x = build_inputs()
    context, present = context_batch()

    logits = wrapped(x, context, present)
    loss = logits.sum()
    loss.backward()

    for i, head in enumerate(wrapped.heads):
        assert head.weight.grad is not None, f"no grad on FiLM head {i} weight"
        assert head.weight.grad.abs().sum() > 0, (
            f"FiLM head {i} weight grad is identically zero — the head is "
            "not on the computation path"
        )
        assert head.bias.grad is not None, f"no grad on FiLM head {i} bias"
        assert head.bias.grad.abs().sum() > 0, f"FiLM head {i} bias grad all-zero"

    backbone_grads = [
        p.grad for p in model.parameters() if p.grad is not None and p.requires_grad
    ]
    assert backbone_grads, "no backbone parameter received a gradient"
    assert any(g.abs().sum() > 0 for g in backbone_grads), (
        "every backbone gradient is identically zero"
    )


@pytest.mark.parametrize("build_model,build_inputs,bare_forward,wrapper_cls", _cases())
def test_flag_zero_forces_identity_after_training(
    build_model, build_inputs, bare_forward, wrapper_cls
):
    """present=0 is the null path: identity even with trained (random) heads.

    Mixed batch: the flag-0 row must be bit-equal to the bare model while the
    flag-1 row differs — per-row routing, not per-batch.
    """
    torch.manual_seed(0)
    model = build_model()
    model.eval()
    encoder = ContextEncoder(N_CLASSES, d_ctx=D_CTX)
    wrapped = wrapper_cls(model, encoder)
    wrapped.eval()

    # Simulate trained heads: anything-but-zero.
    with torch.no_grad():
        for head in wrapped.heads:
            nn.init.normal_(head.weight, std=1.0)
            nn.init.normal_(head.bias, std=1.0)

    x = build_inputs(batch=2)
    context = torch.randn(2, N_CLASSES)
    present = torch.tensor([[1.0], [0.0]])

    with torch.no_grad():
        bare = bare_forward(model, x)
        conditioned = wrapped(x, context, present)

    assert torch.equal(conditioned[1], bare[1]), (
        "flag=0 row is not bit-identical to the bare model — the null path "
        "fabricates conditioning where the provider said 'no context'"
    )
    assert not torch.equal(conditioned[0], bare[0]), (
        "flag=1 row with trained heads equals the bare model — FiLM is inert"
    )


@pytest.mark.parametrize("build_model,build_inputs,bare_forward,wrapper_cls", _cases())
def test_wrapper_never_mutates_inner_model(
    build_model, build_inputs, bare_forward, wrapper_cls
):
    """Wrapping adds nothing to the inner model; a head-only training step
    leaves the inner state_dict bit-identical. Backbone training is an
    optimizer param-group decision, never a wrapping side effect."""
    torch.manual_seed(0)
    model = build_model()
    modules_before = [name for name, _ in model.named_modules()]
    params_before = [name for name, _ in model.named_parameters()]
    state_before = {k: v.clone() for k, v in model.state_dict().items()}

    encoder = ContextEncoder(N_CLASSES, d_ctx=D_CTX)
    wrapped = wrapper_cls(model, encoder)

    assert [name for name, _ in model.named_modules()] == modules_before, (
        "wrapping injected modules INTO the wrapped model"
    )
    assert [name for name, _ in model.named_parameters()] == params_before, (
        "wrapping injected parameters INTO the wrapped model"
    )

    # One real training step on the FiLM heads only.
    head_params = [p for head in wrapped.heads for p in head.parameters()]
    opt = torch.optim.SGD(head_params, lr=0.1)
    wrapped.train()
    x = build_inputs()
    context, present = context_batch()
    loss = wrapped(x, context, present).sum()
    loss.backward()
    opt.step()

    state_after = model.state_dict()
    assert state_before.keys() == state_after.keys()
    for key in state_before:
        assert torch.equal(state_before[key], state_after[key]), (
            f"inner model weight {key!r} changed after a head-only step — "
            "the wrapper leaked its training into the wrapped checkpoint"
        )


def test_convnext_site_count_tracks_stages():
    """One head per ConvNext stage — len(hidden_sizes), never hardcoded 4."""
    torch.manual_seed(0)
    model = tiny_convnext()
    wrapped = FiLMConvNext(model, ContextEncoder(N_CLASSES, d_ctx=D_CTX))
    n_stages = len(model.config.hidden_sizes)
    assert len(wrapped.heads) == n_stages
    for i, head in enumerate(wrapped.heads):
        assert head.out_features == 2 * model.config.hidden_sizes[i], (
            f"head {i} is not Linear(d_ctx, 2*C_stage)"
        )


@pytest.mark.parametrize("n_layers,expected_sites", [(2, 1), (4, 2), (6, 2)])
def test_ast_site_count_is_every_third_block(n_layers, expected_sites):
    """AST sites at range(0, n_layers, 3) — from config, never hardcoded."""
    torch.manual_seed(0)
    model = tiny_ast(n_layers=n_layers)
    wrapped = FiLMAST(model, ContextEncoder(N_CLASSES, d_ctx=D_CTX))
    assert len(wrapped.heads) == expected_sites
    assert len(range(0, n_layers, 3)) == expected_sites  # the rule itself
    for head in wrapped.heads:
        assert head.out_features == 2 * model.config.hidden_size


def test_inner_model_still_callable_directly():
    """Hooks must be inert when the inner model is called WITHOUT the wrapper
    (the twin / repro-gate path calls checkpoints bare)."""
    torch.manual_seed(0)
    model = tiny_convnext()
    model.eval()
    x = convnext_inputs()
    with torch.no_grad():
        before = model(pixel_values=x).logits

    wrapped = FiLMConvNext(model, ContextEncoder(N_CLASSES, d_ctx=D_CTX))
    with torch.no_grad():
        for head in wrapped.heads:
            nn.init.normal_(head.weight, std=1.0)
            nn.init.normal_(head.bias, std=1.0)

    with torch.no_grad():
        after = model(pixel_values=x).logits

    assert torch.equal(before, after), (
        "calling the inner model directly after wrapping changed its output — "
        "hooks fire outside wrapper.forward"
    )
