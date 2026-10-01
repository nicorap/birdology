"""FiLM wrappers around unmodified HF checkpoints — identity-gated at step 0.

THE GATE IS THE DESIGN (spec §1, FiLM paragraph)
------------------------------------------------
Per-site ``(γ, β) = Linear(d_ctx, 2*C)(context)`` with the head's weight AND
bias zero-initialized and the scale computed as ``γ = 1 + γ_raw``. At step 0
``γ_raw ≡ 0``, so every site applies ``h * 1.0 + 0.0`` — IEEE-exact in
float32 (multiplying by exactly 1.0 and adding exactly 0.0 never reorders or
perturbs the value), which is why the executable gate demands ``torch.equal``
and not ``allclose``. Anything that breaks bit-exactness here (an extra op, a
re-normalization, a dtype round-trip) is a real conditioning leak, not noise.

THE NULL PATH IS THE SAME GATE, PER ROW
---------------------------------------
``present`` (B, 1) multiplies ``γ_raw`` and ``β_raw`` BEFORE the ``1 +``:
a flag-0 row sees γ=1, β=0 — bit-exact identity — even after the heads have
trained. Missing context means "the model is told nothing", never a
fabricated prior (context.py's contract, carried through).

THE WRAPPED MODEL IS NEVER MUTATED
----------------------------------
FiLM heads live on the WRAPPER (``self.heads``); forward hooks are
registered at the start of ``forward`` and removed in a ``finally`` — the
inner model called directly (the twin / repro-gate path) is byte-for-byte
the checkpoint, hooks inert, module tree and state_dict untouched. Backbone
fine-tuning is an optimizer param-group decision made by the trainer, never
a side effect of wrapping.

INJECTION SITES — MEASURED, NOT TRUSTED (transformers 5.18)
-----------------------------------------------------------
The plan guessed ``model.audio_spectrogram_transformer.encoder.layer[i]``;
the real 5.x tree has NO ``encoder`` under ``ASTModel``:

* ConvNext: ``model.convnext.encoder.stages[i]`` — output (B, C_i, H, W),
  one site per stage (``len(config.hidden_sizes)``: 4 on the real XCL
  checkpoint, 2 on the tiny test config — counted from config, never
  hardcoded).
* AST: ``model.audio_spectrogram_transformer.layers[i]`` — output (B, S, H),
  sites at ``range(0, num_hidden_layers, 3)`` (every 3rd block; the tiny
  2-layer model gets exactly one site, at layer 0).
"""
from __future__ import annotations

import torch
from torch import nn

from birdology.phase_c.context import ContextEncoder


def _zero_init(head: nn.Linear) -> nn.Linear:
    """Weight AND bias to exact zero — γ_raw ≡ 0, β ≡ 0 at step 0."""
    nn.init.zeros_(head.weight)
    nn.init.zeros_(head.bias)
    return head


class _FiLMBase(nn.Module):
    """Shared machinery: zero-init heads, hook-scoped injection, null path."""

    def __init__(
        self,
        model: nn.Module,
        context_encoder: ContextEncoder,
        channels: list[int],
    ) -> None:
        super().__init__()
        self.model = model
        self.context_encoder = context_encoder
        d_ctx = context_encoder.mlp[-1].out_features
        self.heads = nn.ModuleList(
            _zero_init(nn.Linear(d_ctx, 2 * c)) for c in channels
        )

    # -- subclass contract -------------------------------------------------
    def _sites(self) -> list[nn.Module]:  # pragma: no cover - abstract
        raise NotImplementedError

    def _model_forward(self, inputs: torch.Tensor) -> torch.Tensor:  # pragma: no cover
        raise NotImplementedError

    @staticmethod
    def _broadcast(g: torch.Tensor, hidden: torch.Tensor) -> torch.Tensor:  # pragma: no cover
        raise NotImplementedError

    # -----------------------------------------------------------------------
    def forward(
        self,
        inputs: torch.Tensor,
        context: torch.Tensor,
        present: torch.Tensor,
    ) -> torch.Tensor:
        """``(inputs, context (B, n_classes), present (B, 1)) -> logits``."""
        ctx = self.context_encoder(context, present)  # (B, d_ctx)

        handles = []
        try:
            for site, head in zip(self._sites(), self.heads):
                gamma_raw, beta_raw = head(ctx).chunk(2, dim=-1)  # (B, C) each
                # present gates BEFORE the `1 +`: flag-0 rows get γ=1, β=0
                # exactly — the per-row identity that keeps the null path
                # bit-equal to the bare checkpoint even with trained heads.
                gamma = 1.0 + gamma_raw * present
                beta = beta_raw * present
                handles.append(
                    site.register_forward_hook(self._make_hook(gamma, beta))
                )
            return self._model_forward(inputs)
        finally:
            # Hooks never outlive this forward: the inner model called
            # directly remains the unmodified checkpoint.
            for handle in handles:
                handle.remove()

    def _make_hook(self, gamma: torch.Tensor, beta: torch.Tensor):
        def hook(module, args, output):
            hidden = output[0] if isinstance(output, tuple) else output
            filmed = hidden * self._broadcast(gamma, hidden) + self._broadcast(
                beta, hidden
            )
            if isinstance(output, tuple):
                return (filmed,) + output[1:]
            return filmed

        return hook


class FiLMConvNext(_FiLMBase):
    """Channel-wise FiLM after every ConvNext stage."""

    def __init__(self, model: nn.Module, context_encoder: ContextEncoder) -> None:
        super().__init__(model, context_encoder, list(model.config.hidden_sizes))

    def _sites(self) -> list[nn.Module]:
        return list(self.model.convnext.encoder.stages)

    def _model_forward(self, inputs: torch.Tensor) -> torch.Tensor:
        return self.model(pixel_values=inputs).logits

    @staticmethod
    def _broadcast(g: torch.Tensor, hidden: torch.Tensor) -> torch.Tensor:
        # stage output is (B, C, H, W): scale/shift per channel.
        return g.unsqueeze(-1).unsqueeze(-1)


class FiLMAST(_FiLMBase):
    """Hidden-dim FiLM after every 3rd AST transformer block."""

    def __init__(self, model: nn.Module, context_encoder: ContextEncoder) -> None:
        n_layers = model.config.num_hidden_layers
        self._site_indices = list(range(0, n_layers, 3))
        super().__init__(
            model,
            context_encoder,
            [model.config.hidden_size] * len(self._site_indices),
        )

    def _sites(self) -> list[nn.Module]:
        layers = self.model.audio_spectrogram_transformer.layers
        return [layers[i] for i in self._site_indices]

    def _model_forward(self, inputs: torch.Tensor) -> torch.Tensor:
        return self.model(input_values=inputs).logits

    @staticmethod
    def _broadcast(g: torch.Tensor, hidden: torch.Tensor) -> torch.Tensor:
        # block output is (B, S, H): scale/shift per hidden channel,
        # broadcast across the sequence.
        return g.unsqueeze(1)
