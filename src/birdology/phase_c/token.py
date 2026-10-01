"""Context token for AST — one learned token, a learned null, NO gate.

THE TOKEN GOES AT INDEX 2, AFTER CLS AND DISTILLATION (spec §1)
---------------------------------------------------------------
AST's embedding output is ``[CLS, distill, patch_0 … patch_{n-1}]``; the
projected context vector is INSERTED at index 2 so both special tokens keep
their positions — ``ASTModel`` pools ``(seq[:, 0] + seq[:, 1]) / 2``, so
moving either would silently change what the classifier head reads.

MEASURED injection point (transformers 5.18 — the plan's ``encoder.layer``
path was stale): the embedding module is
``model.audio_spectrogram_transformer.embeddings``; ``ASTModel.forward``
builds its attention mask AFTER embeddings from the embedded sequence, so a
token added by a forward hook on ``embeddings`` is masked and attended like
any other.

THERE IS NO IDENTITY GATE — BY PHYSICS, NOT OMISSION
----------------------------------------------------
FiLM can gate (multiply by exactly 1.0, add exactly 0.0 — bit-exact);
a prepended token cannot: it adds a row and column to every attention
matrix and renormalizes every softmax, so no parameter value reproduces
the bare checkpoint bit-for-bit. The spec's honesty mechanism for this arm
is the TWIN — an unconditioned run with identical schedule/seed — and the
tests pin instead that the step-0 perturbation is nonzero and bounded.

THE NULL PATH IS PER-ROW ``torch.where`` (context.py's contract carried on)
---------------------------------------------------------------------------
``present`` (B, 1) selects between the projected context and the learned
``null_token`` ROW BY ROW: a mixed batch routes each recording on its own
flag, and because ``torch.where`` back-propagates only into the selected
branch, ``null_token`` learns ONLY from flag-0 rows — missing context means
"the model is told it knows nothing", never a fabricated prior.

THE WRAPPED MODEL IS NEVER MUTATED (film.py's discipline, verbatim)
-------------------------------------------------------------------
Projection and null token live on the WRAPPER; the embeddings hook is
registered at the start of ``forward`` and removed in a ``finally``. The
inner model called directly — the twin / repro-gate path — is byte-for-byte
the checkpoint.
"""
from __future__ import annotations

import torch
from torch import nn

from birdology.phase_c.context import ContextEncoder

#: Where the context token lands: right after [CLS, distill].
_INSERT_AT = 2


class TokenAST(nn.Module):
    """``(inputs, context (B, n_classes), present (B, 1)) -> logits``."""

    def __init__(self, model: nn.Module, context_encoder: ContextEncoder) -> None:
        super().__init__()
        self.model = model
        self.context_encoder = context_encoder
        d_ctx = context_encoder.mlp[-1].out_features
        hidden = model.config.hidden_size
        self.project = nn.Linear(d_ctx, hidden)
        # Learned stand-in for "no context here" — trained only by flag-0
        # rows (torch.where routes the gradient). Zero at init: the null
        # token starts as silent as a token can be.
        self.null_token = nn.Parameter(torch.zeros(hidden))

    def forward(
        self,
        inputs: torch.Tensor,
        context: torch.Tensor,
        present: torch.Tensor,
    ) -> torch.Tensor:
        ctx = self.context_encoder(context, present)  # (B, d_ctx)
        projected = self.project(ctx)  # (B, H)
        # Per-row routing: flag-1 rows carry the projected context, flag-0
        # rows the learned null. torch.where back-propagates only into the
        # selected branch, so null_token sees gradient ONLY from flag-0 rows.
        token = torch.where(
            present > 0.5, projected, self.null_token.expand_as(projected)
        )  # (B, H)

        embeddings = self.model.audio_spectrogram_transformer.embeddings

        def insert_token(module, args, output):
            # output: (B, S, H) = [CLS, distill, patches...]; insert ONE
            # token at index 2 so the pooled indices 0/1 keep their meaning.
            return torch.cat(
                [
                    output[:, :_INSERT_AT],
                    token.unsqueeze(1),
                    output[:, _INSERT_AT:],
                ],
                dim=1,
            )

        handle = embeddings.register_forward_hook(insert_token)
        try:
            return self.model(input_values=inputs).logits
        finally:
            # The hook never outlives this forward: the inner model called
            # directly remains the unmodified checkpoint.
            handle.remove()
