"""Phase A: turn the BirdSet corpus into a context-indexed embedding cache.

BirdSet's metadata carries lat/long but NO recording date, and the prior is
P(species | H3 cell, ISO week) -- so without a date the temporal half of the
experiment has no join key. Focal recordings are dated from the Xeno-Canto v3
API; soundscape recordings carry their date in the filename.

See docs/superpowers/plans/2026-09-30-phase-a-embedding-extraction.md
"""
