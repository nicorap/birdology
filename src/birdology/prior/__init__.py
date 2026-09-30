"""Spatiotemporal occurrence priors, built from GBIF occurrence downloads.

Deliberately independent of the RDF graph in `birdology.graph`: the app's
graph is quota-balanced by (month, year) for browsing, which destroys the
temporal signal a prior encodes. See
docs/superpowers/specs/2026-09-28-context-conditioned-bioacoustics-design.md
"""
