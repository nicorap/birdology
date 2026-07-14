"""Tests for the Wikipedia RAG search — no live Ollama, Chroma, or network.

Regression: WikiRAG.search() was an unfiltered vector nearest-neighbour query.
Asked about a species that is not in the index, it returned the closest chunks
from OTHER species with nothing to mark them as the wrong bird — so the
assistant could describe a Black-tailed Godwit's diet using a Redshank's
article. Same fabrication family as the invented "Haliaeetus albicillatus".
The fix is structural: filter by scientific_name, and return nothing rather
than someone else's text.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from birdology.wiki_rag import WikiRAG


def _rag_with_collection(collection) -> WikiRAG:
    rag = WikiRAG.__new__(WikiRAG)          # bypass __init__ (no Chroma/Ollama)
    rag._get_collection = lambda: collection
    rag._embed = lambda text: [0.0, 0.1, 0.2]
    return rag


def _collection(hits: list[dict]) -> MagicMock:
    col = MagicMock()
    col.count.return_value = 100
    col.query.return_value = {
        "documents": [[h["doc"] for h in hits]],
        "metadatas": [[h["meta"] for h in hits]],
        "distances": [[h.get("dist", 0.2) for h in hits]],
    }
    return col


def test_search_filters_chroma_by_scientific_name():
    col = _collection([])
    rag = _rag_with_collection(col)

    rag.search("diet and habitat", scientific_name="Limosa limosa")

    where = col.query.call_args[1]["where"]
    assert where == {"scientific_name": "Limosa limosa"}


def test_search_strips_author_from_scientific_name():
    """The index stores the binomial, so the filter must normalise identically."""
    col = _collection([])
    rag = _rag_with_collection(col)

    rag.search("diet", scientific_name="Limosa limosa (Linnaeus, 1758)")

    where = col.query.call_args[1]["where"]
    assert where == {"scientific_name": "Limosa limosa"}


def test_search_returns_empty_when_species_not_indexed():
    """The whole point: no rows rather than another bird's article."""
    col = _collection([])
    rag = _rag_with_collection(col)

    assert rag.search("diet", scientific_name="Limosa limosa") == []


def test_search_never_returns_a_different_species():
    """Even if Chroma somehow hands back a foreign chunk, we must not pass it on."""
    col = _collection([
        {"doc": "The Common Redshank probes mud for worms.",
         "meta": {"scientific_name": "Tringa totanus", "common_name": "Common Redshank"}},
        {"doc": "The Black-tailed Godwit breeds in wet meadows.",
         "meta": {"scientific_name": "Limosa limosa", "common_name": "Black-tailed Godwit"}},
    ])
    rag = _rag_with_collection(col)

    rows = rag.search("diet", scientific_name="Limosa limosa")

    assert all(r["scientific_name"] == "Limosa limosa" for r in rows), (
        f"leaked another species: {[r['scientific_name'] for r in rows]}"
    )
    assert len(rows) == 1


def test_search_without_species_filter_still_works():
    """Unfiltered search stays available for callers that genuinely want it."""
    col = _collection([
        {"doc": "text", "meta": {"scientific_name": "Tringa totanus", "common_name": "Redshank"}},
    ])
    rag = _rag_with_collection(col)

    rows = rag.search("wading birds")

    assert col.query.call_args[1].get("where") is None
    assert len(rows) == 1


# ── the chat tool ────────────────────────────────────────────────────────────

def test_search_wikipedia_tool_requires_scientific_name():
    """A species question that cannot name its species is exactly how the wrong
    bird's article got returned."""
    from chat import TOOLS_OPENAI

    tool = next(t for t in TOOLS_OPENAI if t["function"]["name"] == "search_wikipedia")
    params = tool["function"]["parameters"]
    assert "scientific_name" in params["properties"]
    assert "scientific_name" in params["required"]


def test_search_wikipedia_says_not_indexed_rather_than_returning_other_species():
    import chat

    class FakeRag:
        def is_built(self):
            return True
        def search(self, query, n_results=4, scientific_name=None):
            return []

    original = chat._get_wiki_rag
    chat._get_wiki_rag = lambda: FakeRag()
    try:
        out = chat._run_tool(
            "search_wikipedia",
            {"query": "diet", "scientific_name": "Limosa limosa"},
            graph=None,
        )
    finally:
        chat._get_wiki_rag = original

    assert "Limosa limosa" in out
    assert "not indexed" in out.lower() or "no wikipedia" in out.lower()
