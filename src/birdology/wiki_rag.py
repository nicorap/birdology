"""Wikipedia RAG: fetch articles, chunk, embed with Ollama, index in ChromaDB."""
from __future__ import annotations

import hashlib
import re
import time
from pathlib import Path
from typing import Optional

import requests

_DEFAULT_OLLAMA_URL = "http://localhost:11434"
_DEFAULT_MODEL = "nomic-embed-text"
_DEFAULT_INDEX_DIR = Path("data/wiki_index")
_COLLECTION_NAME = "bird_wikipedia"
_CHUNK_SIZE = 500  # chars
_REQUEST_DELAY = 0.15  # seconds between Wikipedia requests
_HEADERS = {"User-Agent": "Birdology/1.0 (https://github.com/nicorap/birdology; educational)"}


def _search_wikipedia_title(query: str, lang: str = "en") -> Optional[str]:
    """Return best Wikipedia title matching *query*, or None."""
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "srlimit": 1,
        "format": "json",
    }
    try:
        r = requests.get(url, params=params, timeout=10, headers=_HEADERS)
        r.raise_for_status()
        hits = r.json().get("query", {}).get("search", [])
        return hits[0]["title"] if hits else None
    except Exception:
        return None


def _fetch_wikipedia_extract(title: str, lang: str = "en") -> Optional[str]:
    """Fetch plain-text article extract for *title*."""
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "extracts",
        "titles": title,
        "explaintext": True,
        "exsectionformat": "plain",
        "format": "json",
        "redirects": True,
    }
    try:
        r = requests.get(url, params=params, timeout=15, headers=_HEADERS)
        r.raise_for_status()
        pages = r.json().get("query", {}).get("pages", {})
        page = next(iter(pages.values()))
        if "missing" in page:
            return None
        return page.get("extract", "").strip() or None
    except Exception:
        return None


def _chunk_text(text: str, size: int = _CHUNK_SIZE) -> list[str]:
    """Split *text* into chunks of at most *size* chars, breaking on paragraphs."""
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if len(p.strip()) > 60]
    chunks: list[str] = []
    buf = ""
    for para in paragraphs:
        if len(buf) + len(para) + 2 <= size:
            buf = (buf + "\n\n" + para).strip() if buf else para
        else:
            if buf:
                chunks.append(buf)
            if len(para) > size:
                # Split long paragraph by sentence
                for sent in re.split(r"(?<=[.!?])\s+", para):
                    if len(buf) + len(sent) + 1 <= size:
                        buf = (buf + " " + sent).strip() if buf else sent
                    else:
                        if buf:
                            chunks.append(buf)
                        buf = sent
            else:
                buf = para
    if buf:
        chunks.append(buf)
    return chunks


class WikiRAG:
    """Thin wrapper around ChromaDB + Ollama embeddings for bird Wikipedia articles."""

    def __init__(
        self,
        index_dir: str | Path = _DEFAULT_INDEX_DIR,
        embed_model: str = _DEFAULT_MODEL,
        ollama_url: str = _DEFAULT_OLLAMA_URL,
    ) -> None:
        self.index_dir = Path(index_dir)
        self.embed_model = embed_model
        self.ollama_url = ollama_url.rstrip("/")
        self._collection = None

    # ── Internal helpers ──────────────────────────────────────────────────

    def _get_collection(self):
        if self._collection is None:
            import chromadb
            self.index_dir.mkdir(parents=True, exist_ok=True)
            client = chromadb.PersistentClient(path=str(self.index_dir))
            self._collection = client.get_or_create_collection(
                _COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
        return self._collection

    def _embed(self, text: str) -> list[float]:
        r = requests.post(
            f"{self.ollama_url}/api/embeddings",
            json={"model": self.embed_model, "prompt": text},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["embedding"]

    # ── Public API ────────────────────────────────────────────────────────

    def is_built(self) -> bool:
        """True if the index exists and contains at least one document."""
        try:
            return self._get_collection().count() > 0
        except Exception:
            return False

    def count(self) -> int:
        try:
            return self._get_collection().count()
        except Exception:
            return 0

    def index_species(
        self,
        scientific_name: str,
        common_name: str = "",
        lang: str = "en",
        verbose: bool = False,
    ) -> int:
        """Fetch & index one species. Returns number of new chunks added."""
        col = self._get_collection()

        # Strip author from e.g. "Haematopus ostralegus Linnaeus, 1758" → "Haematopus ostralegus"
        binomial = " ".join(scientific_name.split()[:2])

        # Try scientific name first, then "<common> bird"
        title = _search_wikipedia_title(binomial, lang)
        if not title and common_name:
            title = _search_wikipedia_title(f"{common_name} bird", lang)
        if not title:
            return 0

        extract = _fetch_wikipedia_extract(title, lang)
        if not extract:
            return 0

        chunks = _chunk_text(extract)
        if not chunks:
            return 0

        slug = binomial.lower().replace(" ", "_")
        wiki_url = f"https://{lang}.wikipedia.org/wiki/{title.replace(' ', '_')}"

        ids, documents, embeddings, metadatas = [], [], [], []
        for i, chunk in enumerate(chunks):
            doc_id = hashlib.md5(f"{slug}_{i}_{chunk[:40]}".encode()).hexdigest()[:16]
            if col.get(ids=[doc_id])["ids"]:
                continue  # already indexed
            ids.append(doc_id)
            documents.append(chunk)
            embeddings.append(self._embed(chunk))
            metadatas.append({
                "scientific_name": scientific_name,
                "common_name": common_name or scientific_name,
                "wiki_title": title,
                "wiki_url": wiki_url,
                "chunk_idx": i,
            })
            time.sleep(0.05)

        if ids:
            col.add(ids=ids, documents=documents, embeddings=embeddings, metadatas=metadatas)
        if verbose and ids:
            print(f"  {scientific_name}: {len(ids)} chunks ← '{title}'")
        return len(ids)

    def build_index(
        self,
        species_list: list[dict],
        verbose: bool = True,
    ) -> int:
        """
        Index a list of species dicts (keys: ``scientificName``, optionally ``commonName``).
        Returns total chunks added.
        """
        total = 0
        for i, sp in enumerate(species_list):
            sci = sp.get("scientificName", "").strip()
            if not sci:
                continue
            common = sp.get("commonName", "")
            total += self.index_species(sci, common, verbose=verbose)
            if verbose and (i + 1) % 20 == 0:
                print(f"  [{i + 1}/{len(species_list)}] {total} chunks total")
            time.sleep(_REQUEST_DELAY)
        return total

    def search(self, query: str, n_results: int = 5) -> list[dict]:
        """
        Semantic search over indexed chunks.
        Returns list of dicts with keys: text, species, scientific_name, wiki_url, score.
        """
        col = self._get_collection()
        count = col.count()
        if count == 0:
            return []

        query_emb = self._embed(query)
        results = col.query(
            query_embeddings=[query_emb],
            n_results=min(n_results, count),
            include=["documents", "metadatas", "distances"],
        )

        out = []
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            out.append({
                "text": doc,
                "species": meta.get("common_name") or meta.get("scientific_name", ""),
                "scientific_name": meta.get("scientific_name", ""),
                "wiki_title": meta.get("wiki_title", ""),
                "wiki_url": meta.get("wiki_url", ""),
                "score": round(1.0 - float(dist), 3),
            })
        return out
