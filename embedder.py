"""Shared embedding model, used identically by ingest.py and app.py.

The prefix logic lives here rather than in each script because a mismatch
between the text prefixed at index time and at query time still produces a
plausible-looking index and silently destroys recall.

`intfloat/multilingual-e5-*` is an asymmetric model: it expects `query: ` on the
question and `passage: ` on the indexed text, and its inner-product scores are
only meaningful on normalised vectors. Both are handled here so callers can
just call embed_query / embed_documents.
"""

import os

from langchain_core.embeddings import Embeddings

EMBED_MODEL = os.environ.get("EMBED_MODEL", "intfloat/multilingual-e5-base")
EMBED_DIM = 768
CHUNK_SIZE = 800
CHUNK_OVERLAP = 128

QUERY_PREFIX = "query: "
PASSAGE_PREFIX = "passage: "


def _is_asymmetric(model_name: str) -> bool:
    # e5 models are the asymmetric family; anything else is left unprefixed so
    # a symmetric model can be swapped in via EMBED_MODEL alone.
    return "e5" in model_name.lower()


class PrefixedEmbeddings(Embeddings):
    """SentenceTransformer wrapper adding e5 prefixes and normalising output."""

    def __init__(self, model_name: str = EMBED_MODEL, max_seq_length: int = 512):
        from sentence_transformers import SentenceTransformer

        self.model_name = model_name
        self.asymmetric = _is_asymmetric(model_name)
        self._model = SentenceTransformer(model_name)
        self._model.max_seq_length = min(self._model.max_seq_length, max_seq_length)
        self.dimension = self._model.get_sentence_embedding_dimension()

    def _encode(self, texts, prefix):
        if self.asymmetric:
            texts = [f"{prefix}{t}" for t in texts]
        return self._model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=False,
            convert_to_numpy=True,
        ).tolist()

    def embed_documents(self, texts):
        return self._encode(list(texts), PASSAGE_PREFIX)

    def embed_query(self, text):
        return self._encode([text], QUERY_PREFIX)[0]
