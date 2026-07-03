"""
Token counting used for chunk sizing.

We standardize on tiktoken's `cl100k_base` as a *proxy* tokenizer for sizing
chunks. It is fast, dependency-light, and close enough to most 3B-7B model
tokenizers for the purpose of "is this chunk ~300-800 tokens?".

For exact accounting against your final base model (e.g. Qwen2.5-3B), swap in
that model's HF tokenizer in Phase 3 -- the chunker only needs an approximate,
*consistent* count.
"""

from __future__ import annotations

from functools import lru_cache


@lru_cache(maxsize=1)
def _encoder():
    try:
        import tiktoken
        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


def count_tokens(text: str) -> int:
    enc = _encoder()
    if enc is not None:
        return len(enc.encode(text))
    # Fallback heuristic: ~4 chars/token for English prose.
    return max(1, len(text) // 4)


def truncate_to_tokens(text: str, max_tokens: int) -> str:
    enc = _encoder()
    if enc is None:
        return text[: max_tokens * 4]
    ids = enc.encode(text)
    if len(ids) <= max_tokens:
        return text
    return enc.decode(ids[:max_tokens])
