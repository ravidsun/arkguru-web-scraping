"""
Shared, framework-free chunking primitives used by BOTH Phase 1 (PDF) and
Phase 2 (web). Living in `common/` keeps each phase repo standalone.
"""
from __future__ import annotations

import re
from .tokenizer import count_tokens

_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9])")


def split_sentences(text: str) -> list[str]:
    return [s.strip() for s in _SENT_SPLIT.split(text) if s.strip()]


def pack_windows(
    sentences: list[str],
    target_tokens: int,
    overlap_tokens: int,
) -> list[tuple[str, int]]:
    """Greedy-pack sentences into ~target_tokens windows with sentence overlap.

    Returns list of (window_text, overlap_tokens_used).
    """
    windows: list[tuple[str, int]] = []
    i, n = 0, len(sentences)
    while i < n:
        cur: list[str] = []
        tok = 0
        j = i
        while j < n:
            t = count_tokens(sentences[j])
            if tok + t > target_tokens and cur:
                break
            cur.append(sentences[j]); tok += t; j += 1
        windows.append((" ".join(cur), 0 if not windows else overlap_tokens))
        if j >= n:
            break
        back_tok, k = 0, j
        while k > i and back_tok < overlap_tokens:
            k -= 1; back_tok += count_tokens(sentences[k])
        i = max(k, i + 1)
    return windows
