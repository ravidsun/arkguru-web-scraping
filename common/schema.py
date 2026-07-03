"""
Shared record schema used by ALL phases.

Phase 1 (PDF) and Phase 2 (web) both emit lists of `Chunk` records in the
exact same shape, so Phase 3 can concatenate them without any glue code.

Storage formats:
  - JSONL  : one JSON object per line (human-readable, git-diffable, streamable)
  - Parquet: columnar, compressed, fast to load (preferred for large corpora)

Design goals:
  - Portable: no framework-specific objects, only JSON-serializable primitives.
  - Stable: chunk_id is a deterministic hash so re-runs are idempotent and
    deduplication is trivial.
  - Rich metadata: enough provenance to cite a source in a RAG answer.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Iterable, Optional

SCHEMA_VERSION = "1.0"


def _stable_hash(*parts: str) -> str:
    """Deterministic short id from the given parts."""
    h = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return h[:16]


@dataclass
class Chunk:
    """A single retrievable unit of text plus its provenance."""

    # --- core content ---
    text: str                          # the chunk body (what gets embedded)
    source_type: str                   # "pdf" | "web"
    source_id: str                     # file name or URL
    chunk_index: int                   # position within the source document

    # --- provenance / metadata ---
    title: Optional[str] = None        # doc title or page <title>
    section: Optional[str] = None      # nearest heading (structure-aware)
    page: Optional[int] = None         # PDF page number (1-based); None for web
    url: Optional[str] = None          # web source URL
    domain: Optional[str] = None       # web domain, e.g. "docs.python.org"
    lang: Optional[str] = None         # detected language code

    # --- parent/child chunking ---
    parent_id: Optional[str] = None    # id of the larger parent chunk, if any
    is_parent: bool = False            # True for the big-context parent record

    # --- bookkeeping ---
    token_count: Optional[int] = None
    overlap_tokens: int = 0
    embedding: Optional[list[float]] = None   # optional; usually filled in Phase 3
    extra: dict[str, Any] = field(default_factory=dict)  # domain-specific fields
    chunk_id: str = ""                 # auto-filled deterministic id
    schema_version: str = SCHEMA_VERSION
    created_at: float = field(default_factory=lambda: time.time())

    def __post_init__(self) -> None:
        if not self.chunk_id:
            self.chunk_id = _stable_hash(
                self.source_type, self.source_id, str(self.chunk_index),
                "parent" if self.is_parent else "child",
            )

    # convenience -----------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Chunk":
        known = set(cls.__dataclass_fields__)  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in d.items() if k in known})


# --- I/O helpers -----------------------------------------------------------

def write_jsonl(chunks: Iterable[Chunk], path: str | Path) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for c in chunks:
            f.write(json.dumps(c.to_dict(), ensure_ascii=False) + "\n")
            n += 1
    return n


def read_jsonl(path: str | Path) -> list[Chunk]:
    out: list[Chunk] = []
    with Path(path).open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(Chunk.from_dict(json.loads(line)))
    return out


def write_parquet(chunks: Iterable[Chunk], path: str | Path) -> int:
    """Requires pyarrow. Raises a clear error if it is missing."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "pyarrow is required for Parquet output. "
            "pip install pyarrow  (or just use write_jsonl)."
        ) from e

    rows = []
    for c in chunks:
        d = c.to_dict()
        # Arrow cannot store an empty struct; serialize free-form/nested
        # fields as JSON strings so any shape round-trips cleanly.
        d["extra"] = json.dumps(d.get("extra") or {}, ensure_ascii=False)
        rows.append(d)
    table = pa.Table.from_pylist(rows)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")
    return len(rows)


def read_parquet(path: str | Path) -> list[Chunk]:
    import pyarrow.parquet as pq
    table = pq.read_table(path)
    out = []
    for r in table.to_pylist():
        if isinstance(r.get("extra"), str):
            try:
                r["extra"] = json.loads(r["extra"])
            except Exception:
                r["extra"] = {}
        out.append(Chunk.from_dict(r))
    return out
