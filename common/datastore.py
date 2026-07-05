"""
Postgres + pgvector chunk store -- the single datastore for the whole pipeline.

Phases 1 & 2 UPSERT chunks here (text + metadata, embedding left NULL). Phase 3
fills embeddings IN PLACE and runs hybrid search over the same table. One system
is both the system-of-record for chunks and the vector index.

Why one table: embeddings are disposable and model-specific; chunks are the
durable source of truth. Keeping them together (embedding nullable) means you can
re-embed with a different model anytime without re-extracting, and non-retrieval
consumers (fine-tune dataset prep) read the same rows.

Design:
  - upsert() is idempotent by chunk_id (ON CONFLICT). Re-ingesting the same PDFs
    is a no-op; new PDFs just add rows. Perfect for iterative corpora.
  - upsert() never clobbers an existing embedding unless you pass one.
  - A generated tsvector column gives lexical/BM25-style search for free.
  - HNSW cosine index is created on the embedding column for fast ANN.

Requires:  pip install "psycopg[binary]" pgvector
Set the DSN via env (default PG_DSN), e.g.
    export PG_DSN=postgresql://user:pass@localhost:5432/rag
"""
from __future__ import annotations

import json
import os
from typing import Iterable, Iterator, Optional

from .schema import Chunk

# columns persisted as first-class (everything else rides in meta jsonb)
_COLS = ["chunk_id", "text", "source_type", "source_id", "chunk_index",
         "title", "section", "page", "url", "domain", "lang",
         "parent_id", "is_parent", "token_count", "overlap_tokens"]


class ChunkStore:
    def __init__(self, dsn: Optional[str] = None, table: str = "chunks",
                 dim: int = 1024, dsn_env: str = "PG_DSN"):
        self.dsn = dsn or os.environ.get(dsn_env)
        if not self.dsn:
            raise ValueError(
                f"No Postgres DSN. Pass dsn=... or set ${dsn_env}, e.g. "
                "postgresql://user:pass@localhost:5432/rag")
        self.table = table
        self.dim = dim

    # -- connection --------------------------------------------------------
    def _connect(self):
        import psycopg
        from pgvector.psycopg import register_vector
        conn = psycopg.connect(self.dsn)
        # register_vector needs the extension to exist; ensure_schema does that.
        try:
            register_vector(conn)
        except Exception:
            pass
        return conn

    # -- schema ------------------------------------------------------------
    def ensure_schema(self) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    chunk_id     text PRIMARY KEY,
                    text         text NOT NULL,
                    source_type  text,
                    source_id    text,
                    chunk_index  int,
                    title        text,
                    section      text,
                    page         int,
                    url          text,
                    domain       text,
                    lang         text,
                    parent_id    text,
                    is_parent    boolean DEFAULT false,
                    token_count  int,
                    overlap_tokens int DEFAULT 0,
                    embedding    vector({self.dim}),
                    meta         jsonb DEFAULT '{{}}'::jsonb,
                    ts           tsvector GENERATED ALWAYS AS
                                 (to_tsvector('english', coalesce(text,''))) STORED,
                    created_at   timestamptz DEFAULT now()
                );""")
            cur.execute(f"CREATE INDEX IF NOT EXISTS {self.table}_ts_idx "
                        f"ON {self.table} USING gin(ts);")
            # HNSW only helps non-null rows; build it once embeddings exist.
            cur.execute(f"CREATE INDEX IF NOT EXISTS {self.table}_vec_idx "
                        f"ON {self.table} USING hnsw (embedding vector_cosine_ops);")
            conn.commit()

    # -- write -------------------------------------------------------------
    def upsert(self, chunks: Iterable[Chunk]) -> int:
        """Insert new chunks / update text+metadata for existing chunk_ids.

        Idempotent. Existing embeddings are preserved (embedding not overwritten
        here). Returns number of rows affected.
        """
        rows = []
        for c in chunks:
            d = c.to_dict()
            meta = d.get("extra") or {}
            rows.append(tuple(d.get(k) for k in _COLS) + (json.dumps(meta),))
        if not rows:
            return 0
        placeholders = ",".join(["%s"] * (len(_COLS) + 1))
        collist = ",".join(_COLS + ["meta"])
        updates = ",".join(f"{k}=EXCLUDED.{k}" for k in _COLS if k != "chunk_id")
        sql = (f"INSERT INTO {self.table} ({collist}) VALUES ({placeholders}) "
               f"ON CONFLICT (chunk_id) DO UPDATE SET {updates}, meta=EXCLUDED.meta")
        with self._connect() as conn, conn.cursor() as cur:
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)

    # -- embeddings --------------------------------------------------------
    def iter_missing_embeddings(self, batch: int = 256
                                ) -> Iterator[list[tuple[str, str]]]:
        """Yield batches of (chunk_id, text) for rows with no embedding yet."""
        with self._connect() as conn, conn.cursor(name="missing") as cur:
            cur.itersize = batch
            cur.execute(f"SELECT chunk_id, text FROM {self.table} "
                        f"WHERE embedding IS NULL AND is_parent = false")
            buf = []
            for row in cur:
                buf.append((row[0], row[1]))
                if len(buf) >= batch:
                    yield buf; buf = []
            if buf:
                yield buf

    def update_embeddings(self, ids: list[str], vectors) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            for cid, vec in zip(ids, vectors):
                v = vec.tolist() if hasattr(vec, "tolist") else list(vec)
                cur.execute(f"UPDATE {self.table} SET embedding=%s WHERE chunk_id=%s",
                            (v, cid))
            conn.commit()
            return len(ids)

    # -- read --------------------------------------------------------------
    def count(self, only_missing_embedding: bool = False) -> int:
        q = f"SELECT count(*) FROM {self.table}"
        if only_missing_embedding:
            q += " WHERE embedding IS NULL"
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def read_all(self, include_parents: bool = True) -> list[Chunk]:
        q = f"SELECT {','.join(_COLS)}, meta FROM {self.table}"
        if not include_parents:
            q += " WHERE is_parent = false"
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(q)
            out = []
            for row in cur.fetchall():
                d = dict(zip(_COLS, row))
                d["extra"] = row[-1] or {}
                out.append(Chunk.from_dict(d))
            return out
