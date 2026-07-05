"""
Postgres + pgvector datastore -- TWO tables, cleanly separated:

  1. <chunks_table>      (default "chunks")           -- the chunk text + metadata.
     This is the JSONL-equivalent SOURCE OF TRUTH. No vectors here.
  2. <vectors_table>     (default "chunk_embeddings")  -- the embeddings only,
     keyed by chunk_id (FK -> chunks, ON DELETE CASCADE), with the HNSW index.

Why separate:
  - Embeddings are disposable & model-specific; chunk text/metadata is durable.
    Re-embed with a different model by truncating one table -- chunks untouched.
  - Phases 1 & 2 write ONLY the chunks table (no embedding model needed).
    Phase 3 fills the vectors table in place.
  - Lexical/full-text search lives on the chunks table; dense search JOINs the
    vectors table. Retrieval fuses both.

Phase flow:
    Phase 1/2  -> upsert()             -> chunks table (vectors table stays empty)
    Phase 3    -> update_embeddings()  -> chunk_embeddings table
    retrieve   -> search_dense()/search_lexical() (JOIN when dense)

Requires:  pip install "psycopg[binary]" pgvector
    export PG_DSN=postgresql://user:pass@localhost:5432/rag
"""
from __future__ import annotations

import json
import os
from typing import Iterable, Iterator, Optional

import logging
from .schema import Chunk

log = logging.getLogger("common.datastore")

# columns of the chunks (source-of-truth) table
_COLS = ["chunk_id", "text", "source_type", "source_id", "chunk_index",
         "title", "section", "page", "url", "domain", "lang",
         "parent_id", "is_parent", "token_count", "overlap_tokens"]

# columns returned to retrieval callers
_HIT_COLS = ["chunk_id", "text", "section", "source_id", "page", "url", "parent_id"]


class ChunkStore:
    def __init__(self, dsn: Optional[str] = None, table: str = "chunks",
                 vectors_table: str = "chunk_embeddings", dim: int = 1024,
                 dsn_env: str = "PG_DSN"):
        self.dsn = dsn or os.environ.get(dsn_env)
        if not self.dsn:
            raise ValueError(
                f"No Postgres DSN. Pass dsn=... or set ${dsn_env}, e.g. "
                "postgresql://user:pass@localhost:5432/rag")
        self.chunks = table
        self.vectors = vectors_table
        self.dim = dim

    # -- connection --------------------------------------------------------
    def _connect(self):
        import psycopg
        from pgvector.psycopg import register_vector
        conn = psycopg.connect(self.dsn)
        try:
            register_vector(conn)
        except Exception:
            pass
        return conn

    # -- schema (two tables) ----------------------------------------------
    def ensure_schema(self) -> None:
        """Create the two tables + indexes if missing. Idempotent; safe to call on
        every run. Logs whether each table was created or already present, and
        raises a clear error if the database is unreachable."""
        try:
            conn = self._connect()
        except Exception as e:
            raise RuntimeError(
                "Could not connect to Postgres for the datastore. Check PG_DSN / "
                "config/datastore.yaml (see docs/DATABASE_SETUP.md). "
                f"Underlying error: {e}") from e
        with conn, conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            pre = {}
            for t in (self.chunks, self.vectors):
                cur.execute("SELECT to_regclass(%s)", (t,))
                pre[t] = cur.fetchone()[0] is not None
            # 1) chunks = source of truth (no embedding column)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.chunks} (
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
                    meta         jsonb DEFAULT '{{}}'::jsonb,
                    ts           tsvector GENERATED ALWAYS AS
                                 (to_tsvector('english', coalesce(text,''))) STORED,
                    created_at   timestamptz DEFAULT now()
                );""")
            cur.execute(f"CREATE INDEX IF NOT EXISTS {self.chunks}_ts_idx "
                        f"ON {self.chunks} USING gin(ts);")
            # 2) vectors = embeddings only, keyed to chunks
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.vectors} (
                    chunk_id   text PRIMARY KEY
                               REFERENCES {self.chunks}(chunk_id) ON DELETE CASCADE,
                    embedding  vector({self.dim}),
                    model      text,
                    created_at timestamptz DEFAULT now()
                );""")
            cur.execute(f"CREATE INDEX IF NOT EXISTS {self.vectors}_hnsw_idx "
                        f"ON {self.vectors} USING hnsw (embedding vector_cosine_ops);")
            conn.commit()
            for t in (self.chunks, self.vectors):
                log.info("datastore table '%s': %s", t,
                         "already present" if pre.get(t) else "created")

    # -- write chunks (Phases 1/2) ----------------------------------------
    def upsert(self, chunks: Iterable[Chunk]) -> int:
        """Insert/update chunk rows (source of truth). Idempotent by chunk_id.
        Does NOT touch the vectors table."""
        rows = []
        for c in chunks:
            d = c.to_dict()
            rows.append(tuple(d.get(k) for k in _COLS) + (json.dumps(d.get("extra") or {}),))
        if not rows:
            return 0
        placeholders = ",".join(["%s"] * (len(_COLS) + 1))
        collist = ",".join(_COLS + ["meta"])
        updates = ",".join(f"{k}=EXCLUDED.{k}" for k in _COLS if k != "chunk_id")
        sql = (f"INSERT INTO {self.chunks} ({collist}) VALUES ({placeholders}) "
               f"ON CONFLICT (chunk_id) DO UPDATE SET {updates}, meta=EXCLUDED.meta")
        with self._connect() as conn, conn.cursor() as cur:
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)

    # -- embeddings (Phase 3) ---------------------------------------------
    def iter_missing_embeddings(self, batch: int = 256
                                ) -> Iterator[list[tuple[str, str]]]:
        """Yield (chunk_id, text) for chunks that have no row in the vectors table."""
        with self._connect() as conn, conn.cursor(name="missing") as cur:
            cur.itersize = batch
            cur.execute(
                f"SELECT c.chunk_id, c.text FROM {self.chunks} c "
                f"LEFT JOIN {self.vectors} v ON c.chunk_id = v.chunk_id "
                f"WHERE v.chunk_id IS NULL AND c.is_parent = false")
            buf = []
            for row in cur:
                buf.append((row[0], row[1]))
                if len(buf) >= batch:
                    yield buf; buf = []
            if buf:
                yield buf

    def update_embeddings(self, ids: list[str], vectors, model: Optional[str] = None) -> int:
        """Upsert embeddings into the vectors table (keyed by chunk_id)."""
        with self._connect() as conn, conn.cursor() as cur:
            for cid, vec in zip(ids, vectors):
                v = vec.tolist() if hasattr(vec, "tolist") else list(vec)
                cur.execute(
                    f"INSERT INTO {self.vectors} (chunk_id, embedding, model) "
                    f"VALUES (%s, %s, %s) ON CONFLICT (chunk_id) DO UPDATE "
                    f"SET embedding = EXCLUDED.embedding, model = EXCLUDED.model, "
                    f"created_at = now()", (cid, v, model))
            conn.commit()
            return len(ids)

    # -- counts / reads ----------------------------------------------------
    def count(self, only_missing_embedding: bool = False) -> int:
        if only_missing_embedding:
            q = (f"SELECT count(*) FROM {self.chunks} c "
                 f"LEFT JOIN {self.vectors} v ON c.chunk_id = v.chunk_id "
                 f"WHERE v.chunk_id IS NULL AND c.is_parent = false")
        else:
            q = f"SELECT count(*) FROM {self.chunks}"
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def count_vectors(self) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self.vectors}")
            return cur.fetchone()[0]

    def read_all(self, include_parents: bool = True) -> list[Chunk]:
        q = f"SELECT {','.join(_COLS)}, meta FROM {self.chunks}"
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

    # -- search (Phase 3 retrieval) ---------------------------------------
    def search_dense(self, qvec, k: int = 20) -> list[tuple]:
        """Cosine NN over the vectors table, JOINed back to chunk text/metadata."""
        cols = ",".join(f"c.{x}" for x in _HIT_COLS)
        vec = "[" + ",".join(str(float(x)) for x in qvec) + "]"   # pgvector literal
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT {cols}, 1 - (v.embedding <=> %s::vector) AS score "
                f"FROM {self.vectors} v JOIN {self.chunks} c ON c.chunk_id = v.chunk_id "
                f"ORDER BY v.embedding <=> %s::vector LIMIT %s", (vec, vec, k))
            return cur.fetchall()

    def search_lexical(self, query: str, k: int = 20) -> list[tuple]:
        """Full-text (BM25-like) search over the chunks table."""
        cols = ",".join(f"c.{x}" for x in _HIT_COLS)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT {cols}, ts_rank(c.ts, plainto_tsquery('english', %s)) AS score "
                f"FROM {self.chunks} c "
                f"WHERE c.ts @@ plainto_tsquery('english', %s) "
                f"ORDER BY score DESC LIMIT %s", (query, query, k))
            return cur.fetchall()
