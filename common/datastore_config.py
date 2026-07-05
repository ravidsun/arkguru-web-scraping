"""
Loader + factory for the datastore -- keeps datastore config OUT of code.

Everything about *where* chunks and vectors live is declared in one file
(config/datastore.yaml). Code never hardcodes a DSN, table name, or dimension;
it calls `open_chunk_store()` and gets a ready ChunkStore. Switching database or
tables is a one-file edit; switching the connection is a single env var.

Precedence (highest first):
    explicit kwargs  >  environment variables  >  datastore.yaml  >  defaults

Env overrides (handy for CI / prod without editing files):
    DATASTORE_CONFIG   path to the yaml (default: config/datastore.yaml)
    DATASTORE_BACKEND  postgres | file | local
    PG_DSN (or whatever `dsn_env` names)  the connection string
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

DEFAULT_PATH = "config/datastore.yaml"

_DEFAULTS: dict[str, Any] = {
    "backend": "file",
    "postgres": {"dsn_env": "PG_DSN", "dsn": None,
                 "chunks_table": "chunks", "vectors_table": "chunk_embeddings",
                 "dim": 1024},
    "file": {"out_dir": "data/processed", "out_format": "jsonl"},
    "local": {"path": "data/store/index"},
}


def load_datastore_config(path: Optional[str] = None) -> dict:
    """Read config/datastore.yaml (if present), merged over defaults, with env
    overrides for backend."""
    path = path or os.environ.get("DATASTORE_CONFIG", DEFAULT_PATH)
    cfg = {k: (dict(v) if isinstance(v, dict) else v) for k, v in _DEFAULTS.items()}
    p = Path(path)
    if p.exists():
        import yaml
        loaded = (yaml.safe_load(p.read_text()) or {}).get("datastore", {}) or {}
        for k, v in loaded.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update({kk: vv for kk, vv in v.items() if vv is not None})
            elif v is not None:
                cfg[k] = v
    if os.environ.get("DATASTORE_BACKEND"):
        cfg["backend"] = os.environ["DATASTORE_BACKEND"]
    return cfg


def resolve_dsn(pg: dict) -> Optional[str]:
    """DSN from explicit value, else from the named env var."""
    if pg.get("dsn"):
        return pg["dsn"]
    return os.environ.get(pg.get("dsn_env", "PG_DSN"))


def open_chunk_store(path: Optional[str] = None, **overrides):
    """Construct a ChunkStore from datastore.yaml (+ env + kwargs). Does not
    connect until you call a method on it."""
    from .datastore import ChunkStore
    cfg = load_datastore_config(path)
    pg = dict(cfg.get("postgres", {}))
    pg.update({k: v for k, v in overrides.items() if v is not None})
    dsn = pg.get("dsn") or resolve_dsn(pg)
    return ChunkStore(
        dsn=dsn,
        table=pg.get("chunks_table", "chunks"),
        vectors_table=pg.get("vectors_table", "chunk_embeddings"),
        dim=int(pg.get("dim", 1024)),
    )
