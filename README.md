# arkguru-web-scraping — Phase 2

Crawl websites and convert them into the **same** `Chunk` records as Phase 1, so
both feed Phase 3 with zero conversion. Independent process (different inputs —
URLs instead of PDFs), identical output schema. **Local-first, cloud-optional.**
Part of a 3-repo system with
[`arkguru-pdf-extraction`](https://github.com/ravidsun/arkguru-pdf-extraction)
(Phase 1) and
[`arkguru-rag-slm`](https://github.com/ravidsun/arkguru-rag-slm) (Phase 3).

---

## How it works (the logic)

One flow in `phase2_web/pipeline.py`: **crawl → extract → chunk → dedup → write.**

**1. Crawl frontier.**
Starting from `seeds`, a breadth-first queue visits pages up to `max_pages`.
Discovered links are enqueued; if `same_domain_only` is set, off-domain links are
dropped. The frontier policy (path prefixes, robots.txt, priorities) is left as a
marked `TODO` because it's a per-site decision.

**2. Fetch + extract.**
Two interchangeable backends return `(markdown, title, links)`:

| Backend | Deps | Use |
|---|---|---|
| `local` *(default)* | httpx + trafilatura + selectolax | static / well-behaved sites, no API key |
| `firecrawl` | `FIRECRAWL_API_KEY` | JS-heavy sites, large managed crawls |

The local backend uses **trafilatura** to strip navigation, ads, and boilerplate
and emit clean Markdown, then parses `<title>` and anchor links with selectolax.

**3. Chunk (structure-aware).**
Markdown is split by headings into sections, then packed into ~`target_tokens`
windows with `overlap_pct` overlap — using the **exact same** `pack_windows` /
`split_sentences` helpers as Phase 1 (`common/chunking.py`). That's what keeps
web chunks sized and shaped identically to PDF chunks.

**4. Deduplicate.**
Near-duplicate pages (shared headers/footers, syndicated content) are collapsed
with **MinHash LSH** (`datasketch`, Jaccard ≥ 0.9). If the library is absent it
falls back to exact-text dedup.

**5. Write.**
Output is the shared `Chunk` schema as JSONL or Parquet, with `source_type="web"`
and metadata: `url`, `domain`, `title`, `section`. Deterministic `chunk_id` makes
re-crawls idempotent.

---

## How-to guide

### 1. Install
```bash
git clone https://github.com/ravidsun/arkguru-web-scraping.git
cd arkguru-web-scraping
python -m venv .venv && source .venv/bin/activate     # optional
pip install -r requirements.txt
```
`firecrawl-py` is commented in `requirements.txt`; uncomment it only if you use
the cloud backend.

### 2. Point it at your site
Edit `config/config.yaml`:
```yaml
phase2:
  seeds: ["https://your.site/docs"]   # one or more start URLs
  out_path: "data/processed/web_chunks.jsonl"
  out_format: "jsonl"                 # jsonl | parquet
  backend: "local"                    # local | firecrawl
  firecrawl_api_key_env: "FIRECRAWL_API_KEY"
  max_pages: 200
  same_domain_only: true
  target_tokens: 550
  overlap_pct: 0.12
  min_content_chars: 200              # skip near-empty pages
  # --- datastore sink (optional; default writes JSONL/Parquet files) ---
  sink: "file"                        # file | postgres
  pg_dsn_env: "PG_DSN"
  pg_table: "chunks"
  pg_dim: 1024
```

### 3. Run
```bash
make phase2
# or ad-hoc without editing config:
python -m phase2_web.pipeline --seeds https://your.site/docs --max-pages 100
```
Console shows each URL and its chunk count, then the dedup reduction.

### 4. Use the cloud backend (optional, for JS-heavy sites)
```bash
pip install firecrawl-py
export FIRECRAWL_API_KEY=fc-...
python -m phase2_web.pipeline --backend firecrawl --config config/config.yaml
```

### 5. Hand off to Phase 3
Two options:

- **Files (default).** Copy `data/processed/web_chunks.jsonl` into the
  `arkguru-rag-slm` repo's `data/processed/`. It concatenates automatically with
  the Phase 1 PDF output.
- **Shared datastore (no copying).** Run with `--sink postgres` (see *Datastore
  sink* below) so chunks land in the same Postgres + pgvector table Phase 3
  reads. Phase 3 fills embeddings in place.

---

## Output schema (`common.schema.Chunk`)
`text`, `source_type` (`"web"`), `source_id` (URL), `chunk_index`, `title`,
`section`, `url`, `domain`, `token_count`, `overlap_tokens`, `chunk_id`, `extra`.
Identical to Phase 1 — that's the whole point.

## Layout
```
common/        shared Chunk schema, tokenizer, chunking helpers (vendored)
phase2_web/    pipeline.py
config/        config.yaml
data/processed/  output chunks
```

## Datastore sink

Write scraped chunks straight into the shared Postgres + pgvector datastore
instead of JSONL. The datastore keeps **two separate tables**: `chunks` (text +
metadata) and `chunk_embeddings` (vectors only). Phase 2 writes **only `chunks`**;
Phase 3 fills the vectors later.

```bash
export PG_DSN=postgresql://user:pass@localhost:5432/rag
python -m phase2_web.pipeline --config config/config.yaml --sink postgres
```
**All datastore settings live in one place — `config/datastore.yaml`** (DSN via
the `PG_DSN` env var, table names, and embedding dim). No connection details are
hardcoded in code. To switch database or tables, edit that one file; to switch
the connection, set `PG_DSN` (or copy `.env.example` → `.env`). Env vars
(`PG_DSN`, `DATASTORE_BACKEND`, `DATASTORE_CONFIG`) override the file.

Idempotent by `chunk_id`; embeddings are filled later in Phase 3. Needs
`pip install "psycopg[binary]" pgvector`.

## Troubleshooting
- **Blank pages / no content** → site is JS-rendered; switch to the `firecrawl`
  backend, or raise `min_content_chars` sensitivity.
- **Crawl wanders off-site** → keep `same_domain_only: true` and lower `max_pages`.
- **Slow / rate-limited** → reduce `max_pages`; add polite delays in the fetch
  step; respect robots.txt (the marked `TODO`).
