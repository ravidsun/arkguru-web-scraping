# arkguru-web-scraping — Phase 2

Crawl websites and convert them into the **same** `Chunk` records as Phase 1, so
both feed Phase 3 with zero conversion. **Optional** if you only have PDFs.
Independent process (URLs instead of PDFs), identical output schema.
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
  datastore_config: "config/datastore.yaml"
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

> **Setting up the database?** See [docs/DATABASE_SETUP.md](docs/DATABASE_SETUP.md) for step-by-step Postgres + pgvector setup on a local machine (Docker/Homebrew/apt) or a VPS (remote access, firewall, SSL/SSH tunnel).

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

Idempotent by `chunk_id` (conflict upsert does not reset `created_at`). Embeddings
are filled later in Phase 3. Needs `pip install "psycopg[binary]" pgvector`.

## Backend comparison in detail

### Local backend (trafilatura)
- **Pros:** No API key, runs anywhere, full control, can be rate-limited to respect site policies
- **Cons:** Doesn't handle JavaScript-heavy sites (returns blank or boilerplate), fails on sites with aggressive anti-bot detection
- **Best for:** Documentation sites, static blogs, traditional CMSs (WordPress, etc.), internal wikis
- **Speed:** ~200–500 ms per page (including network + parsing)

### Firecrawl backend
- **Pros:** Handles dynamic JS-rendered content, waits for page load, built-in anti-bot evasion
- **Cons:** Requires API key + paid plan, rate-limited, cloud dependency
- **Best for:** Single-page apps, JavaScript frameworks (React, Vue), sites with render-on-scroll, news/media sites
- **Speed:** ~1–3 s per page (cloud service overhead), priced per crawl

**Decision tree:** Start with `local` (free, offline). If you see blank pages or only navigation elements, switch to `firecrawl` for that site or add it to a separate crawl with that backend.

## Configuration deep dive

### Crawl frontier (`seeds`, `max_pages`, `same_domain_only`)
- `seeds`: list of entry URLs (can be multiple to start multiple crawls in one run)
- `max_pages`: hard cap on total pages visited; reached first page after hitting this limit is dropped
- `same_domain_only`: if `true`, any discovered links not on `seeds` domain are dropped. Set to `false` to crawl cross-domain (e.g., `docs.example.com` → `example.com` → `blog.example.com`). Use `false` cautiously; crawl explosion is real.

**Example: crawl documentation + blog in one run:**
```yaml
phase2:
  seeds: ["https://docs.example.com", "https://blog.example.com"]
  same_domain_only: false   # allows crawl to move between subdomains
  max_pages: 500
```

### Content filtering (`min_content_chars`, `target_tokens`)
- `min_content_chars`: skip pages with less text (helps exclude nav-only or empty pages)
- `target_tokens`: chunk size in tokens; keep 400–550 for retrieval, 600+ for training

### Deduplication sensitivity
Exact dedup (text match) is always run. Near-dedup via MinHash (if `datasketch` is installed) collapses pages sharing ≥90% content (common for syndicated news, paginated lists). If dedup removes too many pages:
```bash
pip uninstall datasketch     # falls back to exact-match only
```

## Troubleshooting

### Blank pages / no content
→ site is JS-rendered; switch to the `firecrawl` backend, or raise `min_content_chars` sensitivity. You can also check a single URL manually:
```python
from phase2_web.fetch import fetch_local
markdown, title, links = fetch_local("https://your.site/page")
print(markdown[:500])  # inspect first 500 chars
```

### Crawl wanders off-site
→ keep `same_domain_only: true` and lower `max_pages`. If you intentionally want cross-domain crawl, set `same_domain_only: false` and use a tighter `max_pages` (e.g., 100 instead of 500).

### Slow / rate-limited
→ reduce `max_pages`; add polite delays in the fetch step (modify `fetch.py`); respect robots.txt (the marked `TODO` in `pipeline.py`). If using firecrawl, contact their support to increase rate limits.

### Extracted markdown is malformed
→ trafilatura occasionally over-strips or misparses complex HTML. For that domain, try firecrawl instead. If problem persists, open an issue with the URL and problematic output.

### Memory usage grows unbounded
→ crawl frontier queue grows with each page's discovered links. Limit `max_pages` or reduce `same_domain_only` scope. For corpora >10K pages, consider chunking into multiple runs (e.g., crawl `/docs` separately from `/blog`).
