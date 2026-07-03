# Phase 2 — Web Scraping & Data Collection

Crawl websites and convert them into the **same** portable `Chunk` schema as
Phase 1, so both feed Phase 3 with zero glue. Independent process, different
inputs (URLs), identical outputs. Local-first, cloud-optional.

## What it does
- Crawl seed URLs (same-domain frontier, `max_pages` cap)
- Extract clean content with `trafilatura` (removes nav/ads/boilerplate) → markdown
- Structure-aware chunking (shared `common/chunking.py`, same sizing as Phase 1)
- Near-duplicate removal via MinHash LSH (`datasketch`)
- Metadata: URL, domain, title, section

## Backends
| Backend | Deps | Use |
|---|---|---|
| `local` (default) | httpx + trafilatura + selectolax | static sites, no keys |
| `firecrawl` | `FIRECRAWL_API_KEY` | JS-heavy sites, large crawls |

## Quick start
```bash
pip install -r requirements.txt
# set phase2.seeds in config/config.yaml, then:
make phase2                            # -> data/processed/web_chunks.jsonl
# or: python -m phase2_web.pipeline --seeds https://your.site/docs --max-pages 100
```

Two `TODO`s are intentional policy choices left to you: crawl-frontier rules
(path prefixes, robots.txt, priorities) and the Firecrawl key.

## Layout
```
common/        shared Chunk schema, tokenizer, chunking helpers (vendored)
phase2_web/    pipeline.py
config/        config.yaml
data/processed/  output chunks
```
