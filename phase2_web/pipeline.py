"""
Phase 2 orchestrator: web pages -> the SAME Chunk schema as Phase 1.

This is a runnable local-first skeleton. The local backend (httpx + trafilatura)
works out of the box for well-behaved static sites; the Firecrawl backend is a
drop-in cloud option for JS-heavy sites or large crawls.

    python -m phase2_web.pipeline --config config/config.yaml

Independent of Phase 1: different inputs (URLs), same outputs (chunks), so
Phase 3 concatenates both without any glue.

DESIGN IS COMPLETE; two spots are marked TODO where you plug in your crawl
frontier policy and (optionally) your Firecrawl key.
"""
from __future__ import annotations

import argparse, logging, os, re, sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, urljoin

from common.schema import Chunk, write_jsonl, write_parquet
from common.tokenizer import count_tokens
from common.chunking import pack_windows as _pack_windows, split_sentences as _sentences

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("phase2.pipeline")


@dataclass
class Phase2Config:
    seeds: list[str] = field(default_factory=list)
    out_path: str = "data/processed/web_chunks.jsonl"
    out_format: str = "jsonl"
    backend: str = "local"                 # local | firecrawl
    firecrawl_api_key_env: str = "FIRECRAWL_API_KEY"
    max_pages: int = 200
    same_domain_only: bool = True
    target_tokens: int = 550
    overlap_pct: float = 0.12
    min_content_chars: int = 200


# --- fetch + extract -------------------------------------------------------
def _fetch_local(url: str) -> tuple[str, str, list[str]]:
    """Return (markdown_text, title, discovered_links). Local, no API key."""
    import httpx, trafilatura
    from selectolax.parser import HTMLParser
    html = httpx.get(url, timeout=20, follow_redirects=True,
                     headers={"User-Agent": "domain-rag/1.0"}).text
    text = trafilatura.extract(html, include_comments=False, include_tables=True,
                               output_format="markdown") or ""
    tree = HTMLParser(html)
    title = (tree.css_first("title").text() if tree.css_first("title") else "") or ""
    links = [a.attributes.get("href", "") for a in tree.css("a[href]")]
    links = [urljoin(url, h) for h in links if h and not h.startswith("#")]
    return text, title.strip(), links


def _fetch_firecrawl(url: str) -> tuple[str, str, list[str]]:
    """Cloud backend. Needs FIRECRAWL_API_KEY. Returns clean markdown directly."""
    from firecrawl import FirecrawlApp
    app = FirecrawlApp(api_key=os.environ[cfg_key_env])  # set below
    res = app.scrape_url(url, params={"formats": ["markdown", "links"]})
    return res.get("markdown", ""), res.get("metadata", {}).get("title", ""), res.get("links", [])


cfg_key_env = "FIRECRAWL_API_KEY"


# --- crawl frontier --------------------------------------------------------
def _crawl(cfg: Phase2Config):
    from collections import deque
    seen: set[str] = set()
    q = deque(cfg.seeds)
    seed_domains = {urlparse(s).netloc for s in cfg.seeds}
    fetch = _fetch_firecrawl if cfg.backend == "firecrawl" else _fetch_local

    while q and len(seen) < cfg.max_pages:
        url = q.popleft()
        if url in seen:
            continue
        seen.add(url)
        try:
            text, title, links = fetch(url)
        except Exception as e:
            log.warning("fetch failed %s: %s", url, e)
            continue
        if len(text) >= cfg.min_content_chars:
            yield url, title, text
        # TODO: refine frontier policy (path prefixes, robots.txt, priorities)
        for link in links:
            if link in seen:
                continue
            if cfg.same_domain_only and urlparse(link).netloc not in seed_domains:
                continue
            q.append(link)


# --- HTML markdown -> chunks (structure-aware, same packer as Phase 1) ------
_H_RE = re.compile(r"^(#{1,6})\s+(.*)$")

def _page_to_chunks(url, title, md, cfg, idx0) -> list[Chunk]:
    domain = urlparse(url).netloc
    overlap = max(1, int(cfg.target_tokens * cfg.overlap_pct))
    # split by markdown headings for structure-aware sections
    sections, cur_head, buf = [], None, []
    for line in md.splitlines():
        m = _H_RE.match(line.strip())
        if m:
            if buf: sections.append((cur_head, "\n".join(buf))); buf = []
            cur_head = re.sub(r"[*_`#]+", "", m.group(2)).strip()
        else:
            buf.append(line)
    if buf: sections.append((cur_head, "\n".join(buf)))

    out, idx = [], idx0
    for head, body in sections:
        body = body.strip()
        if not body: continue
        for w_text, ov in _pack_windows(_sentences(body), cfg.target_tokens, overlap):
            out.append(Chunk(text=w_text, source_type="web", source_id=url,
                             chunk_index=idx, title=title or None, section=head,
                             url=url, domain=domain,
                             token_count=count_tokens(w_text), overlap_tokens=ov))
            idx += 1
    return out


def _dedup_minhash(chunks: list[Chunk], threshold: float = 0.9) -> list[Chunk]:
    """Near-duplicate removal via MinHash LSH; falls back to exact if lib absent."""
    try:
        from datasketch import MinHash, MinHashLSH
    except ImportError:
        seen, out = set(), []
        for c in chunks:
            k = c.text.strip()
            if k not in seen: seen.add(k); out.append(c)
        return out
    lsh = MinHashLSH(threshold=threshold, num_perm=64)
    out = []
    for i, c in enumerate(chunks):
        mh = MinHash(num_perm=64)
        for tok in c.text.lower().split():
            mh.update(tok.encode())
        if lsh.query(mh):
            continue
        lsh.insert(str(i), mh)
        out.append(c)
    return out


def run(cfg: Phase2Config) -> list[Chunk]:
    global cfg_key_env
    cfg_key_env = cfg.firecrawl_api_key_env
    chunks, idx = [], 0
    for url, title, md in _crawl(cfg):
        pc = _page_to_chunks(url, title, md, cfg, idx)
        idx += len(pc)
        chunks.extend(pc)
        log.info("%s -> %d chunks", url, len(pc))
    before = len(chunks)
    chunks = _dedup_minhash(chunks)
    log.info("Dedup: %d -> %d", before, len(chunks))
    out = Path(cfg.out_path)
    if cfg.out_format == "parquet":
        write_parquet(chunks, out.with_suffix(".parquet"))
    else:
        write_jsonl(chunks, out.with_suffix(".jsonl"))
    log.info("Wrote %d web chunks -> %s", len(chunks), out.resolve())
    return chunks


def _load_config(path):
    import yaml
    raw = (yaml.safe_load(open(path)) or {}).get("phase2", {})
    return Phase2Config(**{k: v for k, v in raw.items()
                           if k in Phase2Config.__dataclass_fields__})


def main(argv=None):
    ap = argparse.ArgumentParser(description="Phase 2: web -> structured chunks")
    ap.add_argument("--config")
    ap.add_argument("--seeds", nargs="*")
    ap.add_argument("--out", dest="out_path")
    ap.add_argument("--backend", choices=["local", "firecrawl"])
    ap.add_argument("--max-pages", dest="max_pages", type=int)
    args = ap.parse_args(argv)
    cfg = _load_config(args.config) if args.config else Phase2Config()
    for k, v in vars(args).items():
        if k != "config" and v is not None:
            setattr(cfg, k, v)
    if not cfg.seeds:
        log.error("No seeds. Set phase2.seeds in config or pass --seeds URL ...")
        return 1
    run(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
