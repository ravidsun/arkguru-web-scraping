"""
Phase 2 orchestrator: web pages -> the SAME Chunk schema as Phase 1.

This is a runnable local-first skeleton. The local backend (httpx + trafilatura)
works out of the box for well-behaved static sites; the Firecrawl backend is a
drop-in cloud option for JS-heavy sites or large crawls.

    python -m phase2_web.pipeline --config config/config.yaml

Independent of Phase 1: different inputs (URLs), same outputs (chunks), so
Phase 3 concatenates both without any glue.
"""
from __future__ import annotations

import argparse, logging, os, re, sys, time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, urljoin, urlunparse
from urllib.robotparser import RobotFileParser

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
    sink: str = "file"                 # file | postgres
    datastore_config: str = "config/datastore.yaml"
    # --- crawl frontier ---
    allow_path_prefixes: list[str] = field(default_factory=list)
    deny_path_prefixes: list[str] = field(default_factory=list)
    respect_robots: bool = True
    crawl_delay_s: float = 0.25
    user_agent: str = "domain-rag/1.0"


# --- fetch + extract -------------------------------------------------------
def _fetch_local(url: str, user_agent: str = "domain-rag/1.0") -> tuple[str, str, list[str]]:
    """Return (markdown_text, title, discovered_links). Local, no API key."""
    import httpx, trafilatura
    from selectolax.parser import HTMLParser
    html = httpx.get(url, timeout=20, follow_redirects=True,
                     headers={"User-Agent": user_agent}).text
    text = trafilatura.extract(html, include_comments=False, include_tables=True,
                               output_format="markdown") or ""
    tree = HTMLParser(html)
    title = (tree.css_first("title").text() if tree.css_first("title") else "") or ""
    links = [a.attributes.get("href", "") for a in tree.css("a[href]")]
    links = [urljoin(url, h) for h in links if h and not h.startswith("#")]
    return text, title.strip(), links


def _as_dict(res) -> dict:
    if isinstance(res, dict):
        return res
    out = {}
    for key in ("markdown", "links", "metadata"):
        if hasattr(res, key):
            out[key] = getattr(res, key)
    data = getattr(res, "data", None)
    if data is not None:
        if isinstance(data, dict):
            out.update(data)
        else:
            for key in ("markdown", "links", "metadata"):
                if hasattr(data, key):
                    out[key] = getattr(data, key)
    return out


def _fetch_firecrawl(url: str, api_key: str) -> tuple[str, str, list[str]]:
    """Cloud backend. Needs an API key from firecrawl_api_key_env."""
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=api_key)
    except ImportError:
        try:
            from firecrawl import Firecrawl
            app = Firecrawl(api_key=api_key)
        except ImportError as e:
            raise RuntimeError(
                "firecrawl-py is required for backend=firecrawl. "
                "pip install firecrawl-py"
            ) from e
    scrape = getattr(app, "scrape_url", None) or getattr(app, "scrape")
    try:
        res = scrape(url, params={"formats": ["markdown", "links"]})
    except TypeError:
        res = scrape(url, formats=["markdown", "links"])
    data = _as_dict(res)
    meta = data.get("metadata") or {}
    if not isinstance(meta, dict):
        meta = dict(meta) if meta else {}
    links = data.get("links") or []
    if isinstance(links, dict):
        links = list(links.values())
    return data.get("markdown") or "", meta.get("title") or "", list(links)


_SKIP_SUFFIXES = (
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".gz", ".tgz", ".rar", ".7z",
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
    ".css", ".js", ".mjs", ".map", ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mp3", ".avi", ".mov", ".wav",
)


def normalize_url(url: str) -> str | None:
    """Strip fragments, default ports, and trailing slashes (except root)."""
    try:
        p = urlparse(url.strip())
    except Exception:
        return None
    if p.scheme not in ("http", "https") or not p.netloc:
        return None
    host = (p.hostname or "").lower()
    if not host:
        return None
    port = p.port
    if port in (80, 443, None):
        netloc = host
    else:
        netloc = f"{host}:{port}"
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query = p.query
    return urlunparse((p.scheme, netloc, path, "", query, ""))


def _path_ok(path: str, allow: list[str], deny: list[str]) -> bool:
    path = path or "/"

    def matches(prefix: str) -> bool:
        p = prefix if prefix.startswith("/") else "/" + prefix
        p = p.rstrip("/") or "/"
        return path == p or path.startswith(p + "/")

    if any(matches(prefix) for prefix in deny):
        return False
    if not allow:
        return True
    return any(matches(prefix) for prefix in allow)


class _RobotsCache:
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self._parsers: dict[str, RobotFileParser | None] = {}

    def allowed(self, url: str) -> bool:
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin not in self._parsers:
            rp = RobotFileParser()
            rp.set_url(urljoin(origin + "/", "robots.txt"))
            try:
                rp.read()
                self._parsers[origin] = rp
            except Exception:
                self._parsers[origin] = None
                return True
        rp = self._parsers[origin]
        if rp is None:
            return True
        try:
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            return True


def _frontier_priority(url: str) -> tuple[int, int, str]:
    """Lower tuple sorts first: fewer path segments, then shorter URL."""
    path = urlparse(url).path or "/"
    segments = [s for s in path.split("/") if s]
    return (len(segments), len(url), url)


def should_enqueue(url: str, cfg: Phase2Config, seed_domains: set[str]) -> bool:
    """Apply domain, suffix, and path-prefix filters (robots checked at fetch time)."""
    parsed = urlparse(url)
    if cfg.same_domain_only and parsed.netloc not in seed_domains:
        return False
    path_lower = (parsed.path or "").lower()
    if any(path_lower.endswith(ext) for ext in _SKIP_SUFFIXES):
        return False
    if not _path_ok(parsed.path or "/", cfg.allow_path_prefixes, cfg.deny_path_prefixes):
        return False
    return True


# --- crawl frontier --------------------------------------------------------
def _crawl(cfg: Phase2Config):
    from collections import deque
    seen: set[str] = set()
    seed_urls = []
    for s in cfg.seeds:
        n = normalize_url(s)
        if n:
            seed_urls.append(n)
    q: deque[str] = deque(seed_urls)
    seed_domains = {urlparse(s).netloc for s in seed_urls}
    robots = _RobotsCache(cfg.user_agent) if cfg.respect_robots else None

    if cfg.backend == "firecrawl":
        api_key = os.environ.get(cfg.firecrawl_api_key_env, "").strip()
        if not api_key:
            raise RuntimeError(
                f"Firecrawl backend requires ${cfg.firecrawl_api_key_env}. "
                "Set the env var or switch backend to 'local'."
            )
        def fetch(url: str):
            return _fetch_firecrawl(url, api_key)
    else:
        def fetch(url: str):
            return _fetch_local(url, cfg.user_agent)

    while q and len(seen) < cfg.max_pages:
        url = q.popleft()
        if url in seen:
            continue
        if robots and not robots.allowed(url):
            log.info("robots.txt disallows %s", url)
            seen.add(url)
            continue
        seen.add(url)
        try:
            text, title, links = fetch(url)
        except Exception as e:
            log.warning("fetch failed %s: %s", url, e)
            continue
        if cfg.crawl_delay_s > 0:
            time.sleep(cfg.crawl_delay_s)
        if len(text) >= cfg.min_content_chars:
            yield url, title, text
        discovered: list[str] = []
        for link in links:
            n = normalize_url(link)
            if not n or n in seen:
                continue
            if not should_enqueue(n, cfg, seed_domains):
                continue
            discovered.append(n)
        discovered.sort(key=_frontier_priority)
        for n in discovered:
            q.append(n)


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
    chunks, idx = [], 0
    for url, title, md in _crawl(cfg):
        pc = _page_to_chunks(url, title, md, cfg, idx)
        idx += len(pc)
        chunks.extend(pc)
        log.info("%s -> %d chunks", url, len(pc))
    before = len(chunks)
    chunks = _dedup_minhash(chunks)
    log.info("Dedup: %d -> %d", before, len(chunks))
    if cfg.sink == "postgres":
        from common.datastore_config import open_chunk_store
        store = open_chunk_store(cfg.datastore_config)
        store.ensure_schema()
        n = store.upsert(chunks)
        log.info("Upserted %d web chunks into the pgvector datastore", n)
        return chunks
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
    ap.add_argument("--sink", choices=["file", "postgres"])
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
