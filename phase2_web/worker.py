"""
Phase 2 autonomous worker: crawl the configured seeds on a schedule.

Deterministic: the trigger is the clock (or a manual --once). Each cycle runs the
full crawl -> extract -> chunk -> dedup -> write pass. Because chunk_ids are
deterministic and the datastore upsert is idempotent, re-crawling only refreshes
changed content.

    python -m phase2_web.worker --once
    python -m phase2_web.worker --interval 3600           # hourly
    python -m phase2_web.worker --sink postgres --interval 21600
"""
from __future__ import annotations
import argparse, logging
from pathlib import Path

from common.worker import Worker
from .pipeline import Phase2Config, _load_config, run

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("phase2.worker")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Phase 2 scheduled crawl worker")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--seeds", nargs="*")
    ap.add_argument("--sink", choices=["file", "postgres"])
    ap.add_argument("--interval", type=float, default=3600.0)
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args(argv)

    cfg = _load_config(a.config) if Path(a.config).exists() else Phase2Config()
    if a.seeds: cfg.seeds = a.seeds
    if a.sink: cfg.sink = a.sink
    if not cfg.seeds:
        log.error("No seeds configured. Set phase2.seeds or pass --seeds."); return 1

    def run_once():
        chunks = run(cfg)
        return f"crawled -> {len(chunks)} chunks"

    worker = Worker("phase2", run_once, interval=a.interval)
    worker.run_once() if a.once else worker.run_forever()
    return 0


if __name__ == "__main__":
    main()
