"""
Shared building blocks for autonomous workers (no LLM).

A `Worker` wraps a `run_once()` callable in a resilient loop: run, log, sleep,
repeat -- with graceful shutdown on SIGINT/SIGTERM and per-cycle error isolation
(one failure never kills the loop). `FolderState` tracks which input files have
already been processed (by path + mtime + size) so a folder-watching worker only
handles new or changed files.

These are deterministic: the "agency" is in the trigger (a schedule or a new
file), not in an LLM. Each phase provides its own `run_once()`.
"""
from __future__ import annotations

import json
import logging
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

log = logging.getLogger("worker")


class Worker:
    def __init__(self, name: str, run_once: Callable[[], object],
                 interval: float = 30.0):
        self.name = name
        self._run_once = run_once
        self.interval = interval
        self._stop = False

    def _install_signals(self):
        def handler(signum, _frame):
            log.info("[%s] signal %s -> stopping after current cycle", self.name, signum)
            self._stop = True
        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(s, handler)
            except ValueError:
                pass  # not in main thread

    def run_once(self):
        t0 = time.perf_counter()
        log.info("[%s] cycle start", self.name)
        try:
            result = self._run_once()
            log.info("[%s] cycle ok in %.1fs (%s)", self.name,
                     time.perf_counter() - t0, result)
            return result
        except Exception:
            log.exception("[%s] cycle FAILED (loop continues)", self.name)
            return None

    def run_forever(self):
        self._install_signals()
        log.info("[%s] starting; interval=%ss. Ctrl-C to stop.", self.name, self.interval)
        while not self._stop:
            self.run_once()
            # sleep in small slices so shutdown is responsive
            slept = 0.0
            while not self._stop and slept < self.interval:
                time.sleep(min(1.0, self.interval - slept))
                slept += 1.0
        log.info("[%s] stopped.", self.name)


@dataclass
class FolderState:
    """Remembers processed files so a watcher only picks up new/changed ones."""
    state_path: str
    _seen: dict[str, list] = field(default_factory=dict)

    def __post_init__(self):
        p = Path(self.state_path)
        if p.exists():
            try:
                self._seen = json.loads(p.read_text())
            except Exception:
                self._seen = {}

    def _sig(self, f: Path) -> list:
        st = f.stat()
        return [int(st.st_mtime), st.st_size]

    def new_or_changed(self, files: list[Path]) -> list[Path]:
        out = []
        for f in files:
            sig = self._sig(f)
            if self._seen.get(str(f)) != sig:
                out.append(f)
        return out

    def mark(self, files: list[Path]) -> None:
        for f in files:
            try:
                self._seen[str(f)] = self._sig(f)
            except FileNotFoundError:
                continue
        p = Path(self.state_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self._seen))
