"""CDC capture: batch polling and micro-batch streaming from the source log."""
from __future__ import annotations

import time
from typing import Iterator

from .eventlog import AppendOnlyEventLog, WatermarkStore
from .models import CDCEvent
from .source import LedgerSource


class Capturer:
    def __init__(self, source: LedgerSource, log: AppendOnlyEventLog,
                 watermark: WatermarkStore):
        self.source, self.log, self.watermark = source, log, watermark

    def poll_batch(self, limit: int | None = None) -> list[CDCEvent]:
        """Capture one batch. Watermark advances only after appends succeed,
        and append is idempotent, so re-running a batch is safe (at-least-once
        capture + idempotent sink = effectively-once landing)."""
        events = self.source.changes_since(self.watermark.get(), limit=limit)
        appended = []
        for event in events:
            if self.log.append(event):
                appended.append(event)
        if events:
            self.watermark.set(max(e.lsn for e in events))
        return appended

    def stream(self, max_batches: int = 10, interval_seconds: float = 0.0,
               limit: int | None = None) -> Iterator[list[CDCEvent]]:
        """Yield micro-batches. Stops early after two consecutive empty polls
        when max_batches is reached or when the source is drained in tests."""
        empty = 0
        for _ in range(max_batches):
            batch = self.poll_batch(limit=limit)
            if batch:
                empty = 0
                yield batch
            else:
                empty += 1
                yield batch
                if empty >= 2:
                    break
            if interval_seconds:
                time.sleep(interval_seconds)
