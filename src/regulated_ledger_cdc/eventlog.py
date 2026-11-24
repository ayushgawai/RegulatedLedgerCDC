"""Append-only, hash-chained event log (the CDC transport / bronze log)."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterator

from .models import CDCEvent


def _entry_hash(prev_hash: str, event_dict: dict) -> str:
    payload = prev_hash + json.dumps(event_dict, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


class AppendOnlyEventLog:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._seen_ids: set[str] = set()
        self._seen_lsn: set[tuple[str, int]] = set()
        self._last_hash = "GENESIS"
        self._count = 0
        if self.path.exists():
            for entry in self._iter_entries():
                ev = entry["event"]
                self._seen_ids.add(ev["event_id"])
                self._seen_lsn.add((ev.get("source", "ledger"), int(ev["lsn"])))
                self._last_hash = entry["hash"]
                self._count += 1

    def _iter_entries(self) -> Iterator[dict]:
        with self.path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)

    def __len__(self) -> int:
        return self._count

    def append(self, event: CDCEvent) -> bool:
        """Append iff not a duplicate (event_id or source+LSN). Idempotent."""
        key = (event.source, event.lsn)
        if event.event_id in self._seen_ids or key in self._seen_lsn:
            return False
        ev_dict = event.to_dict()
        digest = _entry_hash(self._last_hash, ev_dict)
        entry = {"event": ev_dict, "prev_hash": self._last_hash, "hash": digest}
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, sort_keys=True) + "\n")
            fh.flush()
        self._seen_ids.add(event.event_id)
        self._seen_lsn.add(key)
        self._last_hash = digest
        self._count += 1
        return True

    def append_many(self, events) -> int:
        return sum(1 for e in events if self.append(e))

    def read_all(self) -> list[CDCEvent]:
        if not self.path.exists():
            return []
        return [CDCEvent.from_dict(e["event"]) for e in self._iter_entries()]

    def read_from(self, lsn: int, source: str | None = None) -> list[CDCEvent]:
        return [e for e in self.read_all()
                if e.lsn > lsn and (source is None or e.source == source)]

    def verify_chain(self) -> bool:
        prev = "GENESIS"
        if not self.path.exists():
            return True
        for entry in self._iter_entries():
            if entry.get("prev_hash") != prev:
                return False
            if _entry_hash(prev, entry["event"]) != entry.get("hash"):
                return False
            prev = entry["hash"]
        return True


class WatermarkStore:
    """Durable capture watermark (last LSN safely appended to the log)."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def get(self) -> int:
        if not self.path.exists():
            return 0
        try:
            return int(json.loads(self.path.read_text(encoding="utf-8"))["lsn"])
        except Exception:
            return 0

    def set(self, lsn: int) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"lsn": int(lsn)}), encoding="utf-8")
        tmp.replace(self.path)  # atomic on same filesystem
