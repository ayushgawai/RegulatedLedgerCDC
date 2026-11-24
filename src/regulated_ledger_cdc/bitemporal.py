"""Bitemporal landing into a lakehouse-style warehouse.

Two time axes are kept for every version:
  * valid time       — when the fact was true in the business world
  * transaction time — when the warehouse learned / stopped believing it

The warehouse below models an Iceberg/Delta table (snapshot manifest +
appended version files) with local files. It does not require a JVM,
Spark, or object storage; the bitemporal and snapshot semantics are the
same ones a real Iceberg/Delta MERGE would implement, and the adapter
boundary (``WarehouseTable.apply``) is where that MERGE would live.
"""
from __future__ import annotations

import copy
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import CDCEvent, Op, canonical_hash


@dataclass
class VersionedRecord:
    table: str
    pk: str
    payload: dict[str, Any] | None  # None = tombstone (delete)
    valid_from: datetime
    valid_to: datetime | None
    tx_from: datetime
    tx_to: datetime | None
    lsn: int
    event_id: str
    source: str = "ledger"

    @property
    def is_deleted(self) -> bool:
        return self.payload is None

    def covers_valid(self, at: datetime) -> bool:
        return self.valid_from <= at and (self.valid_to is None or at < self.valid_to)

    def believed_at(self, tx_at: datetime | None) -> bool:
        if tx_at is None:
            return self.tx_to is None
        return self.tx_from <= tx_at and (self.tx_to is None or tx_at < self.tx_to)


class WarehouseTable:
    def __init__(self, name: str):
        self.name = name
        self.versions: list[VersionedRecord] = []

    def _current(self, pk: str) -> list[VersionedRecord]:
        return sorted(
            [v for v in self.versions if v.pk == pk and v.tx_to is None],
            key=lambda v: v.valid_from,
        )

    def apply(self, event: CDCEvent) -> VersionedRecord:
        vf, tx = event.valid_time, event.tx_time
        current = self._current(event.pk)
        successors = [v.valid_from for v in current if v.valid_from > vf]
        valid_to = min(successors) if successors else None

        # Supersede / split every currently-believed version overlapping
        # [vf, valid_to). A version starting before vf is truncated, not lost.
        for v in list(current):
            v_end = v.valid_to or datetime.max.replace(tzinfo=vf.tzinfo)
            new_end = valid_to or datetime.max.replace(tzinfo=vf.tzinfo)
            if v.valid_from < new_end and vf < v_end:  # overlap
                was_from = v.valid_from
                v.tx_to = tx
                if was_from < vf:
                    self.versions.append(VersionedRecord(
                        table=self.name, pk=event.pk,
                        payload=copy.deepcopy(v.payload),
                        valid_from=was_from, valid_to=vf,
                        tx_from=tx, tx_to=None, lsn=v.lsn,
                        event_id=v.event_id, source=v.source,
                    ))
        record = VersionedRecord(
            table=self.name, pk=event.pk,
            payload=None if event.op == Op.DELETE else copy.deepcopy(event.after),
            valid_from=vf, valid_to=valid_to, tx_from=tx, tx_to=None,
            lsn=event.lsn, event_id=event.event_id, source=event.source,
        )
        self.versions.append(record)
        return record

    # -- queries -----------------------------------------------------------
    def point_in_time(self, pk: str, valid_at: datetime,
                      tx_at: datetime | None = None) -> dict | None:
        candidates = [v for v in self.versions
                      if v.pk == pk and v.covers_valid(valid_at) and v.believed_at(tx_at)]
        if not candidates:
            return None
        # Latest belief, then latest valid_from wins.
        best = max(candidates, key=lambda v: (v.tx_from, v.valid_from, v.lsn))
        return None if best.is_deleted else copy.deepcopy(best.payload)

    def current_view(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for pk in sorted({v.pk for v in self.versions}):
            rows = self._current(pk)
            if rows:
                latest = max(rows, key=lambda v: v.valid_from)
                if not latest.is_deleted:
                    out[pk] = copy.deepcopy(latest.payload)
        return out

    def as_of(self, valid_at: datetime, tx_at: datetime | None = None) -> dict[str, dict]:
        out = {}
        for pk in sorted({v.pk for v in self.versions}):
            row = self.point_in_time(pk, valid_at, tx_at)
            if row is not None:
                out[pk] = row
        return out

    def history(self, pk: str) -> list[VersionedRecord]:
        return sorted([v for v in self.versions if v.pk == pk],
                      key=lambda v: (v.valid_from, v.tx_from, v.lsn))


class Warehouse:
    """Iceberg/Delta-style warehouse: tables + snapshot manifest + lineage."""

    def __init__(self, root: str | Path | None = None):
        self.root = Path(root) if root else None
        self.tables: dict[str, WarehouseTable] = {}
        self.snapshots: list[dict[str, Any]] = []
        if self.root:
            self.root.mkdir(parents=True, exist_ok=True)

    def table(self, name: str) -> WarehouseTable:
        return self.tables.setdefault(name, WarehouseTable(name))

    def apply(self, event: CDCEvent) -> VersionedRecord:
        return self.table(event.table).apply(event)

    def apply_many(self, events) -> list[VersionedRecord]:
        # Land in LSN (commit) order; valid-time order is handled inside
        # the bitemporal apply, so late / out-of-order facts still resolve.
        ordered = sorted(events, key=lambda e: e.lsn)
        out = [self.apply(e) for e in ordered]
        if ordered:
            self._snapshot(ordered)
        return out

    def _snapshot(self, events) -> None:
        snap = {
            "snapshot_id": len(self.snapshots) + 1,
            "parent_id": self.snapshots[-1]["snapshot_id"] if self.snapshots else None,
            "lsn_from": min(e.lsn for e in events), "lsn_to": max(e.lsn for e in events),
            "event_count": len(events),
            "event_ids": [e.event_id for e in events],
            "tables": sorted({e.table for e in events}),
        }
        self.snapshots.append(snap)
        self._persist()

    def lineage(self, table: str, pk: str) -> list[dict[str, Any]]:
        tbl = self.tables.get(table)
        if not tbl:
            return []
        return [{
            "pk": v.pk, "event_id": v.event_id, "lsn": v.lsn, "source": v.source,
            "valid_from": v.valid_from.isoformat(),
            "tx_from": v.tx_from.isoformat(),
            "row_hash": canonical_hash(v.payload),
        } for v in tbl.history(pk)]

    def _persist(self) -> None:
        if not self.root:
            return
        (self.root / "manifest.json").write_text(
            json.dumps(self.snapshots, indent=2), encoding="utf-8")
        rows = []
        for tbl in self.tables.values():
            for v in tbl.versions:
                d = asdict(v)
                for k in ("valid_from", "valid_to", "tx_from", "tx_to"):
                    d[k] = d[k].isoformat() if d[k] else None
                rows.append(d)
        (self.root / "versions.jsonl").write_text(
            "\n".join(json.dumps(r, sort_keys=True, default=str) for r in rows) + ("\n" if rows else ""),
            encoding="utf-8")
