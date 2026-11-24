"""Synthetic regulated transactional ledger source.

In production this adapter would sit on a database log (e.g. WAL / LogMiner /
Debezium-style connector). Here it is a deterministic in-memory + commit-log
model of that source so the CDC semantics — monotonic LSN, before/after
images, valid time vs. commit time — can be tested without a database.
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from .models import CDCEvent, Op, parse_dt, utcnow

DEFAULT_SCHEMA: dict[str, dict[str, str]] = {
    "ledger_entries": {
        "entry_id": "str", "account_id": "str", "amount_minor": "int",
        "currency": "str", "status": "str",
    },
    "ledger_accounts": {
        "account_id": "str", "balance_minor": "int", "currency": "str", "status": "str",
    },
}


class LedgerSource:
    def __init__(self, name: str = "ledger", schema: dict | None = None):
        self.name = name
        self.schema: dict[str, dict[str, str]] = copy.deepcopy(schema or DEFAULT_SCHEMA)
        self.tables: dict[str, dict[str, dict[str, Any]]] = {t: {} for t in self.schema}
        self.commit_log: list[CDCEvent] = []
        self._lsn = 0

    # -- mutations ---------------------------------------------------------
    def _commit(self, table, pk, op, before, after, valid_time, tx_time) -> CDCEvent:
        self._lsn += 1
        event = CDCEvent(
            lsn=self._lsn, table=table, pk=str(pk), op=op,
            before=copy.deepcopy(before), after=copy.deepcopy(after),
            valid_time=parse_dt(valid_time) if valid_time else utcnow(),
            tx_time=parse_dt(tx_time) if tx_time else utcnow(),
            source=self.name, schema_version=1,
        )
        self.commit_log.append(event)
        return event

    def insert(self, table, pk, row, valid_time=None, tx_time=None) -> CDCEvent:
        if pk in self.tables[table]:
            raise ValueError(f"duplicate pk {pk!r} in {table}")
        self.tables[table][str(pk)] = copy.deepcopy(row)
        return self._commit(table, pk, Op.INSERT, None, row, valid_time, tx_time)

    def update(self, table, pk, row, valid_time=None, tx_time=None) -> CDCEvent:
        before = self.tables[table].get(str(pk))
        if before is None:
            raise KeyError(pk)
        self.tables[table][str(pk)] = copy.deepcopy(row)
        return self._commit(table, pk, Op.UPDATE, before, row, valid_time, tx_time)

    def upsert(self, table, pk, row, valid_time=None, tx_time=None) -> CDCEvent:
        if str(pk) in self.tables[table]:
            return self.update(table, pk, row, valid_time, tx_time)
        return self.insert(table, pk, row, valid_time, tx_time)

    def delete(self, table, pk, valid_time=None, tx_time=None) -> CDCEvent:
        before = self.tables[table].pop(str(pk), None)
        if before is None:
            raise KeyError(pk)
        return self._commit(table, pk, Op.DELETE, before, None, valid_time, tx_time)

    def evolve_schema(self, table: str, column: str, type_name: str) -> None:
        """Simulate a source schema change (used by drift tests)."""
        self.schema[table][column] = type_name

    def drop_column(self, table: str, column: str) -> None:
        self.schema[table].pop(column, None)

    # -- reads -------------------------------------------------------------
    @property
    def max_lsn(self) -> int:
        return self._lsn

    def changes_since(self, lsn: int, limit: int | None = None) -> list[CDCEvent]:
        out = [e for e in self.commit_log if e.lsn > lsn]
        return out[:limit] if limit else out

    def snapshot(self) -> dict[str, dict[str, dict[str, Any]]]:
        return copy.deepcopy(self.tables)

    def current_schema(self) -> dict[str, dict[str, str]]:
        return copy.deepcopy(self.schema)


def seed_demo_source(n: int = 20, name: str = "ledger") -> LedgerSource:
    src = LedgerSource(name=name)
    for i in range(n):
        src.insert("ledger_entries", f"E{i:05d}", {
            "entry_id": f"E{i:05d}", "account_id": f"A{i % 5:03d}",
            "amount_minor": 100 + i, "currency": "USD",
            "status": "POSTED" if i % 7 else "PENDING",
        })
    return src
