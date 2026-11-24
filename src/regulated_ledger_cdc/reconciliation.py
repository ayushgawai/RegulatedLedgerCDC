"""Source-to-target reconciliation."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .bitemporal import Warehouse
from .models import canonical_hash


@dataclass
class ReconciliationReport:
    table: str
    source_count: int
    target_count: int
    missing_in_target: list[str] = field(default_factory=list)
    extra_in_target: list[str] = field(default_factory=list)
    mismatched: list[dict[str, Any]] = field(default_factory=list)
    source_checksum: str = ""
    target_checksum: str = ""
    numeric_totals: dict[str, dict[str, float]] = field(default_factory=dict)

    @property
    def status(self) -> str:
        ok = (not self.missing_in_target and not self.extra_in_target
              and not self.mismatched)
        return "PASS" if ok else "FAIL"

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table, "status": self.status,
            "source_count": self.source_count, "target_count": self.target_count,
            "missing_in_target": self.missing_in_target,
            "extra_in_target": self.extra_in_target,
            "mismatched": self.mismatched,
            "source_checksum": self.source_checksum,
            "target_checksum": self.target_checksum,
            "numeric_totals": self.numeric_totals,
        }


def _table_checksum(rows: dict[str, dict]) -> str:
    import hashlib
    parts = [f"{pk}:{canonical_hash(row)}" for pk, row in sorted(rows.items())]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def reconcile_table(source_rows: dict[str, dict], target_rows: dict[str, dict],
                    table: str = "", numeric_fields: tuple[str, ...] = ("amount_minor", "balance_minor")) -> ReconciliationReport:
    missing = sorted(set(source_rows) - set(target_rows))
    extra = sorted(set(target_rows) - set(source_rows))
    mismatched = []
    for pk in sorted(set(source_rows) & set(target_rows)):
        sh, th = canonical_hash(source_rows[pk]), canonical_hash(target_rows[pk])
        if sh != th:
            mismatched.append({"pk": pk, "source_hash": sh, "target_hash": th,
                               "source": source_rows[pk], "target": target_rows[pk]})
    totals: dict[str, dict[str, float]] = {}
    for f in numeric_fields:
        if any(f in r for r in source_rows.values()) or any(f in r for r in target_rows.values()):
            totals[f] = {
                "source": sum(r.get(f, 0) or 0 for r in source_rows.values()),
                "target": sum(r.get(f, 0) or 0 for r in target_rows.values()),
            }
    return ReconciliationReport(
        table=table, source_count=len(source_rows), target_count=len(target_rows),
        missing_in_target=missing, extra_in_target=extra, mismatched=mismatched,
        source_checksum=_table_checksum(source_rows),
        target_checksum=_table_checksum(target_rows), numeric_totals=totals)


def reconcile_source(source_snapshot: dict, warehouse: Warehouse,
                     table: str) -> ReconciliationReport:
    tbl = warehouse.tables.get(table)
    target = tbl.current_view() if tbl else {}
    return reconcile_table(source_snapshot.get(table, {}), target, table=table)
