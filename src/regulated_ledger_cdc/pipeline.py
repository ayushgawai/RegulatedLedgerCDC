"""Pipeline orchestration: capture -> land -> reconcile -> observe."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .bitemporal import Warehouse
from .capture import Capturer
from .eventlog import AppendOnlyEventLog, WatermarkStore
from .observability import run_observability
from .reconciliation import reconcile_source
from .source import LedgerSource


class Pipeline:
    def __init__(self, source: LedgerSource, workdir: str | Path = "data"):
        workdir = Path(workdir)
        workdir.mkdir(parents=True, exist_ok=True)
        self.source = source
        self.log = AppendOnlyEventLog(workdir / "event_log.jsonl")
        self.watermark = WatermarkStore(workdir / "watermark.json")
        self.capturer = Capturer(source, self.log, self.watermark)
        self.warehouse = Warehouse(workdir / "warehouse")
        self.baseline_schema: dict[str, str] = {}
        self.baseline_volumes: list[int] = []
        self.baseline_nulls: dict[str, float] | None = None
        self.last_reconciliation: dict[str, Any] | None = None
        self.last_alerts: list[dict[str, Any]] = []
        self.last_run: dict[str, Any] | None = None
        self.tables = list(source.schema)

    def _land_from_log(self) -> int:
        # Replay events not yet landed (tracked by warehouse snapshot LSN).
        landed_lsn = max((s["lsn_to"] for s in self.warehouse.snapshots), default=0)
        events = self.log.read_from(landed_lsn)
        if events:
            self.warehouse.apply_many(events)
        return len(events)

    def run_batch(self, limit: int | None = None) -> dict[str, Any]:
        captured = self.capturer.poll_batch(limit=limit)
        landed = self._land_from_log()
        return self._finish_run(captured=len(captured), landed=landed)

    def run_stream(self, max_batches: int = 10, interval_seconds: float = 0.0,
                   limit: int | None = None) -> dict[str, Any]:
        captured = landed = 0
        for batch in self.capturer.stream(max_batches=max_batches,
                                          interval_seconds=interval_seconds, limit=limit):
            captured += len(batch)
            landed += self._land_from_log()
            if not batch:
                break
        return self._finish_run(captured=captured, landed=landed)

    def _finish_run(self, captured: int, landed: int) -> dict[str, Any]:
        self.baseline_volumes.append(captured)
        recon = {}
        for table in self.tables:
            report = reconcile_source(self.source.snapshot(), self.warehouse, table)
            recon[table] = report.to_dict()
        primary = self.tables[0] if self.tables else ""
        rows = list(self.source.snapshot().get(primary, {}).values()) if primary else []
        baseline_schema = self.baseline_schema or (self.source.current_schema().get(primary, {}) if primary else {})
        alerts, drift = run_observability(
            self.log.read_all(), rows, baseline_schema,
            baseline_volumes=self.baseline_volumes[:-1] or None,
            baseline_nulls=self.baseline_nulls,
            volume_count=captured)
        if captured and not self.baseline_schema and primary:
            self.baseline_schema = self.source.current_schema().get(primary, {})
            from .observability import null_rates
            self.baseline_nulls = null_rates(rows) if rows else {}
        self.last_reconciliation = recon
        self.last_alerts = [a.to_dict() for a in alerts]
        self.last_run = {
            "captured": captured, "landed": landed,
            "event_log_size": len(self.log), "chain_valid": self.log.verify_chain(),
            "watermark_lsn": self.watermark.get(),
            "reconciliation": recon,
            "alerts": self.last_alerts, "schema_drift": drift.to_dict(),
        }
        return self.last_run

    def health(self) -> dict[str, Any]:
        recon_ok = bool(self.last_reconciliation) and all(
            r["status"] == "PASS" for r in self.last_reconciliation.values())
        critical = [a for a in self.last_alerts if a["severity"] == "CRITICAL"]
        status = "HEALTHY" if (recon_ok and not critical) else ("DEGRADED" if self.last_run else "UNKNOWN")
        if critical:
            status = "UNHEALTHY"
        return {
            "status": status, "reconciliation_pass": recon_ok,
            "event_log_size": len(self.log), "chain_valid": self.log.verify_chain(),
            "watermark_lsn": self.watermark.get(),
            "snapshots": len(self.warehouse.snapshots),
            "alerts": self.last_alerts, "last_run": self.last_run,
        }
