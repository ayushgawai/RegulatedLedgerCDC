"""Data observability: freshness, volume, schema drift, null anomaly."""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

from .models import parse_dt, utcnow


@dataclass
class Alert:
    check: str
    severity: str  # INFO | WARN | CRITICAL
    message: str
    value: Any = None
    threshold: Any = None

    def to_dict(self):
        return {"check": self.check, "severity": self.severity,
                "message": self.message, "value": self.value, "threshold": self.threshold}


# -- schema drift -----------------------------------------------------------

def infer_schema(rows: Iterable[dict]) -> dict[str, str]:
    schema: dict[str, str] = {}
    for row in rows:
        for key, value in row.items():
            schema.setdefault(key, type(value).__name__ if value is not None else "null")
            if schema[key] == "null" and value is not None:
                schema[key] = type(value).__name__
    return schema


@dataclass
class DriftReport:
    added: list[str]
    removed: list[str]
    type_changed: dict[str, dict[str, str]]

    @property
    def has_drift(self) -> bool:
        return bool(self.added or self.removed or self.type_changed)

    def to_dict(self):
        return {"added": self.added, "removed": self.removed,
                "type_changed": self.type_changed, "has_drift": self.has_drift}


def detect_schema_drift(baseline: dict[str, str], current: dict[str, str]) -> DriftReport:
    added = sorted(set(current) - set(baseline))
    removed = sorted(set(baseline) - set(current))
    changed = {k: {"from": baseline[k], "to": current[k]}
               for k in sorted(set(baseline) & set(current)) if baseline[k] != current[k]}
    return DriftReport(added=added, removed=removed, type_changed=changed)


# -- null anomaly -----------------------------------------------------------

def null_rates(rows: list[dict]) -> dict[str, float]:
    if not rows:
        return {}
    cols = sorted({k for r in rows for k in r})
    return {c: sum(1 for r in rows if r.get(c) is None) / len(rows) for c in cols}


def null_anomalies(rows: list[dict], baseline_rates: dict[str, float],
                   delta_threshold: float = 0.2) -> dict[str, dict[str, float]]:
    current = null_rates(rows)
    out = {}
    for col, rate in current.items():
        base = baseline_rates.get(col, 0.0)
        if rate - base >= delta_threshold:
            out[col] = {"baseline": base, "current": rate}
    return out


# -- checks -> alerts -------------------------------------------------------

def freshness_check(events, now: datetime | None = None,
                    threshold_seconds: float = 3600) -> Alert | None:
    if not events:
        return Alert("freshness", "CRITICAL", "no events observed", None, threshold_seconds)
    latest = max(parse_dt(e.tx_time) if not hasattr(e, "tx_time") else e.tx_time for e in events)
    age = ((now or utcnow()) - latest).total_seconds()
    if age > threshold_seconds:
        return Alert("freshness", "CRITICAL" if age > 2 * threshold_seconds else "WARN",
                     f"latest event is {age:.0f}s old", age, threshold_seconds)
    return None


def volume_check(count: int, baseline_counts: list[int], z_threshold: float = 3.0,
                 min_baseline: int = 3) -> Alert | None:
    if len(baseline_counts) < min_baseline:
        return None
    mean = statistics.mean(baseline_counts)
    std = statistics.pstdev(baseline_counts) or 1.0
    z = (count - mean) / std
    if abs(z) >= z_threshold:
        return Alert("volume", "WARN", f"volume z-score {z:.2f} (count={count}, mean={mean:.1f})",
                     count, f"z>={z_threshold}")
    return None


def run_observability(events, rows: list[dict], baseline_schema: dict[str, str],
                      baseline_volumes: list[int] | None = None,
                      baseline_nulls: dict[str, float] | None = None,
                      volume_count: int | None = None,
                      freshness_threshold_seconds: float = 3600,
                      now: datetime | None = None) -> tuple[list[Alert], DriftReport]:
    alerts: list[Alert] = []
    if a := freshness_check(events, now=now, threshold_seconds=freshness_threshold_seconds):
        alerts.append(a)
    current_schema = infer_schema(rows) if rows else {}
    drift = detect_schema_drift(baseline_schema, current_schema) if baseline_schema else DriftReport([], [], {})
    if drift.has_drift:
        alerts.append(Alert("schema_drift", "WARN",
                            f"drift added={drift.added} removed={drift.removed} type_changed={drift.type_changed}",
                            drift.to_dict(), None))
    if baseline_volumes is not None:
        if a := volume_check(volume_count if volume_count is not None else len(events), baseline_volumes):
            alerts.append(a)
    if baseline_nulls is not None:
        anomalies = null_anomalies(rows, baseline_nulls)
        if anomalies:
            alerts.append(Alert("null_anomaly", "WARN",
                                f"null-rate spike in {sorted(anomalies)}", anomalies, 0.2))
    return alerts, drift
