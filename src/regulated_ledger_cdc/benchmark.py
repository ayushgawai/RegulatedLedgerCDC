"""Benchmark: events/s and end-to-end (capture+land) latency percentiles.

Honest scope: single process, local JSONL log, in-memory bitemporal
warehouse. This measures this implementation, not Kafka/Iceberg/Spark.
"""
from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

from .bitemporal import Warehouse
from .capture import Capturer
from .eventlog import AppendOnlyEventLog, WatermarkStore
from .source import LedgerSource


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((pct / 100) * (len(ordered) - 1))))
    return ordered[idx]


def run_benchmark(n_events: int = 2000) -> dict:
    source = LedgerSource()
    for i in range(n_events):
        source.insert("ledger_entries", f"E{i:06d}", {
            "entry_id": f"E{i:06d}", "account_id": f"A{i % 50:03d}",
            "amount_minor": 100 + (i % 1000), "currency": "USD", "status": "POSTED"})
    latencies_ms: list[float] = []
    with tempfile.TemporaryDirectory() as tmp:
        log = AppendOnlyEventLog(Path(tmp) / "log.jsonl")
        capturer = Capturer(source, log, WatermarkStore(Path(tmp) / "wm.json"))
        warehouse = Warehouse()
        events = source.changes_since(0)
        start = time.perf_counter()
        for event in events:
            t0 = time.perf_counter_ns()
            log.append(event)
            warehouse.apply(event)
            latencies_ms.append((time.perf_counter_ns() - t0) / 1e6)
        elapsed = time.perf_counter() - start
    return {
        "n_events": n_events, "elapsed_s": round(elapsed, 4),
        "events_per_s": round(n_events / elapsed, 1) if elapsed else 0,
        "latency_ms_p50": round(statistics.median(latencies_ms), 4) if latencies_ms else 0,
        "latency_ms_p95": round(_percentile(latencies_ms, 95), 4),
        "latency_ms_p99": round(_percentile(latencies_ms, 99), 4),
        "latency_ms_max": round(max(latencies_ms), 4) if latencies_ms else 0,
        "scope": "single-process, local JSONL log + in-memory warehouse; no network/Kafka/Spark",
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="ledger-cdc-bench")
    parser.add_argument("-n", "--events", type=int, default=2000)
    args = parser.parse_args(argv)
    print(json.dumps(run_benchmark(args.events), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
