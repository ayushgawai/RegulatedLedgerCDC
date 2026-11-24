# RegulatedLedgerCDC

Financial-grade change-data-capture and data-quality lakehouse service over a regulated transactional ledger: CDC capture from a transactional (ledger) source into an append-only, hash-chained event log, bitemporal landing into an Iceberg/Delta-style warehouse with lineage, source-to-target reconciliation, and data-observability checks (freshness, volume, schema drift, null anomaly) — exposed through a CLI, a FastAPI service, and Airflow DAG definitions.

> **Status:** working prototype with a passing test suite and a measured local benchmark (below). The warehouse models Iceberg/Delta snapshot and bitemporal semantics with local files — it is **not** a Spark/Iceberg/Kafka deployment. See [Honest limitations](#honest-limitations).

## Design

```
                        valid time (business)  +  transaction time (system)
                                          |
  +-------------------+   commit log    +----------------------+   append    +----------------------+
  |  Ledger source    | --------------> |  CDC capture          | ----------> | Append-only event log |
  |  (tables + LSN,   |  CDCEvent(lsn,  |  batch poll / stream  |  idempotent | JSONL, hash-chained,  |
  |   before/after)   |   op, before/   |  watermark resume     |             | tamper-evident        |
  +-------------------+   after, times) +----------------------+             +-----------+----------+
                                                                                       | replay by LSN
                                                                                       v
  +-------------------+   snapshot()    +----------------------+   versions  +----------------------+
  |  Reconciliation   | <-------------- |  Bitemporal warehouse | ----------> | Point-in-time queries |
  |  counts / hashes /|   current view  |  Iceberg/Delta-style: | valid x tx  | as-of(valid, tx),     |
  |  numeric totals   |                 |  snapshots, lineage   |             | current view, history |
  +-------------------+                 +----------------------+             +----------------------+
             |                                       |
             v                                       v
  +---------------------------------------------------------------+
  |  Observability: freshness - volume (z-score) - schema drift   |
  |  - null-rate anomaly  ->  alerts (INFO / WARN / CRITICAL)      |
  +---------------------------------------------------------------+
             |                                       |
             v                                       v
      FastAPI: /pipeline/health /alerts      Airflow DAG (hourly):
      /reconciliation/latest /lineage        capture >> land >> reconcile
      /metrics, POST /pipeline/run           >> observability >> publish
```

| Concern | Design | Where |
|---|---|---|
| CDC capture | Monotonic LSN commit log with before/after images; batch polling + micro-batch streaming; watermark advances only after append, append is deduplicated by `event_id` and `(source, lsn)` — at-least-once capture + idempotent sink = effectively-once landing | `source.py`, `capture.py`, `eventlog.py` |
| Event log | Append-only JSONL, each entry hash-chained to its predecessor (`verify_chain()` detects tampering) | `eventlog.py` |
| Bitemporal landing | Every version carries `[valid_from, valid_to)` × `[tx_from, tx_to)`; late corrections supersede by transaction time, out-of-order valid times split/truncate overlapping versions; deletes are tombstones, history is never rewritten | `bitemporal.py` |
| Warehouse | Iceberg/Delta-style tables: snapshot manifest (`snapshot_id`, parent, LSN range, event ids), per-key lineage with row hashes, local-file persistence (`manifest.json`, `versions.jsonl`) | `bitemporal.py` |
| Reconciliation | Source snapshot vs. target current view: counts, missing/extra keys, per-row canonical-hash mismatch, numeric totals (`amount_minor`, `balance_minor`), table checksum → `PASS`/`FAIL` | `reconciliation.py` |
| Observability | Freshness (age of latest event), volume (z-score vs. baseline batches), schema drift (added/removed/type-changed columns), null-rate anomaly (delta vs. baseline) → severity-ranked alerts | `observability.py` |
| Serving | FastAPI health / alerts / latest reconciliation / lineage / metrics; in-memory pipeline state | `api.py` |
| Orchestration | Airflow DAG definitions (hourly, catchup off, retries) shelling out to the CLI; importable without Airflow so the task graph is testable | `dags/ledger_cdc_dag.py` |
| Integrity / audit | Hash-chained log, LSN-ordered landing, lineage from every landed row back to its source event | `eventlog.py`, `bitemporal.py` |

## Run

```bash
pip install -e ".[api,dev]"

# Batch: capture -> land -> reconcile -> observe (synthetic demo source)
python -m regulated_ledger_cdc.cli --workdir data/demo --events 20 batch

# Streaming capture (micro-batches)
python -m regulated_ledger_cdc.cli --workdir data/demo --events 20 stream --max-batches 5

# Pipeline health
python -m regulated_ledger_cdc.cli health

# FastAPI service (http://127.0.0.1:8000/docs)
python -m regulated_ledger_cdc.cli serve --port 8000
#   GET  /health  /pipeline/health  /alerts  /reconciliation/latest  /metrics
#   GET  /lineage/{table}/{pk}      POST /pipeline/run

# In a deployed layout the console scripts are: ledger-cdc ... / ledger-cdc-bench
```

## Test

```bash
pytest -q
```

```
.....................                                                     [100%]
21 passed in ~1s
```

Coverage: capture (insert/update/delete, LSN order, idempotency, watermark resume, stream drain, hash-chain tamper detection), bitemporal ordering (valid-time ordering, late corrections visible only after their transaction time, out-of-order valid-time arrival, delete tombstones + history/lineage), reconciliation (pass, missing/extra/mismatch, unlanded change), drift detection (added/removed/type-changed, null anomalies), observability + FastAPI, DAG task graph, CLI, and a benchmark smoke test.

## Benchmark (sanity run, scaled down honestly)

Measured on the development machine (Windows, Python 3.12, single process) with:

```bash
python -m regulated_ledger_cdc.benchmark -n 2000
# equivalently: ledger-cdc bench -n 2000
```

```json
{
  "n_events": 2000,
  "elapsed_s": 0.909,
  "events_per_s": 2200.2,
  "latency_ms_p50": 0.4102,
  "latency_ms_p95": 0.698,
  "latency_ms_p99": 0.9281,
  "latency_ms_max": 4.5431,
  "scope": "single-process, local JSONL log + in-memory warehouse; no network/Kafka/Spark"
}
```

These numbers describe **this local implementation only** — a single process appending to a local JSONL file and landing into an in-memory warehouse. They do not predict throughput or p99 latency of a distributed Kafka → Spark → Iceberg/Delta deployment, where network, serialisation, compaction, and object-storage costs dominate.

## Honest limitations

- **No real database log tailing.** The source is a deterministic in-memory ledger with a commit log that models WAL semantics (LSN, before/after images). Wiring it to Postgres WAL / Debezium / Kafka Connect is an adapter change behind `LedgerSource`, not implemented here.
- **Warehouse is a semantic model, not Iceberg/Delta.** Snapshots, manifest, and bitemporal versions are local JSON/JSONL and an in-memory store. There is no Spark, Parquet, object storage, compaction, or partition evolution; `WarehouseTable.apply` is the boundary where an Iceberg/Delta `MERGE` would go.
- **Single-process, single-writer.** No partitioning, backpressure, exactly-once Kafka transactions, schema-registry integration, or multi-writer conflict handling. Replay-from-log on restart is supported only insofar as the JSONL log and watermark files persist; the warehouse itself is rebuilt in memory per process (persistence files are written but not yet reloaded on startup).
- **Airflow DAG is a definition, not a deployed pipeline.** It is importable/testable without Airflow and shells out to the CLI; it has not been run against a real Airflow scheduler in this repo.
- **Observability baselines are naive** (batch-count z-score, fixed null-rate delta, latest-event freshness) — no seasonality, no per-table SLAs, no alert routing/PagerDuty, no metrics exporter beyond the `/metrics` JSON endpoint.
- **Security/regulatory controls are modelled, not certified:** hash chaining gives tamper evidence, not WORM storage, encryption, access control, retention/erasure workflows, or any compliance certification.
- FastAPI state is in-memory and single-instance; `POST /pipeline/run` runs against the demo source unless an application wires in a real `Pipeline`.

## Layout

```
src/regulated_ledger_cdc/
  models.py          CDCEvent, ops, canonical row hashing
  source.py          transactional ledger source + commit log
  eventlog.py        append-only hash-chained log + watermark store
  capture.py         batch polling + micro-batch streaming
  bitemporal.py      bitemporal warehouse, snapshots, lineage
  reconciliation.py  source-to-target reconciliation
  observability.py   freshness / volume / drift / null checks + alerts
  pipeline.py        capture -> land -> reconcile -> observe
  api.py             FastAPI service
  cli.py             batch / stream / health / bench / serve
  benchmark.py       events/s + latency percentiles
dags/ledger_cdc_dag.py   Airflow DAG definitions
tests/                   pytest suite (21 tests)
```

## License

MIT — see [LICENSE](LICENSE). Copyright (c) 2026 Ayush Gawai.
