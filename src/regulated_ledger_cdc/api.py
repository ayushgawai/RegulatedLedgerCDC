"""FastAPI service: pipeline health, alerts, reconciliation, lineage."""
from __future__ import annotations

from typing import Any

try:
    from fastapi import FastAPI, HTTPException
except ImportError:  # pragma: no cover - exercised only without the extra
    FastAPI = None  # type: ignore

from .pipeline import Pipeline
from .source import seed_demo_source


def create_app(pipeline: Pipeline | None = None):
    if FastAPI is None:
        raise RuntimeError("fastapi is not installed; install with: pip install -e .[api]")
    state = {"pipeline": pipeline or Pipeline(seed_demo_source(20), workdir="data/api_demo")}
    app = FastAPI(title="RegulatedLedgerCDC", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "service": "RegulatedLedgerCDC"}

    @app.get("/pipeline/health")
    def pipeline_health() -> dict[str, Any]:
        return state["pipeline"].health()

    @app.post("/pipeline/run")
    def run_batch(limit: int | None = None) -> dict[str, Any]:
        return state["pipeline"].run_batch(limit=limit)

    @app.get("/alerts")
    def alerts() -> dict[str, Any]:
        h = state["pipeline"].health()
        return {"alerts": h["alerts"], "count": len(h["alerts"])}

    @app.get("/reconciliation/latest")
    def reconciliation() -> dict[str, Any]:
        if state["pipeline"].last_reconciliation is None:
            raise HTTPException(404, "no reconciliation run yet; POST /pipeline/run first")
        return state["pipeline"].last_reconciliation

    @app.get("/lineage/{table}/{pk}")
    def lineage(table: str, pk: str) -> dict[str, Any]:
        rows = state["pipeline"].warehouse.lineage(table, pk)
        if not rows:
            raise HTTPException(404, "no lineage for that key")
        return {"table": table, "pk": pk, "lineage": rows}

    @app.get("/metrics")
    def metrics() -> dict[str, Any]:
        h = state["pipeline"].health()
        return {"event_log_size": h["event_log_size"], "watermark_lsn": h["watermark_lsn"],
                "snapshots": h["snapshots"], "chain_valid": h["chain_valid"],
                "alert_count": len(h["alerts"])}

    app.state.pipeline = state["pipeline"]
    return app


app = create_app() if FastAPI is not None else None
