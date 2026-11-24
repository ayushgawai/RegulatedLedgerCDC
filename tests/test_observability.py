from datetime import timedelta

from fastapi.testclient import TestClient

from regulated_ledger_cdc.api import create_app
from regulated_ledger_cdc.models import utcnow
from regulated_ledger_cdc.observability import freshness_check, volume_check
from regulated_ledger_cdc.pipeline import Pipeline
from regulated_ledger_cdc.source import LedgerSource, seed_demo_source


def test_freshness_and_volume_checks():
    src = LedgerSource()
    event = src.insert("ledger_entries", "E1", {"amount_minor": 1})
    old = event.tx_time - timedelta(hours=5)
    assert freshness_check([event], now=event.tx_time + timedelta(minutes=1)) is None
    alert = freshness_check([event], now=event.tx_time + timedelta(hours=5), threshold_seconds=3600)
    assert alert is not None and alert.check == "freshness"
    assert freshness_check([], now=utcnow()) is not None
    assert volume_check(100, [10, 11, 9, 10, 11]) is not None
    assert volume_check(10, [10, 11, 9, 10, 11]) is None


def test_pipeline_end_to_end(tmp_path):
    pipeline = Pipeline(seed_demo_source(20), workdir=tmp_path)
    result = pipeline.run_batch()
    assert result["captured"] == 20 and result["landed"] == 20
    assert result["chain_valid"] is True
    assert all(r["status"] == "PASS" for r in result["reconciliation"].values())
    health = pipeline.health()
    assert health["status"] in {"HEALTHY", "DEGRADED"}
    assert pipeline.run_batch()["captured"] == 0  # idempotent re-run


def test_api_health_alerts_and_reconciliation(tmp_path):
    pipeline = Pipeline(seed_demo_source(5), workdir=tmp_path)
    client = TestClient(create_app(pipeline))
    assert client.get("/health").json()["status"] == "ok"
    assert client.get("/reconciliation/latest").status_code == 404
    assert client.post("/pipeline/run").status_code == 200
    assert client.get("/pipeline/health").json()["event_log_size"] == 5
    assert client.get("/reconciliation/latest").json()["ledger_entries"]["status"] == "PASS"
    assert client.get("/alerts").json()["count"] >= 0
    assert client.get("/metrics").json()["chain_valid"] is True
    assert client.get("/lineage/ledger_entries/E00000").status_code == 200
