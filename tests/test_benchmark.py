import json

from regulated_ledger_cdc.benchmark import run_benchmark
from regulated_ledger_cdc.cli import main


def test_benchmark_smoke(capsys):
    result = run_benchmark(2000)
    print(json.dumps(result, indent=2))
    assert result["n_events"] == 2000
    assert result["events_per_s"] > 0
    assert 0 <= result["latency_ms_p50"] <= result["latency_ms_p99"]


def test_cli_batch_and_health(tmp_path, capsys):
    assert main(["--workdir", str(tmp_path), "--events", "10", "batch"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["captured"] == 10
    assert all(r["status"] == "PASS" for r in out["reconciliation"].values())
    assert main(["--workdir", str(tmp_path / "h"), "--events", "5", "health"]) == 0
    health = json.loads(capsys.readouterr().out)
    assert health["event_log_size"] == 5 and health["chain_valid"] is True
