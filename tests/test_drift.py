from regulated_ledger_cdc.observability import (
    detect_schema_drift, infer_schema, null_anomalies, null_rates)


def test_schema_drift_added_removed_and_type_changed():
    drift = detect_schema_drift(
        {"entry_id": "str", "amount_minor": "int", "legacy": "str"},
        {"entry_id": "str", "amount_minor": "str", "channel": "str"})
    assert drift.has_drift
    assert drift.added == ["channel"] and drift.removed == ["legacy"]
    assert drift.type_changed == {"amount_minor": {"from": "int", "to": "str"}}


def test_no_drift_when_schemas_match():
    schema = infer_schema([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    assert not detect_schema_drift(schema, dict(schema)).has_drift


def test_infer_schema_recovers_from_null_first_row():
    assert infer_schema([{"a": None}, {"a": 5}]) == {"a": "int"}


def test_null_anomaly_detects_spike():
    rows = [{"a": 1}, {"a": None}, {"a": None}, {"a": 1}]
    assert null_rates(rows)["a"] == 0.5
    anomalies = null_anomalies(rows, {"a": 0.0}, delta_threshold=0.2)
    assert anomalies == {"a": {"baseline": 0.0, "current": 0.5}}
    assert null_anomalies([{"a": 1}], {"a": 0.0}) == {}
