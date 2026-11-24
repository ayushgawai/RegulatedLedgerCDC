from regulated_ledger_cdc.bitemporal import Warehouse
from regulated_ledger_cdc.reconciliation import reconcile_source, reconcile_table
from regulated_ledger_cdc.source import LedgerSource


def test_reconciliation_passes_after_full_landing():
    src = LedgerSource()
    for i in range(10):
        src.insert("ledger_entries", f"E{i}", {"entry_id": f"E{i}", "amount_minor": 10 * i})
    src.update("ledger_entries", "E1", {"entry_id": "E1", "amount_minor": 999})
    src.delete("ledger_entries", "E2")
    wh = Warehouse()
    wh.apply_many(src.changes_since(0))
    report = reconcile_source(src.snapshot(), wh, "ledger_entries")
    assert report.status == "PASS"
    assert report.source_count == report.target_count == 9
    assert report.numeric_totals["amount_minor"]["source"] == report.numeric_totals["amount_minor"]["target"]


def test_reconciliation_detects_missing_extra_and_mismatch():
    report = reconcile_table(
        {"A": {"amount_minor": 10}, "B": {"amount_minor": 20}, "C": {"amount_minor": 30}},
        {"A": {"amount_minor": 10}, "B": {"amount_minor": 21}, "D": {"amount_minor": 40}},
        table="t")
    assert report.status == "FAIL"
    assert report.missing_in_target == ["C"] and report.extra_in_target == ["D"]
    assert [m["pk"] for m in report.mismatched] == ["B"]


def test_reconciliation_detects_unlanded_change():
    src = LedgerSource()
    src.insert("ledger_entries", "E1", {"entry_id": "E1", "amount_minor": 10})
    wh = Warehouse()  # never landed
    report = reconcile_source(src.snapshot(), wh, "ledger_entries")
    assert report.status == "FAIL" and report.missing_in_target == ["E1"]
