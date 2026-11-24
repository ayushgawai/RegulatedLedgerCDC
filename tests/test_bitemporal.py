from datetime import datetime, timezone

from regulated_ledger_cdc.bitemporal import Warehouse
from regulated_ledger_cdc.source import LedgerSource

T = lambda s: datetime.fromisoformat(s).replace(tzinfo=timezone.utc)  # noqa: E731


def land(src):
    wh = Warehouse()
    wh.apply_many(src.changes_since(0))
    return wh


def test_valid_time_ordering():
    src = LedgerSource()
    src.insert("ledger_entries", "E1", {"amount_minor": 100}, valid_time=T("2024-01-01"), tx_time=T("2024-01-01"))
    src.update("ledger_entries", "E1", {"amount_minor": 200}, valid_time=T("2024-02-01"), tx_time=T("2024-02-01"))
    wh = land(src)
    tbl = wh.table("ledger_entries")
    assert tbl.point_in_time("E1", T("2024-01-15"))["amount_minor"] == 100
    assert tbl.point_in_time("E1", T("2024-02-15"))["amount_minor"] == 200
    assert tbl.current_view()["E1"]["amount_minor"] == 200


def test_late_correction_visible_only_after_its_tx_time():
    src = LedgerSource()
    src.insert("ledger_entries", "E1", {"amount_minor": 100}, valid_time=T("2024-01-01"), tx_time=T("2024-01-02"))
    wh = land(src)
    # Correction to the January fact, learned in March.
    src.update("ledger_entries", "E1", {"amount_minor": 90}, valid_time=T("2024-01-01"), tx_time=T("2024-03-01"))
    wh.apply_many(src.changes_since(1))
    tbl = wh.table("ledger_entries")
    assert tbl.point_in_time("E1", T("2024-01-15"), tx_at=T("2024-02-01"))["amount_minor"] == 100
    assert tbl.point_in_time("E1", T("2024-01-15"), tx_at=T("2024-03-02"))["amount_minor"] == 90
    assert tbl.point_in_time("E1", T("2024-01-15"))["amount_minor"] == 90


def test_out_of_order_valid_time_arrival():
    src = LedgerSource()
    # February fact commits (LSN 1) before the January fact (LSN 2) is captured.
    src.insert("ledger_entries", "E1", {"amount_minor": 200}, valid_time=T("2024-02-01"), tx_time=T("2024-02-01"))
    src.update("ledger_entries", "E1", {"amount_minor": 100}, valid_time=T("2024-01-01"), tx_time=T("2024-02-02"))
    wh = land(src)
    tbl = wh.table("ledger_entries")
    assert tbl.point_in_time("E1", T("2024-01-15"))["amount_minor"] == 100
    assert tbl.point_in_time("E1", T("2024-02-15"))["amount_minor"] == 200


def test_delete_is_a_tombstone_and_history_is_kept():
    src = LedgerSource()
    src.insert("ledger_entries", "E1", {"amount_minor": 100}, valid_time=T("2024-01-01"), tx_time=T("2024-01-01"))
    src.delete("ledger_entries", "E1", valid_time=T("2024-02-01"), tx_time=T("2024-02-01"))
    wh = land(src)
    tbl = wh.table("ledger_entries")
    assert "E1" not in tbl.current_view()
    assert tbl.point_in_time("E1", T("2024-01-15"))["amount_minor"] == 100
    assert len(tbl.history("E1")) >= 2
    assert len(wh.lineage("ledger_entries", "E1")) >= 2
