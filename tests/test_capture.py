from datetime import datetime, timezone

from regulated_ledger_cdc.capture import Capturer
from regulated_ledger_cdc.eventlog import AppendOnlyEventLog, WatermarkStore
from regulated_ledger_cdc.source import LedgerSource

T = lambda s: datetime.fromisoformat(s).replace(tzinfo=timezone.utc)  # noqa: E731


def make(tmp_path):
    src = LedgerSource()
    log = AppendOnlyEventLog(tmp_path / "log.jsonl")
    return src, log, Capturer(src, log, WatermarkStore(tmp_path / "wm.json"))


def test_capture_insert_update_delete(tmp_path):
    src, log, cap = make(tmp_path)
    src.insert("ledger_entries", "E1", {"entry_id": "E1", "amount_minor": 100}, valid_time=T("2024-01-01"))
    src.update("ledger_entries", "E1", {"entry_id": "E1", "amount_minor": 150}, valid_time=T("2024-01-02"))
    src.delete("ledger_entries", "E1", valid_time=T("2024-01-03"))
    batch = cap.poll_batch()
    assert [e.op.value for e in batch] == ["c", "u", "d"]
    assert [e.lsn for e in batch] == [1, 2, 3]
    assert batch[1].before["amount_minor"] == 100 and batch[1].after["amount_minor"] == 150
    assert len(log) == 3 and log.verify_chain()


def test_capture_is_idempotent_and_resumes_from_watermark(tmp_path):
    src, log, cap = make(tmp_path)
    for i in range(5):
        src.insert("ledger_entries", f"E{i}", {"entry_id": f"E{i}", "amount_minor": i})
    assert len(cap.poll_batch(limit=2)) == 2
    assert len(cap.poll_batch()) == 3
    assert cap.poll_batch() == []  # drained
    # A fresh capturer on the same watermark/log must not duplicate.
    cap2 = Capturer(src, log, WatermarkStore(tmp_path / "wm.json"))
    assert cap2.poll_batch() == [] and len(log) == 5


def test_stream_drains_source(tmp_path):
    src, log, cap = make(tmp_path)
    for i in range(4):
        src.insert("ledger_entries", f"E{i}", {"entry_id": f"E{i}", "amount_minor": i})
    batches = [b for b in cap.stream(max_batches=5, limit=2) if b]
    assert sum(len(b) for b in batches) == 4


def test_hash_chain_detects_tampering(tmp_path):
    src, log, cap = make(tmp_path)
    src.insert("ledger_entries", "E1", {"entry_id": "E1", "amount_minor": 1})
    cap.poll_batch()
    assert log.verify_chain()
    text = (tmp_path / "log.jsonl").read_text()
    (tmp_path / "log.jsonl").write_text(text.replace('"amount_minor": 1', '"amount_minor": 999'))
    assert not AppendOnlyEventLog(tmp_path / "log.jsonl").verify_chain()
