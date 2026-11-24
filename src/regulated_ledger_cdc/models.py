"""Core data model for RegulatedLedgerCDC."""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str | datetime | None) -> datetime:
    if value is None:
        return utcnow()
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def canonical_hash(row: dict[str, Any] | None) -> str:
    if row is None:
        return hashlib.sha256(b"null").hexdigest()
    payload = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


class Op(str, Enum):
    INSERT = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"


@dataclass
class CDCEvent:
    lsn: int
    table: str
    pk: str
    op: Op
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    valid_time: datetime  # business / effective time
    tx_time: datetime     # source commit time
    source: str = "ledger"
    schema_version: int = 1
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["op"] = self.op.value
        data["valid_time"] = self.valid_time.isoformat()
        data["tx_time"] = self.tx_time.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CDCEvent":
        return cls(
            lsn=int(data["lsn"]), table=data["table"], pk=str(data["pk"]),
            op=Op(data["op"]), before=data.get("before"), after=data.get("after"),
            valid_time=parse_dt(data["valid_time"]), tx_time=parse_dt(data["tx_time"]),
            source=data.get("source", "ledger"), schema_version=int(data.get("schema_version", 1)),
            event_id=data.get("event_id") or str(uuid.uuid4()),
        )
