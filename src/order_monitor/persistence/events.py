"""디텍터 이벤트 DB 기록 (PRD §12, M6) — 임계치 튜닝·사후 분석·오탐 리뷰용.

`_emit()` 관문을 지나는 모든 디텍터 이벤트를 1행씩 적재한다. payload는
JSON-lines 로그와 동일 직렬화(`_event_log_fields` 결과의 JSON)로 저장해
로그와 DB를 같은 필드명으로 조회할 수 있다. side/price는 이벤트가 해당
필드를 가지면 조회용으로 승격하고, 없으면(D2 요약·W 리포트 등) NULL.
행 삭제 메서드는 두지 않는다 — §12.1의 TTL 청소는 레지스트리 전용이고
events 이력은 튜닝 데이터로 보존한다.
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

from order_monitor.ingestion.events import Side

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    recorded_at REAL NOT NULL,
    event_type TEXT NOT NULL,
    side TEXT,
    price TEXT,
    payload TEXT NOT NULL
)
"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, recorded_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_side_price ON events (side, price, recorded_at)",
)


def _canonical(value: Decimal) -> str:
    return format(value.normalize(), "f")


class EventStore:
    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        for ddl in _INDEXES:
            self._conn.execute(ddl)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def record(
        self,
        event_type: str,
        side: Side | None,
        price: Decimal | None,
        fields: dict,
        recorded_at: float,
    ) -> None:
        # default=str: 중첩 dataclass(W 리포트)의 Decimal 등 — 로그 직렬화와 동일 규칙
        payload = json.dumps(fields, ensure_ascii=False, default=str)
        self._conn.execute(
            "INSERT INTO events (recorded_at, event_type, side, price, payload)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                recorded_at,
                event_type,
                side.value if side is not None else None,
                _canonical(price) if price is not None else None,
                payload,
            ),
        )
        self._conn.commit()

    def rows(self, event_type: str | None = None) -> list[dict]:
        sql = "SELECT recorded_at, event_type, side, price, payload FROM events"
        params: tuple = ()
        if event_type is not None:
            sql += " WHERE event_type = ?"
            params = (event_type,)
        sql += " ORDER BY id"
        return [
            {
                "recorded_at": row[0],
                "event_type": row[1],
                "side": row[2],
                "price": row[3],
                "payload": json.loads(row[4]),
            }
            for row in self._conn.execute(sql, params).fetchall()
        ]

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
