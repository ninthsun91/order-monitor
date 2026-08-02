"""디텍터 이벤트 DB 기록 (PRD §12, M6) — 임계치 튜닝·사후 분석·오탐 리뷰용.

`_emit()` 관문을 지나는 모든 디텍터 이벤트를 1행씩 적재한다. payload는
JSON-lines 로그와 동일 직렬화(`_event_log_fields` 결과의 JSON)로 저장해
로그와 DB를 같은 필드명으로 조회할 수 있다. side/price는 이벤트가 해당
필드를 가지면 조회용으로 승격하고, 없으면(D2 요약·W 리포트 등) NULL.
행 삭제 메서드는 두지 않는다 — §12.1의 TTL 청소는 레지스트리 전용이고
events 이력은 튜닝 데이터로 보존한다.
(v1.16) 거래소 스코프: 인스턴스가 `exchange`에 바인딩되어 기록·조회 전부 자기
거래소 행만 — 크로스 거래소 분석은 DB 직접 질의 소관 (PRD §12).
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
    payload TEXT NOT NULL,
    exchange TEXT NOT NULL DEFAULT 'binance'
)
"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, recorded_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_side_price ON events (side, price, recorded_at)",
    # v1.16 — 거래소 축 조회 (PRD §12). 기존 두 인덱스는 크로스 거래소 분석용으로 유지
    "CREATE INDEX IF NOT EXISTS idx_events_exch_type_time"
    " ON events (exchange, event_type, recorded_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_exch_side_price"
    " ON events (exchange, side, price, recorded_at)",
)


def _canonical(value: Decimal) -> str:
    return format(value.normalize(), "f")


class EventStore:
    def __init__(self, path: str | Path, exchange: str = "binance") -> None:
        self._exchange = exchange
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        # v1.16 — 같은 DB 파일에 파이프라인별 연결이 공존 (단일 스레드라 동시 쓰기는
        # 없지만, 짧은 잠금 겹침 방어)
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(_SCHEMA)
        # v1.16 마이그레이션 — 기존 배포 DB에 exchange 컬럼 추가 (v1.8 ALTER 선례 패턴)
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(events)")}
        if "exchange" not in columns:
            self._conn.execute(
                "ALTER TABLE events ADD COLUMN exchange TEXT NOT NULL DEFAULT 'binance'"
            )
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
            "INSERT INTO events (recorded_at, event_type, side, price, payload, exchange)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                recorded_at,
                event_type,
                side.value if side is not None else None,
                _canonical(price) if price is not None else None,
                payload,
                self._exchange,
            ),
        )
        self._conn.commit()

    def rows(self, event_type: str | None = None) -> list[dict]:
        sql = "SELECT recorded_at, event_type, side, price, payload FROM events WHERE exchange = ?"
        params: tuple = (self._exchange,)
        if event_type is not None:
            sql += " AND event_type = ?"
            params = (self._exchange, event_type)
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
        return self._conn.execute(
            "SELECT COUNT(*) FROM events WHERE exchange = ?", (self._exchange,)
        ).fetchone()[0]
