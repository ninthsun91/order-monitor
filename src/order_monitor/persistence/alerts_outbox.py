"""D5 종국 알림 outbox — 크래시에도 확정 알림 유실 방지 (PRD §9.4, M4 선행 도입).

`walls` 테이블(M1 선행)과 같은 패턴: 선기록(unsent) → Telegram 발송 큐 투입 →
발송 확인 시 sent 마킹. 재시작 시 미발송 행을 그대로 재전송한다 — 메시지
텍스트가 이미 완성된 채로 저장돼 있어 인텐트/디텍터 상태를 재구성할 필요가
없다. 적용 범위는 D5 종국 알림(CONFIRMED — v1.11에서 INFERRED_ABOVE 폐지로
단일) 전용 — 진행률·D2·watchdog은 시효성 신호라 재전송 가치가 없고, D4 흡수
방어 알림도 미적용(관측 등급, 재시작 후 재전송 가치 낮음 — PRD §9.4 v1.11).

`UNIQUE(side, price, terminal_state, recorded_at)`는 방어용(단일 프로세스 내
우발적 중복 기록 차단, `INSERT OR IGNORE`)이다. `recorded_at`은 D5Detector의
프로세스 카운터(intent_id)가 아니라 호출자(서비스 계층)가 기록 시점에 찍는
wall-clock 값을 쓴다 — intent_id는 재시작마다 0부터 다시 시작하므로 이를
유일키에 쓰면 재시작 직후 다른 인텐트가 우연히 같은 (side,price,terminal_state,
intent_id)를 얻어 진짜 알림이 조용히 유실될 위험이 있다.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

from order_monitor.ingestion.events import Side

_SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts_outbox (
    id INTEGER PRIMARY KEY,
    exchange TEXT NOT NULL,
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    terminal_state TEXT NOT NULL,
    text TEXT NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0,
    recorded_at REAL NOT NULL,
    UNIQUE (exchange, side, price, terminal_state, recorded_at)
)
"""

_DATA_COLUMNS = "id, side, price, terminal_state, text, sent, recorded_at"


def _canonical(value: Decimal) -> str:
    return format(value.normalize(), "f")


class AlertsOutboxStore:
    def __init__(self, path: str | Path, exchange: str = "binance") -> None:
        self._exchange = exchange
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        # v1.16 — 같은 DB 파일에 파이프라인별 연결이 공존 (단일 스레드라 동시 쓰기는
        # 없지만, 짧은 잠금 겹침 방어)
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(_SCHEMA)
        # v1.16 마이그레이션 (PRD §12) — UNIQUE에 exchange 포함은 테이블 재생성 필요.
        # id를 보존 복사해 미발송 행의 mark_sent 참조 연속성을 유지한다.
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(alerts_outbox)")}
        if "exchange" not in columns:
            self._conn.execute(_SCHEMA.replace("IF NOT EXISTS alerts_outbox", "alerts_outbox_new"))
            self._conn.execute(
                f"INSERT INTO alerts_outbox_new (exchange, {_DATA_COLUMNS})"
                f" SELECT 'binance', {_DATA_COLUMNS} FROM alerts_outbox"
            )
            self._conn.execute("DROP TABLE alerts_outbox")
            self._conn.execute("ALTER TABLE alerts_outbox_new RENAME TO alerts_outbox")
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def record(
        self, side: Side, price: Decimal, terminal_state: str, text: str, recorded_at: float
    ) -> int | None:
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO alerts_outbox"
            " (exchange, side, price, terminal_state, text, sent, recorded_at)"
            " VALUES (?, ?, ?, ?, ?, 0, ?)",
            (self._exchange, side.value, _canonical(price), terminal_state, text, recorded_at),
        )
        self._conn.commit()
        return cur.lastrowid if cur.rowcount > 0 else None

    def mark_sent(self, rowid: int) -> None:
        self._conn.execute("UPDATE alerts_outbox SET sent = 1 WHERE id = ?", (rowid,))
        self._conn.commit()

    def load_unsent(self) -> list[tuple[int, str]]:
        rows = self._conn.execute(
            "SELECT id, text FROM alerts_outbox WHERE sent = 0 AND exchange = ? ORDER BY id",
            (self._exchange,),
        ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def count(self) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) FROM alerts_outbox WHERE exchange = ?", (self._exchange,)
        ).fetchone()[0]
