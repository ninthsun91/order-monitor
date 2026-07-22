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
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    terminal_state TEXT NOT NULL,
    text TEXT NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0,
    recorded_at REAL NOT NULL,
    UNIQUE (side, price, terminal_state, recorded_at)
)
"""


def _canonical(value: Decimal) -> str:
    return format(value.normalize(), "f")


class AlertsOutboxStore:
    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def record(
        self, side: Side, price: Decimal, terminal_state: str, text: str, recorded_at: float
    ) -> int | None:
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO alerts_outbox"
            " (side, price, terminal_state, text, sent, recorded_at)"
            " VALUES (?, ?, ?, ?, 0, ?)",
            (side.value, _canonical(price), terminal_state, text, recorded_at),
        )
        self._conn.commit()
        return cur.lastrowid if cur.rowcount > 0 else None

    def mark_sent(self, rowid: int) -> None:
        self._conn.execute("UPDATE alerts_outbox SET sent = 1 WHERE id = ?", (rowid,))
        self._conn.commit()

    def load_unsent(self) -> list[tuple[int, str]]:
        rows = self._conn.execute(
            "SELECT id, text FROM alerts_outbox WHERE sent = 0 ORDER BY id"
        ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM alerts_outbox").fetchone()[0]
