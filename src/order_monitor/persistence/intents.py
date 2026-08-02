"""D5 인텐트 상태기계 DB 기록 (PRD §12, M6) — 인텐트당 1행 + 상태 갱신.

전이 이력(진행률·종국 이벤트)은 `events` 테이블이 전 필드로 담당하므로
여기는 인스턴스 목록과 현재 상태만 둔다 (사용자 확정 2026-07-23 — 중복
배제). 키는 `(exchange, run_started_at, intent_id)` (v1.16) — intent_id는
프로세스-런 내 카운터라(재시작 시 0부터) 런 시각과 조합해야 유일하고
(alerts_outbox가 intent_id를 유일키에 쓰지 않은 것과 같은 이유, PRD §9.4),
파이프라인별 D5 인스턴스가 각자 0부터 세므로 exchange까지 필요하다 (PRD §12).

상태: `active` → (`confirmed` 래치, PRD §8 D5 v1.9 — 종국 아님, 행은 계속
열림) → D5TerminalState 값. 재시작 마킹(PRD §12 — "진행 중 intent DB
INTERRUPTED 마킹")은 열린 행(active·confirmed) 전부를 UPDATE 한 번으로
처리한다. 크래시 마킹과 라이브 epoch 종료 마킹의 구분은 두지 않는다
(§12 v1.2 — 재시작과 모든 epoch 종료에 동일 적용). 복원은 하지 않는다 —
마킹뿐 (§12 원칙, 인텐트 리셋은 의도된 수용 한계).
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

from order_monitor.detectors.d5 import D5TerminalState
from order_monitor.ingestion.events import Side

STATE_ACTIVE = "active"
STATE_CONFIRMED = "confirmed"  # 케이스 1 래치 (v1.9) — 종국 아님, 열린 상태

_SCHEMA = """
CREATE TABLE IF NOT EXISTS intents (
    exchange TEXT NOT NULL,
    run_started_at REAL NOT NULL,
    intent_id INTEGER NOT NULL,
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    registered_qty TEXT NOT NULL,
    registered_at REAL NOT NULL,
    state TEXT NOT NULL,
    confirmed_at REAL,
    level_realized_rate TEXT,
    updated_at REAL NOT NULL,
    PRIMARY KEY (exchange, run_started_at, intent_id)
)
"""

_DATA_COLUMNS = (
    "run_started_at, intent_id, side, price, registered_qty, registered_at,"
    " state, confirmed_at, level_realized_rate, updated_at"
)


def _canonical(value: Decimal) -> str:
    return format(value.normalize(), "f")


class IntentStore:
    def __init__(self, path: str | Path, exchange: str = "binance") -> None:
        self._exchange = exchange
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        # v1.16 — 같은 DB 파일에 파이프라인별 연결이 공존 (단일 스레드라 동시 쓰기는
        # 없지만, 짧은 잠금 겹침 방어)
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(_SCHEMA)
        # v1.16 마이그레이션 (PRD §12) — PK에 exchange 추가. 파이프라인별 D5가
        # intent_id를 각자 0부터 세므로 exchange 없이는 같은 런의 행이 충돌한다.
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(intents)")}
        if "exchange" not in columns:
            self._conn.execute(_SCHEMA.replace("IF NOT EXISTS intents", "intents_new"))
            self._conn.execute(
                f"INSERT INTO intents_new (exchange, {_DATA_COLUMNS})"
                f" SELECT 'binance', {_DATA_COLUMNS} FROM intents"
            )
            self._conn.execute("DROP TABLE intents")
            self._conn.execute("ALTER TABLE intents_new RENAME TO intents")
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def register(
        self,
        run_started_at: float,
        intent_id: int,
        side: Side,
        price: Decimal,
        registered_qty: Decimal,
        now: float,
    ) -> None:
        self._conn.execute(
            "INSERT INTO intents (exchange, run_started_at, intent_id, side, price,"
            " registered_qty, registered_at, state, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self._exchange,
                run_started_at,
                intent_id,
                side.value,
                _canonical(price),
                _canonical(registered_qty),
                now,
                STATE_ACTIVE,
                now,
            ),
        )
        self._conn.commit()

    def mark_confirmed(
        self, run_started_at: float, intent_id: int, rate: Decimal, now: float
    ) -> None:
        self._conn.execute(
            "UPDATE intents SET state = ?, confirmed_at = ?, level_realized_rate = ?,"
            " updated_at = ? WHERE exchange = ? AND run_started_at = ? AND intent_id = ?",
            (STATE_CONFIRMED, now, _canonical(rate), now, self._exchange, run_started_at, intent_id),
        )
        self._conn.commit()

    def finalize(
        self, run_started_at: float, intent_id: int, state: str, rate: Decimal, now: float
    ) -> None:
        self._conn.execute(
            "UPDATE intents SET state = ?, level_realized_rate = ?, updated_at = ?"
            " WHERE exchange = ? AND run_started_at = ? AND intent_id = ?",
            (state, _canonical(rate), now, self._exchange, run_started_at, intent_id),
        )
        self._conn.commit()

    def mark_interrupted_at_startup(self, now: float) -> int:
        """열린 행(active·confirmed) 전부 INTERRUPTED — 마킹 건수 반환.

        정상 경로에서는 epoch 종료의 D5 reset()이 종국을 이미 기록하므로,
        여기 걸리는 행은 크래시/강제 종료로 종국 기록을 못 받은 것들이다.
        """
        cur = self._conn.execute(
            "UPDATE intents SET state = ?, updated_at = ?"
            " WHERE state IN (?, ?) AND exchange = ?",
            (D5TerminalState.INTERRUPTED.value, now, STATE_ACTIVE, STATE_CONFIRMED, self._exchange),
        )
        self._conn.commit()
        return cur.rowcount

    def rows(self) -> list[dict]:
        return [
            {
                "run_started_at": row[0],
                "intent_id": row[1],
                "side": row[2],
                "price": row[3],
                "registered_qty": row[4],
                "registered_at": row[5],
                "state": row[6],
                "confirmed_at": row[7],
                "level_realized_rate": row[8],
                "updated_at": row[9],
            }
            for row in self._conn.execute(
                "SELECT run_started_at, intent_id, side, price, registered_qty,"
                " registered_at, state, confirmed_at, level_realized_rate, updated_at"
                " FROM intents WHERE exchange = ? ORDER BY run_started_at, intent_id",
                (self._exchange,),
            ).fetchall()
        ]
