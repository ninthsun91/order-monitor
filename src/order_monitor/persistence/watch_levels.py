"""W 주시 레벨 영속화 (PRD §12.2 v1.13 — "상태 미복원" 원칙의 예외 ②).

주시 목록은 사용자가 명령으로 공급한 입력이고, 누적 카운터도 복원한다
(사용자 확정 2026-07-23 — 수십 시간 관측이 재시작에 0이 되면 기능 목적 반감).
flush 시점: 등록/해소 즉시 + 주기 리포트/회차 종료/무효화 (service 소관).

최종 리포트 발송 보장 (§9.4 v1.13): alerts_outbox 미적용 대신 같은 패턴을
자체 행으로 — 무효화 확정 시 완성된 메시지 텍스트를 `final_text`로 선기록,
발송 확인(`on_sent`) 시 행 삭제. `final_text IS NOT NULL` = 미발송 최종
리포트이며 재시작 시 그대로 재전송한다 (outbox처럼 텍스트를 저장해 상태
재구성이 불필요).
"""

from __future__ import annotations

import dataclasses
import sqlite3
from decimal import Decimal
from pathlib import Path

from order_monitor.detectors.watch_level import WatchReportData

_SCHEMA = """
CREATE TABLE IF NOT EXISTS watch_levels (
    price_lo TEXT NOT NULL,
    price_hi TEXT NOT NULL,
    literal TEXT NOT NULL,
    registered_at REAL NOT NULL,
    role TEXT,
    episode_num INTEGER NOT NULL DEFAULT 0,
    first_contact_at REAL,
    cum_buy TEXT NOT NULL DEFAULT '0',
    cum_sell TEXT NOT NULL DEFAULT '0',
    excursion_low TEXT,
    excursion_high TEXT,
    final_reason TEXT,
    final_text TEXT,
    last_flush_at REAL NOT NULL,
    PRIMARY KEY (price_lo, price_hi)
)
"""


def _canonical(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _opt_decimal(value: str | None) -> Decimal | None:
    return Decimal(value) if value is not None else None


@dataclasses.dataclass(frozen=True)
class WatchRow:
    lo: Decimal
    hi: Decimal
    literal: str
    registered_at: float
    role: str | None
    episode_num: int
    first_contact_at: float | None
    cum_buy: Decimal
    cum_sell: Decimal
    excursion_low: Decimal | None
    excursion_high: Decimal | None
    final_reason: str | None
    final_text: str | None


class WatchStore:
    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def upsert(self, data: WatchReportData, flushed_at: float) -> None:
        """등록 즉시 + 카운터 flush (주기 리포트/회차 경계) — 구역 키 upsert."""
        self._conn.execute(
            "INSERT INTO watch_levels"
            " (price_lo, price_hi, literal, registered_at, role, episode_num,"
            "  first_contact_at, cum_buy, cum_sell, excursion_low, excursion_high,"
            "  last_flush_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(price_lo, price_hi) DO UPDATE SET"
            "  role = excluded.role,"
            "  episode_num = excluded.episode_num,"
            "  first_contact_at = excluded.first_contact_at,"
            "  cum_buy = excluded.cum_buy,"
            "  cum_sell = excluded.cum_sell,"
            "  excursion_low = excluded.excursion_low,"
            "  excursion_high = excluded.excursion_high,"
            "  last_flush_at = excluded.last_flush_at",
            (
                _canonical(data.lo),
                _canonical(data.hi),
                data.literal,
                data.registered_at,
                data.role,
                data.episode_num,
                data.first_contact_at,
                _canonical(data.cum_buy),
                _canonical(data.cum_sell),
                _canonical(data.excursion_low) if data.excursion_low is not None else None,
                _canonical(data.excursion_high) if data.excursion_high is not None else None,
                flushed_at,
            ),
        )
        self._conn.commit()

    def mark_final(self, lo: Decimal, hi: Decimal, reason: str, text: str) -> None:
        """무효화 확정 선기록 — 발송 확인 전 크래시 시 재시작 재전송의 근거."""
        self._conn.execute(
            "UPDATE watch_levels SET final_reason = ?, final_text = ?"
            " WHERE price_lo = ? AND price_hi = ?",
            (reason, text, _canonical(lo), _canonical(hi)),
        )
        self._conn.commit()

    def delete(self, lo: Decimal, hi: Decimal) -> None:
        """수동 해소 즉시 / 최종 리포트 발송 확인 후."""
        self._conn.execute(
            "DELETE FROM watch_levels WHERE price_lo = ? AND price_hi = ?",
            (_canonical(lo), _canonical(hi)),
        )
        self._conn.commit()

    def load(self) -> list[WatchRow]:
        rows = self._conn.execute(
            "SELECT price_lo, price_hi, literal, registered_at, role, episode_num,"
            " first_contact_at, cum_buy, cum_sell, excursion_low, excursion_high,"
            " final_reason, final_text"
            " FROM watch_levels ORDER BY price_lo"
        ).fetchall()
        return [
            WatchRow(
                lo=Decimal(row[0]),
                hi=Decimal(row[1]),
                literal=row[2],
                registered_at=row[3],
                role=row[4],
                episode_num=row[5],
                first_contact_at=row[6],
                cum_buy=Decimal(row[7]),
                cum_sell=Decimal(row[8]),
                excursion_low=_opt_decimal(row[9]),
                excursion_high=_opt_decimal(row[10]),
                final_reason=row[11],
                final_text=row[12],
            )
            for row in rows
        ]

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM watch_levels").fetchone()[0]
