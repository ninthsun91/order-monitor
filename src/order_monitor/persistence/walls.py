"""벽 레지스트리 SQLite 영속화 (PRD §12.1 — "상태 미복원" 원칙의 유일한 예외).

`walls` 테이블만 M1에서 선행 도입한다 (전체 영속화는 M6). 인메모리 레지스트리가
단일 진실이고, 이 계층은 diff 이벤트 처리 결과를 미러링한다.

- 가격·수량은 TEXT로 저장해 Decimal 정밀도를 보존한다. 가격 키는 정규화 문자열
  (`format(d.normalize(), "f")`)로 표기 차이("61000.0" vs "61000.00000000")를 흡수한다.
- 시각은 wall-clock epoch 초 (wall_registry와 동일 — 재시작 넘어 보존 필요)
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

from order_monitor.ingestion.events import DiffDepthEvent, Side
from order_monitor.state.wall_registry import Wall, WallRegistry, WallRemoval

_SCHEMA = """
CREATE TABLE IF NOT EXISTS walls (
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    last_qty TEXT NOT NULL,
    peak_qty TEXT NOT NULL,
    first_seen_at REAL NOT NULL,
    first_seen_above_threshold REAL,
    last_seen_at REAL NOT NULL,
    unconfirmed INTEGER NOT NULL DEFAULT 0,
    unconfirmed_since REAL,
    PRIMARY KEY (side, price)
)
"""


def _canonical(value: Decimal) -> str:
    return format(value.normalize(), "f")


class WallStore:
    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def load(self) -> list[Wall]:
        rows = self._conn.execute(
            "SELECT side, price, last_qty, peak_qty, first_seen_at,"
            " first_seen_above_threshold, last_seen_at, unconfirmed, unconfirmed_since"
            " FROM walls"
        ).fetchall()
        return [
            Wall(
                side=Side(row[0]),
                price=Decimal(row[1]),
                last_qty=Decimal(row[2]),
                peak_qty=Decimal(row[3]),
                first_seen_at=row[4],
                first_seen_above_threshold=row[5],
                last_seen_at=row[6],
                unconfirmed=bool(row[7]),
                unconfirmed_since=row[8],
            )
            for row in rows
        ]

    def upsert(self, wall: Wall) -> None:
        self._conn.execute(
            "INSERT INTO walls (side, price, last_qty, peak_qty, first_seen_at,"
            " first_seen_above_threshold, last_seen_at, unconfirmed, unconfirmed_since)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (side, price) DO UPDATE SET"
            " last_qty=excluded.last_qty, peak_qty=excluded.peak_qty,"
            " first_seen_at=excluded.first_seen_at,"
            " first_seen_above_threshold=excluded.first_seen_above_threshold,"
            " last_seen_at=excluded.last_seen_at, unconfirmed=excluded.unconfirmed,"
            " unconfirmed_since=excluded.unconfirmed_since",
            (
                wall.side.value,
                _canonical(wall.price),
                str(wall.last_qty),
                str(wall.peak_qty),
                wall.first_seen_at,
                wall.first_seen_above_threshold,
                wall.last_seen_at,
                int(wall.unconfirmed),
                wall.unconfirmed_since,
            ),
        )

    def delete(self, side: Side, price: Decimal) -> None:
        self._conn.execute(
            "DELETE FROM walls WHERE side = ? AND price = ?",
            (side.value, _canonical(price)),
        )

    def mark_all_unconfirmed(self, since: float) -> None:
        """레지스트리 mark_all_unconfirmed()의 미러 — 기존 unconfirmed_since는 유지."""
        self._conn.execute(
            "UPDATE walls SET unconfirmed = 1,"
            " unconfirmed_since = COALESCE(unconfirmed_since, ?)"
            " WHERE unconfirmed = 0",
            (since,),
        )
        self._conn.commit()

    def sync_diff(
        self, registry: WallRegistry, event: DiffDepthEvent, removals: list[WallRemoval]
    ) -> None:
        """registry.apply_diff(event) 직후 호출해 변경분을 미러링한다."""
        for side, levels in ((Side.BUY, event.bids), (Side.SELL, event.asks)):
            for price, _qty in levels:
                wall = registry.get(side, price)
                if wall is not None:
                    self.upsert(wall)
        for removal in removals:
            self.delete(removal.wall.side, removal.wall.price)
        self._conn.commit()

    def delete_walls(self, walls: list[Wall]) -> None:
        """TTL 청소 결과 미러 (registry.prune_unconfirmed 반환값)."""
        for wall in walls:
            self.delete(wall.side, wall.price)
        self._conn.commit()

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM walls").fetchone()[0]
