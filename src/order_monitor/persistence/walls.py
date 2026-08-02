"""벽 레지스트리 SQLite 영속화 (PRD §12.1 — "상태 미복원" 원칙의 유일한 예외).

`walls` 테이블만 M1에서 선행 도입한다 (전체 영속화는 M6). 인메모리 레지스트리가
단일 진실이고, 이 계층은 diff 이벤트 처리 결과를 미러링한다.

- 가격·수량은 TEXT로 저장해 Decimal 정밀도를 보존한다. 가격 키는 정규화 문자열
  (`format(d.normalize(), "f")`)로 표기 차이("61000.0" vs "61000.00000000")를 흡수한다.
- 시각은 wall-clock epoch 초 (wall_registry와 동일 — 재시작 넘어 보존 필요)
- (v1.16) 거래소 스코프: store 인스턴스가 `exchange`에 바인딩되어 자기 거래소 행만
  읽고 쓴다 (PRD §12 — 파이프라인별 인스턴스, PK는 (exchange, side, price))
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

from order_monitor.ingestion.events import DiffDepthEvent, Side
from order_monitor.state.wall_registry import Wall, WallRegistry, WallRemoval

_SCHEMA = """
CREATE TABLE IF NOT EXISTS walls (
    exchange TEXT NOT NULL,
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    last_qty TEXT NOT NULL,
    peak_qty TEXT NOT NULL,
    first_seen_at REAL NOT NULL,
    first_seen_above_threshold REAL,
    last_seen_at REAL NOT NULL,
    unconfirmed INTEGER NOT NULL DEFAULT 0,
    unconfirmed_since REAL,
    appeared_alerted_since REAL,
    PRIMARY KEY (exchange, side, price)
)
"""

_DATA_COLUMNS = (
    "side, price, last_qty, peak_qty, first_seen_at, first_seen_above_threshold,"
    " last_seen_at, unconfirmed, unconfirmed_since, appeared_alerted_since"
)


def _canonical(value: Decimal) -> str:
    return format(value.normalize(), "f")


class WallStore:
    def __init__(self, path: str | Path, exchange: str = "binance") -> None:
        self._exchange = exchange
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        # v1.16 — 같은 DB 파일에 파이프라인별 연결이 공존 (단일 스레드라 동시 쓰기는
        # 없지만, 짧은 잠금 겹침 방어)
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(_SCHEMA)
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(walls)")}
        # v1.8 마이그레이션 — 기존 배포 DB에 appeared_alerted_since 컬럼 추가
        # (v1.16 재생성 복사가 이 컬럼을 포함하므로 재생성보다 먼저 수행)
        if "appeared_alerted_since" not in columns:
            self._conn.execute("ALTER TABLE walls ADD COLUMN appeared_alerted_since REAL")
        # v1.16 마이그레이션 (PRD §12) — PK (side, price) → (exchange, side, price).
        # SQLite는 PK 변경 ALTER 불가 → 테이블 재생성, 기존 행은 'binance' 백필
        if "exchange" not in columns:
            self._conn.execute(_SCHEMA.replace("IF NOT EXISTS walls", "walls_new"))
            self._conn.execute(
                f"INSERT INTO walls_new (exchange, {_DATA_COLUMNS})"
                f" SELECT 'binance', {_DATA_COLUMNS} FROM walls"
            )
            self._conn.execute("DROP TABLE walls")
            self._conn.execute("ALTER TABLE walls_new RENAME TO walls")
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def load(self) -> list[Wall]:
        rows = self._conn.execute(
            "SELECT side, price, last_qty, peak_qty, first_seen_at,"
            " first_seen_above_threshold, last_seen_at, unconfirmed, unconfirmed_since,"
            " appeared_alerted_since"
            " FROM walls WHERE exchange = ?",
            (self._exchange,),
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
                appeared_alerted_since=row[9],
            )
            for row in rows
        ]

    def upsert(self, wall: Wall) -> None:
        self._conn.execute(
            "INSERT INTO walls (exchange, side, price, last_qty, peak_qty, first_seen_at,"
            " first_seen_above_threshold, last_seen_at, unconfirmed, unconfirmed_since,"
            " appeared_alerted_since)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (exchange, side, price) DO UPDATE SET"
            " last_qty=excluded.last_qty, peak_qty=excluded.peak_qty,"
            " first_seen_at=excluded.first_seen_at,"
            " first_seen_above_threshold=excluded.first_seen_above_threshold,"
            " last_seen_at=excluded.last_seen_at, unconfirmed=excluded.unconfirmed,"
            " unconfirmed_since=excluded.unconfirmed_since,"
            " appeared_alerted_since=excluded.appeared_alerted_since",
            (
                self._exchange,
                wall.side.value,
                _canonical(wall.price),
                str(wall.last_qty),
                str(wall.peak_qty),
                wall.first_seen_at,
                wall.first_seen_above_threshold,
                wall.last_seen_at,
                int(wall.unconfirmed),
                wall.unconfirmed_since,
                wall.appeared_alerted_since,
            ),
        )

    def save(self, wall: Wall) -> None:
        """단건 upsert + 즉시 commit — diff 배치(sync_diff) 밖의 단발 갱신용 (알림 스트릭 마킹)."""
        self.upsert(wall)
        self._conn.commit()

    def delete(self, side: Side, price: Decimal) -> None:
        self._conn.execute(
            "DELETE FROM walls WHERE exchange = ? AND side = ? AND price = ?",
            (self._exchange, side.value, _canonical(price)),
        )

    def mark_all_unconfirmed(self, since: float) -> None:
        """레지스트리 mark_all_unconfirmed()의 미러 — 기존 unconfirmed_since는 유지."""
        self._conn.execute(
            "UPDATE walls SET unconfirmed = 1,"
            " unconfirmed_since = COALESCE(unconfirmed_since, ?)"
            " WHERE unconfirmed = 0 AND exchange = ?",
            (since, self._exchange),
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
        return self._conn.execute(
            "SELECT COUNT(*) FROM walls WHERE exchange = ?", (self._exchange,)
        ).fetchone()[0]
