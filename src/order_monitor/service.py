"""파이프라인 배선 (PRD §6) — M1 범위: 수집 + 상태 적재 + 벽 레지스트리 영속화.

디텍터(M2+)·알림(M2+)·워치독 알림(M5)은 아직 없다. epoch/헬스 통지는 구조화
로그로만 남기고, DiffListeningGap에만 벽 레지스트리 unconfirmed 마킹을 수행한다.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from pathlib import Path

from order_monitor.config import AppConfig
from order_monitor.ingestion.events import (
    AggTradeEvent,
    DepthSnapshot,
    DiffDepthEvent,
    Event,
)
from order_monitor.ingestion.health import (
    DiffListeningGap,
    EpochEnded,
    EpochStarted,
    Notice,
    SessionEpochTracker,
    StreamStale,
)
from order_monitor.ingestion.ws_client import BinanceWSClient
from order_monitor.persistence.walls import WallStore
from order_monitor.state.level_tracker import LevelTracker
from order_monitor.state.order_book import OrderBook
from order_monitor.state.trade_window import TradeWindow
from order_monitor.state.wall_registry import WallRegistry

logger = logging.getLogger(__name__)

STALENESS_CHECK_INTERVAL_SECONDS = 1.0
PRUNE_INTERVAL_SECONDS = 3600.0


class MonitorService:
    def __init__(self, config: AppConfig, db_path: str | Path) -> None:
        self._config = config
        self._db_path = db_path

        self.order_book = OrderBook()
        self.trade_window = TradeWindow(config.thresholds.window_seconds)
        self.level_tracker = LevelTracker()
        # 판정 경로 float 금지 (PRD §7) — config 수치는 여기서 Decimal로 변환
        self.wall_registry = WallRegistry(
            record_min_qty=Decimal(str(config.wall_tracker.record_min_qty_btc)),
            size_threshold=Decimal(str(config.thresholds.size_threshold_btc)),
        )
        self.tracker = SessionEpochTracker(
            symbol=config.symbol,
            stale_seconds=config.watchdog.stale_seconds,
            trade_stale_seconds=config.watchdog.trade_stale_seconds,
        )
        self.ws_client = BinanceWSClient(
            config.symbol,
            on_event=self.on_event,
            on_connected=self.on_connected,
            on_disconnected=self.on_disconnected,
        )
        self._store: WallStore | None = None

    # ── 기동/종료 ──────────────────────────────────────────────

    def startup(self) -> None:
        self._store = WallStore(self._db_path)
        restored = self._store.load()
        if restored:
            self.wall_registry.restore(restored)
            # 재시작 = 청취 공백 → 전체 unconfirmed (PRD §12.1 규칙 1)
            self.wall_registry.mark_all_unconfirmed()
            self._store.mark_all_unconfirmed(since=time.time())
        logger.info("wall registry restored", extra={"count": len(restored)})

    async def run(self) -> None:
        self.startup()
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(self.ws_client.run())
                group.create_task(self._staleness_loop())
                group.create_task(self._prune_loop())
        finally:
            if self._store is not None:
                self._store.close()

    # ── WS 콜백 ────────────────────────────────────────────────

    def on_connected(self) -> None:
        self._handle_notices(self.tracker.on_subscribed())

    def on_disconnected(self) -> None:
        self._handle_notices(self.tracker.on_disconnected())

    def on_event(self, stream: str, event: Event) -> None:
        # 헬스 판정 먼저 — diff U/u 갭의 unconfirmed 마킹은 "이 이벤트 이전"의
        # 공백에 대한 것이므로, 마킹 후에 이 이벤트를 적재해야 자기 가격이 재확인된다
        self._handle_notices(self.tracker.on_event(stream, event))

        if isinstance(event, DepthSnapshot):
            self.order_book.apply_snapshot(event)
            self.level_tracker.apply_snapshot(event)
        elif isinstance(event, AggTradeEvent):
            self.trade_window.add(event)
            self.level_tracker.record_trade(event)
        elif isinstance(event, DiffDepthEvent):
            removals = self.wall_registry.apply_diff(event)
            assert self._store is not None
            self._store.sync_diff(self.wall_registry, event, removals)
            for removal in removals:
                logger.info(
                    "wall removed",
                    extra={
                        "side": removal.wall.side.value,
                        "price": str(removal.wall.price),
                        "reason": removal.reason.value,
                        "last_qty": str(removal.wall.last_qty),
                        "peak_qty": str(removal.wall.peak_qty),
                    },
                )

    # ── 주기 작업 ──────────────────────────────────────────────

    async def _staleness_loop(self) -> None:
        while True:
            await asyncio.sleep(STALENESS_CHECK_INTERVAL_SECONDS)
            self._handle_notices(self.tracker.check_staleness())

    async def _prune_loop(self) -> None:
        while True:
            ttl_seconds = self._config.wall_tracker.ttl_days * 86400.0
            pruned = self.wall_registry.prune_unconfirmed(ttl_seconds)
            if pruned:
                assert self._store is not None
                self._store.delete_walls(pruned)
                logger.info(
                    "unconfirmed walls pruned",
                    extra={"count": len(pruned), "ttl_days": self._config.wall_tracker.ttl_days},
                )
            await asyncio.sleep(PRUNE_INTERVAL_SECONDS)

    # ── 헬스/epoch 통지 처리 ───────────────────────────────────

    def _handle_notices(self, notices: list[Notice]) -> None:
        for notice in notices:
            if isinstance(notice, EpochStarted):
                logger.info("epoch started", extra={"epoch_id": notice.epoch_id})
            elif isinstance(notice, EpochEnded):
                logger.warning(
                    "epoch ended",
                    extra={"epoch_id": notice.epoch_id, "reason": notice.reason},
                )
            elif isinstance(notice, StreamStale):
                logger.warning(
                    "stream stale",
                    extra={"stream": notice.stream, "silent_seconds": notice.silent_seconds},
                )
            elif isinstance(notice, DiffListeningGap):
                self.wall_registry.mark_all_unconfirmed()
                if self._store is not None:
                    self._store.mark_all_unconfirmed(since=time.time())
                logger.warning(
                    "diff listening gap — wall registry marked unconfirmed",
                    extra={"reason": notice.reason, "walls": len(self.wall_registry)},
                )
