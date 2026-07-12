"""파이프라인 배선 (PRD §6) — M2 범위: 수집 + 상태 + D1/D2 판정 + Telegram 알림.

디텍터 판정은 epoch 활성 중에만 수행하고(PRD §5.4 — 상태 적재는 계속),
EpochEnded에서 D1 활성/후보를 리셋한다. D3~D5(M3/M4)·워치독 알림(M5)은 아직 없다.
모든 디텍터 이벤트는 발송 여부와 무관하게 구조화 로그로 남는다 (PRD §11.4).
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import time
from decimal import Decimal
from pathlib import Path

from order_monitor.alerting.dispatcher import AlertDispatcher
from order_monitor.alerting.telegram import TelegramSender
from order_monitor.config import AppConfig
from order_monitor.detectors.d1 import D1Detector
from order_monitor.detectors.d2 import D2Detector
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
    def __init__(self, config: AppConfig, db_path: str | Path, *, telegram_token: str) -> None:
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
        self.d1 = D1Detector(
            size_threshold=Decimal(str(config.thresholds.size_threshold_btc)),
            persist_seconds=config.thresholds.persist_seconds,
            exit_ratio=Decimal(str(config.thresholds.exit_ratio)),
            fill_attribution=Decimal(str(config.thresholds.fill_attribution)),
            cum_traded_lookup=self._cum_traded_at_level,
        )
        self.d2 = D2Detector(
            vol_threshold=Decimal(str(config.thresholds.vol_threshold_btc)),
            cooldown_seconds=config.thresholds.burst_cooldown_seconds,
        )
        self.telegram = TelegramSender(telegram_token, config.telegram.chat_id)
        self.dispatcher = AlertDispatcher(config, self.telegram)
        self._store: WallStore | None = None

    def _cum_traded_at_level(self, side, price: Decimal) -> Decimal:
        # top-20 창 밖이면 관측된 체결이 없다는 뜻 — 0 (§7 스코프 전제)
        level = self.level_tracker.get(side, price)
        return level.cum_traded_at_level if level is not None else Decimal(0)

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
                group.create_task(self.telegram.run())
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
            # 판정은 epoch 활성 중에만 — 상태 적재는 위에서 이미 수행 (PRD §5.4)
            if self.tracker.epoch_active:
                burst = self.d2.on_trade(event, self.trade_window)
                if burst is not None:
                    self._emit(burst)
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
            if self.tracker.epoch_active:
                for d1_event in self.d1.on_removals(removals):
                    self._emit(d1_event)
                for d1_event in self.d1.evaluate(self.wall_registry.walls()):
                    self._emit(d1_event)

    # ── 주기 작업 ──────────────────────────────────────────────

    async def _staleness_loop(self) -> None:
        while True:
            await asyncio.sleep(STALENESS_CHECK_INTERVAL_SECONDS)
            self._handle_notices(self.tracker.check_staleness())
            # D1 지속 타이머 게이트 — diff 이벤트가 안 와도 PERSIST 경과로 발화 (PRD §8 D1)
            if self.tracker.epoch_active:
                for d1_event in self.d1.evaluate(self.wall_registry.walls()):
                    self._emit(d1_event)

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

    # ── 디텍터 이벤트 → 로그 + 알림 ────────────────────────────

    def _emit(self, event: object) -> None:
        logger.info("detector event", extra=_event_log_fields(event))
        self.dispatcher.dispatch(event)

    # ── 헬스/epoch 통지 처리 ───────────────────────────────────

    def _handle_notices(self, notices: list[Notice]) -> None:
        for notice in notices:
            if isinstance(notice, EpochStarted):
                logger.info("epoch started", extra={"epoch_id": notice.epoch_id})
            elif isinstance(notice, EpochEnded):
                # 판정 전제 붕괴 — D1 활성/후보 폐기 (PRD §5.4. D2 쿨다운은
                # 판정 누적이 아니라 유지, trade_window도 상태 계층이라 유지)
                self.d1.reset()
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


def _event_log_fields(event: object) -> dict:
    """디텍터 이벤트 dataclass → JSON 로그 필드 (Decimal/enum 직렬화)."""
    fields: dict = {"detector_event": type(event).__name__}
    for name, value in dataclasses.asdict(event).items():
        if isinstance(value, enum.Enum):
            fields[name] = value.value
        elif isinstance(value, Decimal):
            fields[name] = str(value)
        else:
            fields[name] = value
    return fields
