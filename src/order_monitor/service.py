"""파이프라인 배선 (PRD §6) — M4 범위: 수집 + 상태 + D1~D5 판정 + Telegram 알림.

디텍터 판정은 epoch 활성 중에만 수행하고(PRD §5.4 — 상태 적재는 계속),
EpochEnded에서 D1 활성/후보·접촉 episode·D3 등록·D5 활성 인텐트를
리셋한다. D3는 알림 없이 로그 전용 (PRD §9.1 — DB events 테이블은 M6).
D5 확정 알림은 SQLite `alerts_outbox`(PRD §9.4)에 선기록 후 발송.
모든 디텍터 이벤트는 발송 여부와 무관하게 구조화 로그로 남는다 (PRD §11.4).

**D4·케이스2 임시 비활성 (PRD v1.6, 2026-07-15)**: D4(아이스버그/리필)와
그걸 재료로 쓰는 D5 케이스2는 배선에서 제외됐다 — 실전 조율 단계에서
D1/D2/D3+케이스1(확정 등급)만 운용하고, D4 방식(리필 페어링 vs 체결량-잔량
대조)은 표본이 쌓인 뒤 재설계 논의 후 재배선한다(DEVELOPMENT_PLAN 결정 기록).
`refill_above_lookup`이 상수 0을 반환하므로 케이스2 발화·진행률·인텐트 소모가
구조적으로 불가능하다 — 인텐트는 벽 소멸/epoch 종료까지 살아 케이스1만 감시.
`detectors/d4.py` 모듈·단위 테스트는 재논의 기반으로 보존.

벽 소멸(diff tombstone/하한 미만) 시 배선 순서가 판정 정합성을 가른다:
episode REMOVED 종료 → D3 확정 판정 (D1 등록이 아직 살아있어야 함) →
D1 REMOVED 판정 → D1 이벤트를 D3 등록 해제 + D5 소멸 평가로 라우팅.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import time
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

from order_monitor.alerting.dispatcher import AlertDispatcher
from order_monitor.alerting.telegram import TelegramSender
from order_monitor.alerting.wall_report import format_wall_report
from order_monitor.config import AppConfig
from order_monitor.detectors.contact import (
    ContactEpisodeTracker,
    EpisodeEnd,
    EpisodeEndReason,
)
from order_monitor.detectors.d1 import D1Appeared, D1Detector, D1Event, D1Removed
from order_monitor.detectors.d2 import D2Detector
from order_monitor.detectors.d3 import D3Detector
from order_monitor.detectors.d5 import D5Detector
from order_monitor.ingestion.baseline_bootstrap import BootstrapError, fetch_minute_volumes
from order_monitor.ingestion.events import (
    AggTradeEvent,
    DepthSnapshot,
    DiffDepthEvent,
    Event,
    Side,
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
from order_monitor.persistence.alerts_outbox import AlertsOutboxStore
from order_monitor.persistence.walls import WallStore
from order_monitor.state.level_tracker import LevelTracker
from order_monitor.state.order_book import OrderBook
from order_monitor.state.trade_window import TradeWindow
from order_monitor.state.volume_baseline import VolumeBaseline
from order_monitor.state.wall_registry import WallRegistry
from order_monitor.watchdog.heartbeat import write_heartbeat

logger = logging.getLogger(__name__)

STALENESS_CHECK_INTERVAL_SECONDS = 1.0
PRUNE_INTERVAL_SECONDS = 3600.0


def seconds_until_boundary(now_epoch: float, interval_seconds: float) -> float:
    """다음 벽시계 경계(epoch 초의 interval 배수)까지 남은 시간.

    경계에 정확히 걸린 경우 다음 경계까지 전체 간격을 반환 — 발송 직후
    재계산에서 같은 경계로 0이 나와 이중 발송되는 일을 막는다.
    """
    return interval_seconds - now_epoch % interval_seconds


class MonitorService:
    def __init__(
        self,
        config: AppConfig,
        db_path: str | Path,
        *,
        telegram_token: str,
        heartbeat_path: str | Path = "order_monitor.heartbeat",
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        # 클록 주입은 결정적 replay 테스트용 (PRD §13) — 운영은 기본값
        self._config = config
        self._db_path = db_path
        self._heartbeat_path = Path(heartbeat_path)
        self._clock = clock

        self.order_book = OrderBook()
        self.trade_window = TradeWindow(config.thresholds.window_seconds)
        # 판정 경로 float 금지 (PRD §7) — config 수치는 여기서 Decimal로 변환
        self.wall_registry = WallRegistry(
            record_min_qty=Decimal(str(config.wall_tracker.record_min_qty_btc)),
            size_threshold=Decimal(str(config.thresholds.size_threshold_btc)),
            clock=clock,
        )
        # 벽 레벨은 top-20 창 이탈에도 누적 보존 (§7 v1.4 예외 — level_tracker docstring)
        self.level_tracker = LevelTracker(
            retain=lambda side, price: self.wall_registry.get(side, price) is not None
        )
        self.tracker = SessionEpochTracker(
            symbol=config.symbol,
            stale_seconds=config.watchdog.stale_seconds,
            trade_stale_seconds=config.watchdog.trade_stale_seconds,
            clock=monotonic,
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
            clock=clock,
        )
        self.volume_baseline = VolumeBaseline(config.thresholds.vol_baseline_hours)
        self.d2 = D2Detector(
            vol_floor=Decimal(str(config.thresholds.vol_floor_btc)),
            vol_multiplier=Decimal(str(config.thresholds.vol_multiplier)),
            exit_ratio=Decimal(str(config.thresholds.episode_exit_ratio)),
            merge_seconds=config.thresholds.episode_merge_minutes * 60.0,
            directional_ratio=Decimal(str(config.thresholds.delta_directional_ratio)),
            balanced_ratio=Decimal(str(config.thresholds.delta_balanced_ratio)),
            absorb_delta_min=Decimal(str(config.thresholds.summary_absorb_delta_min)),
            move_min_pct=Decimal(str(config.thresholds.summary_move_min_pct)),
            window_seconds=config.thresholds.window_seconds,
            window=self.trade_window,
            baseline=self.volume_baseline,
            monotonic=monotonic,
        )
        self.contact = ContactEpisodeTracker(
            pierce_persist_snapshots=config.thresholds.pierce_persist_snapshots
        )
        self.d3 = D3Detector(
            absorption_min_pct=Decimal(str(config.thresholds.absorption_min_pct)),
            cum_traded_lookup=self._cum_traded_at_level,
        )
        self.d5 = D5Detector(
            realize_pct=Decimal(str(config.thresholds.realize_pct)),
            realize_pct_above=Decimal(str(config.thresholds.realize_pct_above)),
            progress_step_pct=Decimal(str(config.thresholds.progress_step_pct)),
            cum_traded_lookup=self._cum_traded_at_level,
            refill_above_lookup=self._refill_above_lookup,
            monotonic=monotonic,
        )
        self.telegram = TelegramSender(telegram_token, config.telegram.chat_id)
        self._outbox: AlertsOutboxStore | None = None
        self.dispatcher = AlertDispatcher(
            config,
            self.telegram,
            monotonic=monotonic,
            clock=clock,
            outbox=None,
            d1_streak_gate=self._appeared_streak_unalerted,
            wall_lookup=self.wall_registry.walls,
        )
        self._store: WallStore | None = None

    def _cum_traded_at_level(self, side, price: Decimal) -> Decimal:
        # 창 진입 이력 없는 레벨은 관측된 체결이 없다는 뜻 — 0. 벽 레벨은 창
        # 이탈에도 엔트리가 보존되어 생애 누적이 유지된다 (§7 v1.4 예외)
        level = self.level_tracker.get(side, price)
        return level.cum_traded_at_level if level is not None else Decimal(0)

    def _refill_above_lookup(self, side: Side, price: Decimal) -> Decimal:
        # D4·케이스2 비활성 (모듈 docstring) — 상수 0이라 케이스2 판정·진행률이
        # 구조적으로 발화 불가. 재배선 시 D4의 sum_lifetime_refill_above로 복원
        return Decimal(0)

    # ── 기동/종료 ──────────────────────────────────────────────

    def startup(self) -> None:
        self._store = WallStore(self._db_path)
        restored = self._store.load()
        if restored:
            self.wall_registry.restore(restored)
            # 재시작 = 청취 공백 → 전체 unconfirmed (PRD §12.1 규칙 1)
            self.wall_registry.mark_all_unconfirmed()
            self._store.mark_all_unconfirmed(since=self._clock())
        logger.info("wall registry restored", extra={"count": len(restored)})

        # D5 확정/추정 알림 outbox (PRD §9.4) — 같은 db 파일에 별도 테이블
        self._outbox = AlertsOutboxStore(self._db_path)
        self.dispatcher.set_outbox(self._outbox)
        unsent = self._outbox.load_unsent()
        for rowid, text in unsent:
            self.telegram.enqueue(text, on_sent=lambda rid=rowid: self._outbox.mark_sent(rid))
        if unsent:
            logger.info("unsent alerts requeued from outbox", extra={"count": len(unsent)})

    async def run(self) -> None:
        self.startup()
        await self._bootstrap_baseline()
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(self.ws_client.run())
                group.create_task(self._staleness_loop())
                group.create_task(self._prune_loop())
                group.create_task(self._wall_report_loop())
                group.create_task(self._heartbeat_loop())
                group.create_task(self.telegram.run())
        finally:
            if self._store is not None:
                self._store.close()
            if self._outbox is not None:
                self._outbox.close()

    async def _bootstrap_baseline(self) -> None:
        """D2 기준선 워밍업 (PRD §8 D2 v1.3) — 실패해도 기동은 계속, D2만 보류."""
        minutes = int(self._config.thresholds.vol_baseline_hours * 60)
        try:
            bars = await fetch_minute_volumes(self._config.symbol, minutes)
        except BootstrapError as exc:
            logger.warning(
                "baseline bootstrap failed — D2 held until window fills",
                extra={"error": str(exc), "minutes": minutes},
            )
            return
        self.volume_baseline.bootstrap(bars)
        mean = self.volume_baseline.per_minute_mean()
        logger.info(
            "volume baseline bootstrapped",
            extra={"bars": len(bars), "per_minute_mean": str(mean) if mean is not None else None},
        )

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
            if self.tracker.epoch_active:
                self._handle_episode_ends(
                    self.contact.on_depth_snapshot(
                        self.order_book.best_bid,
                        self.order_book.best_ask,
                        event.local_monotonic_receive_time,
                    )
                )
                for d5_event in self.d5.evaluate():
                    self._emit(d5_event)
        elif isinstance(event, AggTradeEvent):
            self.trade_window.add(event)
            self.level_tracker.record_trade(event)
            self.volume_baseline.add(event.exchange_time_ms, event.qty)
            # 판정은 epoch 활성 중에만 — 상태 적재는 위에서 이미 수행 (PRD §5.4)
            if self.tracker.epoch_active:
                self._handle_episode_ends(self.contact.on_trade(event))  # 체결가 관통
                for d2_event in self.d2.on_trade(event):
                    self._emit(d2_event)
                for d5_event in self.d5.evaluate():
                    self._emit(d5_event)
        elif isinstance(event, DiffDepthEvent):
            diff_result = self.wall_registry.apply_diff(event)
            removals = diff_result.removals
            assert self._store is not None
            self._store.sync_diff(self.wall_registry, event, removals)
            # 등록/소멸 궤적 로그 (M6 관측 보완 — 준임계 벽 생애 기록, epoch 무관)
            for wall in diff_result.registrations:
                logger.info(
                    "wall registered",
                    extra={
                        "side": wall.side.value,
                        "price": str(wall.price),
                        "qty": str(wall.last_qty),
                    },
                )
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
                # 소멸 레벨의 episode를 먼저 REMOVED 종료 — D3 확정 판정은 D1 등록
                # 해제(아래 라우팅) 전에 이뤄져야 한다 (모듈 docstring 배선 순서)
                for removal in removals:
                    end = self.contact.end_episode(
                        removal.wall.side,
                        removal.wall.price,
                        EpisodeEndReason.REMOVED,
                        event.local_monotonic_receive_time,
                    )
                    if end is not None:
                        self._handle_episode_ends([end])
                for d1_event in self.d1.on_removals(removals):
                    self._route_d1(d1_event)
                for d1_event in self.d1.evaluate(self.wall_registry.walls()):
                    self._route_d1(d1_event)

    # ── 주기 작업 ──────────────────────────────────────────────

    async def _staleness_loop(self) -> None:
        while True:
            await asyncio.sleep(STALENESS_CHECK_INTERVAL_SECONDS)
            self._handle_notices(self.tracker.check_staleness())
            # D1 지속 타이머 게이트 — diff 이벤트가 안 와도 PERSIST 경과로 발화 (PRD §8 D1)
            if self.tracker.epoch_active:
                for d1_event in self.d1.evaluate(self.wall_registry.walls()):
                    self._route_d1(d1_event)
                # D2 에피소드 종료 판정 — 체결이 끊겨도 진행 (PRD §8 D2 v1.3)
                # (D5는 여기서 호출 안 함 — TTL 폐지(PRD v1.5) 후 시간 경과만으로
                # 바뀌는 판정이 없고, 실현률은 체결/스냅샷 이벤트 경로가 갱신)
                for d2_event in self.d2.on_tick():
                    self._emit(d2_event)

    async def _wall_report_loop(self) -> None:
        """호가벽 정기 리포트 — 벽시계 정시 경계 발송, epoch 활성 중에만 (현재가는 depth 기반).

        기동 시각 기준 간격이 아니라 epoch 초의 interval 배수 경계에 맞춘다 —
        60분이면 매시 정각 (KST는 정수 시간 오프셋이라 UTC 정각 = KST 정각).
        """
        if not self._config.alerts.send_wall_report:
            return
        interval = self._config.alerts.wall_report_interval_minutes * 60.0
        while True:
            await asyncio.sleep(seconds_until_boundary(self._clock(), interval))
            text = self.build_wall_report()
            if text is None:
                logger.info("wall report skipped — epoch inactive or book empty")
                continue
            self.telegram.enqueue(text)

    def build_wall_report(self) -> str | None:
        best_bid, best_ask = self.order_book.best_bid, self.order_book.best_ask
        if not self.tracker.epoch_active or best_bid is None or best_ask is None:
            return None
        return format_wall_report(
            self.wall_registry.walls(),
            best_bid=best_bid,
            best_ask=best_ask,
            symbol=self._config.symbol,
            size_threshold=Decimal(str(self._config.thresholds.size_threshold_btc)),
            now_epoch_seconds=self._clock(),
        )

    async def _heartbeat_loop(self) -> None:
        """프로세스 하트비트 (PRD §11.1) — 이벤트 루프 생존 신호.

        외부 워치독(deploy/watchdog_check.py)이 이 파일의 mtime 나이로 행을
        감지한다. 기록 실패는 파이프라인을 죽이지 않는다 — 하트비트가 stale로
        남으면 외부 워치독이 PROCESS_DOWN으로 승격하는 것이 에스컬레이션 경로.
        """
        while True:
            try:
                write_heartbeat(self._heartbeat_path)
            except OSError as exc:
                logger.warning("heartbeat write failed", extra={"error": str(exc)})
            await asyncio.sleep(self._config.watchdog.heartbeat_interval)

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

    def _route_d1(self, event: D1Event) -> None:
        """D1 이벤트 발신 + D3/D5 등록 중개 (등록 크기 S = APPEARED 시점 qty)."""
        self._emit(event)
        if isinstance(event, D1Appeared):
            self.d3.on_d1_appeared(event)
            self.d5.on_d1_appeared(event)
        elif isinstance(event, D1Removed):
            self.d3.on_d1_removed(event)
            d5_event = self.d5.on_d1_removed(event)
            if d5_event is not None:
                self._emit(d5_event)

    def _appeared_streak_unalerted(self, event: D1Appeared) -> bool:
        """dispatcher의 APPEARED 스트릭당 1회 게이트 (PRD §8 D1 v1.8) — 미발송
        스트릭이면 마킹(영속) 후 True, 이미 발송된 스트릭이면 False.

        재시작·재연결 재발화는 D5 인텐트 재등록에 필요해 이벤트 흐름은 그대로
        두고 발송만 여기서 걸러진다. 스트릭 키 = `first_seen_above_threshold`
        (재시작을 넘어 보존, §12.1) — 임계 하회 후 재돌파는 값이 바뀌므로 새
        등장으로 다시 발송된다.
        """
        wall = self.wall_registry.get(event.side, event.price)
        if wall is None or wall.first_seen_above_threshold is None:
            return True  # 발화 직후 소멸 등 경합 — 스트릭 식별 불가면 억제 근거도 없음
        if wall.appeared_alerted_since == wall.first_seen_above_threshold:
            return False
        wall.appeared_alerted_since = wall.first_seen_above_threshold
        if self._store is not None:
            self._store.save(wall)
        return True

    def _handle_episode_ends(self, ends: list[EpisodeEnd]) -> None:
        """접촉 episode 종료 → D3 확정 판정 발신."""
        for end in ends:
            for d3_event in self.d3.on_episode_end(end):
                self._emit(d3_event)

    # ── 헬스/epoch 통지 처리 ───────────────────────────────────

    def _handle_notices(self, notices: list[Notice]) -> None:
        for notice in notices:
            if isinstance(notice, EpochStarted):
                logger.info("epoch started", extra={"epoch_id": notice.epoch_id})
            elif isinstance(notice, EpochEnded):
                # 판정 전제 붕괴 — D1 활성/후보, D2 진행 중 에피소드, 접촉 episode·
                # D3 등록·D5 활성 인텐트 폐기 (PRD §5.4. trade_window·
                # volume_baseline은 상태 계층이라 유지)
                self.d1.reset()
                if self.d2.episode_active:
                    logger.warning("d2 episode discarded on epoch end")
                self.d2.reset()
                self.contact.reset()
                self.d3.reset()
                for d5_event in self.d5.reset():
                    self._emit(d5_event)
                logger.warning(
                    "epoch ended",
                    extra={"epoch_id": notice.epoch_id, "reason": notice.reason},
                )
            elif isinstance(notice, StreamStale):
                logger.warning(
                    "stream stale",
                    extra={"stream": notice.stream, "silent_seconds": notice.silent_seconds},
                )
                # M5 워치독 선행분: FEED_STALE 텔레그램 알림 (PRD §11.1)
                self.dispatcher.dispatch(notice)
            elif isinstance(notice, DiffListeningGap):
                self.wall_registry.mark_all_unconfirmed()
                if self._store is not None:
                    self._store.mark_all_unconfirmed(since=self._clock())
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
