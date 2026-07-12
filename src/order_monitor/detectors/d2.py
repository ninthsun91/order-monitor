"""D2 — 에피소드형 상대 임계 볼륨 버스트 (PRD §8 D2 v1.3).

임계 `THR = max(vol_floor, vol_multiplier × 분당 기준선)`. 60초 창의 **총** 테이커
볼륨이 THR를 넘는 순간 에피소드 시작 + 온셋 이벤트, `THR × exit_ratio` 아래로
식으면 잠정 종료, 병합 창(`episode_merge_minutes`) 내 재점화면 같은 에피소드로
계속(온셋 재발화 없음), 경과하면 확정 종료 + 요약 이벤트. 시간 쿨다운 없음 —
에피소드+병합이 억제 메커니즘 (PRD §9.2).

- 기준선 미준비(워밍업) 동안은 전 판정 보류 (PRD §8 D2 워밍업)
- 성격 라벨: 델타비 |매수−매도|/총합 — 방향성/혼합/양방향(흡수성 후보).
  흡수 확정은 D3 몫, D2는 테이커 흐름 성격만 라벨
- 요약의 누적·가격은 잠정 종료 시점 체크포인트 기준 — 병합 대기 중의 저볼륨
  체결은 재점화 시에만 에피소드에 편입되고, 확정 종료 시에는 제외된다
- 시계: 병합 창·체결 두절 감지는 monotonic (§11.1), 구간 표기는 exchange_time
- epoch 종료 시 reset(): 진행 중 에피소드 폐기, 요약 미발송 (부분 누적 신뢰 상실)
"""

from __future__ import annotations

import dataclasses
import enum
import time
from collections.abc import Callable
from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, Side
from order_monitor.state.trade_window import TradeWindow
from order_monitor.state.volume_baseline import VolumeBaseline


class D2Label(enum.Enum):
    DIRECTIONAL_BUY = "directional_buy"
    DIRECTIONAL_SELL = "directional_sell"
    MIXED = "mixed"
    BALANCED = "balanced"  # 양방향 — 흡수성 후보


@dataclasses.dataclass(frozen=True)
class D2BurstOnset:
    window_qty: Decimal  # 온셋 시점 60초 창 총합
    buy_qty: Decimal
    sell_qty: Decimal
    threshold: Decimal
    baseline_per_minute: Decimal
    label: D2Label
    price: Decimal  # 온셋 시점 최근 체결가
    start_exchange_ms: int


@dataclasses.dataclass(frozen=True)
class D2BurstSummary:
    start_exchange_ms: int
    end_exchange_ms: int
    total_qty: Decimal
    buy_qty: Decimal
    sell_qty: Decimal
    baseline_per_minute: Decimal  # 온셋 시점 고정 — 요약의 배수 환산 기준
    label: D2Label
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal


D2Event = D2BurstOnset | D2BurstSummary


class _Episode:
    def __init__(self, baseline_per_minute: Decimal) -> None:
        self.baseline = baseline_per_minute
        self.buy = Decimal(0)
        self.sell = Decimal(0)
        self.start_ex_ms: int | None = None
        self.end_ex_ms: int | None = None
        self.open: Decimal | None = None
        self.high: Decimal | None = None
        self.low: Decimal | None = None
        self.close: Decimal | None = None
        self.cooling_since: float | None = None
        self._checkpoint: tuple | None = None

    def absorb(self, price: Decimal, qty: Decimal, side: Side, ex_ms: int) -> None:
        if side is Side.BUY:
            self.buy += qty
        else:
            self.sell += qty
        if self.open is None:
            self.start_ex_ms = ex_ms
            self.open = self.high = self.low = price
        self.end_ex_ms = ex_ms
        self.close = price
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price

    def begin_cooling(self, now: float) -> None:
        self.cooling_since = now
        self._checkpoint = (self.buy, self.sell, self.end_ex_ms, self.high, self.low, self.close)

    def resume(self) -> None:
        # 재점화 — 병합 대기 중 흡수된 체결까지 그대로 에피소드에 편입
        self.cooling_since = None
        self._checkpoint = None

    def finalize(self, label: Callable[[Decimal, Decimal], D2Label]) -> D2BurstSummary:
        buy, sell, end_ex_ms, high, low, close = self._checkpoint
        return D2BurstSummary(
            start_exchange_ms=self.start_ex_ms,
            end_exchange_ms=end_ex_ms,
            total_qty=buy + sell,
            buy_qty=buy,
            sell_qty=sell,
            baseline_per_minute=self.baseline,
            label=label(buy, sell),
            open_price=self.open,
            high_price=high,
            low_price=low,
            close_price=close,
        )


class D2Detector:
    def __init__(
        self,
        *,
        vol_floor: Decimal,
        vol_multiplier: Decimal,
        exit_ratio: Decimal,
        merge_seconds: float,
        directional_ratio: Decimal,
        balanced_ratio: Decimal,
        window_seconds: float,
        window: TradeWindow,
        baseline: VolumeBaseline,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._vol_floor = vol_floor
        self._vol_multiplier = vol_multiplier
        self._exit_ratio = exit_ratio
        self._merge_seconds = merge_seconds
        self._directional_ratio = directional_ratio
        self._balanced_ratio = balanced_ratio
        self._window_seconds = window_seconds
        self._window = window
        self._baseline = baseline
        self._monotonic = monotonic
        self._episode: _Episode | None = None
        self._last_trade_monotonic: float | None = None

    @property
    def episode_active(self) -> bool:
        return self._episode is not None

    def on_trade(self, trade: AggTradeEvent) -> list[D2Event]:
        now = self._monotonic()
        self._last_trade_monotonic = now
        episode = self._episode
        if episode is not None:
            episode.absorb(trade.price, trade.qty, trade.aggressor_side, trade.exchange_time_ms)

        mean = self._baseline.per_minute_mean()
        if mean is None:
            return []  # 워밍업 — 판정 보류 (에피소드는 시작 자체가 안 되므로 존재 불가)
        threshold = max(self._vol_floor, self._vol_multiplier * mean)
        buy, sell = self._window.totals()
        total = buy + sell

        if episode is None:
            if total >= threshold:
                return [self._start_episode(mean, threshold, buy, sell, trade)]
            return []
        if episode.cooling_since is None:
            if total < threshold * self._exit_ratio:
                episode.begin_cooling(now)
            return []
        if total >= threshold:
            episode.resume()
            return []
        return self._maybe_finalize(now)

    def on_tick(self) -> list[D2Event]:
        """주기 틱 — 체결이 끊겨도 에피소드 종료 판정이 진행되도록 (PRD §8 D2 평가 시점)."""
        episode = self._episode
        if episode is None:
            return []
        now = self._monotonic()
        if episode.cooling_since is None:
            # 체결 자체가 창 길이만큼 두절 = 창이 식은 것과 동치
            if now - self._last_trade_monotonic >= self._window_seconds:
                episode.begin_cooling(now)
            return []
        return self._maybe_finalize(now)

    def reset(self) -> None:
        """epoch 종료 시 호출 — 진행 중 에피소드 폐기, 요약 미발송 (PRD §5.4, §8 D2)."""
        self._episode = None

    def _start_episode(
        self, mean: Decimal, threshold: Decimal, buy: Decimal, sell: Decimal, trade: AggTradeEvent
    ) -> D2BurstOnset:
        # 온셋 시점의 60초 창 내용이 버스트 본체 — 창의 체결들로 에피소드를 시드
        episode = _Episode(baseline_per_minute=mean)
        for t in self._window:
            episode.absorb(t.price, t.qty, t.aggressor_side, t.exchange_time_ms)
        self._episode = episode
        return D2BurstOnset(
            window_qty=buy + sell,
            buy_qty=buy,
            sell_qty=sell,
            threshold=threshold,
            baseline_per_minute=mean,
            label=self._label(buy, sell),
            price=trade.price,
            start_exchange_ms=episode.start_ex_ms,
        )

    def _maybe_finalize(self, now: float) -> list[D2Event]:
        episode = self._episode
        if now - episode.cooling_since < self._merge_seconds:
            return []
        self._episode = None
        return [episode.finalize(self._label)]

    def _label(self, buy: Decimal, sell: Decimal) -> D2Label:
        total = buy + sell
        ratio = abs(buy - sell) / total
        if ratio >= self._directional_ratio:
            return D2Label.DIRECTIONAL_BUY if buy > sell else D2Label.DIRECTIONAL_SELL
        if ratio <= self._balanced_ratio:
            return D2Label.BALANCED
        return D2Label.MIXED
