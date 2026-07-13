"""D4 — 아이스버그/리필 (PRD §8 D4 v1.2 경로 누적). 알림 없음, 로그 전용 (PRD §9.1).

관측 구간은 접촉 episode (detectors/contact.py — D3와 공용). episode 동안
depth 스냅샷 간 표시 잔량 델타를 부호별로 분리해, 양의 델타(회복) 중 직전
`REFILL_WINDOW_MS` 이내에 해당 레벨의 aggTrade 체결이 있었던 것만
`refill_added`(인정 리필량)로 합산한다 — 체결과 무관한 신규 표시 주문은 배제.
peak/current 두 값 산식은 폐기됨 (경로가 다른데 결과값이 같은 오산 사례, PRD).

발화: episode 내 `refill_added ≥ ICEBERG_MARGIN` AND 인정된 체결→회복 쌍
≥ `ICEBERG_MIN_TRADES` — 단위는 aggTrade 메시지 수(단일 taker 주문), f/l로
펼친 원체결 수가 아니다 (PRD §8 D4 v1.2 명시). episode당 1회 발화(래치).

크로스 스트림 근접 비교는 monotonic 수신 시각 (PRD §11.1). 체결 버퍼는 episode
상태와 독립 — 접촉 스냅샷보다 체결이 먼저 도착하는 순서 역전에도 쌍이 성립한다.
버퍼는 REFILL_WINDOW_MS로 시간 바운드 (레벨 무관 전역 프루닝 — 메모리 상한).
누적은 episode 종료·epoch 종료 시 리셋 (PRD §8 D4, §5.4).
"""

from __future__ import annotations

import collections
import dataclasses
from decimal import Decimal

from order_monitor.detectors.contact import ContactEpisode, EpisodeEnd
from order_monitor.ingestion.events import AggTradeEvent, DepthSnapshot, Side

_Key = tuple[Side, Decimal]


@dataclasses.dataclass(frozen=True)
class D4Iceberg:
    side: Side
    price: Decimal
    refill_added: Decimal  # 숨은 물량 추정치 (인정 리필량 합)
    refill_trade_count: int  # 인정된 체결→회복 쌍의 aggTrade 메시지 수


@dataclasses.dataclass
class _Accumulation:
    refill_added: Decimal = Decimal(0)
    credited_trade_ids: set[int] = dataclasses.field(default_factory=set)
    fired: bool = False


class D4Detector:
    def __init__(
        self,
        *,
        iceberg_margin: Decimal,
        iceberg_min_trades: int,
        refill_window_ms: int,
    ) -> None:
        self._margin = iceberg_margin
        self._min_trades = iceberg_min_trades
        self._window_seconds = refill_window_ms / 1000.0
        # 레벨별 최근 체결 (receive_time, agg_trade_id) — 상태 계층 성격, epoch 무관
        self._trades: dict[_Key, collections.deque[tuple[float, int]]] = {}
        self._trade_log: collections.deque[tuple[float, _Key]] = collections.deque()
        # episode별 누적 (episode 종료·epoch 종료 시 리셋)
        self._acc: dict[_Key, _Accumulation] = {}
        self._last_size: dict[_Key, Decimal] = {}

    def on_trade(self, trade: AggTradeEvent) -> None:
        level_side = Side.BUY if trade.aggressor_side is Side.SELL else Side.SELL
        key = (level_side, trade.price)
        now = trade.local_monotonic_receive_time
        self._trades.setdefault(key, collections.deque()).append((now, trade.agg_trade_id))
        self._trade_log.append((now, key))
        # 전역 시간 바운드 프루닝 — 창 밖 체결은 어느 레벨에서도 쌍이 될 수 없다
        while self._trade_log and self._trade_log[0][0] < now - self._window_seconds:
            _, stale_key = self._trade_log.popleft()
            per_level = self._trades.get(stale_key)
            if per_level:
                per_level.popleft()
                if not per_level:
                    del self._trades[stale_key]

    def on_depth_snapshot(
        self, snapshot: DepthSnapshot, active_episodes: dict[_Key, ContactEpisode]
    ) -> list[D4Iceberg]:
        sizes: dict[_Key, Decimal] = {}
        for side, levels in ((Side.BUY, snapshot.bids), (Side.SELL, snapshot.asks)):
            for price, qty in levels:
                sizes[(side, price)] = qty

        now = snapshot.local_monotonic_receive_time
        events: list[D4Iceberg] = []
        for key in active_episodes:
            # 스냅샷 부재 = 잔량 0 — 플리커(0 → 리필)도 델타 경로로 포착
            new_size = sizes.get(key, Decimal(0))
            prev = self._last_size.get(key)
            self._last_size[key] = new_size
            if prev is None:
                continue  # episode 첫 관측 — 기준선만 설정
            delta = new_size - prev
            if delta <= 0:
                continue  # 소진/불변 — 리필 아님
            paired = [
                trade_id
                for receive_time, trade_id in self._trades.get(key, ())
                if now - self._window_seconds <= receive_time <= now
            ]
            if not paired:
                continue  # 체결과 무관한 잔량 증가 (신규 표시 주문) — 배제
            acc = self._acc.setdefault(key, _Accumulation())
            acc.refill_added += delta
            acc.credited_trade_ids.update(paired)
            if (
                not acc.fired
                and acc.refill_added >= self._margin
                and len(acc.credited_trade_ids) >= self._min_trades
            ):
                acc.fired = True
                events.append(
                    D4Iceberg(
                        side=key[0],
                        price=key[1],
                        refill_added=acc.refill_added,
                        refill_trade_count=len(acc.credited_trade_ids),
                    )
                )
        return events

    def on_episode_end(self, end: EpisodeEnd) -> None:
        key = (end.episode.side, end.episode.price)
        self._acc.pop(key, None)
        self._last_size.pop(key, None)

    def reset(self) -> None:
        """epoch 종료 — episode 누적 폐기. 체결 버퍼는 상태 계층이라 유지 (500ms 바운드)."""
        self._acc.clear()
        self._last_size.clear()
