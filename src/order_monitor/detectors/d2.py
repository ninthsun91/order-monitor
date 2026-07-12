"""D2 — 시간창 볼륨 버스트 (PRD §8 D2).

trade_window의 방향 분리 합계가 임계 이상이면 발화. 평가는 aggTrade 수신마다
(체결이 들어온 방향만 — 합계가 변한 쪽만 재평가하면 충분하다). 방향 단위
`BURST_COOLDOWN`으로 연속 발화를 억제하며, 쿨다운 시계는 monotonic (PRD §11.1).

epoch 종료 시 리셋할 자체 누적은 없다 — 시간창은 상태 계층(trade_window) 소유이고
쿨다운은 판정 누적이 아니라 스팸 억제기이므로 epoch를 넘겨 유지한다. 재개 직후
부분 창의 과소평가 가능성은 PRD §5.4가 수용.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable
from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, Side
from order_monitor.state.trade_window import TradeWindow


@dataclasses.dataclass(frozen=True)
class D2Burst:
    aggressor_side: Side
    sum_qty: Decimal  # 창 내 해당 방향 합계 (발화 시점)


class D2Detector:
    def __init__(
        self,
        *,
        vol_threshold: Decimal,
        cooldown_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._vol_threshold = vol_threshold
        self._cooldown_seconds = cooldown_seconds
        self._monotonic = monotonic
        self._last_fired: dict[Side, float] = {}

    def on_trade(self, trade: AggTradeEvent, window: TradeWindow) -> D2Burst | None:
        side = trade.aggressor_side
        total = window.sum_qty(side)
        if total < self._vol_threshold:
            return None
        now = self._monotonic()
        last = self._last_fired.get(side)
        if last is not None and now - last < self._cooldown_seconds:
            return None
        self._last_fired[side] = now
        return D2Burst(aggressor_side=side, sum_qty=total)
