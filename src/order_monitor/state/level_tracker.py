"""top-20 레벨의 현재 크기 + 체결 귀속 (PRD §7 `level_tracker`, v1.2 축소형).

`{price, side, current_size, cum_traded_at_level}`만 유지 — 레벨 생애주기 추적은
`wall_registry` 담당. 체결 귀속 집계 규칙의 본격 구현·검증은 M3.

스코프 전제 (PRD §7): 귀속 판정이 일어나는 시점에는 가격이 레벨에 접촉해 있어
해당 레벨이 top-20 창 안에 있다 — 창을 벗어난 레벨의 엔트리는 제거된다.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, DepthSnapshot, Side


@dataclasses.dataclass
class TrackedLevel:
    price: Decimal
    side: Side  # 레벨의 side: bid 레벨 = BUY, ask 레벨 = SELL
    current_size: Decimal
    cum_traded_at_level: Decimal = Decimal(0)


class LevelTracker:
    def __init__(self) -> None:
        self._levels: dict[tuple[Side, Decimal], TrackedLevel] = {}

    def apply_snapshot(self, snapshot: DepthSnapshot) -> None:
        seen: set[tuple[Side, Decimal]] = set()
        for side, levels in ((Side.BUY, snapshot.bids), (Side.SELL, snapshot.asks)):
            for price, qty in levels:
                key = (side, price)
                seen.add(key)
                entry = self._levels.get(key)
                if entry is None:
                    self._levels[key] = TrackedLevel(price=price, side=side, current_size=qty)
                else:
                    entry.current_size = qty
        # top-20 창을 벗어난 레벨은 제거 (§7 스코프 전제)
        for key in list(self._levels):
            if key not in seen:
                del self._levels[key]

    def record_trade(self, trade: AggTradeEvent) -> None:
        """체결을 레벨에 귀속: sell-aggressor는 bid 레벨을, buy-aggressor는 ask 레벨을 소진."""
        level_side = Side.BUY if trade.aggressor_side is Side.SELL else Side.SELL
        entry = self._levels.get((level_side, trade.price))
        if entry is not None:
            entry.cum_traded_at_level += trade.qty

    def get(self, side: Side, price: Decimal) -> TrackedLevel | None:
        return self._levels.get((side, price))

    def __len__(self) -> int:
        return len(self._levels)
