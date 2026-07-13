"""top-20 레벨의 현재 크기 + 체결 귀속 (PRD §7 `level_tracker`, v1.2 축소형).

`{price, side, current_size, cum_traded_at_level}`만 유지 — 레벨 생애주기 추적은
`wall_registry` 담당.

스코프 전제 (PRD §7): 귀속 판정이 일어나는 시점에는 가격이 레벨에 접촉해 있어
해당 레벨이 top-20 창 안에 있다 — 창을 벗어난 레벨의 엔트리는 제거된다.
**예외 (v1.4, M3)**: `retain`이 참인 레벨(벽 레지스트리 등록 가격)은 창 이탈에도
엔트리를 보존한다 — 접촉 episode 사이의 반등으로 창을 벗어나면 생애 누적
(`cum_traded_at_level`)이 소실되어 D3 흡수·D1 FILLED/PULLED 귀속이 침묵하는
누락을 막는다 (PRD §8 D3 "누적은 레벨 생애 기준"). 벽이 레지스트리에서 소멸하면
predicate가 거짓이 되어 다음 스냅샷에서 자연 제거된다.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, DepthSnapshot, Side


@dataclasses.dataclass
class TrackedLevel:
    price: Decimal
    side: Side  # 레벨의 side: bid 레벨 = BUY, ask 레벨 = SELL
    current_size: Decimal
    cum_traded_at_level: Decimal = Decimal(0)


class LevelTracker:
    def __init__(self, retain: Callable[[Side, Decimal], bool] | None = None) -> None:
        self._levels: dict[tuple[Side, Decimal], TrackedLevel] = {}
        self._retain = retain

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
        # top-20 창을 벗어난 레벨은 제거 — 단 retain 레벨(벽)은 누적 보존 (§7 v1.4 예외)
        for key in list(self._levels):
            if key not in seen and not (self._retain is not None and self._retain(*key)):
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
