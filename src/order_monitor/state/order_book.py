"""top-20 근접 오더북 상태 (PRD §7 `order_book`).

depth 이벤트마다 통째 교체 — partial snapshot이라 시퀀스 재조정 불필요 (PRD §5.1 설계 결정 1).
"""

from __future__ import annotations

from decimal import Decimal

from order_monitor.ingestion.events import DepthSnapshot


class OrderBook:
    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last_update_id: int | None = None
        self.last_receive_monotonic: float | None = None

    def apply_snapshot(self, snapshot: DepthSnapshot) -> None:
        self.bids = {price: qty for price, qty in snapshot.bids}
        self.asks = {price: qty for price, qty in snapshot.asks}
        self.last_update_id = snapshot.last_update_id
        self.last_receive_monotonic = snapshot.local_monotonic_receive_time

    @property
    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None
