"""최근 체결 시간창 (PRD §7 `trade_window`).

만료 기준은 단일 스트림 내 시간창이므로 aggTrade의 `exchange_time_ms`(T)를 쓴다
(PRD §11.1 — exchange_time은 단일 스트림 시간창 전용, monotonic은 크로스 스트림용).
"""

from __future__ import annotations

import collections
from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, Side


class TradeWindow:
    def __init__(self, window_seconds: float) -> None:
        self._window_ms = int(window_seconds * 1000)
        self._trades: collections.deque[AggTradeEvent] = collections.deque()
        self._latest_exchange_time_ms: int | None = None

    def add(self, trade: AggTradeEvent) -> None:
        self._trades.append(trade)
        # 드문 순서 역전에 대비해 최신 시각은 최대값으로 유지
        if self._latest_exchange_time_ms is None or trade.exchange_time_ms > self._latest_exchange_time_ms:
            self._latest_exchange_time_ms = trade.exchange_time_ms
        self._expire()

    def _expire(self) -> None:
        cutoff = self._latest_exchange_time_ms - self._window_ms
        while self._trades and self._trades[0].exchange_time_ms < cutoff:
            self._trades.popleft()

    def sum_qty(self, aggressor_side: Side) -> Decimal:
        """방향 분리 합계."""
        return sum(
            (t.qty for t in self._trades if t.aggressor_side is aggressor_side),
            Decimal(0),
        )

    def totals(self) -> tuple[Decimal, Decimal]:
        """(매수 aggressor 합, 매도 aggressor 합) — D2 총량·델타 산출용 단일 순회 (PRD §8 D2 v1.3)."""
        buy = sell = Decimal(0)
        for t in self._trades:
            if t.aggressor_side is Side.BUY:
                buy += t.qty
            else:
                sell += t.qty
        return buy, sell

    def __iter__(self):
        """창 내 체결 순회 (오래된 것부터) — D2 에피소드 시드용."""
        return iter(self._trades)

    def __len__(self) -> int:
        return len(self._trades)

    def clear(self) -> None:
        self._trades.clear()
        self._latest_exchange_time_ms = None
