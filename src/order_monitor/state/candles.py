"""W 주시 관측기의 무효화 판정용 봉 조립 (PRD §8 W v1.13).

aggTrade `exchange_time_ms`를 15m/1h 경계로 버킷팅한다 — 신규 스트림 없음
(§5.1 3-스트림 역할 분리 유지, kline 구독 기각 §16). 마감가 = 버킷 내 마지막
체결가(도착 순서 기준, §11.1 — 단일 스트림 내 연산이라 거래소 시각 사용 가능).

epoch 공백 취급 (§5.4 v1.13): 공백이 걸친 봉은 `gap_tainted`로 마킹되어
무효화 판정에서 제외된다. 조립기의 초기 상태(재시작 직후 첫 봉)도 관측
공백을 걸친 봉이므로 tainted로 시작한다. 무체결 구간의 빈 봉은 만들지
않는다 — 판정은 실제 마감된 봉에만 걸린다 (빈 봉엔 마감가가 없음).
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

from ..ingestion.events import AggTradeEvent

TIMEFRAME_MS = {"15m": 15 * 60_000, "1h": 60 * 60_000}


@dataclasses.dataclass(frozen=True, slots=True)
class Candle:
    open_time_ms: int  # 버킷 시작 (timeframe 경계 정렬)
    timeframe_ms: int
    close_price: Decimal  # 버킷 내 마지막 체결가
    trade_count: int
    gap_tainted: bool  # epoch 공백/재시작이 걸친 봉 — 무효화 판정 제외


class CandleAssembler:
    def __init__(self, timeframe: str) -> None:
        self._tf_ms = TIMEFRAME_MS[timeframe]
        self._bucket: int | None = None  # exchange_time_ms // tf_ms
        self._close_price: Decimal | None = None
        self._trade_count = 0
        self._tainted = True  # 재시작 직후 첫 봉은 관측 공백을 걸친 봉
        self._gap_open = True  # 공백 진행 중 — 재개 첫 체결의 버킷까지 오염

    def on_trade(self, event: AggTradeEvent) -> Candle | None:
        """체결 반영. 새 버킷 진입 시 직전 봉을 마감해 반환한다.

        순서 주의(호출부 계약): 반환된 마감 봉을 먼저 판정에 소비한 뒤 이
        체결 자체를 계측해야 한다 — 새 버킷의 첫 체결은 다음 봉 소속이다.
        """
        bucket = event.exchange_time_ms // self._tf_ms
        closed: Candle | None = None
        if self._bucket is None:
            self._bucket = bucket
        elif bucket > self._bucket:
            closed = Candle(
                open_time_ms=self._bucket * self._tf_ms,
                timeframe_ms=self._tf_ms,
                close_price=self._close_price,
                trade_count=self._trade_count,
                gap_tainted=self._tainted,
            )
            self._bucket = bucket
            self._close_price = None
            self._trade_count = 0
            self._tainted = False
        if self._gap_open:
            # 공백의 끝(재개 첫 체결)이 속한 버킷도 데이터 결손 — 경계를 걸친
            # 공백은 직전 봉과 재개 봉을 모두 오염시킨다
            self._tainted = True
            self._gap_open = False
        # 드문 순서 역전(bucket < self._bucket)은 현재 봉에 흡수 — 마감가는
        # 도착 순서 기준 마지막 체결가 (volume_baseline의 역전 흡수와 동일 취지)
        self._close_price = event.price
        self._trade_count += 1
        return closed

    def mark_gap(self) -> None:
        """epoch 공백 발생 — 진행 중 봉과 재개 첫 체결의 봉을 tainted로 마킹."""
        self._tainted = True
        self._gap_open = True
