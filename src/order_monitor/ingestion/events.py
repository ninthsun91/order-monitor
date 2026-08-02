"""수신 메시지 정규화 계층 (PRD §7, §11.1).

세 스트림의 raw JSON을 타입이 명확한 이벤트 객체로 변환한다.
- 가격·수량은 수신 문자열을 그대로 `Decimal`로 파싱 — 판정 경로 float 금지 (PRD §7)
- 모든 이벤트에 `local_monotonic_receive_time`을 스탬프, 거래소 시각이 있는 스트림
  (aggTrade `T`, diff `E`)은 `exchange_time_ms`를 병기 (PRD §11.1 — depth20에는 부재)
- 파싱 실패는 `NormalizationError`로 명시 감지한다 (PRD §14 — 스펙 변경 리스크 완화)
"""

from __future__ import annotations

import dataclasses
import enum
from decimal import Decimal, InvalidOperation

DEPTH20_STREAM_SUFFIX = "@depth20@100ms"
AGG_TRADE_STREAM_SUFFIX = "@aggTrade"
DIFF_DEPTH_STREAM_SUFFIX = "@depth@100ms"


class NormalizationError(Exception):
    pass


class Side(enum.Enum):
    BUY = "buy"
    SELL = "sell"


@dataclasses.dataclass(frozen=True, slots=True)
class DepthSnapshot:
    """`@depth20@100ms` partial snapshot — 거래소 시각 부재 (M1 스파이크 실측)."""

    last_update_id: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    local_monotonic_receive_time: float


@dataclasses.dataclass(frozen=True, slots=True)
class AggTradeEvent:
    """`@aggTrade` 체결. `m=True` → 매도 aggressor (PRD §4)."""

    agg_trade_id: int
    price: Decimal
    qty: Decimal
    aggressor_side: Side
    exchange_time_ms: int
    local_monotonic_receive_time: float


@dataclasses.dataclass(frozen=True, slots=True)
class DiffDepthEvent:
    """`@depth@100ms` diff — 절대 잔량 이벤트 탭 (PRD §5.1 설계 결정 2).

    `first_update_id`/`final_update_id`(U/u)는 누락 탐지 전용 (PRD §5.1 v1.2).
    """

    first_update_id: int
    final_update_id: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    exchange_time_ms: int
    local_monotonic_receive_time: float


Event = DepthSnapshot | AggTradeEvent | DiffDepthEvent


def stream_symbol(symbol: str) -> str:
    """config의 `BTC/USDT` → 스트림 이름용 `btcusdt`."""
    return symbol.replace("/", "").lower()


def stream_names(symbol: str) -> tuple[str, str, str]:
    base = stream_symbol(symbol)
    return (
        f"{base}{DEPTH20_STREAM_SUFFIX}",
        f"{base}{AGG_TRADE_STREAM_SUFFIX}",
        f"{base}{DIFF_DEPTH_STREAM_SUFFIX}",
    )


def _require(data: dict, key: str, context: str) -> object:
    if key not in data:
        raise NormalizationError(f"{context}: missing field '{key}'")
    return data[key]


def _parse_decimal(value: object, context: str) -> Decimal:
    if not isinstance(value, str):
        raise NormalizationError(f"{context}: expected string number, got {type(value).__name__}")
    try:
        return Decimal(value)
    except InvalidOperation:
        raise NormalizationError(f"{context}: not a valid number: {value!r}") from None


def _parse_int(value: object, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise NormalizationError(f"{context}: expected int, got {type(value).__name__}")
    return value


def _parse_levels(value: object, context: str) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(value, list):
        raise NormalizationError(f"{context}: expected list of levels")
    levels = []
    for i, entry in enumerate(value):
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            raise NormalizationError(f"{context}[{i}]: expected [price, qty] pair")
        price = _parse_decimal(entry[0], f"{context}[{i}].price")
        qty = _parse_decimal(entry[1], f"{context}[{i}].qty")
        levels.append((price, qty))
    return tuple(levels)


def parse_depth20(data: object, receive_monotonic: float) -> DepthSnapshot:
    if not isinstance(data, dict):
        raise NormalizationError("depth20: payload must be a mapping")
    return DepthSnapshot(
        last_update_id=_parse_int(_require(data, "lastUpdateId", "depth20"), "depth20.lastUpdateId"),
        bids=_parse_levels(_require(data, "bids", "depth20"), "depth20.bids"),
        asks=_parse_levels(_require(data, "asks", "depth20"), "depth20.asks"),
        local_monotonic_receive_time=receive_monotonic,
    )


def parse_agg_trade(data: object, receive_monotonic: float) -> AggTradeEvent:
    if not isinstance(data, dict):
        raise NormalizationError("aggTrade: payload must be a mapping")
    is_buyer_maker = _require(data, "m", "aggTrade")
    if not isinstance(is_buyer_maker, bool):
        raise NormalizationError("aggTrade.m: expected bool")
    return AggTradeEvent(
        agg_trade_id=_parse_int(_require(data, "a", "aggTrade"), "aggTrade.a"),
        price=_parse_decimal(_require(data, "p", "aggTrade"), "aggTrade.p"),
        qty=_parse_decimal(_require(data, "q", "aggTrade"), "aggTrade.q"),
        # m=True → 매수자가 maker → 시장가 매도가 체결을 일으킴 (비드 히트)
        aggressor_side=Side.SELL if is_buyer_maker else Side.BUY,
        exchange_time_ms=_parse_int(_require(data, "T", "aggTrade"), "aggTrade.T"),
        local_monotonic_receive_time=receive_monotonic,
    )


def parse_diff_depth(data: object, receive_monotonic: float) -> DiffDepthEvent:
    if not isinstance(data, dict):
        raise NormalizationError("diff: payload must be a mapping")
    return DiffDepthEvent(
        first_update_id=_parse_int(_require(data, "U", "diff"), "diff.U"),
        final_update_id=_parse_int(_require(data, "u", "diff"), "diff.u"),
        bids=_parse_levels(_require(data, "b", "diff"), "diff.b"),
        asks=_parse_levels(_require(data, "a", "diff"), "diff.a"),
        exchange_time_ms=_parse_int(_require(data, "E", "diff"), "diff.E"),
        local_monotonic_receive_time=receive_monotonic,
    )


def parse_stream_message(message: object, receive_monotonic: float) -> tuple[str, Event | None]:
    """combined stream 래핑 메시지 `{"stream": ..., "data": ...}`를 파싱한다.

    반환값은 `(stream_name, event)` — 스트림별 헬스 추적(PRD §5.4)이 스트림명을 쓴다.
    (v1.16) `coinbase:` 접두 스트림은 Coinbase 파서로 위임 — 이벤트가 None일 수 있다
    (heartbeat 등, 호출자가 스킵). 바이낸스 경로는 None을 반환하지 않는다.
    """
    if not isinstance(message, dict):
        raise NormalizationError("stream message must be a mapping")
    stream = _require(message, "stream", "message")
    if not isinstance(stream, str):
        raise NormalizationError("message.stream: expected string")
    data = _require(message, "data", "message")

    if stream.startswith("coinbase:"):
        # 지연 import — coinbase 모듈이 본 모듈의 이벤트 타입을 import (순환 회피)
        from order_monitor.ingestion.coinbase import parse_coinbase_message

        return parse_coinbase_message(data, receive_monotonic)

    if stream.endswith(DEPTH20_STREAM_SUFFIX):
        return stream, parse_depth20(data, receive_monotonic)
    if stream.endswith(AGG_TRADE_STREAM_SUFFIX):
        return stream, parse_agg_trade(data, receive_monotonic)
    if stream.endswith(DIFF_DEPTH_STREAM_SUFFIX):
        return stream, parse_diff_depth(data, receive_monotonic)
    raise NormalizationError(f"unknown stream: {stream}")
