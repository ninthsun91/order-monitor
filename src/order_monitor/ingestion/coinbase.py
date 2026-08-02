"""Coinbase Exchange WS 어댑터 (M8, PRD §5.5) — level2_batch + matches + ticker + heartbeat.

어댑터 계약: 기존 정규화 이벤트 3종을 생산하면 상태·디텍터·알림·영속화 계층이
무변경으로 재사용된다 (§5.5). 스트림명은 `coinbase:<product>@<channel>` 합성
(scripts/capture_stream.py와 동일 규약 — replay 픽스처 호환).

- `snapshot`/`l2update` → DiffDepthEvent: changes는 [side, price, size] 트리플,
  절대 잔량 (2026-08-02 라이브 캡처 검증 — Binance diff 탭과 동일 의미론).
  update id는 채널에 시퀀스가 없어 0 고정 — U/u 연속성 검사는 비활성(§5.5),
  청취 공백 대응은 trade_id 연속성(health)·staleness·재연결 시 full snapshot 재수신 소관
- `match`/`last_match` → AggTradeEvent: **side는 maker 주문 side** (라이브 캡처 검증:
  best_ask 체결이 side="sell", 동일 sequence의 ticker taker side="buy"와 교차 확인) —
  side=="sell" → 매수 aggressor, side=="buy" → 매도 aggressor
- `ticker` → top-1 DepthSnapshot 합성 (best bid/ask + size) — D3 접촉 계측·best price
  전용. **로컬 풀북 유지 금지** (§5.5 — D2·D4 제외 스코프가 근접북 필요를 제거)
- `heartbeat`·`subscriptions` → 이벤트 없음 (None 반환, 호출자가 스킵)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

import aiohttp

from order_monitor.ingestion.events import (
    AggTradeEvent,
    DepthSnapshot,
    DiffDepthEvent,
    Event,
    NormalizationError,
    Side,
    _parse_decimal,
    _parse_int,
    _require,
)

logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://ws-feed.exchange.coinbase.com"
CHANNELS = ("level2_batch", "matches", "ticker", "heartbeat")
CLIENT_HEARTBEAT_SECONDS = 20.0

_CLOSE_TYPES = (
    aiohttp.WSMsgType.CLOSE,
    aiohttp.WSMsgType.CLOSING,
    aiohttp.WSMsgType.CLOSED,
    aiohttp.WSMsgType.ERROR,
)


def coinbase_stream_name(product_id: str, channel: str) -> str:
    return f"coinbase:{product_id}@{channel}"


def coinbase_stream_names(product_id: str) -> tuple[str, str, str]:
    """SessionEpochTracker의 (depth류, 체결류, diff류) 순서 — ticker가 depth류 (§5.5)."""
    return (
        coinbase_stream_name(product_id, "ticker"),
        coinbase_stream_name(product_id, "matches"),
        coinbase_stream_name(product_id, "level2"),
    )


_CHANNEL_BY_TYPE = {
    "snapshot": "level2",
    "l2update": "level2",
    "match": "matches",
    "last_match": "matches",
    "ticker": "ticker",
    "heartbeat": "heartbeat",
}


def _parse_iso_ms(value: object, context: str) -> int:
    if not isinstance(value, str):
        raise NormalizationError(f"{context}: expected ISO time string, got {type(value).__name__}")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise NormalizationError(f"{context}: not a valid ISO time: {value!r}") from None
    return int(dt.timestamp() * 1000)


def _parse_changes(value: object, context: str) -> tuple[tuple, tuple]:
    """l2update `changes` = [side, price, size] 트리플 목록 → (bids, asks)."""
    if not isinstance(value, list):
        raise NormalizationError(f"{context}: expected list of changes")
    bids: list[tuple[Decimal, Decimal]] = []
    asks: list[tuple[Decimal, Decimal]] = []
    for i, entry in enumerate(value):
        if not isinstance(entry, (list, tuple)) or len(entry) != 3:
            raise NormalizationError(f"{context}[{i}]: expected [side, price, size] triple")
        side, price_raw, size_raw = entry
        price = _parse_decimal(price_raw, f"{context}[{i}].price")
        size = _parse_decimal(size_raw, f"{context}[{i}].size")
        if side == "buy":
            bids.append((price, size))
        elif side == "sell":
            asks.append((price, size))
        else:
            raise NormalizationError(f"{context}[{i}].side: expected buy/sell, got {side!r}")
    return tuple(bids), tuple(asks)


def _parse_snapshot(data: dict, receive_monotonic: float) -> DiffDepthEvent:
    from order_monitor.ingestion.events import _parse_levels

    return DiffDepthEvent(
        first_update_id=0,
        final_update_id=0,
        bids=_parse_levels(_require(data, "bids", "cb.snapshot"), "cb.snapshot.bids"),
        asks=_parse_levels(_require(data, "asks", "cb.snapshot"), "cb.snapshot.asks"),
        exchange_time_ms=_parse_iso_ms(_require(data, "time", "cb.snapshot"), "cb.snapshot.time"),
        local_monotonic_receive_time=receive_monotonic,
    )


def _parse_l2update(data: dict, receive_monotonic: float) -> DiffDepthEvent:
    bids, asks = _parse_changes(_require(data, "changes", "cb.l2update"), "cb.l2update.changes")
    return DiffDepthEvent(
        first_update_id=0,
        final_update_id=0,
        bids=bids,
        asks=asks,
        exchange_time_ms=_parse_iso_ms(_require(data, "time", "cb.l2update"), "cb.l2update.time"),
        local_monotonic_receive_time=receive_monotonic,
    )


def _parse_match(data: dict, receive_monotonic: float) -> AggTradeEvent:
    maker_side = _require(data, "side", "cb.match")
    if maker_side not in ("buy", "sell"):
        raise NormalizationError(f"cb.match.side: expected buy/sell, got {maker_side!r}")
    return AggTradeEvent(
        agg_trade_id=_parse_int(_require(data, "trade_id", "cb.match"), "cb.match.trade_id"),
        price=_parse_decimal(_require(data, "price", "cb.match"), "cb.match.price"),
        qty=_parse_decimal(_require(data, "size", "cb.match"), "cb.match.size"),
        # side = maker side → maker sell(ask 잔량 소진) = 매수 aggressor
        aggressor_side=Side.BUY if maker_side == "sell" else Side.SELL,
        exchange_time_ms=_parse_iso_ms(_require(data, "time", "cb.match"), "cb.match.time"),
        local_monotonic_receive_time=receive_monotonic,
    )


def _parse_ticker(data: dict, receive_monotonic: float) -> DepthSnapshot:
    bid = _parse_decimal(_require(data, "best_bid", "cb.ticker"), "cb.ticker.best_bid")
    ask = _parse_decimal(_require(data, "best_ask", "cb.ticker"), "cb.ticker.best_ask")
    bid_size = (
        _parse_decimal(data["best_bid_size"], "cb.ticker.best_bid_size")
        if "best_bid_size" in data
        else Decimal(0)
    )
    ask_size = (
        _parse_decimal(data["best_ask_size"], "cb.ticker.best_ask_size")
        if "best_ask_size" in data
        else Decimal(0)
    )
    return DepthSnapshot(
        last_update_id=_parse_int(_require(data, "sequence", "cb.ticker"), "cb.ticker.sequence"),
        bids=((bid, bid_size),),
        asks=((ask, ask_size),),
        local_monotonic_receive_time=receive_monotonic,
    )


def parse_coinbase_message(data: object, receive_monotonic: float) -> tuple[str, Event | None]:
    """Coinbase 원시 메시지(래퍼 없음) → (합성 스트림명, 이벤트 | None).

    heartbeat·subscriptions는 정규화 이벤트가 없어 None — 호출자가 스킵한다.
    미지 타입은 NormalizationError (PRD §14 — 스펙 변경 명시 감지).
    """
    if not isinstance(data, dict):
        raise NormalizationError("cb: payload must be a mapping")
    msg_type = _require(data, "type", "cb")
    product = data.get("product_id", "")
    if msg_type == "snapshot":
        return coinbase_stream_name(product, "level2"), _parse_snapshot(data, receive_monotonic)
    if msg_type == "l2update":
        return coinbase_stream_name(product, "level2"), _parse_l2update(data, receive_monotonic)
    if msg_type in ("match", "last_match"):
        return coinbase_stream_name(product, "matches"), _parse_match(data, receive_monotonic)
    if msg_type == "ticker":
        return coinbase_stream_name(product, "ticker"), _parse_ticker(data, receive_monotonic)
    if msg_type in ("heartbeat", "subscriptions"):
        return coinbase_stream_name(product, str(msg_type)), None
    raise NormalizationError(f"cb: unknown message type: {msg_type!r}")


class CoinbaseWSClient:
    """BinanceWSClient와 동일 콜백 계약 (on_event/on_connected/on_disconnected).

    차이: 단일 URL + 연결 후 subscribe 프레임 전송 (combined URL 구독이 아님).
    구독 확인은 subscribe 전송 시점의 on_connected 통지로 충분하다 — epoch 시작은
    어차피 세 스트림 각각의 첫 수신을 요구한다 (§5.4).
    """

    def __init__(
        self,
        product_id: str,
        *,
        on_event: Callable[[str, Event], None],
        on_connected: Callable[[], None],
        on_disconnected: Callable[[], None],
        base_url: str = DEFAULT_WS_URL,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
        stable_connection_seconds: float = 60.0,
        monotonic: Callable[[], float] = time.monotonic,
        sleep=asyncio.sleep,
        connect=None,  # 테스트 주입점: url → ws async context manager
    ) -> None:
        self._product_id = product_id
        self._url = base_url
        self._on_event = on_event
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._initial_backoff = initial_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._stable_seconds = stable_connection_seconds
        self._monotonic = monotonic
        self._sleep = sleep
        self._connect = connect or self._default_connect

    @staticmethod
    @contextlib.asynccontextmanager
    async def _default_connect(url: str):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=CLIENT_HEARTBEAT_SECONDS) as ws:
                yield ws

    async def run(self) -> None:
        """단절 시 지수 백오프로 무한 재연결. 취소로만 종료된다 (BinanceWSClient와 동일)."""
        backoff = self._initial_backoff
        while True:
            connected_at = self._monotonic()
            try:
                await self._run_connection()
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, OSError) as exc:
                logger.error(
                    "coinbase ws connection error",
                    extra={"url": self._url, "error": f"{type(exc).__name__}: {exc}"},
                )
            uptime = self._monotonic() - connected_at
            if uptime >= self._stable_seconds:
                backoff = self._initial_backoff
            logger.info(
                "coinbase ws reconnect wait",
                extra={"backoff_seconds": backoff, "uptime_seconds": uptime},
            )
            await self._sleep(backoff)
            backoff = min(backoff * 2, self._max_backoff)

    async def _run_connection(self) -> None:
        async with self._connect(self._url) as ws:
            await ws.send_json(
                {
                    "type": "subscribe",
                    "product_ids": [self._product_id],
                    "channels": list(CHANNELS),
                }
            )
            logger.info("coinbase ws connected", extra={"product_id": self._product_id})
            self._on_connected()
            try:
                await self._pump(ws)
            finally:
                logger.warning("coinbase ws disconnected", extra={"product_id": self._product_id})
                self._on_disconnected()

    async def _pump(self, ws) -> None:
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                self._handle_text(msg.data)
            elif msg.type in _CLOSE_TYPES:
                logger.warning("coinbase ws stream closed", extra={"msg_type": msg.type.name})
                return

    def _handle_text(self, data: str) -> None:
        receive_monotonic = self._monotonic()
        try:
            payload = json.loads(data)
            stream, event = parse_coinbase_message(payload, receive_monotonic)
        except (json.JSONDecodeError, NormalizationError) as exc:
            logger.error(
                "coinbase message normalization failed",
                extra={"error": str(exc), "raw_prefix": data[:200]},
            )
            return
        if event is None:  # heartbeat/subscriptions
            return
        self._on_event(stream, event)
