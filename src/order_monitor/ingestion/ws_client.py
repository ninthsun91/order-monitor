"""Binance combined stream WebSocket 클라이언트 (raw aiohttp — DEVELOPMENT_PLAN 결정 기록).

- combined stream URL로 세 스트림을 한 연결에 구독 — URL 구독은 연결 성립 = 구독 완료
  (별도 SUBSCRIBE 프레임 없음). epoch 시작은 어차피 세 스트림 각각의 첫 수신을
  요구하므로(§5.4) 구독 확인은 연결 성립 통지로 충분하다
- 재연결: 지수 백오프 1s→60s (PRD §5.3, §11.1). 연결이 `stable_connection_seconds`
  이상 유지됐으면 백오프 리셋 — 24h 강제 단절·서버 유지보수 후 즉시 재연결
- keepalive: 서버 ping(20s 주기)에 대한 pong은 aiohttp autoping이 응답. 추가로
  클라이언트 자체 ping(`heartbeat=20`)을 켠다 — 서버 CLOSE가 도달하지 않는 half-open
  TCP에서 pong 미수신으로 receive()가 실패해 재연결 루프가 작동하게 (M1 검증 런 발견:
  autoping만으로는 죽은 연결에서 무한 블록)
- 연결/단절/재연결 대기는 구조화 로그로 남긴다 (DEVELOPMENT_PLAN M1 체크박스)
- 파싱 실패는 로그 후 계속 — 지속 실패 시 이벤트가 흐르지 않아 staleness로 표면화
  (PRD §14 정규화 계층 감지)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable

import aiohttp

from order_monitor.ingestion.events import Event, NormalizationError, parse_stream_message, stream_names

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "wss://stream.binance.com:9443"
CLIENT_HEARTBEAT_SECONDS = 20.0  # 서버 ping 주기와 동일

_CLOSE_TYPES = (
    aiohttp.WSMsgType.CLOSE,
    aiohttp.WSMsgType.CLOSING,
    aiohttp.WSMsgType.CLOSED,
    aiohttp.WSMsgType.ERROR,
)


class BinanceWSClient:
    def __init__(
        self,
        symbol: str,
        *,
        on_event: Callable[[str, Event], None],
        on_connected: Callable[[], None],
        on_disconnected: Callable[[], None],
        base_url: str = DEFAULT_BASE_URL,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
        stable_connection_seconds: float = 60.0,
        monotonic: Callable[[], float] = time.monotonic,
        sleep=asyncio.sleep,
        connect=None,  # 테스트 주입점: url → ws async context manager
    ) -> None:
        self._streams = stream_names(symbol)
        self._url = f"{base_url}/stream?streams={'/'.join(self._streams)}"
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
        """단절 시 지수 백오프로 무한 재연결. 취소로만 종료된다."""
        backoff = self._initial_backoff
        while True:
            connected_at = self._monotonic()
            try:
                await self._run_connection()
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, OSError) as exc:
                logger.error(
                    "ws connection error",
                    extra={"url": self._url, "error": f"{type(exc).__name__}: {exc}"},
                )
            uptime = self._monotonic() - connected_at
            if uptime >= self._stable_seconds:
                backoff = self._initial_backoff
            logger.info(
                "ws reconnect wait",
                extra={"backoff_seconds": backoff, "uptime_seconds": uptime},
            )
            await self._sleep(backoff)
            backoff = min(backoff * 2, self._max_backoff)

    async def _run_connection(self) -> None:
        async with self._connect(self._url) as ws:
            logger.info("ws connected", extra={"url": self._url})
            self._on_connected()
            try:
                await self._pump(ws)
            finally:
                logger.warning("ws disconnected", extra={"url": self._url})
                self._on_disconnected()

    async def _pump(self, ws) -> None:
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                self._handle_text(msg.data)
            elif msg.type in _CLOSE_TYPES:
                logger.warning("ws stream closed", extra={"msg_type": msg.type.name})
                return
            # PING/PONG/BINARY 등은 무시 (pong 응답은 aiohttp autoping)

    def _handle_text(self, data: str) -> None:
        receive_monotonic = self._monotonic()
        try:
            payload = json.loads(data)
            stream, event = parse_stream_message(payload, receive_monotonic)
        except (json.JSONDecodeError, NormalizationError) as exc:
            logger.error(
                "message normalization failed",
                extra={"error": str(exc), "raw_prefix": data[:200]},
            )
            return
        self._on_event(stream, event)
