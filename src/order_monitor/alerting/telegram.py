"""Telegram 발송기 (PRD §9.2 레이트리밋 대응, §11.1 부분 실패 격리).

- 비동기 큐 분리: `enqueue()`는 논블로킹 — 발송 실패·지연이 파이프라인을 막지 않는다
- 초당 발송 상한(`SEND_INTERVAL_SECONDS`): Bot API 레이트리밋(챗당 ~1msg/s) 대응.
  Telegram 고정 제약이라 설정이 아닌 코드 상수 (PRD §10은 config 키 전수 고정)
- 실패 시 지수 백오프 재시도, `MAX_ATTEMPTS` 소진 시 드롭 + 에러 로그 — M2의
  D1/D2 알림은 시효성 신호라 무한 재시도가 무의미. D5 종국 알림의 유실 방지는
  M4 outbox 소관 (PRD §9.4 적용 범위 한정)
- 토큰은 `TELEGRAM_BOT_TOKEN` 환경변수로만 주입 (main에서 읽어 전달, PRD §9.3).
  URL에 토큰이 들어가므로 로그에는 URL·예외 문자열을 그대로 남기지 않는다
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

import aiohttp

logger = logging.getLogger(__name__)

SEND_INTERVAL_SECONDS = 1.0
MAX_ATTEMPTS = 5
INITIAL_RETRY_BACKOFF_SECONDS = 1.0
MAX_RETRY_BACKOFF_SECONDS = 60.0
REQUEST_TIMEOUT_SECONDS = 10.0


class TelegramSender:
    def __init__(
        self,
        token: str,
        chat_id: str,
        *,
        base_url: str = "https://api.telegram.org",
        monotonic: Callable[[], float] = time.monotonic,
        sleep=asyncio.sleep,
        post=None,  # 테스트 주입점: (url, payload) → (status, body)
    ) -> None:
        self._token = token
        self._url = f"{base_url}/bot{token}/sendMessage"
        self._chat_id = chat_id
        self._monotonic = monotonic
        self._sleep = sleep
        self._post = post or self._default_post
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._last_send_at: float | None = None

    def enqueue(self, text: str) -> None:
        self._queue.put_nowait(text)

    def pending(self) -> int:
        return self._queue.qsize()

    async def run(self) -> None:
        """발송 워커 — 취소로만 종료된다."""
        while True:
            text = await self._queue.get()
            await self._throttle()
            await self._send_with_retry(text)

    async def _throttle(self) -> None:
        if self._last_send_at is None:
            return
        wait = SEND_INTERVAL_SECONDS - (self._monotonic() - self._last_send_at)
        if wait > 0:
            await self._sleep(wait)

    async def _send_with_retry(self, text: str) -> None:
        backoff = INITIAL_RETRY_BACKOFF_SECONDS
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._last_send_at = self._monotonic()
            try:
                status, body = await self._post(
                    self._url, {"chat_id": self._chat_id, "text": text}
                )
                if status == 200:
                    logger.info("telegram alert sent", extra={"attempt": attempt})
                    return
                logger.warning(
                    "telegram send failed",
                    extra={"status": status, "body": self._redact(body)[:200], "attempt": attempt},
                )
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                logger.warning(
                    "telegram send error",
                    extra={
                        "error": f"{type(exc).__name__}: {self._redact(str(exc))}",
                        "attempt": attempt,
                    },
                )
            if attempt < MAX_ATTEMPTS:
                await self._sleep(backoff)
                backoff = min(backoff * 2, MAX_RETRY_BACKOFF_SECONDS)
        logger.error(
            "telegram alert dropped after retries",
            extra={"attempts": MAX_ATTEMPTS, "text_prefix": text[:80]},
        )

    def _redact(self, text: str) -> str:
        return text.replace(self._token, "***") if self._token else text

    async def _default_post(self, url: str, payload: dict) -> tuple[int, str]:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as resp:
                return resp.status, await resp.text()
