"""D2 기준선 워밍업 부트스트랩 (PRD §8 D2 v1.3).

기동 직후 REST klines(1m)로 `volume_baseline`을 선적재한다. 인메모리 상태 복원
(PRD §12 금지 원칙)이 아니라 외부 소스 재조회. 실패는 치명이 아니다 —
`BootstrapError`를 받은 호출자가 로그 후 창이 자연히 찰 때까지 D2 판정을 보류한다.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal, InvalidOperation

import aiohttp

BASE_URL = "https://api.binance.com"
KLINES_LIMIT = 1000  # Binance 요청당 상한
REQUEST_TIMEOUT_SECONDS = 10.0


class BootstrapError(Exception):
    pass


async def fetch_minute_volumes(
    symbol: str,
    minutes: int,
    *,
    base_url: str = BASE_URL,
    get=None,  # 테스트 주입점: url → 파싱된 JSON
    now_ms: int | None = None,
) -> list[tuple[int, Decimal]]:
    """최근 `minutes`분의 (open_time_ms, 분당 총 볼륨) 목록 — VolumeBaseline.bootstrap 입력."""
    get = get or _default_get
    rest_symbol = symbol.replace("/", "").upper()
    start = (now_ms if now_ms is not None else int(time.time() * 1000)) - minutes * 60_000

    bars: list[tuple[int, Decimal]] = []
    cursor = start
    try:
        while True:
            url = (
                f"{base_url}/api/v3/klines?symbol={rest_symbol}&interval=1m"
                f"&startTime={cursor}&limit={KLINES_LIMIT}"
            )
            rows = await get(url)
            if not isinstance(rows, list):
                raise BootstrapError(f"klines: expected list, got {type(rows).__name__}")
            if not rows:
                break
            bars.extend(_parse_bar(row) for row in rows)
            if len(rows) < KLINES_LIMIT:
                break
            cursor = bars[-1][0] + 60_000
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
        raise BootstrapError(f"{type(exc).__name__}: {exc}") from exc
    return bars


def _parse_bar(row: object) -> tuple[int, Decimal]:
    # kline row: [openTime, open, high, low, close, volume, ...]
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        raise BootstrapError(f"klines: malformed row: {row!r}")
    open_time, volume = row[0], row[5]
    if isinstance(open_time, bool) or not isinstance(open_time, int):
        raise BootstrapError(f"klines: openTime must be int, got {type(open_time).__name__}")
    if not isinstance(volume, str):
        raise BootstrapError(f"klines: volume must be string, got {type(volume).__name__}")
    try:
        return open_time, Decimal(volume)
    except InvalidOperation:
        raise BootstrapError(f"klines: not a valid volume: {volume!r}") from None


async def _default_get(url: str):
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise BootstrapError(f"klines: HTTP {resp.status}")
            return await resp.json()
