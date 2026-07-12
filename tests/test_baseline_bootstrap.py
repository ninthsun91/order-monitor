"""D2 기준선 REST 부트스트랩 (PRD §8 D2 v1.3) — 페이지네이션·파싱·실패 래핑."""

import asyncio
from decimal import Decimal

import aiohttp
import pytest

from order_monitor.ingestion.baseline_bootstrap import BootstrapError, fetch_minute_volumes


def make_rows(start_minute, count, vol="4"):
    # kline row: [openTime, o, h, l, c, volume, ...]
    return [
        [m * 60_000, "0", "0", "0", "0", vol, 0, 0, 0, "0", "0", "0"]
        for m in range(start_minute, start_minute + count)
    ]


def test_paginates_until_short_page():
    calls = []

    async def fake_get(url):
        calls.append(url)
        return make_rows(0, 1000) if len(calls) == 1 else make_rows(1000, 440)

    bars = asyncio.run(
        fetch_minute_volumes("BTC/USDT", 1440, get=fake_get, now_ms=1440 * 60_000)
    )
    assert len(bars) == 1440
    assert bars[0] == (0, Decimal(4))
    assert bars[-1][0] == 1439 * 60_000
    assert "symbol=BTCUSDT" in calls[0] and "startTime=0" in calls[0]
    assert "startTime=60000000" in calls[1]  # 직전 마지막 봉 + 1분


def test_malformed_row_raises():
    async def fake_get(url):
        return [[123, "0"]]

    with pytest.raises(BootstrapError, match="malformed"):
        asyncio.run(fetch_minute_volumes("BTC/USDT", 60, get=fake_get, now_ms=0))


def test_non_string_volume_raises():
    async def fake_get(url):
        return [[0, "0", "0", "0", "0", 4.0]]

    with pytest.raises(BootstrapError, match="volume must be string"):
        asyncio.run(fetch_minute_volumes("BTC/USDT", 60, get=fake_get, now_ms=0))


def test_network_error_wrapped():
    async def fake_get(url):
        raise aiohttp.ClientError("boom")

    with pytest.raises(BootstrapError, match="ClientError"):
        asyncio.run(fetch_minute_volumes("BTC/USDT", 60, get=fake_get, now_ms=0))
