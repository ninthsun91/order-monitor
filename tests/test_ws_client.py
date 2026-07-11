import asyncio
import contextlib
import json
from types import SimpleNamespace

import aiohttp
import pytest

from order_monitor.ingestion.events import AggTradeEvent, DepthSnapshot
from order_monitor.ingestion.ws_client import BinanceWSClient

DEPTH_MSG = json.dumps(
    {
        "stream": "btcusdt@depth20@100ms",
        "data": {"lastUpdateId": 1, "bids": [["61000.00", "1.0"]], "asks": []},
    }
)
AGG_MSG = json.dumps(
    {
        "stream": "btcusdt@aggTrade",
        "data": {"a": 1, "p": "61000.00", "q": "0.5", "T": 1000, "m": True},
    }
)


def text(data):
    return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=data)


CLOSED = SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)


class FakeWS:
    """지정된 메시지를 순서대로 내보내고 소진 시 CLOSED를 반환."""

    def __init__(self, messages):
        self._messages = list(messages)

    async def receive(self):
        if self._messages:
            return self._messages.pop(0)
        return CLOSED


class Recorder:
    def __init__(self):
        self.events = []
        self.connected = 0
        self.disconnected = 0

    def on_event(self, stream, event):
        self.events.append((stream, event))

    def on_connected(self):
        self.connected += 1

    def on_disconnected(self):
        self.disconnected += 1


def make_client(recorder, connect, *, sleeps=None, monotonic=None, **kwargs):
    async def fake_sleep(seconds):
        if sleeps is not None:
            sleeps.append(seconds)

    return BinanceWSClient(
        "BTC/USDT",
        on_event=recorder.on_event,
        on_connected=recorder.on_connected,
        on_disconnected=recorder.on_disconnected,
        monotonic=monotonic or (lambda: 0.0),
        sleep=fake_sleep,
        connect=connect,
        **kwargs,
    )


async def run_until(client, done: asyncio.Event, timeout=2.0):
    task = asyncio.create_task(client.run())
    try:
        await asyncio.wait_for(done.wait(), timeout)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_dispatch_and_lifecycle_callbacks():
    recorder = Recorder()
    done = asyncio.Event()

    @contextlib.asynccontextmanager
    async def connect(url):
        assert url == (
            "wss://stream.binance.com:9443/stream?streams="
            "btcusdt@depth20@100ms/btcusdt@aggTrade/btcusdt@depth@100ms"
        )
        if recorder.connected == 0:
            yield FakeWS([text(DEPTH_MSG), text(AGG_MSG)])
        else:
            done.set()
            await asyncio.Event().wait()  # 두 번째 연결은 보류 (테스트 종료 대기)
            yield  # pragma: no cover

    await run_until(make_client(recorder, connect), done)

    assert recorder.connected == 1
    assert recorder.disconnected == 1  # 소진 → CLOSED → 단절 콜백
    assert [s for s, _ in recorder.events] == ["btcusdt@depth20@100ms", "btcusdt@aggTrade"]
    assert isinstance(recorder.events[0][1], DepthSnapshot)
    assert isinstance(recorder.events[1][1], AggTradeEvent)


@pytest.mark.asyncio
async def test_malformed_messages_are_skipped_not_fatal():
    recorder = Recorder()
    done = asyncio.Event()

    @contextlib.asynccontextmanager
    async def connect(url):
        if recorder.connected == 0:
            yield FakeWS(
                [
                    text("not json"),
                    text(json.dumps({"stream": "btcusdt@aggTrade", "data": {"bad": 1}})),
                    text(DEPTH_MSG),
                ]
            )
        else:
            done.set()
            await asyncio.Event().wait()
            yield  # pragma: no cover

    await run_until(make_client(recorder, connect), done)
    assert len(recorder.events) == 1  # 유효 메시지만 통과
    assert recorder.disconnected == 1


@pytest.mark.asyncio
async def test_exponential_backoff_on_connect_failure():
    recorder = Recorder()
    sleeps = []
    done = asyncio.Event()

    @contextlib.asynccontextmanager
    async def connect(url):
        if len(sleeps) >= 8:
            done.set()
            await asyncio.Event().wait()
        raise aiohttp.ClientConnectionError("refused")
        yield  # pragma: no cover

    await run_until(make_client(recorder, connect, sleeps=sleeps), done)
    assert sleeps[:8] == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]  # 60s 상한
    assert recorder.connected == 0


@pytest.mark.asyncio
async def test_backoff_resets_after_stable_connection():
    recorder = Recorder()
    sleeps = []
    done = asyncio.Event()
    clock = SimpleNamespace(now=0.0)

    @contextlib.asynccontextmanager
    async def connect(url):
        if len(sleeps) < 3:
            raise aiohttp.ClientConnectionError("refused")  # 백오프 누적: 1, 2, 4
        if recorder.connected == 0:
            clock.now += 120.0  # 안정 연결 (>60s) 후 단절
            yield FakeWS([])
            return
        done.set()
        await asyncio.Event().wait()
        yield  # pragma: no cover

    client = make_client(recorder, connect, sleeps=sleeps, monotonic=lambda: clock.now)
    await run_until(client, done)
    # 실패 3회(1,2,4) 후 안정 연결 → 다음 대기는 초기값 1로 리셋
    assert sleeps == [1.0, 2.0, 4.0, 1.0]
    assert recorder.connected == 1
    assert recorder.disconnected == 1
