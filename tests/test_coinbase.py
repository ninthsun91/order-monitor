"""Coinbase 어댑터 (M8, PRD §5.5) — 파서·WS 클라이언트·trade_id 갭·LevelTracker 보강.

파서 테스트의 원시 프레임은 2026-08-02 라이브 캡처(scripts/capture_stream.py
--exchange coinbase) 실측 스키마를 그대로 축약한 것 — match side=maker side 검증 포함.
"""

import asyncio
import contextlib
import json
from decimal import Decimal
from types import SimpleNamespace

import aiohttp
import pytest

from order_monitor.ingestion.coinbase import (
    CoinbaseWSClient,
    coinbase_stream_names,
    parse_coinbase_message,
)
from order_monitor.ingestion.events import (
    AggTradeEvent,
    DepthSnapshot,
    DiffDepthEvent,
    NormalizationError,
    Side,
    parse_stream_message,
)
from order_monitor.ingestion.health import (
    DiffListeningGap,
    EpochEnded,
    EpochStarted,
    SessionEpochTracker,
)
from order_monitor.state.level_tracker import LevelTracker

ISO = "2026-08-02T05:04:57.009744Z"
ISO_MS = 1785647097009  # datetime.fromisoformat 기준 검증값

SNAPSHOT = {
    "type": "snapshot",
    "product_id": "BTC-USD",
    "bids": [["63443.93", "0.11271996"], ["60000.00", "120.5"]],
    "asks": [["63443.94", "0.06909624"]],
    "time": ISO,
}
L2UPDATE = {
    "type": "l2update",
    "product_id": "BTC-USD",
    "changes": [
        ["buy", "63387.73", "0.07012982"],
        ["sell", "63451.29", "0.00000000"],
        ["sell", "63451.17", "0.00918735"],
    ],
    "time": ISO,
}
MATCH_MAKER_SELL = {
    "type": "match",
    "trade_id": 1065673843,
    "side": "sell",
    "size": "0.02998440",
    "price": "63443.94",
    "product_id": "BTC-USD",
    "sequence": 133597124978,
    "time": ISO,
}
TICKER = {
    "type": "ticker",
    "sequence": 133597123063,
    "product_id": "BTC-USD",
    "price": "63443.94",
    "best_bid": "63443.93",
    "best_bid_size": "0.64874642",
    "best_ask": "63443.94",
    "best_ask_size": "0.03909624",
    "side": "buy",
    "time": ISO,
}


# ---- 파서 ----


def test_snapshot_parses_to_diff_event():
    stream, event = parse_coinbase_message(SNAPSHOT, 1.5)
    assert stream == "coinbase:BTC-USD@level2"
    assert isinstance(event, DiffDepthEvent)
    assert event.first_update_id == 0 and event.final_update_id == 0
    assert event.bids[1] == (Decimal("60000.00"), Decimal("120.5"))
    assert event.exchange_time_ms == ISO_MS
    assert event.local_monotonic_receive_time == 1.5


def test_l2update_splits_changes_by_side_with_tombstone():
    _, event = parse_coinbase_message(L2UPDATE, 0.0)
    assert isinstance(event, DiffDepthEvent)
    assert event.bids == ((Decimal("63387.73"), Decimal("0.07012982")),)
    assert event.asks == (
        (Decimal("63451.29"), Decimal("0.00000000")),  # 절대 잔량 0 = tombstone
        (Decimal("63451.17"), Decimal("0.00918735")),
    )


def test_match_maker_sell_is_buy_aggressor():
    # 라이브 캡처 검증: best_ask 체결이 side="sell"(maker) — 테이커 매수
    stream, event = parse_coinbase_message(MATCH_MAKER_SELL, 0.0)
    assert stream == "coinbase:BTC-USD@matches"
    assert isinstance(event, AggTradeEvent)
    assert event.aggressor_side is Side.BUY
    assert event.agg_trade_id == 1065673843
    assert event.qty == Decimal("0.02998440")


def test_match_maker_buy_is_sell_aggressor():
    _, event = parse_coinbase_message({**MATCH_MAKER_SELL, "side": "buy"}, 0.0)
    assert event.aggressor_side is Side.SELL


def test_last_match_parses_like_match():
    _, event = parse_coinbase_message({**MATCH_MAKER_SELL, "type": "last_match"}, 0.0)
    assert isinstance(event, AggTradeEvent)


def test_ticker_parses_to_top1_snapshot():
    stream, event = parse_coinbase_message(TICKER, 0.0)
    assert stream == "coinbase:BTC-USD@ticker"
    assert isinstance(event, DepthSnapshot)
    assert event.bids == ((Decimal("63443.93"), Decimal("0.64874642")),)
    assert event.asks == ((Decimal("63443.94"), Decimal("0.03909624")),)
    assert event.last_update_id == 133597123063


def test_heartbeat_and_subscriptions_yield_none():
    for msg_type in ("heartbeat", "subscriptions"):
        _, event = parse_coinbase_message({"type": msg_type, "product_id": "BTC-USD"}, 0.0)
        assert event is None


def test_unknown_type_raises():
    with pytest.raises(NormalizationError, match="unknown message type"):
        parse_coinbase_message({"type": "activate"}, 0.0)


def test_parse_stream_message_delegates_coinbase_streams():
    stream, event = parse_stream_message(
        {"stream": "coinbase:BTC-USD@matches", "data": MATCH_MAKER_SELL}, 0.0
    )
    assert stream == "coinbase:BTC-USD@matches"
    assert isinstance(event, AggTradeEvent)


# ---- SessionEpochTracker trade_id 갭 (PRD §5.5) ----


def cb_tracker():
    ticker, matches, l2 = coinbase_stream_names("BTC-USD")
    return SessionEpochTracker(
        symbol="BTC-USD",
        stale_seconds=30.0,
        trade_stale_seconds=60.0,
        clock=lambda: 0.0,
        streams=(ticker, matches, l2),
        stale_thresholds={l2: 30.0, ticker: 60.0, matches: 60.0},
        check_diff_continuity=False,
        check_trade_continuity=True,
    )


def cb_events(mono=0.0):
    _, snapshot = parse_coinbase_message(SNAPSHOT, mono)
    _, ticker = parse_coinbase_message(TICKER, mono)
    return snapshot, ticker


def match_event(trade_id, mono=0.0):
    _, event = parse_coinbase_message({**MATCH_MAKER_SELL, "trade_id": trade_id}, mono)
    return event


def start_cb_epoch(tracker):
    ticker_s, matches_s, l2_s = coinbase_stream_names("BTC-USD")
    snapshot, ticker = cb_events()
    tracker.on_subscribed()
    tracker.on_event(l2_s, snapshot)
    tracker.on_event(matches_s, match_event(100))
    notices = tracker.on_event(ticker_s, ticker)
    assert any(isinstance(n, EpochStarted) for n in notices)
    return coinbase_stream_names("BTC-USD")


def test_trade_id_gap_ends_epoch_without_diff_gap():
    tracker = cb_tracker()
    _, matches_s, _ = start_cb_epoch(tracker)
    notices = tracker.on_event(matches_s, match_event(101, mono=0.1))
    assert not any(isinstance(n, EpochEnded) for n in notices)  # 연속 — 무사

    notices = tracker.on_event(matches_s, match_event(105, mono=0.2))  # 갭
    ended = [n for n in notices if isinstance(n, EpochEnded)]
    assert len(ended) == 1 and ended[0].reason == "trade_gap"
    # 체결 손실은 diff 청취 공백이 아님 — 레지스트리 unconfirmed 마킹 없음 (§5.5)
    assert not any(isinstance(n, DiffListeningGap) for n in notices)
    # 세 스트림이 모두 fresh라 같은 호출에서 새 epoch 재개 (바이낸스 diff_gap과 동일 의미론)
    assert any(isinstance(n, EpochStarted) for n in notices)


def test_trade_id_baseline_resets_on_disconnect():
    tracker = cb_tracker()
    _, matches_s, _ = start_cb_epoch(tracker)
    tracker.on_disconnected()
    tracker.on_subscribed()
    # 재연결 후 첫 체결은 기준점 없음 — 갭 오발화 금지
    notices = tracker.on_event(matches_s, match_event(9999, mono=1.0))
    assert not any(isinstance(n, EpochEnded) and n.reason == "trade_gap" for n in notices)


# ---- LevelTracker create_on_trade_for_retained (PRD §5.5) ----


def agg(price="60000", qty="0.5"):
    return AggTradeEvent(
        agg_trade_id=1,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=Side.SELL,  # bid 레벨 소진
        exchange_time_ms=0,
        local_monotonic_receive_time=0.0,
    )


def test_create_on_trade_accumulates_for_retained_price():
    # ticker(top-1 스냅샷)가 체결에 후행해도 레지스트리 벽 가격의 누적이 성립
    retained = {(Side.BUY, Decimal("60000"))}
    tracker = LevelTracker(
        retain=lambda side, price: (side, price) in retained,
        create_on_trade_for_retained=True,
    )
    tracker.record_trade(agg())
    entry = tracker.get(Side.BUY, Decimal("60000"))
    assert entry is not None and entry.cum_traded_at_level == Decimal("0.5")
    assert entry.current_size == Decimal(0)  # 표시 잔량은 스냅샷/레지스트리 소관


def test_create_on_trade_ignores_non_retained_price():
    tracker = LevelTracker(retain=lambda side, price: False, create_on_trade_for_retained=True)
    tracker.record_trade(agg())
    assert tracker.get(Side.BUY, Decimal("60000")) is None


def test_default_behavior_unchanged_without_flag():
    # 바이낸스 경로 (기본 False) — 엔트리 없는 체결은 종전대로 무시
    retained = {(Side.BUY, Decimal("60000"))}
    tracker = LevelTracker(retain=lambda side, price: (side, price) in retained)
    tracker.record_trade(agg())
    assert tracker.get(Side.BUY, Decimal("60000")) is None


# ---- CoinbaseWSClient ----


def text(data):
    return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=data)


CLOSED = SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)


class FakeWS:
    def __init__(self, messages):
        self._messages = list(messages)
        self.sent_json = []

    async def send_json(self, payload):
        self.sent_json.append(payload)

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


@pytest.mark.asyncio
async def test_client_subscribes_and_normalizes_frames():
    recorder = Recorder()
    fake_ws = FakeWS(
        [
            text(json.dumps({"type": "subscriptions", "channels": []})),
            text(json.dumps(SNAPSHOT)),
            text(json.dumps(MATCH_MAKER_SELL)),
            text(json.dumps(TICKER)),
            text(json.dumps({"type": "heartbeat", "product_id": "BTC-USD"})),
        ]
    )
    done = asyncio.Event()

    @contextlib.asynccontextmanager
    async def fake_connect(url):
        try:
            yield fake_ws
        finally:
            done.set()

    async def fake_sleep(seconds):
        await asyncio.sleep(0)

    client = CoinbaseWSClient(
        "BTC-USD",
        on_event=recorder.on_event,
        on_connected=recorder.on_connected,
        on_disconnected=recorder.on_disconnected,
        monotonic=lambda: 0.0,
        sleep=fake_sleep,
        connect=fake_connect,
    )
    task = asyncio.create_task(client.run())
    await asyncio.wait_for(done.wait(), timeout=2.0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert fake_ws.sent_json[0]["type"] == "subscribe"
    assert fake_ws.sent_json[0]["product_ids"] == ["BTC-USD"]
    assert recorder.connected == 1 and recorder.disconnected == 1
    # subscriptions·heartbeat는 이벤트 미생성 — 정규화 3건만
    kinds = [type(event).__name__ for _, event in recorder.events]
    assert kinds == ["DiffDepthEvent", "AggTradeEvent", "DepthSnapshot"]
