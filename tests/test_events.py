from decimal import Decimal

import pytest

from order_monitor.ingestion.events import (
    AggTradeEvent,
    DepthSnapshot,
    DiffDepthEvent,
    NormalizationError,
    Side,
    parse_agg_trade,
    parse_depth20,
    parse_diff_depth,
    parse_stream_message,
    stream_names,
)

DEPTH20_PAYLOAD = {
    "lastUpdateId": 160,
    "bids": [["61000.01", "1364.86000000"], ["60999.99", "2.50000000"]],
    "asks": [["61000.02", "0.75000000"]],
}

AGG_TRADE_PAYLOAD = {
    "e": "aggTrade",
    "E": 1751234567890,
    "s": "BTCUSDT",
    "a": 12345,
    "p": "61000.01000000",
    "q": "0.50000000",
    "f": 100,
    "l": 105,
    "T": 1751234567885,
    "m": True,
    "M": True,
}

DIFF_PAYLOAD = {
    "e": "depthUpdate",
    "E": 1751234567900,
    "s": "BTCUSDT",
    "U": 157,
    "u": 160,
    "b": [["61000.00", "1364.86000000"]],
    "a": [["65000.00", "0.00000000"]],
}


def test_stream_names():
    assert stream_names("BTC/USDT") == (
        "btcusdt@depth20@100ms",
        "btcusdt@aggTrade",
        "btcusdt@depth@100ms",
    )


def test_parse_depth20():
    snap = parse_depth20(DEPTH20_PAYLOAD, receive_monotonic=12.5)
    assert snap.last_update_id == 160
    assert snap.bids[0] == (Decimal("61000.01"), Decimal("1364.86000000"))
    assert isinstance(snap.bids[0][0], Decimal)
    assert snap.asks == ((Decimal("61000.02"), Decimal("0.75000000")),)
    assert snap.local_monotonic_receive_time == 12.5


def test_parse_agg_trade_sell_aggressor():
    trade = parse_agg_trade(AGG_TRADE_PAYLOAD, receive_monotonic=1.0)
    assert trade.price == Decimal("61000.01000000")
    assert trade.qty == Decimal("0.50000000")
    assert trade.aggressor_side is Side.SELL  # m=True → 비드 히트
    assert trade.exchange_time_ms == 1751234567885  # T, E 아님
    assert trade.agg_trade_id == 12345


def test_parse_agg_trade_buy_aggressor():
    payload = dict(AGG_TRADE_PAYLOAD, m=False)
    assert parse_agg_trade(payload, 0.0).aggressor_side is Side.BUY


def test_parse_diff_depth():
    event = parse_diff_depth(DIFF_PAYLOAD, receive_monotonic=3.0)
    assert (event.first_update_id, event.final_update_id) == (157, 160)
    assert event.bids == ((Decimal("61000.00"), Decimal("1364.86000000")),)
    assert event.asks[0][1] == Decimal("0")  # tombstone 잔량 0
    assert event.exchange_time_ms == 1751234567900  # E


def test_prices_are_never_float():
    snap = parse_depth20(DEPTH20_PAYLOAD, 0.0)
    trade = parse_agg_trade(AGG_TRADE_PAYLOAD, 0.0)
    diff = parse_diff_depth(DIFF_PAYLOAD, 0.0)
    values = [
        *[v for level in snap.bids + snap.asks for v in level],
        trade.price,
        trade.qty,
        *[v for level in diff.bids + diff.asks for v in level],
    ]
    assert all(type(v) is Decimal for v in values)


def test_decimal_keys_join_across_representations():
    # 세 스트림의 같은 가격이 표기(끝자리 0 개수)가 달라도 dict 키로 조인되어야 한다
    assert hash(Decimal("61000.00000000")) == hash(Decimal("61000.0"))
    assert Decimal("61000.00000000") == Decimal("61000.0")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.pop("bids"),
        lambda p: p.update(bids=[["61000.01"]]),  # qty 누락 쌍
        lambda p: p.update(bids=[[61000.01, "1.0"]]),  # float 가격 거부
        lambda p: p.update(bids=[["abc", "1.0"]]),
        lambda p: p.update(lastUpdateId="160"),
    ],
)
def test_parse_depth20_rejects_malformed(mutate):
    payload = {k: list(v) if isinstance(v, list) else v for k, v in DEPTH20_PAYLOAD.items()}
    mutate(payload)
    with pytest.raises(NormalizationError):
        parse_depth20(payload, 0.0)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.pop("m"),
        lambda p: p.update(m="true"),
        lambda p: p.pop("T"),
        lambda p: p.update(p=61000.01),
    ],
)
def test_parse_agg_trade_rejects_malformed(mutate):
    payload = dict(AGG_TRADE_PAYLOAD)
    mutate(payload)
    with pytest.raises(NormalizationError):
        parse_agg_trade(payload, 0.0)


def test_parse_stream_message_dispatch():
    cases = [
        ("btcusdt@depth20@100ms", DEPTH20_PAYLOAD, DepthSnapshot),
        ("btcusdt@aggTrade", AGG_TRADE_PAYLOAD, AggTradeEvent),
        ("btcusdt@depth@100ms", DIFF_PAYLOAD, DiffDepthEvent),
    ]
    for stream, payload, expected_type in cases:
        name, event = parse_stream_message({"stream": stream, "data": payload}, 7.0)
        assert name == stream
        assert isinstance(event, expected_type)
        assert event.local_monotonic_receive_time == 7.0


def test_parse_stream_message_depth20_not_confused_with_diff():
    # "@depth20@100ms"가 "@depth@100ms" 접미사로 오분류되지 않아야 한다
    _, event = parse_stream_message(
        {"stream": "btcusdt@depth20@100ms", "data": DEPTH20_PAYLOAD}, 0.0
    )
    assert isinstance(event, DepthSnapshot)


def test_parse_stream_message_unknown_stream():
    with pytest.raises(NormalizationError):
        parse_stream_message({"stream": "btcusdt@kline_1m", "data": {}}, 0.0)


def test_parse_stream_message_missing_data():
    with pytest.raises(NormalizationError):
        parse_stream_message({"stream": "btcusdt@aggTrade"}, 0.0)
