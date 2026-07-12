"""D2 볼륨 버스트 (PRD §8 D2) — 방향 분리 집계 + BURST_COOLDOWN."""

from decimal import Decimal

from order_monitor.detectors.d2 import D2Burst, D2Detector
from order_monitor.ingestion.events import AggTradeEvent, Side
from order_monitor.state.trade_window import TradeWindow


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


def trade(qty, side=Side.BUY, t_ms=0):
    return AggTradeEvent(
        agg_trade_id=1,
        price=Decimal("61000"),
        qty=Decimal(qty),
        aggressor_side=side,
        exchange_time_ms=t_ms,
        local_monotonic_receive_time=0.0,
    )


def make_detector(clock):
    return D2Detector(vol_threshold=Decimal(100), cooldown_seconds=120.0, monotonic=clock)


def feed(detector, window, event):
    window.add(event)
    return detector.on_trade(event, window)


def test_fires_when_window_sum_reaches_threshold():
    detector = make_detector(FakeClock())
    window = TradeWindow(60.0)
    assert feed(detector, window, trade("60", t_ms=0)) is None
    event = feed(detector, window, trade("40", t_ms=1000))
    assert event == D2Burst(aggressor_side=Side.BUY, sum_qty=Decimal(100))


def test_direction_separated_aggregation():
    detector = make_detector(FakeClock())
    window = TradeWindow(60.0)
    assert feed(detector, window, trade("60", side=Side.BUY, t_ms=0)) is None
    # 반대 방향 체결은 매수 합계에 안 섞임 — 매도 합계 60으로는 미발화
    assert feed(detector, window, trade("60", side=Side.SELL, t_ms=1000)) is None
    # 각 방향은 독립적으로 발화
    assert feed(detector, window, trade("50", side=Side.SELL, t_ms=2000)).aggressor_side is Side.SELL
    assert feed(detector, window, trade("50", side=Side.BUY, t_ms=3000)).aggressor_side is Side.BUY


def test_cooldown_suppresses_and_expires():
    clock = FakeClock()
    detector = make_detector(clock)
    window = TradeWindow(600.0)  # 창 만료가 끼어들지 않게 길게
    assert feed(detector, window, trade("100", t_ms=0)) is not None
    clock.now = 119.0
    assert feed(detector, window, trade("10", t_ms=1000)) is None
    clock.now = 120.0
    assert feed(detector, window, trade("10", t_ms=2000)) is not None


def test_cooldown_is_per_side():
    clock = FakeClock()
    detector = make_detector(clock)
    window = TradeWindow(60.0)
    assert feed(detector, window, trade("100", side=Side.BUY, t_ms=0)) is not None
    # 매수 쿨다운 중이어도 매도는 발화
    assert feed(detector, window, trade("100", side=Side.SELL, t_ms=1000)) is not None
