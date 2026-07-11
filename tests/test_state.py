from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, DepthSnapshot, Side
from order_monitor.state.level_tracker import LevelTracker
from order_monitor.state.order_book import OrderBook
from order_monitor.state.trade_window import TradeWindow


def snapshot(bids, asks, update_id=1, mono=0.0):
    return DepthSnapshot(
        last_update_id=update_id,
        bids=tuple((Decimal(p), Decimal(q)) for p, q in bids),
        asks=tuple((Decimal(p), Decimal(q)) for p, q in asks),
        local_monotonic_receive_time=mono,
    )


def trade(price, qty, aggressor=Side.SELL, t_ms=0, trade_id=1):
    return AggTradeEvent(
        agg_trade_id=trade_id,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=aggressor,
        exchange_time_ms=t_ms,
        local_monotonic_receive_time=0.0,
    )


class TestOrderBook:
    def test_wholesale_replace(self):
        book = OrderBook()
        book.apply_snapshot(snapshot([("61000", "1.0")], [("61001", "2.0")], update_id=10))
        book.apply_snapshot(snapshot([("60990", "3.0")], [("60991", "4.0")], update_id=11, mono=5.0))
        assert book.bids == {Decimal("60990"): Decimal("3.0")}  # 이전 레벨 잔존 없음
        assert book.asks == {Decimal("60991"): Decimal("4.0")}
        assert book.last_update_id == 11
        assert book.last_receive_monotonic == 5.0

    def test_best_prices(self):
        book = OrderBook()
        assert book.best_bid is None and book.best_ask is None
        book.apply_snapshot(
            snapshot([("60999", "1"), ("61000", "1")], [("61002", "1"), ("61001", "1")])
        )
        assert book.best_bid == Decimal("61000")
        assert book.best_ask == Decimal("61001")


class TestTradeWindow:
    def test_expiry_by_exchange_time(self):
        window = TradeWindow(window_seconds=60)
        window.add(trade("61000", "1.0", t_ms=0))
        window.add(trade("61000", "2.0", t_ms=30_000))
        window.add(trade("61000", "4.0", t_ms=61_000))  # 첫 체결은 창 밖으로
        assert len(window) == 2
        assert window.sum_qty(Side.SELL) == Decimal("6.0")

    def test_side_separated_sums(self):
        window = TradeWindow(window_seconds=60)
        window.add(trade("61000", "1.5", aggressor=Side.SELL, t_ms=0))
        window.add(trade("61000", "2.5", aggressor=Side.BUY, t_ms=1))
        assert window.sum_qty(Side.SELL) == Decimal("1.5")
        assert window.sum_qty(Side.BUY) == Decimal("2.5")

    def test_memory_bounded_over_long_run(self):
        # 시간 상한 동작 = 메모리 바운드 (DEVELOPMENT_PLAN M1 체크박스)
        window = TradeWindow(window_seconds=60)
        for i in range(10_000):
            window.add(trade("61000", "0.1", t_ms=i * 100))  # 100ms 간격 1000초분
        assert len(window) <= 601  # 60s / 100ms + 경계 1건

    def test_out_of_order_timestamp_does_not_rewind_window(self):
        window = TradeWindow(window_seconds=60)
        window.add(trade("61000", "1.0", t_ms=100_000))
        window.add(trade("61000", "1.0", t_ms=99_000))  # 순서 역전
        assert len(window) == 2

    def test_clear(self):
        window = TradeWindow(window_seconds=60)
        window.add(trade("61000", "1.0", t_ms=0))
        window.clear()
        assert len(window) == 0
        assert window.sum_qty(Side.SELL) == Decimal(0)


class TestLevelTracker:
    def test_tracks_top20_sizes(self):
        tracker = LevelTracker()
        tracker.apply_snapshot(snapshot([("61000", "5.0")], [("61001", "7.0")]))
        assert tracker.get(Side.BUY, Decimal("61000")).current_size == Decimal("5.0")
        assert tracker.get(Side.SELL, Decimal("61001")).current_size == Decimal("7.0")

    def test_levels_out_of_window_are_dropped(self):
        tracker = LevelTracker()
        tracker.apply_snapshot(snapshot([("61000", "5.0")], []))
        tracker.apply_snapshot(snapshot([("60990", "1.0")], []))
        assert tracker.get(Side.BUY, Decimal("61000")) is None
        assert len(tracker) == 1

    def test_trade_attribution_to_matching_side(self):
        tracker = LevelTracker()
        tracker.apply_snapshot(snapshot([("61000", "5.0")], [("61001", "5.0")]))
        # sell-aggressor는 bid 레벨 소진, buy-aggressor는 ask 레벨 소진
        tracker.record_trade(trade("61000", "1.0", aggressor=Side.SELL))
        tracker.record_trade(trade("61001", "2.0", aggressor=Side.BUY))
        tracker.record_trade(trade("61000", "9.9", aggressor=Side.BUY))  # ask에 61000 없음 → 무시
        assert tracker.get(Side.BUY, Decimal("61000")).cum_traded_at_level == Decimal("1.0")
        assert tracker.get(Side.SELL, Decimal("61001")).cum_traded_at_level == Decimal("2.0")

    def test_cum_traded_survives_size_updates(self):
        tracker = LevelTracker()
        tracker.apply_snapshot(snapshot([("61000", "5.0")], []))
        tracker.record_trade(trade("61000", "1.0", aggressor=Side.SELL))
        tracker.apply_snapshot(snapshot([("61000", "4.0")], []))
        entry = tracker.get(Side.BUY, Decimal("61000"))
        assert entry.current_size == Decimal("4.0")
        assert entry.cum_traded_at_level == Decimal("1.0")
