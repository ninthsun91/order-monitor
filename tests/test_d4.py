from decimal import Decimal

from order_monitor.detectors.contact import ContactEpisode, EpisodeEnd, EpisodeEndReason
from order_monitor.detectors.d4 import D4Detector
from order_monitor.ingestion.events import AggTradeEvent, DepthSnapshot, Side


def snapshot(bids=(), asks=(), mono=0.0):
    return DepthSnapshot(
        last_update_id=1,
        bids=tuple((Decimal(p), Decimal(q)) for p, q in bids),
        asks=tuple((Decimal(p), Decimal(q)) for p, q in asks),
        local_monotonic_receive_time=mono,
    )


def trade(price, qty="10", aggressor=Side.SELL, mono=0.0, trade_id=1):
    return AggTradeEvent(
        agg_trade_id=trade_id,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=aggressor,
        exchange_time_ms=int(mono * 1000),
        local_monotonic_receive_time=mono,
    )


def episodes(*keys):
    return {
        (side, Decimal(price)): ContactEpisode(
            side=side, price=Decimal(price), started_receive_time=0.0
        )
        for side, price in keys
    }


def make_detector(margin="20", min_trades=5, window_ms=500):
    return D4Detector(
        iceberg_margin=Decimal(margin),
        iceberg_min_trades=min_trades,
        refill_window_ms=window_ms,
    )


BID = (Side.BUY, "61000")


def run_refill_cycles(d4, cycles, *, start=0.0, step=0.2, qty="100"):
    """체결 → 소진 스냅샷 → 회복 스냅샷 사이클 반복 (체결 근접 리필 합성)."""
    active = episodes(BID)
    events = []
    t = start
    events += d4.on_depth_snapshot(snapshot(bids=[("61000", qty)], mono=t), active)
    for i in range(cycles):
        d4.on_trade(trade("61000", "10", aggressor=Side.SELL, mono=t + 0.01, trade_id=i + 1))
        events += d4.on_depth_snapshot(
            snapshot(bids=[("61000", str(Decimal(qty) - 10))], mono=t + 0.05), active
        )
        events += d4.on_depth_snapshot(snapshot(bids=[("61000", qty)], mono=t + 0.1), active)
        t += step
    return events


class TestRefillRecognition:
    def test_recovery_right_after_trade_counts_and_fires(self):
        # 사이클당 리필 10 → 5사이클 = refill 50 ≥ 20, 쌍 5 ≥ 5
        events = run_refill_cycles(make_detector(), 5)
        assert len(events) == 1
        assert events[0].refill_added == Decimal("50")
        assert events[0].refill_trade_count == 5
        assert events[0].side is Side.BUY

    def test_unrelated_addition_outside_window_excluded(self):
        # 체결 후 시간이 지난 무관한 추가는 비인정 (PRD §8 D4 v1.2 명시 케이스)
        d4 = make_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_trade(trade("61000", "10", mono=0.01, trade_id=1))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.05), active)
        # 60초 뒤 신규 표시 주문 80 — 500ms 창 밖
        events = d4.on_depth_snapshot(snapshot(bids=[("61000", "170")], mono=60.0), active)
        assert events == []

    def test_positive_delta_without_any_trade_excluded(self):
        d4 = make_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        events = d4.on_depth_snapshot(snapshot(bids=[("61000", "150")], mono=0.1), active)
        assert events == []

    def test_trade_arriving_before_contact_snapshot_still_pairs(self):
        # 스트림 순서 역전: 체결이 접촉 스냅샷보다 먼저 도착해도 근접 쌍 성립
        d4 = make_detector(margin="5", min_trades=1)
        d4.on_trade(trade("61000", "10", mono=0.0, trade_id=1))
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.1), active)
        events = d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.2), active)
        assert len(events) == 1
        assert events[0].refill_added == Decimal("10")

    def test_negative_delta_ignored(self):
        d4 = make_detector(margin="5", min_trades=1)
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_trade(trade("61000", "10", mono=0.01, trade_id=1))
        events = d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.05), active)
        assert events == []


class TestFireConditions:
    def test_margin_not_met_silent(self):
        # 쌍 수는 충족해도 리필량 미달이면 침묵
        events = run_refill_cycles(make_detector(margin="60"), 5)  # refill 50 < 60
        assert events == []

    def test_min_trades_not_met_silent(self):
        events = run_refill_cycles(make_detector(min_trades=5), 4)  # 쌍 4 < 5, refill 40 ≥ 20
        assert events == []

    def test_fires_once_per_episode_latch(self):
        events = run_refill_cycles(make_detector(), 8)  # 조건 충족 후 계속 리필
        assert len(events) == 1

    def test_pair_count_is_aggtrade_message_count(self):
        # 하나의 회복 델타에 창 내 체결 2건 → 쌍 2 (aggTrade 메시지 수 기준)
        d4 = make_detector(margin="5", min_trades=2)
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_trade(trade("61000", "5", mono=0.01, trade_id=1))
        d4.on_trade(trade("61000", "5", mono=0.02, trade_id=2))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.05), active)
        events = d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.1), active)
        assert len(events) == 1
        assert events[0].refill_trade_count == 2

    def test_same_trade_credited_once(self):
        # 같은 체결이 여러 회복 델타와 겹쳐도 쌍 카운트는 1회
        d4 = make_detector(margin="5", min_trades=2)
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_trade(trade("61000", "5", mono=0.01, trade_id=7))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "102")], mono=0.1), active)
        events = d4.on_depth_snapshot(snapshot(bids=[("61000", "104")], mono=0.2), active)
        assert events == []  # refill 4 < 5이기도 하지만 쌍도 1 < 2

    def test_flicker_to_zero_then_refill_counts(self):
        # 잔량 순간 0(스냅샷 부재) 후 회복 — 아이스버그 전형 패턴
        d4 = make_detector(margin="5", min_trades=1)
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "10")], mono=0.0), active)
        d4.on_trade(trade("61000", "10", mono=0.01, trade_id=1))
        d4.on_depth_snapshot(snapshot(bids=[("60999", "1")], mono=0.05), active)  # 61000 부재=0
        events = d4.on_depth_snapshot(snapshot(bids=[("61000", "10")], mono=0.1), active)
        assert len(events) == 1
        assert events[0].refill_added == Decimal("10")


class TestLifetimeRefillAbove:
    def test_accumulates_across_episode_boundaries(self):
        # 1차 episode에서 리필 10, 종료 후 2차 episode에서 +15 — lifetime은 합산 유지
        d4 = make_detector(margin="5", min_trades=1)
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_trade(trade("61000", "10", mono=0.01, trade_id=1))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "110")], mono=0.05), active)
        d4.on_episode_end(
            EpisodeEnd(
                episode=ContactEpisode(
                    side=Side.BUY, price=Decimal("61000"), started_receive_time=0.0
                ),
                reason=EpisodeEndReason.REBOUND,
                ended_receive_time=1.0,
            )
        )
        d4.on_depth_snapshot(snapshot(bids=[("61000", "110")], mono=10.0), active)  # 2차 기준선
        d4.on_trade(trade("61000", "15", mono=10.01, trade_id=2))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "125")], mono=10.05), active)
        assert d4.sum_lifetime_refill_above(Side.BUY, Decimal("60999"), Decimal("61000")) == Decimal(
            "25"
        )

    def test_range_bounds_for_buy_are_exclusive_low_inclusive_high(self):
        d4 = make_detector(margin="5", min_trades=1)
        d4._lifetime_refill = {
            (Side.BUY, Decimal("61000")): Decimal("10"),
            (Side.BUY, Decimal("61050")): Decimal("20"),
            (Side.BUY, Decimal("61100")): Decimal("30"),  # 범위 밖(현재가 초과)
        }
        total = d4.sum_lifetime_refill_above(Side.BUY, Decimal("61000"), Decimal("61050"))
        assert total == Decimal("20")  # 61000 제외(경계 미포함), 61050 포함, 61100 제외

    def test_range_bounds_for_sell_are_symmetric(self):
        d4 = make_detector(margin="5", min_trades=1)
        d4._lifetime_refill = {
            (Side.SELL, Decimal("62000")): Decimal("10"),
            (Side.SELL, Decimal("61950")): Decimal("20"),
            (Side.SELL, Decimal("61900")): Decimal("30"),  # 범위 밖(현재가 미만)
        }
        total = d4.sum_lifetime_refill_above(Side.SELL, Decimal("62000"), Decimal("61950"))
        assert total == Decimal("20")  # 62000 제외, 61950 포함, 61900 제외

    def test_returns_zero_when_current_price_unknown(self):
        d4 = make_detector(margin="5", min_trades=1)
        d4._lifetime_refill = {(Side.BUY, Decimal("61050")): Decimal("20")}
        assert d4.sum_lifetime_refill_above(Side.BUY, Decimal("61000"), None) == Decimal(0)

    def test_ignores_other_side(self):
        d4 = make_detector(margin="5", min_trades=1)
        d4._lifetime_refill = {(Side.SELL, Decimal("61050")): Decimal("20")}
        assert d4.sum_lifetime_refill_above(Side.BUY, Decimal("61000"), Decimal("61100")) == Decimal(0)

    def test_epoch_reset_clears_lifetime_refill(self):
        d4 = make_detector(margin="5", min_trades=1)
        d4._lifetime_refill = {(Side.BUY, Decimal("61050")): Decimal("20")}
        d4.reset()
        assert d4.sum_lifetime_refill_above(Side.BUY, Decimal("61000"), Decimal("61100")) == Decimal(0)


class TestResets:
    def test_episode_end_resets_accumulation(self):
        d4 = make_detector()
        run_refill_cycles(d4, 3)  # refill 30, 쌍 3 — 미발화 누적
        d4.on_episode_end(
            EpisodeEnd(
                episode=ContactEpisode(
                    side=Side.BUY, price=Decimal("61000"), started_receive_time=0.0
                ),
                reason=EpisodeEndReason.REBOUND,
                ended_receive_time=1.0,
            )
        )
        # 새 episode에서 2사이클만 — 이전 누적이 살아있다면 refill 50으로 발화했을 것
        events = run_refill_cycles(d4, 2, start=10.0)
        assert events == []

    def test_epoch_reset_clears_accumulation(self):
        d4 = make_detector()
        run_refill_cycles(d4, 3)
        d4.reset()
        events = run_refill_cycles(d4, 2, start=10.0)
        assert events == []

    def test_trade_buffer_time_bounded(self):
        d4 = make_detector(window_ms=500)
        for i in range(100):
            d4.on_trade(trade(str(61000 + i), "1", mono=i * 1.0, trade_id=i))
        # 500ms 창 밖 체결은 전역 프루닝 — 마지막 1건만 잔존
        assert len(d4._trades) == 1
        assert len(d4._trade_log) == 1
