from decimal import Decimal

from order_monitor.detectors.contact import (
    ContactEpisodeTracker,
    EpisodeEndReason,
)
from order_monitor.ingestion.events import AggTradeEvent, Side


def trade(price, qty="1.0", aggressor=Side.SELL, mono=0.0, trade_id=1):
    return AggTradeEvent(
        agg_trade_id=trade_id,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=aggressor,
        exchange_time_ms=int(mono * 1000),
        local_monotonic_receive_time=mono,
    )


def make_tracker(persist=2):
    return ContactEpisodeTracker(pierce_persist_snapshots=persist)


def snap(tracker, bid=None, ask=None, t=0.0):
    return tracker.on_depth_snapshot(
        Decimal(bid) if bid is not None else None,
        Decimal(ask) if ask is not None else None,
        t,
    )


class TestEpisodeLifecycle:
    def test_contact_opens_episode_per_side(self):
        tracker = make_tracker()
        ended = snap(tracker, bid="61000", ask="61001", t=1.0)
        assert ended == []
        active = tracker.active()
        assert (Side.BUY, Decimal("61000")) in active
        assert (Side.SELL, Decimal("61001")) in active
        assert active[(Side.BUY, Decimal("61000"))].started_receive_time == 1.0

    def test_rebound_ends_bid_episode(self):
        tracker = make_tracker()
        snap(tracker, bid="61000", t=1.0)
        ended = snap(tracker, bid="61010", t=2.0)
        rebounds = [e for e in ended if e.episode.price == Decimal("61000")]
        assert len(rebounds) == 1
        assert rebounds[0].reason is EpisodeEndReason.REBOUND
        assert rebounds[0].ended_receive_time == 2.0
        # 새 접촉 레벨은 새 episode
        assert (Side.BUY, Decimal("61010")) in tracker.active()

    def test_rebound_ends_ask_episode_symmetric(self):
        tracker = make_tracker()
        snap(tracker, ask="61001", t=1.0)
        ended = snap(tracker, ask="60995", t=2.0)  # ask가 아래로 이탈 = 반등(강세)
        assert [e.reason for e in ended if e.episode.price == Decimal("61001")] == [
            EpisodeEndReason.REBOUND
        ]

    def test_same_level_sequential_episodes(self):
        tracker = make_tracker()
        snap(tracker, bid="61000", t=1.0)
        snap(tracker, bid="61010", t=2.0)  # 1차 종료(반등)
        snap(tracker, bid="61000", t=3.0)  # 2차 접촉
        episode = tracker.active()[(Side.BUY, Decimal("61000"))]
        assert episode.started_receive_time == 3.0  # 새 episode


class TestPierceByTradePrice:
    def test_sell_trade_below_bid_level_pierces_immediately(self):
        tracker = make_tracker()
        snap(tracker, bid="61000", t=1.0)
        ended = tracker.on_trade(trade("60999", aggressor=Side.SELL, mono=1.5))
        assert len(ended) == 1
        assert ended[0].reason is EpisodeEndReason.PIERCED
        assert ended[0].ended_receive_time == 1.5
        assert tracker.active() == {}

    def test_buy_trade_above_ask_level_pierces_immediately(self):
        tracker = make_tracker()
        snap(tracker, ask="61001", t=1.0)
        ended = tracker.on_trade(trade("61002", aggressor=Side.BUY, mono=1.5))
        assert [e.reason for e in ended] == [EpisodeEndReason.PIERCED]

    def test_trade_at_level_price_is_absorption_not_pierce(self):
        tracker = make_tracker()
        snap(tracker, bid="61000", t=1.0)
        assert tracker.on_trade(trade("61000", aggressor=Side.SELL, mono=1.5)) == []
        assert (Side.BUY, Decimal("61000")) in tracker.active()

    def test_opposite_side_trade_does_not_pierce(self):
        # buy-aggressor 체결은 bid 레벨과 무관 (같은 side 기준 — PRD §8 D3 v1.2)
        tracker = make_tracker()
        snap(tracker, bid="61000", t=1.0)
        assert tracker.on_trade(trade("60999", aggressor=Side.BUY, mono=1.5)) == []
        assert (Side.BUY, Decimal("61000")) in tracker.active()


class TestPierceByBestPersistence:
    def test_best_below_for_persist_snapshots_pierces(self):
        tracker = make_tracker(persist=2)
        snap(tracker, bid="61000", t=1.0)
        assert snap(tracker, bid="60990", t=1.1) == []  # streak 1 — 아직 비확정
        ended = snap(tracker, bid="60990", t=1.2)  # streak 2 — 관통 확정
        pierced = [e for e in ended if e.episode.price == Decimal("61000")]
        assert [e.reason for e in pierced] == [EpisodeEndReason.PIERCED]

    def test_flicker_recovery_resets_streak(self):
        # 아이스버그 리필 사이 잔량 순간 0 플리커는 비관통 (PRD §8 D3 v1.2)
        tracker = make_tracker(persist=2)
        snap(tracker, bid="61000", t=1.0)
        snap(tracker, bid="60990", t=1.1)  # streak 1
        snap(tracker, bid="61000", t=1.2)  # 복귀 — streak 리셋
        ended = snap(tracker, bid="60990", t=1.3)  # streak 1부터 다시
        assert [e for e in ended if e.episode.price == Decimal("61000")] == []
        assert (Side.BUY, Decimal("61000")) in tracker.active()

    def test_deeper_contact_opens_episode_while_streak_counts(self):
        tracker = make_tracker(persist=3)
        snap(tracker, bid="61000", t=1.0)
        snap(tracker, bid="60990", t=1.1)
        # 61000 streak 진행 중에도 60990 접촉 episode는 열린다
        assert (Side.BUY, Decimal("60990")) in tracker.active()
        assert (Side.BUY, Decimal("61000")) in tracker.active()


class TestExternalEndAndReset:
    def test_end_episode_on_removal(self):
        tracker = make_tracker()
        snap(tracker, bid="61000", t=1.0)
        end = tracker.end_episode(
            Side.BUY, Decimal("61000"), EpisodeEndReason.REMOVED, receive_time=2.0
        )
        assert end is not None
        assert end.reason is EpisodeEndReason.REMOVED
        assert tracker.active() == {}

    def test_end_episode_missing_returns_none(self):
        tracker = make_tracker()
        assert (
            tracker.end_episode(Side.BUY, Decimal("61000"), EpisodeEndReason.REMOVED, 2.0) is None
        )

    def test_reset_discards_all(self):
        tracker = make_tracker()
        snap(tracker, bid="61000", ask="61001", t=1.0)
        tracker.reset()
        assert tracker.active() == {}

    def test_none_best_leaves_episodes_untouched(self):
        tracker = make_tracker()
        snap(tracker, bid="61000", t=1.0)
        assert snap(tracker, bid=None, ask="61001", t=2.0) == []
        assert (Side.BUY, Decimal("61000")) in tracker.active()
