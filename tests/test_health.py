from decimal import Decimal

from order_monitor.ingestion.events import (
    AggTradeEvent,
    DepthSnapshot,
    DiffDepthEvent,
    Side,
    stream_names,
)
from order_monitor.ingestion.health import (
    DiffListeningGap,
    EpochEnded,
    EpochStarted,
    SessionEpochTracker,
    StreamStale,
)

DEPTH, AGG, DIFF = stream_names("BTC/USDT")


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


def tracker(clock=None):
    return SessionEpochTracker(
        symbol="BTC/USDT",
        stale_seconds=30.0,
        trade_stale_seconds=60.0,
        clock=clock or FakeClock(),
    )


def depth_event(mono=0.0):
    return DepthSnapshot(
        last_update_id=1, bids=(), asks=(), local_monotonic_receive_time=mono
    )


def agg_event(mono=0.0):
    return AggTradeEvent(
        agg_trade_id=1,
        price=Decimal("61000"),
        qty=Decimal("1"),
        aggressor_side=Side.SELL,
        exchange_time_ms=0,
        local_monotonic_receive_time=mono,
    )


def diff_event(first_id, final_id, mono=0.0):
    return DiffDepthEvent(
        first_update_id=first_id,
        final_update_id=final_id,
        bids=(),
        asks=(),
        exchange_time_ms=0,
        local_monotonic_receive_time=mono,
    )


def start_epoch(t, mono=0.0):
    """구독 + 세 스트림 수신으로 epoch을 활성화하는 헬퍼."""
    t.on_subscribed()
    t.on_event(DEPTH, depth_event(mono))
    t.on_event(AGG, agg_event(mono))
    return t.on_event(DIFF, diff_event(100, 110, mono))


class TestEpochStart:
    def test_starts_only_after_all_streams_and_subscribe(self):
        t = tracker()
        assert t.on_subscribed() == []
        assert t.on_event(DEPTH, depth_event()) == []
        assert t.on_event(AGG, agg_event()) == []
        notices = t.on_event(DIFF, diff_event(100, 110))
        assert notices == [EpochStarted(epoch_id=1)]
        assert t.epoch_active

    def test_no_start_without_subscribe_confirmation(self):
        t = tracker()
        t.on_event(DEPTH, depth_event())
        t.on_event(AGG, agg_event())
        t.on_event(DIFF, diff_event(100, 110))
        assert not t.epoch_active
        assert t.on_subscribed() == [EpochStarted(epoch_id=1)]


class TestDiffContinuity:
    def test_consecutive_updates_no_gap(self):
        t = tracker()
        start_epoch(t)
        notices = t.on_event(DIFF, diff_event(111, 120))
        assert notices == []
        assert t.epoch_active

    def test_gap_ends_epoch_and_flags_listening_gap(self):
        t = tracker()
        start_epoch(t)
        notices = t.on_event(DIFF, diff_event(115, 120))  # 111이어야 함 → 갭
        assert EpochEnded(epoch_id=1, reason="diff_gap") in notices
        assert DiffListeningGap(reason="u_gap") in notices
        # 세 스트림이 모두 healthy하므로 즉시 새 epoch (상태 적재는 끊긴 적 없음)
        assert EpochStarted(epoch_id=2) in notices

    def test_no_baseline_after_reconnect(self):
        t = tracker()
        start_epoch(t)
        t.on_disconnected()
        t.on_subscribed()
        t.on_event(DEPTH, depth_event())
        t.on_event(AGG, agg_event())
        # 재연결 후 첫 diff는 U가 얼마든 갭 아님 (공백은 disconnect가 처리)
        notices = t.on_event(DIFF, diff_event(999_999, 1_000_000))
        assert notices == [EpochStarted(epoch_id=2)]


class TestDisconnect:
    def test_disconnect_ends_epoch_with_listening_gap(self):
        t = tracker()
        start_epoch(t)
        notices = t.on_disconnected()
        assert notices == [
            EpochEnded(epoch_id=1, reason="disconnect"),
            DiffListeningGap(reason="disconnect"),
        ]
        assert not t.epoch_active

    def test_disconnect_while_inactive_still_flags_gap(self):
        t = tracker()
        notices = t.on_disconnected()
        assert notices == [DiffListeningGap(reason="disconnect")]


class TestStaleness:
    def test_stale_stream_ends_epoch(self):
        clock = FakeClock()
        t = tracker(clock)
        start_epoch(t, mono=0.0)
        clock.now = 31.0  # depth·diff 임계(30s) 초과, aggTrade(60s)는 미달
        notices = t.check_staleness()
        stale_streams = {n.stream for n in notices if isinstance(n, StreamStale)}
        assert stale_streams == {DEPTH, DIFF}
        assert EpochEnded(epoch_id=1, reason=f"stale:{DEPTH}") in notices
        assert DiffListeningGap(reason="stale") in notices
        assert not t.epoch_active

    def test_agg_trade_uses_looser_threshold(self):
        clock = FakeClock()
        t = tracker(clock)
        start_epoch(t, mono=0.0)
        t.on_event(DEPTH, depth_event(mono=45.0))
        t.on_event(DIFF, diff_event(111, 120, mono=45.0))
        clock.now = 45.0
        assert t.check_staleness() == []  # aggTrade 침묵 45s < 60s
        clock.now = 61.0
        t.on_event(DEPTH, depth_event(mono=61.0))
        t.on_event(DIFF, diff_event(121, 130, mono=61.0))
        notices = t.check_staleness()
        assert notices == [
            StreamStale(stream=AGG, silent_seconds=61.0),
            EpochEnded(epoch_id=1, reason=f"stale:{AGG}"),
        ]  # aggTrade만의 공백 — DiffListeningGap 없음 (§12.1)

    def test_stale_transition_reported_once(self):
        clock = FakeClock()
        t = tracker(clock)
        start_epoch(t, mono=0.0)
        clock.now = 31.0
        t.check_staleness()
        assert t.check_staleness() == []  # 같은 stale 상태 반복 통지 없음

    def test_recovery_restarts_epoch(self):
        clock = FakeClock()
        t = tracker(clock)
        start_epoch(t, mono=0.0)
        clock.now = 31.0
        t.check_staleness()
        # depth·diff 수신 재개 → stale 해제 → 새 epoch
        t.on_event(DEPTH, depth_event(mono=31.5))
        notices = t.on_event(DIFF, diff_event(111, 120, mono=31.5))
        assert notices == [EpochStarted(epoch_id=2)]

    def test_diff_stale_while_epoch_inactive_still_flags_gap(self):
        clock = FakeClock()
        t = tracker(clock)
        start_epoch(t, mono=0.0)
        # aggTrade staleness로 epoch 먼저 종료
        t.on_event(DEPTH, depth_event(mono=61.0))
        t.on_event(DIFF, diff_event(111, 120, mono=61.0))
        clock.now = 61.5
        t.check_staleness()
        assert not t.epoch_active
        # 이후 diff까지 침묵 → epoch은 이미 꺼져 있어도 레지스트리 마킹 신호는 발생
        clock.now = 92.0
        notices = t.check_staleness()
        assert DiffListeningGap(reason="stale") in notices


class TestEpochIds:
    def test_epoch_ids_increment_across_reconnects(self):
        t = tracker()
        start_epoch(t)
        t.on_disconnected()
        t.on_subscribed()
        t.on_event(DEPTH, depth_event())
        t.on_event(AGG, agg_event())
        notices = t.on_event(DIFF, diff_event(200, 210))
        assert notices == [EpochStarted(epoch_id=2)]


# ---- v1.16 거래소별 구성 주입 (PRD §5.5) ----

CB_TICKER = "coinbase:BTC-USD@ticker"
CB_MATCHES = "coinbase:BTC-USD@matches"
CB_L2 = "coinbase:BTC-USD@level2"


def coinbase_tracker(clock=None):
    # ticker(depth류)는 체결 주도 push — trade_stale_seconds 적용 (§5.5)
    return SessionEpochTracker(
        symbol="BTC-USD",
        stale_seconds=30.0,
        trade_stale_seconds=60.0,
        clock=clock or FakeClock(),
        streams=(CB_TICKER, CB_MATCHES, CB_L2),
        stale_thresholds={CB_L2: 30.0, CB_TICKER: 60.0, CB_MATCHES: 60.0},
        check_diff_continuity=False,
    )


def test_custom_streams_epoch_starts_after_all_three():
    t = coinbase_tracker()
    t.on_subscribed()
    assert t.on_event(CB_L2, diff_event(1, 1, mono=0.1)) == []
    assert t.on_event(CB_MATCHES, agg_event(mono=0.2)) == []
    notices = t.on_event(CB_TICKER, depth_event(mono=0.3))
    assert any(isinstance(n, EpochStarted) for n in notices)


def test_diff_continuity_check_disabled_ignores_gaps():
    # Coinbase l2는 어댑터 로컬 카운터라 U/u 갭 검사 무의미 — 비활성 확인
    t = coinbase_tracker()
    t.on_subscribed()
    t.on_event(CB_L2, diff_event(1, 1, mono=0.1))
    t.on_event(CB_MATCHES, agg_event(mono=0.2))
    t.on_event(CB_TICKER, depth_event(mono=0.3))
    assert t.epoch_active
    notices = t.on_event(CB_L2, diff_event(100, 100, mono=0.4))  # 갭이지만 무시
    assert not any(isinstance(n, EpochEnded) for n in notices)
    assert t.epoch_active


def test_custom_stale_thresholds_applied_per_stream():
    clock = FakeClock()
    t = coinbase_tracker(clock)
    t.on_subscribed()
    t.on_event(CB_L2, diff_event(1, 1, mono=0.0))
    t.on_event(CB_MATCHES, agg_event(mono=0.0))
    t.on_event(CB_TICKER, depth_event(mono=0.0))
    clock.now = 45.0  # l2(30s) 초과, ticker/matches(60s) 미만
    notices = t.check_staleness()
    stale = [n for n in notices if isinstance(n, StreamStale)]
    assert [n.stream for n in stale] == [CB_L2]
    assert any(isinstance(n, DiffListeningGap) for n in notices)  # l2 = diff류
