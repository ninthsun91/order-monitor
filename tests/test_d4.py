"""D4 레벨 흡수 방어 (PRD §8 D4 v1.12) — 스트릭 생애 누적·가시/은닉 리필·배수 판정·
래치/진행/종결·1틱 이월 보정·관측 기록."""

import logging
from decimal import Decimal

from order_monitor.detectors.contact import ContactEpisode, EpisodeEnd, EpisodeEndReason
from order_monitor.detectors.d4 import D4Defense, D4DefenseKind, D4Detector
from order_monitor.ingestion.events import AggTradeEvent, DepthSnapshot, Side
from order_monitor.state.wall_registry import RemovalReason, Wall, WallRemoval


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


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


def wall(price="61000", side=Side.BUY, qty="20"):
    return Wall(
        price=Decimal(price),
        side=side,
        last_qty=Decimal(qty),
        peak_qty=Decimal(qty),
        first_seen_at=0.0,
        first_seen_above_threshold=None,
        last_seen_at=0.0,
    )


def removal(price="61000", side=Side.BUY, reason=RemovalReason.TOMBSTONE):
    return WallRemoval(wall=wall(price=price, side=side, qty="0"), reason=reason)


def make_detector(multiple="2.0", step="0.5", min_events=5, window_ms=500, clock=None):
    clock = clock or FakeClock()
    return D4Detector(
        absorb_multiple=Decimal(multiple),
        absorb_progress_step=Decimal(step),
        absorb_min_events=min_events,
        refill_window_ms=window_ms,
        clock=clock,
        monotonic=clock,
    )


BID = (Side.BUY, "61000")
KEY = (Side.BUY, Decimal("61000"))


def registered_detector(qty="20", **kwargs):
    """R = 20 기본 — 배수 2.0이면 absorbed 40에서 발화 경계."""
    d4 = make_detector(**kwargs)
    d4.on_wall_registered(wall(qty=qty))
    return d4


def run_visible_cycles(d4, cycles, *, start=0.0, step=0.2, qty="100", trade_qty="10"):
    """체결 → 소진 스냅샷 → 회복 스냅샷 사이클 — 가시 리필(①) 합성."""
    active = episodes(BID)
    events = []
    t = start
    events += d4.on_depth_snapshot(snapshot(bids=[("61000", qty)], mono=t), active)
    depleted = str(Decimal(qty) - Decimal(trade_qty))
    for i in range(cycles):
        d4.on_trade(trade("61000", trade_qty, mono=t + 0.01, trade_id=i + 1))
        events += d4.on_depth_snapshot(snapshot(bids=[("61000", depleted)], mono=t + 0.05), active)
        events += d4.on_depth_snapshot(snapshot(bids=[("61000", qty)], mono=t + 0.1), active)
        t += step
    return events


def run_hidden_cycles(d4, cycles, *, start=0.0, step=0.2, qty="100", trade_qty="10"):
    """체결은 있는데 표시잔량이 안 줄어드는 틱 — 은닉 리필(②, 네이티브 아이스버그) 합성."""
    active = episodes(BID)
    events = []
    t = start
    events += d4.on_depth_snapshot(snapshot(bids=[("61000", qty)], mono=t), active)
    for i in range(cycles):
        d4.on_trade(trade("61000", trade_qty, mono=t + 0.01, trade_id=100 + i))
        events += d4.on_depth_snapshot(snapshot(bids=[("61000", qty)], mono=t + 0.1), active)
        t += step
    return events


# ── 흡수량 산식 — ① 가시 리필 ─────────────────────────────────


class TestVisibleRefill:
    def test_paired_refill_accumulates_and_fires(self):
        # R=20, 사이클당 가시 리필 10 → 5사이클 = 50 ≥ 2.0×20, 이벤트 5 ≥ 5
        d4 = registered_detector()
        events = run_visible_cycles(d4, 5)
        assert len(events) == 1
        detected = events[0]
        assert detected.kind is D4DefenseKind.DETECTED
        assert detected.absorbed_visible == Decimal("50")
        assert detected.absorbed_hidden == Decimal("0")
        assert detected.multiple == Decimal("2.5")
        assert detected.event_count == 5
        assert detected.base_qty == Decimal("20")

    def test_unrelated_addition_outside_window_excluded(self):
        # 체결 후 시간이 지난 무관 재유입은 비인정 (횡보 회전의 신규 주문, PRD §8 D4)
        d4 = registered_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_trade(trade("61000", "10", mono=0.01, trade_id=1))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.05), active)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "170")], mono=60.0), active)  # 창 밖
        assert d4._streaks[KEY].absorbed_visible == Decimal(0)

    def test_positive_delta_without_any_trade_excluded(self):
        d4 = registered_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "150")], mono=0.1), active)
        assert d4._streaks[KEY].absorbed_visible == Decimal(0)

    def test_trade_arriving_before_contact_snapshot_still_pairs(self):
        # 스트림 순서 역전: 체결이 접촉 스냅샷보다 먼저 도착해도 ① 쌍 성립
        d4 = registered_detector()
        d4.on_trade(trade("61000", "10", mono=0.0, trade_id=1))
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.1), active)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.2), active)
        assert d4._streaks[KEY].absorbed_visible == Decimal("10")

    def test_flicker_to_zero_then_refill_counts(self):
        # 잔량 순간 0(스냅샷 부재) 후 회복 — 아이스버그 전형 패턴
        d4 = registered_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "10")], mono=0.0), active)
        d4.on_trade(trade("61000", "10", mono=0.01, trade_id=1))
        d4.on_depth_snapshot(snapshot(bids=[("60999", "1")], mono=0.05), active)  # 61000 부재=0
        d4.on_depth_snapshot(snapshot(bids=[("61000", "10")], mono=0.1), active)
        assert d4._streaks[KEY].absorbed_visible == Decimal("10")


# ── 흡수량 산식 — ② 은닉 리필 (틱 대조 + 1틱 이월 보정) ────────


class TestHiddenRefill:
    def test_native_iceberg_tick_counts_and_fires(self):
        # 체결 10인데 잔량 불변 틱 — 원자적 재표시. 5틱 = 50 ≥ 40, 이벤트 5
        d4 = registered_detector()
        events = run_hidden_cycles(d4, 5)
        assert len(events) == 1
        assert events[0].kind is D4DefenseKind.DETECTED
        assert events[0].absorbed_hidden == Decimal("50")
        assert events[0].absorbed_visible == Decimal("0")

    def test_sideways_rotation_produces_zero_mismatch(self):
        # 횡보: 체결이 실제로 잔량을 깎음 — 틱별 mismatch ≈ 0, 은닉 0 (PRD 산식 문단)
        d4 = registered_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        for i in range(5):
            t = 0.2 * (i + 1)
            d4.on_trade(trade("61000", "10", mono=t, trade_id=i + 1))
            d4.on_depth_snapshot(
                snapshot(bids=[("61000", str(100 - 10 * (i + 1)))], mono=t + 0.05), active
            )
        streak = d4._streaks[KEY]
        assert streak.absorbed_hidden == Decimal(0)
        assert streak.absorbed_visible == Decimal(0)

    def test_partial_depletion_credits_only_mismatch(self):
        # 체결 10, 잔량 감소 4 → 은닉 리필 6
        d4 = registered_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_trade(trade("61000", "10", mono=0.01, trade_id=1))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "96")], mono=0.1), active)
        assert d4._streaks[KEY].absorbed_hidden == Decimal("6")

    def test_carry_absorbs_decrease_before_trade_inversion(self):
        # 감소 반영 스냅샷이 체결 이벤트보다 먼저 도착(역전) — 이월 보정으로 과대 계상 0
        d4 = registered_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.1), active)  # 감소 선행
        d4.on_trade(trade("61000", "10", mono=0.15, trade_id=1))  # 체결 후행
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.2), active)
        assert d4._streaks[KEY].absorbed_hidden == Decimal(0)

    def test_carry_is_limited_to_one_tick(self):
        # 이월은 직전 1틱 한정 — 2틱 지난 감소는 상쇄에 쓰이지 않는다 (오차 국소화)
        d4 = registered_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.1), active)  # carry 10
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.2), active)  # 빈 틱 — 소멸
        d4.on_trade(trade("61000", "10", mono=0.25, trade_id=1))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "90")], mono=0.3), active)
        assert d4._streaks[KEY].absorbed_hidden == Decimal("10")

    def test_same_tick_trade_with_visible_growth_credits_both(self):
        # 체결 10 + 순증 5 틱: ① 5 + ② 10 = 총 재유입 15와 정합
        d4 = registered_detector()
        active = episodes(BID)
        d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=0.0), active)
        d4.on_trade(trade("61000", "10", mono=0.05, trade_id=1))
        d4.on_depth_snapshot(snapshot(bids=[("61000", "105")], mono=0.1), active)
        streak = d4._streaks[KEY]
        assert streak.absorbed_visible == Decimal("5")
        assert streak.absorbed_hidden == Decimal("10")


# ── 발화 조건 ─────────────────────────────────────────────────


class TestFireConditions:
    def test_multiple_not_met_silent(self):
        d4 = registered_detector(qty="100")  # 2.0×100 = 200 필요
        events = run_visible_cycles(d4, 5)  # absorbed 50 < 200, 이벤트 5
        assert events == []

    def test_min_events_not_met_silent(self):
        # 단발 대형 틱: absorbed 50 ≥ 40이지만 이벤트 1 < 5 — 원샷 오발화 방지
        d4 = registered_detector()
        events = run_hidden_cycles(d4, 1, trade_qty="50")
        assert events == []

    def test_fires_once_per_streak_latch(self):
        d4 = registered_detector()
        events = run_visible_cycles(d4, 8)
        assert len([e for e in events if e.kind is D4DefenseKind.DETECTED]) == 1

    def test_untracked_level_ignored(self):
        # 레지스트리 미추적 레벨(스트릭 없음)은 접촉·리필이 있어도 무시
        d4 = make_detector()
        events = run_visible_cycles(d4, 8)
        assert events == []
        assert d4._streaks == {}

    def test_no_accumulation_outside_active_episode(self):
        # 비관통 게이트의 구조적 충족 — episode 밖(관통 종료 후 등) 틱은 대조·평가 없음
        d4 = registered_detector()
        run_visible_cycles(d4, 4)  # absorbed 40 경계 직전(이벤트 4)
        d4.on_episode_end(
            EpisodeEnd(
                episode=ContactEpisode(side=Side.BUY, price=Decimal("61000"), started_receive_time=0.0),
                reason=EpisodeEndReason.PIERCED,
                ended_receive_time=1.0,
            )
        )
        d4.on_trade(trade("61000", "10", mono=5.0, trade_id=99))
        events = d4.on_depth_snapshot(snapshot(bids=[("61000", "100")], mono=5.1), {})
        assert events == []
        assert d4._streaks[KEY].absorbed_visible == Decimal("40")  # 누적은 유지, 추가 인정 없음


# ── 스트릭 수명 — 생애 누적·래치·진행·종결 ────────────────────


class TestStreakLifecycle:
    def test_accumulation_survives_episode_end(self):
        # episode 경계 리셋 없음 — 구 episode 스코프 산식과 반대 (스트릭 생애 기준)
        d4 = registered_detector()
        run_visible_cycles(d4, 3)  # 30
        d4.on_episode_end(
            EpisodeEnd(
                episode=ContactEpisode(side=Side.BUY, price=Decimal("61000"), started_receive_time=0.0),
                reason=EpisodeEndReason.REBOUND,
                ended_receive_time=1.0,
            )
        )
        events = run_visible_cycles(d4, 2, start=10.0)  # +20 → 50 ≥ 40, 이벤트 5
        assert len(events) == 1
        assert events[0].kind is D4DefenseKind.DETECTED
        assert events[0].absorbed_total == Decimal("50")

    def test_progress_boundaries_after_latch_unbounded(self):
        # 발화(2.5×) 후 진행 경계 M+k·step: 3.0, 3.5, … 상한 없음
        d4 = registered_detector()
        events = run_visible_cycles(d4, 8)  # absorbed 80 = 4.0×
        progress = [e for e in events if e.kind is D4DefenseKind.PROGRESS]
        assert [e.boundary_multiple for e in progress] == [
            Decimal("3.0"),
            Decimal("3.5"),
            Decimal("4.0"),
        ]

    def test_removal_of_latched_streak_emits_closed(self):
        d4 = registered_detector()
        run_visible_cycles(d4, 5)
        closed = d4.on_wall_removed(removal())
        assert closed is not None and closed.kind is D4DefenseKind.CLOSED
        assert closed.absorbed_total == Decimal("50")
        assert closed.multiple == Decimal("2.5")
        assert d4._streaks == {}

    def test_removal_of_unlatched_streak_is_silent(self):
        d4 = registered_detector()
        run_visible_cycles(d4, 2)  # absorbed 20 — 미발화
        assert d4.on_wall_removed(removal()) is None

    def test_removal_of_unknown_key_is_noop(self):
        d4 = make_detector()
        assert d4.on_wall_removed(removal()) is None

    def test_base_qty_fixed_at_registration(self):
        # 벽이 100으로 성장해도 R = 등록 시점 20 유지 (분모 고정 — PRD §8 D4 스트릭)
        d4 = registered_detector(qty="20")
        events = run_visible_cycles(d4, 5, qty="100")
        assert events[0].base_qty == Decimal("20")


# ── epoch 경계 — INTERRUPTED·재개시 ───────────────────────────


class TestEpochBoundary:
    def test_reset_interrupts_latched_and_clears(self):
        d4 = registered_detector()
        run_visible_cycles(d4, 5)
        events = d4.reset()
        assert len(events) == 1
        assert events[0].kind is D4DefenseKind.INTERRUPTED
        assert d4._streaks == {}

    def test_reset_without_latch_returns_nothing(self):
        d4 = registered_detector()
        run_visible_cycles(d4, 2)
        assert d4.reset() == []

    def test_epoch_start_reincepts_with_current_qty(self):
        # v1.12 — 생존 벽 전체 새 스트릭, R = 현재 last_qty 재고정, 누적 0부터
        clock = FakeClock()
        d4 = registered_detector(clock=clock)
        run_visible_cycles(d4, 3)
        d4.reset()
        clock.now = 100.0
        d4.on_epoch_start([wall(qty="50")])
        streak = d4._streaks[KEY]
        assert streak.base_qty == Decimal("50")
        assert streak.absorbed_total == Decimal(0)
        assert streak.started_at == 100.0  # 새 스트릭 식별자 (dedup 충돌 방지)


# ── 관측 기록 (PRD §8 D4 v1.11 — 발화와 별개) ─────────────────


class TestObservationLogs:
    def test_absorb_event_logged_per_accepted_event(self, caplog):
        d4 = registered_detector()
        with caplog.at_level(logging.INFO, logger="order_monitor.detectors.d4"):
            run_visible_cycles(d4, 2)
            run_hidden_cycles(d4, 1, start=10.0)
        records = [r for r in caplog.records if r.message == "d4 absorb event"]
        assert [r.kind for r in records] == ["visible", "visible", "hidden"]
        assert records[0].qty == "10"
        assert hasattr(records[0], "pairing_delay_ms")  # ① 근거값
        assert records[2].tick_traded == "10" and records[2].tick_decrease == "0"  # ② 근거값

    def test_streak_summary_logged_on_near_miss_removal(self, caplog):
        # 발화 무관 absorbed > 0 전건 — near-miss 분포가 absorb_multiple 튜닝 근거
        d4 = registered_detector()
        run_visible_cycles(d4, 2)  # 20 = 1.0× — 미발화
        with caplog.at_level(logging.INFO, logger="order_monitor.detectors.d4"):
            d4.on_wall_removed(removal())
        summaries = [r for r in caplog.records if r.message == "d4 streak summary"]
        assert len(summaries) == 1
        assert summaries[0].max_multiple == "1"
        assert summaries[0].latched is False
        assert summaries[0].reason == "tombstone"

    def test_streak_summary_skipped_when_nothing_absorbed(self, caplog):
        d4 = registered_detector()
        with caplog.at_level(logging.INFO, logger="order_monitor.detectors.d4"):
            d4.on_wall_removed(removal())
            d4.reset()
        assert [r for r in caplog.records if r.message == "d4 streak summary"] == []

    def test_streak_summary_logged_on_epoch_end(self, caplog):
        d4 = registered_detector()
        run_visible_cycles(d4, 2)
        with caplog.at_level(logging.INFO, logger="order_monitor.detectors.d4"):
            d4.reset()
        summaries = [r for r in caplog.records if r.message == "d4 streak summary"]
        assert len(summaries) == 1
        assert summaries[0].reason == "epoch_end"


# ── 체결 버퍼 바운드 ──────────────────────────────────────────


def test_trade_buffer_time_bounded():
    d4 = make_detector(window_ms=500)
    for i in range(100):
        d4.on_trade(trade(str(61000 + i), "1", mono=i * 1.0, trade_id=i))
    # 500ms 창 밖 체결은 전역 프루닝 — 마지막 1건만 잔존
    assert len(d4._trades) == 1
    assert len(d4._trade_log) == 1
