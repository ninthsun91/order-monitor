"""D5 인텐트→실체결 상태기계 (PRD §8 D5) — 케이스1/2·소멸 4분류·TTL·INTERRUPTED·진행률."""

from decimal import Decimal

from order_monitor.detectors.d1 import D1Appeared, D1Attribution, D1Removed
from order_monitor.detectors.d5 import (
    MAX_ACTIVE_INTENTS,
    D5Detector,
    D5Progress,
    D5Terminal,
    D5TerminalState,
)
from order_monitor.ingestion.events import Side


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


def appeared(side=Side.BUY, price="61000", qty="1200"):
    return D1Appeared(side=side, price=Decimal(price), qty=Decimal(qty), persisted_seconds=3.0)


def removed(side=Side.BUY, price="61000", attribution=D1Attribution.PULLED):
    return D1Removed(
        side=side,
        price=Decimal(price),
        last_qty=Decimal(0),
        peak_qty=Decimal("1200"),
        cum_traded=Decimal(0),
        attribution=attribution,
    )


def make_detector(
    clock,
    cum: dict | None = None,
    above: dict | None = None,
    realize_pct="0.6",
    realize_pct_above="0.6",
    ttl=1800.0,
    step="0.2",
):
    cum = cum if cum is not None else {}
    above = above if above is not None else {}
    return D5Detector(
        realize_pct=Decimal(realize_pct),
        realize_pct_above=Decimal(realize_pct_above),
        intent_ttl_seconds=ttl,
        progress_step_pct=Decimal(step),
        cum_traded_lookup=lambda side, price: cum.get((side, price), Decimal(0)),
        refill_above_lookup=lambda side, price: above.get((side, price), Decimal(0)),
        monotonic=clock,
    )


# ── 등록 ──────────────────────────────────────────────────────


def test_registration_ignores_duplicate_and_bounds_by_max_active():
    clock = FakeClock()
    d5 = make_detector(clock)
    d5.on_d1_appeared(appeared())
    d5.on_d1_appeared(appeared())  # 중복 — no-op
    assert len(d5._intents) == 1

    for i in range(MAX_ACTIVE_INTENTS):
        d5.on_d1_appeared(appeared(price=str(70000 + i)))
    assert len(d5._intents) == MAX_ACTIVE_INTENTS  # 상한 초과분 거부


# ── 케이스1/2 발화 (evaluate) ─────────────────────────────────


class TestEvaluateCases:
    def test_case1_fires_on_threshold_cross(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("720")}  # 720/1200 = 60%
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert events == [
            D5Terminal(
                intent_id=0,
                side=Side.BUY,
                price=Decimal("61000"),
                registered_qty=Decimal("1200"),
                state=D5TerminalState.EXECUTION_CONFIRMED,
                level_realized_rate=Decimal("0.6"),
                above_realized_rate=Decimal(0),
                registered_seconds=0.0,
            )
        ]

    def test_case1_not_fired_below_threshold(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("100")}  # < 진행률 경계(0.2×1200=240)도 미달
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert events == []

    def test_case2_fires_when_case1_unmet(self):
        clock = FakeClock()
        above = {(Side.BUY, Decimal("61000")): Decimal("720")}
        d5 = make_detector(clock, above=above)
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert len(events) == 1
        assert events[0].state is D5TerminalState.EXECUTION_INFERRED_ABOVE
        assert events[0].above_realized_rate == Decimal("0.6")

    def test_case1_wins_priority_when_both_met_simultaneously(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("720")}
        above = {(Side.BUY, Decimal("61000")): Decimal("720")}
        d5 = make_detector(clock, cum=cum, above=above)
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert len(events) == 1
        assert events[0].state is D5TerminalState.EXECUTION_CONFIRMED

    def test_terminal_removes_intent_from_active_tracking(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("720")}
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        d5.evaluate()
        assert d5.evaluate() == []  # 더 이상 활성 아님
        assert d5.on_d1_removed(removed()) is None  # 소멸 재확인도 no-op


# ── 소멸(D1 REMOVED) 4분류 우선순위 ───────────────────────────


class TestDissolution:
    def test_case1_recheck_confirms_on_removal(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("720")}
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        event = d5.on_d1_removed(removed(attribution=D1Attribution.FILLED))
        assert event.state is D5TerminalState.EXECUTION_CONFIRMED

    def test_case2_recheck_takes_priority_over_pulled_fallback(self):
        clock = FakeClock()
        above = {(Side.BUY, Decimal("61000")): Decimal("720")}
        d5 = make_detector(clock, above=above)
        d5.on_d1_appeared(appeared())
        event = d5.on_d1_removed(removed(attribution=D1Attribution.PULLED))
        assert event.state is D5TerminalState.EXECUTION_INFERRED_ABOVE

    def test_filled_attribution_below_realize_is_partially_executed(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("300")}  # 25% < 60%
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        event = d5.on_d1_removed(removed(attribution=D1Attribution.FILLED))
        assert event.state is D5TerminalState.PARTIALLY_EXECUTED
        assert event.level_realized_rate == Decimal("0.25")

    def test_pulled_attribution_below_case2_is_withdrawn(self):
        clock = FakeClock()
        d5 = make_detector(clock)
        d5.on_d1_appeared(appeared())
        event = d5.on_d1_removed(removed(attribution=D1Attribution.PULLED))
        assert event.state is D5TerminalState.INTENT_WITHDRAWN

    def test_removal_of_unknown_key_is_noop(self):
        clock = FakeClock()
        d5 = make_detector(clock)
        assert d5.on_d1_removed(removed()) is None

    def test_terminal_record_carries_both_realized_rates(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("300")}
        above = {(Side.BUY, Decimal("61000")): Decimal("120")}
        d5 = make_detector(clock, cum=cum, above=above)
        d5.on_d1_appeared(appeared())
        event = d5.on_d1_removed(removed(attribution=D1Attribution.FILLED))
        assert event.level_realized_rate == Decimal("0.25")
        assert event.above_realized_rate == Decimal("0.1")


# ── TTL 만료 ──────────────────────────────────────────────────


class TestTTL:
    def test_expires_after_ttl_with_no_transition(self):
        clock = FakeClock(0.0)
        d5 = make_detector(clock, ttl=1800.0)
        d5.on_d1_appeared(appeared())
        clock.now = 1799.0
        assert d5.evaluate() == []
        clock.now = 1800.0
        events = d5.evaluate()
        assert len(events) == 1
        assert events[0].state is D5TerminalState.INTENT_EXPIRED
        assert events[0].registered_seconds == 1800.0

    def test_ttl_check_yields_to_case1_if_also_crossed(self):
        clock = FakeClock(0.0)
        cum = {(Side.BUY, Decimal("61000")): Decimal("720")}
        d5 = make_detector(clock, cum=cum, ttl=1800.0)
        d5.on_d1_appeared(appeared())
        clock.now = 1800.0
        events = d5.evaluate()
        assert len(events) == 1
        assert events[0].state is D5TerminalState.EXECUTION_CONFIRMED


# ── epoch 종료 → INTERRUPTED ──────────────────────────────────


class TestEpochEndReset:
    def test_reset_interrupts_all_active_intents(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("300")}
        above = {(Side.SELL, Decimal("62000")): Decimal("120")}
        d5 = make_detector(clock, cum=cum, above=above)
        d5.on_d1_appeared(appeared())
        d5.on_d1_appeared(appeared(side=Side.SELL, price="62000", qty="1200"))
        events = d5.reset()
        assert len(events) == 2
        assert {e.state for e in events} == {D5TerminalState.INTERRUPTED}
        # above_realized_rate가 함께 남음 (d4.reset() 전에 호출되어야 하는 이유)
        bid_event = next(e for e in events if e.side is Side.BUY)
        assert bid_event.level_realized_rate == Decimal("0.25")

    def test_reset_clears_active_tracking(self):
        clock = FakeClock()
        d5 = make_detector(clock)
        d5.on_d1_appeared(appeared())
        d5.reset()
        assert d5.on_d1_removed(removed()) is None
        assert d5.evaluate() == []


# ── 진행률 알림 ───────────────────────────────────────────────


class TestProgress:
    def test_progress_fires_at_step_boundaries_below_realize_pct(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("240")}  # 240/1200 = 20%
        d5 = make_detector(clock, cum=cum, step="0.2")
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert events == [
            D5Progress(
                intent_id=0,
                side=Side.BUY,
                price=Decimal("61000"),
                registered_qty=Decimal("1200"),
                series="case1",
                boundary_pct=Decimal("0.2"),
                realized_qty=Decimal("240"),
            )
        ]

    def test_progress_boundary_fires_once_only(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("240")}
        d5 = make_detector(clock, cum=cum, step="0.2")
        d5.on_d1_appeared(appeared())
        d5.evaluate()
        assert d5.evaluate() == []  # 같은 경계 재발화 없음

    def test_progress_skips_boundaries_at_or_above_realize_pct(self):
        # step 0.2, realize_pct 0.6 → 유효 경계는 0.2/0.4뿐, 0.6 도달 시엔 종국이 대체
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("719")}  # 719/1200 ≈ 0.599
        d5 = make_detector(clock, cum=cum, step="0.2")
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert [e.boundary_pct for e in events] == [Decimal("0.2"), Decimal("0.4")]

    def test_case1_and_case2_progress_cursors_are_independent(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("240")}  # case1 20%
        above = {(Side.BUY, Decimal("61000")): Decimal("120")}  # case2 10% — 아직 미달
        d5 = make_detector(clock, cum=cum, above=above, step="0.2")
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert [e.series for e in events] == ["case1"]  # case2는 10% < 20% 경계 미도달

    def test_multi_boundary_jump_emits_each_crossed_boundary(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("540")}  # 45% → 0.2, 0.4 둘 다 신규
        d5 = make_detector(clock, cum=cum, step="0.2")
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert [e.boundary_pct for e in events] == [Decimal("0.2"), Decimal("0.4")]
