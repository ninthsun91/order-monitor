"""D5 인텐트→실체결 상태기계 (PRD §8 D5 v1.11 — 케이스 1 전용)·소멸 분류·INTERRUPTED·진행률."""

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
    realize_pct="0.6",
    step="0.2",
):
    cum = cum if cum is not None else {}
    return D5Detector(
        realize_pct=Decimal(realize_pct),
        progress_step_pct=Decimal(step),
        cum_traded_lookup=lambda side, price: cum.get((side, price), Decimal(0)),
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


# ── 케이스1 발화 (evaluate) ───────────────────────────────────


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

    def test_confirm_latches_intent_and_closes_on_removal(self):
        """(v1.9) 확정은 래치 — 인텐트 유지·재확정 없음, 소멸 시 CONFIRMED_CLOSED 마감."""
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("720")}
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        d5.evaluate()
        assert d5.evaluate() == []  # 확정 재발화 없음 (래치), 진행 경계도 아직 없음
        closing = d5.on_d1_removed(removed())
        assert closing.state is D5TerminalState.CONFIRMED_CLOSED
        assert closing.level_realized_rate == Decimal("0.6")
        assert d5.on_d1_removed(removed()) is None  # 마감 후 재확인은 no-op


# ── 소멸(D1 REMOVED) 분류 우선순위 ────────────────────────────


class TestDissolution:
    def test_case1_recheck_confirms_on_removal(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("720")}
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        event = d5.on_d1_removed(removed(attribution=D1Attribution.FILLED))
        assert event.state is D5TerminalState.EXECUTION_CONFIRMED

    def test_filled_attribution_below_realize_is_partially_executed(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("300")}  # 25% < 60%
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        event = d5.on_d1_removed(removed(attribution=D1Attribution.FILLED))
        assert event.state is D5TerminalState.PARTIALLY_EXECUTED
        assert event.level_realized_rate == Decimal("0.25")

    def test_pulled_attribution_is_withdrawn(self):
        clock = FakeClock()
        d5 = make_detector(clock)
        d5.on_d1_appeared(appeared())
        event = d5.on_d1_removed(removed(attribution=D1Attribution.PULLED))
        assert event.state is D5TerminalState.INTENT_WITHDRAWN

    def test_removal_of_unknown_key_is_noop(self):
        clock = FakeClock()
        d5 = make_detector(clock)
        assert d5.on_d1_removed(removed()) is None


# ── 무만료 — 인텐트 수명 = 벽 수명 (PRD v1.5, TTL 폐지) ───────


class TestNoExpiry:
    def test_intent_survives_arbitrarily_long_without_transition(self):
        # 실측 사각지대 시나리오의 역: 등록 23h+ 뒤에 도달해도 판정 가능해야 한다
        clock = FakeClock(0.0)
        cum = {}
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        clock.now = 83114.0  # 실측 61k 벽 지속시간 — 구 TTL(1800s)의 46배
        assert d5.evaluate() == []  # 만료 없이 활성 유지
        cum[(Side.BUY, Decimal("61000"))] = Decimal("720")  # 뒤늦은 도달+흡수
        events = d5.evaluate()
        assert len(events) == 1
        assert events[0].state is D5TerminalState.EXECUTION_CONFIRMED
        assert events[0].registered_seconds == 83114.0

    def test_late_removal_still_judged(self):
        # 등록 한참 뒤 벽 소진 소멸 — 인텐트가 살아있어 4분류 평가가 성립
        clock = FakeClock(0.0)
        cum = {(Side.BUY, Decimal("61000")): Decimal("300")}
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        clock.now = 90000.0
        event = d5.on_d1_removed(removed(attribution=D1Attribution.FILLED))
        assert event.state is D5TerminalState.PARTIALLY_EXECUTED


# ── epoch 종료 → INTERRUPTED ──────────────────────────────────


class TestEpochEndReset:
    def test_reset_interrupts_all_active_intents(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("300")}
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        d5.on_d1_appeared(appeared(side=Side.SELL, price="62000", qty="1200"))
        events = d5.reset()
        assert len(events) == 2
        assert {e.state for e in events} == {D5TerminalState.INTERRUPTED}
        bid_event = next(e for e in events if e.side is Side.BUY)
        assert bid_event.level_realized_rate == Decimal("0.25")  # 최종 실현률 동반

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

    def test_multi_boundary_jump_emits_each_crossed_boundary(self):
        clock = FakeClock()
        cum = {(Side.BUY, Decimal("61000")): Decimal("540")}  # 45% → 0.2, 0.4 둘 다 신규
        d5 = make_detector(clock, cum=cum, step="0.2")
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert [e.boundary_pct for e in events] == [Decimal("0.2"), Decimal("0.4")]


# ── 케이스1 확정 래치 후 진행률 무상한 (PRD §8 D5 v1.9) ───────


class TestConfirmedLatchProgress:
    KEY = (Side.BUY, Decimal("61000"))

    def confirmed_detector(self, cum_qty="720"):
        clock = FakeClock()
        cum = {self.KEY: Decimal(cum_qty)}
        d5 = make_detector(clock, cum=cum)
        d5.on_d1_appeared(appeared())
        events = d5.evaluate()
        assert events[-1].state is D5TerminalState.EXECUTION_CONFIRMED
        return d5, cum

    def test_progress_continues_past_100pct_after_confirm(self):
        d5, cum = self.confirmed_detector()  # 60% 확정
        cum[self.KEY] = Decimal("1450")  # 리필 재흡수 — 120.8%
        events = d5.evaluate()
        assert [e.boundary_pct for e in events] == [
            Decimal("0.8"),
            Decimal("1.0"),
            Decimal("1.2"),
        ]
        assert all(isinstance(e, D5Progress) for e in events)
        assert d5.evaluate() == []  # 경계당 1회

    def test_no_second_confirm_while_latched(self):
        d5, cum = self.confirmed_detector()
        cum[self.KEY] = Decimal("1450")
        assert not any(isinstance(e, D5Terminal) for e in d5.evaluate())

    def test_boundary_skipped_by_confirm_jump_fires_next_evaluate(self):
        # 급등 확정(0→85%): 확정 1회만, 이미 넘어선 80% 경계는 다음 evaluate에서 발화
        d5, _ = self.confirmed_detector(cum_qty="1020")  # 85%
        events = d5.evaluate()
        assert [e.boundary_pct for e in events] == [Decimal("0.8")]

    def test_removal_after_extended_progress_records_final_rate(self):
        d5, cum = self.confirmed_detector()
        cum[self.KEY] = Decimal("1450")
        d5.evaluate()
        closing = d5.on_d1_removed(removed(attribution=D1Attribution.FILLED))
        assert closing.state is D5TerminalState.CONFIRMED_CLOSED
        assert closing.level_realized_rate == Decimal("1450") / Decimal("1200")

    def test_reset_interrupts_latched_intent_with_final_rate(self):
        d5, cum = self.confirmed_detector()
        cum[self.KEY] = Decimal("1450")
        events = d5.reset()
        assert len(events) == 1
        assert events[0].state is D5TerminalState.INTERRUPTED
        assert events[0].level_realized_rate == Decimal("1450") / Decimal("1200")
