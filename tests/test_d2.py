"""D2 에피소드형 상대 임계 볼륨 버스트 (PRD §8 D2 v1.3).

온셋/히스테리시스 종료/병합/성격 라벨/요약 판정/워밍업 보류/epoch 폐기.
"""

from decimal import Decimal

from order_monitor.detectors.d2 import (
    D2BurstOnset,
    D2BurstSummary,
    D2Detector,
    D2Label,
    D2Verdict,
)
from order_monitor.ingestion.events import AggTradeEvent, Side
from order_monitor.state.trade_window import TradeWindow


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


class StubBaseline:
    def __init__(self, mean):
        self.mean = mean

    def per_minute_mean(self):
        return self.mean


def trade(qty, side=Side.BUY, t_ms=0, price="64000"):
    return AggTradeEvent(
        agg_trade_id=1,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=side,
        exchange_time_ms=t_ms,
        local_monotonic_receive_time=0.0,
    )


def make_detector(mean="4", clock=None):
    # thr = max(30, 10 × 4) = 40, exit = 20, 병합 600s
    window = TradeWindow(60.0)
    detector = D2Detector(
        vol_floor=Decimal(30),
        vol_multiplier=Decimal(10),
        exit_ratio=Decimal("0.5"),
        merge_seconds=600.0,
        directional_ratio=Decimal("0.5"),
        balanced_ratio=Decimal("0.2"),
        absorb_delta_min=Decimal("0.35"),
        move_min_pct=Decimal("0.1"),
        window_seconds=60.0,
        window=window,
        baseline=StubBaseline(Decimal(mean) if mean is not None else None),
        monotonic=clock or FakeClock(),
    )
    return detector, window


def feed(detector, window, event):
    window.add(event)
    return detector.on_trade(event)


# ── 워밍업 보류 + 온셋 ───────────────────────────────────────


def test_warmup_holds_all_judgment():
    detector, window = make_detector(mean=None)
    assert feed(detector, window, trade("100")) == []
    assert not detector.episode_active
    assert detector.on_tick() == []


def test_onset_fires_when_window_total_reaches_threshold():
    detector, window = make_detector()
    assert feed(detector, window, trade("20", t_ms=0, price="64000")) == []
    events = feed(detector, window, trade("20", t_ms=1000, price="64010"))
    assert events == [
        D2BurstOnset(
            window_qty=Decimal(40),
            buy_qty=Decimal(40),
            sell_qty=Decimal(0),
            threshold=Decimal(40),
            baseline_per_minute=Decimal(4),
            label=D2Label.DIRECTIONAL_BUY,
            price=Decimal("64010"),
            start_exchange_ms=0,
        )
    ]
    assert detector.episode_active


def test_total_volume_triggers_where_single_side_would_not():
    # 매수 25 + 매도 25 — 방향 분리(구판)면 미발화, 총량 트리거는 발화 (v1.3 개편 목적)
    detector, window = make_detector()
    feed(detector, window, trade("25", side=Side.BUY, t_ms=0))
    events = feed(detector, window, trade("25", side=Side.SELL, t_ms=1000))
    assert len(events) == 1 and events[0].label is D2Label.BALANCED


def test_floor_binds_when_baseline_low():
    detector, window = make_detector(mean="1")  # 10×1=10 < floor 30
    events = feed(detector, window, trade("30"))
    assert events[0].threshold == Decimal(30)


def test_no_second_onset_while_episode_active():
    detector, window = make_detector()
    feed(detector, window, trade("40", t_ms=0))
    assert feed(detector, window, trade("50", t_ms=1000)) == []


# ── 히스테리시스 종료 + 병합 + 요약 ──────────────────────────


def test_summary_uses_checkpoint_and_excludes_cooling_trades():
    clock = FakeClock()
    detector, window = make_detector(clock=clock)
    feed(detector, window, trade("40", t_ms=0, price="64000"))  # 온셋
    feed(detector, window, trade("10", t_ms=10_000, price="64100"))  # 활성 중 흡수

    # 창 만료로 총합 1 < exit(20) → 잠정 종료 (이 체결까지가 체크포인트)
    clock.now = 100.0
    feed(detector, window, trade("1", side=Side.SELL, t_ms=100_000, price="63900"))
    assert detector.episode_active

    # 병합 대기 중 저볼륨 체결 — 확정 종료 시 제외되어야 함
    clock.now = 200.0
    feed(detector, window, trade("2", t_ms=200_000, price="65000"))

    # 병합 창(600s) 경과 후 확정 종료
    clock.now = 800.0
    events = feed(detector, window, trade("1", side=Side.SELL, t_ms=700_000, price="63000"))
    assert len(events) == 1
    summary = events[0]
    assert isinstance(summary, D2BurstSummary)
    assert summary.start_exchange_ms == 0
    assert summary.end_exchange_ms == 100_000
    assert summary.total_qty == Decimal(51)  # 40 + 10 + 1 (체크포인트까지)
    assert summary.buy_qty == Decimal(50) and summary.sell_qty == Decimal(1)
    assert summary.open_price == Decimal("64000")
    assert summary.high_price == Decimal("64100")
    assert summary.low_price == Decimal("63900")
    assert summary.close_price == Decimal("63900")
    assert summary.baseline_per_minute == Decimal(4)
    # 확정 시점 최근 체결가(병합 대기 중 체결 포함) — 판정의 되돌림 근거
    assert summary.finalize_price == Decimal("63000")
    # 매수 쏠림(50/1)인데 가격이 델타 방향으로 못 감(-0.16%) → 매수 흡수 (정체)
    assert summary.verdict is D2Verdict.BUY_ABSORBED_STALL
    assert not detector.episode_active


def test_reignition_within_merge_resumes_without_new_onset():
    clock = FakeClock()
    detector, window = make_detector(clock=clock)
    feed(detector, window, trade("40", t_ms=0))
    clock.now = 100.0
    feed(detector, window, trade("1", t_ms=100_000))  # 잠정 종료

    # 병합 창 내 재점화 — 온셋 재발화 없이 같은 에피소드
    clock.now = 200.0
    feed(detector, window, trade("5", t_ms=200_000))  # 대기 중 체결 (재점화엔 미달)
    events = feed(detector, window, trade("40", t_ms=201_000))  # 총합 45+1 ≥ 40 → 재점화
    assert events == []
    assert detector.episode_active

    # 이후 확정 종료 — 대기 중 체결(5)이 편입되어 있어야 함
    clock.now = 300.0
    feed(detector, window, trade("1", t_ms=300_000))  # 잠정 종료 2차
    clock.now = 1000.0
    summary = detector.on_tick()[0]
    assert summary.total_qty == Decimal(40 + 1 + 5 + 40 + 1)


def test_tick_drives_cooling_and_finalize_when_trades_stop():
    clock = FakeClock()
    detector, window = make_detector(clock=clock)
    feed(detector, window, trade("40", t_ms=0))

    clock.now = 59.0
    assert detector.on_tick() == []  # 창 길이 미경과 — 아직 활성
    clock.now = 60.0
    assert detector.on_tick() == []  # 체결 두절 = 잠정 종료 시작
    clock.now = 659.0
    assert detector.on_tick() == []
    clock.now = 660.0
    events = detector.on_tick()
    assert len(events) == 1 and isinstance(events[0], D2BurstSummary)


# ── 성격 라벨 경계 ───────────────────────────────────────────


def test_label_boundaries():
    detector, _ = make_detector()
    assert detector._label(Decimal(30), Decimal(10)) is D2Label.DIRECTIONAL_BUY  # r=0.5
    assert detector._label(Decimal(10), Decimal(30)) is D2Label.DIRECTIONAL_SELL
    assert detector._label(Decimal(24), Decimal(16)) is D2Label.BALANCED  # r=0.2
    assert detector._label(Decimal(25), Decimal(15)) is D2Label.MIXED  # r=0.25


# ── 요약 판정 — 델타비 × 가격 반응 (결정 기록 2026-07-13) ────


def test_verdict_branches():
    detector, _ = make_detector()
    v, D = detector._verdict, Decimal
    # (buy, sell, open, close, finalize_price)
    # 쏠림 없음 (r=0.09 ≤ 0.2) → 양방향 충돌
    assert v(D(50), D(60), D(64000), D(64000), D(64000)) is D2Verdict.TWO_WAY
    # 판단 유보 구간 (0.2 < r=0.27 < 0.35) → 혼합
    assert v(D(40), D(70), D(64000), D(63800), D(63800)) is D2Verdict.MIXED
    # 매도 쏠림(r=0.82)인데 변화 -0.03% < 0.1% → 매도 흡수 (정체)
    assert v(D(10), D(100), D(64000), D(63980), D(63980)) is D2Verdict.SELL_ABSORBED_STALL
    # 밀렸지만(-0.31%) 확정 시점에 시가 회복 → 매도 흡수 (되돌림)
    assert v(D(10), D(100), D(64000), D(63800), D(64050)) is D2Verdict.SELL_ABSORBED_RETRACE
    # 밀렸고 시가 아래 유지 → 매도 관철
    assert v(D(10), D(100), D(64000), D(63800), D(63850)) is D2Verdict.SELL_FOLLOW
    # 매수 쪽 대칭
    assert v(D(100), D(10), D(64000), D(64020), D(64020)) is D2Verdict.BUY_ABSORBED_STALL
    assert v(D(100), D(10), D(64000), D(64200), D(63990)) is D2Verdict.BUY_ABSORBED_RETRACE
    assert v(D(100), D(10), D(64000), D(64200), D(64150)) is D2Verdict.BUY_FOLLOW


# ── epoch 종료 폐기 (PRD §5.4) ───────────────────────────────


def test_reset_discards_episode_without_summary():
    clock = FakeClock()
    detector, window = make_detector(clock=clock)
    feed(detector, window, trade("40", t_ms=0))
    detector.reset()
    assert not detector.episode_active
    clock.now = 2000.0
    assert detector.on_tick() == []
