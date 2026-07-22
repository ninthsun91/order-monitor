"""알림 계층 (PRD §9.1, §9.2) — dedup/쿨다운, on/off 정책, Telegram 발송기."""

import asyncio
import contextlib
import dataclasses
from decimal import Decimal
from pathlib import Path

from order_monitor.alerting.dispatcher import AlertDispatcher
from order_monitor.alerting.telegram import TelegramSender
from order_monitor.config import load_config
from order_monitor.detectors.d1 import D1Appeared, D1Attribution, D1Removed, D1Suppressed
from order_monitor.detectors.d2 import D2BurstOnset, D2BurstSummary, D2Label, D2Verdict
from order_monitor.detectors.d5 import D5Progress, D5Terminal, D5TerminalState
from order_monitor.ingestion.events import Side
from order_monitor.persistence.alerts_outbox import AlertsOutboxStore

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


class FakeSender:
    def __init__(self):
        self.sent = []
        self.on_sent_callbacks = []

    def enqueue(self, text, on_sent=None):
        self.sent.append(text)
        self.on_sent_callbacks.append(on_sent)


def make_config(**alert_overrides):
    config = load_config(EXAMPLE_CONFIG)
    if alert_overrides:
        config = dataclasses.replace(
            config, alerts=dataclasses.replace(config.alerts, **alert_overrides)
        )
    return config


def appeared(price="61000"):
    return D1Appeared(side=Side.BUY, price=Decimal(price), qty=Decimal(1200), persisted_seconds=3.2)


def removed():
    return D1Removed(
        side=Side.BUY,
        price=Decimal("61000"),
        last_qty=Decimal(0),
        peak_qty=Decimal("1364.86"),
        cum_traded=Decimal(980),
        attribution=D1Attribution.FILLED,
    )


def onset():
    return D2BurstOnset(
        window_qty=Decimal("152"),
        buy_qty=Decimal("30"),
        sell_qty=Decimal("122"),
        threshold=Decimal("84"),
        baseline_per_minute=Decimal("8.4"),
        label=D2Label.DIRECTIONAL_SELL,
        price=Decimal("64120"),
        start_exchange_ms=1783825200000,
    )


def summary():
    return D2BurstSummary(
        start_exchange_ms=1783816920000,  # 2026-07-12 09:42 KST
        end_exchange_ms=1783817520000,  # 09:52 KST (10분 — 고정 15분 환산과 구분되는 길이)
        total_qty=Decimal("1207"),
        buy_qty=Decimal("590"),
        sell_qty=Decimal("617"),
        baseline_per_minute=Decimal("8.4"),
        label=D2Label.BALANCED,
        open_price=Decimal("64300"),
        high_price=Decimal("64320"),
        low_price=Decimal("64020"),
        close_price=Decimal("64150"),
        finalize_price=Decimal("64155"),
        verdict=D2Verdict.TWO_WAY,
    )


# ── 발송 정책 on/off (PRD §9.1) ──────────────────────────────


def test_send_d1_off_suppresses_d1_only():
    # 게이트 동작 검증 — example의 토글 현재값(기본 on, PRD v1.10)에 의존하지 않게 명시 off
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d1=False), sender, monotonic=FakeClock())
    assert dispatcher.dispatch(appeared()) is False
    assert dispatcher.dispatch(removed()) is False
    assert dispatcher.dispatch(onset()) is True
    assert dispatcher.dispatch(summary()) is True
    assert len(sender.sent) == 2


def test_d2_send_flags_are_independent():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d2=False), sender, monotonic=FakeClock())
    assert dispatcher.dispatch(onset()) is False
    assert dispatcher.dispatch(summary()) is True  # send_d2_summary는 별개 (PRD §9.1 v1.3)

    sender2 = FakeSender()
    dispatcher2 = AlertDispatcher(make_config(send_d2_summary=False), sender2, monotonic=FakeClock())
    assert dispatcher2.dispatch(onset()) is True
    assert dispatcher2.dispatch(summary()) is False


def test_d1_suppressed_event_is_log_only():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d1=True), sender, monotonic=FakeClock())
    event = D1Suppressed(
        side=Side.BUY, price=Decimal("61000"), peak_qty=Decimal(1200), above_threshold_seconds=1.5
    )
    assert dispatcher.dispatch(event) is False
    assert sender.sent == []


def test_message_formats():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d1=True), sender, monotonic=FakeClock())
    dispatcher.dispatch(appeared())
    dispatcher.dispatch(onset())
    assert "대형 벽 출현 (D1)" in sender.sent[0]
    assert "61,000 (bid) · 표시 1,200 BTC" in sender.sent[0]
    assert "볼륨 버스트 시작 (D2)" in sender.sent[1]
    assert "60초 체결 152 BTC (매수 30 / 매도 122 · Δ -92.0)" in sender.sent[1]
    assert "기준선: 분당 8.4 BTC (24h 평균 체결량)" in sender.sent[1]
    assert "성격: 방향성 매도 (델타비 0.61) · 현재가 64,120" in sender.sent[1]


def test_d2_summary_format():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender, monotonic=FakeClock())
    dispatcher.dispatch(summary())
    text = sender.sent[0]
    assert "볼륨 버스트 요약 (D2) — 10분 (KST 09:42~09:52)" in text
    # 1207 ÷ (분당 8.4 × 10분) ≈ 14.4 — 표시 구간과 같은 값으로 환산
    assert "누적 1,207 BTC (매수 590 / 매도 617 · Δ -27.0) — 평상시 10분치의 14.4배" in text
    assert "성격: 양방향(흡수성 후보) (델타비 0.02)" in text
    assert "판정: 양방향 충돌 — 요약 시점 64,155 (+0.01%)" in text
    assert "가격: 64,300 → 64,150 (-0.23%) · 고 64,320 / 저 64,020" in text


def test_d2_summary_nearby_wall_context():
    # M6 관측 보완 ② — 종가(64,150) 기준 bid/ask 각 최근접 추적 벽 1개 동봉
    from order_monitor.state.wall_registry import Wall

    def wall(price, side, last, peak):
        return Wall(
            price=Decimal(price),
            side=side,
            last_qty=Decimal(last),
            peak_qty=Decimal(peak),
            first_seen_at=0.0,
            first_seen_above_threshold=None,
            last_seen_at=0.0,
        )

    walls = [
        wall("62800", Side.BUY, "180", "220.65"),
        wall("61000", Side.BUY, "1200", "1300"),  # 더 먼 bid — 제외
        wall("64500", Side.SELL, "150", "150"),
    ]
    sender = FakeSender()
    dispatcher = AlertDispatcher(
        make_config(), sender, monotonic=FakeClock(), wall_lookup=lambda: walls
    )
    dispatcher.dispatch(summary())
    assert "근접 벽: bid 62,800 (잔량 180 / 피크 220.6 BTC) · ask 64,500 (잔량 150 / 피크 150 BTC)" in sender.sent[0]

    # 벽이 없으면 줄 자체가 생략
    sender2 = FakeSender()
    dispatcher2 = AlertDispatcher(
        make_config(), sender2, monotonic=FakeClock(), wall_lookup=lambda: []
    )
    dispatcher2.dispatch(summary())
    assert "근접 벽" not in sender2.sent[0]


def test_d2_summary_verdict_absorbed_retrace_format():
    # 실측 사례(2026-07-13 14:27 KST): 델타비 0.90 매도인데 요약 시점 시가 회복
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender, monotonic=FakeClock())
    event = dataclasses.replace(
        summary(),
        buy_qty=Decimal("6.1"),
        sell_qty=Decimal("112.1"),
        total_qty=Decimal("118.2"),
        verdict=D2Verdict.SELL_ABSORBED_RETRACE,
        finalize_price=Decimal("64310"),
    )
    dispatcher.dispatch(event)
    assert "판정: 매도 흡수 (되돌림) — 요약 시점 64,310 (+0.25%)" in sender.sent[0]


def test_d1_removed_format_shows_attribution():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d1=True), sender, monotonic=FakeClock())
    dispatcher.dispatch(removed())
    assert "체결 소진(FILLED)" in sender.sent[0]
    assert "피크 1,364.86 BTC → 잔량 0 BTC" in sender.sent[0]


# ── FEED_STALE 워치독 알림 (PRD §11.1 — M5 선행분) ───────────


def test_feed_stale_sent_regardless_of_flags_with_per_stream_cooldown():
    from order_monitor.ingestion.health import StreamStale

    clock = FakeClock()
    sender = FakeSender()
    dispatcher = AlertDispatcher(
        make_config(send_d1=False, send_d2=False, send_d2_summary=False), sender, monotonic=clock
    )
    stale = StreamStale(stream="btcusdt@depth20@100ms", silent_seconds=32.4)
    assert dispatcher.dispatch(stale) is True
    assert "🛑 피드 정지 (FEED_STALE)" in sender.sent[0]
    assert "btcusdt@depth20@100ms — 32초간 수신 없음" in sender.sent[0]
    # 같은 스트림 재발화는 쿨다운 내 억제, 다른 스트림은 즉시 발송
    assert dispatcher.dispatch(stale) is False
    assert dispatcher.dispatch(StreamStale(stream="btcusdt@aggTrade", silent_seconds=61)) is True
    clock.now = 300.0
    assert dispatcher.dispatch(stale) is True


# ── dedup/쿨다운 (PRD §9.2) ──────────────────────────────────


def test_same_bucket_suppressed_within_cooldown():
    clock = FakeClock()
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d1=True), sender, monotonic=clock)
    assert dispatcher.dispatch(appeared("61000")) is True
    # 같은 50 USDT 버킷 (61,020) — 쿨다운(300s) 내 억제, APPEARED/REMOVED 공용 키
    clock.now = 299.0
    assert dispatcher.dispatch(appeared("61020")) is False
    assert dispatcher.dispatch(removed()) is False
    clock.now = 300.0
    assert dispatcher.dispatch(appeared("61020")) is True


def test_different_bucket_not_suppressed_and_d2_has_no_cooldown():
    clock = FakeClock()
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d1=True), sender, monotonic=clock)
    assert dispatcher.dispatch(appeared("61000")) is True
    assert dispatcher.dispatch(appeared("61050")) is True  # 다음 버킷
    # D2는 시간 쿨다운 미적용 — 연속 온셋도 모두 발송 (에피소드 모델이 억제, PRD §9.2 v1.3)
    assert dispatcher.dispatch(onset()) is True
    assert dispatcher.dispatch(onset()) is True


# ── D5 종국/진행률 (PRD §8 D5, §9.1, §9.4) ───────────────────


def confirmed(intent_id=1, side=Side.BUY, price="106250", registered_qty="342", rate="0.68"):
    return D5Terminal(
        intent_id=intent_id,
        side=side,
        price=Decimal(price),
        registered_qty=Decimal(registered_qty),
        state=D5TerminalState.EXECUTION_CONFIRMED,
        level_realized_rate=Decimal(rate),
        registered_seconds=872.0,  # 14m 32s
    )


def log_only_terminal(state, intent_id=3):
    return D5Terminal(
        intent_id=intent_id,
        side=Side.BUY,
        price=Decimal("61000"),
        registered_qty=Decimal("1200"),
        state=state,
        level_realized_rate=Decimal("0.1"),
        registered_seconds=100.0,
    )


def progress(intent_id=1, boundary="0.4", realized="138"):
    return D5Progress(
        intent_id=intent_id,
        side=Side.BUY,
        price=Decimal("106250"),
        registered_qty=Decimal("342"),
        boundary_pct=Decimal(boundary),
        realized_qty=Decimal(realized),
    )


def test_d5_confirmed_message_format():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender, clock=FakeClock(1783825200.0))
    assert dispatcher.dispatch(confirmed()) is True
    text = sender.sent[0]
    assert "🟢 매수 의도 실체결 확인 (케이스 1)" in text
    assert "의도 레벨: 106,250 (bid) · 표시 342 BTC" in text
    assert "체결: 232.6 BTC (실현률 68%)" in text
    assert "등록→확정: 14m 32s" in text
    assert "발생:" in text and "KST" in text


def test_d5_progress_message_format():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender)
    assert dispatcher.dispatch(progress()) is True
    text = sender.sent[0]
    assert "🔵 매수 의도 흡수 진행 40%" in text
    assert "의도 레벨: 106,250 (bid) · 표시 342 BTC" in text
    assert "체결 누적: 138 BTC (실현률 40% 경계 도달)" in text


def test_d5_log_only_terminal_states_are_never_sent():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender)
    for state in (
        D5TerminalState.CONFIRMED_CLOSED,  # (v1.9) 확정 래치 마감 — D1 REMOVED 알림이 대체
        D5TerminalState.PARTIALLY_EXECUTED,
        D5TerminalState.INTENT_WITHDRAWN,
        D5TerminalState.INTERRUPTED,
    ):
        assert dispatcher.dispatch(log_only_terminal(state)) is False
    assert sender.sent == []


def test_d5_progress_gated_by_config_flag():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d5_progress=False), sender)
    assert dispatcher.dispatch(progress()) is False


def test_d5_terminal_dedup_is_idempotent_not_cooldown():
    clock = FakeClock()
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender, clock=clock)
    assert dispatcher.dispatch(confirmed()) is True
    clock.now = 999999.0  # 시간이 아무리 지나도 같은 (intent_id, state)는 재전송 안 됨
    assert dispatcher.dispatch(confirmed()) is False
    assert len(sender.sent) == 1


def test_d5_progress_dedup_is_per_boundary():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender)
    assert dispatcher.dispatch(progress(boundary="0.2")) is True
    assert dispatcher.dispatch(progress(boundary="0.2")) is False  # 같은 경계 재발화 없음
    assert dispatcher.dispatch(progress(boundary="0.4")) is True  # 다른 경계는 발화


# ── D4 흡수 방어 (PRD §9.1, §9.2 v1.11) ──────────────────────


def d4_defense(kind=None, boundary=None, streak_started_at=1000.0):
    from order_monitor.detectors.d4 import D4Defense, D4DefenseKind

    return D4Defense(
        kind=kind or D4DefenseKind.DETECTED,
        side=Side.BUY,
        price=Decimal("62800"),
        base_qty=Decimal("180"),
        absorbed_visible=Decimal("250"),
        absorbed_hidden=Decimal("130.5"),
        absorbed_total=Decimal("380.5"),
        multiple=Decimal("380.5") / Decimal("180"),
        event_count=12,
        streak_started_at=streak_started_at,
        streak_seconds=312.0,  # 5m 12s
        boundary_multiple=Decimal(boundary) if boundary is not None else None,
    )


def test_d4_detected_message_format():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender)
    assert dispatcher.dispatch(d4_defense()) is True
    text = sender.sent[0]
    assert "🛡 레벨 흡수 방어 감지 (D4)" in text
    assert "레벨: 62,800 (bid) · 기준 180 BTC (스트릭 개시 표시크기)" in text
    assert "흡수 380.5 BTC = 가시 250 + 은닉 130.5 — 기준의 2.1배" in text
    assert "인정 이벤트 12건 · 스트릭 5m 12s" in text
    assert "관측 수치 통지" in text


def test_d4_progress_and_closed_titles():
    from order_monitor.detectors.d4 import D4DefenseKind

    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender)
    assert dispatcher.dispatch(d4_defense(kind=D4DefenseKind.PROGRESS, boundary="2.5")) is True
    assert dispatcher.dispatch(d4_defense(kind=D4DefenseKind.CLOSED)) is True
    assert "레벨 흡수 방어 진행 — 2.5× 경계 (D4)" in sender.sent[0]
    assert "레벨 흡수 방어 종결 (벽 소멸) (D4)" in sender.sent[1]


def test_d4_gated_by_send_d4_flag():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(send_d4=False), sender)
    assert dispatcher.dispatch(d4_defense()) is False
    assert sender.sent == []


def test_d4_interrupted_is_log_only():
    from order_monitor.detectors.d4 import D4DefenseKind

    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender)
    assert dispatcher.dispatch(d4_defense(kind=D4DefenseKind.INTERRUPTED)) is False
    assert sender.sent == []


def test_d4_dedup_is_idempotent_per_streak_and_boundary():
    from order_monitor.detectors.d4 import D4DefenseKind

    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender)
    assert dispatcher.dispatch(d4_defense()) is True
    assert dispatcher.dispatch(d4_defense()) is False  # 같은 스트릭 DETECTED 재전송 없음
    # 새 스트릭(재등록·epoch 재개시)은 식별자가 달라 다시 발송
    assert dispatcher.dispatch(d4_defense(streak_started_at=2000.0)) is True
    # 진행은 경계 단위 dedup
    assert dispatcher.dispatch(d4_defense(kind=D4DefenseKind.PROGRESS, boundary="2.5")) is True
    assert dispatcher.dispatch(d4_defense(kind=D4DefenseKind.PROGRESS, boundary="2.5")) is False
    assert dispatcher.dispatch(d4_defense(kind=D4DefenseKind.PROGRESS, boundary="3.0")) is True


def test_d5_confirmed_records_to_outbox_and_marks_sent_on_delivery(tmp_path):
    outbox = AlertsOutboxStore(tmp_path / "outbox.db")
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender, clock=FakeClock(1000.0), outbox=outbox)
    dispatcher.dispatch(confirmed())
    assert outbox.load_unsent() == [(1, sender.sent[0])]
    outbox.close()


def test_d5_confirmed_outbox_mark_sent_via_callback(tmp_path):
    outbox = AlertsOutboxStore(tmp_path / "outbox.db")
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender, clock=FakeClock(1000.0), outbox=outbox)
    dispatcher.dispatch(confirmed())
    sender.on_sent_callbacks[0]()  # 발송 성공 시뮬레이션
    assert outbox.load_unsent() == []
    outbox.close()


def test_d5_progress_does_not_use_outbox(tmp_path):
    outbox = AlertsOutboxStore(tmp_path / "outbox.db")
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender, outbox=outbox)
    dispatcher.dispatch(progress())
    assert outbox.count() == 0
    outbox.close()


# ── Telegram 발송기 (PRD §9.2, §11.1) ────────────────────────


class FakePost:
    """(status, body) 시퀀스를 재생하는 post 주입체. 마지막 항목이 예외면 raise."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    async def __call__(self, url, payload):
        self.calls.append((url, payload))
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def run_sender_until(sender, done):
    async def main():
        task = asyncio.create_task(sender.run())
        try:
            for _ in range(2000):
                if done():
                    break
                await asyncio.sleep(0)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    asyncio.run(main())


def make_sender(post, clock=None, sleeps=None):
    async def fake_sleep(seconds):
        if sleeps is not None:
            sleeps.append(seconds)

    return TelegramSender(
        "TOKEN123",
        "-1001234",
        monotonic=clock or FakeClock(),
        sleep=fake_sleep,
        post=post,
    )


def test_sender_posts_chat_id_and_text():
    post = FakePost([(200, "ok")])
    sender = make_sender(post)
    sender.enqueue("hello")
    run_sender_until(sender, lambda: len(post.calls) >= 1)
    url, payload = post.calls[0]
    assert url.endswith("/botTOKEN123/sendMessage")
    assert payload == {"chat_id": "-1001234", "text": "hello"}


def test_sender_retries_with_backoff_then_succeeds():
    post = FakePost([(502, "bad gateway"), OSError("boom"), (200, "ok")])
    sleeps = []
    sender = make_sender(post, sleeps=sleeps)
    sender.enqueue("msg")
    run_sender_until(sender, lambda: len(post.calls) >= 3)
    assert len(post.calls) == 3
    assert sleeps == [1.0, 2.0]  # 지수 백오프


def test_sender_drops_after_max_attempts(caplog):
    post = FakePost([(500, "err")])
    sender = make_sender(post, sleeps=[])
    sender.enqueue("msg1")
    sender.enqueue("msg2")
    # msg1이 5회 소진 후 드롭되고 msg2로 넘어간다 — 파이프라인 비블로킹
    run_sender_until(sender, lambda: len(post.calls) >= 6)
    assert post.calls[5][1]["text"] == "msg2"


def test_sender_throttles_one_message_per_second():
    post = FakePost([(200, "ok")])
    sleeps = []
    clock = FakeClock(100.0)  # 발송 사이에 시간이 흐르지 않는 것으로 고정
    sender = make_sender(post, clock=clock, sleeps=sleeps)
    sender.enqueue("a")
    sender.enqueue("b")
    run_sender_until(sender, lambda: len(post.calls) >= 2)
    assert sleeps == [1.0]  # 두 번째 발송 전 초당 상한 대기


def test_sender_redacts_token_in_failure_logs(caplog):
    post = FakePost([(404, "Not Found: botTOKEN123 invalid")])
    sender = make_sender(post)
    sender.enqueue("msg")
    with caplog.at_level("WARNING"):
        run_sender_until(sender, lambda: len(post.calls) >= 5)
    for record in caplog.records:
        assert "TOKEN123" not in getattr(record, "body", "")


def test_on_sent_called_once_on_success():
    post = FakePost([(200, "ok")])
    sender = make_sender(post)
    calls = []
    sender.enqueue("msg", on_sent=lambda: calls.append(1))
    run_sender_until(sender, lambda: len(post.calls) >= 1)
    assert calls == [1]


def test_on_sent_not_called_after_retries_exhausted():
    post = FakePost([(500, "err")])
    sender = make_sender(post, sleeps=[])
    calls = []
    sender.enqueue("msg", on_sent=lambda: calls.append(1))
    run_sender_until(sender, lambda: len(post.calls) >= 5)
    assert calls == []  # 소진 드롭 — outbox가 미발송으로 남아야 함


def test_enqueue_without_on_sent_still_works():
    post = FakePost([(200, "ok")])
    sender = make_sender(post)
    sender.enqueue("msg")  # on_sent 생략 — D1/D2 기존 경로
    run_sender_until(sender, lambda: len(post.calls) >= 1)
    assert post.calls[0][1]["text"] == "msg"
