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
from order_monitor.ingestion.events import Side

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


class FakeSender:
    def __init__(self):
        self.sent = []

    def enqueue(self, text):
        self.sent.append(text)


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


def test_send_d1_off_by_default_suppresses_d1_only():
    sender = FakeSender()
    dispatcher = AlertDispatcher(make_config(), sender, monotonic=FakeClock())
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
