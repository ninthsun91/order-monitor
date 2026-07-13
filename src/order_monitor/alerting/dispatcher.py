"""알림 정책 게이트 — on/off, dedup·쿨다운, 메시지 포맷 (PRD §9.1, §9.2).

- D1 전용 dedup (v1.3 범위 재분리): 키 `(detector, side, price_bucket)` +
  `ALERT_COOLDOWN`. D2는 시간 쿨다운 미적용 — 에피소드당 온셋/요약 각 1회가
  구조적으로 보장되고 병합 창이 재점화 스팸을 흡수 (PRD §9.2 v1.3).
  D5 종국/진행률은 intent 기반 순수 멱등 셋(시간 쿨다운 없음) — 아래 참고
- 쿨다운 시계는 monotonic (PRD §11.1). D5 메시지의 "발생 시각" 표기와 outbox
  기록 시각은 wall-clock(`clock` 생성자 인자, M4) — 재시작을 넘어 의미 있는
  실제 시각이어야 하므로 monotonic과 분리
- 발화 여부와 무관하게 디텍터 이벤트 로그는 service가 남긴다 — 여기는 발송만 판단
- 메시지는 한국어 고정 (PRD 오픈 퀘스천 #4 — M2에서 확정, DEVELOPMENT_PLAN 결정
  기록). 요약의 구간 시각은 KST 표기 (단일 사용자 전제)
- D5 종국(확정/추정) 알림은 outbox(M4, PRD §9.4)에 선기록 후 발송 — 크래시로
  발송 확인 전 죽어도 재시작 시 재전송된다. 로그 전용 종국 상태(부분체결/철회/
  만료/INTERRUPTED)와 진행률은 outbox 미사용(§9.4 범위 한정)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from order_monitor.config import AppConfig
from order_monitor.detectors.d1 import D1Appeared, D1Attribution, D1Removed
from order_monitor.detectors.d2 import D2BurstOnset, D2BurstSummary, D2Label, D2Verdict
from order_monitor.detectors.d5 import D5Progress, D5Terminal, D5TerminalState
from order_monitor.ingestion.events import Side
from order_monitor.ingestion.health import StreamStale
from order_monitor.persistence.alerts_outbox import AlertsOutboxStore

logger = logging.getLogger(__name__)

_KST = timezone(timedelta(hours=9))

_SIDE_LABEL = {Side.BUY: "bid", Side.SELL: "ask"}
_D5_INTENT_LABEL = {Side.BUY: "매수 의도", Side.SELL: "매도 의도"}
_D5_LOG_ONLY_STATES = frozenset(
    {
        D5TerminalState.PARTIALLY_EXECUTED,
        D5TerminalState.INTENT_WITHDRAWN,
        D5TerminalState.INTENT_EXPIRED,
        D5TerminalState.INTERRUPTED,
    }
)
_ATTRIBUTION_LABEL = {
    D1Attribution.FILLED: "체결 소진(FILLED)",
    D1Attribution.PULLED: "철회(PULLED)",
}
_D2_LABEL = {
    D2Label.DIRECTIONAL_BUY: "방향성 매수",
    D2Label.DIRECTIONAL_SELL: "방향성 매도",
    D2Label.MIXED: "혼합",
    D2Label.BALANCED: "양방향(흡수성 후보)",
}
_D2_VERDICT = {
    D2Verdict.BUY_FOLLOW: "매수 관철",
    D2Verdict.SELL_FOLLOW: "매도 관철",
    D2Verdict.BUY_ABSORBED_STALL: "매수 흡수 (정체)",
    D2Verdict.BUY_ABSORBED_RETRACE: "매수 흡수 (되돌림)",
    D2Verdict.SELL_ABSORBED_STALL: "매도 흡수 (정체)",
    D2Verdict.SELL_ABSORBED_RETRACE: "매도 흡수 (되돌림)",
    D2Verdict.TWO_WAY: "양방향 충돌",
    D2Verdict.MIXED: "혼합",
}


def _fmt(value: Decimal) -> str:
    """천 단위 구분 + 지수 표기 없는 고정소수점 (Decimal 전용)."""
    return format(value.normalize(), ",f")


def _fmt_approx(value: Decimal) -> str:
    """소수 1자리 반올림 표기 — 합산 수량처럼 자릿수가 긴 값용."""
    return format(value.quantize(Decimal("0.1")).normalize(), ",f")


def _delta_ratio_text(buy: Decimal, sell: Decimal) -> str:
    total = buy + sell
    ratio = abs(buy - sell) / total if total else Decimal(0)
    return f"델타비 {ratio:.2f}"


def _fmt_duration(seconds: float) -> str:
    """"등록→확정/추정" 소요시간 — "Nm 0Ns" (PRD §9.3 예시 "14m 32s"/"22m 05s")."""
    total = int(seconds)
    minutes, secs = divmod(total, 60)
    return f"{minutes}m {secs:02d}s"


class AlertDeduper:
    """키당 쿨다운 — 마지막 발송 시각 기록 (PRD §9.2 D1/D2 전용)."""

    def __init__(self, cooldown_seconds: float, monotonic: Callable[[], float]) -> None:
        self._cooldown_seconds = cooldown_seconds
        self._monotonic = monotonic
        self._last_sent: dict[tuple, float] = {}

    def should_send(self, key: tuple) -> bool:
        now = self._monotonic()
        last = self._last_sent.get(key)
        if last is not None and now - last < self._cooldown_seconds:
            return False
        self._last_sent[key] = now
        return True


class AlertDispatcher:
    def __init__(
        self,
        config: AppConfig,
        sender,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        clock: Callable[[], float] = time.time,
        outbox: AlertsOutboxStore | None = None,
    ) -> None:
        self._symbol = config.symbol
        self._alerts = config.alerts
        self._thresholds = config.thresholds
        self._bucket_size = Decimal(str(config.alerts.bucket_size_usdt))
        self._deduper = AlertDeduper(config.alerts.cooldown_seconds, monotonic)
        self._sender = sender
        self._clock = clock
        self._outbox = outbox
        # D5 종국/진행률 dedup — 시간 쿨다운이 아닌 순수 멱등(재전송 안전용, PRD §9.2)
        self._d5_sent: set[tuple] = set()

    def set_outbox(self, outbox: AlertsOutboxStore) -> None:
        """outbox는 db_path 확정 후 `startup()`에서 열리므로 생성자 이후 별도 주입."""
        self._outbox = outbox

    def dispatch(self, event: object) -> bool:
        """발송 큐 투입 여부를 반환한다. 미대상 이벤트(D1Suppressed 등)는 조용히 무시."""
        if isinstance(event, D1Appeared):
            if not self._alerts.send_d1:
                return False
            key = ("d1", event.side.value, self._bucket(event.price))
            text = self._format_d1_appeared(event)
        elif isinstance(event, D1Removed):
            if not self._alerts.send_d1:
                return False
            key = ("d1", event.side.value, self._bucket(event.price))
            text = self._format_d1_removed(event)
        elif isinstance(event, D2BurstOnset):
            # D2는 시간 쿨다운 미적용 — 에피소드 모델이 억제 (PRD §9.2 v1.3)
            if not self._alerts.send_d2:
                return False
            self._sender.enqueue(self._format_d2_onset(event))
            return True
        elif isinstance(event, D2BurstSummary):
            if not self._alerts.send_d2_summary:
                return False
            self._sender.enqueue(self._format_d2_summary(event))
            return True
        elif isinstance(event, StreamStale):
            # 워치독 알림은 on/off 없이 상시 — 조용한 실패가 최우선 리스크 (PRD §11.1).
            # 쿨다운은 재연결 플랩 시 스트림당 반복 발송 억제용
            key = ("watchdog", "feed_stale", event.stream)
            text = self._format_feed_stale(event)
        elif isinstance(event, D5Terminal):
            return self._dispatch_d5_terminal(event)
        elif isinstance(event, D5Progress):
            return self._dispatch_d5_progress(event)
        else:
            return False

        if not self._deduper.should_send(key):
            logger.info("alert suppressed by cooldown", extra={"dedup_key": repr(key)})
            return False
        self._sender.enqueue(text)
        return True

    def _bucket(self, price: Decimal) -> int:
        return int(price / self._bucket_size)

    def _dispatch_d5_terminal(self, event: D5Terminal) -> bool:
        if event.state in _D5_LOG_ONLY_STATES:
            return False  # 로그 전용 종국 상태 (PRD §9.1)
        key = ("d5", event.intent_id, event.state.value)
        if key in self._d5_sent:
            return False  # 순수 멱등 — 쿨다운 아님 (PRD §9.2)
        self._d5_sent.add(key)

        recorded_at = self._clock()
        formatter = (
            self._format_d5_confirmed
            if event.state is D5TerminalState.EXECUTION_CONFIRMED
            else self._format_d5_inferred_above
        )
        text = formatter(event, recorded_at)

        if self._outbox is not None:
            rowid = self._outbox.record(event.side, event.price, event.state.value, text, recorded_at)
            if rowid is not None:
                self._sender.enqueue(text, on_sent=lambda rid=rowid: self._outbox.mark_sent(rid))
                return True
        self._sender.enqueue(text)
        return True

    def _dispatch_d5_progress(self, event: D5Progress) -> bool:
        if not self._alerts.send_d5_progress:
            return False
        key = ("d5progress", event.intent_id, event.series, str(event.boundary_pct))
        if key in self._d5_sent:
            return False
        self._d5_sent.add(key)
        self._sender.enqueue(self._format_d5_progress(event))
        return True

    def _d5_intent_line(self, event: D5Terminal | D5Progress) -> str:
        return (
            f"의도 레벨: {_fmt(event.price)} ({_SIDE_LABEL[event.side]}) · "
            f"표시 {_fmt(event.registered_qty)} BTC"
        )

    def _format_d5_confirmed(self, event: D5Terminal, recorded_at: float) -> str:
        traded_qty = event.registered_qty * event.level_realized_rate
        occurred = datetime.fromtimestamp(recorded_at, _KST)
        return (
            f"🟢 {_D5_INTENT_LABEL[event.side]} 실체결 확인 (케이스 1)\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"{self._d5_intent_line(event)}\n"
            f"체결: {_fmt_approx(traded_qty)} BTC (실현률 {event.level_realized_rate * 100:.0f}%)\n"
            f"등록→확정: {_fmt_duration(event.registered_seconds)}\n"
            f"발생: {occurred:%H:%M:%S} KST"
        )

    def _format_d5_inferred_above(self, event: D5Terminal, recorded_at: float) -> str:
        above_qty = event.registered_qty * event.above_realized_rate
        occurred = datetime.fromtimestamp(recorded_at, _KST)
        return (
            f"🟡 {_D5_INTENT_LABEL[event.side]} 상위 체결 추정 (케이스 2)\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"{self._d5_intent_line(event)}\n"
            f"상위 구간 리필 확인 체결: {_fmt_approx(above_qty)} BTC "
            f"(추정 실현률 {event.above_realized_rate * 100:.0f}%)\n"
            f"등록→추정: {_fmt_duration(event.registered_seconds)}\n"
            f"발생: {occurred:%H:%M:%S} KST\n"
            f"※ 추정 등급 — 체결 주체 귀속 불가 (공개 데이터 한계)"
        )

    def _format_d5_progress(self, event: D5Progress) -> str:
        series_label = "케이스 1 계열" if event.series == "case1" else "케이스 2 계열"
        pct = event.boundary_pct * 100
        return (
            f"🔵 {_D5_INTENT_LABEL[event.side]} 흡수 진행 {pct:.0f}% ({series_label})\n"
            f"{self._d5_intent_line(event)}\n"
            f"체결 누적: {_fmt_approx(event.realized_qty)} BTC (실현률 {pct:.0f}% 경계 도달)"
        )

    def _format_d1_appeared(self, event: D1Appeared) -> str:
        return (
            f"🧱 대형 벽 출현 (D1)\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"레벨: {_fmt(event.price)} ({_SIDE_LABEL[event.side]}) · 표시 {_fmt(event.qty)} BTC\n"
            f"지속 {event.persisted_seconds:.0f}s 확인 (임계 {self._thresholds.persist_seconds:g}s)"
        )

    def _format_d1_removed(self, event: D1Removed) -> str:
        return (
            f"🧱 대형 벽 소멸 — {_ATTRIBUTION_LABEL[event.attribution]} (D1)\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"레벨: {_fmt(event.price)} ({_SIDE_LABEL[event.side]}) · "
            f"피크 {_fmt(event.peak_qty)} BTC → 잔량 {_fmt(event.last_qty)} BTC\n"
            f"레벨 체결 누적: {_fmt(event.cum_traded)} BTC"
        )

    def _format_feed_stale(self, event: StreamStale) -> str:
        return (
            f"🛑 피드 정지 (FEED_STALE)\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"스트림: {event.stream} — {event.silent_seconds:.0f}초간 수신 없음\n"
            f"판정 중단(epoch 종료) — 수신 재개 시 자동 복귀"
        )

    def _format_d2_onset(self, event: D2BurstOnset) -> str:
        delta = event.buy_qty - event.sell_qty
        return (
            f"⚡ 볼륨 버스트 시작 (D2)\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"{self._thresholds.window_seconds:g}초 체결 {_fmt_approx(event.window_qty)} BTC "
            f"(매수 {_fmt_approx(event.buy_qty)} / 매도 {_fmt_approx(event.sell_qty)} · Δ {delta:+.1f})\n"
            f"기준선: 분당 {_fmt_approx(event.baseline_per_minute)} BTC "
            f"({self._thresholds.vol_baseline_hours:g}h 평균 체결량)\n"
            f"성격: {_D2_LABEL[event.label]} ({_delta_ratio_text(event.buy_qty, event.sell_qty)}) · "
            f"현재가 {_fmt(event.price)}"
        )

    def _format_d2_summary(self, event: D2BurstSummary) -> str:
        start = datetime.fromtimestamp(event.start_exchange_ms / 1000, _KST)
        end = datetime.fromtimestamp(event.end_exchange_ms / 1000, _KST)
        duration_min = max(1, round((event.end_exchange_ms - event.start_exchange_ms) / 60_000))
        # 표시된 구간 분수와 같은 값으로 환산 — 메시지 안에서 자체 검산되도록
        multiple = event.total_qty / (event.baseline_per_minute * duration_min)
        delta = event.buy_qty - event.sell_qty
        change_pct = (event.close_price - event.open_price) / event.open_price * 100
        # 판정 줄의 %는 에피소드 종가 → 요약 확정 시점(종료 +병합 창) 가격
        follow_pct = (event.finalize_price - event.close_price) / event.close_price * 100
        return (
            f"⚡ 볼륨 버스트 요약 (D2) — {duration_min}분 "
            f"(KST {start:%H:%M}~{end:%H:%M})\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"누적 {_fmt_approx(event.total_qty)} BTC "
            f"(매수 {_fmt_approx(event.buy_qty)} / 매도 {_fmt_approx(event.sell_qty)} · Δ {delta:+.1f}) — "
            f"평상시 {duration_min}분치의 {multiple:.1f}배\n"
            f"성격: {_D2_LABEL[event.label]} ({_delta_ratio_text(event.buy_qty, event.sell_qty)})\n"
            f"판정: {_D2_VERDICT[event.verdict]} — 요약 시점 {_fmt(event.finalize_price)} "
            f"({follow_pct:+.2f}%)\n"
            f"가격: {_fmt(event.open_price)} → {_fmt(event.close_price)} ({change_pct:+.2f}%) · "
            f"고 {_fmt(event.high_price)} / 저 {_fmt(event.low_price)}"
        )
