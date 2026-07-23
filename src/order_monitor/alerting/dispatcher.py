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
- D5 종국(확정) 알림은 outbox(M4, PRD §9.4)에 선기록 후 발송 — 크래시로
  발송 확인 전 죽어도 재시작 시 재전송된다. 로그 전용 종국 상태(부분체결/철회/
  만료/INTERRUPTED)와 진행률은 outbox 미사용(§9.4 범위 한정)
- D4 흡수 방어(v1.11): `send_d4` 게이트, 순수 멱등 dedup(스트릭·종류·경계배수),
  outbox 미적용. DEFENSE_INTERRUPTED는 로그만
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
from order_monitor.detectors.d4 import D4Defense, D4DefenseKind
from order_monitor.detectors.d5 import D5Progress, D5Terminal, D5TerminalState
from order_monitor.detectors.watch_level import (
    FINAL_MANUAL,
    FINAL_RESISTANCE_BROKEN,
    FINAL_SUPPORT_BROKEN,
    ROLE_RESISTANCE,
    ROLE_SUPPORT,
    WatchFinal,
    WatchFirstContact,
    WatchPeriodicReport,
    WatchReportData,
)
from order_monitor.ingestion.events import Side
from order_monitor.ingestion.health import StreamStale
from order_monitor.persistence.alerts_outbox import AlertsOutboxStore
from order_monitor.persistence.watch_levels import WatchStore
from order_monitor.state.wall_registry import Wall

logger = logging.getLogger(__name__)

_KST = timezone(timedelta(hours=9))

_SIDE_LABEL = {Side.BUY: "bid", Side.SELL: "ask"}
_D5_INTENT_LABEL = {Side.BUY: "매수 의도", Side.SELL: "매도 의도"}
_D5_LOG_ONLY_STATES = frozenset(
    {
        D5TerminalState.CONFIRMED_CLOSED,
        D5TerminalState.PARTIALLY_EXECUTED,
        D5TerminalState.INTENT_WITHDRAWN,
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


def _fmt_span(seconds: float) -> str:
    """수 시간 단위 경과 표기 — "1h 42m" (W 주시는 수십 시간 지속, PRD §9.3 v1.13)."""
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    if hours:
        return f"{hours}h {rem // 60:02d}m"
    return _fmt_duration(seconds)


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
        d1_streak_gate: Callable[[D1Appeared], bool] | None = None,
        wall_lookup: Callable[[], list[Wall]] | None = None,
    ) -> None:
        self._symbol = config.symbol
        self._alerts = config.alerts
        self._thresholds = config.thresholds
        self._bucket_size = Decimal(str(config.alerts.bucket_size_usdt))
        self._deduper = AlertDeduper(config.alerts.cooldown_seconds, monotonic)
        self._sender = sender
        self._clock = clock
        self._outbox = outbox
        # APPEARED 스트릭당 1회 게이트 (PRD §8 D1 v1.8) — service가 벽 레지스트리
        # 기반 판정·마킹을 주입. None이면 게이트 없음 (기존 동작)
        self._d1_streak_gate = d1_streak_gate
        # D2 요약 근접 벽 컨텍스트용 벽 레지스트리 스냅샷 조회 (M6 관측 보완,
        # 결정 기록 2026-07-23). None이면 요약에 근접 벽 줄 없음
        self._wall_lookup = wall_lookup
        # D5 종국/진행률 dedup — 시간 쿨다운이 아닌 순수 멱등(재전송 안전용, PRD §9.2)
        self._d5_sent: set[tuple] = set()
        # D4 흡수 방어 dedup — 동일 원리의 순수 멱등 셋 (PRD §9.2 v1.11)
        self._d4_sent: set[tuple] = set()
        # W 주시 config (밴드 폭 — 구역 내 벽 필터 하한 fallback, PRD §9.3 v1.13)
        self._watch_cfg = config.watch
        self._watch_store: WatchStore | None = None

    def set_outbox(self, outbox: AlertsOutboxStore) -> None:
        """outbox는 db_path 확정 후 `startup()`에서 열리므로 생성자 이후 별도 주입."""
        self._outbox = outbox

    def set_watch_store(self, store: WatchStore) -> None:
        """W 최종 리포트 발송 보장용 (§9.4 v1.13) — outbox와 같은 지연 주입 관례."""
        self._watch_store = store

    def dispatch(self, event: object) -> bool:
        """발송 큐 투입 여부를 반환한다. 미대상 이벤트(D1Suppressed 등)는 조용히 무시."""
        if isinstance(event, D1Appeared):
            if not self._alerts.send_d1:
                return False
            key = ("d1", event.side.value, self._bucket(event.price))
            if not self._deduper.should_send(key):
                logger.info("alert suppressed by cooldown", extra={"dedup_key": repr(key)})
                return False
            # 스트릭당 1회 게이트는 쿨다운 뒤에 평가 — 쿨다운에 눌린 발화가
            # 스트릭을 소모(마킹)하지 않게 한다 (마킹은 실제 발송 시에만)
            if self._d1_streak_gate is not None and not self._d1_streak_gate(event):
                logger.info(
                    "d1 appeared alert suppressed — streak already announced",
                    extra={"side": event.side.value, "price": str(event.price)},
                )
                return False
            self._sender.enqueue(self._format_d1_appeared(event))
            return True
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
        elif isinstance(event, D4Defense):
            return self._dispatch_d4(event)
        elif isinstance(event, (WatchFirstContact, WatchPeriodicReport, WatchFinal)):
            return self._dispatch_watch(event)
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
        text = self._format_d5_confirmed(event, recorded_at)

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
        key = ("d5progress", event.intent_id, str(event.boundary_pct))
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

    def _format_d5_progress(self, event: D5Progress) -> str:
        pct = event.boundary_pct * 100
        return (
            f"🔵 {_D5_INTENT_LABEL[event.side]} 흡수 진행 {pct:.0f}%\n"
            f"{self._d5_intent_line(event)}\n"
            f"체결 누적: {_fmt_approx(event.realized_qty)} BTC (실현률 {pct:.0f}% 경계 도달)"
        )

    # ── D4 흡수 방어 (PRD §9.1/§9.2 v1.11) ─────────────────────

    def _dispatch_d4(self, event: D4Defense) -> bool:
        if event.kind is D4DefenseKind.INTERRUPTED:
            return False  # epoch 종료 — 로그만 (PRD §9.1)
        if not self._alerts.send_d4:
            return False
        # dedup 키 (price, side, 스트릭, 이벤트 종류/경계배수) — 시간 쿨다운 미적용:
        # 감지·종결은 스트릭당 1회가 구조적 보장(래치), 진행은 경계 중복만 차단 (PRD §9.2)
        key = (
            "d4",
            event.side.value,
            str(event.price),
            event.streak_started_at,
            event.kind.value,
            str(event.boundary_multiple),
        )
        if key in self._d4_sent:
            return False
        self._d4_sent.add(key)
        self._sender.enqueue(self._format_d4(event))  # outbox 미적용 (PRD §9.4)
        return True

    def _format_d4(self, event: D4Defense) -> str:
        title = {
            D4DefenseKind.DETECTED: "레벨 흡수 방어 감지",
            D4DefenseKind.PROGRESS: f"레벨 흡수 방어 진행 — {event.boundary_multiple:.1f}× 경계"
            if event.boundary_multiple is not None
            else "레벨 흡수 방어 진행",
            D4DefenseKind.CLOSED: "레벨 흡수 방어 종결 (벽 소멸)",
        }[event.kind]
        return (
            f"🛡 {title} (D4)\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"레벨: {_fmt(event.price)} ({_SIDE_LABEL[event.side]}) · "
            f"기준 {_fmt(event.base_qty)} BTC (스트릭 개시 표시크기)\n"
            f"흡수 {_fmt_approx(event.absorbed_total)} BTC = "
            f"가시 {_fmt_approx(event.absorbed_visible)} + 은닉 {_fmt_approx(event.absorbed_hidden)} "
            f"— 기준의 {event.multiple:.1f}배\n"
            f"인정 이벤트 {event.event_count}건 · 스트릭 {_fmt_duration(event.streak_seconds)}\n"
            f"※ 관측 수치 통지 — 주체·의도 해석 없음 (PRD §8 D4)"
        )

    # ── W 주시 리포트 (PRD §9.1–§9.4 v1.13) ─────────────────────

    def _dispatch_watch(self, event: WatchFirstContact | WatchPeriodicReport | WatchFinal) -> bool:
        """항상 발송 — 등록 자체가 발송 의사, on/off 키·dedup·쿨다운 없음 (§9.1/§9.2).
        자체 케이던스(리포트 간격 + 활동 게이팅)가 억제 메커니즘."""
        if isinstance(event, WatchFinal):
            text = self._format_watch(event.data, final_reason=event.reason)
            if self._watch_store is not None and event.reason != FINAL_MANUAL:
                # 선기록 → 발송 확인 시 행 삭제 (§9.4 — 수동 해소는 행이 이미
                # 삭제돼 있어 보장 불필요, 자동 무효화만 크래시 재전송 대상)
                self._watch_store.mark_final(event.data.lo, event.data.hi, event.reason, text)
                store = self._watch_store
                self._sender.enqueue(
                    text,
                    on_sent=lambda lo=event.data.lo, hi=event.data.hi: store.delete(lo, hi),
                )
                return True
            self._sender.enqueue(text)
            return True
        text = self._format_watch(event.data, first=isinstance(event, WatchFirstContact))
        self._sender.enqueue(text)
        return True

    def format_watch_status(self, datas: list[WatchReportData]) -> str:
        """`/watching` 온디맨드 현황 (§9.5) — 수신 응답 경로에서 사용."""
        if not datas:
            return "👁 주시 중인 레벨이 없습니다. (/watch <가격> 으로 등록)"
        lines = ["👁 주시 현황"]
        for data in datas:
            role = self._watch_role_label(data.role)
            detail = f"{role}"
            if data.first_contact_at is not None:
                detail += (
                    f" {data.episode_num}회차 · "
                    f"누적 매도 {_fmt_approx(data.cum_sell)} / 매수 {_fmt_approx(data.cum_buy)} BTC"
                )
            lines.append(f"- {self._watch_zone_text(data)}: {detail}")
        return "\n".join(lines)

    def _watch_zone_text(self, data: WatchReportData) -> str:
        if data.lo == data.hi:
            return _fmt(data.lo)
        return f"{_fmt(data.lo)}~{_fmt(data.hi)}"

    def _watch_role_label(self, role: str | None) -> str:
        return {ROLE_SUPPORT: "지지 테스트", ROLE_RESISTANCE: "저항 테스트"}.get(role, "대기")

    def _format_watch(
        self,
        data: WatchReportData,
        *,
        first: bool = False,
        final_reason: str | None = None,
    ) -> str:
        if final_reason == FINAL_SUPPORT_BROKEN:
            status = "지지 이탈 확정 — 주시 종료"
        elif final_reason == FINAL_RESISTANCE_BROKEN:
            status = "저항 돌파 확정 — 주시 종료"
        elif final_reason == FINAL_MANUAL:
            status = "주시 해소 (수동)"
        elif first:
            status = f"{self._watch_role_label(data.role)} 시작"
        else:
            status = f"{self._watch_role_label(data.role)} 중"

        if data.first_contact_at is not None:
            if first:
                contact = f" ({data.episode_num}회차, 첫 접촉)"
            else:
                elapsed = _fmt_span(self._clock() - data.first_contact_at)
                contact = f" ({data.episode_num}회차, 첫 접촉 후 {elapsed})"
        else:
            contact = " (접촉 없음)"

        lines = [f"👁 주시 {self._watch_zone_text(data)} — {status}{contact}"]

        price_line = self._watch_price_line(data)
        if price_line is not None:
            lines.append(price_line)
        if final_reason is None:
            interval_min = max(1, round(data.interval_seconds / 60))
            lines.append(
                f"이번 {interval_min}분: 매도 {_fmt_approx(data.interval_sell)} / "
                f"매수 {_fmt_approx(data.interval_buy)} BTC"
            )
        lines.append(
            f"누적: 매도 {_fmt_approx(data.cum_sell)} / 매수 {_fmt_approx(data.cum_buy)} BTC"
        )

        tail_parts = []
        if final_reason is None and data.role is not None:
            tail_parts.append(f"{data.timeframe} 이탈 마감 {data.breach_closes}/{data.confirm_closes}")
        zone_walls = self._watch_zone_wall_line(data)
        if zone_walls is not None:
            tail_parts.append(zone_walls)
        if tail_parts:
            lines.append(" · ".join(tail_parts))
        if data.gap_flag:
            lines.append("※ 관측 공백 포함 (epoch/재시작)")
        return "\n".join(lines)

    def _watch_price_line(self, data: WatchReportData) -> str | None:
        if data.last_price is None:
            return None
        # 거리 기준가 = 테스트받는 구역 경계 (지지: hi, 저항: lo — 폭 0이면 동일)
        ref = data.hi if data.role == ROLE_SUPPORT else data.lo
        dist_pct = (data.last_price - ref) / ref * 100
        line = f"현재가 {_fmt(data.last_price)} ({dist_pct:+.2f}%)"
        if data.role == ROLE_SUPPORT and data.excursion_low is not None:
            low_pct = (data.excursion_low - ref) / ref * 100
            line += f" · 저점 {_fmt(data.excursion_low)} ({low_pct:+.2f}%)"
        elif data.role == ROLE_RESISTANCE and data.excursion_high is not None:
            high_pct = (data.excursion_high - ref) / ref * 100
            line += f" · 고점 {_fmt(data.excursion_high)} ({high_pct:+.2f}%)"
        return line

    def _watch_zone_wall_line(self, data: WatchReportData) -> str | None:
        """계측 영향권(§8 W — 구역 ∪ excursion 확장) 내 레지스트리 벽, 구역
        경계 근접순 2개 캡. 기존 `_nearby_wall_line`(전역 최근접 1개)과 달리
        주시 구역으로 필터한다."""
        if self._wall_lookup is None:
            return None
        if data.role == ROLE_SUPPORT:
            lower = data.excursion_low if data.excursion_low is not None else data.lo
            bounds = (min(lower, data.lo), data.hi)
            edge = data.hi
        elif data.role == ROLE_RESISTANCE:
            upper = data.excursion_high if data.excursion_high is not None else data.hi
            bounds = (data.lo, max(upper, data.hi))
            edge = data.lo
        else:
            bounds = (data.lo, data.hi)
            edge = data.lo
        walls = [w for w in self._wall_lookup() if bounds[0] <= w.price <= bounds[1]]
        if not walls:
            return None
        walls.sort(key=lambda w: abs(w.price - edge))
        parts = [
            f"{_fmt(w.price)} {_SIDE_LABEL[w.side]} {_fmt_approx(w.last_qty)} BTC"
            for w in walls[:2]
        ]
        return "구역 내 벽: " + " · ".join(parts)

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

    def _nearby_wall_line(self, close_price: Decimal) -> str | None:
        """에피소드 종가 기준 bid/ask 각 최근접 추적 벽 1개 — `sell_absorbed_stall` 류
        흡수 판정의 해석 근거 (결정 기록 2026-07-23: 거리 제한·config 키 없음)."""
        if self._wall_lookup is None:
            return None
        parts = []
        walls = self._wall_lookup()
        for side in (Side.BUY, Side.SELL):
            candidates = [w for w in walls if w.side is side]
            if not candidates:
                continue
            nearest = min(candidates, key=lambda w: abs(w.price - close_price))
            parts.append(
                f"{_SIDE_LABEL[side]} {_fmt(nearest.price)} "
                f"(잔량 {_fmt_approx(nearest.last_qty)} / 피크 {_fmt_approx(nearest.peak_qty)} BTC)"
            )
        return "근접 벽: " + " · ".join(parts) if parts else None

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
        text = (
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
        nearby = self._nearby_wall_line(event.close_price)
        if nearby is not None:
            text += f"\n{nearby}"
        return text
