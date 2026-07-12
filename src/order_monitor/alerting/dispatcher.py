"""알림 정책 게이트 — on/off, dedup·쿨다운, 메시지 포맷 (PRD §9.1, §9.2).

- D1/D2 전용 dedup (v1.2 범위 분리): 키 `(detector, side, price_bucket)` +
  `ALERT_COOLDOWN`. D2는 레벨 가격이 없으므로 버킷 없음(None) — 방향당 쿨다운.
  D5 종국/진행률 알림은 intent 기반 별도 키로 M4에서 구현
- 쿨다운 시계는 monotonic (PRD §11.1)
- 발화 여부와 무관하게 디텍터 이벤트 로그는 service가 남긴다 — 여기는 발송만 판단
- 메시지는 한국어 고정 (PRD 오픈 퀘스천 #4 — M2에서 확정, DEVELOPMENT_PLAN 결정 기록)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from decimal import Decimal

from order_monitor.config import AppConfig
from order_monitor.detectors.d1 import D1Appeared, D1Attribution, D1Removed
from order_monitor.detectors.d2 import D2Burst
from order_monitor.ingestion.events import Side

logger = logging.getLogger(__name__)

_SIDE_LABEL = {Side.BUY: "bid", Side.SELL: "ask"}
_AGGRESSOR_LABEL = {Side.BUY: "매수", Side.SELL: "매도"}
_ATTRIBUTION_LABEL = {
    D1Attribution.FILLED: "체결 소진(FILLED)",
    D1Attribution.PULLED: "철회(PULLED)",
}


def _fmt(value: Decimal) -> str:
    """천 단위 구분 + 지수 표기 없는 고정소수점 (Decimal 전용)."""
    return format(value.normalize(), ",f")


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
    ) -> None:
        self._symbol = config.symbol
        self._alerts = config.alerts
        self._thresholds = config.thresholds
        self._bucket_size = Decimal(str(config.alerts.bucket_size_usdt))
        self._deduper = AlertDeduper(config.alerts.cooldown_seconds, monotonic)
        self._sender = sender

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
        elif isinstance(event, D2Burst):
            if not self._alerts.send_d2:
                return False
            key = ("d2", event.aggressor_side.value, None)
            text = self._format_d2(event)
        else:
            return False

        if not self._deduper.should_send(key):
            logger.info("alert suppressed by cooldown", extra={"dedup_key": repr(key)})
            return False
        self._sender.enqueue(text)
        return True

    def _bucket(self, price: Decimal) -> int:
        return int(price / self._bucket_size)

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

    def _format_d2(self, event: D2Burst) -> str:
        return (
            f"⚡ 볼륨 버스트 — {_AGGRESSOR_LABEL[event.aggressor_side]} aggressor (D2)\n"
            f"심볼: {self._symbol} (Binance Spot)\n"
            f"최근 {self._thresholds.window_seconds:g}초 체결 합계 {_fmt(event.sum_qty)} BTC "
            f"(임계 {self._thresholds.vol_threshold_btc:g} BTC)"
        )
