"""W — 주시 레벨 관측기 (PRD §8 W v1.13). 디텍터가 아니다.

임계 판정 없이, 사용자가 텔레그램 명령(§9.5)으로 등록한 가격 구역의 체결을
계측해 주기 보고만 한다 — v1.11 원칙("관측은 기계가, 의도 해석은 수신자가")의
직접 적용. D5에 공급하지 않고 다른 디텍터와 상호작용하지 않는다.

- **접촉 밴드와 계측 범위의 분리**: 밴드 `[lo×(1−w), hi×(1+w)]`는 회차(episode)
  경계·리포트 게이팅 전용. 계측은 단방향 무제한 — 지지 테스트 중 `price ≤ hi`
  전 체결(taker 방향 분리), 저항은 대칭. 누적·excursion은 회차를 관통하는
  **주시 생애 기준**.
- **역할 판정**: 회차 시작(밴드 진입) 시점 체결가 vs 구역 — 위→지지, 아래→저항.
  진입가가 구역 내부면 직전에 관측된 구역 외 위치(`prev_zone_rel`)로 판정하고,
  그것도 없으면(구역 내부 등록 직후 등) 역할 미정 "대기" 유지 — 첫 구역 이탈
  후의 재진입부터 회차가 개시된다 (§8 W).
- **무효화**: 역할 반대편 봉 마감(몸통) 연속 `confirm_closes`회 + 마감가 버퍼.
  gap 오염 봉은 판정 제외 + 연속 카운트 리셋 (§5.4 v1.13 — 계측은 적재로서
  계속되고 판정만 보류). 확정 시 WatchFinal 반환 후 관측 종료 — 행 삭제는
  발송 확인 후 (§9.4, dispatcher/store 소관).
- **시계**: 봉 판정은 CandleAssembler(exchange_time), 리포트 due는 monotonic,
  표시·영속 시각(등록/첫 접촉)은 wall-clock — 재시작 후에도 "첫 접촉 후 1h"
  표시가 성립해야 하므로 (wall_registry 시각 필드와 같은 예외, 결정 기록
  2026-07-11 선례).
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable
from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, Side
from order_monitor.state.candles import Candle

ROLE_SUPPORT = "support"
ROLE_RESISTANCE = "resistance"

FINAL_SUPPORT_BROKEN = "support_broken"
FINAL_RESISTANCE_BROKEN = "resistance_broken"
FINAL_MANUAL = "manual"

_Key = tuple[Decimal, Decimal]


@dataclasses.dataclass(frozen=True)
class WatchReportData:
    lo: Decimal
    hi: Decimal
    literal: str
    role: str | None  # None = 대기 (역할 미정)
    episode_num: int
    in_band: bool
    registered_at: float  # wall-clock
    first_contact_at: float | None  # wall-clock
    last_price: Decimal | None
    excursion_low: Decimal | None
    excursion_high: Decimal | None
    interval_buy: Decimal  # 직전 리포트 이후
    interval_sell: Decimal
    cum_buy: Decimal  # 주시 생애
    cum_sell: Decimal
    interval_seconds: float  # 직전 리포트 이후 경과 (monotonic)
    breach_closes: int  # 이탈 마감 연속 카운트
    confirm_closes: int
    timeframe: str
    gap_flag: bool  # 직전 리포트 이후 epoch/재시작 공백 존재


@dataclasses.dataclass(frozen=True)
class WatchFirstContact:
    data: WatchReportData


@dataclasses.dataclass(frozen=True)
class WatchPeriodicReport:
    data: WatchReportData


@dataclasses.dataclass(frozen=True)
class WatchFinal:
    data: WatchReportData
    reason: str  # support_broken | resistance_broken | manual


WatchEvent = WatchFirstContact | WatchPeriodicReport | WatchFinal


@dataclasses.dataclass
class _Watch:
    lo: Decimal
    hi: Decimal
    literal: str
    registered_at: float  # wall-clock
    role: str | None = None
    prev_zone_rel: str | None = None  # "above" | "below" — 최근 구역 외 위치
    episode_num: int = 0
    in_band: bool = False
    first_contact_at: float | None = None  # wall-clock
    last_price: Decimal | None = None
    cum_buy: Decimal = Decimal(0)
    cum_sell: Decimal = Decimal(0)
    excursion_low: Decimal | None = None
    excursion_high: Decimal | None = None
    report_buy: Decimal = Decimal(0)  # 직전 리포트 시점 누적 스냅샷
    report_sell: Decimal = Decimal(0)
    last_report_monotonic: float = 0.0
    breach_closes: int = 0
    gap_flag: bool = False


class WatchLevelObserver:
    def __init__(
        self,
        *,
        contact_band_pct: Decimal,
        confirm_timeframe: str,
        confirm_closes: int,
        invalidate_buffer_pct: Decimal,
        report_interval_seconds: float,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._band_pct = contact_band_pct
        self._timeframe = confirm_timeframe
        self._confirm_closes = confirm_closes
        self._buffer_pct = invalidate_buffer_pct
        self._report_interval = report_interval_seconds
        self._clock = clock
        self._monotonic = monotonic
        self._watches: dict[_Key, _Watch] = {}
        self._flush_pending = False  # 회차 경계 등 상태 전이 발생 — service의 영속 flush 신호

    # ---- 등록/해소/복원 -------------------------------------------------

    def register(self, lo: Decimal, hi: Decimal, literal: str) -> bool:
        """신규 등록. 이미 같은 구역이 있으면 False."""
        key = (lo, hi)
        if key in self._watches:
            return False
        self._watches[key] = _Watch(
            lo=lo,
            hi=hi,
            literal=literal,
            registered_at=self._clock(),
            last_report_monotonic=self._monotonic(),
        )
        self._flush_pending = True
        return True

    def unregister(self, lo: Decimal, hi: Decimal) -> WatchFinal | None:
        """수동 해소 — 최종 리포트 이벤트 반환 (§8 W 종료 사유 병기)."""
        watch = self._watches.pop((lo, hi), None)
        if watch is None:
            return None
        self._flush_pending = True
        return WatchFinal(data=self._snapshot(watch), reason=FINAL_MANUAL)

    def restore(
        self,
        *,
        lo: Decimal,
        hi: Decimal,
        literal: str,
        registered_at: float,
        role: str | None,
        episode_num: int,
        first_contact_at: float | None,
        cum_buy: Decimal,
        cum_sell: Decimal,
        excursion_low: Decimal | None,
        excursion_high: Decimal | None,
    ) -> None:
        """재시작 복원 (§12.2) — 회차 단절(in_band=False), 이탈 마감 카운트
        리셋, 관측 공백 플래그 셋. 누적·excursion·첫 접촉 시각은 보존."""
        self._watches[(lo, hi)] = _Watch(
            lo=lo,
            hi=hi,
            literal=literal,
            registered_at=registered_at,
            role=role,
            episode_num=episode_num,
            first_contact_at=first_contact_at,
            cum_buy=cum_buy,
            cum_sell=cum_sell,
            excursion_low=excursion_low,
            excursion_high=excursion_high,
            report_buy=cum_buy,
            report_sell=cum_sell,
            last_report_monotonic=self._monotonic(),
            gap_flag=True,
        )

    def watches(self) -> list[WatchReportData]:
        return [self._snapshot(w) for w in self._watches.values()]

    def take_flush_pending(self) -> bool:
        pending = self._flush_pending
        self._flush_pending = False
        return pending

    # ---- 이벤트 경로 ----------------------------------------------------

    def on_trade(self, event: AggTradeEvent) -> list[WatchEvent]:
        """계측 + 회차 경계. epoch 게이트 밖에서 호출된다 — 계측은 상태
        적재로 분류되어 aggTrade가 살아있는 한 계속 (§5.4 v1.13)."""
        events: list[WatchEvent] = []
        price = event.price
        for watch in self._watches.values():
            band_lo = watch.lo * (1 - self._band_pct)
            band_hi = watch.hi * (1 + self._band_pct)
            in_band_now = band_lo <= price <= band_hi

            if in_band_now and not watch.in_band:
                role = self._entry_role(watch, price)
                if role is not None:
                    if watch.role is not None and role != watch.role:
                        watch.breach_closes = 0  # 역할 전환 — 이탈 방향이 바뀜
                    watch.role = role
                    watch.in_band = True
                    watch.episode_num += 1
                    self._flush_pending = True
                    if watch.first_contact_at is None:
                        watch.first_contact_at = self._clock()
                        watch.last_price = price
                        events.append(WatchFirstContact(data=self._snapshot(watch)))
                        watch.last_report_monotonic = self._monotonic()
                        watch.report_buy = watch.cum_buy
                        watch.report_sell = watch.cum_sell
            elif not in_band_now and watch.in_band:
                watch.in_band = False  # 회차 종료 — 주시는 유지, 누적은 생애 기준
                self._flush_pending = True

            # 계측 — 역할 활성 중 단방향 무제한 (지지: ≤ hi, 저항: ≥ lo)
            if watch.role == ROLE_SUPPORT and price <= watch.hi:
                self._count(watch, event)
            elif watch.role == ROLE_RESISTANCE and price >= watch.lo:
                self._count(watch, event)

            # 구역 외 위치 기억 (역할 판정 fallback용) + 표시용 현재가
            if price > watch.hi:
                watch.prev_zone_rel = "above"
            elif price < watch.lo:
                watch.prev_zone_rel = "below"
            watch.last_price = price
        return events

    def on_candle_close(self, candle: Candle) -> list[WatchEvent]:
        """무효화 판정 — 봉 마감(몸통)만 사용, 꼬리 무시 (§8 W)."""
        events: list[WatchEvent] = []
        finished: list[_Key] = []
        for key, watch in self._watches.items():
            if watch.role is None:
                continue  # 역할 미정 — 이탈 방향이 정의되지 않음
            if candle.gap_tainted:
                watch.breach_closes = 0  # 공백 낀 봉 — 판정 제외 + 연속 리셋
                continue
            if watch.role == ROLE_SUPPORT:
                breach = candle.close_price < watch.lo * (1 - self._buffer_pct)
                reason = FINAL_SUPPORT_BROKEN
            else:
                breach = candle.close_price > watch.hi * (1 + self._buffer_pct)
                reason = FINAL_RESISTANCE_BROKEN
            watch.breach_closes = watch.breach_closes + 1 if breach else 0
            if watch.breach_closes >= self._confirm_closes:
                events.append(WatchFinal(data=self._snapshot(watch), reason=reason))
                finished.append(key)
        for key in finished:
            del self._watches[key]  # 발송 보장은 store 마킹 소관 (§9.4)
            self._flush_pending = True
        return events

    def on_epoch_end(self) -> None:
        """계측은 중단하지 않는다 — 리포트에 공백 플래그만 (§5.4 v1.13).
        봉 오염(카운트 리셋)은 CandleAssembler.mark_gap() 경유."""
        for watch in self._watches.values():
            watch.gap_flag = True

    def on_tick(self) -> list[WatchEvent]:
        """주기 리포트 — 활동 게이팅 (§8 W). service의 1s 틱에서 호출.

        게이팅 침묵 시 due를 소모하지 않는다 — due 경과 후 첫 활동이
        관측되는 틱에 즉시 발송된다 (interval 표기는 실제 경과 기준)."""
        events: list[WatchEvent] = []
        now = self._monotonic()
        for watch in self._watches.values():
            if watch.first_contact_at is None:
                continue  # 접촉 전 — 보고할 것이 없음
            if now - watch.last_report_monotonic < self._report_interval:
                continue
            has_activity = (
                watch.cum_buy > watch.report_buy or watch.cum_sell > watch.report_sell
            )
            if not has_activity and not watch.in_band:
                continue
            events.append(WatchPeriodicReport(data=self._snapshot(watch)))
            watch.report_buy = watch.cum_buy
            watch.report_sell = watch.cum_sell
            watch.last_report_monotonic = now
            watch.gap_flag = False
        return events

    # ---- 내부 -----------------------------------------------------------

    def _entry_role(self, watch: _Watch, price: Decimal) -> str | None:
        """회차 시작 시점 역할 판정 (§8 W) — 회차마다 재판정한다 (마감 미확정
        관통 후 반대편 재진입 시 역할 전환 허용). None = 대기 유지."""
        if price > watch.hi:
            return ROLE_SUPPORT
        if price < watch.lo:
            return ROLE_RESISTANCE
        if watch.prev_zone_rel == "above":
            return ROLE_SUPPORT
        if watch.prev_zone_rel == "below":
            return ROLE_RESISTANCE
        return watch.role  # 구역 내부 진입 + 이력 없음 — 기존 역할 유지(없으면 대기)

    def _count(self, watch: _Watch, event: AggTradeEvent) -> None:
        if event.aggressor_side is Side.BUY:
            watch.cum_buy += event.qty
        else:
            watch.cum_sell += event.qty
        price = event.price
        if watch.excursion_low is None or price < watch.excursion_low:
            watch.excursion_low = price
        if watch.excursion_high is None or price > watch.excursion_high:
            watch.excursion_high = price

    def _snapshot(self, watch: _Watch) -> WatchReportData:
        return WatchReportData(
            lo=watch.lo,
            hi=watch.hi,
            literal=watch.literal,
            role=watch.role,
            episode_num=watch.episode_num,
            in_band=watch.in_band,
            registered_at=watch.registered_at,
            first_contact_at=watch.first_contact_at,
            last_price=watch.last_price,
            excursion_low=watch.excursion_low,
            excursion_high=watch.excursion_high,
            interval_buy=watch.cum_buy - watch.report_buy,
            interval_sell=watch.cum_sell - watch.report_sell,
            cum_buy=watch.cum_buy,
            cum_sell=watch.cum_sell,
            interval_seconds=self._monotonic() - watch.last_report_monotonic,
            breach_closes=watch.breach_closes,
            confirm_closes=self._confirm_closes,
            timeframe=self._timeframe,
            gap_flag=watch.gap_flag,
        )
