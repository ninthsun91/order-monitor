"""D5 — 인텐트→실체결 상태기계 (PRD §8 D5 v1.2). 핵심 신호 — D1~D4는 재료.

등록: D1 APPEARED에서 인텐트 등록, 등록 크기 S = 발화 시점 `qty` 고정(이후
벽 크기 변화 미반영). 판정:

- **케이스1 (CONFIRMED)**: `cum_traded_at_level ≥ S × REALIZE_PCT` — 그 가격에
  체결이 있었다는 사실 자체가 이미 "가격 도달"을 증명하므로 별도 접촉/관통
  판정이 불필요하다(D3와 의미가 다름: D3="관통 없이 버텼는가", D5 케이스1=
  "실제로 그만큼 체결됐는가" — 관통 여부 무관, PRD 원문에 관통 배제 조건 없음).
  조건 충족 즉시 발화(지연 없음, PRD "임계는 넘는 순간 즉시 발화").
- **케이스2 (INFERRED_ABOVE)**: 상위 구간(의도 레벨과 같은 side 현재가 사이)
  D4 lifetime `refill_added` 합 ≥ `S × REALIZE_PCT_ABOVE` — 리필 확인분만 합산
  (단순 체결량 아님, 오탐 방지). 귀속 불가로 "추정" 등급.
- **소멸(D1 REMOVED) 시 즉시 종국 평가**, 4분류 우선순위: 케이스1 재확인 →
  케이스2 재확인(PULLED 시 먼저 평가) → D1 FILLED 귀속+케이스1 미달=
  PARTIALLY_EXECUTED(로그만) → D1 PULLED 귀속+케이스2 미달=INTENT_WITHDRAWN(로그만).
- **인텐트는 만료되지 않는다 — 수명 = 벽 수명 (PRD v1.5, TTL 폐지)**: 구
  INTENT_TTL은 기산점이 등록 시점이라 원거리 상시 벽(실측 61k bid, 지속 23h+)이
  하루 대부분 인텐트 부재 사각지대에 놓였고, 그 사이 벽이 전량 소진되면 D1
  latch 탓에 재등록 경로가 없어 핵심 알림이 영영 누락됐다. 메모리는 벽
  희소성(SIZE_THRESHOLD) + `MAX_ACTIVE_INTENTS`로 바운드.
- **epoch 종료 → 활성 인텐트 전부 INTERRUPTED**(로그만, 우선순위 무시 즉시 —
  `reset()`이 이벤트를 반환하는 이유. 호출자는 D4의 lifetime 리필이 지워지기
  전에 이걸 먼저 호출해야 한다 — service.py 배선 순서 참고).
- **진행률**: 실현률이 `progress_step_pct` 경계를 넘을 때마다 계열(케이스1/2)별
  독립 커서로 발화, `realize_pct`/`realize_pct_above` 미만 경계만(그 이상은
  종국 알림이 대체).

모든 종국 레코드는 레벨 실현률과 상위 구간 추정 실현률을 함께 남긴다(PRD) —
6개 종국 상태가 필드셋이 동일해 단일 `D5Terminal{state}`로 통합했다(D1처럼
필드가 다른 별도 클래스가 아님).

시간축은 `monotonic` (D2와 동일 주입) — 인텐트는 재시작을 넘어 살아남지
않으므로(wall_registry만 예외, PRD §12) wall-clock 영속성이 불필요하고,
replay 결정성도 기존 주입 패턴과 정합.
"""

from __future__ import annotations

import dataclasses
import enum
import time
from collections.abc import Callable
from decimal import Decimal

from order_monitor.detectors.d1 import D1Appeared, D1Attribution, D1Removed
from order_monitor.ingestion.events import Side

_Key = tuple[Side, Decimal]

MAX_ACTIVE_INTENTS = 1000  # 방어적 메모리 상한 — SIZE_THRESHOLD 벽 자체가 희소해 실질적으로 절대 안 걸림


class D5TerminalState(enum.Enum):
    EXECUTION_CONFIRMED = "execution_confirmed"
    EXECUTION_INFERRED_ABOVE = "execution_inferred_above"
    PARTIALLY_EXECUTED = "partially_executed"
    INTENT_WITHDRAWN = "intent_withdrawn"
    INTERRUPTED = "interrupted"


_LOG_ONLY_STATES = frozenset(
    {
        D5TerminalState.PARTIALLY_EXECUTED,
        D5TerminalState.INTENT_WITHDRAWN,
        D5TerminalState.INTERRUPTED,
    }
)


@dataclasses.dataclass(frozen=True)
class D5Terminal:
    intent_id: int
    side: Side
    price: Decimal
    registered_qty: Decimal  # S
    state: D5TerminalState
    level_realized_rate: Decimal  # cum_traded_at_level / registered_qty
    above_realized_rate: Decimal  # 상위 구간 lifetime 리필 / registered_qty
    registered_seconds: float  # 등록→종국 소요시간 (monotonic)


@dataclasses.dataclass(frozen=True)
class D5Progress:
    intent_id: int
    side: Side
    price: Decimal
    registered_qty: Decimal
    series: str  # "case1" | "case2"
    boundary_pct: Decimal
    realized_qty: Decimal  # 경계 도달 시점 절대 체결/리필량


D5Event = D5Terminal | D5Progress


@dataclasses.dataclass
class _Intent:
    intent_id: int
    side: Side
    price: Decimal
    registered_qty: Decimal
    registered_at: float
    case1_cursor: int = 0
    case2_cursor: int = 0


class D5Detector:
    def __init__(
        self,
        *,
        realize_pct: Decimal,
        realize_pct_above: Decimal,
        progress_step_pct: Decimal,
        cum_traded_lookup: Callable[[Side, Decimal], Decimal],
        refill_above_lookup: Callable[[Side, Decimal], Decimal],
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._realize_pct = realize_pct
        self._realize_pct_above = realize_pct_above
        self._progress_step_pct = progress_step_pct
        self._cum_traded = cum_traded_lookup
        self._refill_above = refill_above_lookup
        self._monotonic = monotonic
        self._intents: dict[_Key, _Intent] = {}
        self._next_intent_id = 0

    def on_d1_appeared(self, event: D1Appeared) -> None:
        key = (event.side, event.price)
        if key in self._intents:
            return  # 이미 등록된 인텐트 — D1 latch가 중복 APPEARED를 막으므로 방어용
        if len(self._intents) >= MAX_ACTIVE_INTENTS:
            return  # 메모리 상한 — 신규 등록 거부(결정 10)
        intent_id = self._next_intent_id
        self._next_intent_id += 1
        self._intents[key] = _Intent(
            intent_id=intent_id,
            side=event.side,
            price=event.price,
            registered_qty=event.qty,
            registered_at=self._monotonic(),
        )

    def on_d1_removed(self, event: D1Removed) -> D5Terminal | None:
        """레벨 소멸 시 즉시 종국 평가 — 이미 종국 처리된 키는 존재하지 않아 no-op."""
        key = (event.side, event.price)
        intent = self._intents.get(key)
        if intent is None:
            return None

        level_rate = self._level_rate(intent)
        if level_rate >= self._realize_pct:
            return self._finalize(intent, D5TerminalState.EXECUTION_CONFIRMED, level_rate)

        above_rate = self._above_rate(intent)
        if above_rate >= self._realize_pct_above:
            return self._finalize(intent, D5TerminalState.EXECUTION_INFERRED_ABOVE, level_rate, above_rate)

        if event.attribution is D1Attribution.FILLED:
            return self._finalize(intent, D5TerminalState.PARTIALLY_EXECUTED, level_rate, above_rate)
        return self._finalize(intent, D5TerminalState.INTENT_WITHDRAWN, level_rate, above_rate)

    def evaluate(self) -> list[D5Event]:
        """체결/스냅샷마다 호출 — 케이스1/2 즉시 발화 + 진행률 경계."""
        events: list[D5Event] = []
        for key, intent in list(self._intents.items()):
            level_rate = self._level_rate(intent)
            if level_rate >= self._realize_pct:
                events.append(self._finalize(intent, D5TerminalState.EXECUTION_CONFIRMED, level_rate))
                continue

            above_rate = self._above_rate(intent)
            if above_rate >= self._realize_pct_above:
                events.append(
                    self._finalize(
                        intent, D5TerminalState.EXECUTION_INFERRED_ABOVE, level_rate, above_rate
                    )
                )
                continue

            events += self._progress_events(intent, "case1", level_rate, self._realize_pct)
            events += self._progress_events(intent, "case2", above_rate, self._realize_pct_above)
        return events

    def reset(self) -> list[D5Terminal]:
        """epoch 종료 — 활성 인텐트 전부 즉시 INTERRUPTED(우선순위 무시, PRD §5.4).

        D4의 lifetime 리필 조회에 의존하므로 호출자는 이 메서드를 d4.reset()보다
        먼저 호출해야 above_realized_rate가 올바르게 남는다.
        """
        events = [
            self._finalize(
                intent,
                D5TerminalState.INTERRUPTED,
                self._level_rate(intent),
                self._above_rate(intent),
            )
            for intent in list(self._intents.values())
        ]
        self._intents.clear()
        return events

    def _level_rate(self, intent: _Intent) -> Decimal:
        return self._cum_traded(intent.side, intent.price) / intent.registered_qty

    def _above_rate(self, intent: _Intent) -> Decimal:
        return self._refill_above(intent.side, intent.price) / intent.registered_qty

    def _finalize(
        self,
        intent: _Intent,
        state: D5TerminalState,
        level_rate: Decimal,
        above_rate: Decimal | None = None,
    ) -> D5Terminal:
        key = (intent.side, intent.price)
        self._intents.pop(key, None)
        return D5Terminal(
            intent_id=intent.intent_id,
            side=intent.side,
            price=intent.price,
            registered_qty=intent.registered_qty,
            state=state,
            level_realized_rate=level_rate,
            above_realized_rate=above_rate if above_rate is not None else self._above_rate(intent),
            registered_seconds=self._monotonic() - intent.registered_at,
        )

    def _progress_events(
        self, intent: _Intent, series: str, rate: Decimal, terminal_threshold: Decimal
    ) -> list[D5Progress]:
        cursor_attr = "case1_cursor" if series == "case1" else "case2_cursor"
        cursor = getattr(intent, cursor_attr)
        target = int(rate / self._progress_step_pct)
        events: list[D5Progress] = []
        while cursor < target:
            cursor += 1
            boundary = self._progress_step_pct * cursor
            if boundary >= terminal_threshold:
                break  # 종국 임계 이상 경계는 종국 알림이 대체 (PRD)
            events.append(
                D5Progress(
                    intent_id=intent.intent_id,
                    side=intent.side,
                    price=intent.price,
                    registered_qty=intent.registered_qty,
                    series=series,
                    boundary_pct=boundary,
                    realized_qty=boundary * intent.registered_qty,
                )
            )
        setattr(intent, cursor_attr, cursor)
        return events
