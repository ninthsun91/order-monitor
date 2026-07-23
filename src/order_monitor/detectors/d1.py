"""D1 — 대형 잔량 발생/제거 (PRD §8 D1).

소스는 벽 레지스트리 전용 (v1.1). 레지스트리 갱신은 WallRegistry 소관이고
이 디텍터는 판정만 한다:

- APPEARED: `last_qty ≥ SIZE_THRESHOLD`가 `PERSIST_SECONDS` 연속 유지 (기준 시각
  `first_seen_above_threshold` — 레지스트리와 같은 wall-clock, M1 결정 기록),
  레벨당 1회, `unconfirmed` 레벨은 발화 억제 (PRD §12.1 규칙 2)
- REMOVED: 활성(APPEARED 발화) 레벨이 `SIZE_THRESHOLD × EXIT_RATIO` 밑으로 하락
  또는 레지스트리에서 소멸(tombstone/하한 미만) 시 발화. `cum_traded_at_level ≥
  (peak_qty − last_qty) × FILL_ATTRIBUTION`이면 FILLED, 아니면 PULLED
- SUPPRESSED: 후보(임계 이상 관측)가 지속 필터 통과 전에 임계 밑으로 내려가거나
  소멸한 경우. 발화가 아니라 로그 전용 — M2 완료 기준 "스푸핑 필터 동작 확인"의
  근거 데이터

활성 latch는 REMOVED까지 유지한다 — 임계(1000) 미만·exit(500) 이상의 데드밴드로
내려갔다 회복해도 재발화하지 않는다. PRD §8 D1 조건 3의 "임계 밑으로 내려갔다
다시 올라오면 리셋"은 REMOVED 이후의 재출현 재계측으로 해석 (데드밴드 내 재발화는
같은 벽에 중복 인텐트를 만들고 EXIT_RATIO 히스테리시스 설계 의도와 충돌 —
docs/MILESTONE_ARCHIVE.md M2 구현 노트 참고).

epoch 종료 시 reset(): 활성·후보 전부 폐기, REMOVED 판정 없음 — 판정 보류 중엔
체결 귀속이 얼어 FILLED/PULLED가 오판되기 때문 (PRD §5.4). 새 epoch에서 임계
이상 벽은 다시 APPEARED부터 시작한다 (지속 타이머는 레지스트리 필드가 보존).
"""

from __future__ import annotations

import dataclasses
import enum
import time
from collections.abc import Callable, Iterable
from decimal import Decimal

from order_monitor.ingestion.events import Side
from order_monitor.state.wall_registry import Wall, WallRemoval


class D1Attribution(enum.Enum):
    FILLED = "filled"  # 실체결로 소진
    PULLED = "pulled"  # 취소/스푸핑 추정


@dataclasses.dataclass(frozen=True)
class D1Appeared:
    side: Side
    price: Decimal
    qty: Decimal  # 발화 시점 last_qty — D5 등록 크기 S의 원천 (PRD §8 D5, M4)
    persisted_seconds: float


@dataclasses.dataclass(frozen=True)
class D1Removed:
    side: Side
    price: Decimal
    last_qty: Decimal
    peak_qty: Decimal
    cum_traded: Decimal
    attribution: D1Attribution


@dataclasses.dataclass(frozen=True)
class D1Suppressed:
    """지속 필터(PERSIST_SECONDS) 미달로 발화가 억제된 후보 — 로그 전용."""

    side: Side
    price: Decimal
    peak_qty: Decimal
    above_threshold_seconds: float  # 근사치 — 평가 주기 해상도(1s)로 상한 계측


D1Event = D1Appeared | D1Removed | D1Suppressed

_Key = tuple[Side, Decimal]


class D1Detector:
    def __init__(
        self,
        *,
        size_threshold: Decimal,
        persist_seconds: float,
        exit_ratio: Decimal,
        fill_attribution: Decimal,
        cum_traded_lookup: Callable[[Side, Decimal], Decimal],
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._exit_qty = size_threshold * exit_ratio
        self._persist_seconds = persist_seconds
        self._fill_attribution = fill_attribution
        self._cum_traded = cum_traded_lookup
        self._clock = clock
        self._active: set[_Key] = set()
        # 임계 이상 관측됐지만 아직 지속 필터를 통과하지 못한 후보:
        # key → 관측 시작 시각 (wall.first_seen_above_threshold의 사본 — 값이
        # 바뀌면 사이에 임계 하회가 있었다는 뜻이므로 스트릭 교체로 감지)
        self._candidate_since: dict[_Key, float] = {}

    def evaluate(self, walls: Iterable[Wall]) -> list[D1Event]:
        """레지스트리 현재 상태 평가 — diff 이벤트 후 + 주기 틱(지속 타이머 게이트)."""
        now = self._clock()
        events: list[D1Event] = []
        for wall in walls:
            key = (wall.side, wall.price)

            if key in self._active:
                if wall.last_qty < self._exit_qty:
                    self._active.discard(key)
                    events.append(self._judge_removed(wall))
                continue

            since = wall.first_seen_above_threshold
            if since is None:
                suppressed = self._pop_candidate(key, wall, now)
                if suppressed is not None:
                    events.append(suppressed)
                continue

            prev_since = self._candidate_since.get(key)
            if prev_since is not None and prev_since != since:
                # 평가 사이에 임계 하회 후 재상승 — 구 스트릭은 억제로 기록
                events.append(self._suppressed(wall, now - prev_since))

            if wall.unconfirmed:
                # 발화만 억제, 타이머는 보존 (PRD §12.1 규칙 2)
                self._candidate_since[key] = since
                continue

            if now - since >= self._persist_seconds:
                self._candidate_since.pop(key, None)
                self._active.add(key)
                events.append(
                    D1Appeared(
                        side=wall.side,
                        price=wall.price,
                        qty=wall.last_qty,
                        persisted_seconds=now - since,
                    )
                )
            else:
                self._candidate_since[key] = since
        return events

    def on_removals(self, removals: Iterable[WallRemoval]) -> list[D1Event]:
        """레지스트리 소멸(tombstone/하한 미만) 처리 — 활성 레벨이면 REMOVED 판정."""
        now = self._clock()
        events: list[D1Event] = []
        for removal in removals:
            wall = removal.wall
            key = (wall.side, wall.price)
            if key in self._active:
                self._active.discard(key)
                self._candidate_since.pop(key, None)
                events.append(self._judge_removed(wall))
            else:
                suppressed = self._pop_candidate(key, wall, now)
                if suppressed is not None:
                    events.append(suppressed)
        return events

    def reset(self) -> None:
        """epoch 종료 시 호출 — 판정 전제가 깨졌으므로 활성·후보 폐기 (PRD §5.4)."""
        self._active.clear()
        self._candidate_since.clear()

    def _judge_removed(self, wall: Wall) -> D1Removed:
        cum_traded = self._cum_traded(wall.side, wall.price)
        dropped = wall.peak_qty - wall.last_qty
        filled = cum_traded >= dropped * self._fill_attribution
        return D1Removed(
            side=wall.side,
            price=wall.price,
            last_qty=wall.last_qty,
            peak_qty=wall.peak_qty,
            cum_traded=cum_traded,
            attribution=D1Attribution.FILLED if filled else D1Attribution.PULLED,
        )

    def _pop_candidate(self, key: _Key, wall: Wall, now: float) -> D1Suppressed | None:
        since = self._candidate_since.pop(key, None)
        if since is None:
            return None
        return self._suppressed(wall, now - since)

    def _suppressed(self, wall: Wall, elapsed: float) -> D1Suppressed:
        return D1Suppressed(
            side=wall.side,
            price=wall.price,
            peak_qty=wall.peak_qty,
            above_threshold_seconds=elapsed,
        )
