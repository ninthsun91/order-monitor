"""벽 레지스트리 — diff 스트림 이벤트 탭으로 원거리 대형 레벨 추적 (PRD §5.1 설계 결정 2, §7).

full book을 유지하지 않는다. diff 이벤트는 절대 잔량을 운반하므로 가격별 덮어쓰기만으로
자가치유된다. 갱신 규칙 (PRD §7, §8 D1 v1.2):

- 신규 가격은 `record_min_qty_btc` 이상일 때만 등록 (등록 게이트)
- 추적 중인 가격은 **잔량 값과 무관하게 모든 이벤트 처리** (유령 벽 방지)
- 잔량 0 = tombstone 소멸, 0 < 잔량 < 하한 = 하한 미만 소멸 — 소멸 레코드를 반환하고
  활성 레지스트리에서 제거 (D1 REMOVED 판정은 M2의 소비자 몫)
- unconfirmed 마킹/해제 (PRD §12.1): 청취 공백 시 전체 마킹, 가격별 첫 이벤트(값 불문)로 해제

시각 필드는 wall-clock epoch 초를 쓴다 — `first_seen_above_threshold` 등이 SQLite로
영속화되어 재시작을 넘어 보존되어야 하므로(PRD §12.1 규칙 2) monotonic을 쓸 수 없다.
"""

from __future__ import annotations

import dataclasses
import enum
import time
from collections.abc import Callable
from decimal import Decimal

from order_monitor.ingestion.events import DiffDepthEvent, Side


class RemovalReason(enum.Enum):
    TOMBSTONE = "tombstone"  # 잔량 0 명시 소멸
    BELOW_FLOOR = "below_floor"  # 기록 하한 미만으로 하락


@dataclasses.dataclass
class Wall:
    price: Decimal
    side: Side  # bid 벽 = BUY, ask 벽 = SELL
    last_qty: Decimal
    peak_qty: Decimal
    first_seen_at: float
    first_seen_above_threshold: float | None  # D1 스푸핑 지속 필터 타이머 기준 (PRD §8 D1)
    last_seen_at: float  # 판정 미사용 — 관측·튜닝용 (PRD §7 v1.2)
    unconfirmed: bool = False
    unconfirmed_since: float | None = None
    # 알림이 발송된 임계 스트릭(발송 시점의 first_seen_above_threshold 값) —
    # APPEARED 알림 스트릭당 1회 규칙의 멱등 키 (PRD §8 D1 v1.8). 레지스트리는
    # 보존·영속만 하고 기록은 service의 발송 게이트 몫
    appeared_alerted_since: float | None = None


@dataclasses.dataclass(frozen=True)
class WallRemoval:
    wall: Wall  # last_qty는 소멸을 일으킨 최종 관측값으로 갱신된 상태
    reason: RemovalReason


@dataclasses.dataclass(frozen=True)
class DiffResult:
    """apply_diff 1회분의 등록/소멸 — 등록은 궤적 로깅·D4 스트릭 개시의 소비 대상
    (DEVELOPMENT_PLAN M6 관측 보완, PRD §8 D4 v1.12 스트릭)."""

    registrations: list[Wall]
    removals: list[WallRemoval]


class WallRegistry:
    def __init__(
        self,
        record_min_qty: Decimal,
        size_threshold: Decimal,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._record_min_qty = record_min_qty
        self._size_threshold = size_threshold
        self._clock = clock
        self._walls: dict[tuple[Side, Decimal], Wall] = {}

    def apply_diff(self, event: DiffDepthEvent) -> DiffResult:
        registrations: list[Wall] = []
        removals: list[WallRemoval] = []
        for side, levels in ((Side.BUY, event.bids), (Side.SELL, event.asks)):
            for price, qty in levels:
                outcome = self._apply_level(side, price, qty)
                if isinstance(outcome, Wall):
                    registrations.append(outcome)
                elif isinstance(outcome, WallRemoval):
                    removals.append(outcome)
        return DiffResult(registrations=registrations, removals=removals)

    def _apply_level(self, side: Side, price: Decimal, qty: Decimal) -> Wall | WallRemoval | None:
        now = self._clock()
        key = (side, price)
        wall = self._walls.get(key)

        if wall is None:
            # 등록 게이트는 신규 가격에만 적용 (PRD §8 D1 소멸 규칙)
            if qty < self._record_min_qty:
                return None
            wall = Wall(
                price=price,
                side=side,
                last_qty=qty,
                peak_qty=qty,
                first_seen_at=now,
                first_seen_above_threshold=now if qty >= self._size_threshold else None,
                last_seen_at=now,
            )
            self._walls[key] = wall
            return wall

        # 추적 중인 가격: 값 불문 처리 + unconfirmed 해제 (PRD §12.1 규칙 3)
        wall.last_qty = qty
        wall.last_seen_at = now
        wall.unconfirmed = False
        wall.unconfirmed_since = None

        if qty == 0:
            del self._walls[key]
            return WallRemoval(wall=wall, reason=RemovalReason.TOMBSTONE)
        if qty < self._record_min_qty:
            del self._walls[key]
            return WallRemoval(wall=wall, reason=RemovalReason.BELOW_FLOOR)

        if qty > wall.peak_qty:
            wall.peak_qty = qty
        if qty >= self._size_threshold:
            if wall.first_seen_above_threshold is None:
                wall.first_seen_above_threshold = now
        else:
            # 임계 밑으로 내려가면 지속 타이머 리셋 (재상승 시 재계측 — PRD §8 D1 조건 3)
            wall.first_seen_above_threshold = None
        return None

    def mark_all_unconfirmed(self) -> None:
        """청취 공백(재시작 복원, diff 단절·staleness·U/u 갭) 시 호출 (PRD §12.1 규칙 1)."""
        now = self._clock()
        for wall in self._walls.values():
            if not wall.unconfirmed:
                wall.unconfirmed = True
                wall.unconfirmed_since = now
            # 이미 unconfirmed면 unconfirmed_since 유지 (최초 마킹 시각)

    def prune_unconfirmed(self, ttl_seconds: float) -> list[Wall]:
        """`unconfirmed_since`로부터 ttl 경과한 unconfirmed 벽 삭제 (PRD §12.1 TTL — unconfirmed 전용)."""
        now = self._clock()
        pruned = []
        for key, wall in list(self._walls.items()):
            if wall.unconfirmed and wall.unconfirmed_since is not None:
                if now - wall.unconfirmed_since >= ttl_seconds:
                    del self._walls[key]
                    pruned.append(wall)
        return pruned

    def restore(self, walls: list[Wall]) -> None:
        """영속화 계층이 재시작 복원 시 호출. 이후 mark_all_unconfirmed()는 호출자 책임."""
        for wall in walls:
            self._walls[(wall.side, wall.price)] = wall

    def get(self, side: Side, price: Decimal) -> Wall | None:
        return self._walls.get((side, price))

    def walls(self) -> list[Wall]:
        return list(self._walls.values())

    def __len__(self) -> int:
        return len(self._walls)
