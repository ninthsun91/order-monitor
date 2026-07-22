"""D4 — 레벨 흡수 방어 (PRD §8 D4 v1.12, 구 아이스버그/리필 전면 교체).

독립 알림 디텍터 — 케이스 2 폐지(v1.11)로 D5에 공급하지 않는다. 대상은 벽
레지스트리 추적 레벨 전체(관측 하한 100+). 추적 단위는 **스트릭**(레지스트리
등록 → 소멸)이고 누적은 스트릭 생애 기준 — 접촉 episode 사이 리셋되지 않는다.
기준크기 R = 스트릭 개시 시점 `last_qty` 고정(D5의 S와 같은 논리), 재등록·
epoch 재개시(v1.12)는 새 스트릭 + R 재고정.

absorbed = ① 가시 리필 + ② 은닉 리필 — **리필로 입증된 흡수만** 집계.
원시 `cum_traded`는 쓰지 않는다: 횡보 회전(소진 + 무관 재유입)도 총량은
배수를 넘어 오발화 — 횡보는 틱별 mismatch ≈ 0·페어링 실패로 자연 배제된다.

- ① 가시 리필 (v1.2 산식 유지): depth 스냅샷 간 양의 델타 중 직전
  `REFILL_WINDOW_MS` 내 해당 레벨 aggTrade 체결이 있는 것만 인정. 체결 버퍼는
  episode 상태와 독립 — 접촉 스냅샷보다 체결이 먼저 도착하는 순서 역전에도
  쌍이 성립한다. 버퍼는 시간 바운드 전역 프루닝 (메모리 상한).
- ② 은닉 리필 (v1.11 신설): 스냅샷 틱 단위 `max(0, 체결 − 표시감소 − carry)`.
  네이티브 아이스버그(엔진이 소진과 원자적으로 재표시 — 양의 델타 없음)를
  "체결은 있는데 잔량이 안 줄어든 틱"으로 포착. **episode/스트릭 전체 뭉뚱그림
  대조 금지** (v1.2가 폐기한 경로 소실 결함의 재발). carry = 직전 틱
  `max(0, 표시감소 − 체결)`의 1틱 한정 이월 (v1.12) — 스트림 순서 역전으로
  체결과 그 감소 반영 스냅샷이 틱 경계에 엇갈릴 때 max(0,·) 클램프가 만드는
  단방향(양의) 과대 계상을 흡수한다.

틱 대조 상태(직전 잔량·carry·틱 체결 버킷)는 접촉 episode 스코프 — episode
밖에서는 레벨이 top-20 스냅샷 창에 없어 잔량 대조가 성립하지 않고, 창 재진입
시의 겉보기 델타를 리필로 오인하면 안 되기 때문. 스트릭 누적(absorbed·이벤트
수·래치)은 episode 종료에도 유지된다.

발화 `DEFENSE_DETECTED` (스트릭당 1회 후 래치): absorbed ≥ M×R AND 인정
이벤트 수(① 인정 델타 + ② 양성 틱) ≥ `ABSORB_MIN_EVENTS` AND 비관통 AND
레지스트리 활성. 뒤의 두 게이트는 구조적으로 충족된다 — 발화 평가는 활성
접촉 episode의 틱 처리에서만 일어나는데 관통(체결가 주 신호·best 지속 보조
신호)은 episode를 즉시 종료시키고(contact.py), 스트릭은 레지스트리 소멸에서
끝난다. 래치 후 absorbed/R가 `M + k×ABSORB_PROGRESS_STEP` 경계를 넘을 때마다
`DEFENSE_PROGRESS`(상한 없음), 스트릭 종료 시 래치면 `DEFENSE_CLOSED` 1회.
epoch 종료: 누적 리셋, 래치는 `DEFENSE_INTERRUPTED`(로그만) — reset()이
이벤트를 반환하는 이유(d5와 동일 패턴). epoch 시작: 활성 벽 전체 새 스트릭.

관측 기록 (v1.11 — 발화와 별개, 튜닝 필수 데이터): 흡수 인정 이벤트마다 1줄
("d4 absorb event") + 스트릭 종료 시 absorbed > 0 전건 요약 1줄("d4 streak
summary", 발화 무관) — near-miss 분포가 `ABSORB_MULTIPLE` 튜닝의 직접 근거.
로그 볼륨은 자연 바운드라 스로틀 없음 (PRD §8 D4 관측 기록 문단).
"""

from __future__ import annotations

import collections
import dataclasses
import enum
import logging
import time
from collections.abc import Callable
from decimal import Decimal

from order_monitor.detectors.contact import ContactEpisode, EpisodeEnd
from order_monitor.ingestion.events import AggTradeEvent, DepthSnapshot, Side
from order_monitor.state.wall_registry import Wall, WallRemoval

logger = logging.getLogger(__name__)

_Key = tuple[Side, Decimal]


class D4DefenseKind(enum.Enum):
    DETECTED = "defense_detected"
    PROGRESS = "defense_progress"
    CLOSED = "defense_closed"
    INTERRUPTED = "defense_interrupted"  # epoch 종료 — 로그만 (PRD §9.1)


@dataclasses.dataclass(frozen=True)
class D4Defense:
    kind: D4DefenseKind
    side: Side
    price: Decimal
    base_qty: Decimal  # R — 스트릭 개시 시점 표시크기
    absorbed_visible: Decimal
    absorbed_hidden: Decimal
    absorbed_total: Decimal
    multiple: Decimal  # absorbed_total / R
    event_count: int
    streak_started_at: float  # wall-clock — 스트릭 식별자 성분 (dedup 키, PRD §9.2)
    streak_seconds: float
    boundary_multiple: Decimal | None = None  # PROGRESS 전용 — 통과한 경계값


@dataclasses.dataclass
class _Streak:
    base_qty: Decimal
    started_at: float
    started_monotonic: float
    absorbed_visible: Decimal = Decimal(0)
    absorbed_hidden: Decimal = Decimal(0)
    event_count: int = 0
    latched: bool = False
    progress_cursor: int = 0  # 래치 후 통과한 M+k·step 경계 수
    # 틱 대조 상태 — 접촉 episode 스코프 (on_episode_end에서 소거)
    last_size: Decimal | None = None
    carry: Decimal = Decimal(0)  # 1틱 이월 보정 (v1.12)
    tick_traded: Decimal = Decimal(0)  # 직전 스냅샷 이후 해당 레벨 체결량

    @property
    def absorbed_total(self) -> Decimal:
        return self.absorbed_visible + self.absorbed_hidden


class D4Detector:
    def __init__(
        self,
        *,
        absorb_multiple: Decimal,
        absorb_progress_step: Decimal,
        absorb_min_events: int,
        refill_window_ms: int,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._multiple = absorb_multiple
        self._step = absorb_progress_step
        self._min_events = absorb_min_events
        self._window_seconds = refill_window_ms / 1000.0
        self._clock = clock
        self._monotonic = monotonic
        self._streaks: dict[_Key, _Streak] = {}
        # ① 페어링용 레벨별 최근 체결 (receive_time, agg_trade_id) — 상태 계층 성격
        self._trades: dict[_Key, collections.deque[tuple[float, int]]] = {}
        self._trade_log: collections.deque[tuple[float, _Key]] = collections.deque()

    # ── 스트릭 수명 ────────────────────────────────────────────

    def on_wall_registered(self, wall: Wall) -> None:
        """레지스트리 등록 → 새 스트릭 개시, R = 등록 시점 last_qty 고정."""
        self._start_streak((wall.side, wall.price), wall.last_qty)

    def on_epoch_start(self, walls: list[Wall]) -> None:
        """새 epoch 시작(재시작 복원 포함) — 활성 벽 전체 새 스트릭 + R 재고정 (v1.12)."""
        self._streaks = {}
        for wall in walls:
            self._start_streak((wall.side, wall.price), wall.last_qty)

    def on_wall_removed(self, removal: WallRemoval) -> D4Defense | None:
        """스트릭 종료 — 요약 로그(absorbed > 0 전건) + 래치면 DEFENSE_CLOSED."""
        key = (removal.wall.side, removal.wall.price)
        streak = self._streaks.pop(key, None)
        if streak is None:
            return None
        self._log_streak_summary(key, streak, reason=removal.reason.value)
        if streak.latched:
            return self._make(D4DefenseKind.CLOSED, key, streak)
        return None

    def _start_streak(self, key: _Key, qty: Decimal) -> None:
        self._streaks[key] = _Streak(
            base_qty=qty, started_at=self._clock(), started_monotonic=self._monotonic()
        )

    # ── 입력 이벤트 ────────────────────────────────────────────

    def on_trade(self, trade: AggTradeEvent) -> None:
        level_side = Side.BUY if trade.aggressor_side is Side.SELL else Side.SELL
        key = (level_side, trade.price)
        now = trade.local_monotonic_receive_time
        self._trades.setdefault(key, collections.deque()).append((now, trade.agg_trade_id))
        self._trade_log.append((now, key))
        # 전역 시간 바운드 프루닝 — 창 밖 체결은 어느 레벨에서도 쌍이 될 수 없다
        while self._trade_log and self._trade_log[0][0] < now - self._window_seconds:
            _, stale_key = self._trade_log.popleft()
            per_level = self._trades.get(stale_key)
            if per_level:
                per_level.popleft()
                if not per_level:
                    del self._trades[stale_key]
        # ② 틱 버킷 — 스트릭 추적 레벨만 적재 (메모리 바운드)
        streak = self._streaks.get(key)
        if streak is not None:
            streak.tick_traded += trade.qty

    def on_depth_snapshot(
        self, snapshot: DepthSnapshot, active_episodes: dict[_Key, ContactEpisode]
    ) -> list[D4Defense]:
        sizes: dict[_Key, Decimal] = {}
        for side, levels in ((Side.BUY, snapshot.bids), (Side.SELL, snapshot.asks)):
            for price, qty in levels:
                sizes[(side, price)] = qty

        now = snapshot.local_monotonic_receive_time
        events: list[D4Defense] = []
        for key, streak in self._streaks.items():
            if key not in active_episodes:
                # 접촉 밖 — 창(top-20)에 레벨이 없어 대조 불성립. 순서 역전으로
                # 미리 도착한 체결이 다음 episode 기준선 틱으로 새지 않게 버킷 소거
                streak.tick_traded = Decimal(0)
                continue
            new_size = sizes.get(key, Decimal(0))  # 부재 = 잔량 0 — 플리커도 델타 경로 포착
            prev = streak.last_size
            streak.last_size = new_size
            traded = streak.tick_traded
            streak.tick_traded = Decimal(0)
            if prev is None:
                continue  # episode 첫 관측 — 기준선만 설정
            delta = new_size - prev
            decrease = -delta if delta < 0 else Decimal(0)
            accumulated = False

            # ① 가시 리필 — 체결 페어링된 양의 델타만
            if delta > 0:
                paired_times = [
                    receive_time
                    for receive_time, _ in self._trades.get(key, ())
                    if now - self._window_seconds <= receive_time <= now
                ]
                if paired_times:
                    streak.absorbed_visible += delta
                    streak.event_count += 1
                    accumulated = True
                    self._log_absorb_event(
                        key,
                        streak,
                        kind="visible",
                        qty=delta,
                        evidence={"pairing_delay_ms": round((now - max(paired_times)) * 1000.0, 1)},
                    )

            # ② 은닉 리필 — 틱 단위 대조 + 1틱 이월 보정 (v1.12)
            hidden = traded - decrease - streak.carry
            if hidden > 0:
                streak.absorbed_hidden += hidden
                streak.event_count += 1
                accumulated = True
                self._log_absorb_event(
                    key,
                    streak,
                    kind="hidden",
                    qty=hidden,
                    evidence={"tick_traded": str(traded), "tick_decrease": str(decrease)},
                )
            streak.carry = decrease - traded if decrease > traded else Decimal(0)

            if accumulated:
                events += self._evaluate(key, streak)
        return events

    def on_episode_end(self, end: EpisodeEnd) -> None:
        """접촉 종료 — 틱 대조 상태만 소거, 스트릭 누적은 유지 (스트릭 생애 기준)."""
        streak = self._streaks.get((end.episode.side, end.episode.price))
        if streak is not None:
            streak.last_size = None
            streak.carry = Decimal(0)
            streak.tick_traded = Decimal(0)

    def reset(self) -> list[D4Defense]:
        """epoch 종료 — 누적 신뢰 상실. 요약 로그(absorbed > 0 전건) 후 전부 폐기,
        래치된 방어는 DEFENSE_INTERRUPTED 레코드 반환(로그만). 체결 버퍼는 상태
        계층 성격이라 유지 (REFILL_WINDOW_MS 바운드)."""
        events = []
        for key, streak in self._streaks.items():
            self._log_streak_summary(key, streak, reason="epoch_end")
            if streak.latched:
                events.append(self._make(D4DefenseKind.INTERRUPTED, key, streak))
        self._streaks.clear()
        return events

    # ── 판정·산출 ──────────────────────────────────────────────

    def _evaluate(self, key: _Key, streak: _Streak) -> list[D4Defense]:
        if not streak.latched:
            if (
                streak.absorbed_total >= self._multiple * streak.base_qty
                and streak.event_count >= self._min_events
            ):
                streak.latched = True
                # 감지 시점까지 도달한 경계는 감지 발화가 대체 — 커서를 당긴다
                # (D5 _confirm_latch와 같은 원리)
                reached = streak.absorbed_total / streak.base_qty - self._multiple
                streak.progress_cursor = int(reached / self._step)
                return [self._make(D4DefenseKind.DETECTED, key, streak)]
            return []
        events = []
        multiple = streak.absorbed_total / streak.base_qty
        while multiple >= self._multiple + self._step * (streak.progress_cursor + 1):
            streak.progress_cursor += 1
            boundary = self._multiple + self._step * streak.progress_cursor
            events.append(
                self._make(D4DefenseKind.PROGRESS, key, streak, boundary_multiple=boundary)
            )
        return events

    def _make(
        self,
        kind: D4DefenseKind,
        key: _Key,
        streak: _Streak,
        boundary_multiple: Decimal | None = None,
    ) -> D4Defense:
        return D4Defense(
            kind=kind,
            side=key[0],
            price=key[1],
            base_qty=streak.base_qty,
            absorbed_visible=streak.absorbed_visible,
            absorbed_hidden=streak.absorbed_hidden,
            absorbed_total=streak.absorbed_total,
            multiple=streak.absorbed_total / streak.base_qty,
            event_count=streak.event_count,
            streak_started_at=streak.started_at,
            streak_seconds=self._monotonic() - streak.started_monotonic,
            boundary_multiple=boundary_multiple,
        )

    # ── 관측 기록 (PRD §8 D4 v1.11) ────────────────────────────

    def _log_absorb_event(
        self, key: _Key, streak: _Streak, *, kind: str, qty: Decimal, evidence: dict
    ) -> None:
        logger.info(
            "d4 absorb event",
            extra={
                "side": key[0].value,
                "price": str(key[1]),
                "streak_started_at": streak.started_at,
                "kind": kind,
                "qty": str(qty),
                "absorbed_total": str(streak.absorbed_total),
                **evidence,
            },
        )

    def _log_streak_summary(self, key: _Key, streak: _Streak, *, reason: str) -> None:
        if streak.absorbed_total <= 0:
            return
        logger.info(
            "d4 streak summary",
            extra={
                "side": key[0].value,
                "price": str(key[1]),
                "streak_started_at": streak.started_at,
                "base_qty": str(streak.base_qty),
                "absorbed_visible": str(streak.absorbed_visible),
                "absorbed_hidden": str(streak.absorbed_hidden),
                "absorbed_total": str(streak.absorbed_total),
                "max_multiple": str(streak.absorbed_total / streak.base_qty),
                "event_count": streak.event_count,
                "duration_seconds": self._monotonic() - streak.started_monotonic,
                "latched": streak.latched,
                "reason": reason,
            },
        )
