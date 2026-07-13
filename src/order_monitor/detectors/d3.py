"""D3 — 흡수 (PRD §8 D3 v1.2). 알림 없음, 로그 전용 (PRD §9.1).

전제: D1 활성(APPEARED 발화) 레벨. 접촉 episode(detectors/contact.py)가
비관통(반등/소멸)으로 종료되는 시점에 확정 판정한다 — episode당 최대 1회:

    cum_traded_at_level ≥ 등록 크기 S × ABSORPTION_MIN → 발화

발화 시점을 episode 종료로 확정한 근거 (사용자 확정 2026-07-13, 결정 기록):
중도(조건 충족 즉시) 발화하면 이후 같은 episode에서 관통될 경우 이미 낸 흡수
이벤트가 사후에 거짓이 되어 M6 튜닝 데이터를 오염시킨다. "관통 확정 시 해당
episode에서 D3는 발화하지 않는다"(PRD) 조항과 정합. D3는 로그 전용이고 D5
케이스 1은 자체 누적 감시(M4)라 지연 비용이 없다.

누적(cum_traded_at_level)은 레벨 생애 기준 (PRD §8 D3) — LevelTracker의 벽
레벨 보존(§7 v1.4 예외)이 접촉 episode 사이의 창 이탈에도 누적을 유지한다.
등록 크기 S = D1 APPEARED 발화 시점 qty (service가 D1 이벤트를 중개).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from decimal import Decimal

from order_monitor.detectors.contact import EpisodeEnd, EpisodeEndReason
from order_monitor.detectors.d1 import D1Appeared, D1Removed
from order_monitor.ingestion.events import Side

_Key = tuple[Side, Decimal]


@dataclasses.dataclass(frozen=True)
class D3Absorption:
    side: Side
    price: Decimal
    registered_qty: Decimal  # D1 APPEARED 시점 S
    absorbed_qty: Decimal  # 생애 cum_traded_at_level (episode 종료 시점)
    realized_rate: Decimal  # absorbed / registered
    episode_seconds: float
    end_reason: str  # rebound | removed


class D3Detector:
    def __init__(
        self,
        *,
        absorption_min_pct: Decimal,
        cum_traded_lookup: Callable[[Side, Decimal], Decimal],
    ) -> None:
        self._absorption_min_pct = absorption_min_pct
        self._cum_traded = cum_traded_lookup
        # D1 활성 벽의 등록 크기 S (APPEARED 시점 qty)
        self._registered: dict[_Key, Decimal] = {}

    def on_d1_appeared(self, event: D1Appeared) -> None:
        self._registered[(event.side, event.price)] = event.qty

    def on_d1_removed(self, event: D1Removed) -> None:
        self._registered.pop((event.side, event.price), None)

    def on_episode_end(self, end: EpisodeEnd) -> list[D3Absorption]:
        """비관통 종료 시 확정 판정. 벽 소멸 경로에서는 D1 REMOVED 처리(등록 해제)
        전에 호출되어야 한다 — service 배선 순서 참고."""
        if end.reason is EpisodeEndReason.PIERCED:
            return []
        key = (end.episode.side, end.episode.price)
        registered = self._registered.get(key)
        if registered is None:
            return []
        absorbed = self._cum_traded(*key)
        if absorbed < registered * self._absorption_min_pct:
            return []
        return [
            D3Absorption(
                side=end.episode.side,
                price=end.episode.price,
                registered_qty=registered,
                absorbed_qty=absorbed,
                realized_rate=absorbed / registered,
                episode_seconds=end.ended_receive_time - end.episode.started_receive_time,
                end_reason=end.reason.value,
            )
        ]

    def reset(self) -> None:
        """epoch 종료 — D1 활성이 리셋되므로 등록도 폐기, 새 epoch의 재APPEARED로 재등록."""
        self._registered.clear()
