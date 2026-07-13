"""접촉 episode 추적 — D3/D4 공용 판정 단위 (PRD §8 D3 v1.2).

episode = 베스트 프라이스가 레벨 가격 P에 도달한 시점부터 (a) 반등 이탈,
(b) 관통 확정, (c) 레벨 소멸 중 하나까지의 구간. 관통 판정은 여기 한곳에만 둔다:

- bid 레벨: sell-aggressor 체결가 < P (주 신호 — 즉시 확정), 또는 best_bid < P가
  `pierce_persist_snapshots` 연속 스냅샷 지속 (보조 신호 — 아이스버그 리필 사이
  잔량이 순간 0이 되는 플리커를 관통으로 오판하지 않도록 지속성 요구)
- ask 레벨: 대칭 (buy-aggressor 체결가 > P / best_ask > P 지속)

D4가 인텐트 상위 구간(비-벽 레벨)의 리필도 관측해야 하므로(PRD §8 D5 케이스 2),
episode는 D1 벽에 한정하지 않고 best가 접촉한 모든 레벨에 연다 — D3 쪽에서
D1 활성 벽만 걸러 소비한다. 시간축은 이벤트의 monotonic 수신 시각(PRD §11.1).
"""

from __future__ import annotations

import dataclasses
import enum
from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, Side


class EpisodeEndReason(enum.Enum):
    REBOUND = "rebound"  # 베스트가 레벨에서 이탈 (비관통)
    PIERCED = "pierced"  # 관통 확정 (체결가 주 신호 또는 best 지속 보조 신호)
    REMOVED = "removed"  # 레벨 소멸 (tombstone/하한 미만)


@dataclasses.dataclass
class ContactEpisode:
    side: Side  # 레벨의 side: bid 레벨 = BUY, ask 레벨 = SELL
    price: Decimal
    started_receive_time: float  # 접촉 스냅샷의 monotonic 수신 시각
    below_best_streak: int = 0  # 관통 보조 신호의 연속 스냅샷 카운터


@dataclasses.dataclass(frozen=True)
class EpisodeEnd:
    episode: ContactEpisode
    reason: EpisodeEndReason
    ended_receive_time: float


class ContactEpisodeTracker:
    def __init__(self, pierce_persist_snapshots: int) -> None:
        self._pierce_persist_snapshots = pierce_persist_snapshots
        self._episodes: dict[tuple[Side, Decimal], ContactEpisode] = {}

    def active(self) -> dict[tuple[Side, Decimal], ContactEpisode]:
        return self._episodes

    def on_depth_snapshot(
        self, best_bid: Decimal | None, best_ask: Decimal | None, receive_time: float
    ) -> list[EpisodeEnd]:
        """스냅샷마다 기존 episode의 반등/관통(보조 신호) 판정 후 신규 접촉을 연다."""
        ended: list[EpisodeEnd] = []
        for key, episode in list(self._episodes.items()):
            best = best_bid if episode.side is Side.BUY else best_ask
            if best is None:
                continue  # 해당 side 스냅샷 공백 — 판정 불가, 유지
            # bid 레벨 기준: best > P = 반등, best == P = 접촉 지속, best < P = 관통 카운트
            away = best > episode.price if episode.side is Side.BUY else best < episode.price
            beyond = best < episode.price if episode.side is Side.BUY else best > episode.price
            if away:
                del self._episodes[key]
                ended.append(EpisodeEnd(episode, EpisodeEndReason.REBOUND, receive_time))
            elif beyond:
                episode.below_best_streak += 1
                if episode.below_best_streak >= self._pierce_persist_snapshots:
                    del self._episodes[key]
                    ended.append(EpisodeEnd(episode, EpisodeEndReason.PIERCED, receive_time))
            else:
                episode.below_best_streak = 0  # 플리커 복귀 — 비관통 (PRD §8 D3 v1.2)

        for side, best in ((Side.BUY, best_bid), (Side.SELL, best_ask)):
            if best is not None and (side, best) not in self._episodes:
                self._episodes[(side, best)] = ContactEpisode(
                    side=side, price=best, started_receive_time=receive_time
                )
        return ended

    def on_trade(self, trade: AggTradeEvent) -> list[EpisodeEnd]:
        """체결가 관통 (주 신호 — 확정적 증거, 즉시 종료)."""
        level_side = Side.BUY if trade.aggressor_side is Side.SELL else Side.SELL
        ended: list[EpisodeEnd] = []
        for key, episode in list(self._episodes.items()):
            if episode.side is not level_side:
                continue
            pierced = (
                trade.price < episode.price
                if level_side is Side.BUY
                else trade.price > episode.price
            )
            if pierced:
                del self._episodes[key]
                ended.append(
                    EpisodeEnd(
                        episode, EpisodeEndReason.PIERCED, trade.local_monotonic_receive_time
                    )
                )
        return ended

    def end_episode(
        self, side: Side, price: Decimal, reason: EpisodeEndReason, receive_time: float
    ) -> EpisodeEnd | None:
        """레벨 소멸 등 외부 사유로 인한 종료 (호출자: service의 벽 소멸 경로)."""
        episode = self._episodes.pop((side, price), None)
        if episode is None:
            return None
        return EpisodeEnd(episode, reason, receive_time)

    def reset(self) -> None:
        """epoch 종료 — 진행 중 episode 전부 폐기 (무판정, PRD §5.4)."""
        self._episodes.clear()
