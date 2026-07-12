"""D2 상대 임계의 기준선 — 분당 총 테이커 볼륨의 이동 창 (PRD §7 `volume_baseline`, v1.3).

aggTrade를 `exchange_time` 분 버킷으로 적재한다 (단일 스트림 시간창 — §11.1).
창(`vol_baseline_hours`)이 다 차기 전에는 `per_minute_mean()`이 None — D2 판정 보류
(PRD §8 D2 워밍업). 기동 시 REST klines 부트스트랩으로 선적재할 수 있다.
"""

from __future__ import annotations

import collections
from collections.abc import Iterable
from decimal import Decimal


class VolumeBaseline:
    def __init__(self, baseline_hours: float) -> None:
        self._max_minutes = int(baseline_hours * 60)
        self._buckets: collections.deque[list] = collections.deque()  # [minute, qty]
        self._sum = Decimal(0)

    def add(self, exchange_time_ms: int, qty: Decimal) -> None:
        minute = exchange_time_ms // 60_000
        if self._buckets and minute <= self._buckets[-1][0]:
            # 같은 분 누적. 드문 순서 역전도 최신 버킷에 흡수 — 기준선 정밀도에 무의미
            self._buckets[-1][1] += qty
        else:
            self._buckets.append([minute, qty])
        self._sum += qty
        cutoff = self._buckets[-1][0] - self._max_minutes + 1
        while self._buckets[0][0] < cutoff:
            self._sum -= self._buckets.popleft()[1]

    def bootstrap(self, bars: Iterable[tuple[int, Decimal]]) -> None:
        """(open_time_ms, 분당 총 볼륨) 목록 선적재 — REST klines (PRD §8 D2 워밍업)."""
        for open_time_ms, volume in bars:
            self.add(open_time_ms, volume)

    def per_minute_mean(self) -> Decimal | None:
        """분당 평균. 창이 다 차기 전에는 None — 체결 없는 분은 0으로 계산."""
        if not self._buckets:
            return None
        span = self._buckets[-1][0] - self._buckets[0][0] + 1
        if span < self._max_minutes:
            return None
        return self._sum / Decimal(self._max_minutes)

    def __len__(self) -> int:
        return len(self._buckets)
