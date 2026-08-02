"""스트림별 헬스 추적 + 세션 epoch (PRD §5.4, §5.1 diff 누락 탐지).

- epoch = 세 스트림이 모두 healthy한 연속 구간. 단절·staleness·diff U/u 갭이면 종료
- 새 epoch = 구독 확인 + 세 스트림 모두 수신 재개(depth는 완전 스냅샷이므로 depth
  수신 = "첫 depth 스냅샷" 충족) 후 시작
- diff `U`/`u`는 연속성(직전 u+1 = 현재 U) 검사 전용 — full book 재구성 금지 (§5.1)
- 벽 레지스트리 unconfirmed 마킹은 **diff 청취 공백에만** 해당 (§12.1 규칙 1) —
  통지의 `diff_listening_gap` 플래그로 구분해 호출자가 마킹한다

M1에서는 상태 산출·통지까지만 담당하고, 소비(디텍터 판정 보류)는 M2+,
staleness의 Telegram 경보는 M5 워치독이 얹는다.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable

from order_monitor.ingestion.events import (
    AGG_TRADE_STREAM_SUFFIX,
    DEPTH20_STREAM_SUFFIX,
    DIFF_DEPTH_STREAM_SUFFIX,
    AggTradeEvent,
    DiffDepthEvent,
    Event,
    stream_names,
)


@dataclasses.dataclass(frozen=True)
class EpochEnded:
    epoch_id: int
    reason: str  # "disconnect" | "stale:<stream>" | "diff_gap"


@dataclasses.dataclass(frozen=True)
class EpochStarted:
    epoch_id: int


@dataclasses.dataclass(frozen=True)
class StreamStale:
    stream: str
    silent_seconds: float


@dataclasses.dataclass(frozen=True)
class DiffListeningGap:
    """diff 스트림 청취 공백 — 벽 레지스트리 전체 unconfirmed 마킹 대상 (§12.1 규칙 1).

    epoch 활성 여부와 무관하게 발생할 수 있다 (예: aggTrade staleness로 epoch이 이미
    종료된 상태에서 diff까지 끊긴 경우).
    """

    reason: str  # "disconnect" | "stale" | "u_gap"


Notice = EpochEnded | EpochStarted | StreamStale | DiffListeningGap


class SessionEpochTracker:
    def __init__(
        self,
        symbol: str,
        stale_seconds: float,
        trade_stale_seconds: float,
        clock: Callable[[], float] = time.monotonic,
        streams: tuple[str, str, str] | None = None,
        stale_thresholds: dict[str, float] | None = None,
        check_diff_continuity: bool = True,
        check_trade_continuity: bool = False,
    ) -> None:
        """(v1.16) streams/stale_thresholds/check_diff_continuity로 거래소별 구성 주입.

        - streams: (depth류, 체결류, diff류) — 미지정 시 바이낸스 stream_names(symbol)
        - stale_thresholds: 스트림별 임계 오버라이드 — Coinbase는 ticker(depth류)도
          체결 주도 push라 trade_stale_seconds를 써야 한다 (PRD §5.5)
        - check_diff_continuity: 바이낸스 U/u 연속성 검사 — Coinbase l2 채널은
          시퀀스가 없어 비활성 (갭 대응은 trade_id 갭·staleness가 담당, PRD §5.5)
        - check_trade_continuity: Coinbase match trade_id 연속성 검사 (+1 단위,
          2026-08-02 라이브 캡처로 연속성 실증) — 갭이면 epoch 종료. **DiffListeningGap은
          아님** (체결 손실이지 diff 청취 공백이 아님 — 레지스트리 unconfirmed 미마킹, §5.5)
        """
        depth, agg_trade, diff = streams if streams is not None else stream_names(symbol)
        self._depth_stream = depth
        self._agg_trade_stream = agg_trade
        self._diff_stream = diff
        self._stale_thresholds = (
            dict(stale_thresholds)
            if stale_thresholds is not None
            else {
                depth: stale_seconds,
                diff: stale_seconds,
                agg_trade: trade_stale_seconds,  # 체결 발생 시에만 push라 느슨한 임계 (§10)
            }
        )
        self._check_diff_continuity = check_diff_continuity
        self._check_trade_continuity = check_trade_continuity
        self._clock = clock

        self._subscribed = False
        self._active = False
        self._epoch_id = 0
        self._last_recv: dict[str, float] = {}
        self._stale: set[str] = set()
        self._prev_final_update_id: int | None = None
        self._prev_trade_id: int | None = None

    @property
    def epoch_active(self) -> bool:
        return self._active

    @property
    def epoch_id(self) -> int:
        return self._epoch_id

    def last_receive_times(self) -> dict[str, float]:
        return dict(self._last_recv)

    def on_subscribed(self) -> list[Notice]:
        """WS 클라이언트가 세 스트림 구독 확인 후 호출."""
        self._subscribed = True
        return self._maybe_start()

    def on_disconnected(self) -> list[Notice]:
        notices = self._end_epoch("disconnect")
        notices.append(DiffListeningGap(reason="disconnect"))
        self._subscribed = False
        self._last_recv.clear()
        self._stale.clear()
        # 재연결 후 첫 diff/체결 이벤트는 연속성 기준점이 없음 (공백은 disconnect가 이미 처리)
        self._prev_final_update_id = None
        self._prev_trade_id = None
        return notices

    def on_event(self, stream: str, event: Event) -> list[Notice]:
        notices: list[Notice] = []
        self._last_recv[stream] = event.local_monotonic_receive_time
        self._stale.discard(stream)

        if isinstance(event, DiffDepthEvent) and self._check_diff_continuity:
            prev = self._prev_final_update_id
            self._prev_final_update_id = event.final_update_id
            if prev is not None and event.first_update_id != prev + 1:
                # 누락 탐지 전용 — 이 이벤트 자체는 절대 잔량이라 상태 적재에는 유효
                notices += self._end_epoch("diff_gap")
                notices.append(DiffListeningGap(reason="u_gap"))

        if isinstance(event, AggTradeEvent) and self._check_trade_continuity:
            prev_trade = self._prev_trade_id
            self._prev_trade_id = event.agg_trade_id
            if prev_trade is not None and event.agg_trade_id != prev_trade + 1:
                # 체결 손실 — cum_traded 신뢰 불가로 epoch 종료. diff 청취 공백은 아니다
                notices += self._end_epoch("trade_gap")

        notices += self._maybe_start()
        return notices

    def check_staleness(self) -> list[Notice]:
        """주기 호출 (100ms 스트림 대비 1s 주기면 충분). fresh→stale 전이만 통지."""
        if not self._subscribed:
            return []
        now = self._clock()
        notices: list[Notice] = []
        for stream, threshold in self._stale_thresholds.items():
            last = self._last_recv.get(stream)
            if last is None or stream in self._stale:
                continue
            silent = now - last
            if silent > threshold:
                self._stale.add(stream)
                notices.append(StreamStale(stream=stream, silent_seconds=silent))
                notices += self._end_epoch(f"stale:{stream}")
                if stream == self._diff_stream:
                    notices.append(DiffListeningGap(reason="stale"))
        return notices

    def _end_epoch(self, reason: str) -> list[Notice]:
        if not self._active:
            return []
        self._active = False
        return [EpochEnded(self._epoch_id, reason)]

    def _maybe_start(self) -> list[Notice]:
        if self._active or not self._subscribed or self._stale:
            return []
        if set(self._stale_thresholds) - set(self._last_recv):
            return []  # 아직 전 스트림 수신 확인 전
        self._epoch_id += 1
        self._active = True
        return [EpochStarted(self._epoch_id)]
