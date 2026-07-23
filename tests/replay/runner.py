"""결정적 replay 러너 (PRD §13 — M3/M4 정확성 검증의 주 수단).

픽스처(JSONL)를 실물 MonitorService에 재생한다 — 하니스가 service 로직을
미러링하면 배선 자체가 검증 밖에 남으므로, 클록 주입 + `on_connected`/
`on_disconnected`/`on_event` 직접 구동으로 실코드 경로를 그대로 지난다.
raw Binance 프레임은 ingestion 정규화(`parse_stream_message`)로 파싱한다.

픽스처 포맷 — 한 줄에 레코드 하나:
    {"t": <monotonic 초>, "stream": "btcusdt@depth20@100ms", "data": {...raw...}}
    {"t": <monotonic 초>, "control": "connect" | "disconnect"}
    {"t": <monotonic 초>, "control": "watch" | "unwatch", "arg": "65600"}   (v1.13 — §9.5 명령 경로)
    {"t": <monotonic 초>, "control": "tick"}   (v1.13 — staleness 틱의 W 부분: 주기 리포트 + flush)

`t`는 monotonic 수신 시각이자 주입 클록의 기준 — wall-clock은 `WALL_CLOCK_BASE + t`.
같은 픽스처 → 같은 이벤트 시퀀스가 결정성 계약이다 (test_replay의 이중 재생 단언).
"""

from __future__ import annotations

import json
from pathlib import Path

from order_monitor.alerting.telegram_commands import parse_zone
from order_monitor.config import AppConfig
from order_monitor.ingestion.events import parse_stream_message
from order_monitor.service import MonitorService

FIXTURES_DIR = Path(__file__).parent / "fixtures"
WALL_CLOCK_BASE = 1_700_000_000.0


def load_fixture(name: str) -> list[dict]:
    path = FIXTURES_DIR / name
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def replay(records: list[dict], config: AppConfig, db_path) -> tuple[MonitorService, list]:
    """레코드 시퀀스를 재생하고 (service, 발신된 디텍터 이벤트 목록)을 반환한다.

    호출자는 service._store.close()로 정리한다.
    """
    now = {"t": 0.0}
    service = MonitorService(
        config,
        db_path=db_path,
        telegram_token="replay-token",
        clock=lambda: WALL_CLOCK_BASE + now["t"],
        monotonic=lambda: now["t"],
    )
    service.startup()

    emitted: list = []
    original_emit = service._emit

    def capturing_emit(event: object) -> None:
        emitted.append(event)
        original_emit(event)

    service._emit = capturing_emit

    for record in records:
        now["t"] = record["t"]
        control = record.get("control")
        if control == "connect":
            service.on_connected()
        elif control == "disconnect":
            service.on_disconnected()
        elif control in ("watch", "unwatch"):
            # 실제 수신 명령과 같은 파싱·핸들러 경로 (§9.5) — 응답 텍스트는 버림
            zone = parse_zone(record["arg"])
            if zone is None:
                raise ValueError(f"fixture watch arg unparsable: {record['arg']!r}")
            handler = (
                service._handle_watch_command
                if control == "watch"
                else service._handle_unwatch_command
            )
            handler(zone[0], zone[1], record["arg"])
        elif control == "tick":
            # _staleness_loop 본문의 W 부분 — 주기 리포트는 동기 on_tick (계획 결정)
            for watch_event in service.watch.on_tick():
                service._emit(watch_event)
            service._flush_watches()
        elif control is not None:
            raise ValueError(f"unknown control record: {control!r}")
        else:
            stream, event = parse_stream_message(
                {"stream": record["stream"], "data": record["data"]}, record["t"]
            )
            service.on_event(stream, event)
    return service, emitted
