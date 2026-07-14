"""프로세스 하트비트 파일 기록 (PRD §11.1, M5).

의미론 = asyncio 이벤트 루프 생존 신호: service가 `watchdog.heartbeat_interval`
주기로 이 파일의 mtime을 갱신하고, 외부 경량 워치독(deploy/watchdog_check.py,
systemd timer 구동)이 mtime 나이로 행(hang)을 감지해 PROCESS_DOWN을 발송한다.
피드 정지 감시는 인프로세스 FEED_STALE 소관이므로 파이프라인 헬스로 게이트하지
않는다 — 이벤트 루프가 돌고 있다는 사실만 기록한다.
"""

from __future__ import annotations

from pathlib import Path


def write_heartbeat(path: Path) -> None:
    """하트비트 파일 touch — 없으면 생성, 있으면 mtime 갱신 (utime, 원자적)."""
    path.touch()
