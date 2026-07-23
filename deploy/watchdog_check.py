#!/usr/bin/env python3
"""외부 경량 워치독 — 하트비트 정지(프로세스 행) 감지 → PROCESS_DOWN + 자동 재시작 (PRD §11.1, M5).

메인 프로세스가 행(hang) 상태면 인프로세스 FEED_STALE도 systemd Restart=always도
무력하다(프로세스가 살아있으므로) — 이 스크립트가 그 조용한 실패의 최후 방어선.
systemd timer(order-monitor-watchdog.timer, 60s 주기)가 root로 구동한다.

**시스템 python3 + stdlib 전용** (앱 venv 미사용): venv가 깨진 상황에서도 동작해야
하는 최후 방어선이기 때문. 임계·경로는 스크립트 상수 (config.yaml 스키마는 PRD §10
키 전수 고정이라 건드리지 않음 — Telegram 재시도 상수와 동일 취급).

동작 (사용자 확정 2026-07-15, docs/DECISIONS.md):
- `systemctl is-active` == inactive → 의도적 정지(유지보수)로 간주, 조용히 스킵
- 하트비트 파일 부재 또는 mtime 나이 > STALE_AFTER_SECONDS → stale
- ok→stale 전이: PROCESS_DOWN 알림 1회 → `systemctl restart order-monitor`
  (발송 실패는 재시작을 막지 않는다 — 부분 실패 격리, §11.1)
- stale→ok 전이: 해소 알림 1회. 전이 없으면 무동작 (반복 재알림 없음)
- 전이는 STATE_FILE로 추적. 전이 처리 중 스크립트가 죽으면 상태가 남지 않아
  다음 주기에 재시도된다 (알림 유실보다 중복이 낫다)
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

HEARTBEAT_FILE = Path("/var/lib/order-monitor/heartbeat")
STATE_FILE = Path("/var/lib/order-monitor/watchdog_state")
ENV_FILE = Path("/etc/order-monitor/env")
CONFIG_FILE = Path("/opt/order-monitor/config.yaml")
SERVICE_NAME = "order-monitor"

# 하트비트 주기 10s(config watchdog.heartbeat_interval)의 6배 — systemd 재시작
# 창(RestartSec=5)이나 일시 부하로는 넘지 않고, 진짜 행만 넘는 여유
STALE_AFTER_SECONDS = 60.0
SYSTEMCTL_TIMEOUT_SECONDS = 30
TELEGRAM_TIMEOUT_SECONDS = 10

_KST = timezone(timedelta(hours=9))

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger("watchdog_check")


def parse_env_token(text: str) -> str | None:
    """EnvironmentFile 형식(KEY=VALUE 행)에서 TELEGRAM_BOT_TOKEN 추출."""
    for line in text.splitlines():
        key, sep, value = line.strip().partition("=")
        if sep and key == "TELEGRAM_BOT_TOKEN":
            return value.strip().strip("\"'") or None
    return None


def parse_chat_id(text: str) -> str | None:
    """config.yaml에서 telegram.chat_id 추출 — env 이중화 대신 단일 진실원 유지."""
    match = re.search(r"^\s*chat_id:\s*[\"']?([^\"'#\n]+?)[\"']?\s*(?:#.*)?$", text, re.MULTILINE)
    return match.group(1).strip() if match else None


def heartbeat_age(path: Path, now: float) -> float | None:
    """하트비트 파일 mtime 나이(초). 부재 시 None (= stale 취급)."""
    try:
        return now - path.stat().st_mtime
    except FileNotFoundError:
        return None


def send_telegram(token: str, chat_id: str, text: str) -> None:
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage", data=data
    )
    with urllib.request.urlopen(request, timeout=TELEGRAM_TIMEOUT_SECONDS) as response:
        response.read()


def format_process_down(age: float | None) -> str:
    now = datetime.now(_KST)
    reason = (
        "하트비트 파일 없음"
        if age is None
        else f"하트비트 {age:.0f}초간 갱신 없음 (임계 {STALE_AFTER_SECONDS:g}s)"
    )
    return (
        f"🛑 프로세스 정지 (PROCESS_DOWN)\n"
        f"{reason} — 프로세스 행 또는 재시작 반복 실패 의심\n"
        f"systemctl restart {SERVICE_NAME} 자동 실행\n"
        f"발생: {now:%H:%M:%S} KST"
    )


def format_recovered() -> str:
    now = datetime.now(_KST)
    return (
        f"✅ 프로세스 정지 해소 (PROCESS_DOWN 해소)\n"
        f"하트비트 갱신 재개 확인\n"
        f"해소: {now:%H:%M:%S} KST"
    )


def run_check(
    *,
    now: float,
    heartbeat_file: Path = HEARTBEAT_FILE,
    state_file: Path = STATE_FILE,
    is_active=None,
    send=None,
    restart=None,
) -> str:
    """1회 점검 — 수행한 동작을 문자열로 반환 (테스트·로그용).

    is_active/send/restart는 테스트 주입점 — 미주입 시 실물(systemctl·Telegram).
    """
    is_active = is_active if is_active is not None else _systemctl_is_active
    send = send if send is not None else _send_with_real_credentials
    restart = restart if restart is not None else _systemctl_restart

    if is_active() == "inactive":
        return "skipped: service intentionally stopped"

    age = heartbeat_age(heartbeat_file, now)
    stale = age is None or age > STALE_AFTER_SECONDS
    prev = state_file.read_text().strip() if state_file.exists() else "ok"

    if stale and prev != "stale":
        try:
            send(format_process_down(age))
        except Exception as exc:  # 발송 실패는 재시작을 막지 않는다 (§11.1)
            logger.warning("PROCESS_DOWN alert send failed: %s", exc)
        restart()
        state_file.write_text("stale")
        return f"stale transition: alerted + restarted (age={age})"
    if not stale and prev == "stale":
        try:
            send(format_recovered())
        except Exception as exc:
            logger.warning("recovery alert send failed: %s", exc)
        state_file.write_text("ok")
        return "recovered: alerted"
    return f"no transition (stale={stale})"


def _systemctl_is_active() -> str:
    result = subprocess.run(
        ["systemctl", "is-active", SERVICE_NAME],
        capture_output=True,
        text=True,
        timeout=SYSTEMCTL_TIMEOUT_SECONDS,
    )
    return result.stdout.strip()


def _systemctl_restart() -> None:
    subprocess.run(
        ["systemctl", "restart", SERVICE_NAME],
        timeout=SYSTEMCTL_TIMEOUT_SECONDS,
        check=False,
    )


def _send_with_real_credentials(text: str) -> None:
    token = parse_env_token(ENV_FILE.read_text())
    chat_id = parse_chat_id(CONFIG_FILE.read_text())
    if not token or not chat_id:
        raise RuntimeError(f"credentials missing (token={bool(token)}, chat_id={bool(chat_id)})")
    send_telegram(token, chat_id, text)


def main() -> None:
    action = run_check(now=time.time())
    logger.info(json.dumps({"action": action}, ensure_ascii=False))


if __name__ == "__main__":
    main()
