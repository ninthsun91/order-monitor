"""외부 워치독 deploy/watchdog_check.py (PRD §11.1, M5) — 판정·전이·파싱.

deploy/는 패키지가 아니므로(시스템 python3 단독 실행 스크립트) 파일 경로로 로드.
"""

import importlib.util
import os
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "deploy" / "watchdog_check.py"
_spec = importlib.util.spec_from_file_location("watchdog_check", _SCRIPT)
wc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wc)

NOW = 1_000_000.0


@pytest.fixture
def env(tmp_path):
    """가짜 하트비트/상태 파일 + 주입 기록기."""

    class Env:
        heartbeat = tmp_path / "heartbeat"
        state = tmp_path / "watchdog_state"
        sent: list[str] = []
        restarts: int = 0
        active = "active"

    e = Env()

    def check(now=NOW):
        return wc.run_check(
            now=now,
            heartbeat_file=e.heartbeat,
            state_file=e.state,
            is_active=lambda: e.active,
            send=e.sent.append,
            restart=lambda: setattr(e, "restarts", e.restarts + 1),
        )

    e.check = check
    return e


def _touch(path, mtime):
    path.write_text("")
    os.utime(path, (mtime, mtime))


# ── stale 판정 3분기 ─────────────────────────────────────────


def test_fresh_heartbeat_is_ok(env):
    _touch(env.heartbeat, NOW - 10)
    assert "no transition (stale=False)" in env.check()
    assert env.sent == [] and env.restarts == 0


def test_old_heartbeat_is_stale(env):
    _touch(env.heartbeat, NOW - 61)
    action = env.check()
    assert "stale transition" in action
    assert env.restarts == 1
    assert len(env.sent) == 1 and "PROCESS_DOWN" in env.sent[0]
    assert "61" in env.sent[0]  # 나이 표기


def test_missing_heartbeat_is_stale(env):
    action = env.check()
    assert "stale transition" in action
    assert "하트비트 파일 없음" in env.sent[0]


def test_boundary_age_is_not_stale(env):
    _touch(env.heartbeat, NOW - wc.STALE_AFTER_SECONDS)  # 나이 == 임계 → 아직 ok
    assert "stale=False" in env.check()


# ── 전이 상태기계 (1회성 보장) ───────────────────────────────


def test_persistent_stale_alerts_and_restarts_once(env):
    _touch(env.heartbeat, NOW - 100)
    env.check()
    env.check()  # stale 지속 — 반복 재알림/재재시작 없음 (사용자 확정 정책)
    assert len(env.sent) == 1
    assert env.restarts == 1


def test_recovery_sends_resolution_once(env):
    _touch(env.heartbeat, NOW - 100)
    env.check()
    _touch(env.heartbeat, NOW - 5)  # 재시작 후 하트비트 재개
    env.check()
    env.check()  # ok 지속 — 해소 재알림 없음
    assert len(env.sent) == 2
    assert "해소" in env.sent[1]
    assert env.restarts == 1


def test_recovery_without_prior_stale_is_silent(env):
    _touch(env.heartbeat, NOW - 5)
    env.check()
    assert env.sent == []


def test_send_failure_does_not_block_restart(env):
    _touch(env.heartbeat, NOW - 100)

    def failing_send(text):
        raise OSError("network down")

    action = wc.run_check(
        now=NOW,
        heartbeat_file=env.heartbeat,
        state_file=env.state,
        is_active=lambda: "active",
        send=failing_send,
        restart=lambda: setattr(env, "restarts", env.restarts + 1),
    )
    assert "stale transition" in action
    assert env.restarts == 1  # 발송 실패해도 재시작은 수행 (§11.1 부분 실패 격리)
    assert env.state.read_text() == "stale"  # 전이는 기록 — 다음 주기 중복 방지


# ── 의도적 정지 스킵 ─────────────────────────────────────────


def test_intentionally_stopped_service_is_skipped(env):
    env.active = "inactive"
    # 하트비트가 stale이어도 (파일 부재) 유지보수 중 오알림/오재시작 없음
    assert "skipped" in env.check()
    assert env.sent == [] and env.restarts == 0


def test_failed_service_is_not_skipped(env):
    env.active = "failed"  # 재시작 반복 실패(디스크 풀 등) — 감지 대상
    assert "stale transition" in env.check()


# ── 시크릿/설정 파싱 ─────────────────────────────────────────


def test_parse_env_token():
    text = "# comment\nTELEGRAM_BOT_TOKEN=123:abc-DEF\nOTHER=x\n"
    assert wc.parse_env_token(text) == "123:abc-DEF"


def test_parse_env_token_missing():
    assert wc.parse_env_token("OTHER=x\n") is None


def test_parse_chat_id_quoted_negative():
    # 그룹 chat_id는 YAML에서 따옴표 필수 (M0 노트의 함정) — 따옴표 제거 확인
    text = 'telegram:\n  chat_id: "-1001234567890"   # 토큰은 env로\n'
    assert wc.parse_chat_id(text) == "-1001234567890"


def test_parse_chat_id_missing():
    assert wc.parse_chat_id("telegram: {}\n") is None
