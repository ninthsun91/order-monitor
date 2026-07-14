"""하트비트 파일 기록 (PRD §11.1, M5) — 기록기 + service 배선."""

import asyncio
import contextlib
import os
from pathlib import Path

import pytest

from order_monitor.config import load_config
from order_monitor.service import MonitorService
from order_monitor.watchdog.heartbeat import write_heartbeat

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


def test_write_heartbeat_creates_file(tmp_path):
    hb = tmp_path / "heartbeat"
    write_heartbeat(hb)
    assert hb.exists()


def test_write_heartbeat_advances_mtime(tmp_path):
    hb = tmp_path / "heartbeat"
    write_heartbeat(hb)
    os.utime(hb, (1000.0, 1000.0))
    write_heartbeat(hb)
    assert hb.stat().st_mtime > 1000.0


@pytest.mark.asyncio
async def test_heartbeat_loop_writes_immediately_on_start(tmp_path):
    hb = tmp_path / "heartbeat"
    svc = MonitorService(
        load_config(EXAMPLE_CONFIG),
        db_path=tmp_path / "m.db",
        telegram_token="test-token",
        heartbeat_path=hb,
    )
    task = asyncio.create_task(svc._heartbeat_loop())
    try:
        # 첫 기록은 첫 sleep(10s) 전에 일어난다 — 이벤트 루프 양보만으로 관측 가능
        await asyncio.sleep(0)
        assert hb.exists()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_heartbeat_loop_survives_write_failure(tmp_path):
    # 기록 실패(디렉터리 부재 등)는 파이프라인을 죽이지 않는다 — 외부 워치독이
    # stale 하트비트를 PROCESS_DOWN으로 승격하는 것이 설계된 에스컬레이션 경로
    svc = MonitorService(
        load_config(EXAMPLE_CONFIG),
        db_path=tmp_path / "m.db",
        telegram_token="test-token",
        heartbeat_path=tmp_path / "no-such-dir" / "heartbeat",
    )
    task = asyncio.create_task(svc._heartbeat_loop())
    try:
        await asyncio.sleep(0)
        assert not task.done()  # OSError를 삼키고 계속 돈다
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
