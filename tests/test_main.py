import json
import logging
from pathlib import Path

import pytest

from order_monitor.main import main
from order_monitor.service import MonitorService

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


def test_main_loads_config_and_starts_service(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    log_path = tmp_path / "app.log"
    monkeypatch.setattr(
        "sys.argv",
        [
            "order-monitor",
            "--config", str(EXAMPLE_CONFIG),
            "--log-file", str(log_path),
            "--db-file", str(tmp_path / "monitor.db"),
        ],
    )

    launched = []

    def fake_asyncio_run(coro):
        assert coro.cr_code.co_name == "run_all"  # 전 파이프라인 러너 (M8 — 바이낸스 + exchanges)
        coro.close()
        launched.append(True)

    monkeypatch.setattr("order_monitor.main.asyncio.run", fake_asyncio_run)

    attached = []
    orig_attach = MonitorService.attach_report_peer

    def spy_attach(self, peer):
        attached.append((self._exchange, peer._exchange))
        orig_attach(self, peer)

    monkeypatch.setattr(MonitorService, "attach_report_peer", spy_attach)

    main()
    logging.shutdown()

    assert launched == [True]
    # 형제 벽의 리포트 합류 배선 (2026-08-03) — 프라이머리에 coinbase peer 부착
    assert attached == [("binance", "coinbase")]
    lines = log_path.read_text().strip().splitlines()
    record = json.loads(lines[-1])
    assert record["message"] == "config loaded"
    assert record["exchanges"] == ["binance", "coinbase"]  # v1.17 — symbol은 exchanges로 이동


def test_main_refuses_to_start_without_telegram_token(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setattr("sys.argv", ["order-monitor", "--config", str(EXAMPLE_CONFIG)])
    with pytest.raises(SystemExit):
        main()


def test_service_constructible_from_example_config(tmp_path):
    from order_monitor.config import load_config

    service = MonitorService(
        load_config(EXAMPLE_CONFIG), db_path=tmp_path / "m.db", telegram_token="test-token"
    )
    assert service.tracker.epoch_active is False
