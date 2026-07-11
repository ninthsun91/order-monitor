import json
import logging
from pathlib import Path

from order_monitor.main import main
from order_monitor.service import MonitorService

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


def test_main_loads_config_and_starts_service(tmp_path, monkeypatch):
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
        assert coro.cr_code.co_name == "run"  # MonitorService.run 코루틴
        coro.close()
        launched.append(True)

    monkeypatch.setattr("order_monitor.main.asyncio.run", fake_asyncio_run)

    main()
    logging.shutdown()

    assert launched == [True]
    lines = log_path.read_text().strip().splitlines()
    record = json.loads(lines[-1])
    assert record["message"] == "config loaded"
    assert record["symbol"] == "BTC/USDT"


def test_service_constructible_from_example_config(tmp_path):
    from order_monitor.config import load_config

    service = MonitorService(load_config(EXAMPLE_CONFIG), db_path=tmp_path / "m.db")
    assert service.tracker.epoch_active is False
