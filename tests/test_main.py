import json
import logging
from pathlib import Path

from order_monitor.main import main

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


def test_main_loads_config_and_logs(tmp_path, monkeypatch):
    log_path = tmp_path / "app.log"
    monkeypatch.setattr(
        "sys.argv",
        ["order-monitor", "--config", str(EXAMPLE_CONFIG), "--log-file", str(log_path)],
    )

    main()
    logging.shutdown()

    lines = log_path.read_text().strip().splitlines()
    assert lines

    record = json.loads(lines[-1])
    assert record["message"] == "config loaded"
    assert record["exchange"] == "binance"
    assert record["symbol"] == "BTC/USDT"
