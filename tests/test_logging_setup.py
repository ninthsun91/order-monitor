import json
import logging

from order_monitor.logging_setup import setup_logging


def test_log_line_is_valid_json(tmp_path):
    log_path = tmp_path / "app.log"
    setup_logging(log_path, also_stdout=False)

    logger = logging.getLogger("order_monitor.test")
    logger.info("depth event received", extra={"event": "depth", "symbol": "BTC/USDT"})
    logging.shutdown()

    lines = log_path.read_text().strip().splitlines()
    assert len(lines) == 1

    record = json.loads(lines[0])
    assert record["message"] == "depth event received"
    assert record["level"] == "INFO"
    assert record["logger"] == "order_monitor.test"
    assert record["event"] == "depth"
    assert record["symbol"] == "BTC/USDT"


def test_setup_logging_is_idempotent(tmp_path):
    log_path = tmp_path / "app.log"
    setup_logging(log_path, also_stdout=False)
    setup_logging(log_path, also_stdout=False)

    root = logging.getLogger()
    assert len(root.handlers) == 1
