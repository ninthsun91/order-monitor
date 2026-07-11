from pathlib import Path

import pytest

from order_monitor.config import ConfigError, load_config

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


def test_load_example_config():
    config = load_config(EXAMPLE_CONFIG)

    assert config.exchange == "binance"
    assert config.symbol == "BTC/USDT"
    assert config.depth_stream == "depth20@100ms"
    assert config.thresholds.size_threshold_btc == 1000.0
    assert config.thresholds.iceberg_min_trades == 5
    assert config.alerts.send_d1 is False
    assert config.alerts.send_d2 is True
    assert config.wall_tracker.record_min_qty_btc == 100.0
    assert config.wall_tracker.ttl_days == 7.0
    assert config.telegram.chat_id == "..."
    assert config.watchdog.stale_seconds == 30.0


def test_missing_file_raises():
    with pytest.raises(ConfigError, match="not found"):
        load_config("/nonexistent/config.yaml")


def test_missing_top_level_key_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text("exchange: binance\nsymbol: BTC/USDT\n")

    with pytest.raises(ConfigError, match="missing required top-level keys"):
        load_config(bad_config)


def test_missing_section_key_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("size_threshold_btc: 1000", "")
    )

    with pytest.raises(ConfigError, match="thresholds' is missing required keys"):
        load_config(bad_config)


def test_missing_wall_tracker_key_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("ttl_days: 7", "")
    )

    with pytest.raises(ConfigError, match="wall_tracker' is missing required keys"):
        load_config(bad_config)


def test_wall_tracker_wrong_type_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("record_min_qty_btc: 100", "record_min_qty_btc: many")
    )

    with pytest.raises(ConfigError, match="wall_tracker.record_min_qty_btc' must be a number"):
        load_config(bad_config)


def test_wall_tracker_unknown_key_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace(
            "  ttl_days: 7", "  ttl_days: 7\n  bogus_key: 1"
        )
    )

    with pytest.raises(ConfigError, match="wall_tracker' has unknown keys: bogus_key"):
        load_config(bad_config)


def test_wrong_type_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("send_d1: false", "send_d1: not-a-bool")
    )

    with pytest.raises(ConfigError, match="alerts.send_d1' must be a bool"):
        load_config(bad_config)


def test_unknown_key_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(EXAMPLE_CONFIG.read_text() + "\nunknown_key: 1\n")

    with pytest.raises(ConfigError, match="unknown top-level keys"):
        load_config(bad_config)
