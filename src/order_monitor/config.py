from __future__ import annotations

import dataclasses
import typing
from pathlib import Path

import yaml


class ConfigError(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class ThresholdsConfig:
    size_threshold_btc: float
    persist_seconds: float
    exit_ratio: float
    fill_attribution: float
    vol_threshold_btc: float
    window_seconds: float
    burst_cooldown_seconds: float
    absorption_min_pct: float
    iceberg_margin_btc: float
    iceberg_min_trades: int
    realize_pct: float
    realize_pct_above: float
    intent_ttl_seconds: float
    progress_step_pct: float


@dataclasses.dataclass(frozen=True)
class AlertsConfig:
    send_d1: bool
    send_d2: bool
    send_d5_progress: bool
    bucket_size_usdt: float
    cooldown_seconds: float


@dataclasses.dataclass(frozen=True)
class TelegramConfig:
    chat_id: str


@dataclasses.dataclass(frozen=True)
class WatchdogConfig:
    stale_seconds: float
    heartbeat_interval: float


@dataclasses.dataclass(frozen=True)
class WallTrackerConfig:
    record_min_qty_btc: float
    ttl_days: float


@dataclasses.dataclass(frozen=True)
class AppConfig:
    exchange: str
    symbol: str
    depth_stream: str
    thresholds: ThresholdsConfig
    alerts: AlertsConfig
    wall_tracker: WallTrackerConfig
    telegram: TelegramConfig
    watchdog: WatchdogConfig


_TOP_LEVEL_STR_FIELDS = ("exchange", "symbol", "depth_stream")
_TOP_LEVEL_SECTION_FIELDS = ("thresholds", "alerts", "wall_tracker", "telegram", "watchdog")


def _check_value_type(section: str, name: str, value: object, expected_type: type) -> object:
    if expected_type is float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"'{section}.{name}' must be a number, got {type(value).__name__}")
        return float(value)
    if expected_type is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(f"'{section}.{name}' must be an int, got {type(value).__name__}")
        return value
    if expected_type is bool:
        if not isinstance(value, bool):
            raise ConfigError(f"'{section}.{name}' must be a bool, got {type(value).__name__}")
        return value
    if expected_type is str:
        if not isinstance(value, str):
            raise ConfigError(f"'{section}.{name}' must be a string, got {type(value).__name__}")
        return value
    raise ConfigError(f"unsupported field type {expected_type!r} for '{section}.{name}'")


def _build_section(cls: type, data: object, section: str) -> object:
    if not isinstance(data, dict):
        raise ConfigError(f"'{section}' must be a mapping")

    hints = typing.get_type_hints(cls)
    missing = [name for name in hints if name not in data]
    if missing:
        raise ConfigError(f"'{section}' is missing required keys: {', '.join(sorted(missing))}")

    extra = [name for name in data if name not in hints]
    if extra:
        raise ConfigError(f"'{section}' has unknown keys: {', '.join(sorted(extra))}")

    kwargs = {
        name: _check_value_type(section, name, data[name], expected_type)
        for name, expected_type in hints.items()
    }
    return cls(**kwargs)


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ConfigError("config file must contain a top-level mapping")

    known = _TOP_LEVEL_STR_FIELDS + _TOP_LEVEL_SECTION_FIELDS
    missing = [key for key in known if key not in raw]
    if missing:
        raise ConfigError(f"config is missing required top-level keys: {', '.join(missing)}")

    extra = [key for key in raw if key not in known]
    if extra:
        raise ConfigError(f"config has unknown top-level keys: {', '.join(sorted(extra))}")

    top_level_values = {
        name: _check_value_type("<root>", name, raw[name], str) for name in _TOP_LEVEL_STR_FIELDS
    }

    return AppConfig(
        **top_level_values,
        thresholds=_build_section(ThresholdsConfig, raw["thresholds"], "thresholds"),
        alerts=_build_section(AlertsConfig, raw["alerts"], "alerts"),
        wall_tracker=_build_section(WallTrackerConfig, raw["wall_tracker"], "wall_tracker"),
        telegram=_build_section(TelegramConfig, raw["telegram"], "telegram"),
        watchdog=_build_section(WatchdogConfig, raw["watchdog"], "watchdog"),
    )
