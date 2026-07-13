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
    vol_floor_btc: float
    vol_multiplier: float
    vol_baseline_hours: float
    window_seconds: float
    episode_exit_ratio: float
    episode_merge_minutes: float
    delta_directional_ratio: float
    delta_balanced_ratio: float
    summary_absorb_delta_min: float
    summary_move_min_pct: float
    absorption_min_pct: float
    pierce_persist_snapshots: int
    iceberg_margin_btc: float
    iceberg_min_trades: int
    refill_window_ms: int
    realize_pct: float
    realize_pct_above: float
    progress_step_pct: float


@dataclasses.dataclass(frozen=True)
class AlertsConfig:
    send_d1: bool
    send_d2: bool
    send_d2_summary: bool
    send_d5_progress: bool
    send_wall_report: bool
    wall_report_interval_minutes: float
    bucket_size_usdt: float
    cooldown_seconds: float


@dataclasses.dataclass(frozen=True)
class TelegramConfig:
    chat_id: str


@dataclasses.dataclass(frozen=True)
class WatchdogConfig:
    stale_seconds: float
    trade_stale_seconds: float
    heartbeat_interval: float


@dataclasses.dataclass(frozen=True)
class WallTrackerConfig:
    record_min_qty_btc: float
    ttl_days: float


@dataclasses.dataclass(frozen=True)
class AppConfig:
    symbol: str
    thresholds: ThresholdsConfig
    alerts: AlertsConfig
    wall_tracker: WallTrackerConfig
    telegram: TelegramConfig
    watchdog: WatchdogConfig


_TOP_LEVEL_STR_FIELDS = ("symbol",)
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


_RATIO_FIELDS = (
    "exit_ratio",
    "fill_attribution",
    "absorption_min_pct",
    "realize_pct",
    "realize_pct_above",
    "progress_step_pct",
    "episode_exit_ratio",
    "delta_directional_ratio",
    "delta_balanced_ratio",
    "summary_absorb_delta_min",
)


def _validate_invariants(config: AppConfig) -> None:
    sections = (
        ("thresholds", config.thresholds),
        ("alerts", config.alerts),
        ("wall_tracker", config.wall_tracker),
        ("watchdog", config.watchdog),
    )
    for section_name, section in sections:
        for name, value in dataclasses.asdict(section).items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            if value <= 0:
                raise ConfigError(f"'{section_name}.{name}' must be positive, got {value}")

    for name in _RATIO_FIELDS:
        value = getattr(config.thresholds, name)
        if not 0 < value <= 1:
            raise ConfigError(f"'thresholds.{name}' must be in (0, 1], got {value}")

    if config.wall_tracker.record_min_qty_btc >= config.thresholds.size_threshold_btc:
        raise ConfigError(
            "'wall_tracker.record_min_qty_btc' must be less than 'thresholds.size_threshold_btc'"
        )

    if config.thresholds.delta_balanced_ratio >= config.thresholds.delta_directional_ratio:
        raise ConfigError(
            "'thresholds.delta_balanced_ratio' must be less than 'thresholds.delta_directional_ratio'"
        )

    # 요약 판정의 흡수/관철 대상 하한은 양방향 경계보다 높아야 판정 구간이 성립
    if config.thresholds.summary_absorb_delta_min <= config.thresholds.delta_balanced_ratio:
        raise ConfigError(
            "'thresholds.summary_absorb_delta_min' must be greater than 'thresholds.delta_balanced_ratio'"
        )

    if config.thresholds.vol_multiplier <= 1:
        raise ConfigError(f"'thresholds.vol_multiplier' must be > 1, got {config.thresholds.vol_multiplier}")

    if config.watchdog.heartbeat_interval >= config.watchdog.stale_seconds:
        raise ConfigError(
            "'watchdog.heartbeat_interval' must be less than 'watchdog.stale_seconds'"
        )


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

    config = AppConfig(
        **top_level_values,
        thresholds=_build_section(ThresholdsConfig, raw["thresholds"], "thresholds"),
        alerts=_build_section(AlertsConfig, raw["alerts"], "alerts"),
        wall_tracker=_build_section(WallTrackerConfig, raw["wall_tracker"], "wall_tracker"),
        telegram=_build_section(TelegramConfig, raw["telegram"], "telegram"),
        watchdog=_build_section(WatchdogConfig, raw["watchdog"], "watchdog"),
    )
    _validate_invariants(config)
    return config
