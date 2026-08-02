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
    refill_window_ms: int
    absorb_multiple: float
    absorb_progress_step: float
    absorb_min_events: int
    realize_pct: float
    progress_step_pct: float


@dataclasses.dataclass(frozen=True)
class AlertsConfig:
    send_d1: bool
    send_d2: bool
    send_d2_summary: bool
    send_d4: bool
    send_d5_progress: bool
    send_wall_report: bool
    wall_report_interval_minutes: float
    bucket_size_usdt: float
    cooldown_seconds: float


@dataclasses.dataclass(frozen=True)
class TelegramConfig:
    chat_id: str  # 알림 발송 대상 (단일)
    command_chat_ids: list[str]  # v1.14 — 명령 수신 허용 목록 (§9.5, 발송 대상과 분리)


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
class WatchConfig:
    contact_band_pct: float
    confirm_timeframe: str
    confirm_closes: int
    invalidate_buffer_pct: float
    report_interval_seconds: float


@dataclasses.dataclass(frozen=True)
class ExchangeConfig:
    """v1.16 — 신규 거래소별 설정 (PRD §5.5·§10). 바이낸스는 기존 top-level 키 유지."""

    symbol: str
    size_threshold_btc: float
    record_min_qty_btc: float
    band_pct: float  # 레지스트리 등록 가격대역 (full-book 원거리 쓰레기 주문 차단)


@dataclasses.dataclass(frozen=True)
class AppConfig:
    symbol: str
    thresholds: ThresholdsConfig
    alerts: AlertsConfig
    wall_tracker: WallTrackerConfig
    watch: WatchConfig
    telegram: TelegramConfig
    watchdog: WatchdogConfig
    # v1.16 — 선택 섹션: 부재 시 빈 dict = 바이낸스 단독 (기존 config 무변경 기동 보장)
    exchanges: dict[str, ExchangeConfig] = dataclasses.field(default_factory=dict)


_TOP_LEVEL_STR_FIELDS = ("symbol",)
_TOP_LEVEL_SECTION_FIELDS = ("thresholds", "alerts", "wall_tracker", "watch", "telegram", "watchdog")
# v1.16 — M8은 coinbase만. kraken/bitfinex는 어댑터 지원 시 추가 (미지 거래소명은 거부)
_SUPPORTED_EXCHANGES = ("coinbase",)


def _check_value_type(section: str, name: str, value: object, expected_type: type) -> object:
    if typing.get_origin(expected_type) is list:
        (item_type,) = typing.get_args(expected_type)
        if not isinstance(value, list):
            raise ConfigError(f"'{section}.{name}' must be a list, got {type(value).__name__}")
        return [
            _check_value_type(section, f"{name}[{i}]", item, item_type)
            for i, item in enumerate(value)
        ]
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
        ("watch", config.watch),
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

    # D4 배수 판정의 의미 조건 (PRD §10 v1.11) — step·min_events 양수는 위 공통 검증이 커버
    if config.thresholds.absorb_multiple <= 1:
        raise ConfigError(f"'thresholds.absorb_multiple' must be > 1, got {config.thresholds.absorb_multiple}")

    if config.watchdog.heartbeat_interval >= config.watchdog.stale_seconds:
        raise ConfigError(
            "'watchdog.heartbeat_interval' must be less than 'watchdog.stale_seconds'"
        )

    # W 주시 관측기 (PRD §10 v1.13)
    if config.watch.confirm_timeframe not in ("15m", "1h"):
        raise ConfigError(
            f"'watch.confirm_timeframe' must be '15m' or '1h', got {config.watch.confirm_timeframe!r}"
        )
    for name in ("contact_band_pct", "invalidate_buffer_pct"):
        value = getattr(config.watch, name)
        if not 0 < value <= 1:
            raise ConfigError(f"'watch.{name}' must be in (0, 1], got {value}")
    if config.watch.report_interval_seconds < 60:
        raise ConfigError(
            f"'watch.report_interval_seconds' must be >= 60, got {config.watch.report_interval_seconds}"
        )

    # v1.14 — 빈 목록은 수신 기능 자체를 무의미하게 만드는 오설정
    if not config.telegram.command_chat_ids:
        raise ConfigError("'telegram.command_chat_ids' must contain at least one chat id")

    # v1.16 — 거래소별 섹션 (PRD §10)
    for name, exch in config.exchanges.items():
        if not exch.symbol:
            raise ConfigError(f"'exchanges.{name}.symbol' must be non-empty")
        if exch.size_threshold_btc <= 0 or exch.record_min_qty_btc <= 0:
            raise ConfigError(f"'exchanges.{name}' thresholds must be positive")
        if exch.record_min_qty_btc >= exch.size_threshold_btc:
            raise ConfigError(
                f"'exchanges.{name}.record_min_qty_btc' must be less than"
                f" 'exchanges.{name}.size_threshold_btc'"
            )
        if not 0 < exch.band_pct <= 1:
            raise ConfigError(f"'exchanges.{name}.band_pct' must be in (0, 1], got {exch.band_pct}")


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ConfigError("config file must contain a top-level mapping")

    # v1.16 — exchanges는 선택 섹션 (부재 = 바이낸스 단독, 기존 배포 config 무변경 기동)
    exchanges_raw = raw.pop("exchanges", None)
    exchanges: dict[str, ExchangeConfig] = {}
    if exchanges_raw is not None:
        if not isinstance(exchanges_raw, dict):
            raise ConfigError("'exchanges' must be a mapping")
        unknown = [key for key in exchanges_raw if key not in _SUPPORTED_EXCHANGES]
        if unknown:
            raise ConfigError(f"'exchanges' has unsupported exchanges: {', '.join(sorted(unknown))}")
        exchanges = {
            name: _build_section(ExchangeConfig, exchanges_raw[name], f"exchanges.{name}")
            for name in exchanges_raw
        }

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
        watch=_build_section(WatchConfig, raw["watch"], "watch"),
        telegram=_build_section(TelegramConfig, raw["telegram"], "telegram"),
        watchdog=_build_section(WatchdogConfig, raw["watchdog"], "watchdog"),
        exchanges=exchanges,
    )
    _validate_invariants(config)
    return config
