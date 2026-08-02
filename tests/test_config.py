import re
from pathlib import Path

import pytest

from order_monitor.config import ConfigError, load_config

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


def test_load_example_config():
    config = load_config(EXAMPLE_CONFIG)

    assert config.symbol == "BTC/USDT"
    assert config.thresholds.size_threshold_btc == 1000.0
    assert config.thresholds.absorb_multiple == 2.0  # D4 배수 판정 (PRD v1.11)
    assert config.thresholds.absorb_progress_step == 0.5
    assert config.thresholds.absorb_min_events == 5
    assert config.alerts.send_d4 is True  # D4 흡수 방어 알림 기본 on (PRD v1.11)
    assert config.alerts.send_d1 is True  # 기본 on (PRD v1.10 — 52230f2에서 임시 on으로 시작해 정식 승격)
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
    bad_config.write_text("symbol: BTC/USDT\n")

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
    # example의 토글 값에 무관하게 주입 (값 문자열 치환은 52230f2에서 한 번 깨졌음)
    bad_config.write_text(
        re.sub(r"send_d1: \S+", "send_d1: not-a-bool", EXAMPLE_CONFIG.read_text())
    )

    with pytest.raises(ConfigError, match="alerts.send_d1' must be a bool"):
        load_config(bad_config)


def test_unknown_key_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(EXAMPLE_CONFIG.read_text() + "\nunknown_key: 1\n")

    with pytest.raises(ConfigError, match="unknown top-level keys"):
        load_config(bad_config)


def test_ratio_out_of_range_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("realize_pct: 0.6", "realize_pct: 1.5")
    )

    with pytest.raises(ConfigError, match=r"thresholds.realize_pct' must be in \(0, 1\]"):
        load_config(bad_config)


def test_non_positive_value_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("persist_seconds: 3", "persist_seconds: -1")
    )

    with pytest.raises(ConfigError, match="thresholds.persist_seconds' must be positive"):
        load_config(bad_config)


def test_absorb_multiple_must_exceed_one(tmp_path):
    # PRD §10 v1.11 불변조건 — 배수 1 이하면 표시 크기만큼의 흡수로도 발화 (의미 조건 위반)
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("absorb_multiple: 2.0", "absorb_multiple: 1.0")
    )

    with pytest.raises(ConfigError, match="thresholds.absorb_multiple' must be > 1"):
        load_config(bad_config)


def test_stale_v1_10_keys_rejected(tmp_path):
    # v1.11 키 교체 — 구 config.yaml(iceberg_*·realize_pct_above 잔존)은 기동 거부
    # (엄격 스키마 — VPS 배포 시 config 동시 수정 필수의 실증 — config.example.yaml 대조)
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace(
            "absorb_multiple: 2.0", "iceberg_margin_btc: 20"
        )
    )

    # 엄격 스키마는 missing을 먼저 보고 — 어느 쪽이든 기동 거부가 계약
    with pytest.raises(ConfigError, match="missing required keys: absorb_multiple"):
        load_config(bad_config)


def test_record_min_above_size_threshold_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("record_min_qty_btc: 100", "record_min_qty_btc: 2000")
    )

    with pytest.raises(ConfigError, match="record_min_qty_btc' must be less than"):
        load_config(bad_config)


def test_heartbeat_not_below_stale_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("heartbeat_interval: 10", "heartbeat_interval: 30")
    )

    with pytest.raises(ConfigError, match="heartbeat_interval' must be less than"):
        load_config(bad_config)


def test_d2_v13_keys_loaded():
    config = load_config(EXAMPLE_CONFIG)
    assert config.thresholds.vol_floor_btc == 30.0
    assert config.thresholds.vol_multiplier == 10.0
    assert config.thresholds.vol_baseline_hours == 24.0
    assert config.thresholds.episode_exit_ratio == 0.5
    assert config.thresholds.episode_merge_minutes == 10.0
    assert config.alerts.send_d2_summary is True


def test_watch_keys_loaded():
    # PRD §10 v1.13 — W 주시 관측기 섹션
    config = load_config(EXAMPLE_CONFIG)
    assert config.watch.contact_band_pct == 0.001
    assert config.watch.confirm_timeframe == "15m"
    assert config.watch.confirm_closes == 2
    assert config.watch.invalidate_buffer_pct == 0.0025
    assert config.watch.report_interval_seconds == 600.0


def test_watch_missing_key_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("confirm_closes: 2", "")
    )

    with pytest.raises(ConfigError, match="watch' is missing required keys: confirm_closes"):
        load_config(bad_config)


def test_watch_unknown_key_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace(
            "  confirm_closes: 2", "  confirm_closes: 2\n  bogus_watch_key: 1"
        )
    )

    with pytest.raises(ConfigError, match="watch' has unknown keys: bogus_watch_key"):
        load_config(bad_config)


def test_watch_timeframe_enum_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace('confirm_timeframe: "15m"', 'confirm_timeframe: "5m"')
    )

    with pytest.raises(ConfigError, match="confirm_timeframe' must be '15m' or '1h'"):
        load_config(bad_config)


def test_watch_closes_zero_raises(tmp_path):
    # 공통 "> 0" 불변조건이 커버 — 0이면 즉시 무효화라 의미 조건 위반
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("confirm_closes: 2", "confirm_closes: 0")
    )

    with pytest.raises(ConfigError, match="watch.confirm_closes' must be positive"):
        load_config(bad_config)


def test_watch_closes_float_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("confirm_closes: 2", "confirm_closes: 1.5")
    )

    with pytest.raises(ConfigError, match="watch.confirm_closes' must be an int"):
        load_config(bad_config)


def test_watch_report_interval_floor_raises(tmp_path):
    # Telegram 초당 상한·스팸 하한 (PRD §10 v1.13)
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("report_interval_seconds: 600", "report_interval_seconds: 30")
    )

    with pytest.raises(ConfigError, match="report_interval_seconds' must be >= 60"):
        load_config(bad_config)


def test_delta_label_boundary_order_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("delta_balanced_ratio: 0.2", "delta_balanced_ratio: 0.6")
    )

    with pytest.raises(ConfigError, match="delta_balanced_ratio' must be less than"):
        load_config(bad_config)


def test_vol_multiplier_must_exceed_one(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace("vol_multiplier: 10", "vol_multiplier: 1")
    )

    with pytest.raises(ConfigError, match="vol_multiplier' must be > 1"):
        load_config(bad_config)


def test_telegram_command_chat_ids_loaded():
    # PRD §10 v1.14 — 명령 수신 허용 목록 (발송 대상과 분리)
    config = load_config(EXAMPLE_CONFIG)
    assert config.telegram.command_chat_ids == ["..."]


def test_telegram_command_chat_ids_empty_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace('command_chat_ids: ["..."]', "command_chat_ids: []")
    )

    with pytest.raises(ConfigError, match="command_chat_ids' must contain at least one"):
        load_config(bad_config)


def test_telegram_command_chat_ids_non_string_element_raises(tmp_path):
    # YAML에서 따옴표 없는 음수 id는 int로 파싱됨 — 문자열 강제 (기존 chat_id 관례와 동일 함정)
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace(
            'command_chat_ids: ["..."]', "command_chat_ids: [-1003954363679]"
        )
    )

    with pytest.raises(ConfigError, match=r"command_chat_ids\[0\]' must be a string"):
        load_config(bad_config)


def test_telegram_command_chat_ids_not_list_raises(tmp_path):
    bad_config = tmp_path / "config.yaml"
    bad_config.write_text(
        EXAMPLE_CONFIG.read_text().replace(
            'command_chat_ids: ["..."]', 'command_chat_ids: "..."'
        )
    )

    with pytest.raises(ConfigError, match="command_chat_ids' must be a list"):
        load_config(bad_config)


# ---- v1.16 exchanges 섹션 (PRD §10) ----


def test_exchanges_section_absent_yields_empty_dict(tmp_path):
    # 섹션 부재 = 바이낸스 단독 — 기존 배포 config 무변경 기동 (PRD §10 v1.16)
    text = EXAMPLE_CONFIG.read_text()
    lines = text.splitlines(keepends=True)
    start = next(i for i, line in enumerate(lines) if line.startswith("exchanges:"))
    end = next(i for i in range(start + 1, len(lines)) if lines[i].startswith("telegram:"))
    stripped = "".join(lines[:start] + lines[end:])
    path = tmp_path / "config.yaml"
    path.write_text(stripped)
    config = load_config(path)
    assert config.exchanges == {}


def test_exchanges_coinbase_parses(tmp_path):
    config = load_config(EXAMPLE_CONFIG)
    assert set(config.exchanges) == {"coinbase"}
    coinbase = config.exchanges["coinbase"]
    assert coinbase.symbol == "BTC-USD"
    assert coinbase.size_threshold_btc == 500.0
    assert coinbase.record_min_qty_btc == 50.0
    assert coinbase.band_pct == 0.20


def test_exchanges_unsupported_name_raises(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(EXAMPLE_CONFIG.read_text().replace("  coinbase:", "  kraken:"))
    with pytest.raises(ConfigError, match="unsupported exchanges: kraken"):
        load_config(path)


def test_exchanges_unknown_key_raises(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        EXAMPLE_CONFIG.read_text().replace("    band_pct: 0.20", "    band_pct: 0.20\n    bogus: 1")
    )
    with pytest.raises(ConfigError, match="exchanges.coinbase' has unknown keys: bogus"):
        load_config(path)


def test_exchanges_missing_key_raises(tmp_path):
    path = tmp_path / "config.yaml"
    lines = [
        line
        for line in EXAMPLE_CONFIG.read_text().splitlines(keepends=True)
        if not line.startswith("    record_min_qty_btc: 50")
    ]
    path.write_text("".join(lines))
    with pytest.raises(ConfigError, match="exchanges.coinbase' is missing required keys"):
        load_config(path)


def test_exchanges_record_min_above_threshold_raises(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        EXAMPLE_CONFIG.read_text().replace("    record_min_qty_btc: 50", "    record_min_qty_btc: 600")
    )
    with pytest.raises(ConfigError, match="exchanges.coinbase.record_min_qty_btc' must be less"):
        load_config(path)


def test_exchanges_band_pct_out_of_range_raises(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(EXAMPLE_CONFIG.read_text().replace("    band_pct: 0.20", "    band_pct: 1.5"))
    with pytest.raises(ConfigError, match="exchanges.coinbase.band_pct' must be in"):
        load_config(path)
