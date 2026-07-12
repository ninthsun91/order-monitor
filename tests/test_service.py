from decimal import Decimal
from pathlib import Path

import pytest

from order_monitor.config import load_config
from order_monitor.ingestion.events import (
    AggTradeEvent,
    DepthSnapshot,
    DiffDepthEvent,
    Side,
    stream_names,
)
from order_monitor.persistence.walls import WallStore
from order_monitor.service import MonitorService

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"
DEPTH, AGG, DIFF = stream_names("BTC/USDT")


@pytest.fixture
def service(tmp_path):
    svc = make_service(load_config(EXAMPLE_CONFIG), tmp_path)
    yield svc
    svc._store.close()


def make_service(config, tmp_path):
    svc = MonitorService(config, db_path=tmp_path / "monitor.db", telegram_token="test-token")
    svc.startup()
    return svc


def depth_event(bids=(("61000", "1.0"),), asks=(("61001", "1.0"),), mono=0.0):
    return DepthSnapshot(
        last_update_id=1,
        bids=tuple((Decimal(p), Decimal(q)) for p, q in bids),
        asks=tuple((Decimal(p), Decimal(q)) for p, q in asks),
        local_monotonic_receive_time=mono,
    )


def agg_event(price="61000", qty="0.5", mono=0.0):
    return AggTradeEvent(
        agg_trade_id=1,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=Side.SELL,
        exchange_time_ms=0,
        local_monotonic_receive_time=mono,
    )


def diff_event(first_id, final_id, bids=(), mono=0.0):
    return DiffDepthEvent(
        first_update_id=first_id,
        final_update_id=final_id,
        bids=tuple((Decimal(p), Decimal(q)) for p, q in bids),
        asks=(),
        exchange_time_ms=0,
        local_monotonic_receive_time=mono,
    )


def start_feed(svc):
    svc.on_connected()
    svc.on_event(DEPTH, depth_event())
    svc.on_event(AGG, agg_event())
    svc.on_event(DIFF, diff_event(100, 110, bids=[("60000", "1200")]))


def test_events_flow_into_state_and_epoch_starts(service):
    start_feed(service)

    assert service.order_book.best_bid == Decimal("61000")
    assert len(service.trade_window) == 1
    assert service.level_tracker.get(Side.BUY, Decimal("61000")).cum_traded_at_level == Decimal(
        "0.5"
    )
    assert service.wall_registry.get(Side.BUY, Decimal("60000")).last_qty == Decimal("1200")
    assert service.tracker.epoch_active
    # diff 적재가 DB에 미러링됨
    assert service._store.count() == 1


def test_diff_gap_marks_prior_walls_unconfirmed(service):
    start_feed(service)
    # U/u 갭 이벤트: 기존 벽(60000)은 unconfirmed, 이 이벤트의 가격(59000)은 확인 상태
    service.on_event(DIFF, diff_event(150, 160, bids=[("59000", "500")]))

    old_wall = service.wall_registry.get(Side.BUY, Decimal("60000"))
    new_wall = service.wall_registry.get(Side.BUY, Decimal("59000"))
    assert old_wall.unconfirmed
    assert not new_wall.unconfirmed
    stored = {w.price: w for w in service._store.load()}
    assert stored[Decimal("60000")].unconfirmed
    assert not stored[Decimal("59000")].unconfirmed


def test_disconnect_marks_registry_and_epoch(service):
    start_feed(service)
    service.on_disconnected()
    assert not service.tracker.epoch_active
    assert service.wall_registry.get(Side.BUY, Decimal("60000")).unconfirmed
    assert service._store.load()[0].unconfirmed


def test_wall_removal_synced_to_db(service):
    start_feed(service)
    service.on_event(DIFF, diff_event(111, 120, bids=[("60000", "0")]))
    assert service.wall_registry.get(Side.BUY, Decimal("60000")) is None
    assert service._store.count() == 0


def test_restart_restores_walls_as_unconfirmed(tmp_path):
    config = load_config(EXAMPLE_CONFIG)

    svc1 = make_service(config, tmp_path)
    start_feed(svc1)
    svc1._store.close()

    svc2 = make_service(config, tmp_path)
    wall = svc2.wall_registry.get(Side.BUY, Decimal("60000"))
    assert wall is not None and wall.unconfirmed  # 복원 직후 공백 마킹 (§12.1 규칙 1)
    assert svc2._store.load()[0].unconfirmed
    # 새 이벤트로 해제
    svc2.on_connected()
    svc2.on_event(DIFF, diff_event(500, 510, bids=[("60000", "1250")]))
    assert not svc2.wall_registry.get(Side.BUY, Decimal("60000")).unconfirmed
    svc2._store.close()


# ── M2 배선: 디텍터 판정 + 알림 (PRD §5.4 epoch 게이팅, §9.1) ──


def config_with(*, persist_seconds=None, send_d1=None):
    import dataclasses

    config = load_config(EXAMPLE_CONFIG)
    if persist_seconds is not None:
        config = dataclasses.replace(
            config, thresholds=dataclasses.replace(config.thresholds, persist_seconds=persist_seconds)
        )
    if send_d1 is not None:
        config = dataclasses.replace(
            config, alerts=dataclasses.replace(config.alerts, send_d1=send_d1)
        )
    return config


def test_d1_appeared_flows_to_alert_queue_when_enabled(tmp_path):
    svc = make_service(config_with(persist_seconds=1e-9, send_d1=True), tmp_path)
    start_feed(svc)
    # 다음 diff 이벤트가 평가를 트리거 — 60000 벽(1200 ≥ 1000)이 지속 필터 통과
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert svc.telegram.pending() == 1
    svc._store.close()


def test_d1_alert_gated_off_by_default(tmp_path):
    svc = make_service(config_with(persist_seconds=1e-9), tmp_path)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert svc.telegram.pending() == 0  # send_d1 기본 off (PRD §9.1)
    svc._store.close()


def test_d2_burst_flows_to_alert_queue(service):
    start_feed(service)
    service.on_event(AGG, agg_event(qty="60"))
    assert service.telegram.pending() == 0
    service.on_event(AGG, agg_event(qty="50"))  # 창 합계 110.5 ≥ 100
    assert service.telegram.pending() == 1


def test_detector_judgment_suspended_outside_epoch(service):
    # epoch 시작 전(세 스트림 수신 확인 전) — 상태는 적재되지만 판정은 보류
    service.on_connected()
    service.on_event(AGG, agg_event(qty="200"))
    assert len(service.trade_window) == 1
    assert service.telegram.pending() == 0
