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
    svc = MonitorService(load_config(EXAMPLE_CONFIG), db_path=tmp_path / "monitor.db")
    svc.startup()
    yield svc
    svc._store.close()


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
    db = tmp_path / "monitor.db"
    config = load_config(EXAMPLE_CONFIG)

    svc1 = MonitorService(config, db_path=db)
    svc1.startup()
    start_feed(svc1)
    svc1._store.close()

    svc2 = MonitorService(config, db_path=db)
    svc2.startup()
    wall = svc2.wall_registry.get(Side.BUY, Decimal("60000"))
    assert wall is not None and wall.unconfirmed  # 복원 직후 공백 마킹 (§12.1 규칙 1)
    assert svc2._store.load()[0].unconfirmed
    # 새 이벤트로 해제
    svc2.on_connected()
    svc2.on_event(DIFF, diff_event(500, 510, bids=[("60000", "1250")]))
    assert not svc2.wall_registry.get(Side.BUY, Decimal("60000")).unconfirmed
    svc2._store.close()
