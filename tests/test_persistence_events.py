from decimal import Decimal

import pytest

from order_monitor.ingestion.events import Side
from order_monitor.persistence.events import EventStore


@pytest.fixture
def store(tmp_path):
    s = EventStore(tmp_path / "events.db")
    yield s
    s.close()


def test_record_roundtrip_with_side_price_promotion(store):
    fields = {
        "detector_event": "D1Appeared",
        "side": "buy",
        "price": "60000",
        "qty": "1200",
        "persisted_seconds": 12.5,
    }
    store.record("D1Appeared", Side.BUY, Decimal("60000"), fields, 1000.0)

    rows = store.rows()
    assert len(rows) == 1
    assert rows[0]["event_type"] == "D1Appeared"
    assert rows[0]["side"] == "buy"
    assert rows[0]["price"] == "60000"
    assert rows[0]["recorded_at"] == 1000.0
    assert rows[0]["payload"] == fields


def test_record_without_side_price_stores_null(store):
    # D2 요약·W 리포트류 — side/price 필드가 없는 이벤트
    store.record("D2BurstSummary", None, None, {"detector_event": "D2BurstSummary"}, 5.0)
    rows = store.rows()
    assert rows[0]["side"] is None
    assert rows[0]["price"] is None


def test_nested_decimal_serialized_like_log(store):
    # W 리포트의 중첩 dataclass — json default=str 규칙 (logging_setup과 동일)
    fields = {"detector_event": "WatchPeriodicReport", "data": {"cum_buy": Decimal("1.5")}}
    store.record("WatchPeriodicReport", None, None, fields, 1.0)
    assert store.rows()[0]["payload"]["data"]["cum_buy"] == "1.5"


def test_price_canonicalized(store):
    store.record("D1Removed", Side.SELL, Decimal("61000.00000000"), {}, 1.0)
    assert store.rows()[0]["price"] == "61000"


def test_rows_filter_by_event_type_and_order(store):
    store.record("D1Appeared", Side.BUY, Decimal("60000"), {"n": 1}, 1.0)
    store.record("D5Progress", Side.BUY, Decimal("60000"), {"n": 2}, 2.0)
    store.record("D1Appeared", Side.BUY, Decimal("59000"), {"n": 3}, 3.0)

    assert store.count() == 3
    d1_rows = store.rows("D1Appeared")
    assert [r["payload"]["n"] for r in d1_rows] == [1, 3]


def test_history_survives_reopen(tmp_path):
    # events 이력은 튜닝 데이터 — 재시작 후에도 그대로 남는다 (PRD §12.1)
    store1 = EventStore(tmp_path / "events.db")
    store1.record("D1Appeared", Side.BUY, Decimal("60000"), {}, 1.0)
    store1.close()

    store2 = EventStore(tmp_path / "events.db")
    assert store2.count() == 1
    store2.close()
