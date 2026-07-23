from decimal import Decimal

import pytest

from order_monitor.detectors.d5 import D5TerminalState
from order_monitor.ingestion.events import Side
from order_monitor.persistence.intents import STATE_ACTIVE, STATE_CONFIRMED, IntentStore

RUN1 = 1000.0
RUN2 = 2000.0


@pytest.fixture
def store(tmp_path):
    s = IntentStore(tmp_path / "intents.db")
    yield s
    s.close()


def test_register_creates_active_row(store):
    store.register(RUN1, 0, Side.BUY, Decimal("60000.00"), Decimal("1200"), 1001.0)
    rows = store.rows()
    assert len(rows) == 1
    assert rows[0]["state"] == STATE_ACTIVE
    assert rows[0]["price"] == "60000"  # canonical
    assert rows[0]["registered_qty"] == "1200"
    assert rows[0]["confirmed_at"] is None
    assert rows[0]["level_realized_rate"] is None


def test_mark_confirmed_keeps_row_open(store):
    store.register(RUN1, 0, Side.BUY, Decimal("60000"), Decimal("1200"), 1001.0)
    store.mark_confirmed(RUN1, 0, Decimal("0.6"), 1005.0)
    row = store.rows()[0]
    assert row["state"] == STATE_CONFIRMED
    assert row["confirmed_at"] == 1005.0
    assert row["level_realized_rate"] == "0.6"


def test_finalize_records_terminal_state(store):
    store.register(RUN1, 0, Side.BUY, Decimal("60000"), Decimal("1200"), 1001.0)
    store.finalize(RUN1, 0, D5TerminalState.INTENT_WITHDRAWN.value, Decimal("0.01"), 1010.0)
    row = store.rows()[0]
    assert row["state"] == "intent_withdrawn"
    assert row["level_realized_rate"] == "0.01"


def test_same_intent_id_across_runs_distinct(store):
    # intent_id는 런마다 0부터 — run_started_at 조합으로만 유일 (alerts_outbox와 동일 원리)
    store.register(RUN1, 0, Side.BUY, Decimal("60000"), Decimal("1200"), 1001.0)
    store.register(RUN2, 0, Side.SELL, Decimal("65000"), Decimal("1500"), 2001.0)
    assert len(store.rows()) == 2


def test_mark_interrupted_at_startup_marks_open_rows_only(store):
    store.register(RUN1, 0, Side.BUY, Decimal("60000"), Decimal("1200"), 1001.0)  # active
    store.register(RUN1, 1, Side.BUY, Decimal("59000"), Decimal("1100"), 1002.0)
    store.mark_confirmed(RUN1, 1, Decimal("0.7"), 1005.0)  # confirmed 래치 — 열린 상태
    store.register(RUN1, 2, Side.SELL, Decimal("65000"), Decimal("1300"), 1003.0)
    store.finalize(RUN1, 2, D5TerminalState.CONFIRMED_CLOSED.value, Decimal("1.2"), 1010.0)

    marked = store.mark_interrupted_at_startup(2000.0)

    assert marked == 2  # active + confirmed — 종국 행은 제외
    states = {row["intent_id"]: row["state"] for row in store.rows()}
    assert states[0] == D5TerminalState.INTERRUPTED.value
    assert states[1] == D5TerminalState.INTERRUPTED.value
    assert states[2] == "confirmed_closed"  # 불변
    # confirmed_at은 튜닝 데이터로 보존 (마킹은 state만)
    assert store.rows()[1]["confirmed_at"] == 1005.0


def test_mark_interrupted_survives_reopen(tmp_path):
    # 크래시 시나리오: 종국 기록 없이 프로세스 종료 → 다음 기동에서 마킹
    store1 = IntentStore(tmp_path / "intents.db")
    store1.register(RUN1, 0, Side.BUY, Decimal("60000"), Decimal("1200"), 1001.0)
    store1.close()

    store2 = IntentStore(tmp_path / "intents.db")
    assert store2.mark_interrupted_at_startup(RUN2) == 1
    assert store2.rows()[0]["state"] == D5TerminalState.INTERRUPTED.value
    assert store2.mark_interrupted_at_startup(RUN2 + 1) == 0  # 멱등 — 재마킹 없음
    store2.close()
