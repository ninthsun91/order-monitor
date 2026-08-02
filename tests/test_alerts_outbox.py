from decimal import Decimal

import pytest

from order_monitor.ingestion.events import Side
from order_monitor.persistence.alerts_outbox import AlertsOutboxStore


@pytest.fixture
def store(tmp_path):
    s = AlertsOutboxStore(tmp_path / "outbox.db")
    yield s
    s.close()


def test_record_load_unsent_roundtrip(store):
    rowid = store.record(Side.BUY, Decimal("61000"), "execution_confirmed", "text-1", 1000.0)
    assert rowid is not None
    assert store.load_unsent() == [(rowid, "text-1")]


def test_mark_sent_removes_from_unsent(store):
    rowid = store.record(Side.BUY, Decimal("61000"), "execution_confirmed", "text-1", 1000.0)
    store.mark_sent(rowid)
    assert store.load_unsent() == []
    assert store.count() == 1  # 행 자체는 유지 — 이력


def test_duplicate_record_within_process_is_ignored(store):
    first = store.record(Side.BUY, Decimal("61000"), "execution_confirmed", "text-1", 1000.0)
    second = store.record(Side.BUY, Decimal("61000"), "execution_confirmed", "text-2", 1000.0)
    assert second is None  # 동일 (side,price,state,recorded_at) — 방어용 무시
    assert store.count() == 1
    assert store.load_unsent() == [(first, "text-1")]


def test_different_recorded_at_creates_distinct_rows(store):
    # 같은 벽이 다시 등록→종결되면 recorded_at이 달라 별개 알림으로 취급
    first = store.record(Side.BUY, Decimal("61000"), "execution_confirmed", "text-1", 1000.0)
    second = store.record(Side.BUY, Decimal("61000"), "execution_confirmed", "text-2", 2000.0)
    assert first != second
    assert store.count() == 2


def test_price_representation_is_canonicalized(store):
    store.record(Side.BUY, Decimal("61000.00000000"), "execution_confirmed", "a", 1.0)
    dup = store.record(Side.BUY, Decimal("61000.0"), "execution_confirmed", "b", 1.0)
    assert dup is None
    assert store.count() == 1


def test_restart_resend_flow(tmp_path):
    # 세션 1: 확정 알림 선기록 후 프로세스 종료(발송 확인 전 크래시 가정)
    store1 = AlertsOutboxStore(tmp_path / "outbox.db")
    rowid = store1.record(Side.BUY, Decimal("61000"), "execution_confirmed", "확정 알림", 1000.0)
    store1.close()

    # 세션 2: 재시작 — 미발송 행이 그대로 재전송 대상으로 남아있음
    store2 = AlertsOutboxStore(tmp_path / "outbox.db")
    unsent = store2.load_unsent()
    assert unsent == [(rowid, "확정 알림")]
    store2.mark_sent(rowid)
    assert store2.load_unsent() == []
    store2.close()


def test_count_includes_sent_rows(store):
    rowid = store.record(Side.BUY, Decimal("61000"), "execution_confirmed", "text-1", 1000.0)
    store.mark_sent(rowid)
    store.record(Side.SELL, Decimal("62000"), "execution_inferred_above", "text-2", 2000.0)
    assert store.count() == 2
    assert len(store.load_unsent()) == 1


def test_migration_v1_16_preserves_unsent_rows(tmp_path):
    # v1.15 스키마(UNIQUE에 exchange 없음) — 미발송 행이 id 보존 복사로 살아남는다
    import sqlite3

    path = tmp_path / "v115.db"
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE alerts_outbox (
            id INTEGER PRIMARY KEY, side TEXT NOT NULL, price TEXT NOT NULL,
            terminal_state TEXT NOT NULL, text TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0, recorded_at REAL NOT NULL,
            UNIQUE (side, price, terminal_state, recorded_at))"""
    )
    conn.execute(
        "INSERT INTO alerts_outbox (id, side, price, terminal_state, text, sent, recorded_at)"
        " VALUES (7, 'buy', '61000', 'execution_confirmed', 'text-7', 0, 1000.0)"
    )
    conn.commit()
    conn.close()

    s = AlertsOutboxStore(path)
    assert s.load_unsent() == [(7, "text-7")]
    s.mark_sent(7)
    assert s.load_unsent() == []
    s.close()


def test_exchange_isolation(tmp_path):
    # 같은 (side, price, state, recorded_at)이라도 거래소가 다르면 별개 행
    path = tmp_path / "multi.db"
    binance = AlertsOutboxStore(path)
    coinbase = AlertsOutboxStore(path, exchange="coinbase")

    b_id = binance.record(Side.BUY, Decimal("61000"), "execution_confirmed", "b-text", 1000.0)
    c_id = coinbase.record(Side.BUY, Decimal("61000"), "execution_confirmed", "c-text", 1000.0)
    assert b_id is not None and c_id is not None

    assert binance.load_unsent() == [(b_id, "b-text")]
    assert coinbase.load_unsent() == [(c_id, "c-text")]
    binance.close()
    coinbase.close()
