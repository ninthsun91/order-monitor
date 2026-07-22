from decimal import Decimal

import pytest

from order_monitor.ingestion.events import DiffDepthEvent, Side
from order_monitor.persistence.walls import WallStore
from order_monitor.state.wall_registry import Wall, WallRegistry

FLOOR = Decimal(100)
THRESHOLD = Decimal(1000)


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


def diff(bids=(), asks=()):
    return DiffDepthEvent(
        first_update_id=1,
        final_update_id=1,
        bids=tuple((Decimal(p), Decimal(q)) for p, q in bids),
        asks=tuple((Decimal(p), Decimal(q)) for p, q in asks),
        exchange_time_ms=0,
        local_monotonic_receive_time=0.0,
    )


def make_wall(price="61000", qty="1200", **kwargs):
    defaults = dict(
        price=Decimal(price),
        side=Side.BUY,
        last_qty=Decimal(qty),
        peak_qty=Decimal(qty),
        first_seen_at=100.0,
        first_seen_above_threshold=100.0,
        last_seen_at=100.0,
    )
    defaults.update(kwargs)
    return Wall(**defaults)


@pytest.fixture
def store(tmp_path):
    s = WallStore(tmp_path / "walls.db")
    yield s
    s.close()


def test_upsert_load_roundtrip(store):
    wall = make_wall(unconfirmed=True, unconfirmed_since=500.0)
    store.upsert(wall)
    loaded = store.load()
    assert len(loaded) == 1
    restored = loaded[0]
    assert restored == wall
    assert type(restored.price) is Decimal and type(restored.last_qty) is Decimal


def test_appeared_alerted_since_roundtrip(store):
    wall = make_wall(appeared_alerted_since=100.0)
    store.upsert(wall)
    assert store.load()[0].appeared_alerted_since == 100.0


def test_migration_adds_appeared_alerted_since_column(tmp_path):
    # v1.8 이전 스키마로 만든 DB가 열리면서 컬럼이 추가되고 기존 행은 None으로 읽힌다
    import sqlite3

    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE walls (
            side TEXT NOT NULL, price TEXT NOT NULL, last_qty TEXT NOT NULL,
            peak_qty TEXT NOT NULL, first_seen_at REAL NOT NULL,
            first_seen_above_threshold REAL, last_seen_at REAL NOT NULL,
            unconfirmed INTEGER NOT NULL DEFAULT 0, unconfirmed_since REAL,
            PRIMARY KEY (side, price))"""
    )
    conn.execute(
        "INSERT INTO walls VALUES ('buy', '61000', '1200', '1200', 100.0, 100.0, 100.0, 0, NULL)"
    )
    conn.commit()
    conn.close()

    s = WallStore(path)
    loaded = s.load()
    assert len(loaded) == 1 and loaded[0].appeared_alerted_since is None
    s.close()


def test_upsert_is_idempotent_across_price_representations(store):
    # "61000.00000000"과 "61000.0"은 같은 레벨 — 행이 갈라지면 안 된다
    store.upsert(make_wall(price="61000.00000000"))
    store.upsert(make_wall(price="61000.0", qty="900"))
    loaded = store.load()
    assert len(loaded) == 1
    assert loaded[0].last_qty == Decimal("900")


def test_delete_by_any_representation(store):
    store.upsert(make_wall(price="61000.00000000"))
    store.delete(Side.BUY, Decimal("61000.0"))
    assert store.count() == 0


def test_sides_are_distinct_rows(store):
    store.upsert(make_wall(side=Side.BUY))
    store.upsert(make_wall(side=Side.SELL))
    assert store.count() == 2


def test_mark_all_unconfirmed_preserves_existing_since(store):
    store.upsert(make_wall(price="61000"))  # confirmed
    store.upsert(make_wall(price="62000", unconfirmed=True, unconfirmed_since=300.0))
    store.mark_all_unconfirmed(since=900.0)
    by_price = {w.price: w for w in store.load()}
    assert by_price[Decimal("61000")].unconfirmed_since == 900.0
    assert by_price[Decimal("62000")].unconfirmed_since == 300.0  # 최초 마킹 유지
    assert all(w.unconfirmed for w in by_price.values())


def test_sync_diff_mirrors_registry(store):
    clock = FakeClock()
    reg = WallRegistry(record_min_qty=FLOOR, size_threshold=THRESHOLD, clock=clock)

    event = diff(bids=[("61000", "1200")], asks=[("65000", "150"), ("64000", "50")])
    removals = reg.apply_diff(event)
    store.sync_diff(reg, event, removals)
    assert store.count() == 2  # 64000은 하한 미만 신규 → 미등록·미기록

    # 소멸 미러링
    event2 = diff(bids=[("61000", "0")])
    removals2 = reg.apply_diff(event2)
    store.sync_diff(reg, event2, removals2)
    loaded = store.load()
    assert [w.price for w in loaded] == [Decimal("65000")]


def test_restart_restore_flow(tmp_path):
    # 세션 1: 벽 기록
    clock = FakeClock(now=100.0)
    store = WallStore(tmp_path / "walls.db")
    reg = WallRegistry(record_min_qty=FLOOR, size_threshold=THRESHOLD, clock=clock)
    event = diff(bids=[("61000", "1364.86")])
    store.sync_diff(reg, event, reg.apply_diff(event))
    store.close()

    # 세션 2: 복원 → 전체 unconfirmed 마킹 (PRD §12.1 규칙 1) → 이벤트로 해제
    clock2 = FakeClock(now=5000.0)
    store2 = WallStore(tmp_path / "walls.db")
    reg2 = WallRegistry(record_min_qty=FLOOR, size_threshold=THRESHOLD, clock=clock2)
    reg2.restore(store2.load())
    reg2.mark_all_unconfirmed()
    store2.mark_all_unconfirmed(since=clock2.now)

    wall = reg2.get(Side.BUY, Decimal("61000"))
    assert wall.unconfirmed and wall.unconfirmed_since == 5000.0
    assert wall.first_seen_above_threshold == 100.0  # 스푸핑 타이머 보존 (§12.1 규칙 2)
    assert store2.load()[0].unconfirmed

    event2 = diff(bids=[("61000", "1300")])
    store2.sync_diff(reg2, event2, reg2.apply_diff(event2))
    assert not store2.load()[0].unconfirmed  # 해제도 영속화됨
    store2.close()


def test_prune_mirror(tmp_path):
    ttl = 7 * 86400.0
    clock = FakeClock(now=0.0)
    store = WallStore(tmp_path / "walls.db")
    reg = WallRegistry(record_min_qty=FLOOR, size_threshold=THRESHOLD, clock=clock)
    event = diff(bids=[("61000", "1200")])
    store.sync_diff(reg, event, reg.apply_diff(event))
    reg.mark_all_unconfirmed()
    store.mark_all_unconfirmed(since=0.0)

    clock.now = ttl + 1.0
    pruned = reg.prune_unconfirmed(ttl)
    store.delete_walls(pruned)
    assert store.count() == 0
    store.close()
