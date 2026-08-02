from decimal import Decimal

from order_monitor.ingestion.events import DiffDepthEvent, Side
from order_monitor.state.wall_registry import RemovalReason, Wall, WallRegistry

FLOOR = Decimal(100)
THRESHOLD = Decimal(1000)


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


def registry(clock=None):
    return WallRegistry(
        record_min_qty=FLOOR, size_threshold=THRESHOLD, clock=clock or FakeClock()
    )


def diff(bids=(), asks=(), first_id=1, final_id=1):
    return DiffDepthEvent(
        first_update_id=first_id,
        final_update_id=final_id,
        bids=tuple((Decimal(p), Decimal(q)) for p, q in bids),
        asks=tuple((Decimal(p), Decimal(q)) for p, q in asks),
        exchange_time_ms=0,
        local_monotonic_receive_time=0.0,
    )


class TestRegistrationGate:
    def test_new_price_below_floor_not_registered(self):
        reg = registry()
        result = reg.apply_diff(diff(bids=[("61000", "99.9")]))
        assert len(reg) == 0
        assert result.registrations == []

    def test_new_price_at_floor_registered(self):
        reg = registry()
        result = reg.apply_diff(diff(bids=[("61000", "100")]))
        wall = reg.get(Side.BUY, Decimal("61000"))
        assert wall.last_qty == Decimal("100")
        assert wall.first_seen_above_threshold is None  # 임계 미만
        assert result.registrations == [wall]  # 등록 반환 — 궤적 로깅·D4 스트릭 소비

    def test_tracked_update_not_reported_as_registration(self):
        reg = registry()
        reg.apply_diff(diff(bids=[("61000", "500")]))
        result = reg.apply_diff(diff(bids=[("61000", "800")]))
        assert result.registrations == [] and result.removals == []

    def test_both_sides_tracked(self):
        reg = registry()
        reg.apply_diff(diff(bids=[("61000", "1364.86")], asks=[("65000", "102")]))
        assert reg.get(Side.BUY, Decimal("61000")) is not None
        assert reg.get(Side.SELL, Decimal("65000")) is not None

    def test_first_seen_above_threshold_set_on_registration(self):
        clock = FakeClock(now=5000.0)
        reg = registry(clock)
        reg.apply_diff(diff(bids=[("61000", "1364.86")]))
        wall = reg.get(Side.BUY, Decimal("61000"))
        assert wall.first_seen_above_threshold == 5000.0
        assert wall.first_seen_at == 5000.0


class TestTrackedPriceUpdates:
    def test_ghost_wall_prevention(self):
        # PRD §8 D1 v1.2: 1200 → 50 하락이 "하한 미만"으로 무시되면 유령 벽 잔존
        reg = registry()
        reg.apply_diff(diff(bids=[("61000", "1200")]))
        removals = reg.apply_diff(diff(bids=[("61000", "50")])).removals
        assert len(removals) == 1
        assert removals[0].reason is RemovalReason.BELOW_FLOOR
        assert removals[0].wall.last_qty == Decimal("50")  # 최종 관측값 반영
        assert removals[0].wall.peak_qty == Decimal("1200")
        assert reg.get(Side.BUY, Decimal("61000")) is None

    def test_tombstone(self):
        reg = registry()
        reg.apply_diff(diff(bids=[("61000", "1200")]))
        removals = reg.apply_diff(diff(bids=[("61000", "0")])).removals
        assert removals[0].reason is RemovalReason.TOMBSTONE
        assert removals[0].wall.last_qty == Decimal(0)
        assert len(reg) == 0

    def test_peak_qty_tracks_maximum(self):
        reg = registry()
        reg.apply_diff(diff(bids=[("61000", "500")]))
        reg.apply_diff(diff(bids=[("61000", "1500")]))
        reg.apply_diff(diff(bids=[("61000", "800")]))
        wall = reg.get(Side.BUY, Decimal("61000"))
        assert wall.peak_qty == Decimal("1500")
        assert wall.last_qty == Decimal("800")

    def test_threshold_timer_set_and_reset_on_dip(self):
        # PRD §8 D1 조건 3: 임계 밑으로 내려갔다 다시 올라오면 리셋
        clock = FakeClock(now=100.0)
        reg = registry(clock)
        reg.apply_diff(diff(bids=[("61000", "1200")]))
        wall = reg.get(Side.BUY, Decimal("61000"))
        assert wall.first_seen_above_threshold == 100.0

        clock.now = 200.0
        reg.apply_diff(diff(bids=[("61000", "1300")]))
        assert wall.first_seen_above_threshold == 100.0  # 유지 (최초 시각)

        clock.now = 300.0
        reg.apply_diff(diff(bids=[("61000", "900")]))  # 임계 미만, 하한 이상
        assert wall.first_seen_above_threshold is None
        assert len(reg) == 1  # 소멸 아님

        clock.now = 400.0
        reg.apply_diff(diff(bids=[("61000", "1100")]))
        assert wall.first_seen_above_threshold == 400.0  # 재계측


class TestUnconfirmed:
    def test_mark_all_and_clear_by_event(self):
        clock = FakeClock(now=100.0)
        reg = registry(clock)
        reg.apply_diff(diff(bids=[("61000", "1200")], asks=[("65000", "150")]))

        clock.now = 200.0
        reg.mark_all_unconfirmed()
        assert all(w.unconfirmed and w.unconfirmed_since == 200.0 for w in reg.walls())

        # 어떤 값이든 이벤트 1건이면 해제 (PRD §12.1 규칙 3)
        clock.now = 300.0
        reg.apply_diff(diff(bids=[("61000", "1100")]))
        bid_wall = reg.get(Side.BUY, Decimal("61000"))
        assert not bid_wall.unconfirmed and bid_wall.unconfirmed_since is None
        assert reg.get(Side.SELL, Decimal("65000")).unconfirmed  # 다른 벽은 그대로

    def test_remark_keeps_original_unconfirmed_since(self):
        clock = FakeClock(now=100.0)
        reg = registry(clock)
        reg.apply_diff(diff(bids=[("61000", "1200")]))
        clock.now = 200.0
        reg.mark_all_unconfirmed()
        clock.now = 500.0
        reg.mark_all_unconfirmed()
        assert reg.get(Side.BUY, Decimal("61000")).unconfirmed_since == 200.0

    def test_first_seen_above_threshold_preserved_while_unconfirmed(self):
        # PRD §12.1 규칙 2 — 스푸핑 타이머 리셋 금지
        clock = FakeClock(now=100.0)
        reg = registry(clock)
        reg.apply_diff(diff(bids=[("61000", "1200")]))
        clock.now = 200.0
        reg.mark_all_unconfirmed()
        assert reg.get(Side.BUY, Decimal("61000")).first_seen_above_threshold == 100.0


class TestPrune:
    def test_prunes_only_expired_unconfirmed(self):
        ttl = 7 * 86400.0
        clock = FakeClock(now=0.0)
        reg = registry(clock)
        reg.apply_diff(diff(bids=[("61000", "1200")]))
        reg.mark_all_unconfirmed()

        clock.now = 1000.0
        reg.apply_diff(diff(bids=[("62000", "500")]))  # confirmed 벽

        clock.now = ttl + 1.0
        pruned = reg.prune_unconfirmed(ttl)
        assert [w.price for w in pruned] == [Decimal("61000")]
        # 확인된 벽은 무기한 유지 (PRD §12.1 — 무이벤트 = 유효)
        assert reg.get(Side.BUY, Decimal("62000")) is not None

    def test_unexpired_unconfirmed_kept(self):
        clock = FakeClock(now=0.0)
        reg = registry(clock)
        reg.apply_diff(diff(bids=[("61000", "1200")]))
        reg.mark_all_unconfirmed()
        clock.now = 3600.0
        assert reg.prune_unconfirmed(7 * 86400.0) == []
        assert len(reg) == 1


class TestRestore:
    def test_restore_then_mark_unconfirmed(self):
        reg = registry(FakeClock(now=9000.0))
        reg.restore(
            [
                Wall(
                    price=Decimal("61000"),
                    side=Side.BUY,
                    last_qty=Decimal("1200"),
                    peak_qty=Decimal("1300"),
                    first_seen_at=100.0,
                    first_seen_above_threshold=150.0,
                    last_seen_at=8000.0,
                )
            ]
        )
        reg.mark_all_unconfirmed()
        wall = reg.get(Side.BUY, Decimal("61000"))
        assert wall.unconfirmed and wall.unconfirmed_since == 9000.0
        assert wall.first_seen_above_threshold == 150.0  # 보존


# ---- v1.16 등록 가격대역 게이트 (PRD §5.5) ----


def band_registry(mid, band="0.20"):
    return WallRegistry(
        record_min_qty=FLOOR,
        size_threshold=THRESHOLD,
        clock=FakeClock(),
        band_pct=Decimal(band),
        mid_price_supplier=lambda: mid,
    )


def test_band_rejects_far_garbage_registration():
    # Coinbase full-book 실측 사례: $0.01에 65,000 BTC 매수 주문 — 등록 거부
    r = band_registry(mid=Decimal("62900"))
    result = r.apply_diff(diff(bids=[("0.01", "65000")]))
    assert result.registrations == [] and len(r) == 0


def test_band_accepts_in_band_registration():
    r = band_registry(mid=Decimal("62900"))
    result = r.apply_diff(diff(bids=[("60000", "150")]))
    assert len(result.registrations) == 1 and len(r) == 1


def test_band_boundary_is_inclusive():
    r = band_registry(mid=Decimal("50000"))
    result = r.apply_diff(diff(bids=[("40000", "150")], asks=[("60000", "150")]))
    assert len(result.registrations) == 2


def test_band_none_mid_allows_registration():
    # 기동 직후 ticker 미수신 (mid=None) — 관측 우선으로 등록 허용
    r = WallRegistry(
        record_min_qty=FLOOR,
        size_threshold=THRESHOLD,
        clock=FakeClock(),
        band_pct=Decimal("0.20"),
        mid_price_supplier=lambda: None,
    )
    assert len(r.apply_diff(diff(bids=[("0.01", "65000")])).registrations) == 1


def test_band_zero_disables_gate():
    # band_pct=0 (바이낸스 기본) — 종전 동작
    r = registry()
    assert len(r.apply_diff(diff(bids=[("0.01", "65000")])).registrations) == 1


def test_tracked_wall_updates_and_tombstone_ignore_band():
    # 추적 중 가격은 대역 밖으로 벗어나도(mid 이동) 갱신·tombstone 전부 처리 — 유령 벽 방지
    mid = {"v": Decimal("62900")}
    r = WallRegistry(
        record_min_qty=FLOOR,
        size_threshold=THRESHOLD,
        clock=FakeClock(),
        band_pct=Decimal("0.20"),
        mid_price_supplier=lambda: mid["v"],
    )
    r.apply_diff(diff(bids=[("60000", "150")]))
    mid["v"] = Decimal("100000")  # 60000이 대역(80k~120k) 밖으로
    r.apply_diff(diff(bids=[("60000", "200")]))
    assert r.get(Side.BUY, Decimal("60000")).last_qty == Decimal("200")
    removals = r.apply_diff(diff(bids=[("60000", "0")])).removals
    assert len(removals) == 1 and removals[0].reason is RemovalReason.TOMBSTONE
    assert len(r) == 0


def test_band_snapshot_derived_mid_rejects_garbage_before_ticker():
    # 기동 직후: supplier mid 미확보 상태에서 양측 full snapshot 도착 —
    # 이벤트 자체 유도 mid로 원거리 쓰레기 주문 등록을 거부해야 한다 (기동 오알림 방어)
    r = WallRegistry(
        record_min_qty=FLOOR,
        size_threshold=THRESHOLD,
        clock=FakeClock(),
        band_pct=Decimal("0.20"),
        mid_price_supplier=lambda: None,
    )
    result = r.apply_diff(
        diff(
            bids=[("62900", "1.0"), ("60000", "150"), ("0.01", "65000")],
            asks=[("62901", "1.0"), ("65000", "200")],
        )
    )
    prices = {w.price for w in result.registrations}
    assert prices == {Decimal("60000"), Decimal("65000")}
    assert r.get(Side.BUY, Decimal("0.01")) is None
