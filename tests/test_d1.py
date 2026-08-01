"""D1 대형 잔량 발생/제거 (PRD §8 D1) — 지속 필터·FILLED/PULLED 분기·억제."""

from decimal import Decimal

from order_monitor.detectors.d1 import D1Appeared, D1Attribution, D1Detector, D1Removed, D1Suppressed
from order_monitor.ingestion.events import Side
from order_monitor.state.wall_registry import RemovalReason, Wall, WallRemoval


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


def make_wall(price="61000", qty="1200", side=Side.BUY, first_above=0.0, unconfirmed=False):
    qty = Decimal(qty)
    return Wall(
        price=Decimal(price),
        side=side,
        last_qty=qty,
        peak_qty=qty,
        first_seen_at=0.0,
        first_seen_above_threshold=first_above,
        last_seen_at=0.0,
        unconfirmed=unconfirmed,
    )


def make_detector(clock, cum_traded=Decimal(0)):
    return D1Detector(
        size_threshold=Decimal(1000),
        persist_seconds=3.0,
        exit_ratio=Decimal("0.5"),
        fill_attribution=Decimal("0.7"),
        cum_traded_lookup=lambda side, price: cum_traded,
        clock=clock,
    )


# ── APPEARED: 지속 필터 (스푸핑 배제) ─────────────────────────


def test_appeared_fires_after_persist_seconds():
    clock = FakeClock(3.0)
    detector = make_detector(clock)
    events = detector.evaluate([make_wall(first_above=0.0)])
    assert events == [
        D1Appeared(side=Side.BUY, price=Decimal("61000"), qty=Decimal("1200"), persisted_seconds=3.0)
    ]


def test_appeared_not_fired_before_persist_seconds():
    clock = FakeClock(2.9)
    detector = make_detector(clock)
    assert detector.evaluate([make_wall(first_above=0.0)]) == []


def test_candidate_dropping_below_threshold_is_suppressed():
    clock = FakeClock(1.0)
    detector = make_detector(clock)
    wall = make_wall(first_above=0.0)
    detector.evaluate([wall])  # 후보 등록

    clock.now = 2.0
    wall.last_qty = Decimal(800)  # 임계 하회 → 레지스트리가 타이머 리셋
    wall.first_seen_above_threshold = None
    events = detector.evaluate([wall])
    assert events == [
        D1Suppressed(
            side=Side.BUY, price=Decimal("61000"), peak_qty=Decimal("1200"), above_threshold_seconds=2.0
        )
    ]
    # 억제 기록은 1회만
    assert detector.evaluate([wall]) == []


def test_candidate_removed_before_persist_is_suppressed():
    clock = FakeClock(1.0)
    detector = make_detector(clock)
    wall = make_wall(first_above=0.0)
    detector.evaluate([wall])

    clock.now = 2.0
    wall.last_qty = Decimal(0)
    events = detector.on_removals([WallRemoval(wall=wall, reason=RemovalReason.TOMBSTONE)])
    assert isinstance(events[0], D1Suppressed)


def test_candidate_streak_replacement_records_old_streak_as_suppressed():
    clock = FakeClock(1.0)
    detector = make_detector(clock)
    wall = make_wall(first_above=0.0)
    detector.evaluate([wall])  # 스트릭 1 후보

    # 평가 사이에 임계 하회 후 재상승 — first_above가 교체됨
    clock.now = 4.0
    wall.first_seen_above_threshold = 3.5
    events = detector.evaluate([wall])
    assert events == [
        D1Suppressed(
            side=Side.BUY, price=Decimal("61000"), peak_qty=Decimal("1200"), above_threshold_seconds=4.0
        )
    ]
    # 새 스트릭은 자기 기준으로 지속 필터 통과 후 발화
    clock.now = 6.5
    events = detector.evaluate([wall])
    assert isinstance(events[0], D1Appeared)
    assert events[0].persisted_seconds == 3.0


# ── APPEARED: 레벨당 1회 + unconfirmed 억제 ──────────────────


def test_appeared_fires_once_per_level():
    clock = FakeClock(3.0)
    detector = make_detector(clock)
    wall = make_wall(first_above=0.0)
    assert len(detector.evaluate([wall])) == 1
    clock.now = 10.0
    assert detector.evaluate([wall]) == []


def test_unconfirmed_wall_suppresses_appeared_but_keeps_timer():
    clock = FakeClock(10.0)
    detector = make_detector(clock)
    wall = make_wall(first_above=0.0, unconfirmed=True)
    assert detector.evaluate([wall]) == []  # 발화 억제 (PRD §12.1 규칙 2)

    # 재확인되면 보존된 타이머 기준으로 즉시 발화
    wall.unconfirmed = False
    events = detector.evaluate([wall])
    assert isinstance(events[0], D1Appeared)
    assert events[0].persisted_seconds == 10.0


def test_active_level_does_not_refire_within_exit_deadband():
    clock = FakeClock(3.0)
    detector = make_detector(clock)
    wall = make_wall(first_above=0.0)
    detector.evaluate([wall])  # APPEARED

    # 임계(1000) 미만·exit(500) 이상 데드밴드로 하락 후 회복 — 재발화 없음
    clock.now = 5.0
    wall.last_qty = Decimal(700)
    wall.first_seen_above_threshold = None
    assert detector.evaluate([wall]) == []

    clock.now = 20.0
    wall.last_qty = Decimal(1100)
    wall.first_seen_above_threshold = 10.0
    assert detector.evaluate([wall]) == []


# ── REMOVED: FILLED / PULLED 판정 ────────────────────────────


def activated_detector(clock, cum_traded):
    detector = make_detector(clock, cum_traded=cum_traded)
    clock.now = 3.0
    wall = make_wall(first_above=0.0)
    assert len(detector.evaluate([wall])) == 1
    return detector, wall


def test_removed_filled_when_traded_covers_drop():
    clock = FakeClock()
    # 하락분 = 1200 − 300 = 900, 귀속 임계 = 900 × 0.7 = 630
    detector, wall = activated_detector(clock, cum_traded=Decimal(700))
    wall.last_qty = Decimal(300)  # exit(500) 미만
    events = detector.evaluate([wall])
    assert events == [
        D1Removed(
            side=Side.BUY,
            price=Decimal("61000"),
            last_qty=Decimal(300),
            peak_qty=Decimal(1200),
            cum_traded=Decimal(700),
            attribution=D1Attribution.FILLED,
            announced=False,
        )
    ]


def test_removed_pulled_when_traded_below_attribution():
    clock = FakeClock()
    detector, wall = activated_detector(clock, cum_traded=Decimal(100))
    wall.last_qty = Decimal(300)
    events = detector.evaluate([wall])
    assert events[0].attribution is D1Attribution.PULLED


def test_removed_fires_once():
    clock = FakeClock()
    detector, wall = activated_detector(clock, cum_traded=Decimal(0))
    wall.last_qty = Decimal(300)
    wall.first_seen_above_threshold = None  # 레지스트리가 임계 하회 시 타이머 리셋
    assert len(detector.evaluate([wall])) == 1
    assert detector.evaluate([wall]) == []


def test_tombstone_of_active_level_judged_immediately():
    clock = FakeClock()
    # 하락분 = 1200 − 0 = 1200, 귀속 임계 = 840
    detector, wall = activated_detector(clock, cum_traded=Decimal(900))
    wall.last_qty = Decimal(0)
    events = detector.on_removals([WallRemoval(wall=wall, reason=RemovalReason.TOMBSTONE)])
    assert events[0].attribution is D1Attribution.FILLED


def test_removed_announced_when_streak_was_alerted():
    # 발송 게이트(service)가 마킹한 스트릭의 소멸 → announced (PRD §9.2 v1.15 페어링)
    clock = FakeClock()
    detector, wall = activated_detector(clock, cum_traded=Decimal(0))
    wall.appeared_alerted_since = 0.0  # APPEARED 발송 시점의 스트릭 값
    wall.last_qty = Decimal(300)
    wall.first_seen_above_threshold = None  # 레지스트리가 임계 하회 시 타이머 리셋
    events = detector.evaluate([wall])
    assert events[0].announced is True


def test_removed_not_announced_when_alert_was_suppressed():
    # 쿨다운에 눌린 APPEARED는 마킹되지 않음 → 소멸도 쿨다운 대상 유지
    clock = FakeClock()
    detector, wall = activated_detector(clock, cum_traded=Decimal(0))
    wall.last_qty = Decimal(300)
    events = detector.evaluate([wall])
    assert events[0].announced is False


def test_tombstone_removal_carries_announced():
    clock = FakeClock()
    detector, wall = activated_detector(clock, cum_traded=Decimal(0))
    wall.appeared_alerted_since = 0.0
    wall.last_qty = Decimal(0)
    events = detector.on_removals([WallRemoval(wall=wall, reason=RemovalReason.TOMBSTONE)])
    assert events[0].announced is True


def test_removed_not_announced_when_marking_is_from_older_streak():
    # 구 스트릭에서 발송·마킹된 뒤 새 스트릭의 APPEARED가 억제된 경우 — 값 불일치
    clock = FakeClock(13.0)
    detector = make_detector(clock)
    wall = make_wall(first_above=10.0)
    wall.appeared_alerted_since = 0.0  # 이전 스트릭의 마킹
    assert len(detector.evaluate([wall])) == 1  # 새 래치 (스트릭 10.0)
    wall.last_qty = Decimal(0)
    events = detector.on_removals([WallRemoval(wall=wall, reason=RemovalReason.TOMBSTONE)])
    assert events[0].announced is False


def test_below_floor_removal_of_inactive_noncandidate_is_silent():
    clock = FakeClock()
    detector = make_detector(clock)
    wall = make_wall(qty="90", first_above=None)
    assert detector.on_removals([WallRemoval(wall=wall, reason=RemovalReason.BELOW_FLOOR)]) == []


# ── epoch 종료 리셋 (PRD §5.4) ───────────────────────────────


def test_reset_discards_active_without_removed_judgment():
    clock = FakeClock()
    detector, wall = activated_detector(clock, cum_traded=Decimal(0))
    detector.reset()

    # 보류 중 소멸해도 REMOVED 없음 (체결 귀속이 얼어 오판되므로)
    wall.last_qty = Decimal(300)
    wall.first_seen_above_threshold = None
    assert detector.evaluate([wall]) == []

    # 새 epoch에서 임계 이상 벽은 다시 APPEARED부터 (타이머는 레지스트리가 보존)
    wall.last_qty = Decimal(1200)
    wall.first_seen_above_threshold = 0.0
    clock.now = 10.0
    events = detector.evaluate([wall])
    assert isinstance(events[0], D1Appeared)
