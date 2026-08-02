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
from order_monitor.service import MonitorService, seconds_until_boundary

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


def config_with(*, persist_seconds=None, send_d1=None, cooldown_seconds=None):
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
    if cooldown_seconds is not None:
        config = dataclasses.replace(
            config, alerts=dataclasses.replace(config.alerts, cooldown_seconds=cooldown_seconds)
        )
    return config


def test_d1_appeared_flows_to_alert_queue_when_enabled(tmp_path):
    svc = make_service(config_with(persist_seconds=1e-9, send_d1=True), tmp_path)
    start_feed(svc)
    # 다음 diff 이벤트가 평가를 트리거 — 60000 벽(1200 ≥ 1000)이 지속 필터 통과
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert svc.telegram.pending() == 1
    svc._store.close()


def test_d1_alert_gated_off(tmp_path):
    # 게이트 검증 — example 기본값이 on(PRD v1.10)이라 명시적으로 off
    svc = make_service(config_with(persist_seconds=1e-9, send_d1=False), tmp_path)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert svc.telegram.pending() == 0  # send_d1 off → 미발송 (PRD §9.1)
    svc._store.close()


# ── v1.8: APPEARED 알림은 임계 스트릭당 1회 (PRD §8 D1) ──


def d1_alert_config():
    # cooldown 0 — 스트릭 게이트만이 억제 요인임을 보장 (쿨다운과의 교란 제거)
    return config_with(persist_seconds=1e-9, send_d1=True, cooldown_seconds=0)


def resume_feed(svc, first_id, final_id, qty):
    """재시작/재연결 후 epoch 재개 + 60000 벽 재확인 diff 1건."""
    svc.on_connected()
    svc.on_event(DEPTH, depth_event())
    svc.on_event(AGG, agg_event())
    svc.on_event(DIFF, diff_event(first_id, final_id, bids=[("60000", qty)]))


def test_d1_alert_suppressed_on_restart_refire(tmp_path):
    svc1 = make_service(d1_alert_config(), tmp_path)
    start_feed(svc1)
    svc1.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert svc1.telegram.pending() == 1  # 최초 스트릭 — 발송 + 마킹
    svc1._store.close()

    svc2 = make_service(d1_alert_config(), tmp_path)
    resume_feed(svc2, 500, 510, "1250")  # 복원 벽 재확인 → D1 재발화
    assert (Side.BUY, Decimal("60000")) in svc2.d5._intents  # D5 재등록은 유지
    assert svc2.telegram.pending() == 0  # 같은 스트릭 — 발송 억제
    svc2._store.close()


def test_d1_alert_suppressed_on_reconnect_refire(tmp_path):
    svc = make_service(d1_alert_config(), tmp_path)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert svc.telegram.pending() == 1
    svc.on_disconnected()  # epoch 종료 → D1 latch 리셋 (재발화 전제)
    resume_feed(svc, 200, 210, "1250")
    assert (Side.BUY, Decimal("60000")) in svc.d5._intents
    assert svc.telegram.pending() == 1  # 같은 스트릭 재발화 — 발송 억제
    svc._store.close()


def test_d1_alert_resent_on_new_streak_after_removed(tmp_path):
    svc = make_service(d1_alert_config(), tmp_path)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert svc.telegram.pending() == 1
    # exit(500) 미만 하락 → REMOVED 발송 (하한 100 이상이라 레지스트리 잔존, 스트릭 리셋)
    svc.on_event(DIFF, diff_event(121, 130, bids=[("60000", "300")]))
    assert svc.telegram.pending() == 2
    # 재돌파 = 새 스트릭 → 새 등장으로 발송 (다음 diff가 평가 트리거)
    svc.on_event(DIFF, diff_event(131, 140, bids=[("60000", "1300")]))
    svc.on_event(DIFF, diff_event(141, 150, bids=[("59500", "150")]))
    assert svc.telegram.pending() == 3
    svc._store.close()


def test_d1_alert_sent_after_restart_when_streak_never_announced(tmp_path):
    # 지속 필터 통과 전에 재시작 — 스트릭은 미발송 상태로 영속되므로 억제하면 안 됨
    svc1 = make_service(
        config_with(persist_seconds=9999, send_d1=True, cooldown_seconds=0), tmp_path
    )
    start_feed(svc1)
    assert svc1.telegram.pending() == 0
    svc1._store.close()

    svc2 = make_service(d1_alert_config(), tmp_path)
    resume_feed(svc2, 500, 510, "1250")
    assert svc2.telegram.pending() == 1  # 미발송 스트릭 — 억제 없이 발송
    svc2._store.close()


def test_d1_removed_sent_within_cooldown_when_appeared_was_announced(tmp_path):
    # 07-31 63k 사례 재현: 출현 알림 발송 직후(쿨다운 300s 내) tombstone 소멸 —
    # 출현이 발송된 벽의 소멸은 쿨다운 우회로 반드시 발송 (PRD §9.2 v1.15 페어링 보장)
    svc = make_service(config_with(persist_seconds=1e-9, send_d1=True), tmp_path)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert svc.telegram.pending() == 1  # APPEARED 발송 — 같은 버킷 쿨다운 arm
    svc.on_event(DIFF, diff_event(121, 130, bids=[("60000", "0")]))
    assert svc.telegram.pending() == 2  # REMOVED 우회 발송
    svc._store.close()


def test_d2_onset_flows_to_alert_queue(service):
    # 기준선 워밍업 완료 상태 흉내 (분당 4 BTC × 24h) → thr = max(30, 40) ≈ 40
    service.volume_baseline.bootstrap((m * 60_000, Decimal(4)) for m in range(1440))
    start_feed(service)
    service.on_event(AGG, agg_event(qty="20"))
    assert service.telegram.pending() == 0
    service.on_event(AGG, agg_event(qty="20"))  # 창 총합 40.5 ≥ 임계 → 온셋
    assert service.telegram.pending() == 1
    assert service.d2.episode_active


def test_d2_held_during_baseline_warmup(service):
    # 부트스트랩 실패 시나리오 — 창이 차기 전에는 대량 체결도 판정 보류 (PRD §8 D2)
    start_feed(service)
    service.on_event(AGG, agg_event(qty="500"))
    assert service.telegram.pending() == 0
    assert not service.d2.episode_active


def test_wall_report_built_from_registry_and_book(service):
    start_feed(service)
    text = service.build_wall_report()
    # start_feed의 diff 벽(60000/1200 BTC bid), 현재가는 best bid/ask 중간값
    assert "🧱 60,000 — 1,200 BTC" in text
    assert "현재가 61,000" in text  # (61000+61001)/2 반올림


def test_wall_report_skipped_outside_epoch(service):
    service.on_connected()
    assert service.build_wall_report() is None


def test_wall_report_boundary_alignment():
    # 정시 발송 (사용자 요청 2026-07-13) — 기동 시각이 아니라 벽시계 경계 기준
    assert seconds_until_boundary(3000.0, 3600.0) == 600.0
    # 경계 정각에서는 다음 경계까지 전체 간격 — 발송 직후 이중 발송 방지
    assert seconds_until_boundary(7200.0, 3600.0) == 3600.0
    assert seconds_until_boundary(7200.5, 3600.0) == 3599.5


def test_stream_stale_notice_flows_to_alert_queue(service):
    from order_monitor.ingestion.health import StreamStale

    start_feed(service)
    service._handle_notices([StreamStale(stream=DEPTH, silent_seconds=31.0)])
    assert service.telegram.pending() == 1


def test_detector_judgment_suspended_outside_epoch(service):
    # epoch 시작 전(세 스트림 수신 확인 전) — 상태는 적재되지만 판정은 보류
    service.volume_baseline.bootstrap((m * 60_000, Decimal(4)) for m in range(1440))
    service.on_connected()
    service.on_event(AGG, agg_event(qty="200"))
    assert len(service.trade_window) == 1
    assert service.telegram.pending() == 0


# ── M3 배선: 접촉 episode + D3 (PRD §8 D3, §9.1 로그 전용. D4는 PRD v1.6 비활성) ──


def capture_emitted(svc):
    events = []
    original = svc._emit
    svc._emit = lambda event: (events.append(event), original(event))
    return events


def agg_at(price, qty, mono=0.0, trade_id=1):
    return AggTradeEvent(
        agg_trade_id=trade_id,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=Side.SELL,
        exchange_time_ms=int(mono * 1000),
        local_monotonic_receive_time=mono,
    )


def test_d3_absorption_emitted_through_pipeline(tmp_path):
    from order_monitor.detectors.d1 import D1Appeared
    from order_monitor.detectors.d3 import D3Absorption

    svc = make_service(config_with(persist_seconds=1e-9), tmp_path)
    events = capture_emitted(svc)
    start_feed(svc)  # 벽 60000/1200 등록
    # 다음 diff가 D1 평가 트리거 → APPEARED (지속 필터 즉시 통과) → D3 등록 라우팅
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    assert any(isinstance(e, D1Appeared) for e in events)
    # 접촉: best_bid가 60000까지 하락 → episode 시작, 체결 400 흡수 (33% ≥ 30%)
    svc.on_event(DEPTH, depth_event(bids=[("60000", "1200")], asks=[("60001", "1")], mono=1.0))
    svc.on_event(AGG, agg_at("60000", "400", mono=1.1))
    # 반등 이탈 → episode 비관통 종료 → D3 확정 판정
    svc.on_event(DEPTH, depth_event(bids=[("60500", "5")], asks=[("60501", "1")], mono=2.0))
    d3_events = [e for e in events if isinstance(e, D3Absorption)]
    assert len(d3_events) == 1
    assert d3_events[0].absorbed_qty == Decimal("400")
    assert d3_events[0].registered_qty == Decimal("1200")
    # D3 자신은 무발송이지만, 같은 400/1200(33%) 체결이 D5 진행률 20% 경계를
    # 넘겨(M4 배선) 그건 발송된다 — D3의 무발송 자체는 별도로 확인
    assert not any(svc.dispatcher.dispatch(e) for e in [d3_events[0]])
    svc._store.close()


def test_d3_pierced_episode_silent_through_pipeline(tmp_path):
    from order_monitor.detectors.d3 import D3Absorption

    svc = make_service(config_with(persist_seconds=1e-9), tmp_path)
    events = capture_emitted(svc)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    svc.on_event(DEPTH, depth_event(bids=[("60000", "1200")], asks=[("60001", "1")], mono=1.0))
    svc.on_event(AGG, agg_at("60000", "400", mono=1.1))
    svc.on_event(AGG, agg_at("59999", "1", mono=1.2, trade_id=2))  # 체결가 관통 (주 신호)
    svc.on_event(DEPTH, depth_event(bids=[("60500", "5")], asks=[("60501", "1")], mono=2.0))
    assert [e for e in events if isinstance(e, D3Absorption)] == []
    svc._store.close()


def test_d4_silent_for_untracked_level(service):
    # PRD §8 D4 v1.11: 대상은 레지스트리 추적 레벨 전체 — 61000은 스냅샷에만
    # 등장(미등록)이라 리필 사이클 입력에도 스트릭이 없어 침묵한다
    from order_monitor.detectors.d4 import D4Defense

    events = capture_emitted(service)
    start_feed(service)
    t = 1.0
    for i in range(5):
        service.on_event(AGG, agg_at("61000", "10", mono=t, trade_id=i + 10))
        service.on_event(
            DEPTH, depth_event(bids=[("61000", "90")], asks=[("61001", "1")], mono=t + 0.05)
        )
        service.on_event(
            DEPTH, depth_event(bids=[("61000", "100")], asks=[("61001", "1")], mono=t + 0.1)
        )
        t += 0.2
    assert [e for e in events if isinstance(e, D4Defense)] == []
    assert (Side.BUY, Decimal("61000")) not in service.d4._streaks


def test_epoch_end_resets_contact_and_d3(service):
    start_feed(service)
    service.on_event(DEPTH, depth_event(bids=[("61000", "100")], asks=[("61001", "1")], mono=1.0))
    assert service.contact.active()
    service.on_disconnected()  # epoch 종료
    assert service.contact.active() == {}
    assert service.d3._registered == {}


def test_contact_episodes_not_opened_outside_epoch(service):
    service.on_connected()
    service.on_event(DEPTH, depth_event())  # epoch 미시작 (세 스트림 확인 전)
    assert service.contact.active() == {}


# ── M4 배선: D5 상태기계 + outbox (PRD §8 D5, §9.4) ──────────


def test_d5_case1_confirmed_through_pipeline_and_recorded_to_outbox(tmp_path):
    from order_monitor.detectors.d1 import D1Appeared
    from order_monitor.detectors.d5 import D5Terminal, D5TerminalState

    # send_d1 off — 발송 큐 카운트를 D5 알림만으로 고정 (D1 게이트는 별도 테스트)
    svc = make_service(config_with(persist_seconds=1e-9, send_d1=False), tmp_path)
    events = capture_emitted(svc)
    start_feed(svc)  # 벽 60000/1200 등록
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # D1 평가 트리거 → APPEARED
    assert any(isinstance(e, D1Appeared) for e in events)
    # LevelTracker가 60000을 추적하도록 top-20 스냅샷으로 먼저 진입시킨다
    svc.on_event(DEPTH, depth_event(bids=[("60000", "1200")], asks=[("60001", "1")], mono=1.0))

    # 60000에서 720 BTC 체결 (720/1200 = 60% = REALIZE_PCT) → 케이스1 즉시 확정
    svc.on_event(AGG, agg_at("60000", "720", mono=1.1))
    confirmed = [
        e for e in events if isinstance(e, D5Terminal) and e.state is D5TerminalState.EXECUTION_CONFIRMED
    ]
    assert len(confirmed) == 1
    assert confirmed[0].level_realized_rate == Decimal("0.6")
    assert svc.telegram.pending() == 1
    unsent = svc._outbox.load_unsent()
    assert len(unsent) == 1
    assert "실체결 확인 (케이스 1)" in unsent[0][1]
    svc._store.close()
    svc._outbox.close()


def test_d5_unaffected_by_upper_range_refill(tmp_path):
    # PRD v1.11: 케이스2 폐지 — 상위 구간(60000 초과) 리필은 D5에 아무 영향이 없고
    # (귀속 없음), 인텐트는 살아남아 케이스1만 감시한다. 60500은 레지스트리
    # 미추적(스냅샷에만 등장)이라 D4도 침묵 — 상위 구간 활동의 통지는 그 레벨이
    # 벽으로 등록됐을 때 D4 몫 (아래 D4 파이프라인 테스트)
    from order_monitor.detectors.d4 import D4Defense
    from order_monitor.detectors.d5 import D5Progress, D5Terminal

    svc = make_service(config_with(persist_seconds=1e-9), tmp_path)
    events = capture_emitted(svc)
    start_feed(svc)  # 벽 60000/1200 등록
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # APPEARED

    t = 1.0
    for i in range(5):
        svc.on_event(AGG, agg_at("60500", "144", mono=t, trade_id=100 + i))
        svc.on_event(
            DEPTH,
            depth_event(bids=[("60500", "1000")], asks=[("61001", "1")], mono=t + 0.01),
        )
        svc.on_event(
            DEPTH,
            depth_event(bids=[("60500", "1144")], asks=[("61001", "1")], mono=t + 0.02),
        )
        t += 0.2
    assert [e for e in events if isinstance(e, (D5Terminal, D5Progress, D4Defense))] == []
    assert len(svc.d5._intents) == 1  # 인텐트 잔존 — 케이스1 감시 유지
    svc._store.close()
    svc._outbox.close()


def test_d4_defense_detected_through_pipeline(tmp_path):
    # 준임계 벽(59500/150 — D1 임계 미만)이 접촉 중 가시 리필로 방어 → 스트릭 생애
    # 누적이 2.0×R=300 + 이벤트 5건을 넘는 순간 DEFENSE_DETECTED 발송 (PRD §8 D4 v1.12)
    from order_monitor.detectors.d4 import D4Defense, D4DefenseKind

    svc = make_service(config_with(persist_seconds=1e-9, send_d1=False), tmp_path)
    events = capture_emitted(svc)
    start_feed(svc)  # 60000/1200 등록 (D1 벽)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # 준임계 벽 등록, R=150

    # 59500이 best_bid로 접촉 → 사이클당 체결 60 + 회복 60 (가시 리필 인정)
    t = 1.0
    for i in range(6):  # 6사이클 × 60 = 360 ≥ 300, 이벤트 6 ≥ 5
        svc.on_event(AGG, agg_at("59500", "60", mono=t, trade_id=200 + i))
        svc.on_event(
            DEPTH, depth_event(bids=[("59500", "90")], asks=[("59501", "1")], mono=t + 0.01)
        )
        svc.on_event(
            DEPTH, depth_event(bids=[("59500", "150")], asks=[("59501", "1")], mono=t + 0.02)
        )
        t += 0.2
    detected = [
        e for e in events if isinstance(e, D4Defense) and e.kind is D4DefenseKind.DETECTED
    ]
    assert len(detected) == 1
    assert detected[0].base_qty == Decimal("150")
    assert detected[0].absorbed_total >= Decimal("300")
    assert svc.telegram.pending() == 1  # send_d4 기본 on — 발송 큐 투입
    svc._store.close()
    svc._outbox.close()


def test_d5_intent_survives_long_after_registration(tmp_path):
    # PRD v1.5 (TTL 폐지): 등록 한참 뒤(구 TTL의 수십 배) 도달해도 케이스1 판정 성립
    import dataclasses

    from order_monitor.detectors.d5 import D5Terminal, D5TerminalState

    config = config_with(persist_seconds=1e-9)
    # monotonic 점프가 staleness로 epoch를 끊지 않게 임계를 크게 — 인텐트 수명만 격리
    config = dataclasses.replace(
        config,
        watchdog=dataclasses.replace(config.watchdog, stale_seconds=1e9, trade_stale_seconds=1e9),
    )
    now = {"t": 0.0}
    svc = MonitorService(
        config,
        db_path=tmp_path / "monitor.db",
        telegram_token="test-token",
        monotonic=lambda: now["t"],
    )
    svc.startup()
    events = capture_emitted(svc)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # APPEARED — 인텐트 등록

    now["t"] = 83114.0  # 실측 61k 벽 지속시간 — 등록 23h 뒤 가격 도달 시나리오
    svc.on_event(DEPTH, depth_event(bids=[("60000", "1200")], asks=[("60001", "1")], mono=83114.0))
    svc.on_event(AGG, agg_at("60000", "720", mono=83114.1))
    confirmed = [
        e for e in events if isinstance(e, D5Terminal) and e.state is D5TerminalState.EXECUTION_CONFIRMED
    ]
    assert len(confirmed) == 1
    assert confirmed[0].registered_seconds >= 83114.0
    svc._store.close()


def test_d5_interrupted_on_epoch_end(tmp_path):
    from order_monitor.detectors.d5 import D5Terminal, D5TerminalState

    # send_d1 off — pending 0 단언이 D5 로그 전용 여부만 보게 한다
    svc = make_service(config_with(persist_seconds=1e-9, send_d1=False), tmp_path)
    events = capture_emitted(svc)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # APPEARED — 인텐트 등록
    svc.on_event(AGG, agg_at("60000", "100", mono=1.0))  # 미달 체결 — 활성 유지

    svc.on_disconnected()  # epoch 종료
    interrupted = [
        e for e in events if isinstance(e, D5Terminal) and e.state is D5TerminalState.INTERRUPTED
    ]
    assert len(interrupted) == 1
    assert svc.telegram.pending() == 0  # 로그 전용
    assert svc.d5._intents == {}
    svc._store.close()


def test_outbox_unsent_alert_resent_on_restart(tmp_path):
    config = config_with(persist_seconds=1e-9)

    svc1 = make_service(config, tmp_path)
    start_feed(svc1)
    svc1.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    svc1.on_event(DEPTH, depth_event(bids=[("60000", "1200")], asks=[("60001", "1")], mono=1.0))
    svc1.on_event(AGG, agg_at("60000", "720", mono=1.1))  # 케이스1 확정 — outbox 선기록
    assert svc1._outbox.count() == 1
    assert len(svc1._outbox.load_unsent()) == 1  # 발송 확인 전(Telegram 미구동)
    svc1._store.close()
    svc1._outbox.close()

    svc2 = make_service(config, tmp_path)  # 재시작
    assert svc2.telegram.pending() == 1  # 미발송 행이 재큐잉됨
    svc2._store.close()
    svc2._outbox.close()


# ── M6 배선: 디텍터 이벤트·인텐트 DB 기록 (PRD §12) ──────────


def test_detector_events_recorded_to_db(tmp_path):
    svc = make_service(config_with(persist_seconds=1e-9), tmp_path)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # D1 평가 → APPEARED
    rows = svc._event_store.rows("D1Appeared")
    assert len(rows) == 1
    assert rows[0]["side"] == "buy"
    assert rows[0]["price"] == "60000"
    assert rows[0]["payload"]["qty"] == "1200"  # payload = 로그와 동일 전 필드
    svc._store.close()


def test_intent_lifecycle_recorded_to_db(tmp_path):
    svc = make_service(config_with(persist_seconds=1e-9, send_d1=False), tmp_path)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # APPEARED — 인텐트 등록
    rows = svc._intent_store.rows()
    assert len(rows) == 1
    assert rows[0]["state"] == "active"
    assert rows[0]["price"] == "60000"
    assert rows[0]["registered_qty"] == "1200"

    # 케이스1 확정 — 래치 (v1.9): 행은 confirmed로 갱신되고 열린 채 유지
    svc.on_event(DEPTH, depth_event(bids=[("60000", "1200")], asks=[("60001", "1")], mono=1.0))
    svc.on_event(AGG, agg_at("60000", "720", mono=1.1))
    row = svc._intent_store.rows()[0]
    assert row["state"] == "confirmed"
    assert row["confirmed_at"] is not None
    assert row["level_realized_rate"] == "0.6"

    # 벽 소멸 → 래치 마감 종국 (on_d1_removed 경유는 종국)
    svc.on_event(DIFF, diff_event(121, 130, bids=[("60000", "0")]))
    assert svc._intent_store.rows()[0]["state"] == "confirmed_closed"
    svc._store.close()


def test_intent_interrupted_on_epoch_end_recorded(tmp_path):
    svc = make_service(config_with(persist_seconds=1e-9, send_d1=False), tmp_path)
    start_feed(svc)
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))
    svc.on_disconnected()  # epoch 종료 — D5 reset → INTERRUPTED 종국
    assert svc._intent_store.rows()[0]["state"] == "interrupted"
    assert len(svc._event_store.rows("D5Terminal")) == 1  # 이벤트 기록도 동반
    svc._store.close()


def test_crash_open_intent_marked_interrupted_on_restart(tmp_path):
    config = config_with(persist_seconds=1e-9, send_d1=False)

    svc1 = make_service(config, tmp_path)
    start_feed(svc1)
    svc1.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # 인텐트 active
    assert svc1._intent_store.rows()[0]["state"] == "active"
    svc1._store.close()  # 크래시 가정 — epoch 종료 없이 소멸 (종국 기록 없음)

    svc2 = make_service(config, tmp_path)  # 재시작 — 기동 시 열린 행 마킹 (PRD §12)
    assert svc2._intent_store.rows()[0]["state"] == "interrupted"
    svc2._store.close()


# ── W 주시 관측 파이프라인 (PRD §8 W v1.13) ──────────────────


class _Clock:
    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


class _CaptureSender:
    def __init__(self):
        self.sent = []
        self.on_sent_callbacks = []

    def enqueue(self, text, on_sent=None):
        self.sent.append(text)
        self.on_sent_callbacks.append(on_sent)


TF_15M_MS = 15 * 60_000


def watch_agg(price, qty="1", t_ms=0, side=Side.SELL):
    return AggTradeEvent(
        agg_trade_id=t_ms,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=side,
        exchange_time_ms=t_ms,
        local_monotonic_receive_time=t_ms / 1000.0,
    )


def make_watch_service(tmp_path, mono, wall):
    svc = MonitorService(
        load_config(EXAMPLE_CONFIG),
        db_path=tmp_path / "monitor.db",
        telegram_token="test-token",
        clock=wall,
        monotonic=mono,
    )
    svc.startup()
    sender = _CaptureSender()
    svc.dispatcher._sender = sender  # 발송 텍스트 검증용 치환 (기존 큐 카운트 대신)
    return svc, sender


def _tick(svc):
    """_staleness_loop 본문의 W 부분 — replay 방식의 동기 구동."""
    for event in svc.watch.on_tick():
        svc._emit(event)
    svc._flush_watches()


def test_watch_full_cycle_register_contact_report_invalidate(tmp_path):
    mono, wall = _Clock(0.0), _Clock(1_000_000.0)
    svc, sender = make_watch_service(tmp_path, mono, wall)
    start_feed(svc)

    # 등록 (텔레그램 명령 핸들러 경로) — 즉시 영속
    response = svc._handle_watch_command(Decimal(65600), Decimal(65600), "65600")
    assert "주시 등록" in response
    assert svc._watch_store.count() == 1

    # 위에서 접근 → 첫 접촉 (지지 테스트)
    svc.on_event(AGG, watch_agg("65700", t_ms=1000))
    svc.on_event(AGG, watch_agg("65650", t_ms=2000))
    assert any("지지 테스트 시작" in text for text in sender.sent)

    # 계측 + 주기 리포트 (활동 게이팅 통과)
    svc.on_event(AGG, watch_agg("65600", qty="5", t_ms=3000))
    mono.now = 601.0
    _tick(svc)
    report = [t for t in sender.sent if "지지 테스트 중" in t]
    assert len(report) == 1
    assert "매도 5" in report[0]
    # flush — 누적이 DB에 반영
    assert svc._watch_store.load()[0].cum_sell == Decimal(5)

    # 무효화: 초기 봉(재시작 오염)을 소진한 뒤 깨끗한 이탈 마감 2연속
    svc.on_event(AGG, watch_agg("65300", qty="2", t_ms=TF_15M_MS + 1000))  # 버킷0 마감(오염)
    svc.on_event(AGG, watch_agg("65300", qty="2", t_ms=TF_15M_MS * 2 + 1000))  # 버킷1 마감 — 이탈 1/2
    assert svc.watch.watches()[0].breach_closes == 1
    svc.on_event(AGG, watch_agg("65300", qty="2", t_ms=TF_15M_MS * 3 + 1000))  # 버킷2 마감 — 확정
    final = [t for t in sender.sent if "지지 이탈 확정" in t]
    assert len(final) == 1
    assert svc.watch.watches() == []  # 관측 종료
    # 발송 보장 — 선기록 유지, 발송 확인 시 행 삭제 (§9.4)
    assert svc._watch_store.load()[0].final_text == final[0]
    sender.on_sent_callbacks[sender.sent.index(final[0])]()
    assert svc._watch_store.count() == 0

    svc._store.close()


def test_watch_counting_continues_during_epoch_gap(tmp_path):
    mono, wall = _Clock(0.0), _Clock(1_000_000.0)
    svc, sender = make_watch_service(tmp_path, mono, wall)
    start_feed(svc)
    svc._handle_watch_command(Decimal(65600), Decimal(65600), "65600")
    svc.on_event(AGG, watch_agg("65700", t_ms=1000))
    svc.on_event(AGG, watch_agg("65650", t_ms=2000))

    svc.on_disconnected()  # epoch 종료 — 디텍터는 리셋, W는 지속 (§5.4 v1.13)
    assert not svc.tracker.epoch_active
    svc.on_event(AGG, watch_agg("65500", qty="7", t_ms=3000))  # aggTrade만 재개된 상황
    assert svc.watch.watches()[0].cum_sell == Decimal(7)  # epoch 비활성에도 계측 (65650은 > hi라 애초 미계측)

    # 공백 걸친 봉은 이탈 마감이어도 카운트 리셋 (판정 제외)
    svc.on_event(AGG, watch_agg("65300", t_ms=TF_15M_MS + 1000))  # 오염 봉 마감
    assert svc.watch.watches()[0].breach_closes == 0

    # 공백 플래그가 다음 리포트에 표기
    mono.now = 601.0
    _tick(svc)
    report = [t for t in sender.sent if "지지 테스트 중" in t]
    assert "관측 공백 포함" in report[0]

    svc._store.close()


def test_watch_restore_via_service_startup(tmp_path):
    mono, wall = _Clock(0.0), _Clock(1_000_000.0)
    svc1, _ = make_watch_service(tmp_path, mono, wall)
    start_feed(svc1)
    svc1._handle_watch_command(Decimal(65600), Decimal(65600), "65600")
    svc1.on_event(AGG, watch_agg("65700", t_ms=1000))
    svc1.on_event(AGG, watch_agg("65600", qty="9", t_ms=2000))
    _tick(svc1)  # flush (첫 접촉으로 flush_pending)
    svc1._store.close()
    svc1._watch_store.close()
    svc1._kv.close()

    svc2, _ = make_watch_service(tmp_path, _Clock(0.0), _Clock(2_000_000.0))
    data = svc2.watch.watches()[0]
    assert data.cum_sell == Decimal(9)  # 누적 보존 (§12.2)
    assert data.gap_flag is True  # 관측 공백 표기
    assert data.in_band is False  # 회차 단절
    svc2._store.close()


def test_watch_unsent_final_resent_on_startup(tmp_path):
    from order_monitor.persistence.watch_levels import WatchStore

    mono, wall = _Clock(0.0), _Clock(1_000_000.0)
    svc1, _ = make_watch_service(tmp_path, mono, wall)
    start_feed(svc1)
    svc1._handle_watch_command(Decimal(65600), Decimal(65600), "65600")
    _tick(svc1)
    # 무효화 확정 후 발송 확인 전 크래시 시나리오 — final_text만 남긴다
    svc1._watch_store.mark_final(Decimal(65600), Decimal(65600), "support_broken", "미발송 최종 리포트")
    svc1._store.close()
    svc1._watch_store.close()
    svc1._kv.close()

    svc2, _ = make_watch_service(tmp_path, _Clock(0.0), _Clock(2_000_000.0))
    # startup은 dispatcher._sender 치환 전에 재전송을 큐잉 — 실제 TelegramSender 큐로 확인
    assert svc2.telegram.pending() == 1
    assert svc2.watch.watches() == []  # 무효화된 주시는 복원 대상 아님
    svc2._store.close()


def test_watch_unwatch_command_final_report(tmp_path):
    mono, wall = _Clock(0.0), _Clock(1_000_000.0)
    svc, sender = make_watch_service(tmp_path, mono, wall)
    start_feed(svc)
    svc._handle_watch_command(Decimal(65600), Decimal(65600), "65600")
    response = svc._handle_unwatch_command(Decimal(65600), Decimal(65600), "65600")
    assert "주시 해소" in response
    assert any("주시 해소 (수동)" in text for text in sender.sent)
    assert svc._watch_store.count() == 0
    # 미등록 해소는 오류 안내
    assert "주시 중이 아닙니다" in svc._handle_unwatch_command(Decimal(1), Decimal(2), "1-2")
    svc._store.close()


# ---- M8 멀티 거래소 파이프라인 (PRD §5.5 v1.16) ----

from order_monitor.ingestion.coinbase import coinbase_stream_names  # noqa: E402

CB_TICKER, CB_MATCHES, CB_L2 = coinbase_stream_names("BTC-USD")


def make_coinbase_service(config, tmp_path):
    svc = MonitorService(
        config,
        db_path=tmp_path / "monitor.db",
        telegram_token="test-token",
        exchange="coinbase",
    )
    svc.startup()
    return svc


def cb_ticker_event(bid="62900", ask="62901", mono=0.0):
    return DepthSnapshot(
        last_update_id=1,
        bids=((Decimal(bid), Decimal("0.5")),),
        asks=((Decimal(ask), Decimal("0.5")),),
        local_monotonic_receive_time=mono,
    )


def start_coinbase_feed(svc):
    svc.on_connected()
    svc.on_event(CB_TICKER, cb_ticker_event())
    svc.on_event(CB_MATCHES, agg_event(price="62900"))
    svc.on_event(CB_L2, diff_event(0, 0, bids=[("60000", "600")]))


def test_coinbase_service_gates_binance_only_components(tmp_path):
    svc = make_coinbase_service(load_config(EXAMPLE_CONFIG), tmp_path)
    assert svc.d2 is None and svc.d4 is None
    assert svc.watch is None and svc.candles is None and svc.volume_baseline is None
    assert svc.d1 is not None and svc.d3 is not None and svc.d5 is not None
    svc._store.close()


def test_coinbase_pipeline_epoch_and_wall_registration(tmp_path):
    svc = make_coinbase_service(load_config(EXAMPLE_CONFIG), tmp_path)
    start_coinbase_feed(svc)
    assert svc.tracker.epoch_active
    # exchanges.coinbase 임계 적용 — 관측 플로어 50: 600 BTC 벽 등록
    wall = svc.wall_registry.get(Side.BUY, Decimal("60000"))
    assert wall is not None and wall.last_qty == Decimal("600")
    svc._store.close()


def test_coinbase_d1_alert_carries_venue_label(tmp_path):
    svc = make_coinbase_service(config_with(persist_seconds=1e-9), tmp_path)
    start_coinbase_feed(svc)
    # 600 ≥ size_threshold(500) — persist 경과 후 다음 diff에서 APPEARED 발화
    svc.on_event(CB_L2, diff_event(0, 0, bids=[("60000.5", "70")], mono=0.1))
    assert svc.telegram.pending() == 1
    text = svc.telegram._queue.get_nowait()[0]
    assert "심볼: BTC-USD (Coinbase)" in text
    assert "Binance" not in text
    svc._store.close()


def test_coinbase_band_gate_blocks_far_garbage_in_pipeline(tmp_path):
    svc = make_coinbase_service(load_config(EXAMPLE_CONFIG), tmp_path)
    start_coinbase_feed(svc)
    # mid ~62900.5, band ±20% — $0.01의 65,000 BTC 쓰레기 주문 (실측 사례) 미등록
    svc.on_event(CB_L2, diff_event(0, 0, bids=[("0.01", "65000")], mono=0.2))
    assert svc.wall_registry.get(Side.BUY, Decimal("0.01")) is None
    svc._store.close()


def test_binance_and_coinbase_walls_isolated_in_same_db(tmp_path):
    config = load_config(EXAMPLE_CONFIG)
    binance = make_service(config, tmp_path)
    coinbase = make_coinbase_service(config, tmp_path)
    start_feed(binance)  # 60000에 1200 BTC (binance)
    start_coinbase_feed(coinbase)  # 60000에 600 BTC (coinbase)

    assert WallStore(tmp_path / "monitor.db").load()[0].last_qty == Decimal("1200")
    assert WallStore(tmp_path / "monitor.db", exchange="coinbase").load()[0].last_qty == Decimal(
        "600"
    )
    binance._store.close()
    coinbase._store.close()
