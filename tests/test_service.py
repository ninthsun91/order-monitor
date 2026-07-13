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


# ── M3 배선: 접촉 episode + D3/D4 (PRD §8 D3/D4, §9.1 로그 전용) ──


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


def test_d4_iceberg_emitted_through_pipeline(service):
    from order_monitor.detectors.d4 import D4Iceberg

    events = capture_emitted(service)
    start_feed(service)
    # best bid 61000에서 체결→소진→회복 사이클 5회 (리필 10×5 ≥ 20, 쌍 5 ≥ 5)
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
    d4_events = [e for e in events if isinstance(e, D4Iceberg)]
    assert len(d4_events) == 1
    assert d4_events[0].refill_added == Decimal("50")
    assert service.telegram.pending() == 0  # D4는 알림 없음 (PRD §9.1)


def test_epoch_end_resets_contact_and_d3_d4(service):
    start_feed(service)
    service.on_event(DEPTH, depth_event(bids=[("61000", "100")], asks=[("61001", "1")], mono=1.0))
    assert service.contact.active()
    service.on_disconnected()  # epoch 종료
    assert service.contact.active() == {}
    assert service.d3._registered == {}
    assert service.d4._acc == {}


def test_contact_episodes_not_opened_outside_epoch(service):
    service.on_connected()
    service.on_event(DEPTH, depth_event())  # epoch 미시작 (세 스트림 확인 전)
    assert service.contact.active() == {}


# ── M4 배선: D5 상태기계 + outbox (PRD §8 D5, §9.4) ──────────


def test_d5_case1_confirmed_through_pipeline_and_recorded_to_outbox(tmp_path):
    from order_monitor.detectors.d1 import D1Appeared
    from order_monitor.detectors.d5 import D5Terminal, D5TerminalState

    svc = make_service(config_with(persist_seconds=1e-9), tmp_path)
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


def test_d5_case2_inferred_above_through_pipeline(tmp_path):
    from order_monitor.detectors.d5 import D5Terminal, D5TerminalState

    svc = make_service(config_with(persist_seconds=1e-9), tmp_path)
    events = capture_emitted(svc)
    start_feed(svc)  # 벽 60000/1200 등록
    svc.on_event(DIFF, diff_event(111, 120, bids=[("59500", "150")]))  # APPEARED

    # 상위 구간(60000 초과 ~ best_bid) 레벨에서 리필 720 BTC 인정 → 케이스2(60%) 충족
    # best_bid는 61000(start_feed의 depth 스냅샷) — 60500이 상위 구간 안에 위치
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
    inferred = [
        e
        for e in events
        if isinstance(e, D5Terminal) and e.state is D5TerminalState.EXECUTION_INFERRED_ABOVE
    ]
    assert len(inferred) == 1
    assert inferred[0].above_realized_rate == Decimal("0.6")
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


def test_d5_interrupted_on_epoch_end_before_d4_reset(tmp_path):
    from order_monitor.detectors.d5 import D5Terminal, D5TerminalState

    svc = make_service(config_with(persist_seconds=1e-9), tmp_path)
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
