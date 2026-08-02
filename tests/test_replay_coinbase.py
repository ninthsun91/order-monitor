"""Coinbase 파이프라인 결정적 replay (M8, PRD §5.5·§13 준용).

시나리오: ① D1 풀사이클(출현→접촉 체결→진행→확정→FILLED 소멸, band 게이트 겸용)
② 재연결(INTERRUPTED + unconfirmed → full snapshot 재확인) ③ trade_id 갭(epoch 종료,
unconfirmed 미마킹). 픽스처 원시 프레임은 2026-08-02 라이브 캡처 실측 스키마.
바이낸스 픽스처·골든은 무변경이 M8 완료 기준 — 이 파일은 추가만 한다.
"""

from decimal import Decimal
from pathlib import Path

from order_monitor.config import load_config
from order_monitor.detectors.d1 import D1Appeared, D1Attribution, D1Removed
from order_monitor.detectors.d3 import D3Absorption
from order_monitor.detectors.d5 import D5Progress, D5Terminal, D5TerminalState
from order_monitor.ingestion.events import Side
from order_monitor.persistence.walls import WallStore

from tests.replay.runner import load_fixture, replay

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


def run(name, tmp_path, db_name="replay.db"):
    service, emitted = replay(
        load_fixture(name), load_config(EXAMPLE_CONFIG), tmp_path / db_name, exchange="coinbase"
    )
    service._store.close()
    return service, emitted


class TestD1FullCycle:
    """벽 출현 → 접촉 체결 누적 → 진행 20/40% → 확정(래치) → FILLED 소멸 —
    바이낸스 실운영(07-31 텔레그램)과 동일한 수명주기의 Coinbase 재현."""

    FIXTURE = "coinbase_d1_cycle.jsonl"

    def test_full_lifecycle_golden(self, tmp_path):
        service, emitted = run(self.FIXTURE, tmp_path)

        assert [type(e).__name__ for e in emitted] == [
            "D1Appeared",
            "D5Progress",
            "D5Progress",
            "D5Terminal",  # EXECUTION_CONFIRMED (래치, v1.9)
            "D3Absorption",  # episode REMOVED 종료 — D1 REMOVED보다 먼저 (배선 순서)
            "D1Removed",
            "D5Terminal",  # CONFIRMED_CLOSED (로그 전용 종국)
        ]

        appeared = emitted[0]
        assert isinstance(appeared, D1Appeared)
        assert appeared.price == Decimal("60000.00") and appeared.qty == Decimal("600")

        assert [e.boundary_pct for e in emitted if isinstance(e, D5Progress)] == [
            Decimal("0.2"),
            Decimal("0.4"),
        ]

        confirmed, closed = (e for e in emitted if isinstance(e, D5Terminal))
        assert confirmed.state is D5TerminalState.EXECUTION_CONFIRMED
        assert confirmed.level_realized_rate == Decimal("0.6")  # 360/600
        assert closed.state is D5TerminalState.CONFIRMED_CLOSED
        assert closed.level_realized_rate == Decimal("440") / Decimal("600")

        absorption = next(e for e in emitted if isinstance(e, D3Absorption))
        assert absorption.absorbed_qty == Decimal("440")
        assert absorption.end_reason == "removed"

        removed = next(e for e in emitted if isinstance(e, D1Removed))
        assert removed.attribution is D1Attribution.FILLED  # 440 ≥ 0.7×600
        assert removed.cum_traded == Decimal("440")
        assert removed.announced is True

    def test_band_gate_blocks_snapshot_garbage(self, tmp_path):
        # 스냅샷의 $0.01/65,000 BTC 실측 쓰레기 주문 — 이벤트 유도 mid로 미등록,
        # D1 오알림 없음 (첫 ticker 선행 여부와 무관)
        service, emitted = run(self.FIXTURE, tmp_path)
        assert service.wall_registry.get(Side.BUY, Decimal("0.01")) is None
        assert all(
            e.price == Decimal("60000.00") for e in emitted if isinstance(e, D1Appeared)
        )

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run(self.FIXTURE, tmp_path, "a.db")
        _, second = run(self.FIXTURE, tmp_path, "b.db")
        assert first == second


class TestReconnect:
    """단절 → INTERRUPTED + 레지스트리 unconfirmed → full snapshot 재확인 → 재발화."""

    FIXTURE = "coinbase_reconnect.jsonl"

    def test_interrupt_reconfirm_and_refire(self, tmp_path):
        service, emitted = run(self.FIXTURE, tmp_path)

        appeared = [e for e in emitted if isinstance(e, D1Appeared)]
        assert len(appeared) == 2  # 단절 전 1회 + 재개 후 재발화 (인텐트 재등록 경로)

        interrupted = [
            e
            for e in emitted
            if isinstance(e, D5Terminal) and e.state is D5TerminalState.INTERRUPTED
        ]
        assert len(interrupted) == 1
        assert interrupted[0].level_realized_rate == Decimal("0.2")  # 단절 시점 120/600

        # 재접속 full snapshot이 곧바로 재확인 — Coinbase 고유 이점 (§5.5)
        wall = service.wall_registry.get(Side.BUY, Decimal("60000.00"))
        assert wall is not None and wall.unconfirmed is False
        assert len(service.d5._intents) == 1  # 재개 후 인텐트 활성 잔존

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run(self.FIXTURE, tmp_path, "a.db")
        _, second = run(self.FIXTURE, tmp_path, "b.db")
        assert first == second


class TestTradeGap:
    """match trade_id 불연속 → epoch 종료(trade_gap)·즉시 재개.
    체결 손실이지 diff 청취 공백이 아니다 — 레지스트리 unconfirmed 미마킹 (§5.5)."""

    FIXTURE = "coinbase_trade_gap.jsonl"

    def test_gap_interrupts_without_unconfirming_registry(self, tmp_path):
        service, emitted = run(self.FIXTURE, tmp_path)

        interrupted = [
            e
            for e in emitted
            if isinstance(e, D5Terminal) and e.state is D5TerminalState.INTERRUPTED
        ]
        assert len(interrupted) == 1
        assert interrupted[0].level_realized_rate == Decimal("0.2")

        appeared = [e for e in emitted if isinstance(e, D1Appeared)]
        assert len(appeared) == 2  # 새 epoch에서 재발화 → 인텐트 재등록

        wall = service.wall_registry.get(Side.BUY, Decimal("60000.00"))
        assert wall is not None and wall.unconfirmed is False  # diff 공백 아님
        assert not any(isinstance(e, D1Removed) for e in emitted)

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run(self.FIXTURE, tmp_path, "a.db")
        _, second = run(self.FIXTURE, tmp_path, "b.db")
        assert first == second


class TestLiveCapture:
    """실캡처 픽스처 재생 (PRD §13 "합성 + 실캡처" 준용) — 2026-08-02 60s 캡처.

    골든(벽 10개 — 전부 ±20% 대역 내 50+ BTC 매수벽, D1 무발화)은 캡처 시점 시장
    상태의 고정 산물: 당시 최대 오더 ~150 BTC로 500 임계 미달 (§15 #6 실측과 정합).
    스냅샷의 원거리 쓰레기 주문은 band 게이트가 걸러 레지스트리에 없다.
    재캡처 시 골든 재생성 (scripts/capture_stream.py --exchange coinbase).
    """

    FIXTURE = "live_coinbase_btcusd_60s.jsonl"

    def test_replays_without_error_and_matches_golden(self, tmp_path):
        service, emitted = run(self.FIXTURE, tmp_path)
        assert service.tracker.epoch_active
        assert len(service.wall_registry) == 10
        assert emitted == []  # 500 BTC 임계 미달 — D1/D5 무발화
        # band 게이트 — full snapshot의 원거리 잔량이 하나도 등록되지 않았다
        prices = [w.price for w in service.wall_registry.walls()]
        assert min(prices) == Decimal("51000") and max(prices) == Decimal("63133.82")

    def test_deterministic_same_input_same_output(self, tmp_path):
        service_a, first = run(self.FIXTURE, tmp_path, "a.db")
        service_b, second = run(self.FIXTURE, tmp_path, "b.db")
        assert first == second


class TestSameDbIsolation:
    """바이낸스·Coinbase 픽스처를 같은 DB 파일에 재생 — walls 행 완전 격리 (PRD §12)."""

    def test_walls_isolated_across_pipelines(self, tmp_path):
        config = load_config(EXAMPLE_CONFIG)
        db = tmp_path / "shared.db"

        binance_svc, _ = replay(load_fixture("reconnect.jsonl"), config, db)
        binance_svc._store.close()
        coinbase_svc, _ = replay(
            load_fixture("coinbase_reconnect.jsonl"), config, db, exchange="coinbase"
        )
        coinbase_svc._store.close()

        binance_store = WallStore(db)
        coinbase_store = WallStore(db, exchange="coinbase")
        binance_walls = binance_store.load()
        coinbase_walls = coinbase_store.load()
        assert {w.price for w in binance_walls}  # 바이낸스 픽스처의 벽 존재
        assert {w.price for w in coinbase_walls} == {Decimal("60000")}
        # 같은 가격이라도 행이 섞이지 않는다 — coinbase 벽 600 vs 바이낸스 자체 값
        assert all(w.last_qty == Decimal("600") for w in coinbase_walls)
        binance_store.close()
        coinbase_store.close()
