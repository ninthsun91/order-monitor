"""결정적 replay 테스트 — M3 완료 게이트 (PRD §13).

필수 3 시나리오: 재연결(epoch 종료/재개), 스트림 순서 역전, diff U/u 갭.
실물 MonitorService를 클록 주입으로 구동하므로 판정 로직만이 아니라
service 배선(epoch 게이팅, D1→D3 라우팅, 소멸 순서)까지 검증 범위다.
"""

from decimal import Decimal
from pathlib import Path

from order_monitor.config import load_config
from order_monitor.detectors.d1 import D1Appeared
from order_monitor.detectors.d3 import D3Absorption
from order_monitor.detectors.d4 import D4Iceberg
from order_monitor.ingestion.events import Side

from tests.replay.runner import load_fixture, replay

EXAMPLE_CONFIG = Path(__file__).resolve().parent.parent / "config.example.yaml"


def run(name, tmp_path, db_name="replay.db"):
    service, emitted = replay(load_fixture(name), load_config(EXAMPLE_CONFIG), tmp_path / db_name)
    service._store.close()
    return service, emitted


class TestReconnect:
    """단절 → EpochEnded(무판정 폐기) → 재개 → 이전 epoch와 무오염 판정."""

    def test_episode_discarded_on_disconnect_and_rejudged_after_resume(self, tmp_path):
        service, emitted = run("reconnect.jsonl", tmp_path)

        # D1 APPEARED는 epoch마다 1회 — 단절 전 1회 + 재개 후 재발화 1회
        # (지속 타이머는 레지스트리 필드가 보존 — PRD §12.1 규칙 2)
        appeared = [e for e in emitted if isinstance(e, D1Appeared)]
        assert len(appeared) == 2
        assert {e.price for e in appeared} == {Decimal("60000")}

        # 단절 시점의 진행 중 episode(흡수 300, 20%)는 무판정 폐기.
        # D3는 재개 후 episode에서만 확정 — 생애 누적 500(33%) ≥ 30%
        absorptions = [e for e in emitted if isinstance(e, D3Absorption)]
        assert len(absorptions) == 1
        assert absorptions[0].absorbed_qty == Decimal("500")
        assert absorptions[0].registered_qty == Decimal("1500")
        assert absorptions[0].end_reason == "rebound"

    def test_disconnect_marks_walls_unconfirmed_until_reconfirmed(self, tmp_path):
        service, _ = run("reconnect.jsonl", tmp_path)
        # 60000은 재개 후 diff로 재확인, 59000은 공백 이후 무이벤트 → unconfirmed 잔존
        assert not service.wall_registry.get(Side.BUY, Decimal("60000")).unconfirmed
        assert service.wall_registry.get(Side.BUY, Decimal("59000")).unconfirmed

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run("reconnect.jsonl", tmp_path, "a.db")
        _, second = run("reconnect.jsonl", tmp_path, "b.db")
        assert first == second  # dataclass 동등성 — 전 필드 일치


class TestStreamOrderInversion:
    """접촉 스냅샷보다 aggTrade가 먼저 도착해도 리필 쌍 판정이 성립."""

    def test_trade_before_contact_snapshot_still_pairs(self, tmp_path):
        service, emitted = run("order_inversion.jsonl", tmp_path)
        icebergs = [e for e in emitted if isinstance(e, D4Iceberg)]
        # 5사이클 전부 인정되어야만 발화 (쌍 5 ≥ ICEBERG_MIN_TRADES 5) —
        # 1사이클(역전 도착)이 누락되면 쌍 4로 이 이벤트 자체가 없다
        assert len(icebergs) == 1
        assert icebergs[0].price == Decimal("61000")
        assert icebergs[0].refill_added == Decimal("50")
        assert icebergs[0].refill_trade_count == 5

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run("order_inversion.jsonl", tmp_path, "a.db")
        _, second = run("order_inversion.jsonl", tmp_path, "b.db")
        assert first == second


class TestDiffGap:
    """U/u 갭 → 레지스트리 전체 unconfirmed + epoch 종료(진행 판정 폐기)·즉시 재개."""

    def test_gap_discards_in_flight_judgment_and_marks_unconfirmed(self, tmp_path):
        service, emitted = run("diff_gap.jsonl", tmp_path)

        # 갭 전 APPEARED 1회뿐 — 갭 후 60000은 unconfirmed라 재발화 억제 (PRD §12.1 규칙 2)
        assert len([e for e in emitted if isinstance(e, D1Appeared)]) == 1
        # 흡수 500(33% ≥ 30%)이었지만 episode가 갭에서 폐기 → 반등에도 D3 무발화
        assert [e for e in emitted if isinstance(e, D3Absorption)] == []

        # 갭 마킹: 기존 벽은 unconfirmed, 갭 이벤트 자신의 가격(58000)은 확인 상태
        assert service.wall_registry.get(Side.BUY, Decimal("60000")).unconfirmed
        assert service.wall_registry.get(Side.BUY, Decimal("59000")).unconfirmed
        assert not service.wall_registry.get(Side.BUY, Decimal("58000")).unconfirmed
        # 갭은 같은 이벤트 처리 내에서 새 epoch 시작 (M1 구현 노트)
        assert service.tracker.epoch_active

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run("diff_gap.jsonl", tmp_path, "a.db")
        _, second = run("diff_gap.jsonl", tmp_path, "b.db")
        assert first == second
