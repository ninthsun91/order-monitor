"""결정적 replay 테스트 — M3/M4 완료 게이트 (PRD §13).

필수 3 시나리오: 재연결(epoch 종료/재개), 스트림 순서 역전, diff U/u 갭.
실물 MonitorService를 클록 주입으로 구동하므로 판정 로직만이 아니라
service 배선(epoch 게이팅, D1→D3/D5 라우팅, 소멸 순서)까지 검증 범위다.
M4: 재연결·diff 갭 시나리오에서 D5 INTERRUPTED가 결정적으로 발화하는지도 검증
(PRD §13 "D5 replay 테스트: M3 픽스처 확장 — ... 상태기계 결정성 검증").
"""

from decimal import Decimal
from pathlib import Path

from order_monitor.config import load_config
from order_monitor.detectors.d1 import D1Appeared
from order_monitor.detectors.d3 import D3Absorption
from order_monitor.detectors.d4 import D4Iceberg
from order_monitor.detectors.d5 import D5Terminal, D5TerminalState
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

        # 단절 시점 활성이던 첫 인텐트(60%에 못 미친 20%)만 INTERRUPTED —
        # 재개 후 두 번째 인텐트는 이 픽스처 끝까지 활성 유지(REALIZE_PCT 미도달)
        interrupted = [
            e for e in emitted if isinstance(e, D5Terminal) and e.state is D5TerminalState.INTERRUPTED
        ]
        assert len(interrupted) == 1
        assert interrupted[0].level_realized_rate == Decimal("0.2")  # 300/1500, 단절 시점 누적
        assert len(service.d5._intents) == 1  # 재개 후 인텐트는 종료 없이 활성 잔존

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
    """순서 역전 픽스처 — D4 비활성(PRD v1.6) 후에는 무이벤트가 골든.

    구 골든은 리필 쌍 5개의 D4Iceberg 1건 — 픽스처는 D4 재배선 논의를 위해
    보존하고, 비활성 동안은 역전 입력이 아무 이벤트도 만들지 않음을 고정한다.
    """

    def test_inversion_input_emits_nothing_while_d4_unwired(self, tmp_path):
        _, emitted = run("order_inversion.jsonl", tmp_path)
        assert [e for e in emitted if isinstance(e, D4Iceberg)] == []

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run("order_inversion.jsonl", tmp_path, "a.db")
        _, second = run("order_inversion.jsonl", tmp_path, "b.db")
        assert first == second


class TestLiveCapture:
    """실캡처 픽스처 재생 (PRD §13 "합성 + 실캡처") — 2026-07-13 60s 캡처.

    골든 값(벽 7개, D1 APPEARED 1건)은 캡처 시점 시장 상태의 고정 산물 —
    픽스처가 불변이므로 판정 로직 회귀 감지에 쓴다 (scripts/capture_stream.py로
    재캡처 시 골든도 재생성할 것).
    """

    FIXTURE = "live_btcusdt_60s.jsonl"

    def test_replays_without_error_and_matches_golden(self, tmp_path):
        service, emitted = run(self.FIXTURE, tmp_path)
        assert service.tracker.epoch_active
        assert len(service.wall_registry) == 7
        appeared = [e for e in emitted if isinstance(e, D1Appeared)]
        assert len(appeared) == 1  # 캡처 중 실존한 1k+ BTC 벽의 지속 필터 통과
        assert [type(e).__name__ for e in emitted] == ["D1Appeared"]
        # D5가 벽 APPEARED에서 인텐트를 등록했지만 60s 동안 그 가격에 체결이
        # 없어 진행률/종국 이벤트 없음(위 golden과 일치) — 등록 자체만 확인
        assert len(service.d5._intents) == 1

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run(self.FIXTURE, tmp_path, "a.db")
        _, second = run(self.FIXTURE, tmp_path, "b.db")
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

        # 갭으로 인한 epoch 종료 시 유일 활성 인텐트가 INTERRUPTED — 재확인 억제로
        # 재등록도 없어(위 APPEARED 1건 단언) 갭 이후 활성 인텐트는 0
        interrupted = [
            e for e in emitted if isinstance(e, D5Terminal) and e.state is D5TerminalState.INTERRUPTED
        ]
        assert len(interrupted) == 1
        assert interrupted[0].level_realized_rate == Decimal("500") / Decimal("1500")
        assert service.d5._intents == {}

    def test_deterministic_same_input_same_output(self, tmp_path):
        _, first = run("diff_gap.jsonl", tmp_path, "a.db")
        _, second = run("diff_gap.jsonl", tmp_path, "b.db")
        assert first == second
