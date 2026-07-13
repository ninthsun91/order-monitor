from decimal import Decimal

from order_monitor.detectors.contact import (
    ContactEpisode,
    EpisodeEnd,
    EpisodeEndReason,
)
from order_monitor.detectors.d1 import D1Appeared, D1Attribution, D1Removed
from order_monitor.detectors.d3 import D3Detector
from order_monitor.ingestion.events import Side


def appeared(side=Side.BUY, price="61000", qty="1200"):
    return D1Appeared(side=side, price=Decimal(price), qty=Decimal(qty), persisted_seconds=3.0)


def removed(side=Side.BUY, price="61000"):
    return D1Removed(
        side=side,
        price=Decimal(price),
        last_qty=Decimal("100"),
        peak_qty=Decimal("1200"),
        cum_traded=Decimal("0"),
        attribution=D1Attribution.PULLED,
    )


def episode_end(side=Side.BUY, price="61000", reason=EpisodeEndReason.REBOUND, start=1.0, end=5.0):
    return EpisodeEnd(
        episode=ContactEpisode(side=side, price=Decimal(price), started_receive_time=start),
        reason=reason,
        ended_receive_time=end,
    )


def make_detector(cum: dict, min_pct="0.3"):
    return D3Detector(
        absorption_min_pct=Decimal(min_pct),
        cum_traded_lookup=lambda side, price: cum.get((side, price), Decimal(0)),
    )


class TestAbsorptionJudgment:
    def test_fires_on_rebound_end_when_cum_meets_min(self):
        cum = {(Side.BUY, Decimal("61000")): Decimal("400")}  # 400/1200 = 33% ≥ 30%
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared())
        events = d3.on_episode_end(episode_end())
        assert len(events) == 1
        event = events[0]
        assert event.registered_qty == Decimal("1200")
        assert event.absorbed_qty == Decimal("400")
        assert event.realized_rate == Decimal("400") / Decimal("1200")
        assert event.episode_seconds == 4.0
        assert event.end_reason == "rebound"

    def test_silent_below_min(self):
        cum = {(Side.BUY, Decimal("61000")): Decimal("300")}  # 25% < 30%
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared())
        assert d3.on_episode_end(episode_end()) == []

    def test_boundary_exactly_min_fires(self):
        cum = {(Side.BUY, Decimal("61000")): Decimal("360")}  # 정확히 30%
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared())
        assert len(d3.on_episode_end(episode_end())) == 1

    def test_pierced_episode_never_fires(self):
        # 관통 확정 시 해당 episode에서 D3 무발화 (PRD §8 D3 조건 3)
        cum = {(Side.BUY, Decimal("61000")): Decimal("999")}
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared())
        assert d3.on_episode_end(episode_end(reason=EpisodeEndReason.PIERCED)) == []

    def test_removed_end_fires(self):
        # 벽 소멸(FILLED성 소진)로 끝난 episode도 비관통 종료 — 판정 대상
        cum = {(Side.BUY, Decimal("61000")): Decimal("1100")}
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared())
        events = d3.on_episode_end(episode_end(reason=EpisodeEndReason.REMOVED))
        assert [e.end_reason for e in events] == ["removed"]

    def test_non_d1_level_silent(self):
        # D1 활성 전제 (PRD §8 D3) — 등록 없는 레벨은 무발화
        cum = {(Side.BUY, Decimal("61000")): Decimal("9999")}
        d3 = make_detector(cum)
        assert d3.on_episode_end(episode_end()) == []


class TestRegistrationLifecycle:
    def test_d1_removed_deregisters(self):
        cum = {(Side.BUY, Decimal("61000")): Decimal("400")}
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared())
        d3.on_d1_removed(removed())
        assert d3.on_episode_end(episode_end()) == []

    def test_multiple_episodes_judged_independently_with_lifetime_cum(self):
        # 생애 누적: 1차 접촉 300(25%, 침묵) 후 반등 이탈, 2차 접촉 +150 → 450(37.5%) 발화
        cum = {(Side.BUY, Decimal("61000")): Decimal("300")}
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared())
        assert d3.on_episode_end(episode_end(start=1.0, end=5.0)) == []
        cum[(Side.BUY, Decimal("61000"))] = Decimal("450")  # 2차 episode 중 추가 흡수
        events = d3.on_episode_end(episode_end(start=10.0, end=12.0))
        assert len(events) == 1
        assert events[0].absorbed_qty == Decimal("450")

    def test_ask_wall_symmetric(self):
        cum = {(Side.SELL, Decimal("62000")): Decimal("500")}
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared(side=Side.SELL, price="62000", qty="1000"))
        events = d3.on_episode_end(episode_end(side=Side.SELL, price="62000"))
        assert len(events) == 1
        assert events[0].side is Side.SELL

    def test_reset_clears_registrations(self):
        cum = {(Side.BUY, Decimal("61000")): Decimal("400")}
        d3 = make_detector(cum)
        d3.on_d1_appeared(appeared())
        d3.reset()
        assert d3.on_episode_end(episode_end()) == []
