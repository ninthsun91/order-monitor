"""WatchLevelObserver 단위 테스트 (PRD §8 W v1.13)."""

from decimal import Decimal

from order_monitor.detectors.watch_level import (
    FINAL_MANUAL,
    FINAL_RESISTANCE_BROKEN,
    FINAL_SUPPORT_BROKEN,
    ROLE_RESISTANCE,
    ROLE_SUPPORT,
    WatchFinal,
    WatchFirstContact,
    WatchLevelObserver,
    WatchPeriodicReport,
)
from order_monitor.ingestion.events import AggTradeEvent, Side
from order_monitor.state.candles import Candle

TF_15M_MS = 15 * 60_000


class FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def trade(price: str, qty: str = "1", side: Side = Side.SELL, t_ms: int = 0) -> AggTradeEvent:
    return AggTradeEvent(
        agg_trade_id=t_ms,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=side,
        exchange_time_ms=t_ms,
        local_monotonic_receive_time=t_ms / 1000.0,
    )


def candle(close: str, *, tainted: bool = False) -> Candle:
    return Candle(
        open_time_ms=0,
        timeframe_ms=TF_15M_MS,
        close_price=Decimal(close),
        trade_count=1,
        gap_tainted=tainted,
    )


def make_observer(clock: FakeClock | None = None, monotonic: FakeClock | None = None) -> WatchLevelObserver:
    return WatchLevelObserver(
        contact_band_pct=Decimal("0.001"),
        confirm_timeframe="15m",
        confirm_closes=2,
        invalidate_buffer_pct=Decimal("0.0025"),
        report_interval_seconds=600.0,
        clock=clock or FakeClock(1_000_000.0),
        monotonic=monotonic or FakeClock(0.0),
    )


def register_65600(observer: WatchLevelObserver) -> None:
    assert observer.register(Decimal(65600), Decimal(65600), "65600")


class TestRegistration:
    def test_duplicate_rejected(self):
        obs = make_observer()
        register_65600(obs)
        assert obs.register(Decimal(65600), Decimal(65600), "65600") is None

    def test_unregister_returns_manual_final(self):
        obs = make_observer()
        register_65600(obs)
        final = obs.unregister(Decimal(65600), Decimal(65600))
        assert isinstance(final, WatchFinal)
        assert final.reason == FINAL_MANUAL
        assert obs.watches() == []

    def test_unregister_unknown_returns_none(self):
        obs = make_observer()
        assert obs.unregister(Decimal(1), Decimal(2)) is None


class TestRoleAndEpisode:
    def test_approach_from_above_is_support(self):
        obs = make_observer()
        register_65600(obs)
        obs.on_trade(trade("65700"))  # 구역 위 — 이력 기록
        events = obs.on_trade(trade("65650"))  # 밴드 진입 (band_hi=65665.6)
        assert len(events) == 1
        assert isinstance(events[0], WatchFirstContact)
        assert events[0].data.role == ROLE_SUPPORT
        assert events[0].data.episode_num == 1

    def test_approach_from_below_is_resistance(self):
        obs = make_observer()
        register_65600(obs)
        events = obs.on_trade(trade("65550"))  # 밴드 진입 (band_lo=65534.4), price < lo
        assert isinstance(events[0], WatchFirstContact)
        assert events[0].data.role == ROLE_RESISTANCE

    def test_registered_inside_range_zone_waits(self):
        # 구역 내부 등록 — 첫 구역 이탈 후 재진입까지 대기 (§8 W)
        obs = make_observer()
        obs.register(Decimal(64900), Decimal(66000), "64900-66000")
        assert obs.on_trade(trade("65500")) == []  # 구역 내부 — 역할 미정
        assert obs.watches()[0].role is None
        obs.on_trade(trade("64000"))  # 구역 아래로 이탈 (밴드도 이탈)
        events = obs.on_trade(trade("64850"))  # 아래에서 밴드 재진입
        assert isinstance(events[0], WatchFirstContact)
        assert events[0].data.role == ROLE_RESISTANCE

    def test_band_exit_ends_episode_but_keeps_watch(self):
        obs = make_observer()
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))  # 회차 1
        obs.on_trade(trade("65800"))  # 밴드 이탈 — 회차 종료
        events = obs.on_trade(trade("65660"))  # 재진입 — 회차 2, 첫 접촉 아님
        assert events == []
        assert obs.watches()[0].episode_num == 2

    def test_role_flips_on_reentry_from_other_side(self):
        # 마감 미확정 관통 후 반대편 재진입 — 역할 재판정 (§8 W)
        obs = make_observer()
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))  # 지지 테스트
        obs.on_trade(trade("65300"))  # 깊은 관통 — 밴드 아래로 이탈
        obs.on_trade(trade("65550"))  # 아래에서 재진입
        assert obs.watches()[0].role == ROLE_RESISTANCE


class TestCounting:
    def test_support_counts_at_or_below_hi_with_side_split(self):
        obs = make_observer()
        register_65600(obs)
        obs.on_trade(trade("65700"))  # 구역 위 — 미계측
        obs.on_trade(trade("65650"))  # 접촉 — 하지만 price > hi라 미계측
        obs.on_trade(trade("65600", qty="2", side=Side.SELL))
        obs.on_trade(trade("65580", qty="3", side=Side.BUY))
        data = obs.watches()[0]
        assert data.cum_sell == Decimal(2)
        assert data.cum_buy == Decimal(3)

    def test_deep_excursion_fully_counted(self):
        # 깊이 캡 없음 — 저점까지 전부 계측 (§8 W, 07-20 17:30 급락 사례)
        obs = make_observer()
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))
        obs.on_trade(trade("64000", qty="50"))  # -2.4% 급락 체결
        data = obs.watches()[0]
        assert data.cum_sell == Decimal(50)
        assert data.excursion_low == Decimal(64000)

    def test_cumulative_survives_episode_boundaries(self):
        # 64.9k "두 차례 흡수" — 누적은 회차 관통 생애 기준
        obs = make_observer()
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65600", qty="5"))  # 회차 1
        obs.on_trade(trade("65800"))  # 이탈
        obs.on_trade(trade("65600", qty="7"))  # 회차 2
        assert obs.watches()[0].cum_sell == Decimal(12)

    def test_no_counting_while_pending(self):
        obs = make_observer()
        obs.register(Decimal(64900), Decimal(66000), "64900-66000")
        obs.on_trade(trade("65500", qty="9"))  # 구역 내부, 역할 미정
        assert obs.watches()[0].cum_sell == Decimal(0)


class TestInvalidation:
    def _contacted_support(self) -> WatchLevelObserver:
        obs = make_observer()
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))
        return obs

    def test_two_consecutive_breach_closes_finalize(self):
        obs = self._contacted_support()
        # buffer 0.25% → 임계 65,436
        assert obs.on_candle_close(candle("65400")) == []  # 1/2
        events = obs.on_candle_close(candle("65300"))  # 2/2 — 확정
        assert len(events) == 1
        assert isinstance(events[0], WatchFinal)
        assert events[0].reason == FINAL_SUPPORT_BROKEN
        assert obs.watches() == []  # 관측 종료

    def test_recovery_close_resets_count(self):
        obs = self._contacted_support()
        obs.on_candle_close(candle("65400"))  # 1/2
        obs.on_candle_close(candle("65600"))  # 회복 마감 — 리셋
        assert obs.on_candle_close(candle("65400")) == []  # 다시 1/2
        assert obs.watches()[0].breach_closes == 1

    def test_close_within_buffer_is_not_breach(self):
        # 마감가 버퍼 — 65,436(=65600×0.99775) 이상 마감은 이탈 아님
        obs = self._contacted_support()
        obs.on_candle_close(candle("65500"))
        obs.on_candle_close(candle("65450"))
        assert obs.watches()[0].breach_closes == 0

    def test_wick_is_ignored_only_close_matters(self):
        # 62.8k 사례 — 깊은 꼬리 체결이 있어도 마감이 회복하면 무효화 없음
        obs = self._contacted_support()
        obs.on_trade(trade("65280", qty="30"))  # 0.5% 꼬리 체결 (계측은 됨)
        obs.on_candle_close(candle("65590"))  # 마감 회복
        data = obs.watches()[0]
        assert data.breach_closes == 0
        assert data.cum_sell == Decimal(30)  # 스윕 체결은 흡수로 집계

    def test_gap_tainted_candle_resets_and_skips(self):
        obs = self._contacted_support()
        obs.on_candle_close(candle("65400"))  # 1/2
        obs.on_candle_close(candle("65300", tainted=True))  # 공백 봉 — 제외 + 리셋
        assert obs.watches()[0].breach_closes == 0

    def test_resistance_breach_direction(self):
        obs = make_observer()
        register_65600(obs)
        events = obs.on_trade(trade("65550"))  # 아래에서 접촉 — 저항
        assert events[0].data.role == ROLE_RESISTANCE
        obs.on_candle_close(candle("65800"))  # buffer 0.25% → 임계 65,764
        events = obs.on_candle_close(candle("65800"))
        assert events[0].reason == FINAL_RESISTANCE_BROKEN

    def test_pending_watch_never_invalidates(self):
        obs = make_observer()
        obs.register(Decimal(64900), Decimal(66000), "64900-66000")
        obs.on_trade(trade("65500"))  # 대기
        assert obs.on_candle_close(candle("60000")) == []


class TestPeriodicReport:
    def test_report_due_with_activity(self):
        mono = FakeClock(0.0)
        obs = make_observer(monotonic=mono)
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))  # 첫 접촉 — 리포트 기준점
        obs.on_trade(trade("65590", qty="4"))
        mono.now = 601.0
        events = obs.on_tick()
        assert len(events) == 1
        assert isinstance(events[0], WatchPeriodicReport)
        assert events[0].data.interval_sell == Decimal(4)
        mono.now = 700.0
        assert obs.on_tick() == []  # due 미도래

    def test_no_activity_out_of_band_is_silent(self):
        mono = FakeClock(0.0)
        obs = make_observer(monotonic=mono)
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))
        obs.on_trade(trade("66500"))  # 밴드 밖으로 멀리 이탈, 이후 무활동
        mono.now = 601.0
        assert obs.on_tick() == []

    def test_silence_does_not_consume_due(self):
        # 게이팅 침묵은 due를 소모하지 않는다 — 활동 재개 시 즉시 발송
        mono = FakeClock(0.0)
        obs = make_observer(monotonic=mono)
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))
        obs.on_trade(trade("66500"))  # 이탈
        mono.now = 601.0
        assert obs.on_tick() == []
        obs.on_trade(trade("65590", qty="2"))  # 활동 재개
        mono.now = 602.0
        events = obs.on_tick()
        assert len(events) == 1

    def test_in_band_reports_even_without_counted_delta(self):
        mono = FakeClock(0.0)
        obs = make_observer(monotonic=mono)
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))  # 밴드 내 (price > hi — 미계측이지만 테스트 중)
        mono.now = 601.0
        assert len(obs.on_tick()) == 1

    def test_no_report_before_first_contact(self):
        mono = FakeClock(0.0)
        obs = make_observer(monotonic=mono)
        register_65600(obs)
        obs.on_trade(trade("67000"))  # 접촉 없음
        mono.now = 601.0
        assert obs.on_tick() == []

    def test_gap_flag_set_and_cleared_by_report(self):
        mono = FakeClock(0.0)
        obs = make_observer(monotonic=mono)
        register_65600(obs)
        obs.on_trade(trade("65700"))
        obs.on_trade(trade("65650"))
        obs.on_epoch_end()
        obs.on_trade(trade("65590"))
        mono.now = 601.0
        events = obs.on_tick()
        assert events[0].data.gap_flag is True
        obs.on_trade(trade("65590"))
        mono.now = 1202.0
        assert obs.on_tick()[0].data.gap_flag is False


class TestRestore:
    def test_restore_preserves_counters_and_severs_episode(self):
        obs = make_observer()
        obs.restore(
            lo=Decimal(65600),
            hi=Decimal(65600),
            literal="65600",
            registered_at=999.0,
            role=ROLE_SUPPORT,
            episode_num=3,
            first_contact_at=1000.0,
            cum_buy=Decimal(10),
            cum_sell=Decimal(20),
            excursion_low=Decimal(65400),
            excursion_high=Decimal(65600),
        )
        data = obs.watches()[0]
        assert data.cum_sell == Decimal(20)
        assert data.episode_num == 3
        assert data.in_band is False  # 회차 단절
        assert data.breach_closes == 0  # 카운트 리셋
        assert data.gap_flag is True  # 관측 공백 표기

    def test_restored_watch_resumes_counting(self):
        obs = make_observer()
        obs.restore(
            lo=Decimal(65600),
            hi=Decimal(65600),
            literal="65600",
            registered_at=999.0,
            role=ROLE_SUPPORT,
            episode_num=3,
            first_contact_at=1000.0,
            cum_buy=Decimal(0),
            cum_sell=Decimal(20),
            excursion_low=None,
            excursion_high=None,
        )
        obs.on_trade(trade("65650"))  # 위에서 밴드 재진입 — 새 회차, 지지 유지
        obs.on_trade(trade("65590", qty="5"))
        data = obs.watches()[0]
        assert data.role == ROLE_SUPPORT
        assert data.cum_sell == Decimal(25)
        assert data.episode_num == 4  # 재진입 = 새 회차

    def test_restored_watch_role_flips_if_price_now_other_side(self):
        # 공백 중 구역 완전 통과 — 무효화 없이 역할만 바뀐 채 재개 (§8 W 수용 한계)
        obs = make_observer()
        obs.restore(
            lo=Decimal(65600),
            hi=Decimal(65600),
            literal="65600",
            registered_at=999.0,
            role=ROLE_SUPPORT,
            episode_num=3,
            first_contact_at=1000.0,
            cum_buy=Decimal(0),
            cum_sell=Decimal(20),
            excursion_low=None,
            excursion_high=None,
        )
        obs.on_trade(trade("65590"))  # 아래에서 재진입 — 저항으로 재판정
        assert obs.watches()[0].role == ROLE_RESISTANCE
