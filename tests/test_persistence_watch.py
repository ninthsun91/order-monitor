"""WatchStore / KVStore 단위 테스트 (PRD §12.2 v1.13)."""

from decimal import Decimal

from order_monitor.detectors.watch_level import ROLE_SUPPORT, WatchReportData
from order_monitor.persistence.kv import KVStore
from order_monitor.persistence.watch_levels import WatchStore


def data(
    lo: str = "65600",
    hi: str = "65600",
    *,
    cum_sell: str = "0",
    episode_num: int = 0,
    role: str | None = None,
    first_contact_at: float | None = None,
    excursion_low: str | None = None,
) -> WatchReportData:
    return WatchReportData(
        lo=Decimal(lo),
        hi=Decimal(hi),
        literal=f"{lo}" if lo == hi else f"{lo}-{hi}",
        role=role,
        episode_num=episode_num,
        in_band=False,
        registered_at=1000.0,
        first_contact_at=first_contact_at,
        last_price=None,
        excursion_low=Decimal(excursion_low) if excursion_low else None,
        excursion_high=None,
        interval_buy=Decimal(0),
        interval_sell=Decimal(0),
        cum_buy=Decimal(0),
        cum_sell=Decimal(cum_sell),
        interval_seconds=0.0,
        breach_closes=0,
        confirm_closes=2,
        timeframe="15m",
        gap_flag=False,
    )


class TestWatchStore:
    def test_roundtrip_preserves_decimals(self, tmp_path):
        store = WatchStore(tmp_path / "w.db")
        store.upsert(
            data(role=ROLE_SUPPORT, cum_sell="123.456", episode_num=3,
                 first_contact_at=1234.5, excursion_low="65412.3"),
            flushed_at=2000.0,
        )
        rows = store.load()
        assert len(rows) == 1
        row = rows[0]
        assert row.lo == Decimal("65600")
        assert row.role == ROLE_SUPPORT
        assert row.episode_num == 3
        assert row.first_contact_at == 1234.5
        assert row.cum_sell == Decimal("123.456")
        assert row.excursion_low == Decimal("65412.3")
        assert row.final_text is None
        store.close()

    def test_upsert_updates_counters_in_place(self, tmp_path):
        store = WatchStore(tmp_path / "w.db")
        store.upsert(data(cum_sell="1"), flushed_at=1.0)
        store.upsert(data(cum_sell="9", episode_num=2), flushed_at=2.0)
        rows = store.load()
        assert len(rows) == 1
        assert rows[0].cum_sell == Decimal(9)
        assert rows[0].episode_num == 2
        store.close()

    def test_range_and_single_zone_are_distinct_rows(self, tmp_path):
        store = WatchStore(tmp_path / "w.db")
        store.upsert(data("65600", "65600"), flushed_at=1.0)
        store.upsert(data("64900", "66000"), flushed_at=1.0)
        assert store.count() == 2
        store.close()

    def test_mark_final_then_delete_after_send(self, tmp_path):
        # §9.4 v1.13 — 선기록(final_text) → 발송 확인 → 삭제
        store = WatchStore(tmp_path / "w.db")
        store.upsert(data(), flushed_at=1.0)
        store.mark_final(Decimal("65600"), Decimal("65600"), "support_broken", "최종 리포트 텍스트")
        row = store.load()[0]
        assert row.final_reason == "support_broken"
        assert row.final_text == "최종 리포트 텍스트"
        store.delete(Decimal("65600"), Decimal("65600"))
        assert store.count() == 0
        store.close()

    def test_restart_survives_with_unsent_final(self, tmp_path):
        path = tmp_path / "w.db"
        store = WatchStore(path)
        store.upsert(data(), flushed_at=1.0)
        store.mark_final(Decimal("65600"), Decimal("65600"), "support_broken", "미발송 최종")
        store.close()
        reopened = WatchStore(path)  # 크래시 후 재시작
        row = reopened.load()[0]
        assert row.final_text == "미발송 최종"
        reopened.close()

    def test_canonical_decimal_key_matching(self, tmp_path):
        # Decimal("65600.0")과 Decimal("65600")이 같은 행을 가리켜야 함
        store = WatchStore(tmp_path / "w.db")
        store.upsert(data(), flushed_at=1.0)
        store.delete(Decimal("65600.0"), Decimal("65600.0"))
        assert store.count() == 0
        store.close()


class TestKVStore:
    def test_get_missing_returns_none(self, tmp_path):
        kv = KVStore(tmp_path / "kv.db")
        assert kv.get("telegram_update_offset") is None
        kv.close()

    def test_set_and_overwrite(self, tmp_path):
        kv = KVStore(tmp_path / "kv.db")
        kv.set("telegram_update_offset", "100")
        kv.set("telegram_update_offset", "101", updated_at=5.0)
        assert kv.get("telegram_update_offset") == "101"
        kv.close()

    def test_survives_reopen(self, tmp_path):
        path = tmp_path / "kv.db"
        kv = KVStore(path)
        kv.set("k", "v")
        kv.close()
        reopened = KVStore(path)
        assert reopened.get("k") == "v"
        reopened.close()
