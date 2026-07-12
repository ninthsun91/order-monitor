"""물량벽 정기 리포트 포맷터 — 분류·정렬·캡·unconfirmed 표시 규칙."""

from decimal import Decimal

from order_monitor.alerting.wall_report import SMALL_WALL_CAP, format_wall_report
from order_monitor.ingestion.events import Side
from order_monitor.state.wall_registry import Wall

NOW = 1783825200.0  # 2026-07-12 12:00 KST


def wall(price, qty, side=Side.BUY, unconfirmed=False):
    return Wall(
        price=Decimal(price),
        side=side,
        last_qty=Decimal(qty),
        peak_qty=Decimal(qty),
        first_seen_at=NOW,
        first_seen_above_threshold=None,
        last_seen_at=NOW,
        unconfirmed=unconfirmed,
    )


def report(walls, best_bid="63999", best_ask="64001"):
    return format_wall_report(
        walls,
        best_bid=Decimal(best_bid),
        best_ask=Decimal(best_ask),
        symbol="BTC/USDT",
        size_threshold=Decimal(1000),
        now_epoch_seconds=NOW,
    )


def test_header_and_sections():
    text = report(
        [
            wall("66000", "1850", Side.SELL),
            wall("62000", "1200", Side.BUY),
            wall("63800", "210", Side.BUY),
        ]
    )
    assert "📊 물량벽 현황 — BTC/USDT (KST 12:00)" in text
    assert "현재가 64,000 · 추적 3개" in text
    assert "🧱 66,000 — 1,850 BTC (+3.1%)" in text
    assert "🧱 62,000 — 1,200 BTC (-3.1%)" in text
    assert "· 63,800 — 210 BTC (-0.3%)" in text
    assert "🧱 ≥1,000 BTC · ? = 미확인" in text


def test_sides_map_to_resistance_and_support():
    text = report([wall("65000", "300", Side.SELL), wall("63000", "300", Side.BUY)])
    resistance, support = text.split("── 지지 (bid) ──")
    assert "65,000" in resistance and "63,000" not in resistance
    assert "63,000" in support


def test_sections_sorted_nearest_first():
    text = report(
        [
            wall("70000", "200", Side.SELL),
            wall("65000", "200", Side.SELL),
            wall("58000", "200", Side.BUY),
            wall("63000", "200", Side.BUY),
        ]
    )
    assert text.index("65,000") < text.index("70,000")
    assert text.index("63,000") < text.index("58,000")


def test_small_walls_capped_but_large_always_shown():
    smalls = [wall(str(63000 - i * 100), "150") for i in range(SMALL_WALL_CAP + 3)]
    far_large = wall("50000", "5000")
    text = report(smalls + [far_large])
    assert "🧱 50,000 — 5,000 BTC" in text  # 캡과 무관하게 대형은 전부
    assert "외 소형 3개" in text
    # 근접순 상위 8개만 — 가장 먼 소형(62,000 아래 3개)은 제외
    assert "62,300" in text and "62,200" not in text


def test_unconfirmed_marker_and_wrong_side_exclusion():
    text = report(
        [
            wall("63000", "500", Side.BUY, unconfirmed=True),
            # 현재가(64,000) 위에 남은 bid 벽 잔재 — 표시 제외
            wall("65000", "800", Side.BUY, unconfirmed=True),
        ]
    )
    assert "· 63,000 — 500 BTC (-1.6%) ?" in text
    assert "65,000" not in text


def test_empty_sections_render_placeholder():
    text = report([])
    assert text.count("(없음)") == 2
    assert "추적 0개" in text
