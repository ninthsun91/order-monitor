"""호가벽 정기 리포트 포맷터 — 합집합 선정(근접5/대형 전부/볼륨 탑3)·다거래소 표시 규칙.

표시 규칙 개정 2026-08-03 (docs/DECISIONS.md) — 거래소별 스냅샷 리스트 입력,
거리%는 각자 자기 mid 기준.
"""

from decimal import Decimal

from order_monitor.alerting.wall_report import (
    NEAREST_CAP,
    VOLUME_TOP_N,
    ExchangeWallSnapshot,
    format_wall_report,
)
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


def snap(
    walls,
    label="BN",
    symbol="BTC/USDT",
    best_bid="63999",
    best_ask="64001",
    threshold="1000",
    active=True,
):
    return ExchangeWallSnapshot(
        label=label,
        symbol=symbol,
        walls=walls,
        best_bid=Decimal(best_bid),
        best_ask=Decimal(best_ask),
        size_threshold=Decimal(threshold),
        active=active,
    )


def report(*snapshots):
    return format_wall_report(list(snapshots), now_epoch_seconds=NOW)


def test_header_and_sections_single_exchange_keeps_legacy_format():
    text = report(
        snap(
            [
                wall("66000", "1850", Side.SELL),
                wall("62000", "1200", Side.BUY),
                wall("63800", "210", Side.BUY),
            ]
        )
    )
    assert "📊 호가벽 현황 — BTC/USDT (KST 12:00)" in text
    assert "현재가 64,000" in text
    assert "🧱 66,000 — 1,850 BTC (+3.1%)" in text
    assert "🧱 62,000 — 1,200 BTC (-3.1%)" in text
    assert "· 63,800 — 210 BTC (-0.3%)" in text
    assert "[BN]" not in text  # 단일 거래소면 태그 생략 — 현행 포맷 유지


def test_sides_map_to_resistance_and_support():
    text = report(snap([wall("65000", "300", Side.SELL), wall("63000", "300", Side.BUY)]))
    resistance, support = text.split("── 매수벽 (지지) ──")
    assert "65,000" in resistance and "63,000" not in resistance
    assert "63,000" in support


def test_sections_sorted_nearest_first():
    text = report(
        snap(
            [
                wall("70000", "200", Side.SELL),
                wall("65000", "200", Side.SELL),
                wall("58000", "200", Side.BUY),
                wall("63000", "200", Side.BUY),
            ]
        )
    )
    assert text.index("65,000") < text.index("70,000")
    assert text.index("63,000") < text.index("58,000")


def test_union_of_nearest_large_and_volume_top():
    # 근접 5(63,800~63,400) + 볼륨 탑3(63,300/63,200/63,100) + 대형 전부(50,000)
    nearest = [wall(str(63800 - i * 100), str(150 + i)) for i in range(NEAREST_CAP)]
    far = [
        wall("63300", "300"),
        wall("63200", "290"),
        wall("63100", "280"),
        wall("63000", "270"),  # 어느 집합에도 못 들어감
        wall("62900", "260"),
    ]
    text = report(snap(nearest + far + [wall("50000", "5000")]))
    assert "🧱 50,000 — 5,000 BTC" in text  # 대형은 거리 무관 전부
    assert "63,400" in text  # 근접 5의 끝
    assert "63,100" in text  # 볼륨 탑3의 끝
    assert "63,000" not in text and "62,900" not in text
    assert "외 2개" in text


def test_nearest_slots_not_consumed_by_large():
    # 가장 근접한 벽이 대형이어도 근접 5 슬롯은 서브임계 벽으로만 채운다
    large = wall("63900", "1500")
    smalls = [wall(str(63800 - i * 100), str(200 - i * 10)) for i in range(6)]
    text = report(snap([large] + smalls))
    assert "🧱 63,900" in text
    assert "63,400" in text  # 근접 5번째 소형 — 대형이 슬롯을 먹으면 탈락했을 벽
    assert "63,300" not in text  # 6번째 소형 (볼륨 탑3도 근접 상위와 겹침)
    assert "외 1개" in text


def test_two_exchanges_rank_by_own_mid_distance_with_tags():
    bn = snap([wall("63800", "150"), wall("63700", "140")])
    cb = snap(
        [
            wall("63850", "60"),  # CB mid(63,900) 기준 -0.1% — 전 거래소 통합 근접 1위
            wall("62000", "600"),  # CB 대형 (임계 500)
            wall("55000", "312"),  # 볼륨 탑3 — 근접으로는 절대 안 보이는 원거리 벽
            wall("51000", "154"),
            wall("52013", "117"),
            wall("60000", "51"),
        ],
        label="CB",
        symbol="BTC-USD",
        best_bid="63899",
        best_ask="63901",
        threshold="500",
    )
    text = report(bn, cb)
    assert "현재가 BN 64,000 · CB 63,900" in text
    assert "🧱 [CB] 62,000 — 600 BTC" in text
    assert "· [CB] 55,000 — 312 BTC" in text  # 거래소별 볼륨 탑3로 진입
    assert "· [CB] 52,013 — 117 BTC" in text
    # 통합 정렬은 자기 mid 기준 거리% — CB 63,850(-0.1%)이 BN 63,800(-0.3%)보다 앞
    assert text.index("[CB] 63,850") < text.index("[BN] 63,800")


def test_volume_top_is_per_exchange():
    # BN 소형이 CB 소형보다 전부 크더라도 CB 몫의 볼륨 탑 VOLUME_TOP_N은 따로 뽑힌다
    bn_smalls = [wall(str(63800 - i * 100), str(900 - i)) for i in range(NEAREST_CAP + 4)]
    cb = snap(
        [wall("55000", "312"), wall("51000", "154"), wall("52013", "117"), wall("53000", "90")],
        label="CB",
        best_bid="63899",
        best_ask="63901",
        threshold="500",
    )
    text = report(snap(bn_smalls), cb)
    assert "· [CB] 55,000" in text and "· [CB] 51,000" in text and "· [CB] 52,013" in text
    assert "[CB] 53,000" not in text  # CB 볼륨 4위 (근접 5는 BN 근접벽들 차지)


def test_inactive_snapshot_excluded_with_notice():
    cb = snap([wall("60000", "300")], label="CB", active=False)
    text = report(snap([wall("63800", "200")]), cb)
    assert "⚠ CB: 수집 중단 중" in text
    assert "60,000" not in text
    assert "현재가 BN 64,000" in text  # 활성 거래소만 병기


def test_unconfirmed_wrong_side_judged_by_own_mid():
    # CB mid 62,000 — 그 위에 남은 unconfirmed bid 잔재는 BN mid(64,000) 아래라도 제외
    cb = snap(
        [wall("63000", "80", unconfirmed=True), wall("61000", "80", unconfirmed=True)],
        label="CB",
        best_bid="61999",
        best_ask="62001",
        threshold="500",
    )
    text = report(snap([]), cb)
    assert "63,000" not in text
    assert "· [CB] 61,000 — 80 BTC" in text  # 정상 측 unconfirmed는 구분 없이 표시


def test_empty_sections_render_placeholder():
    text = report(snap([]))
    assert text.count("(없음)") == 2
