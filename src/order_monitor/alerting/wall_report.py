"""호가벽 정기 리포트 포맷터 (docs/DECISIONS.md 2026-07-12 — PRD 외 신규).

벽 레지스트리 스냅샷을 매수벽(지지)/매도벽(저항)으로 나눠 한 장의 텔레그램
메시지로 요약한다. 판정이 아니라 표시 전용 — dedup/쿨다운 미적용, epoch 활성
중에만 발송 (현재가는 depth 스냅샷 기반이라 epoch 밖에서는 신뢰 불가).

표시 규칙 (사용자 확정 2026-07-12, 포맷 간소화 2026-07-13):
- 대형(🧱) = size_threshold_btc 이상 → 전부 표시. 소형(·) = 그 미만(등록 하한 이상)
  → 현재가 근접순 상위 SMALL_WALL_CAP개, 잘린 개수는 "외 소형 N개"
- 섹션 내 정렬은 현재가 근접순 (매도벽 = 가격 오름차순, 매수벽 = 내림차순)
- unconfirmed도 구분 없이 표시하되, 현재가 반대편에 남은 unconfirmed 벽(가격
  통과 후 재확인 이벤트가 없던 잔재)은 잔량 신뢰가 안 되므로 표시 제외
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from order_monitor.ingestion.events import Side
from order_monitor.state.wall_registry import Wall

_KST = timezone(timedelta(hours=9))

SMALL_WALL_CAP = 8


def _fmt(value: Decimal) -> str:
    return format(value.normalize(), ",f")


def _fmt_qty(value: Decimal) -> str:
    return format(value.quantize(Decimal(1)) if value >= 100 else value.normalize(), ",f")


def _wall_line(wall: Wall, mid: Decimal, size_threshold: Decimal) -> str:
    marker = "🧱" if wall.last_qty >= size_threshold else "·"
    distance = (wall.price - mid) / mid * 100
    return f"{marker} {_fmt(wall.price)} — {_fmt_qty(wall.last_qty)} BTC ({distance:+.1f}%)"


def _section(
    title: str, walls: list[Wall], mid: Decimal, size_threshold: Decimal
) -> list[str]:
    lines = [f"── {title} ──"]
    if not walls:
        lines.append("(없음)")
        return lines
    large = [w for w in walls if w.last_qty >= size_threshold]
    small = [w for w in walls if w.last_qty < size_threshold]
    shown = sorted(
        large + small[:SMALL_WALL_CAP], key=lambda w: abs(w.price - mid)
    )
    lines += [_wall_line(w, mid, size_threshold) for w in shown]
    if len(small) > SMALL_WALL_CAP:
        lines.append(f"외 소형 {len(small) - SMALL_WALL_CAP}개")
    return lines


def format_wall_report(
    walls: list[Wall],
    *,
    best_bid: Decimal,
    best_ask: Decimal,
    symbol: str,
    size_threshold: Decimal,
    now_epoch_seconds: float,
) -> str:
    mid = (best_bid + best_ask) / 2
    resistance: list[Wall] = []
    support: list[Wall] = []
    for wall in walls:
        wrong_side = (wall.side is Side.SELL) == (wall.price < mid)
        if wall.unconfirmed and wrong_side:
            continue  # 가격 통과 후 재확인 없는 잔재 — 잔량 신뢰 불가
        (resistance if wall.side is Side.SELL else support).append(wall)

    # 근접순: 매도벽은 가격 오름차순, 매수벽은 내림차순
    resistance.sort(key=lambda w: w.price)
    support.sort(key=lambda w: w.price, reverse=True)

    now = datetime.fromtimestamp(now_epoch_seconds, _KST)
    lines = [
        f"📊 호가벽 현황 — {symbol} (KST {now:%H:%M})",
        f"현재가 {_fmt(mid.quantize(Decimal(1)))}",
        "",
        *_section("매도벽 (저항)", resistance, mid, size_threshold),
        "",
        *_section("매수벽 (지지)", support, mid, size_threshold),
    ]
    return "\n".join(lines)
