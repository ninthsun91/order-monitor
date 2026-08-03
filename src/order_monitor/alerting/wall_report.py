"""호가벽 정기 리포트 포맷터 (docs/DECISIONS.md 2026-07-12 신설, 2026-08-03 다거래소 개정).

거래소별 벽 레지스트리 스냅샷들을 매수벽(지지)/매도벽(저항)으로 나눠 한 장의
텔레그램 메시지로 요약한다. 판정이 아니라 표시 전용 — dedup/쿨다운 미적용,
프라이머리 epoch 활성 중에만 발송. 다거래소 합류는 §5.5의 교차 거래소 판정
금지와 무관한 표시 집계다 — 통합 북이 아니며, 거리%는 각 벽의 자기 거래소
mid 기준이라 통화가 다른 심볼(USDT/USD) 가격을 직접 비교하지 않는다.

표시 규칙 (사용자 확정 2026-08-03 — 잠정 스펙, 사용성 따라 재개선 예정):
- 섹션별 표시 = 다음 3집합의 합집합, (거래소, 가격) 중복 제거
  ① 근접 NEAREST_CAP개 — 거래소 불문, 서브임계 벽 중 |거리%| 최소 순
    (대형은 ②로 어차피 전부 표시되므로 선정에서 제외해 슬롯을 아낀다)
  ② 대형(🧱) = 각 거래소 size_threshold_btc 이상 → 전부
  ③ 볼륨 탑 VOLUME_TOP_N개 — 거래소별×사이드별, 서브임계 벽 중 last_qty 상위
- 섹션 내 정렬은 |거리%| 오름차순, 미표시 관측 벽 수는 "외 N개"
- 스냅샷 2개 이상이면 라인에 [BN]/[CB] 태그, 현재가는 거래소별 병기
- unconfirmed도 구분 없이 표시하되, 자기 거래소 현재가 반대편에 남은
  unconfirmed 벽(가격 통과 후 재확인 이벤트가 없던 잔재)은 표시 제외
- epoch 비활성/mid 미확보 거래소는 집계에서 빼고 "⚠ {label}: 수집 중단 중" 표기
  (조용한 누락 방지 — 벽 없음과 수집 중단을 구분)
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from order_monitor.ingestion.events import Side
from order_monitor.state.wall_registry import Wall

_KST = timezone(timedelta(hours=9))

NEAREST_CAP = 5
VOLUME_TOP_N = 3


@dataclasses.dataclass(frozen=True)
class ExchangeWallSnapshot:
    """리포트 1회분의 거래소별 읽기 전용 스냅샷 — service가 자기 상태로 생성."""

    label: str  # 짧은 태그 (BN/CB)
    symbol: str
    walls: list[Wall]
    best_bid: Decimal | None
    best_ask: Decimal | None
    size_threshold: Decimal
    active: bool  # epoch 활성 + bid/ask 확보


@dataclasses.dataclass(frozen=True)
class _Entry:
    label: str
    wall: Wall
    distance_pct: Decimal  # 자기 거래소 mid 기준
    large: bool


def _fmt(value: Decimal) -> str:
    return format(value.normalize(), ",f")


def _fmt_qty(value: Decimal) -> str:
    # 정수 BTC 표시 — CB 플로어(50)로 100 미만 벽이 처음 생겨 소수 원값 노출 방지 (2026-08-03)
    return format(value.quantize(Decimal(1)), ",f")


def _entry_line(entry: _Entry, tagged: bool) -> str:
    marker = "🧱" if entry.large else "·"
    tag = f"[{entry.label}] " if tagged else ""
    return (
        f"{marker} {tag}{_fmt(entry.wall.price)} — {_fmt_qty(entry.wall.last_qty)} BTC"
        f" ({entry.distance_pct:+.1f}%)"
    )


def _select(entries: list[_Entry]) -> list[_Entry]:
    """합집합 선정 규칙 ①+②+③ — 반환은 |거리%| 오름차순."""
    large = [e for e in entries if e.large]
    small = [e for e in entries if not e.large]
    nearest = sorted(small, key=lambda e: abs(e.distance_pct))[:NEAREST_CAP]
    by_label: dict[str, list[_Entry]] = {}
    for entry in small:
        by_label.setdefault(entry.label, []).append(entry)
    volume_top = [
        e
        for group in by_label.values()
        for e in sorted(group, key=lambda e: e.wall.last_qty, reverse=True)[:VOLUME_TOP_N]
    ]
    shown: dict[tuple[str, Decimal], _Entry] = {}
    for entry in large + nearest + volume_top:
        shown.setdefault((entry.label, entry.wall.price), entry)
    return sorted(shown.values(), key=lambda e: abs(e.distance_pct))


def _section(title: str, entries: list[_Entry], tagged: bool) -> list[str]:
    lines = [f"── {title} ──"]
    if not entries:
        lines.append("(없음)")
        return lines
    shown = _select(entries)
    lines += [_entry_line(e, tagged) for e in shown]
    hidden = len(entries) - len(shown)
    if hidden:
        lines.append(f"외 {hidden}개")
    return lines


def format_wall_report(
    snapshots: list[ExchangeWallSnapshot],
    *,
    now_epoch_seconds: float,
) -> str:
    """첫 스냅샷 = 프라이머리 (발송 게이트는 service 몫이라 active 전제)."""
    tagged = len(snapshots) > 1
    mids: list[tuple[str, Decimal]] = []
    inactive: list[str] = []
    resistance: list[_Entry] = []
    support: list[_Entry] = []
    for snap in snapshots:
        if not snap.active or snap.best_bid is None or snap.best_ask is None:
            inactive.append(snap.label)
            continue
        mid = (snap.best_bid + snap.best_ask) / 2
        mids.append((snap.label, mid))
        for wall in snap.walls:
            wrong_side = (wall.side is Side.SELL) == (wall.price < mid)
            if wall.unconfirmed and wrong_side:
                continue  # 가격 통과 후 재확인 없는 잔재 — 잔량 신뢰 불가
            entry = _Entry(
                label=snap.label,
                wall=wall,
                distance_pct=(wall.price - mid) / mid * 100,
                large=wall.last_qty >= snap.size_threshold,
            )
            (resistance if wall.side is Side.SELL else support).append(entry)

    price_line = " · ".join(
        f"{label} {_fmt(mid.quantize(Decimal(1)))}" if tagged else _fmt(mid.quantize(Decimal(1)))
        for label, mid in mids
    )
    now = datetime.fromtimestamp(now_epoch_seconds, _KST)
    lines = [
        f"📊 호가벽 현황 — {snapshots[0].symbol} (KST {now:%H:%M})",
        f"현재가 {price_line}",
        "",
        *_section("매도벽 (저항)", resistance, tagged),
        "",
        *_section("매수벽 (지지)", support, tagged),
    ]
    lines += [f"⚠ {label}: 수집 중단 중" for label in inactive]
    return "\n".join(lines)
