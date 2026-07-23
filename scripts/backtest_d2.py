"""D2 백테스트 — 실제 D2Detector에 Binance 1m klines를 재생 (M6 튜닝 재사용).

용도: 상대 임계·에피소드 파라미터가 실데이터에서 어떤 온셋/요약을 내는지 확인.
2026-07-12 M2 개편 검증에 사용 (docs/DECISIONS.md 2026-07-12 참고).

데이터 파일: [{"t": openTimeMs, "vol": float, "buy": takerBuyFloat, "close": float}, ...]
수집 예:
  curl "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=...&limit=1000"
  → row에서 t=[0], vol=[5], buy=[9], close=[4] 추출 (페이지네이션 필요)

근사: 1분봉을 매수/매도 합성 체결 2건으로 재생 — 실서비스의 체결 단위 롤링 60초
창보다 약간 둔감(분 경계 정렬). 기동 부트스트랩 대신 첫 24h 데이터로 워밍업.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from order_monitor.config import load_config
from order_monitor.detectors.d2 import D2BurstOnset, D2BurstSummary, D2Detector
from order_monitor.ingestion.events import AggTradeEvent, Side
from order_monitor.state.trade_window import TradeWindow
from order_monitor.state.volume_baseline import VolumeBaseline

KST = timezone(timedelta(hours=9))


class ManualClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def kst(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, KST).strftime("%m-%d %H:%M")


def replay(bars: list[dict], config) -> list:
    clock = ManualClock()
    # 1ms 축소: 합성 체결이 전부 분 경계에 정렬되어, 창 경계 포함(≥) 규칙상 직전
    # 분봉까지 창에 남는 해상도 아티팩트 보정 (실서비스의 체결 단위 창은 정확히 60s)
    window = TradeWindow(config.thresholds.window_seconds - 0.001)
    baseline = VolumeBaseline(config.thresholds.vol_baseline_hours)
    detector = D2Detector(
        vol_floor=Decimal(str(config.thresholds.vol_floor_btc)),
        vol_multiplier=Decimal(str(config.thresholds.vol_multiplier)),
        exit_ratio=Decimal(str(config.thresholds.episode_exit_ratio)),
        merge_seconds=config.thresholds.episode_merge_minutes * 60.0,
        directional_ratio=Decimal(str(config.thresholds.delta_directional_ratio)),
        balanced_ratio=Decimal(str(config.thresholds.delta_balanced_ratio)),
        window_seconds=config.thresholds.window_seconds,
        window=window,
        baseline=baseline,
        monotonic=clock,
    )

    events = []
    trade_id = 0
    for bar in bars:
        clock.now = bar["t"] / 1000
        vol = Decimal(str(bar["vol"]))
        buy = Decimal(str(bar["buy"]))
        price = Decimal(str(bar["close"]))
        baseline.add(bar["t"], vol)
        for side, qty in ((Side.BUY, buy), (Side.SELL, vol - buy)):
            if qty <= 0:
                continue
            trade_id += 1
            trade = AggTradeEvent(
                agg_trade_id=trade_id,
                price=price,
                qty=qty,
                aggressor_side=side,
                exchange_time_ms=bar["t"],
                local_monotonic_receive_time=clock.now,
            )
            window.add(trade)
            events.extend(detector.on_trade(trade))
        events.extend(detector.on_tick())
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="D2 에피소드 백테스트 (1m klines 재생)")
    parser.add_argument("--data", required=True, help="1m 봉 JSON 파일")
    parser.add_argument("--config", default="config.example.yaml")
    args = parser.parse_args()

    bars = json.loads(Path(args.data).read_text())
    config = load_config(args.config)
    events = replay(bars, config)

    onsets = [e for e in events if isinstance(e, D2BurstOnset)]
    summaries = [e for e in events if isinstance(e, D2BurstSummary)]
    span_days = (bars[-1]["t"] - bars[0]["t"]) / 86_400_000
    print(f"기간 {kst(bars[0]['t'])} ~ {kst(bars[-1]['t'])} KST ({span_days:.1f}일, 워밍업 24h 포함)")
    print(f"온셋 {len(onsets)}건 · 요약 {len(summaries)}건 (≈ {len(onsets) / max(span_days - 1, 0.01):.1f} 에피소드/일)")
    print()
    for s in summaries:
        duration = max(1, round((s.end_exchange_ms - s.start_exchange_ms) / 60_000))
        ratio = abs(s.buy_qty - s.sell_qty) / s.total_qty
        change = (s.close_price - s.open_price) / s.open_price * 100
        print(
            f"{kst(s.start_exchange_ms)}~{kst(s.end_exchange_ms)[-5:]} ({duration:3d}분) "
            f"누적 {s.total_qty:9.1f} BTC | {s.label.value:16s} |Δ|/V={ratio:.2f} | {change:+.2f}%"
        )


if __name__ == "__main__":
    main()
