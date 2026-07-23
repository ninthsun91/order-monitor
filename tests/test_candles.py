"""CandleAssembler 단위 테스트 (PRD §8 W v1.13 — 봉 조립)."""

from decimal import Decimal

from order_monitor.ingestion.events import AggTradeEvent, Side
from order_monitor.state.candles import CandleAssembler

TF_15M_MS = 15 * 60_000


def trade(t_ms: int, price: str, qty: str = "1") -> AggTradeEvent:
    return AggTradeEvent(
        agg_trade_id=t_ms,
        price=Decimal(price),
        qty=Decimal(qty),
        aggressor_side=Side.SELL,
        exchange_time_ms=t_ms,
        local_monotonic_receive_time=t_ms / 1000.0,
    )


def fresh_assembler(timeframe: str = "15m") -> CandleAssembler:
    """공백 상태를 소진한 조립기 — gap 테스트가 아닌 케이스의 기저."""
    asm = CandleAssembler(timeframe)
    # 첫 봉(재시작 공백)을 소진: 버킷 0에 체결 후 버킷 1로 진입해 마감
    asm.on_trade(trade(0, "65000"))
    asm.on_trade(trade(TF_15M_MS, "65000"))
    return asm


class TestBucketing:
    def test_no_close_within_same_bucket(self):
        asm = CandleAssembler("15m")
        assert asm.on_trade(trade(0, "65000")) is None
        assert asm.on_trade(trade(TF_15M_MS - 1, "65100")) is None

    def test_close_on_new_bucket_with_last_price(self):
        asm = fresh_assembler()
        base = TF_15M_MS
        asm.on_trade(trade(base + 1000, "65200"))
        asm.on_trade(trade(base + 2000, "65150"))  # 마지막 체결가 = 마감가
        closed = asm.on_trade(trade(base + TF_15M_MS, "65300"))
        assert closed is not None
        assert closed.close_price == Decimal("65150")
        assert closed.open_time_ms == base
        assert closed.trade_count == 3  # fresh_assembler의 경계 체결 1 + 위 2
        assert closed.gap_tainted is False

    def test_boundary_alignment_1h(self):
        asm = CandleAssembler("1h")
        tf = 60 * 60_000
        asm.on_trade(trade(tf * 3 + 5000, "65000"))
        closed = asm.on_trade(trade(tf * 4, "65100"))
        assert closed.open_time_ms == tf * 3

    def test_skipped_buckets_do_not_emit_empty_candles(self):
        # 무체결 구간의 빈 봉은 만들지 않는다 — 판정은 실제 마감 봉에만
        asm = fresh_assembler()
        closed = asm.on_trade(trade(TF_15M_MS * 10, "64000"))
        assert closed is not None  # 직전 봉 1개만 마감
        assert closed.open_time_ms == TF_15M_MS

    def test_order_inversion_absorbed_into_current_bucket(self):
        # 드문 순서 역전 — 마감가는 도착 순서 기준 마지막 체결가
        asm = fresh_assembler()
        base = TF_15M_MS
        asm.on_trade(trade(base + 5000, "65200"))
        asm.on_trade(trade(base + 4000, "65250"))  # 역전 도착
        closed = asm.on_trade(trade(base + TF_15M_MS, "65300"))
        assert closed.close_price == Decimal("65250")


class TestGapTaint:
    def test_initial_candle_is_tainted(self):
        # 재시작 직후 첫 봉 = 관측 공백을 걸친 봉 (PRD §12.2 복원 규칙과 정합)
        asm = CandleAssembler("15m")
        asm.on_trade(trade(0, "65000"))
        closed = asm.on_trade(trade(TF_15M_MS, "65100"))
        assert closed.gap_tainted is True

    def test_second_candle_is_clean(self):
        asm = CandleAssembler("15m")
        asm.on_trade(trade(0, "65000"))
        asm.on_trade(trade(TF_15M_MS, "65100"))
        closed = asm.on_trade(trade(TF_15M_MS * 2, "65200"))
        assert closed.gap_tainted is False

    def test_gap_taints_in_progress_candle(self):
        asm = fresh_assembler()
        base = TF_15M_MS
        asm.on_trade(trade(base + 1000, "65200"))
        asm.mark_gap()
        asm.on_trade(trade(base + 2000, "65100"))  # 재개 — 같은 버킷
        closed = asm.on_trade(trade(base + TF_15M_MS, "65300"))
        assert closed.gap_tainted is True

    def test_gap_spanning_boundary_taints_both_candles(self):
        # 공백이 버킷 경계를 걸치면 직전 봉과 재개 봉이 모두 오염
        asm = fresh_assembler()
        base = TF_15M_MS
        asm.on_trade(trade(base + 1000, "65200"))
        asm.mark_gap()
        first = asm.on_trade(trade(base + TF_15M_MS + 1000, "65100"))  # 재개 — 다음 버킷
        assert first.gap_tainted is True
        second = asm.on_trade(trade(base + TF_15M_MS * 2, "65000"))
        assert second.gap_tainted is True  # 재개 체결이 속한 봉도 오염
        third = asm.on_trade(trade(base + TF_15M_MS * 3, "64900"))
        assert third.gap_tainted is False  # 그 다음 봉부터 정상
