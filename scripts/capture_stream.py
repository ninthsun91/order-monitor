"""거래소 raw 프레임 캡처 → replay 픽스처 JSONL (M3, PRD §13 실캡처. M8 Coinbase 확장).

tests/replay/runner.py가 읽는 포맷 그대로 기록한다:
    {"t": <시작 기준 monotonic 초>, "stream": "...", "data": {...raw...}}
첫 줄에는 {"t": 0.0, "control": "connect"}를 넣어 픽스처 단독 재생이 성립하게 한다.

Coinbase(--exchange coinbase)는 combined 래퍼가 없어 메시지 `type`에서 합성
스트림명(`coinbase:<product>@<channel>`)을 부여한다 — 어댑터 파서와 동일 규약 (PRD §5.5).

사용:
    python scripts/capture_stream.py --duration 60 --out tests/replay/fixtures/live.jsonl
    python scripts/capture_stream.py --exchange coinbase --symbol BTC-USD --duration 60 --out cb.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import aiohttp

from order_monitor.ingestion.events import stream_names

WS_BASE = "wss://stream.binance.com:9443/stream?streams="


async def capture(symbol: str, duration: float, out_path: str) -> int:
    url = WS_BASE + "/".join(stream_names(symbol))
    frames = 0
    with open(out_path, "w") as out:
        out.write(json.dumps({"t": 0.0, "control": "connect"}) + "\n")
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=20) as ws:
                start = time.monotonic()
                while True:
                    remaining = duration - (time.monotonic() - start)
                    if remaining <= 0:
                        break
                    try:
                        msg = await ws.receive(timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    if msg.type is not aiohttp.WSMsgType.TEXT:
                        continue
                    t = time.monotonic() - start
                    payload = json.loads(msg.data)
                    record = {"t": round(t, 4), "stream": payload["stream"], "data": payload["data"]}
                    out.write(json.dumps(record, separators=(",", ":")) + "\n")
                    frames += 1
    return frames


COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
_COINBASE_CHANNEL_BY_TYPE = {
    "snapshot": "level2",
    "l2update": "level2",
    "match": "matches",
    "last_match": "matches",
    "ticker": "ticker",
    "heartbeat": "heartbeat",
}


def coinbase_stream_name(product_id: str, msg_type: str) -> str:
    channel = _COINBASE_CHANNEL_BY_TYPE.get(msg_type, msg_type)
    return f"coinbase:{product_id}@{channel}"


async def capture_coinbase(product_id: str, duration: float, out_path: str) -> int:
    subscribe = {
        "type": "subscribe",
        "product_ids": [product_id],
        "channels": ["level2_batch", "matches", "ticker", "heartbeat"],
    }
    frames = 0
    with open(out_path, "w") as out:
        out.write(json.dumps({"t": 0.0, "control": "connect"}) + "\n")
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(COINBASE_WS, heartbeat=20) as ws:
                await ws.send_json(subscribe)
                start = time.monotonic()
                while True:
                    remaining = duration - (time.monotonic() - start)
                    if remaining <= 0:
                        break
                    try:
                        msg = await ws.receive(timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    if msg.type is not aiohttp.WSMsgType.TEXT:
                        continue
                    t = time.monotonic() - start
                    payload = json.loads(msg.data)
                    msg_type = payload.get("type", "unknown")
                    if msg_type == "subscriptions":  # 구독 ack — 데이터 아님
                        continue
                    record = {
                        "t": round(t, 4),
                        "stream": coinbase_stream_name(product_id, msg_type),
                        "data": payload,
                    }
                    out.write(json.dumps(record, separators=(",", ":")) + "\n")
                    frames += 1
    return frames


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", default="binance", choices=["binance", "coinbase"])
    parser.add_argument("--symbol", default=None, help="기본: binance=BTC/USDT, coinbase=BTC-USD")
    parser.add_argument("--duration", type=float, default=60.0, help="캡처 시간(초)")
    parser.add_argument("--out", required=True, help="출력 JSONL 경로")
    args = parser.parse_args()
    if args.exchange == "coinbase":
        product = args.symbol or "BTC-USD"
        frames = asyncio.run(capture_coinbase(product, args.duration, args.out))
    else:
        frames = asyncio.run(capture(args.symbol or "BTC/USDT", args.duration, args.out))
    print(f"captured {frames} frames → {args.out}")


if __name__ == "__main__":
    main()
