"""Binance combined 스트림 raw 프레임 캡처 → replay 픽스처 JSONL (M3, PRD §13 실캡처).

tests/replay/runner.py가 읽는 포맷 그대로 기록한다:
    {"t": <시작 기준 monotonic 초>, "stream": "...", "data": {...raw...}}
첫 줄에는 {"t": 0.0, "control": "connect"}를 넣어 픽스처 단독 재생이 성립하게 한다.

사용:
    python scripts/capture_stream.py --duration 60 --out tests/replay/fixtures/live.jsonl
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--duration", type=float, default=60.0, help="캡처 시간(초)")
    parser.add_argument("--out", required=True, help="출력 JSONL 경로")
    args = parser.parse_args()
    frames = asyncio.run(capture(args.symbol, args.duration, args.out))
    print(f"captured {frames} frames → {args.out}")


if __name__ == "__main__":
    main()
