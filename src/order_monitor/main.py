from __future__ import annotations

import argparse
import asyncio
import logging
import os

from order_monitor.config import load_config
from order_monitor.logging_setup import setup_logging
from order_monitor.service import MonitorService

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="BTC 오더북 인텐트→실체결 모니터")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--log-file", default="order_monitor.log")
    parser.add_argument("--db-file", default="order_monitor.db")
    parser.add_argument("--heartbeat-file", default="order_monitor.heartbeat")
    args = parser.parse_args()

    # 토큰은 환경변수로만 주입 (PRD §9.3). 없으면 기동 거부 — 알림 없는 조용한
    # 실행은 조용한 실패와 같다 (§11.1 신뢰성 최우선)
    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not telegram_token:
        parser.error("TELEGRAM_BOT_TOKEN 환경변수가 설정되어 있지 않습니다")

    setup_logging(args.log_file)
    config = load_config(args.config)
    logger.info(
        "config loaded",
        extra={"symbol": config.symbol, "exchanges": sorted(config.exchanges)},
    )

    # 바이낸스 = 프라이머리 (공유 sender·heartbeat·수신 명령 소유), 신규 거래소는
    # 독립 파이프라인으로 병행 (M8, PRD §5.5 — exchanges 섹션 부재 시 바이낸스 단독)
    service = MonitorService(
        config,
        db_path=args.db_file,
        telegram_token=telegram_token,
        heartbeat_path=args.heartbeat_file,
    )
    services = [service]
    for exchange in sorted(config.exchanges):
        services.append(
            MonitorService(
                config,
                db_path=args.db_file,
                telegram_token=telegram_token,
                heartbeat_path=args.heartbeat_file,
                exchange=exchange,
                telegram_sender=service.telegram,
            )
        )

    async def run_all() -> None:
        async with asyncio.TaskGroup() as group:
            for svc in services:
                group.create_task(svc.run())

    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        logger.info("shutdown requested")


if __name__ == "__main__":
    main()
