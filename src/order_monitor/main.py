from __future__ import annotations

import argparse
import asyncio
import logging

from order_monitor.config import load_config
from order_monitor.logging_setup import setup_logging
from order_monitor.service import MonitorService

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="BTC 오더북 인텐트→실체결 모니터")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--log-file", default="order_monitor.log")
    parser.add_argument("--db-file", default="order_monitor.db")
    args = parser.parse_args()

    setup_logging(args.log_file)
    config = load_config(args.config)
    logger.info(
        "config loaded",
        extra={"symbol": config.symbol},
    )

    service = MonitorService(config, db_path=args.db_file)
    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        logger.info("shutdown requested")


if __name__ == "__main__":
    main()
