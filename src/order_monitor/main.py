from __future__ import annotations

import argparse
import logging

from order_monitor.config import load_config
from order_monitor.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="BTC 오더북 인텐트→실체결 모니터")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--log-file", default="order_monitor.log")
    args = parser.parse_args()

    setup_logging(args.log_file)
    config = load_config(args.config)
    logger.info(
        "config loaded",
        extra={"exchange": config.exchange, "symbol": config.symbol},
    )


if __name__ == "__main__":
    main()
