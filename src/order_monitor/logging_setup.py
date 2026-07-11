from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

_RESERVED_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        extra = {
            key: value for key, value in record.__dict__.items() if key not in _RESERVED_ATTRS
        }
        payload.update(extra)

        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(
    log_path: str | Path,
    *,
    level: int = logging.INFO,
    max_bytes: int = 10_000_000,
    backup_count: int = 5,
    also_stdout: bool = True,
) -> None:
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = JsonFormatter()

    handlers: list[logging.Handler] = [
        RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    ]
    if also_stdout:
        handlers.append(logging.StreamHandler())

    for handler in handlers:
        handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    for handler in handlers:
        root.addHandler(handler)
