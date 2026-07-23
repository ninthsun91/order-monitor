"""소형 운영 키-값 저장 (PRD §12 v1.13) — 텔레그램 `update_id` offset 등.

offset을 영속하지 않으면 재시작 시 이전 명령이 재실행된다 (§9.5 — 등록/해소는
멱등이라 피해는 제한적이나, 응답 메시지 중복 발송 방지).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL DEFAULT 0
)
"""


class KVStore:
    def __init__(self, path: str | Path) -> None:
        self._conn = sqlite3.connect(str(path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def get(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set(self, key: str, value: str, updated_at: float = 0.0) -> None:
        self._conn.execute(
            "INSERT INTO kv (key, value, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value,"
            " updated_at = excluded.updated_at",
            (key, value, updated_at),
        )
        self._conn.commit()
