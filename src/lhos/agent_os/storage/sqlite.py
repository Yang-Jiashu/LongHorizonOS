"""SQLite storage backend for Agent OS.

Provides:
- Schema initialization
- Raw query helpers (dict rows)
- Transaction context manager
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from lhos.agent_os.storage.schema import ALL_DDL, CREATE_INDEXES

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class SQLiteStorage:
    """Thin wrapper around sqlite3.Connection with dict-row access."""

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.conn: sqlite3.Connection = sqlite3.connect(
            self.db_path,
            isolation_level=None,  # autocommit; we manage transactions explicitly
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        with self.transaction():
            for ddl in ALL_DDL:
                self.conn.execute(ddl)
            for idx in CREATE_INDEXES:
                self.conn.execute(idx)
            # Initialize journal meta
            self.conn.execute(
                "INSERT OR IGNORE INTO journal_meta(key, value) VALUES ('next_offset', 0)"
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO journal_meta(key, value) VALUES ('next_pid_seq', 0)"
            )

    def transaction(self) -> _Tx:
        return _Tx(self.conn)

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        self.conn.close()

    # ── JSON helpers ──────────────────────────────────────────────────────

    @staticmethod
    def dumps(obj: Any) -> str:
        return json.dumps(obj, default=str, sort_keys=True)

    @staticmethod
    def loads(s: str | None) -> Any:
        if s is None:
            return None
        return json.loads(s)


class _Tx:
    """Explicit transaction context manager."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __enter__(self) -> _Tx:
        self.conn.execute("BEGIN")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
