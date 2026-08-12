"""SQLite storage backend for Agent OS.

Provides:
- Schema initialization
- Raw query helpers (dict rows)
- Transaction context manager
"""

from __future__ import annotations

import json
import sqlite3
import threading
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
        # A single sqlite3.Connection permits only one in-flight transaction at
        # a time even with check_same_thread=False. Serialize every explicit
        # BEGIN/COMMIT pair so an IMMEDIATE acquisition cannot overlap a deferred
        # waiter/journal transaction on the same connection.
        self._write_lock = threading.RLock()
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        # Allow concurrent writers to wait for a held write lock instead of
        # failing immediately with SQLITE_BUSY. Required so N concurrent
        # `BEGIN IMMEDIATE` txns serialize cleanly (LEASE-01 downstream).
        self.conn.execute("PRAGMA busy_timeout = 5000")
        self._init_schema()

    def _init_schema(self) -> None:
        with self.transaction():
            for ddl in ALL_DDL:
                self.conn.execute(ddl)
            # Backward-compatible additive migrations for databases created
            # before action resource contracts were persisted.
            action_columns = {
                row["name"]
                for row in self.conn.execute("PRAGMA table_info(actions_projection)").fetchall()
            }
            if "resource_claims_json" not in action_columns:
                self.conn.execute(
                    "ALTER TABLE actions_projection "
                    "ADD COLUMN resource_claims_json TEXT NOT NULL DEFAULT '[]'"
                )
            if "fencing_tokens_json" not in action_columns:
                self.conn.execute(
                    "ALTER TABLE actions_projection "
                    "ADD COLUMN fencing_tokens_json TEXT NOT NULL DEFAULT '{}'"
                )
            lease_columns = {
                row["name"]
                for row in self.conn.execute("PRAGMA table_info(leases_projection)").fetchall()
            }
            if "fencing_token" not in lease_columns:
                # Existing leases pre-date fencing and therefore cannot safely
                # authorize a new external commit. Give every legacy row a
                # positive token and advance the durable per-resource counter
                # to at least that value before accepting new acquisitions.
                self.conn.execute(
                    "ALTER TABLE leases_projection "
                    "ADD COLUMN fencing_token INTEGER NOT NULL DEFAULT 1"
                )
            self.conn.execute(
                """
                INSERT INTO resource_fencing_tokens(resource_id, last_token)
                SELECT resource_id, MAX(fencing_token)
                FROM leases_projection
                GROUP BY resource_id
                ON CONFLICT(resource_id) DO UPDATE SET
                    last_token = MAX(last_token, excluded.last_token)
                """
            )
            for idx in CREATE_INDEXES:
                self.conn.execute(idx)
            # Initialize journal meta
            self.conn.execute(
                "INSERT OR IGNORE INTO journal_meta(key, value) VALUES ('next_offset', 0)"
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO journal_meta(key, value) VALUES ('next_pid_seq', 0)"
            )

    def transaction(self, immediate: bool = False) -> _Tx:
        return _Tx(self.conn, immediate=immediate, write_lock=self._write_lock)

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> sqlite3.Cursor:
        with self._write_lock:
            return self.conn.execute(sql, params)

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._write_lock:
            row = self.conn.execute(sql, params).fetchone()
            return dict(row) if row else None

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._write_lock:
            rows = self.conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def close(self) -> None:
        with self._write_lock:
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

    def __init__(
        self,
        conn: sqlite3.Connection,
        immediate: bool = False,
        write_lock: Any | None = None,
    ):
        self.conn = conn
        self.immediate = immediate
        self.write_lock = write_lock

    def __enter__(self) -> _Tx:
        # Every explicit transaction shares one sqlite3.Connection, so all
        # BEGIN/COMMIT pairs must be serialized across threads.
        if self.write_lock is not None:
            self.write_lock.acquire()
        # BEGIN IMMEDIATE takes a write lock up front, blocking concurrent writers
        # so that a check-then-insert sequence inside the txn is serializable.
        self.conn.execute("BEGIN IMMEDIATE" if self.immediate else "BEGIN")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if exc_type is None:
                self.conn.execute("COMMIT")
            else:
                self.conn.execute("ROLLBACK")
        finally:
            if self.write_lock is not None:
                self.write_lock.release()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
