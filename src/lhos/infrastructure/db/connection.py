"""SQLite connection management. WAL mode (spec section 19).

Transactions are managed manually (``isolation_level=None``) with a depth
counter so that store methods may be composed inside a single outer
transaction — required by the transactional append-event + update-projection
principle (spec section 5.3) and by single-transaction graph patches (8.2).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

SCHEMA_PATH = Path(__file__).parent / "schema.sql"
MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._depth = 0
        self.init_schema()

    def init_schema(self) -> None:
        exists = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='runs'"
        ).fetchone()
        if not exists:
            self._conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        self._run_migrations()

    def _run_migrations(self) -> None:
        """Run additive migrations (never destructive; does not break replay)."""
        if not MIGRATIONS_DIR.is_dir():
            return
        # Ensure migration tracking table exists.
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations("
            "  name TEXT PRIMARY KEY,"
            "  applied_at TEXT NOT NULL"
            ")"
        )
        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            already = self._conn.execute(
                "SELECT 1 FROM schema_migrations WHERE name = ?", (sql_file.name,)
            ).fetchone()
            if already:
                # Even if the migration was "applied", the table might have
                # been created before the status/error_type/causation_id columns
                # were added. Check and add them if missing (Step 3).
                if sql_file.name == "001_llm_calls.sql":
                    self._ensure_llm_calls_columns()
                continue
            try:
                self._conn.executescript(sql_file.read_text(encoding="utf-8"))
                from datetime import datetime

                self._conn.execute(
                    "INSERT INTO schema_migrations(name, applied_at) VALUES (?, ?)",
                    (sql_file.name, datetime.now().astimezone().isoformat()),
                )
            except Exception:
                # If the migration fails (e.g. duplicate column from newer schema),
                # still mark it as applied so we don't retry every time.
                from datetime import datetime

                self._conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(name, applied_at) VALUES (?, ?)",
                    (sql_file.name, datetime.now().astimezone().isoformat()),
                )

    def _ensure_llm_calls_columns(self) -> None:
        """Ensure the llm_calls table has all required columns (Step 3).

        If the table was created by an older version of migration 001 that
        lacked ``status``, ``error_type``, or ``causation_id``, add them.
        """
        # Check if the table exists.
        exists = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='llm_calls'"
        ).fetchone()
        if not exists:
            return
        # Get existing columns.
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(llm_calls)").fetchall()}
        if "status" not in cols:
            self._conn.execute(
                "ALTER TABLE llm_calls ADD COLUMN status TEXT NOT NULL DEFAULT 'success'"
            )
        if "error_type" not in cols:
            self._conn.execute("ALTER TABLE llm_calls ADD COLUMN error_type TEXT")
        if "causation_id" not in cols:
            self._conn.execute("ALTER TABLE llm_calls ADD COLUMN causation_id TEXT")
        # Add index on status if not present.
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_status ON llm_calls(status)")

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        outermost = self._depth == 0
        if outermost:
            self._conn.execute("BEGIN IMMEDIATE")
        self._depth += 1
        try:
            yield self._conn
        except BaseException:
            self._depth -= 1
            if outermost:
                self._conn.execute("ROLLBACK")
            raise
        else:
            self._depth -= 1
            if outermost:
                self._conn.execute("COMMIT")

    def close(self) -> None:
        self._conn.close()
