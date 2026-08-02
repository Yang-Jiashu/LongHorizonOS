"""SQLite connection management. WAL mode (spec section 19).

Transactions are managed manually (``isolation_level=None``) with a depth
counter so that store methods may be composed inside a single outer
transaction — required by the transactional append-event + update-projection
principle (spec section 5.3) and by single-transaction graph patches (8.2).
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
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
