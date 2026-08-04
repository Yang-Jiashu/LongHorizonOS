"""Migration compatibility tests (Milestone 2.3).

Tests migration 003 on three database scenarios:
1. New database (uses updated schema.sql directly)
2. Old schema empty database (migration must rebuild table)
3. Old schema populated database (migration must preserve all data)
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from lhos.infrastructure.db.connection import Database

OLD_SCHEMA_SQL = """
CREATE TABLE runs(
    id TEXT PRIMARY KEY, goal TEXT NOT NULL, status TEXT NOT NULL,
    config_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE events(
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL, actor_type TEXT NOT NULL, actor_id TEXT,
    payload_json TEXT NOT NULL, evidence_ids_json TEXT NOT NULL,
    causation_id TEXT, correlation_id TEXT, idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, sequence), UNIQUE(run_id, idempotency_key)
);
CREATE TABLE nodes(
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL,
    title TEXT NOT NULL, specification TEXT NOT NULL, state TEXT NOT NULL,
    version INTEGER NOT NULL, schedulable INTEGER NOT NULL,
    priority REAL NOT NULL, progress_weight REAL NOT NULL,
    estimated_token_cost INTEGER, estimated_time_ms INTEGER,
    estimated_tool_calls INTEGER,
    actual_token_cost INTEGER NOT NULL, actual_time_ms INTEGER NOT NULL,
    actual_tool_calls INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
    verification_spec_json TEXT, metadata_json TEXT NOT NULL,
    lease_owner TEXT, lease_expires_at TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE edges(
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
    source_node_id TEXT NOT NULL, target_node_id TEXT NOT NULL,
    kind TEXT NOT NULL, active INTEGER NOT NULL, version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    UNIQUE(run_id, source_node_id, target_node_id, kind)
);
CREATE TABLE evidence(
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, evidence_type TEXT NOT NULL,
    source_event_id TEXT NOT NULL, uri TEXT, content_hash TEXT,
    summary TEXT, metadata_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE executions(
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL, context_hash TEXT NOT NULL,
    model_name TEXT, status TEXT NOT NULL,
    input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL,
    tool_calls INTEGER NOT NULL, cost_usd REAL NOT NULL,
    started_at TEXT NOT NULL, finished_at TEXT,
    result_json TEXT, error_json TEXT,
    checkpoint_before TEXT, checkpoint_after TEXT,
    UNIQUE(node_id, attempt_number)
);
CREATE TABLE checkpoints(
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, checkpoint_type TEXT NOT NULL,
    location TEXT NOT NULL, manifest_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX idx_events_run_sequence ON events(run_id, sequence);
CREATE INDEX idx_nodes_run_state ON nodes(run_id, state);
CREATE INDEX idx_edges_source ON edges(run_id, source_node_id);
CREATE INDEX idx_edges_target ON edges(run_id, target_node_id);
CREATE INDEX idx_executions_node ON executions(node_id, attempt_number);
"""


def _create_old_db(db_path: Path) -> None:
    """Create a database with the OLD schema (UNIQUE(node_id, attempt_number))."""
    conn = sqlite3.connect(str(db_path))
    conn.executescript(OLD_SCHEMA_SQL)
    conn.commit()
    conn.close()


def _insert_old_exec(conn, exec_id, run_id, node_id, attempt, status="verified"):
    conn.execute(
        "INSERT INTO executions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            exec_id,
            run_id,
            node_id,
            attempt,
            "hash",
            "model",
            status,
            100,
            50,
            2,
            0.01,
            "2024-01-01T00:00:00",
            "2024-01-01T00:01:00",
            "{}",
            None,
            None,
            None,
        ),
    )
    conn.commit()


def _exec_content_hash(conn) -> str:
    """Compute a hash of all execution data for comparison."""
    rows = conn.execute(
        "SELECT id, run_id, node_id, attempt_number, status, input_tokens, "
        "output_tokens, tool_calls, cost_usd, started_at, finished_at "
        "FROM executions ORDER BY id"
    ).fetchall()
    data = "|".join("|".join(str(c) for c in r) for r in rows)
    return hashlib.sha256(data.encode()).hexdigest()


class TestNewDatabaseUsesCorrectConstraint:
    def test_new_database_uses_correct_constraint(self, tmp_path):
        """A fresh database must have UNIQUE(run_id, node_id, attempt_number)."""
        db = Database(tmp_path / "fresh.db")
        try:
            schema = db.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='executions'"
            ).fetchone()
            assert "UNIQUE(run_id, node_id, attempt_number)" in schema["sql"]
            assert "UNIQUE(node_id, attempt_number)" not in schema["sql"]
        finally:
            db.close()


class TestOldEmptyDatabaseMigrates:
    def test_old_empty_database_migrates(self, tmp_path):
        """An old-schema empty database must migrate without errors."""
        db_path = tmp_path / "old_empty.db"
        _create_old_db(db_path)

        db = Database(db_path)
        try:
            schema = db.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='executions'"
            ).fetchone()
            assert "UNIQUE(run_id, node_id, attempt_number)" in schema["sql"]
            assert "UNIQUE(node_id, attempt_number)" not in schema["sql"]

            # Old index should be gone
            old_idx = db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_executions_node'"
            ).fetchone()
            assert old_idx is None
        finally:
            db.close()


class TestOldPopulatedDatabaseMigratesWithoutDataLoss:
    def test_old_populated_database_migrates_without_data_loss(self, tmp_path):
        """An old-schema database with data must preserve ALL execution records."""
        db_path = tmp_path / "old_data.db"
        _create_old_db(db_path)

        # Insert test data: multiple runs, same node_id suffix, same attempt
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "INSERT INTO runs VALUES ('run-a', 'goal', 'ok', '{}', '2024-01-01', '2024-01-01')"
        )
        conn.execute(
            "INSERT INTO runs VALUES ('run-b', 'goal', 'ok', '{}', '2024-01-01', '2024-01-01')"
        )

        # Run A: n1 attempt 1, n2 attempt 1
        _insert_old_exec(conn, "exec-a1", "run-a", "run-a:n1", 1)
        _insert_old_exec(conn, "exec-a2", "run-a", "run-a:n2", 1)
        # Run B: n1 attempt 1 (same node suffix + attempt, different run)
        _insert_old_exec(conn, "exec-b1", "run-b", "run-b:n1", 1)
        _insert_old_exec(conn, "exec-b2", "run-b", "run-b:n1", 2, status="failed")
        conn.close()

        # Compute pre-migration hash
        conn_pre = sqlite3.connect(str(db_path))
        pre_hash = _exec_content_hash(conn_pre)
        pre_count = conn_pre.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        conn_pre.close()

        # Run migration
        db = Database(db_path)
        try:
            # Compute post-migration hash
            post_hash = _exec_content_hash(db.conn)
            post_count = db.conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]

            # Data preserved
            assert post_count == pre_count == 4
            assert post_hash == pre_hash

            # All execution IDs preserved
            ids = {r[0] for r in db.conn.execute("SELECT id FROM executions").fetchall()}
            assert ids == {"exec-a1", "exec-a2", "exec-b1", "exec-b2"}

            # All run IDs preserved
            run_ids = {
                r[0] for r in db.conn.execute("SELECT DISTINCT run_id FROM executions").fetchall()
            }
            assert run_ids == {"run-a", "run-b"}

            # New constraint in place
            schema = db.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='executions'"
            ).fetchone()
            assert "UNIQUE(run_id, node_id, attempt_number)" in schema["sql"]
        finally:
            db.close()


class TestMigrationPreservesAllExecutionColumns:
    def test_migration_preserves_all_execution_columns(self, tmp_path):
        """All columns and their values must survive migration."""
        db_path = tmp_path / "old_cols.db"
        _create_old_db(db_path)

        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "INSERT INTO runs VALUES ('run-x', 'g', 'ok', '{}', '2024-01-01', '2024-01-01')"
        )
        conn.execute(
            "INSERT INTO executions VALUES ("
            "'exec-full', 'run-x', 'run-x:n1', 1, 'ctx-hash-123', 'test-model', 'verified', "
            "1500, 750, 3, 0.05, '2024-01-01T10:00:00', '2024-01-01T10:05:00', "
            "'{\"key\": \"value\"}', '{\"err\": null}', 'ckpt-before', 'ckpt-after'"
            ")"
        )
        conn.commit()
        conn.close()

        db = Database(db_path)
        try:
            row = db.conn.execute("SELECT * FROM executions WHERE id = 'exec-full'").fetchone()
            assert row["id"] == "exec-full"
            assert row["run_id"] == "run-x"
            assert row["node_id"] == "run-x:n1"
            assert row["attempt_number"] == 1
            assert row["context_hash"] == "ctx-hash-123"
            assert row["model_name"] == "test-model"
            assert row["status"] == "verified"
            assert row["input_tokens"] == 1500
            assert row["output_tokens"] == 750
            assert row["tool_calls"] == 3
            assert row["cost_usd"] == 0.05
            assert row["started_at"] == "2024-01-01T10:00:00"
            assert row["finished_at"] == "2024-01-01T10:05:00"
            assert "value" in row["result_json"]
            assert row["checkpoint_before"] == "ckpt-before"
            assert row["checkpoint_after"] == "ckpt-after"
        finally:
            db.close()


class TestOldAndNewDatabaseHaveEquivalentSchema:
    def test_old_and_new_database_have_equivalent_schema(self, tmp_path):
        """After migration, an old database's schema must match a new database's."""
        # Create old DB and migrate
        old_path = tmp_path / "old.db"
        _create_old_db(old_path)
        old_db = Database(old_path)

        # Create new DB
        new_db = Database(tmp_path / "new.db")

        try:
            old_schema = old_db.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='executions'"
            ).fetchone()["sql"]
            new_schema = new_db.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='executions'"
            ).fetchone()["sql"]

            # Schemas should be identical (same columns, same constraint)
            assert old_schema == new_schema

            # Indexes should match
            old_indexes = {
                r[0]
                for r in old_db.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='executions'"
                ).fetchall()
            }
            new_indexes = {
                r[0]
                for r in new_db.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='executions'"
                ).fetchall()
            }
            assert old_indexes == new_indexes
        finally:
            old_db.close()
            new_db.close()
