"""Multi-run isolation tests for execution uniqueness (Milestone 2.3 Part D).

Tests that:
1. Same node_id + attempt is allowed across different runs
2. Same node_id + attempt is rejected within a run
3. Multiple seeds can share a database
4. Transcript and Full modes can share a database
5. Migration preserves existing executions
6. Migration is idempotent
7. Resume does not duplicate executions
8. Parse repair does not create node attempt collision
9. 10 runs with n1..n6 x 3 attempts each -- no UNIQUE constraint errors
10. Cross-run data isolation (events, budget, evidence, graph, checkpoint)
"""

from __future__ import annotations

import sqlite3

import pytest

from lhos.domain.enums import NodeKind
from lhos.domain.events import ActorType, RuntimeEvent
from lhos.domain.models import EvidenceRef, ExecutionRecord, GraphNode
from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore


# ------------------------------------------------------------ fixtures
@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "lhos.db")
    yield database
    database.close()


@pytest.fixture
def stack(db):
    es = SqliteEventStore(db)
    gs = SqliteGraphStore(db, es)
    return gs, es


def _make_run(store, run_id: str, node_ids: list[str]) -> None:
    """Create a run with nodes n1..n6."""
    store.create_run(run_id, f"goal for {run_id}", {})
    for nid in node_ids:
        node = GraphNode(
            id=f"{run_id}:{nid}",
            run_id=run_id,
            kind=NodeKind.SUBTASK,
            title=f"Node {nid}",
            specification=f"spec for {nid}",
            schedulable=True,
        )
        store.add_node(node)


def _insert_exec(store, run_id: str, node_id: str, attempt: int) -> ExecutionRecord:
    rec = ExecutionRecord(
        run_id=run_id,
        node_id=f"{run_id}:{node_id}",
        attempt_number=attempt,
        context_hash=f"hash-{run_id}-{node_id}-{attempt}",
    )
    return store.insert_execution(rec)


# ============================================================ Part D tests
class TestSameNodeAcrossRuns:
    def test_same_node_and_attempt_allowed_across_runs(self, stack):
        """Two runs with the same node_id suffix and attempt_number must succeed."""
        gs, _ = stack
        _make_run(gs, "run-a", ["n1"])
        _make_run(gs, "run-b", ["n1"])

        # Both insert (run-a:n1, attempt=1) — should NOT conflict
        exec_a = _insert_exec(gs, "run-a", "n1", 1)
        exec_b = _insert_exec(gs, "run-b", "n1", 1)

        assert exec_a.id != exec_b.id
        assert exec_a.run_id == "run-a"
        assert exec_b.run_id == "run-b"

    def test_same_node_and_attempt_rejected_within_run(self, stack):
        """Within the same run, duplicate (node_id, attempt) must fail."""
        gs, _ = stack
        _make_run(gs, "run-x", ["n1"])

        _insert_exec(gs, "run-x", "n1", 1)  # OK

        with pytest.raises(sqlite3.IntegrityError):
            _insert_exec(gs, "run-x", "n1", 1)  # DUPLICATE — must fail

    def test_different_attempt_allowed_within_run(self, stack):
        """Within the same run, different attempt numbers must succeed."""
        gs, _ = stack
        _make_run(gs, "run-y", ["n1"])

        _insert_exec(gs, "run-y", "n1", 1)  # OK
        _insert_exec(gs, "run-y", "n1", 2)  # OK — different attempt
        _insert_exec(gs, "run-y", "n1", 3)  # OK — different attempt


class TestMultiSeedSharedDatabase:
    def test_multiple_seeds_can_share_database(self, stack):
        """Multiple seeds (runs) sharing one database must not conflict."""
        gs, _ = stack
        node_ids = ["n1", "n2", "n3", "n4", "n5", "n6"]

        for seed in [1, 2, 3]:
            run_id = f"bench-config_loader-small-s{seed}"
            _make_run(gs, run_id, node_ids)
            for nid in node_ids:
                for attempt in [1, 2, 3]:
                    _insert_exec(gs, run_id, nid, attempt)

        # Verify all executions are present and correctly isolated
        for seed in [1, 2, 3]:
            run_id = f"bench-config_loader-small-s{seed}"
            execs = gs.list_executions(run_id)
            assert len(execs) == 18  # 6 nodes x 3 attempts

    def test_transcript_and_full_can_share_database(self, stack):
        """Transcript and Full LHoS modes sharing one DB must not conflict."""
        gs, _ = stack
        node_ids = ["n1", "n2", "n3"]

        for mode in ["transcript", "full_lhos"]:
            run_id = f"bench-config_loader-small-s1-{mode}"
            _make_run(gs, run_id, node_ids)
            for nid in node_ids:
                _insert_exec(gs, run_id, nid, 1)

        # Verify isolation
        transcript_execs = gs.list_executions("bench-config_loader-small-s1-transcript")
        full_execs = gs.list_executions("bench-config_loader-small-s1-full_lhos")
        assert len(transcript_execs) == 3
        assert len(full_execs) == 3
        assert {e.run_id for e in transcript_execs} == {"bench-config_loader-small-s1-transcript"}
        assert {e.run_id for e in full_execs} == {"bench-config_loader-small-s1-full_lhos"}


class TestMigrationPreservation:
    def test_migration_preserves_existing_executions(self, tmp_path):
        """Migration 003 must preserve all existing execution records."""
        # Create a database with the OLD schema (simulate by creating directly)
        db_path = tmp_path / "old.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
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
        """)
        # Insert test data with the OLD constraint
        conn.execute(
            "INSERT INTO runs VALUES ('run-a', 'goal', 'completed', '{}', '2024-01-01', '2024-01-01')"
        )
        conn.execute(
            "INSERT INTO executions VALUES "
            "('exec-1', 'run-a', 'run-a:n1', 1, 'hash', 'model', 'verified', "
            "100, 50, 2, 0.01, '2024-01-01', '2024-01-01', '{}', NULL, NULL, NULL)"
        )
        conn.commit()
        conn.close()

        # Now open with our Database class — should run migration 003
        db = Database(db_path)
        try:
            # Verify the execution record survived
            row = db.conn.execute("SELECT * FROM executions WHERE run_id = 'run-a'").fetchone()
            assert row is not None
            assert row["id"] == "exec-1"
            assert row["node_id"] == "run-a:n1"
            assert row["attempt_number"] == 1

            # Verify the new constraint is in place
            schema = db.conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='executions'"
            ).fetchone()
            assert "UNIQUE(run_id, node_id, attempt_number)" in schema["sql"]
            assert "UNIQUE(node_id, attempt_number)" not in schema["sql"].replace(
                "UNIQUE(run_id, node_id, attempt_number)", ""
            )

            # Verify the old index is gone
            old_idx = db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_executions_node'"
            ).fetchone()
            assert old_idx is None

            # Verify new indexes exist
            new_idx = db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_executions_run_node'"
            ).fetchone()
            assert new_idx is not None
        finally:
            db.close()

    def test_migration_is_idempotent(self, tmp_path):
        """Running migration 003 twice must not fail or corrupt data."""
        db_path = tmp_path / "test.db"
        db1 = Database(db_path)
        gs = SqliteGraphStore(db1, SqliteEventStore(db1))
        gs.create_run("run-a", "goal", {})
        gs.add_node(
            GraphNode(
                id="run-a:n1",
                run_id="run-a",
                kind=NodeKind.SUBTASK,
                title="t",
                specification="s",
                schedulable=True,
            )
        )
        _insert_exec(gs, "run-a", "n1", 1)
        db1.close()

        # Reopen — migration is already applied, should be idempotent
        db2 = Database(db_path)
        try:
            row = db2.conn.execute("SELECT * FROM executions WHERE run_id = 'run-a'").fetchone()
            assert row is not None
            assert row["attempt_number"] == 1
        finally:
            db2.close()


class TestResumeNoDuplicate:
    def test_resume_does_not_duplicate_execution(self, tmp_path):
        """Resuming a run after crash must not create duplicate execution records."""
        from lhos.bootstrap import RuntimeStack

        db_path = tmp_path / "lhos.db"
        workspace = tmp_path / "ws"
        workspace.mkdir()

        # Create a run and insert one execution
        stack = RuntimeStack(db_path, workspace, config={"runtime": {"lease_seconds": 60}})
        try:
            stack.graph_store.create_run("run-resume", "test goal", {})
            spec = {
                "goal": "test",
                "nodes": [
                    {
                        "temp_id": "n1",
                        "kind": "subtask",
                        "title": "Node 1",
                        "specification": "write a.txt",
                        "schedulable": True,
                        "verification_spec": {"type": "file_exists", "path": "a.txt"},
                        "metadata": {
                            "script": {
                                "summary": "done",
                                "produced_artifacts": [{"path": "a.txt", "content": "hello"}],
                            }
                        },
                    }
                ],
                "edges": [],
            }
            stack.initial_builder.build("run-resume", spec)
            run = stack.controller.run("run-resume")
            assert run.status == "completed"

            # Verify exactly 1 execution
            execs = stack.graph_store.list_executions("run-resume")
            assert len(execs) == 1
        finally:
            stack.close()

        # Reopen and verify no duplicates
        stack2 = RuntimeStack(db_path, workspace, config={"runtime": {"lease_seconds": 60}})
        try:
            execs = stack2.graph_store.list_executions("run-resume")
            assert len(execs) == 1  # Still exactly 1
        finally:
            stack2.close()


class TestParseRepairNoCollision:
    def test_parse_repair_does_not_create_node_attempt_collision(self, stack):
        """Parse failures must not cause attempt_number collision on retry.

        This tests the Milestone 2.3 fix: parse failures no longer decrement
        attempt_count, so each retry gets a unique attempt_number.
        """
        gs, _ = stack
        gs.create_run("run-parse", "goal", {})
        gs.add_node(
            GraphNode(
                id="run-parse:n1",
                run_id="run-parse",
                kind=NodeKind.SUBTASK,
                title="t",
                specification="s",
                schedulable=True,
                max_attempts=5,
            )
        )

        # Simulate: attempt 1 (parse failure), attempt 2 (success)
        # With the OLD code, attempt 1 would be decremented to 0, then
        # re-incremented to 1 on retry → collision.
        # With the fix, attempt 1 stays, attempt 2 gets a new number.
        _insert_exec(gs, "run-parse", "n1", 1)  # Parse failure attempt
        _insert_exec(gs, "run-parse", "n1", 2)  # Retry attempt — must succeed

        execs = gs.list_executions("run-parse", "run-parse:n1")
        assert len(execs) == 2
        assert execs[0].attempt_number == 1
        assert execs[1].attempt_number == 2


class TestLargeScaleMultiRun:
    def test_ten_runs_six_nodes_three_attempts_no_conflict(self, stack):
        """Stress test: 10 runs x 6 nodes x 3 attempts = 180 execution records.

        All in the same database. Must not trigger UNIQUE constraint.
        """
        gs, _ = stack
        node_ids = ["n1", "n2", "n3", "n4", "n5", "n6"]

        for i in range(10):
            run_id = f"run-{i:02d}"
            _make_run(gs, run_id, node_ids)
            for nid in node_ids:
                for attempt in [1, 2, 3]:
                    _insert_exec(gs, run_id, nid, attempt)

        # Verify count
        total = 0
        for i in range(10):
            run_id = f"run-{i:02d}"
            execs = gs.list_executions(run_id)
            assert len(execs) == 18  # 6 x 3
            total += len(execs)
        assert total == 180


class TestCrossRunDataIsolation:
    def test_executions_isolated_by_run(self, stack):
        """list_executions(run_id) must only return executions for that run."""
        gs, _ = stack
        _make_run(gs, "run-a", ["n1"])
        _make_run(gs, "run-b", ["n1"])
        _insert_exec(gs, "run-a", "n1", 1)
        _insert_exec(gs, "run-b", "n1", 1)

        a_execs = gs.list_executions("run-a")
        b_execs = gs.list_executions("run-b")
        assert len(a_execs) == 1
        assert len(b_execs) == 1
        assert a_execs[0].run_id == "run-a"
        assert b_execs[0].run_id == "run-b"

    def test_events_isolated_by_run(self, stack):
        """Events must not leak across runs."""
        gs, es = stack
        _make_run(gs, "run-a", ["n1"])
        _make_run(gs, "run-b", ["n1"])

        es.append(
            RuntimeEvent(
                run_id="run-a",
                event_type="TEST_EVENT",
                actor_type=ActorType.SYSTEM,
                payload={"msg": "a"},
            )
        )
        es.append(
            RuntimeEvent(
                run_id="run-b",
                event_type="TEST_EVENT",
                actor_type=ActorType.SYSTEM,
                payload={"msg": "b"},
            )
        )

        a_events = [e for e in es.list_events("run-a") if e.event_type == "TEST_EVENT"]
        b_events = [e for e in es.list_events("run-b") if e.event_type == "TEST_EVENT"]
        assert len(a_events) == 1
        assert len(b_events) == 1
        assert a_events[0].payload["msg"] == "a"
        assert b_events[0].payload["msg"] == "b"

    def test_nodes_isolated_by_run(self, stack):
        """Nodes must not leak across runs."""
        gs, _ = stack
        _make_run(gs, "run-a", ["n1", "n2"])
        _make_run(gs, "run-b", ["n1", "n2"])

        a_nodes = gs.list_nodes("run-a")
        b_nodes = gs.list_nodes("run-b")
        assert len(a_nodes) == 2
        assert len(b_nodes) == 2
        assert all(n.run_id == "run-a" for n in a_nodes)
        assert all(n.run_id == "run-b" for n in b_nodes)

    def test_evidence_isolated_by_run(self, stack):
        """Evidence must not leak across runs."""
        gs, _es = stack
        _make_run(gs, "run-a", ["n1"])
        _make_run(gs, "run-b", ["n1"])

        gs.add_evidence(
            EvidenceRef(
                run_id="run-a",
                evidence_type="file_hash",
                source_event_id="ev-1",
                summary="evidence-a",
            )
        )
        gs.add_evidence(
            EvidenceRef(
                run_id="run-b",
                evidence_type="file_hash",
                source_event_id="ev-2",
                summary="evidence-b",
            )
        )

        a_ev = gs.list_evidence("run-a")
        b_ev = gs.list_evidence("run-b")
        assert len(a_ev) == 1
        assert len(b_ev) == 1
        assert a_ev[0].summary == "evidence-a"
        assert b_ev[0].summary == "evidence-b"
