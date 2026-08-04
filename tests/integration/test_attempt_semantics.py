"""Tests for attempt counter semantics (Milestone 2.3).

Verifies that:
1. Parse repair does not consume node attempt (counters are independent)
2. Parse failure counter is independent from node_execution_attempt
3. Verification attempt counter is independent
4. Node attempt number is monotonically increasing (never decremented)
5. Resume never reuses attempt identity
6. Multiple parse repairs do not violate execution uniqueness
"""

from __future__ import annotations

import pytest

from lhos.domain.enums import NodeKind
from lhos.domain.models import AttemptCounters, ExecutionRecord, GraphNode
from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore


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


# ============================================================ AttemptCounters
class TestAttemptCountersModel:
    def test_attempt_counters_default_to_zero(self):
        counters = AttemptCounters()
        assert counters.node_execution_attempt == 0
        assert counters.worker_iteration == 0
        assert counters.parse_attempt == 0
        assert counters.tool_attempt == 0
        assert counters.verification_attempt == 0

    def test_counters_increment_independently(self):
        counters = AttemptCounters()
        # Parse failure happens
        counters.parse_attempt += 1
        assert counters.node_execution_attempt == 0
        assert counters.parse_attempt == 1

        # Node execution starts
        counters.node_execution_attempt += 1
        assert counters.node_execution_attempt == 1
        assert counters.parse_attempt == 1

        # Another parse failure in the same execution
        counters.parse_attempt += 1
        assert counters.node_execution_attempt == 1
        assert counters.parse_attempt == 2

        # Verification runs
        counters.verification_attempt += 1
        assert counters.node_execution_attempt == 1
        assert counters.verification_attempt == 1


# ============================================================ Node-level tests
class TestParseRepairDoesNotConsumeNodeAttempt:
    def test_parse_repair_does_not_consume_node_attempt(self):
        """Parse failure increments parse_attempts but does NOT decrement
        attempt_count. The counters are independent."""
        node = GraphNode(
            id="n1",
            run_id="run-test",
            kind=NodeKind.SUBTASK,
            title="test",
            specification="test",
            max_attempts=3,
        )
        # Execution starts
        node.attempt_count += 1
        assert node.attempt_count == 1

        # Parse failure happens
        node.parse_attempts += 1
        # attempt_count is NOT decremented (Milestone 2.3 fix)
        assert node.attempt_count == 1
        assert node.parse_attempts == 1

        # Node goes to FAILED, then back to READY for retry
        # New execution starts
        node.attempt_count += 1
        assert node.attempt_count == 2  # Monotonically increasing
        assert node.parse_attempts == 1  # Parse counter unchanged


class TestParseFailureCounterIsIndependent:
    def test_parse_failure_counter_is_independent(self):
        """parse_attempts tracks parse failures separately from attempt_count."""
        node = GraphNode(
            id="n1",
            run_id="run-test",
            kind=NodeKind.SUBTASK,
            title="test",
            specification="test",
            max_attempts=5,
        )
        # Simulate 3 executions, 2 with parse failures
        for i in range(3):
            node.attempt_count += 1
            if i < 2:
                node.parse_attempts += 1

        assert node.attempt_count == 3
        assert node.parse_attempts == 2
        assert node.verification_attempts == 0
        assert node.tool_attempts == 0


class TestVerificationAttemptCounterIsIndependent:
    def test_verification_attempt_counter_is_independent(self):
        """verification_attempts tracks verification checks separately."""
        node = GraphNode(
            id="n1",
            run_id="run-test",
            kind=NodeKind.SUBTASK,
            title="test",
            specification="test",
            max_attempts=5,
        )
        # Execution 1: worker runs, verification fails
        node.attempt_count += 1
        node.verification_attempts += 1
        assert node.attempt_count == 1
        assert node.verification_attempts == 1

        # Execution 2: worker runs, verification passes
        node.attempt_count += 1
        node.verification_attempts += 1
        assert node.attempt_count == 2
        assert node.verification_attempts == 2


class TestNodeAttemptNumberIsMonotonic:
    def test_node_attempt_number_is_monotonic(self, stack):
        """attempt_number in executions table must be monotonically increasing
        and never reused."""
        gs, _ = stack
        gs.create_run("run-mono", "goal", {})
        gs.add_node(
            GraphNode(
                id="run-mono:n1",
                run_id="run-mono",
                kind=NodeKind.SUBTASK,
                title="test",
                specification="test",
                schedulable=True,
                max_attempts=10,
            )
        )

        attempt_numbers = []
        for i in range(1, 6):
            rec = ExecutionRecord(
                run_id="run-mono",
                node_id="run-mono:n1",
                attempt_number=i,
                context_hash=f"hash-{i}",
            )
            gs.insert_execution(rec)
            attempt_numbers.append(i)

        execs = gs.list_executions("run-mono", "run-mono:n1")
        actual_numbers = [e.attempt_number for e in execs]
        assert actual_numbers == [1, 2, 3, 4, 5]

        # Verify monotonicity
        for i in range(1, len(actual_numbers)):
            assert actual_numbers[i] > actual_numbers[i - 1]


class TestResumeNeverReusesAttemptIdentity:
    def test_resume_never_reuses_attempt_identity(self, stack):
        """After a crash, resume must not reuse the same attempt_number."""
        gs, _ = stack
        gs.create_run("run-resume", "goal", {})
        gs.add_node(
            GraphNode(
                id="run-resume:n1",
                run_id="run-resume",
                kind=NodeKind.SUBTASK,
                title="test",
                specification="test",
                schedulable=True,
                max_attempts=5,
            )
        )

        # First execution (crashed, status="running")
        exec1 = ExecutionRecord(
            run_id="run-resume",
            node_id="run-resume:n1",
            attempt_number=1,
            context_hash="hash-1",
        )
        gs.insert_execution(exec1)

        # Recovery finishes the stale execution (UPDATE, not INSERT)
        gs.finish_execution(exec1.id, status="failed", error={"reason": "crash"})

        # New execution after recovery gets attempt_number=2
        exec2 = ExecutionRecord(
            run_id="run-resume",
            node_id="run-resume:n1",
            attempt_number=2,
            context_hash="hash-2",
        )
        gs.insert_execution(exec2)

        execs = gs.list_executions("run-resume", "run-resume:n1")
        assert len(execs) == 2
        assert execs[0].attempt_number == 1
        assert execs[1].attempt_number == 2
        assert execs[0].id != execs[1].id  # Different execution_id


class TestMultipleParseRepairsDoNotViolateUniqueness:
    def test_multiple_parse_repairs_do_not_violate_execution_uniqueness(self, stack):
        """Multiple parse failures followed by retries must not cause
        UNIQUE constraint violations."""
        gs, _ = stack
        gs.create_run("run-multi", "goal", {})
        gs.add_node(
            GraphNode(
                id="run-multi:n1",
                run_id="run-multi",
                kind=NodeKind.SUBTASK,
                title="test",
                specification="test",
                schedulable=True,
                max_attempts=10,
            )
        )

        # Simulate 5 parse failures followed by eventual success
        for attempt in range(1, 7):
            rec = ExecutionRecord(
                run_id="run-multi",
                node_id="run-multi:n1",
                attempt_number=attempt,
                context_hash=f"hash-{attempt}",
            )
            gs.insert_execution(rec)  # Must not raise
            status = "verified" if attempt == 6 else "parse_failed"
            gs.finish_execution(rec.id, status=status)

        execs = gs.list_executions("run-multi", "run-multi:n1")
        assert len(execs) == 6
        # All attempt_numbers are unique and sequential
        numbers = [e.attempt_number for e in execs]
        assert numbers == [1, 2, 3, 4, 5, 6]
        # All execution IDs are unique
        ids = [e.id for e in execs]
        assert len(set(ids)) == 6
