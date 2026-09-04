"""Crash/reopen tests for the Scheduler-owned durable projection."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta

import pytest

from lhos.runtimes.multi_agent.durable_state import (
    SCRUB_EVERY_WRITES,
    SchedulerStateCorruption,
    SchedulerStateStore,
    _require_list,
)
from lhos.runtimes.multi_agent.events import SchedulerEventType, record_event
from lhos.runtimes.multi_agent.models import (
    AgentSnapshot,
    AttemptState,
    ComputationCost,
    MatchDecision,
    ScheduledExecutionAttempt,
    TaskClaim,
)
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def _agents() -> dict[str, dict]:
    return {
        "a1": {
            "supported_task_kinds": ("*",),
            "specializations": ("python",),
            "max_concurrency": 2,
        }
    }


def _rewrite_snapshot(db, mutate) -> None:
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT state_json FROM scheduler_snapshot WHERE snapshot_id = 1"
        ).fetchone()
        assert row is not None
        state = json.loads(row[0])
        mutate(state)
        state_json = json.dumps(
            state,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        state_hash = hashlib.sha256(state_json.encode("utf-8")).hexdigest()
        conn.execute(
            "UPDATE scheduler_snapshot SET state_json = ?, state_hash = ? WHERE snapshot_id = 1",
            (state_json, state_hash),
        )


def _durable_write_fingerprint(db):
    """Capture durable metadata/projection rows without the event payload."""

    with sqlite3.connect(db) as conn:
        snapshot = conn.execute(
            "SELECT state_json, state_hash, last_event_seq, last_event_hash, generation "
            "FROM scheduler_snapshot WHERE snapshot_id = 1"
        ).fetchone()
        event_count = conn.execute("SELECT COUNT(*) FROM scheduler_events").fetchone()[0]
        projections = {
            "claims": conn.execute(
                "SELECT claim_id, ordinal, payload_json, payload_hash "
                "FROM scheduler_claims_projection ORDER BY claim_id"
            ).fetchall(),
            "attempts": conn.execute(
                "SELECT attempt_id, ordinal, payload_json, payload_hash "
                "FROM scheduler_attempts_projection ORDER BY attempt_id"
            ).fetchall(),
            "match_log": conn.execute(
                "SELECT ordinal, payload_json, payload_hash "
                "FROM scheduler_match_log_projection ORDER BY ordinal"
            ).fetchall(),
            "idempotency": conn.execute(
                "SELECT idempotency_key FROM scheduler_idempotency_projection "
                "ORDER BY idempotency_key"
            ).fetchall(),
        }
    return snapshot, event_count, projections


def _store_with_two_events(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    store.append_event(
        record_event(SchedulerEventType.EXECUTION_DISPATCHED, graph_id="graph-1"),
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )
    store.append_event(
        record_event(SchedulerEventType.EXECUTION_STARTED, graph_id="graph-1"),
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )
    return db, store, claims, attempts, match_log, keys


def test_scheduler_reopens_with_claim_attempt_event_and_idempotency(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    first = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    result = first.schedule_once(vpg.graph_id)
    assert len(result.dispatched) == 1
    claim_id = result.dispatched[0]["claim_id"]
    first.mark_execution_started(claim_id)

    reopened = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    assert [c.claim_id for c in reopened.claims] == [claim_id]
    assert reopened.attempt_for_claim(claim_id).state.value == "running"
    assert len(reopened.events) == len(first.events)
    replay = reopened.schedule_once(vpg.graph_id)
    assert replay.dispatched == []
    assert any("active claim" in reason or "idempotent" in reason for _, reason in replay.skipped)


def test_scheduler_reopens_agent_snapshot_context_and_stale_state(tmp_path):
    """Agent cognition/context metadata survives a real SQLite close/reopen.

    The snapshot is intentionally populated with both read and write sets so
    this covers the durable projection rather than only the scalar attempt
    state.  A second reopen verifies that the stale-cognition quarantine and
    its audit event are also replayed.
    """

    db = tmp_path / "scheduler-snapshot.sqlite"
    vpg = FakeVPG()
    first = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    result = first.schedule_once(vpg.graph_id)
    claim_id = result.dispatched[0]["claim_id"]
    assert first.mark_execution_started(claim_id) is not None

    assert first.bind_attempt_context_snapshot(
        claim_id,
        snapshot_id="ctx-1",
        manifest_id="manifest-1",
        manifest_hash="A" * 64,
        working_set_hash="B" * 64,
        materialized_hash="C" * 64,
    )
    attempt = first.attempt_for_claim(claim_id)
    assert attempt is not None
    base = AgentSnapshot.from_attempt(
        attempt,
        progress=0.4,
        cost=ComputationCost(input_tokens=120, output_tokens=30, elapsed_ms=900),
        captured_at=attempt.started_at + timedelta(seconds=1),
    )
    snapshot = AgentSnapshot.model_validate(
        {
            **base.model_dump(mode="json"),
            "read_set": [
                {
                    "operation": "read",
                    "resource_uri": "artifact://requirements",
                    "artifact_id": "requirements",
                    "version": 8,
                    "content_hash": "1" * 64,
                }
            ],
            "write_set": [
                {
                    "operation": "write",
                    "resource_uri": "workspace://output.py",
                    "content_hash": "2" * 64,
                }
            ],
        }
    )
    assert first.bind_agent_snapshot(claim_id, snapshot)
    first.close()

    reopened = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    restored = reopened.attempt_for_claim(claim_id)
    assert restored is not None
    assert restored.context_snapshot_id == "ctx-1"
    assert restored.context_manifest_id == "manifest-1"
    assert restored.context_manifest_hash == "a" * 64
    assert restored.context_working_set_hash == "b" * 64
    assert restored.context_materialized_hash == "c" * 64
    assert restored.agent_snapshot is not None
    assert restored.agent_snapshot.fingerprint() == snapshot.fingerprint()
    assert restored.agent_snapshot.context_identity is not None
    assert restored.agent_snapshot.context_identity.snapshot_id == "ctx-1"
    assert [item.resource_uri for item in restored.agent_snapshot.read_set] == [
        "artifact://requirements"
    ]
    assert [item.resource_uri for item in restored.agent_snapshot.write_set] == [
        "workspace://output.py"
    ]

    assert reopened.mark_stale_cognition(claim_id, "requirements changed")
    assert reopened.release_task(
        vpg.graph_id,
        "t1",
        reason="stale_cognition:requirements changed",
        expected_claim_id=claim_id,
    )
    reopened.close()

    final = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    final_attempt = final.attempt_for_claim(claim_id)
    assert final_attempt is not None
    assert final_attempt.state == AttemptState.STALE_COGNITION
    assert final_attempt.agent_snapshot is not None
    assert final_attempt.agent_snapshot.fingerprint() == snapshot.fingerprint()
    assert any(
        event.event_type.value == "execution_stale_cognition"
        and event.attempt_id == final_attempt.attempt_id
        for event in final.events
    )
    final.close()


def test_scheduler_event_chain_tampering_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE scheduler_events SET event_json = ? WHERE event_seq = 1",
        ('{"event_id":"tampered"}',),
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchedulerStateCorruption, match="hash mismatch"):
        fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))


def test_scheduler_snapshot_tampering_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE scheduler_snapshot SET state_json = ? WHERE snapshot_id = 1",
        ('{"claims":[],"attempts":[],"match_log":[],"idempotent_keys":[]}',),
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchedulerStateCorruption, match="snapshot hash"):
        fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))


def test_scheduler_event_timestamp_column_tampering_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE scheduler_events SET created_at = ? WHERE event_seq = 1",
            ("1970-01-01T00:00:00+00:00",),
        )

    with pytest.raises(SchedulerStateCorruption, match="timestamp mismatch"):
        fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))


def test_early_event_corruption_is_detected_by_scrub_and_reload(tmp_path) -> None:
    """Externally tampered history is caught by ``scrub()`` and by ``load()``.

    Writes used to re-verify the whole journal, so a corrupted early row was
    rejected by the *next* write. That made one write O(events) and a run of N
    writes O(N^2) -- measured 1.37ms per append at 100 events rising to 5.84ms at
    800. A write now verifies only the run appended since this instance last
    checked, and the full check runs on ``load()``, on ``scrub()``, on any write
    failure, and every ``SCRUB_EVERY_WRITES`` writes.

    The traded guarantee is therefore *relocated, not removed*, and this test
    pins its new location. Corruption reachable through the API is still caught
    immediately: a concurrent writer bumps the snapshot generation and loses the
    compare-and-swap. What moved is corruption that bypasses the API entirely.
    """

    db, store, _claims, _attempts, _match_log, _keys = _store_with_two_events(tmp_path)
    try:
        with sqlite3.connect(db) as conn:
            conn.execute(
                "UPDATE scheduler_events SET event_json = ? WHERE event_seq = 1",
                ('{"event_id":"tampered-middle-event"}',),
            )

        with pytest.raises(SchedulerStateCorruption, match="event hash mismatch"):
            store.scrub()

        with pytest.raises(SchedulerStateCorruption, match="event hash mismatch"):
            store.load()
    finally:
        store.close()

    # A fresh store must also refuse to open the tampered journal.
    reopened = SchedulerStateStore(db)
    try:
        with pytest.raises(SchedulerStateCorruption, match="event hash mismatch"):
            reopened.load()
    finally:
        reopened.close()


def test_a_periodic_scrub_still_re_verifies_the_whole_journal(tmp_path) -> None:
    """The write path must not defer the full check indefinitely."""

    assert SCRUB_EVERY_WRITES > 0
    db, store, claims, attempts, match_log, keys = _store_with_two_events(tmp_path)
    try:
        with sqlite3.connect(db) as conn:
            conn.execute(
                "UPDATE scheduler_events SET event_json = ? WHERE event_seq = 1",
                ('{"event_id":"tampered-middle-event"}',),
            )

        # Writes verify only the appended run, so the tampered prefix is not
        # revisited until the cadence forces a full check.
        with pytest.raises(SchedulerStateCorruption, match="event hash mismatch"):
            for index in range(SCRUB_EVERY_WRITES + 2):
                store.persist_state(
                    claims=claims,
                    attempts=attempts,
                    match_log=match_log,
                    idempotent_keys=keys | {f"scrub-probe-{index}"},
                )
    finally:
        store.close()


def test_scheduler_snapshot_duplicate_claim_id_fails_closed_with_valid_hash(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    def duplicate_claim(state):
        state["claims"].append(dict(state["claims"][0]))

    _rewrite_snapshot(db, duplicate_claim)

    with pytest.raises(SchedulerStateCorruption, match=r"invalid.*snapshot payload"):
        fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))


def test_scheduler_snapshot_orphan_attempt_fails_closed_with_valid_hash(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    def orphan_attempt(state):
        state["attempts"][0]["claim_id"] = "missing-claim"

    _rewrite_snapshot(db, orphan_attempt)

    with pytest.raises(SchedulerStateCorruption, match=r"invalid.*snapshot payload"):
        fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))


def test_scheduler_snapshot_allows_multiple_historical_attempts_for_one_claim(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    def add_older_attempt(state):
        historical = dict(state["attempts"][0])
        historical["attempt_id"] = "historical-attempt"
        historical["state"] = "failed"
        state["attempts"].insert(0, historical)

    _rewrite_snapshot(db, add_older_attempt)
    reopened = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))

    assert [attempt.attempt_id for attempt in reopened.attempts] == [
        "historical-attempt",
        "attempt-graph-1-t1-0",
    ]


def test_scheduler_snapshot_non_string_idempotency_key_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)

    def corrupt_key(state):
        state["idempotent_keys"].append({"not": "a string"})

    _rewrite_snapshot(db, corrupt_key)

    with pytest.raises(SchedulerStateCorruption, match=r"invalid.*snapshot payload"):
        fake_scheduler(_agents(), fake_vpg=vpg, state_path=str(db))


def test_scheduler_store_rolls_back_event_and_snapshot_on_failure(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_store=store)
    vpg.add_ready_task("t1", required_specializations=("python",))

    original = store.append_event

    def fail_once(*args, **kwargs):
        raise RuntimeError("disk full")

    store.append_event = fail_once  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="disk full"):
        scheduler.schedule_once(vpg.graph_id)
    store.append_event = original  # type: ignore[method-assign]

    assert scheduler.claims == []
    assert scheduler.events == []
    assert store.event_count() == 0
    with pytest.raises(SchedulerStateCorruption, match="hash mismatch"):
        # No snapshot is expected for a failed first transaction; force a
        # malformed event row to ensure loading remains fail-closed.
        with sqlite3.connect(db) as conn:
            conn.execute(
                "INSERT INTO scheduler_events "
                "(event_seq,event_id,event_json,event_hash,created_at) "
                "VALUES (1,'orphan','{}','bad','now')"
            )
        store.load()


def _large_projection(size: int = 24):
    claims = [
        TaskClaim(
            claim_id=f"claim-{index}",
            graph_id="graph-1",
            graph_version=1,
            task_id=f"task-{index}",
            agent_id="a1",
            process_id="p1",
            lease_resource=f"claim://graph-1/task-{index}",
        )
        for index in range(size)
    ]
    attempts = [
        ScheduledExecutionAttempt(
            attempt_id=f"attempt-{index}",
            graph_id=claim.graph_id,
            graph_version=claim.graph_version,
            task_id=claim.task_id,
            claim_id=claim.claim_id,
            agent_id=claim.agent_id,
            process_id=claim.process_id,
            attempt_number=claim.attempt_number,
        )
        for index, claim in enumerate(claims)
    ]
    match_log = [
        MatchDecision(
            graph_id=claim.graph_id,
            graph_version=claim.graph_version,
            task_id=claim.task_id,
            selected_agent_id=claim.agent_id,
            decision_hash=f"decision-{index}",
        )
        for index, claim in enumerate(claims)
    ]
    keys = {f"graph-1:task-{index}:v1" for index in range(size)}
    return claims, attempts, match_log, keys


def test_large_scheduler_projection_reopens_from_normalized_rows(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    event = record_event(SchedulerEventType.EXECUTION_DISPATCHED, graph_id="graph-1")
    store.append_event(
        event,
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )

    with sqlite3.connect(db) as conn:
        manifest = json.loads(
            conn.execute(
                "SELECT state_json FROM scheduler_snapshot WHERE snapshot_id = 1"
            ).fetchone()[0]
        )
        assert manifest["encoding"] == "normalized-v1"
        for field in ("claims", "attempts", "match_log", "idempotent_keys"):
            with pytest.raises(ValueError, match=field):
                # This is the collection decoder used by the previous inline
                # reader. The normalized manifest must make it fail closed.
                _require_list(manifest, field)
        assert conn.execute("SELECT COUNT(*) FROM scheduler_claims_projection").fetchone()[0] == 24
        assert (
            conn.execute("SELECT COUNT(*) FROM scheduler_attempts_projection").fetchone()[0] == 24
        )

    restored = store.load()
    assert [claim.claim_id for claim in restored.claims] == [claim.claim_id for claim in claims]
    assert [attempt.attempt_id for attempt in restored.attempts] == [
        attempt.attempt_id for attempt in attempts
    ]
    assert [decision.task_id for decision in restored.match_log] == [
        decision.task_id for decision in match_log
    ]
    assert restored.idempotent_keys == keys
    assert restored.events == [event]


def test_normalized_projection_updates_only_changed_entity_rows(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    store.append_event(
        record_event(SchedulerEventType.EXECUTION_DISPATCHED, graph_id="graph-1"),
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )

    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE projection_write_audit (
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL
            );
            CREATE TRIGGER audit_claim_insert
            AFTER INSERT ON scheduler_claims_projection
            BEGIN
                INSERT INTO projection_write_audit VALUES ('claims', 'insert');
            END;
            CREATE TRIGGER audit_claim_update
            AFTER UPDATE ON scheduler_claims_projection
            BEGIN
                INSERT INTO projection_write_audit VALUES ('claims', 'update');
            END;
            CREATE TRIGGER audit_attempt_insert
            AFTER INSERT ON scheduler_attempts_projection
            BEGIN
                INSERT INTO projection_write_audit VALUES ('attempts', 'insert');
            END;
            CREATE TRIGGER audit_attempt_update
            AFTER UPDATE ON scheduler_attempts_projection
            BEGIN
                INSERT INTO projection_write_audit VALUES ('attempts', 'update');
            END;
            CREATE TRIGGER audit_match_insert
            AFTER INSERT ON scheduler_match_log_projection
            BEGIN
                INSERT INTO projection_write_audit VALUES ('matches', 'insert');
            END;
            CREATE TRIGGER audit_match_update
            AFTER UPDATE ON scheduler_match_log_projection
            BEGIN
                INSERT INTO projection_write_audit VALUES ('matches', 'update');
            END;
            CREATE TRIGGER audit_key_insert
            AFTER INSERT ON scheduler_idempotency_projection
            BEGIN
                INSERT INTO projection_write_audit VALUES ('keys', 'insert');
            END;
            """
        )

    attempts[7].state = AttemptState.RUNNING
    store.append_event(
        record_event(SchedulerEventType.EXECUTION_STARTED, graph_id="graph-1"),
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )

    with sqlite3.connect(db) as conn:
        writes = conn.execute(
            "SELECT table_name, operation, COUNT(*) "
            "FROM projection_write_audit GROUP BY table_name, operation"
        ).fetchall()
    assert writes == [("attempts", "update", 1)]


def test_normalized_projection_row_tampering_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    store.append_event(
        record_event(SchedulerEventType.EXECUTION_DISPATCHED, graph_id="graph-1"),
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT payload_json FROM scheduler_attempts_projection WHERE attempt_id = 'attempt-0'"
        ).fetchone()
        payload = json.loads(row[0])
        payload["state"] = "running"
        conn.execute(
            "UPDATE scheduler_attempts_projection SET payload_json = ? "
            "WHERE attempt_id = 'attempt-0'",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")),),
        )

    with pytest.raises(SchedulerStateCorruption, match="attempt payload hash mismatch"):
        store.load()


@pytest.mark.parametrize("write_kind", ["persist_state", "append_event"])
def test_tampered_normalized_rows_cannot_be_repaired_by_writer(tmp_path, write_kind):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    store.persist_state(
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )
    original_event_count = store.event_count()

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT payload_json FROM scheduler_attempts_projection WHERE attempt_id = 'attempt-0'"
        ).fetchone()
        assert row is not None
        payload = json.loads(row[0])
        payload["state"] = "running"
        conn.execute(
            "UPDATE scheduler_attempts_projection SET payload_json = ? "
            "WHERE attempt_id = 'attempt-0'",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")),),
        )

    with pytest.raises(SchedulerStateCorruption, match="attempt payload hash mismatch"):
        if write_kind == "append_event":
            store.append_event(
                record_event(
                    SchedulerEventType.EXECUTION_STARTED,
                    graph_id="graph-1",
                ),
                claims=claims,
                attempts=attempts,
                match_log=match_log,
                idempotent_keys=keys,
            )
        else:
            store.persist_state(
                claims=claims,
                attempts=attempts,
                match_log=match_log,
                idempotent_keys=keys,
            )

    # The failed write must not append an event or overwrite the tampered row.
    assert store.event_count() == original_event_count
    with sqlite3.connect(db) as conn:
        stored_payload = conn.execute(
            "SELECT payload_json FROM scheduler_attempts_projection WHERE attempt_id = 'attempt-0'"
        ).fetchone()[0]
    assert json.loads(stored_payload)["state"] == "running"
    store.close()


@pytest.mark.parametrize(
    "field, value",
    [
        ("match_log", [{"malformed": True}]),
        ("idempotent_keys", [{"malformed": True}]),
    ],
)
def test_hash_rewritten_inline_snapshot_cannot_be_repaired_by_writer(
    tmp_path,
    field,
    value,
):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    vpg = FakeVPG()
    scheduler = fake_scheduler(_agents(), fake_vpg=vpg, state_store=store)
    vpg.add_ready_task("t1", required_specializations=("python",))
    scheduler.schedule_once(vpg.graph_id)
    original_event_count = store.event_count()
    baseline = store.load()

    with sqlite3.connect(db) as conn:
        state = json.loads(
            conn.execute(
                "SELECT state_json FROM scheduler_snapshot WHERE snapshot_id = 1"
            ).fetchone()[0]
        )
        state[field] = value
        state_json = json.dumps(
            state,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        conn.execute(
            "UPDATE scheduler_snapshot SET state_json = ?, state_hash = ? WHERE snapshot_id = 1",
            (state_json, hashlib.sha256(state_json.encode("utf-8")).hexdigest()),
        )

    with pytest.raises(
        SchedulerStateCorruption,
        match="invalid scheduler projection snapshot payload before write",
    ):
        store.persist_state(
            claims=baseline.claims,
            attempts=baseline.attempts,
            match_log=baseline.match_log,
            idempotent_keys=baseline.idempotent_keys,
        )
    assert store.event_count() == original_event_count
    store.close()


def test_missing_snapshot_with_normalized_rows_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    store.persist_state(
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )

    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM scheduler_snapshot WHERE snapshot_id = 1")

    with pytest.raises(
        SchedulerStateCorruption,
        match="normalized projection rows exist but snapshot is missing",
    ):
        store.load()
    store.close()


def test_inline_snapshot_with_residual_normalized_rows_fails_closed(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    store = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    store.persist_state(
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )

    # Replace the normalized manifest with a valid-looking inline snapshot
    # while leaving the normalized tables populated.  A loader must reject
    # this hybrid state instead of returning only the forged inline payload.
    inline = {
        "claims": [],
        "attempts": [],
        "match_log": [],
        "idempotent_keys": [],
    }
    state_json = json.dumps(
        inline,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    state_hash = hashlib.sha256(state_json.encode("utf-8")).hexdigest()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE scheduler_snapshot SET state_json = ?, state_hash = ? WHERE snapshot_id = 1",
            (state_json, state_hash),
        )

    with pytest.raises(
        SchedulerStateCorruption,
        match="inline snapshot has residual normalized projection rows",
    ):
        store.load()
    store.close()


@pytest.mark.parametrize("write_kind", ["persist_state", "append_event"])
def test_stale_store_cannot_overwrite_newer_projection(tmp_path, write_kind):
    db = tmp_path / "scheduler.sqlite"
    initial = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    initial.persist_state(
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )

    stale = SchedulerStateStore(db)
    stale_state = stale.load()
    current = SchedulerStateStore(db)
    current_state = current.load()
    current_state.attempts[0].state = AttemptState.RUNNING
    current.persist_state(
        claims=current_state.claims,
        attempts=current_state.attempts,
        match_log=current_state.match_log,
        idempotent_keys=current_state.idempotent_keys,
    )

    with pytest.raises(SchedulerStateCorruption, match="stale scheduler writer"):
        if write_kind == "append_event":
            stale.append_event(
                record_event(
                    SchedulerEventType.EXECUTION_FAILED,
                    graph_id="graph-1",
                    reason="stale writer",
                ),
                claims=stale_state.claims,
                attempts=stale_state.attempts,
                match_log=stale_state.match_log,
                idempotent_keys=stale_state.idempotent_keys,
            )
        else:
            stale.persist_state(
                claims=stale_state.claims,
                attempts=stale_state.attempts,
                match_log=stale_state.match_log,
                idempotent_keys=stale_state.idempotent_keys,
            )

    latest = SchedulerStateStore(db)
    restored = latest.load()
    assert restored.attempts[0].state == AttemptState.RUNNING
    assert not any(event.reason == "stale writer" for event in restored.events)
    initial.close()
    stale.close()
    current.close()
    latest.close()


def test_load_reads_event_manifest_and_normalized_rows_from_one_snapshot(tmp_path):
    db = tmp_path / "scheduler.sqlite"
    initial = SchedulerStateStore(db)
    claims, attempts, match_log, keys = _large_projection()
    initial.persist_state(
        claims=claims,
        attempts=attempts,
        match_log=match_log,
        idempotent_keys=keys,
    )

    reader = SchedulerStateStore(db)
    writer = SchedulerStateStore(db)
    writer_state = writer.load()
    original_load_projection = reader._load_normalized_projection
    interleaved = False

    def load_projection_after_concurrent_commit(manifest):
        nonlocal interleaved
        if not interleaved:
            interleaved = True
            writer_state.attempts[0].state = AttemptState.RUNNING
            writer.persist_state(
                claims=writer_state.claims,
                attempts=writer_state.attempts,
                match_log=writer_state.match_log,
                idempotent_keys=writer_state.idempotent_keys,
            )
        return original_load_projection(manifest)

    reader._load_normalized_projection = load_projection_after_concurrent_commit  # type: ignore[method-assign]
    observed = reader.load()

    # The writer commits between the reader's manifest SELECT and projection
    # SELECTs. A single read transaction keeps the reader on the older,
    # internally consistent generation rather than mixing both generations.
    assert interleaved is True
    assert observed.attempts[0].state == AttemptState.DISPATCHED
    latest = SchedulerStateStore(db)
    assert latest.load().attempts[0].state == AttemptState.RUNNING

    with pytest.raises(SchedulerStateCorruption, match="stale scheduler writer"):
        reader.persist_state(
            claims=observed.claims,
            attempts=observed.attempts,
            match_log=observed.match_log,
            idempotent_keys=observed.idempotent_keys,
        )
    initial.close()
    reader.close()
    writer.close()
    latest.close()
