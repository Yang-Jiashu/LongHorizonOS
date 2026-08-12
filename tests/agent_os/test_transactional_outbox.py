"""Transactional-outbox consistency, crash-recovery, and fencing tests."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from lhos.agent_os.services.outbox import (
    OutboxIdempotencyConflict,
    OutboxIntegrityError,
    OutboxIntent,
    OutboxStatus,
    TransactionalOutbox,
)
from lhos.agent_os.storage.sqlite import SQLiteStorage

NOW = datetime(2026, 8, 12, 8, 0, tzinfo=UTC)


def _intent(
    *,
    key: str = "action:a1:dispatch",
    payload: Mapping[str, Any] | None = None,
    available_at: datetime | None = None,
    outbox_id: str | None = None,
) -> OutboxIntent:
    return OutboxIntent(
        destination="driver://tool/http",
        payload=payload or {"action_id": "a1", "operation": "post"},
        headers={"content-type": "application/json"},
        idempotency_key=key,
        aggregate_type="action",
        aggregate_id="a1",
        causation_id="claim:c1",
        correlation_id="graph:g1",
        available_at=available_at,
        outbox_id=outbox_id,
    )


def _state_value(storage: SQLiteStorage, key: str = "a1") -> str | None:
    row = storage.query_one("SELECT value FROM test_internal_state WHERE key = ?", (key,))
    return str(row["value"]) if row is not None else None


class RecordingPublisher:
    def __init__(self, *, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[dict[str, Any]] = []

    async def publish(
        self,
        *,
        destination: str,
        payload: Mapping[str, Any],
        idempotency_key: str,
        headers: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "destination": destination,
                "payload": dict(payload),
                "idempotency_key": idempotency_key,
                "headers": dict(headers),
            }
        )
        if len(self.calls) <= self.failures:
            raise OSError("broker unavailable")
        return {"receipt": f"r-{len(self.calls)}"}


@pytest.fixture
def storage() -> Generator[SQLiteStorage, None, None]:
    value = SQLiteStorage(":memory:")
    value.execute(
        """
        CREATE TABLE test_internal_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    try:
        yield value
    finally:
        value.close()


def test_internal_mutation_and_intent_commit_atomically(storage: SQLiteStorage) -> None:
    outbox = TransactionalOutbox(storage)

    result = outbox.commit_intent(
        _intent(),
        lambda tx: tx.execute(
            "INSERT INTO test_internal_state(key, value) VALUES (?, ?)",
            ("a1", "ready"),
        ),
        transaction_id="commit-1",
        now=NOW,
    )

    assert result.applied is True
    assert _state_value(storage) == "ready"
    assert result.record.status == OutboxStatus.PENDING
    assert result.record.transaction_id == "commit-1"


def test_outbox_insert_failure_rolls_back_internal_mutation(storage: SQLiteStorage) -> None:
    outbox = TransactionalOutbox(storage)

    outbox.enqueue(
        _intent(
            key="existing-key",
            payload={"action_id": "existing"},
            outbox_id="existing",
        ),
        now=NOW,
    )
    conflicting = _intent(
        key="different-key",
        payload={"action_id": "different"},
        outbox_id="existing",
    )
    with pytest.raises(sqlite3.IntegrityError):
        outbox.commit_intent(
            conflicting,
            lambda tx: tx.execute(
                "INSERT INTO test_internal_state(key, value) VALUES (?, ?)",
                ("a1", "must-roll-back"),
            ),
            now=NOW,
        )

    assert _state_value(storage) is None


def test_mutation_failure_does_not_leave_an_outbox_row(storage: SQLiteStorage) -> None:
    outbox = TransactionalOutbox(storage)

    def failing_mutation(tx: Any) -> None:
        tx.execute(
            "INSERT INTO test_internal_state(key, value) VALUES (?, ?)",
            ("a1", "temporary"),
        )
        raise RuntimeError("mutation failed")

    with pytest.raises(RuntimeError, match="mutation failed"):
        outbox.commit_intent(_intent(), failing_mutation, now=NOW)

    assert _state_value(storage) is None
    assert outbox.list_records() == []


def test_same_commit_intent_is_idempotent_for_internal_state(storage: SQLiteStorage) -> None:
    outbox = TransactionalOutbox(storage)
    mutation_calls = 0

    def mutate(tx: Any) -> None:
        nonlocal mutation_calls
        mutation_calls += 1
        tx.execute(
            "INSERT INTO test_internal_state(key, value) VALUES (?, ?)",
            ("a1", "once"),
        )

    first = outbox.commit_intent(_intent(), mutate, now=NOW)
    second = outbox.commit_intent(_intent(), mutate, now=NOW + timedelta(seconds=1))

    assert first.applied is True
    assert second.applied is False
    assert second.record.outbox_id == first.record.outbox_id
    assert mutation_calls == 1
    assert _state_value(storage) == "once"


def test_idempotency_key_reuse_with_different_payload_fails_closed(
    storage: SQLiteStorage,
) -> None:
    outbox = TransactionalOutbox(storage)
    outbox.enqueue(_intent(), now=NOW)

    with pytest.raises(OutboxIdempotencyConflict):
        outbox.enqueue(
            _intent(payload={"action_id": "a1", "operation": "delete"}),
            now=NOW,
        )


@pytest.mark.asyncio
async def test_dispatch_passes_stable_idempotency_key_and_acks(
    storage: SQLiteStorage,
) -> None:
    outbox = TransactionalOutbox(storage)
    record = outbox.enqueue(_intent(), now=NOW)
    publisher = RecordingPublisher()

    result = await outbox.dispatch_once(
        publisher,
        worker_id="worker-1",
        now=NOW,
    )

    assert result.delivered is True
    assert publisher.calls == [
        {
            "destination": "driver://tool/http",
            "payload": {"action_id": "a1", "operation": "post"},
            "idempotency_key": "action:a1:dispatch",
            "headers": {"content-type": "application/json"},
        }
    ]
    delivered = outbox.get(record.outbox_id)
    assert delivered is not None
    assert delivered.status == OutboxStatus.DELIVERED
    assert delivered.attempts == 1
    assert delivered.delivery_result == {"receipt": "r-1"}


@pytest.mark.asyncio
async def test_retryable_failure_survives_and_is_redelivered(
    storage: SQLiteStorage,
) -> None:
    outbox = TransactionalOutbox(storage)
    record = outbox.enqueue(_intent(), now=NOW)
    publisher = RecordingPublisher(failures=1)

    failed = await outbox.dispatch_once(
        publisher,
        worker_id="worker-1",
        now=NOW,
        retry_delay=timedelta(seconds=5),
    )
    assert failed.delivered is False
    pending = outbox.get(record.outbox_id)
    assert pending is not None
    assert pending.status == OutboxStatus.PENDING
    assert pending.attempts == 1
    assert pending.last_error == {
        "message": "broker unavailable",
        "type": "OSError",
    }

    too_early = await outbox.dispatch_once(
        publisher,
        worker_id="worker-2",
        now=NOW + timedelta(seconds=4),
    )
    assert too_early.record is None

    delivered = await outbox.dispatch_once(
        publisher,
        worker_id="worker-2",
        now=NOW + timedelta(seconds=5),
    )
    assert delivered.delivered is True
    assert [call["idempotency_key"] for call in publisher.calls] == [
        "action:a1:dispatch",
        "action:a1:dispatch",
    ]
    final = outbox.get(record.outbox_id)
    assert final is not None
    assert final.status == OutboxStatus.DELIVERED
    assert final.attempts == 2


def test_expired_claim_replays_after_process_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    first_storage = SQLiteStorage(db_path)
    first = TransactionalOutbox(first_storage)
    record = first.enqueue(_intent(), now=NOW)
    abandoned = first.claim_next(
        "crashed-worker",
        now=NOW,
        claim_ttl=timedelta(seconds=10),
    )
    assert abandoned is not None
    assert abandoned.claim_token is not None
    stale_token = abandoned.claim_token
    first_storage.close()

    restarted_storage = SQLiteStorage(db_path)
    try:
        restarted = TransactionalOutbox(restarted_storage)
        assert (
            restarted.claim_next(
                "new-worker",
                now=NOW + timedelta(seconds=9),
            )
            is None
        )
        reclaimed = restarted.claim_next(
            "new-worker",
            now=NOW + timedelta(seconds=10),
        )
        assert reclaimed is not None
        assert reclaimed.outbox_id == record.outbox_id
        assert reclaimed.attempts == 2
        assert reclaimed.claim_token != stale_token

        assert (
            restarted.ack(record.outbox_id, stale_token, now=NOW + timedelta(seconds=10)) is False
        )
        assert reclaimed.claim_token is not None
        assert (
            restarted.ack(
                record.outbox_id,
                reclaimed.claim_token,
                result={"ok": True},
                now=NOW + timedelta(seconds=10),
            )
            is True
        )
    finally:
        restarted_storage.close()


def test_payload_tampering_fails_closed(storage: SQLiteStorage) -> None:
    outbox = TransactionalOutbox(storage)
    record = outbox.enqueue(_intent(), now=NOW)
    storage.execute(
        "UPDATE transactional_outbox SET payload_json = ? WHERE outbox_id = ?",
        ('{"action_id":"attacker"}', record.outbox_id),
    )

    with pytest.raises(OutboxIntegrityError, match="payload hash mismatch"):
        outbox.get(record.outbox_id)


@pytest.mark.asyncio
async def test_max_attempts_moves_record_to_failed_and_requires_explicit_retry(
    storage: SQLiteStorage,
) -> None:
    outbox = TransactionalOutbox(storage)
    record = outbox.enqueue(_intent(), now=NOW)
    publisher = RecordingPublisher(failures=3)

    first = await outbox.dispatch_once(
        publisher,
        worker_id="worker",
        now=NOW,
        retry_delay=timedelta(0),
        max_attempts=2,
    )
    second = await outbox.dispatch_once(
        publisher,
        worker_id="worker",
        now=NOW,
        retry_delay=timedelta(0),
        max_attempts=2,
    )

    assert first.delivered is False
    assert second.delivered is False
    failed = outbox.get(record.outbox_id)
    assert failed is not None
    assert failed.status == OutboxStatus.FAILED
    assert failed.attempts == 2
    assert (
        await outbox.dispatch_once(
            publisher,
            worker_id="worker",
            now=NOW,
        )
    ).record is None

    assert outbox.retry_failed(record.outbox_id, available_at=NOW) is True
    retryable = outbox.get(record.outbox_id)
    assert retryable is not None
    assert retryable.status == OutboxStatus.PENDING


def test_enqueue_tx_composes_with_journal_and_projection_in_one_transaction(
    storage: SQLiteStorage,
) -> None:
    """The primitive composes with existing tx-aware AgentOS services."""
    from lhos.agent_os.kernel.models import (
        ActionControlBlock,
        ActionState,
        KernelEvent,
    )
    from lhos.agent_os.services.action_service import ActionService
    from lhos.agent_os.services.journal import JournalService

    journal = JournalService(storage)
    actions = ActionService(storage, journal)
    outbox = TransactionalOutbox(storage)
    acb = ActionControlBlock(
        action_id="a-cross-plane",
        pid="p1",
        device_type="tool/http",
        operation="post",
        state=ActionState.RUNNING,
        idempotency_key="external:a-cross-plane",
    )
    event = KernelEvent(
        event_id="event-a-cross-plane-running",
        pid="p1",
        event_type="ACTION_RUNNING",
        payload={"action_id": acb.action_id},
        created_at=NOW,
    )

    with storage.transaction(immediate=True) as tx:
        journal.append_events_tx(tx, [event])
        actions._upsert_projection_tx(tx, acb)
        outbox.enqueue_tx(
            tx,
            OutboxIntent(
                destination="driver://tool/http",
                payload={
                    "action_id": acb.action_id,
                    "operation": acb.operation,
                    "arguments": acb.arguments,
                },
                idempotency_key=acb.idempotency_key or acb.action_id,
                aggregate_type="action",
                aggregate_id=acb.action_id,
                causation_id=event.event_id,
            ),
            transaction_id="action-running-and-dispatch-intent",
            now=NOW,
        )

    assert actions.get_action(acb.action_id) is not None
    assert journal.read_all()[0].event_id == event.event_id
    assert outbox.get_by_idempotency_key("external:a-cross-plane") is not None
