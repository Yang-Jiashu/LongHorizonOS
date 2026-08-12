"""Durable transactional outbox for cross-plane commit intents.

This module closes one precise consistency gap:

* a mutation to AgentOS-owned SQLite state and its external-delivery intent can
  be committed in one SQLite transaction;
* committed intents survive process crashes and can be claimed again after a
  worker lease expires;
* every delivery carries a stable idempotency key.

It deliberately does *not* claim that SQLite and an arbitrary external system
commit atomically.  Delivery is at-least-once.  A crash after the external
effect but before :meth:`TransactionalOutbox.ack` causes redelivery, so the
publisher/destination must enforce the supplied idempotency key.
"""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol, TypeVar
from uuid import uuid4

from lhos.agent_os.storage.sqlite import SQLiteStorage

T = TypeVar("T")


class OutboxError(RuntimeError):
    """Base class for transactional-outbox failures."""


class OutboxIdempotencyConflict(OutboxError):
    """The same idempotency key was reused for a different external intent."""


class OutboxIntegrityError(OutboxError):
    """Durable outbox data failed its canonical payload integrity check."""


class OutboxStatus(StrEnum):
    """Durable delivery state.

    ``FAILED`` is a terminal/dead-letter state.  Retryable failures return the
    row to ``PENDING`` with a later ``available_at`` timestamp.
    """

    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    DELIVERED = "delivered"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class OutboxIntent:
    """An externally observable operation that must be delivered durably."""

    destination: str
    payload: Mapping[str, Any]
    idempotency_key: str
    headers: Mapping[str, Any] = field(default_factory=dict)
    aggregate_type: str | None = None
    aggregate_id: str | None = None
    causation_id: str | None = None
    correlation_id: str | None = None
    outbox_id: str | None = None
    available_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.destination.strip():
            raise ValueError("destination must be non-empty")
        if not self.idempotency_key.strip():
            raise ValueError("idempotency_key must be non-empty")


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    """Materialized durable outbox row."""

    outbox_id: str
    idempotency_key: str
    destination: str
    payload: dict[str, Any]
    payload_hash: str
    headers: dict[str, Any]
    aggregate_type: str | None
    aggregate_id: str | None
    causation_id: str | None
    correlation_id: str | None
    transaction_id: str
    status: OutboxStatus
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: str | None
    claimed_at: datetime | None
    claim_expires_at: datetime | None
    last_error: dict[str, Any] | None
    delivery_result: Any | None
    created_at: datetime
    delivered_at: datetime | None


@dataclass(frozen=True, slots=True)
class CommitIntentResult:
    """Result of an idempotent internal-mutation + outbox commit."""

    record: OutboxRecord
    applied: bool


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """One dispatcher iteration result."""

    record: OutboxRecord | None
    delivered: bool
    error: str | None = None


class OutboxPublisher(Protocol):
    """External adapter contract.

    Implementations must forward/enforce ``idempotency_key`` at the destination
    (for example an HTTP Idempotency-Key header or a broker deduplication key).
    """

    def publish(
        self,
        *,
        destination: str,
        payload: Mapping[str, Any],
        idempotency_key: str,
        headers: Mapping[str, Any],
    ) -> Any | Awaitable[Any]: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _normalize_time(value: datetime | None, *, default: datetime | None = None) -> datetime:
    result = value or default or _utc_now()
    if result.tzinfo is None:
        raise ValueError("outbox timestamps must be timezone-aware")
    return result.astimezone(UTC)


def _payload_bytes(payload_json: str) -> bytes:
    return payload_json.encode("utf-8")


def _payload_hash(payload_json: str) -> str:
    return hashlib.sha256(_payload_bytes(payload_json)).hexdigest()


class TransactionalOutbox:
    """SQLite-backed commit-intent store and at-least-once dispatcher."""

    def __init__(self, storage: SQLiteStorage):
        self._storage = storage

    def commit_intent(
        self,
        intent: OutboxIntent,
        mutate: Callable[[Any], T],
        *,
        transaction_id: str | None = None,
        now: datetime | None = None,
    ) -> CommitIntentResult:
        """Atomically apply one internal mutation and persist one outbox row.

        The intent's idempotency key is also the idempotency boundary for the
        internal mutation.  If the exact intent was committed previously,
        ``mutate`` is not called again and ``applied`` is false.  Reusing the
        key with different externally observable data fails closed.

        ``mutate`` must use the supplied transaction object directly.  Calling
        another service that opens its own SQLite transaction would be a nested
        transaction and is intentionally outside this API's contract.
        """

        commit_time = _normalize_time(now)
        tx_id = transaction_id or uuid4().hex
        with self._storage.transaction(immediate=True) as tx:
            existing = self._get_by_idempotency_key_tx(tx, intent.idempotency_key)
            if existing is not None:
                self._assert_same_intent(existing, intent)
                return CommitIntentResult(record=existing, applied=False)

            mutate(tx)
            record = self.enqueue_tx(
                tx,
                intent,
                transaction_id=tx_id,
                now=commit_time,
            )
            return CommitIntentResult(record=record, applied=True)

    def enqueue(
        self,
        intent: OutboxIntent,
        *,
        transaction_id: str | None = None,
        now: datetime | None = None,
    ) -> OutboxRecord:
        """Persist an intent in its own transaction.

        Use :meth:`commit_intent` or :meth:`enqueue_tx` when an internal state
        mutation must share the same commit boundary.
        """

        commit_time = _normalize_time(now)
        with self._storage.transaction(immediate=True) as tx:
            return self.enqueue_tx(
                tx,
                intent,
                transaction_id=transaction_id or uuid4().hex,
                now=commit_time,
            )

    def enqueue_tx(
        self,
        tx: Any,
        intent: OutboxIntent,
        *,
        transaction_id: str,
        now: datetime | None = None,
    ) -> OutboxRecord:
        """Persist an intent through an already-open SQLite transaction."""

        commit_time = _normalize_time(now)
        available_at = _normalize_time(intent.available_at, default=commit_time)
        payload_json = SQLiteStorage.dumps(dict(intent.payload))
        headers_json = SQLiteStorage.dumps(dict(intent.headers))
        digest = _payload_hash(payload_json)

        existing = self._get_by_idempotency_key_tx(tx, intent.idempotency_key)
        if existing is not None:
            self._assert_same_intent(existing, intent)
            return existing

        outbox_id = intent.outbox_id or uuid4().hex
        tx.execute(
            """
            INSERT INTO transactional_outbox (
                outbox_id, idempotency_key, destination, payload_json,
                payload_hash, headers_json, aggregate_type, aggregate_id,
                causation_id, correlation_id, transaction_id, status,
                attempts, available_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)
            """,
            (
                outbox_id,
                intent.idempotency_key,
                intent.destination,
                payload_json,
                digest,
                headers_json,
                intent.aggregate_type,
                intent.aggregate_id,
                intent.causation_id,
                intent.correlation_id,
                transaction_id,
                available_at.isoformat(),
                commit_time.isoformat(),
            ),
        )
        row = tx.query_one(
            "SELECT * FROM transactional_outbox WHERE outbox_id = ?",
            (outbox_id,),
        )
        if row is None:
            raise OutboxIntegrityError(f"inserted outbox row vanished: {outbox_id}")
        return self._row_to_record(row)

    def get(self, outbox_id: str) -> OutboxRecord | None:
        row = self._storage.query_one(
            "SELECT * FROM transactional_outbox WHERE outbox_id = ?",
            (outbox_id,),
        )
        return self._row_to_record(row) if row is not None else None

    def get_by_idempotency_key(self, idempotency_key: str) -> OutboxRecord | None:
        row = self._storage.query_one(
            "SELECT * FROM transactional_outbox WHERE idempotency_key = ?",
            (idempotency_key,),
        )
        return self._row_to_record(row) if row is not None else None

    def list_records(
        self,
        *,
        status: OutboxStatus | None = None,
        limit: int = 1000,
    ) -> list[OutboxRecord]:
        if limit < 1:
            return []
        if status is None:
            rows = self._storage.query_all(
                """
                SELECT * FROM transactional_outbox
                ORDER BY created_at ASC, outbox_id ASC
                LIMIT ?
                """,
                (limit,),
            )
        else:
            rows = self._storage.query_all(
                """
                SELECT * FROM transactional_outbox
                WHERE status = ?
                ORDER BY created_at ASC, outbox_id ASC
                LIMIT ?
                """,
                (status.value, limit),
            )
        return [self._row_to_record(row) for row in rows]

    def claim_next(
        self,
        worker_id: str,
        *,
        now: datetime | None = None,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> OutboxRecord | None:
        """Claim one ready row, including an expired in-flight claim.

        The returned ``claim_token`` fences acknowledgements from an older
        worker after its claim expires and a new worker takes ownership.
        """

        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if claim_ttl <= timedelta(0):
            raise ValueError("claim_ttl must be positive")

        claim_time = _normalize_time(now)
        expires_at = claim_time + claim_ttl
        claim_token = uuid4().hex

        with self._storage.transaction(immediate=True) as tx:
            row = tx.query_one(
                """
                SELECT * FROM transactional_outbox
                WHERE
                    (status = 'pending' AND available_at <= ?)
                    OR
                    (
                        status = 'in_flight'
                        AND claim_expires_at IS NOT NULL
                        AND claim_expires_at <= ?
                    )
                ORDER BY available_at ASC, created_at ASC, outbox_id ASC
                LIMIT 1
                """,
                (claim_time.isoformat(), claim_time.isoformat()),
            )
            if row is None:
                return None

            cursor = tx.execute(
                """
                UPDATE transactional_outbox
                SET status = 'in_flight',
                    attempts = attempts + 1,
                    claimed_by = ?,
                    claim_token = ?,
                    claimed_at = ?,
                    claim_expires_at = ?
                WHERE outbox_id = ?
                  AND (
                    (status = 'pending' AND available_at <= ?)
                    OR
                    (
                        status = 'in_flight'
                        AND claim_expires_at IS NOT NULL
                        AND claim_expires_at <= ?
                    )
                  )
                """,
                (
                    worker_id,
                    claim_token,
                    claim_time.isoformat(),
                    expires_at.isoformat(),
                    row["outbox_id"],
                    claim_time.isoformat(),
                    claim_time.isoformat(),
                ),
            )
            if cursor.rowcount != 1:
                return None
            claimed = tx.query_one(
                "SELECT * FROM transactional_outbox WHERE outbox_id = ?",
                (row["outbox_id"],),
            )
            if claimed is None:
                raise OutboxIntegrityError(f"claimed outbox row vanished: {row['outbox_id']}")
            return self._row_to_record(claimed)

    def ack(
        self,
        outbox_id: str,
        claim_token: str,
        *,
        result: Any | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Mark a claimed row delivered if the caller still owns its claim."""

        delivered_at = _normalize_time(now)
        result_json = SQLiteStorage.dumps(result) if result is not None else None
        with self._storage.transaction(immediate=True) as tx:
            cursor = tx.execute(
                """
                UPDATE transactional_outbox
                SET status = 'delivered',
                    delivery_result_json = ?,
                    delivered_at = ?,
                    claimed_by = NULL,
                    claim_token = NULL,
                    claimed_at = NULL,
                    claim_expires_at = NULL,
                    last_error_json = NULL
                WHERE outbox_id = ?
                  AND status = 'in_flight'
                  AND claim_token = ?
                  AND claim_expires_at > ?
                """,
                (
                    result_json,
                    delivered_at.isoformat(),
                    outbox_id,
                    claim_token,
                    delivered_at.isoformat(),
                ),
            )
            return cursor.rowcount == 1

    def nack(
        self,
        outbox_id: str,
        claim_token: str,
        error: BaseException | Mapping[str, Any] | str,
        *,
        retry_at: datetime | None = None,
        terminal: bool = False,
        now: datetime | None = None,
    ) -> bool:
        """Record a delivery failure under the current fenced claim.

        Retryable failures return to ``PENDING``.  ``terminal=True`` moves the
        row to ``FAILED`` for explicit operator intervention.
        """

        failure_time = _normalize_time(now)
        next_time = _normalize_time(retry_at, default=failure_time)
        error_json = SQLiteStorage.dumps(self._serialize_error(error))
        next_status = OutboxStatus.FAILED if terminal else OutboxStatus.PENDING

        with self._storage.transaction(immediate=True) as tx:
            cursor = tx.execute(
                """
                UPDATE transactional_outbox
                SET status = ?,
                    available_at = ?,
                    last_error_json = ?,
                    claimed_by = NULL,
                    claim_token = NULL,
                    claimed_at = NULL,
                    claim_expires_at = NULL
                WHERE outbox_id = ?
                  AND status = 'in_flight'
                  AND claim_token = ?
                  AND claim_expires_at > ?
                """,
                (
                    next_status.value,
                    next_time.isoformat(),
                    error_json,
                    outbox_id,
                    claim_token,
                    failure_time.isoformat(),
                ),
            )
            return cursor.rowcount == 1

    def retry_failed(
        self,
        outbox_id: str,
        *,
        available_at: datetime | None = None,
    ) -> bool:
        """Move a terminal failed row back to the pending queue."""

        retry_time = _normalize_time(available_at)
        with self._storage.transaction(immediate=True) as tx:
            cursor = tx.execute(
                """
                UPDATE transactional_outbox
                SET status = 'pending',
                    available_at = ?
                WHERE outbox_id = ? AND status = 'failed'
                """,
                (retry_time.isoformat(), outbox_id),
            )
            return cursor.rowcount == 1

    async def dispatch_once(
        self,
        publisher: OutboxPublisher,
        *,
        worker_id: str,
        now: datetime | None = None,
        claim_ttl: timedelta = timedelta(seconds=30),
        retry_delay: timedelta = timedelta(seconds=1),
        max_attempts: int | None = None,
    ) -> DispatchResult:
        """Claim and deliver one row.

        A successful publish followed by a process crash before ``ack`` is
        intentionally indistinguishable from a failed publish.  The expired
        claim is delivered again with the same idempotency key.
        """

        if retry_delay < timedelta(0):
            raise ValueError("retry_delay must be non-negative")
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be positive")

        dispatch_time = _normalize_time(now)
        record = self.claim_next(
            worker_id,
            now=dispatch_time,
            claim_ttl=claim_ttl,
        )
        if record is None:
            return DispatchResult(record=None, delivered=False)
        if record.claim_token is None:
            raise OutboxIntegrityError(f"claimed row has no token: {record.outbox_id}")

        try:
            published = publisher.publish(
                destination=record.destination,
                payload=record.payload,
                idempotency_key=record.idempotency_key,
                headers=record.headers,
            )
            result = await published if inspect.isawaitable(published) else published
        except Exception as exc:
            terminal = max_attempts is not None and record.attempts >= max_attempts
            self.nack(
                record.outbox_id,
                record.claim_token,
                exc,
                retry_at=dispatch_time + retry_delay,
                terminal=terminal,
                now=dispatch_time,
            )
            return DispatchResult(record=record, delivered=False, error=str(exc))

        acked = self.ack(
            record.outbox_id,
            record.claim_token,
            result=result,
            now=dispatch_time,
        )
        if not acked:
            # A claim can expire while the external call is in flight.  Do not
            # report delivery as durably acknowledged under a stale token.
            return DispatchResult(
                record=record,
                delivered=False,
                error="outbox claim lost before acknowledgement",
            )
        return DispatchResult(record=record, delivered=True)

    async def dispatch_ready(
        self,
        publisher: OutboxPublisher,
        *,
        worker_id: str,
        limit: int = 100,
        now: datetime | None = None,
        claim_ttl: timedelta = timedelta(seconds=30),
        retry_delay: timedelta = timedelta(seconds=1),
        max_attempts: int | None = None,
    ) -> list[DispatchResult]:
        """Drain up to ``limit`` currently-ready records."""

        if limit < 0:
            raise ValueError("limit must be non-negative")
        results: list[DispatchResult] = []
        cursor_time = _normalize_time(now)
        deferred_ids: list[str] = []
        for _ in range(limit):
            result = await self.dispatch_once(
                publisher,
                worker_id=worker_id,
                now=cursor_time,
                claim_ttl=claim_ttl,
                retry_delay=retry_delay,
                max_attempts=max_attempts,
            )
            if result.record is None:
                break
            results.append(result)
            # A zero retry delay must not let one poison record monopolize an
            # entire drain call.  Temporarily move retryable failures past the
            # current scan timestamp; restore their availability afterwards.
            current = self.get(result.record.outbox_id)
            if (
                not result.delivered
                and current is not None
                and current.status == OutboxStatus.PENDING
                and current.available_at <= cursor_time
            ):
                deferred_until = cursor_time + timedelta(microseconds=1)
                with self._storage.transaction(immediate=True) as tx:
                    cursor = tx.execute(
                        """
                        UPDATE transactional_outbox
                        SET available_at = ?
                        WHERE outbox_id = ?
                          AND status = 'pending'
                          AND available_at <= ?
                        """,
                        (
                            deferred_until.isoformat(),
                            current.outbox_id,
                            cursor_time.isoformat(),
                        ),
                    )
                    if cursor.rowcount == 1:
                        deferred_ids.append(current.outbox_id)
        if deferred_ids:
            with self._storage.transaction(immediate=True) as tx:
                for outbox_id in deferred_ids:
                    tx.execute(
                        """
                        UPDATE transactional_outbox
                        SET available_at = ?
                        WHERE outbox_id = ?
                          AND status = 'pending'
                          AND available_at > ?
                        """,
                        (
                            cursor_time.isoformat(),
                            outbox_id,
                            cursor_time.isoformat(),
                        ),
                    )
        return results

    @staticmethod
    def _serialize_error(
        error: BaseException | Mapping[str, Any] | str,
    ) -> dict[str, Any]:
        if isinstance(error, BaseException):
            return {
                "type": type(error).__name__,
                "message": str(error),
            }
        if isinstance(error, str):
            return {"message": error}
        return dict(error)

    @staticmethod
    def _assert_same_intent(record: OutboxRecord, intent: OutboxIntent) -> None:
        payload_json = SQLiteStorage.dumps(dict(intent.payload))
        headers_json = SQLiteStorage.dumps(dict(intent.headers))
        same = (
            record.destination == intent.destination
            and record.payload_hash == _payload_hash(payload_json)
            and SQLiteStorage.dumps(record.headers) == headers_json
            and record.aggregate_type == intent.aggregate_type
            and record.aggregate_id == intent.aggregate_id
        )
        if not same:
            raise OutboxIdempotencyConflict(
                "idempotency key was already committed for a different intent: "
                f"{intent.idempotency_key}"
            )

    @classmethod
    def _get_by_idempotency_key_tx(
        cls,
        tx: Any,
        idempotency_key: str,
    ) -> OutboxRecord | None:
        row = tx.query_one(
            "SELECT * FROM transactional_outbox WHERE idempotency_key = ?",
            (idempotency_key,),
        )
        return cls._row_to_record(row) if row is not None else None

    @staticmethod
    def _row_to_record(row: Mapping[str, Any]) -> OutboxRecord:
        payload_json = str(row["payload_json"])
        expected_hash = _payload_hash(payload_json)
        stored_hash = str(row["payload_hash"])
        if stored_hash != expected_hash:
            raise OutboxIntegrityError(f"outbox payload hash mismatch for {row['outbox_id']}")

        payload = SQLiteStorage.loads(payload_json)
        headers = SQLiteStorage.loads(str(row["headers_json"]))
        if not isinstance(payload, dict):
            raise OutboxIntegrityError(f"outbox payload is not an object: {row['outbox_id']}")
        if not isinstance(headers, dict):
            raise OutboxIntegrityError(f"outbox headers are not an object: {row['outbox_id']}")

        def parse_time(value: Any | None) -> datetime | None:
            if value is None:
                return None
            parsed = datetime.fromisoformat(str(value))
            if parsed.tzinfo is None:
                raise OutboxIntegrityError(
                    f"outbox timestamp is timezone-naive for {row['outbox_id']}"
                )
            return parsed.astimezone(UTC)

        status_value = str(row["status"])
        try:
            status = OutboxStatus(status_value)
        except ValueError as exc:
            raise OutboxIntegrityError(
                f"unknown outbox status for {row['outbox_id']}: {status_value}"
            ) from exc

        return OutboxRecord(
            outbox_id=str(row["outbox_id"]),
            idempotency_key=str(row["idempotency_key"]),
            destination=str(row["destination"]),
            payload=payload,
            payload_hash=stored_hash,
            headers=headers,
            aggregate_type=(
                str(row["aggregate_type"]) if row.get("aggregate_type") is not None else None
            ),
            aggregate_id=(
                str(row["aggregate_id"]) if row.get("aggregate_id") is not None else None
            ),
            causation_id=(
                str(row["causation_id"]) if row.get("causation_id") is not None else None
            ),
            correlation_id=(
                str(row["correlation_id"]) if row.get("correlation_id") is not None else None
            ),
            transaction_id=str(row["transaction_id"]),
            status=status,
            attempts=int(row["attempts"]),
            available_at=parse_time(row["available_at"]) or _utc_now(),
            claimed_by=(str(row["claimed_by"]) if row.get("claimed_by") is not None else None),
            claim_token=(str(row["claim_token"]) if row.get("claim_token") is not None else None),
            claimed_at=parse_time(row.get("claimed_at")),
            claim_expires_at=parse_time(row.get("claim_expires_at")),
            last_error=(
                SQLiteStorage.loads(str(row["last_error_json"]))
                if row.get("last_error_json") is not None
                else None
            ),
            delivery_result=(
                SQLiteStorage.loads(str(row["delivery_result_json"]))
                if row.get("delivery_result_json") is not None
                else None
            ),
            created_at=parse_time(row["created_at"]) or _utc_now(),
            delivered_at=parse_time(row.get("delivered_at")),
        )


__all__ = [
    "CommitIntentResult",
    "DispatchResult",
    "OutboxError",
    "OutboxIdempotencyConflict",
    "OutboxIntegrityError",
    "OutboxIntent",
    "OutboxPublisher",
    "OutboxRecord",
    "OutboxStatus",
    "TransactionalOutbox",
]
