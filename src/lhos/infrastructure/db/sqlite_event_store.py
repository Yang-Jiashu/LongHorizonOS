"""Append-only SQLite event store (spec section 5).

Guarantees:
- strictly increasing per-run sequence numbers (invariant 8);
- (run_id, idempotency_key) uniqueness — replayed side-effect calls are no-ops;
- append happens inside the caller's transaction when one is active, so event
  and projection updates commit or roll back together (spec section 5.3).
"""

from __future__ import annotations

import json

from lhos.domain.events import RuntimeEvent
from lhos.infrastructure.db.connection import Database
from lhos.ports.telemetry import Tracer


class SqliteEventStore:
    def __init__(self, db: Database, tracer: Tracer | None = None):
        self._db = db
        self._tracer = tracer

    def next_sequence(self, run_id: str) -> int:
        row = self._db.conn.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS nxt FROM events WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return int(row["nxt"])

    def append(self, event: RuntimeEvent) -> RuntimeEvent:
        if event.idempotency_key:
            existing = self.find_by_idempotency(event.run_id, event.idempotency_key)
            if existing is not None:
                return existing
        event.sequence = self.next_sequence(event.run_id)
        self._db.conn.execute(
            """
            INSERT INTO events(
                id, run_id, sequence, event_type, actor_type, actor_id,
                payload_json, evidence_ids_json, causation_id, correlation_id,
                idempotency_key, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                event.run_id,
                event.sequence,
                event.event_type,
                event.actor_type,
                event.actor_id,
                json.dumps(event.payload, sort_keys=True, default=str),
                json.dumps(event.evidence_ids),
                event.causation_id,
                event.correlation_id,
                event.idempotency_key,
                event.created_at.isoformat(),
            ),
        )
        if self._tracer is not None:
            self._tracer.record_event(event)
        return event

    def list_events(self, run_id: str, since_sequence: int = 0) -> list[RuntimeEvent]:
        rows = self._db.conn.execute(
            "SELECT * FROM events WHERE run_id = ? AND sequence > ? ORDER BY sequence ASC",
            (run_id, since_sequence),
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def find_by_idempotency(self, run_id: str, idempotency_key: str) -> RuntimeEvent | None:
        row = self._db.conn.execute(
            "SELECT * FROM events WHERE run_id = ? AND idempotency_key = ?",
            (run_id, idempotency_key),
        ).fetchone()
        return self._row_to_event(row) if row else None

    def count_events(self, run_id: str, event_type: str | None = None) -> int:
        if event_type is None:
            row = self._db.conn.execute(
                "SELECT COUNT(*) AS c FROM events WHERE run_id = ?", (run_id,)
            ).fetchone()
        else:
            row = self._db.conn.execute(
                "SELECT COUNT(*) AS c FROM events WHERE run_id = ? AND event_type = ?",
                (run_id, event_type),
            ).fetchone()
        return int(row["c"])

    @staticmethod
    def _row_to_event(row) -> RuntimeEvent:  # noqa: ANN001 - sqlite3.Row
        from datetime import datetime

        return RuntimeEvent(
            id=row["id"],
            run_id=row["run_id"],
            sequence=row["sequence"],
            event_type=row["event_type"],
            actor_type=row["actor_type"],
            actor_id=row["actor_id"],
            payload=json.loads(row["payload_json"]),
            evidence_ids=json.loads(row["evidence_ids_json"]),
            causation_id=row["causation_id"],
            correlation_id=row["correlation_id"],
            idempotency_key=row["idempotency_key"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )
