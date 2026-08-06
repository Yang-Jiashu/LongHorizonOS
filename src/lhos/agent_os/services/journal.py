"""Journal Service — append-only event log backed by SQLite.

The Journal is the single source of truth. All kernel state
(processes, actions, leases, signals) are projections that can be
rebuilt from the Journal.

Key invariants:
- journal_offset is globally monotonic.
- process_sequence is per-pid monotonic.
- event_id append is idempotent (duplicate → returns existing offset).
- Event append and sequence allocation happen in the same transaction.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lhos.agent_os.kernel.models import KernelEvent
from lhos.agent_os.storage.sqlite import SQLiteStorage


class JournalService:
    """Append-only event journal with deterministic replay."""

    def __init__(self, storage: SQLiteStorage):
        self._storage = storage

    # ── Append ─────────────────────────────────────────────────────────────

    def append_event(self, event: KernelEvent) -> KernelEvent:
        """Append a single event. Returns the event with assigned offset."""
        return self.append_events_atomically([event])[0]

    def append_events_atomically(self, events: list[KernelEvent]) -> list[KernelEvent]:
        """Append multiple events in a single transaction.

        - Allocates journal_offset (global) and process_sequence (per-pid).
        - Idempotent on event_id: duplicate returns existing.
        - All or nothing.
        """
        if not events:
            return []

        with self._storage.transaction() as tx:
            results: list[KernelEvent] = []
            for ev in events:
                # Idempotency check
                existing = tx.query_one(
                    "SELECT journal_offset, process_sequence FROM journal_events WHERE event_id = ?",
                    (ev.event_id,),
                )
                if existing:
                    ev.journal_offset = existing["journal_offset"]
                    ev.process_sequence = existing["process_sequence"]
                    results.append(ev)
                    continue

                # Allocate offset
                meta = tx.query_one("SELECT value FROM journal_meta WHERE key = 'next_offset'")
                assert meta is not None
                offset = meta["value"]

                # Allocate per-pid sequence
                seq_row = tx.query_one(
                    "SELECT MAX(process_sequence) AS max_seq FROM journal_events WHERE pid = ?",
                    (ev.pid,),
                )
                seq = (
                    (seq_row["max_seq"] + 1) if (seq_row and seq_row["max_seq"] is not None) else 0
                )

                ev.journal_offset = offset
                ev.process_sequence = seq

                tx.execute(
                    """INSERT INTO journal_events
                       (event_id, journal_offset, pid, process_sequence,
                        event_type, causation_id, correlation_id,
                        payload_json, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ev.event_id,
                        ev.journal_offset,
                        ev.pid,
                        ev.process_sequence,
                        ev.event_type,
                        ev.causation_id,
                        ev.correlation_id,
                        SQLiteStorage.dumps(ev.payload),
                        ev.created_at.isoformat(),
                    ),
                )
                tx.execute("UPDATE journal_meta SET value = value + 1 WHERE key = 'next_offset'")
                results.append(ev)

            return results

    # ── Mutate injection socket for LeaseService atomic_acquire ─────────────
    # LEASE-01 downstream: LeaseService must coalesce the lease INSERT + the
    # LEASE_ACQUIRED journal INSERT inside ONE BEGIN IMMEDIATE, otherwise the
    # side-effect transaction races concurrent acquisitions and inserts
    # duplicate journal rows, desyncing event-sourcing. append_events_tx
    # writes through an already-open _Tx (no BEGIN/COMMIT wrapper).

    def append_events_tx(self, tx: Any, events: list[KernelEvent]) -> list[KernelEvent]:
        """Append journal events *inside* an already-open transaction."""
        if not events:
            return []
        results: list[KernelEvent] = []
        for ev in events:
            existing = tx.query_one(
                "SELECT journal_offset, process_sequence FROM journal_events WHERE event_id = ?",
                (ev.event_id,),
            )
            if existing:
                ev.journal_offset = existing["journal_offset"]
                ev.process_sequence = existing["process_sequence"]
                results.append(ev)
                continue

            meta = tx.query_one("SELECT value FROM journal_meta WHERE key = 'next_offset'")
            assert meta is not None
            offset = meta["value"]

            seq_row = tx.query_one(
                "SELECT MAX(process_sequence) AS max_seq FROM journal_events WHERE pid = ?",
                (ev.pid,),
            )
            seq = (seq_row["max_seq"] + 1) if (seq_row and seq_row["max_seq"] is not None) else 0

            ev.journal_offset = offset
            ev.process_sequence = seq

            tx.execute(
                """INSERT INTO journal_events
                   (event_id, journal_offset, pid, process_sequence,
                    event_type, causation_id, correlation_id,
                    payload_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ev.event_id,
                    ev.journal_offset,
                    ev.pid,
                    ev.process_sequence,
                    ev.event_type,
                    ev.causation_id,
                    ev.correlation_id,
                    SQLiteStorage.dumps(ev.payload),
                    ev.created_at.isoformat(),
                ),
            )
            tx.execute("UPDATE journal_meta SET value = value + 1 WHERE key = 'next_offset'")
            results.append(ev)
        return results

    # ── Read ───────────────────────────────────────────────────────────────

    def read_from_offset(self, offset: int, limit: int = 10000) -> list[KernelEvent]:
        rows = self._storage.query_all(
            """SELECT * FROM journal_events
               WHERE journal_offset >= ?
               ORDER BY journal_offset ASC
               LIMIT ?""",
            (offset, limit),
        )
        return [self._row_to_event(r) for r in rows]

    def read_all(self) -> list[KernelEvent]:
        return self.read_from_offset(0, limit=10**9)

    def replay_all(self) -> list[KernelEvent]:
        return self.read_all()

    # ── Projection rebuild ─────────────────────────────────────────────────

    def rebuild_projections(self, handlers: list[Any]) -> int:
        """Delete all projections and rebuild from Journal.

        ``handlers`` is a list of objects with ``handle_event(event)`` method.
        Returns the number of events replayed.
        """
        # Clear projections
        with self._storage.transaction() as tx:
            for table in [
                "processes_projection",
                "actions_projection",
                "leases_projection",
                "signals_projection",
                "program_states",
                "checkpoints",
                "capability_sets",
                "lease_waiters",
                # Phase C1: Artifact FS projections
                "artifacts_projection",
                "artifact_versions_projection",
                "artifact_handles_projection",
                "write_transactions_projection",
                "namespaces_projection",
                "mounts_projection",
                "artifact_watches_projection",
                "artifact_idempotency",
                "namespace_snapshots_projection",
            ]:
                tx.execute(f"DELETE FROM {table}")
            # Reset meta
            tx.execute(
                "INSERT OR REPLACE INTO journal_meta(key, value) VALUES ('next_offset', "
                "(SELECT COALESCE(MAX(journal_offset), 0) + 1 FROM journal_events))"
            )

        events = self.replay_all()
        for ev in events:
            for handler in handlers:
                handler.handle_event(ev)
        return len(events)

    # ── Helpers ────────────────────────────────────────────────────────────

    def next_offset(self) -> int:
        meta = self._storage.query_one("SELECT value FROM journal_meta WHERE key = 'next_offset'")
        return meta["value"] if meta else 0

    def last_sequence_for_pid(self, pid: str) -> int:
        row = self._storage.query_one(
            "SELECT MAX(process_sequence) AS max_seq FROM journal_events WHERE pid = ?",
            (pid,),
        )
        return (row["max_seq"] + 1) if (row and row["max_seq"] is not None) else 0

    @staticmethod
    def _row_to_event(row: dict[str, Any]) -> KernelEvent:
        return KernelEvent(
            event_id=row["event_id"],
            journal_offset=row["journal_offset"],
            pid=row["pid"],
            process_sequence=row["process_sequence"],
            event_type=row["event_type"],
            causation_id=row["causation_id"],
            correlation_id=row["correlation_id"],
            payload=SQLiteStorage.loads(row["payload_json"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
