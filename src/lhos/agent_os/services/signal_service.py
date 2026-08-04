"""Signal Service — durable signal delivery.

Signal types:
- ACTION_COMPLETED
- ACTION_FAILED
- ACTION_UNCERTAIN
- LEASE_AVAILABLE
- CANCEL
- RESUME

Rules:
- Signal is durable (persisted in Journal).
- Signal delivery is replayable.
- Consumed signals do not re-wake processes.
- BLOCKED process only woken by matching wait condition.
- Non-matching signals stay in mailbox.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lhos.agent_os.kernel.models import KernelEvent, ProcessState, Signal
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.process_service import ProcessService
from lhos.agent_os.storage.sqlite import SQLiteStorage


class SignalService:
    """Durable signal delivery with wait-condition matching."""

    def __init__(
        self,
        storage: SQLiteStorage,
        journal: JournalService,
        process_service: ProcessService,
    ):
        self._storage = storage
        self._journal = journal
        self._process_service = process_service

    def send(
        self,
        target_pid: str,
        signal_type: str,
        source_pid: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Signal:
        """Send a durable signal."""
        signal = Signal(
            target_pid=target_pid,
            signal_type=signal_type,
            source_pid=source_pid,
            payload=payload or {},
        )

        ev = KernelEvent(
            pid=source_pid or target_pid,
            event_type="SIGNAL_SENT",
            payload=signal.model_dump(mode="json"),
        )
        self._journal.append_event(ev)
        self._upsert_signal(signal)
        return signal

    def deliver_pending(self) -> int:
        """Deliver unconsumed signals to their target processes.

        A signal is delivered if:
        - Target process exists
        - Target process is BLOCKED
        - Signal matches the wait condition

        Returns count of signals delivered.
        """
        unconsumed = self._storage.query_all(
            "SELECT * FROM signals_projection WHERE consumed = 0 ORDER BY created_at ASC"
        )
        delivered = 0
        for row in unconsumed:
            signal = self._row_to_signal(row)
            pcb = self._process_service.get_process(signal.target_pid)
            if pcb is None:
                continue
            if pcb.state != ProcessState.BLOCKED:
                continue
            if not self._matches_wait_condition(pcb.wait_condition, signal):
                continue

            # Deliver: mark consumed, wake process
            self._mark_consumed(signal.signal_id)
            self._process_service.transition(signal.target_pid, ProcessState.READY)
            delivered += 1

        return delivered

    def deliver_to_pid(self, target_pid: str) -> int:
        """Deliver unconsumed signals to a specific pid."""
        unconsumed = self._storage.query_all(
            "SELECT * FROM signals_projection WHERE consumed = 0 AND target_pid = ? ORDER BY created_at ASC",
            (target_pid,),
        )
        delivered = 0
        for row in unconsumed:
            signal = self._row_to_signal(row)
            pcb = self._process_service.get_process(signal.target_pid)
            if pcb is None:
                continue
            if pcb.state != ProcessState.BLOCKED:
                continue
            if not self._matches_wait_condition(pcb.wait_condition, signal):
                continue

            self._mark_consumed(signal.signal_id)
            self._process_service.transition(signal.target_pid, ProcessState.READY)
            delivered += 1

        return delivered

    def list_pending(self, target_pid: str) -> list[Signal]:
        rows = self._storage.query_all(
            "SELECT * FROM signals_projection WHERE consumed = 0 AND target_pid = ? ORDER BY created_at ASC",
            (target_pid,),
        )
        return [self._row_to_signal(r) for r in rows]

    def list_all(self) -> list[Signal]:
        rows = self._storage.query_all("SELECT * FROM signals_projection ORDER BY created_at ASC")
        return [self._row_to_signal(r) for r in rows]

    # ── Projection ─────────────────────────────────────────────────────────

    def handle_event(self, ev: KernelEvent) -> None:
        if ev.event_type == "SIGNAL_SENT":
            signal = Signal(**ev.payload)
            self._upsert_signal(signal)

    # ── Internal ───────────────────────────────────────────────────────────

    @staticmethod
    def _matches_wait_condition(
        wait_condition: dict[str, Any] | None,
        signal: Signal,
    ) -> bool:
        """Check if a signal matches a process's wait condition."""
        if wait_condition is None:
            return True  # No condition → any signal matches

        # Check signal_type match
        expected_type = wait_condition.get("signal_type")
        if expected_type and signal.signal_type != expected_type:
            return False

        # Check action_id match (for ACTION_COMPLETED/FAILED signals)
        expected_action_id = wait_condition.get("action_id")
        if expected_action_id:
            actual_action_id = signal.payload.get("action_id")
            if actual_action_id != expected_action_id:
                return False

        # Check signal_type list match
        expected_types = wait_condition.get("signal_types")
        return not (
            expected_types
            and isinstance(expected_types, list)
            and signal.signal_type not in expected_types
        )

    def _mark_consumed(self, signal_id: str) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                "UPDATE signals_projection SET consumed = 1 WHERE signal_id = ?",
                (signal_id,),
            )
        ev = KernelEvent(
            pid="kernel",
            event_type="SIGNAL_CONSUMED",
            payload={"signal_id": signal_id},
        )
        self._journal.append_event(ev)

    def _upsert_signal(self, signal: Signal) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT OR REPLACE INTO signals_projection
                   (signal_id, target_pid, signal_type, source_pid,
                    payload_json, created_at, consumed)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    signal.signal_id,
                    signal.target_pid,
                    signal.signal_type,
                    signal.source_pid,
                    SQLiteStorage.dumps(signal.payload),
                    signal.created_at.isoformat(),
                    int(signal.consumed),
                ),
            )

    @staticmethod
    def _row_to_signal(row: dict[str, Any]) -> Signal:
        return Signal(
            signal_id=row["signal_id"],
            target_pid=row["target_pid"],
            signal_type=row["signal_type"],
            source_pid=row.get("source_pid"),
            payload=SQLiteStorage.loads(row.get("payload_json") or "{}"),
            created_at=datetime.fromisoformat(row["created_at"]),
            consumed=bool(row["consumed"]),
        )
