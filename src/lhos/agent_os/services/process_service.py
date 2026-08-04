"""Process Service — spawn, schedule, state transitions.

Maintains the processes_projection table as a projection of the Journal.
All state transitions go through the explicit state machine.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from lhos.agent_os.kernel.errors import IllegalStateTransition
from lhos.agent_os.kernel.models import (
    Clock,
    KernelEvent,
    ProcessControlBlock,
    ProcessState,
)
from lhos.agent_os.kernel.state_machine import apply_process_transition, validate_process_transition
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


class ProcessService:
    """Manages process lifecycle and state transitions."""

    def __init__(
        self,
        storage: SQLiteStorage,
        journal: JournalService,
        clock: Clock,
    ):
        self._storage = storage
        self._journal = journal
        self._clock = clock

    # ── Spawn ──────────────────────────────────────────────────────────────

    def spawn(
        self,
        program_id: str,
        namespace_id: str = "default",
        capability_set_id: str | None = None,
        parent_pid: str | None = None,
        resource_group_id: str = "default",
        priority: int = 10,
        program_state: dict[str, Any] | None = None,
    ) -> ProcessControlBlock:
        pid = uuid4().hex
        cap_set_id = capability_set_id or uuid4().hex

        pcb = ProcessControlBlock(
            pid=pid,
            parent_pid=parent_pid,
            program_id=program_id,
            state=ProcessState.CREATED,
            priority=priority,
            effective_priority=priority,
            capability_set_id=cap_set_id,
            namespace_id=namespace_id,
            resource_group_id=resource_group_id,
        )

        # Persist program state
        state_data = program_state or {}
        self._save_program_state(pid, state_data)
        pcb.program_state_ref = pid

        # Journal PROCESS_SPAWNED
        ev = KernelEvent(
            pid=pid,
            event_type="PROCESS_SPAWNED",
            payload=pcb.model_dump(mode="json"),
        )
        self._journal.append_event(ev)
        self._upsert_projection(pcb)

        # CREATED → READY
        self.transition(pid, ProcessState.READY)

        # Re-fetch the PCB so we return the updated state
        updated = self.get_process(pid)
        return updated if updated is not None else pcb

    # ── State transitions ──────────────────────────────────────────────────

    def transition(
        self,
        pid: str,
        target: ProcessState,
        wait_condition: dict[str, Any] | None = None,
    ) -> KernelEvent:
        """Perform a validated state transition.

        Raises IllegalStateTransition if invalid.
        """
        pcb = self.get_process(pid)
        if pcb is None:
            raise ValueError(f"Process not found: {pid}")

        reason = validate_process_transition(pcb, target, wait_condition)

        apply_process_transition(pcb, target, wait_condition)

        ev = KernelEvent(
            pid=pid,
            event_type=f"PROCESS_{target.value.upper()}",
            causation_id=None,
            payload={
                "old_state": pcb.state.value if target == ProcessState.READY else target.value,
                "new_state": target.value,
                "reason": reason,
                "wait_condition": wait_condition,
            },
        )
        self._journal.append_event(ev)
        self._upsert_projection(pcb)
        return ev

    def safe_transition(
        self,
        pid: str,
        target: ProcessState,
        wait_condition: dict[str, Any] | None = None,
    ) -> KernelEvent | None:
        """Attempt a transition; return None and journal rejection if invalid."""
        try:
            return self.transition(pid, target, wait_condition)
        except (IllegalStateTransition, Exception):
            # Journal the rejection
            pcb = self.get_process(pid)
            current = pcb.state.value if pcb else "unknown"
            ev = KernelEvent(
                pid=pid,
                event_type="PROCESS_TRANSITION_REJECTED",
                payload={
                    "from": current,
                    "to": target.value,
                    "reason": "illegal_transition",
                },
            )
            self._journal.append_event(ev)
            return None

    # ── Queries ────────────────────────────────────────────────────────────

    def get_process(self, pid: str) -> ProcessControlBlock | None:
        row = self._storage.query_one(
            "SELECT * FROM processes_projection WHERE pid = ?",
            (pid,),
        )
        if not row:
            return None
        return self._row_to_pcb(row)

    def list_ready(self) -> list[ProcessControlBlock]:
        rows = self._storage.query_all(
            "SELECT * FROM processes_projection WHERE state = 'ready' ORDER BY priority ASC, created_at ASC"
        )
        return [self._row_to_pcb(r) for r in rows]

    def list_all(self) -> list[ProcessControlBlock]:
        rows = self._storage.query_all("SELECT * FROM processes_projection ORDER BY created_at ASC")
        return [self._row_to_pcb(r) for r in rows]

    def list_blocked(self) -> list[ProcessControlBlock]:
        rows = self._storage.query_all("SELECT * FROM processes_projection WHERE state = 'blocked'")
        return [self._row_to_pcb(r) for r in rows]

    # ── Program state ──────────────────────────────────────────────────────

    def get_program_state(self, pid: str) -> dict[str, Any]:
        row = self._storage.query_one(
            "SELECT state_json FROM program_states WHERE pid = ?",
            (pid,),
        )
        if not row:
            return {}
        return SQLiteStorage.loads(row["state_json"])

    def save_program_state(self, pid: str, state: dict[str, Any]) -> None:
        self._save_program_state(pid, state)

    def _save_program_state(self, pid: str, state: dict[str, Any]) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO program_states (pid, state_json, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(pid) DO UPDATE SET state_json = excluded.state_json, updated_at = excluded.updated_at""",
                (pid, SQLiteStorage.dumps(state), datetime.utcnow().isoformat()),
            )

    # ── Projection ─────────────────────────────────────────────────────────

    def handle_event(self, ev: KernelEvent) -> None:
        """Projection handler — called during replay."""
        if ev.event_type == "PROCESS_SPAWNED":
            pcb = ProcessControlBlock(**ev.payload)
            self._upsert_projection(pcb)
            self._save_program_state(pcb.pid, self.get_program_state(pcb.pid))
        elif ev.event_type.startswith("PROCESS_") and ev.event_type not in (
            "PROCESS_SPAWNED",
            "PROCESS_TRANSITION_REJECTED",
        ):
            # Update state from payload
            new_state = ev.payload.get("new_state")
            if new_state:
                pcb = self.get_process(ev.pid)
                if pcb:
                    pcb.state = ProcessState(new_state)
                    if ev.payload.get("wait_condition"):
                        pcb.wait_condition = ev.payload["wait_condition"]
                    elif new_state in ("ready", "running"):
                        pcb.wait_condition = None
                    self._upsert_projection(pcb)

    def _upsert_projection(self, pcb: ProcessControlBlock) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO processes_projection
                   (pid, parent_pid, program_id, state, priority, effective_priority,
                    capability_set_id, namespace_id, resource_group_id,
                    program_state_ref, pending_request_id, wait_condition_json,
                    checkpoint_ref, exit_code, result_ref, event_cursor, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(pid) DO UPDATE SET
                    state = excluded.state,
                    priority = excluded.priority,
                    effective_priority = excluded.effective_priority,
                    pending_request_id = excluded.pending_request_id,
                    wait_condition_json = excluded.wait_condition_json,
                    checkpoint_ref = excluded.checkpoint_ref,
                    exit_code = excluded.exit_code,
                    result_ref = excluded.result_ref,
                    event_cursor = excluded.event_cursor""",
                (
                    pcb.pid,
                    pcb.parent_pid,
                    pcb.program_id,
                    pcb.state.value,
                    pcb.priority,
                    pcb.effective_priority,
                    pcb.capability_set_id,
                    pcb.namespace_id,
                    pcb.resource_group_id,
                    pcb.program_state_ref,
                    pcb.pending_request_id,
                    SQLiteStorage.dumps(pcb.wait_condition) if pcb.wait_condition else None,
                    pcb.checkpoint_ref,
                    pcb.exit_code,
                    pcb.result_ref,
                    pcb.event_cursor,
                    pcb.created_at.isoformat(),
                ),
            )

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _row_to_pcb(row: dict[str, Any]) -> ProcessControlBlock:
        wc = (
            SQLiteStorage.loads(row["wait_condition_json"])
            if row.get("wait_condition_json")
            else None
        )
        return ProcessControlBlock(
            pid=row["pid"],
            parent_pid=row["parent_pid"],
            program_id=row["program_id"],
            state=ProcessState(row["state"]),
            priority=row["priority"],
            effective_priority=row["effective_priority"],
            capability_set_id=row["capability_set_id"],
            namespace_id=row["namespace_id"],
            resource_group_id=row["resource_group_id"],
            program_state_ref=row.get("program_state_ref") or "",
            pending_request_id=row.get("pending_request_id"),
            wait_condition=wc,
            checkpoint_ref=row.get("checkpoint_ref"),
            exit_code=row.get("exit_code"),
            result_ref=row.get("result_ref"),
            event_cursor=row.get("event_cursor", 0),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
