"""Action Service — manages the Action lifecycle:

SUBMITTED → ADMITTED → RUNNING → terminal (COMMITTED / FAILED / TIMED_OUT / UNCERTAIN / CANCELLED)

Key rules:
- Capability check before ADMITTED.
- Atomic lease acquire before INTENT_DURABLE.
- Driver dispatch only after durable intent.
- Driver cannot directly update Action projection.
- One Action can only enter one terminal state.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from lhos.agent_os.kernel.models import (
    ActionControlBlock,
    ActionState,
    KernelEvent,
    SideEffectClass,
)
from lhos.agent_os.kernel.state_machine import apply_action_transition
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


class ActionService:
    """Manages Action lifecycle and projection."""

    def __init__(
        self,
        storage: SQLiteStorage,
        journal: JournalService,
    ):
        self._storage = storage
        self._journal = journal

    # ── Submit ─────────────────────────────────────────────────────────────

    def submit(
        self,
        pid: str,
        device_type: str,
        operation: str,
        arguments: dict[str, Any] | None = None,
        side_effect_class: SideEffectClass = SideEffectClass.PURE,
        idempotency_key: str | None = None,
        timeout_seconds: int | None = None,
        action_id: str | None = None,
    ) -> ActionControlBlock:
        acb = ActionControlBlock(
            action_id=action_id or uuid4().hex,
            pid=pid,
            device_type=device_type,
            operation=operation,
            arguments=arguments or {},
            side_effect_class=side_effect_class,
            idempotency_key=idempotency_key,
            timeout_seconds=timeout_seconds,
        )

        ev = KernelEvent(
            pid=pid,
            event_type="ACTION_SUBMITTED",
            payload=acb.model_dump(mode="json"),
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return acb

    # ── Admit (after capability check passes) ───────────────────────────────

    def admit(self, action_id: str) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        apply_action_transition(acb, ActionState.ADMITTED)
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_ADMITTED",
            payload={"action_id": action_id, "state": "admitted"},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    def reject(self, action_id: str, reason: str) -> KernelEvent:
        """Mark as FAILED before admit (capability denial, etc.)."""
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        apply_action_transition(acb, ActionState.FAILED)
        acb.error = {"reason": reason}
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_REJECTED",
            payload={"action_id": action_id, "reason": reason},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    # ── Intent durable (after lease acquired) ───────────────────────────────

    def mark_intent_durable(self, action_id: str, lease_ids: list[str]) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        acb.lease_ids = lease_ids
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_INTENT_DURABLE",
            payload={"action_id": action_id, "lease_ids": lease_ids},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    # ── Dispatch to driver ─────────────────────────────────────────────────

    def dispatch(self, action_id: str) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        apply_action_transition(acb, ActionState.RUNNING)
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_RUNNING",
            payload={"action_id": action_id},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    # ── Complete ───────────────────────────────────────────────────────────

    def commit(self, action_id: str, result: dict[str, Any] | None = None) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        apply_action_transition(acb, ActionState.COMMITTED)
        acb.result = result or {}
        acb.finished_at = datetime.utcnow()
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_COMMITTED",
            payload={"action_id": action_id, "result": result or {}},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    def fail(self, action_id: str, error: dict[str, Any] | None = None) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        apply_action_transition(acb, ActionState.FAILED)
        acb.error = error or {}
        acb.finished_at = datetime.utcnow()
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_FAILED",
            payload={"action_id": action_id, "error": error or {}},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    def mark_timed_out(self, action_id: str) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        apply_action_transition(acb, ActionState.TIMED_OUT)
        acb.error = {"reason": "timeout"}
        acb.finished_at = datetime.utcnow()
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_TIMED_OUT",
            payload={"action_id": action_id},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    def mark_uncertain(self, action_id: str, detail: dict[str, Any] | None = None) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        apply_action_transition(acb, ActionState.UNCERTAIN)
        acb.error = detail or {"reason": "uncertain"}
        acb.finished_at = datetime.utcnow()
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_UNCERTAIN",
            payload={"action_id": action_id, "detail": detail or {}},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    def cancel(self, action_id: str) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        apply_action_transition(acb, ActionState.CANCELLED)
        acb.finished_at = datetime.utcnow()
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_CANCELLED",
            payload={"action_id": action_id},
        )
        self._journal.append_event(ev)
        self._upsert_projection(acb)
        return ev

    # ── Queries ────────────────────────────────────────────────────────────

    def get_action(self, action_id: str) -> ActionControlBlock | None:
        row = self._storage.query_one(
            "SELECT * FROM actions_projection WHERE action_id = ?",
            (action_id,),
        )
        if not row:
            return None
        return self._row_to_acb(row)

    def list_by_pid(self, pid: str) -> list[ActionControlBlock]:
        rows = self._storage.query_all(
            "SELECT * FROM actions_projection WHERE pid = ? ORDER BY submitted_at ASC",
            (pid,),
        )
        return [self._row_to_acb(r) for r in rows]

    def list_running(self) -> list[ActionControlBlock]:
        rows = self._storage.query_all("SELECT * FROM actions_projection WHERE state = 'running'")
        return [self._row_to_acb(r) for r in rows]

    def list_non_terminal(self) -> list[ActionControlBlock]:
        rows = self._storage.query_all(
            """SELECT * FROM actions_projection
               WHERE state NOT IN ('committed', 'failed', 'cancelled', 'timed_out', 'uncertain')"""
        )
        return [self._row_to_acb(r) for r in rows]

    def list_incomplete(self) -> list[ActionControlBlock]:
        """Actions that are RUNNING or ADMITTED but not yet terminal."""
        rows = self._storage.query_all(
            "SELECT * FROM actions_projection WHERE state IN ('submitted', 'admitted', 'running')"
        )
        return [self._row_to_acb(r) for r in rows]

    # ── Projection ─────────────────────────────────────────────────────────

    def handle_event(self, ev: KernelEvent) -> None:
        if ev.event_type == "ACTION_SUBMITTED":
            acb = ActionControlBlock(**ev.payload)
            self._upsert_projection(acb)
        elif ev.event_type in (
            "ACTION_ADMITTED",
            "ACTION_INTENT_DURABLE",
            "ACTION_RUNNING",
            "ACTION_COMMITTED",
            "ACTION_FAILED",
            "ACTION_TIMED_OUT",
            "ACTION_UNCERTAIN",
            "ACTION_CANCELLED",
            "ACTION_REJECTED",
        ):
            action_id = ev.payload.get("action_id")
            if not action_id:
                return
            current_acb: ActionControlBlock | None = self.get_action(action_id)
            if current_acb is None:
                return
            state_map = {
                "ACTION_ADMITTED": ActionState.ADMITTED,
                "ACTION_RUNNING": ActionState.RUNNING,
                "ACTION_COMMITTED": ActionState.COMMITTED,
                "ACTION_FAILED": ActionState.FAILED,
                "ACTION_TIMED_OUT": ActionState.TIMED_OUT,
                "ACTION_UNCERTAIN": ActionState.UNCERTAIN,
                "ACTION_CANCELLED": ActionState.CANCELLED,
            }
            if ev.event_type in state_map:
                current_acb.state = state_map[ev.event_type]
            if ev.event_type == "ACTION_INTENT_DURABLE":
                current_acb.lease_ids = ev.payload.get("lease_ids", [])
            if ev.event_type == "ACTION_COMMITTED":
                current_acb.result = ev.payload.get("result", {})
                current_acb.finished_at = ev.created_at
            if ev.event_type in ("ACTION_FAILED", "ACTION_TIMED_OUT", "ACTION_UNCERTAIN"):
                current_acb.error = ev.payload.get("error") or ev.payload.get("detail", {})
                current_acb.finished_at = ev.created_at
            self._upsert_projection(current_acb)

    # ── Internal ───────────────────────────────────────────────────────────

    def _upsert_projection(self, acb: ActionControlBlock) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO actions_projection
                   (action_id, pid, device_type, operation, arguments_json, state,
                    lease_ids_json, idempotency_key, side_effect_class, recovery_policy,
                    timeout_seconds, result_json, error_json, submitted_at, finished_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(action_id) DO UPDATE SET
                    state = excluded.state,
                    lease_ids_json = excluded.lease_ids_json,
                    result_json = excluded.result_json,
                    error_json = excluded.error_json,
                    finished_at = excluded.finished_at""",
                (
                    acb.action_id,
                    acb.pid,
                    acb.device_type,
                    acb.operation,
                    SQLiteStorage.dumps(acb.arguments),
                    acb.state.value,
                    SQLiteStorage.dumps(acb.lease_ids),
                    acb.idempotency_key,
                    acb.side_effect_class.value,
                    acb.recovery_policy,
                    acb.timeout_seconds,
                    SQLiteStorage.dumps(acb.result) if acb.result else None,
                    SQLiteStorage.dumps(acb.error) if acb.error else None,
                    acb.submitted_at.isoformat(),
                    acb.finished_at.isoformat() if acb.finished_at else None,
                ),
            )

    @staticmethod
    def _row_to_acb(row: dict[str, Any]) -> ActionControlBlock:
        return ActionControlBlock(
            action_id=row["action_id"],
            pid=row["pid"],
            device_type=row["device_type"],
            operation=row["operation"],
            arguments=SQLiteStorage.loads(row["arguments_json"]),
            state=ActionState(row["state"]),
            lease_ids=SQLiteStorage.loads(row.get("lease_ids_json") or "[]"),
            idempotency_key=row.get("idempotency_key"),
            side_effect_class=SideEffectClass(row.get("side_effect_class", "pure")),
            recovery_policy=row.get("recovery_policy", "retry"),
            timeout_seconds=row.get("timeout_seconds"),
            result=SQLiteStorage.loads(row["result_json"]) if row.get("result_json") else None,
            error=SQLiteStorage.loads(row["error_json"]) if row.get("error_json") else None,
            submitted_at=datetime.fromisoformat(row["submitted_at"]),
            finished_at=datetime.fromisoformat(row["finished_at"])
            if row.get("finished_at")
            else None,
        )
