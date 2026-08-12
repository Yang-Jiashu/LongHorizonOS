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

from datetime import UTC, datetime
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
        resource_claims: list[dict[str, Any]] | None = None,
    ) -> ActionControlBlock:
        acb = ActionControlBlock(
            action_id=action_id or uuid4().hex,
            pid=pid,
            device_type=device_type,
            operation=operation,
            arguments=arguments or {},
            resource_claims=resource_claims or [],
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

    def mark_intent_durable(
        self,
        action_id: str,
        lease_ids: list[str],
        *,
        fencing_tokens: dict[str, int] | None = None,
    ) -> KernelEvent:
        acb = self.get_action(action_id)
        if acb is None:
            raise ValueError(f"Action not found: {action_id}")
        acb.lease_ids = lease_ids
        if fencing_tokens is not None:
            acb.fencing_tokens = dict(fencing_tokens)
        ev = KernelEvent(
            pid=acb.pid,
            event_type="ACTION_INTENT_DURABLE",
            payload={
                "action_id": action_id,
                "lease_ids": lease_ids,
                "fencing_tokens": acb.fencing_tokens,
            },
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

    def commit_if_fenced(
        self,
        action_id: str,
        result: dict[str, Any] | None = None,
        *,
        now: datetime | None = None,
    ) -> tuple[bool, str | None]:
        """Atomically commit a RUNNING Action only under its original leases.

        Fence validation, the RUNNING->COMMITTED transition, journal append,
        and projection update share one ``BEGIN IMMEDIATE`` transaction.
        Lease expiry/release/reacquisition therefore linearizes either before
        this commit (which is rejected) or after it (which is harmless).
        """
        commit_time = now or datetime.now(UTC)
        if commit_time.tzinfo is None:
            commit_time = commit_time.replace(tzinfo=UTC)

        with self._storage.transaction(immediate=True) as tx:
            row = tx.query_one(
                "SELECT * FROM actions_projection WHERE action_id = ?",
                (action_id,),
            )
            if row is None:
                return False, "action_missing"
            acb = self._row_to_acb(row)
            if acb.state != ActionState.RUNNING:
                return False, f"action_not_running:{acb.state.value}"

            fence_error = self._validate_fencing_contract_tx(tx, acb, commit_time)
            if fence_error is not None:
                return False, fence_error

            apply_action_transition(acb, ActionState.COMMITTED)
            acb.result = result or {}
            acb.finished_at = commit_time
            ev = KernelEvent(
                pid=acb.pid,
                event_type="ACTION_COMMITTED",
                payload={
                    "action_id": action_id,
                    "result": result or {},
                    "fencing_tokens": acb.fencing_tokens,
                },
                created_at=commit_time,
            )
            self._journal.append_events_tx(tx, [ev])
            self._upsert_projection_tx(tx, acb)
        return True, None

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

    def fail_if_running(
        self,
        action_id: str,
        error: dict[str, Any] | None = None,
    ) -> bool:
        """Fail an in-flight action iff it is still RUNNING.

        Driver completions race cancellation, timeout, recovery, and another
        worker.  The ordinary ``fail`` method intentionally raises on a
        terminal action, which is useful for API misuse but unsafe in those
        race paths.  This conditional variant linearizes the state check and
        transition in one transaction and treats an already-terminal action as
        a harmless no-op.
        """
        detail = error or {}
        with self._storage.transaction(immediate=True) as tx:
            row = tx.query_one(
                "SELECT * FROM actions_projection WHERE action_id = ?",
                (action_id,),
            )
            if row is None:
                return False
            acb = self._row_to_acb(row)
            if acb.state != ActionState.RUNNING:
                return False
            apply_action_transition(acb, ActionState.FAILED)
            acb.error = detail
            acb.finished_at = datetime.now(UTC)
            ev = KernelEvent(
                pid=acb.pid,
                event_type="ACTION_FAILED",
                payload={"action_id": action_id, "error": detail},
                created_at=acb.finished_at,
            )
            self._journal.append_events_tx(tx, [ev])
            self._upsert_projection_tx(tx, acb)
        return True

    def mark_uncertain_if_running(
        self,
        action_id: str,
        detail: dict[str, Any] | None = None,
    ) -> bool:
        """Mark RUNNING action UNCERTAIN, ignoring a concurrent terminal race."""
        error = detail or {"reason": "uncertain"}
        with self._storage.transaction(immediate=True) as tx:
            row = tx.query_one(
                "SELECT * FROM actions_projection WHERE action_id = ?",
                (action_id,),
            )
            if row is None:
                return False
            acb = self._row_to_acb(row)
            if acb.state != ActionState.RUNNING:
                return False
            apply_action_transition(acb, ActionState.UNCERTAIN)
            acb.error = error
            acb.finished_at = datetime.now(UTC)
            ev = KernelEvent(
                pid=acb.pid,
                event_type="ACTION_UNCERTAIN",
                payload={"action_id": action_id, "detail": error},
                created_at=acb.finished_at,
            )
            self._journal.append_events_tx(tx, [ev])
            self._upsert_projection_tx(tx, acb)
        return True

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
                "ACTION_REJECTED": ActionState.FAILED,
                "ACTION_TIMED_OUT": ActionState.TIMED_OUT,
                "ACTION_UNCERTAIN": ActionState.UNCERTAIN,
                "ACTION_CANCELLED": ActionState.CANCELLED,
            }
            if ev.event_type in state_map:
                current_acb.state = state_map[ev.event_type]
            if ev.event_type == "ACTION_INTENT_DURABLE":
                current_acb.lease_ids = ev.payload.get("lease_ids", [])
                current_acb.fencing_tokens = {
                    str(resource_id): int(token)
                    for resource_id, token in ev.payload.get(
                        "fencing_tokens",
                        {},
                    ).items()
                }
            if ev.event_type == "ACTION_COMMITTED":
                current_acb.result = ev.payload.get("result", {})
                current_acb.finished_at = ev.created_at
            if ev.event_type in (
                "ACTION_FAILED",
                "ACTION_REJECTED",
                "ACTION_TIMED_OUT",
                "ACTION_UNCERTAIN",
            ):
                current_acb.error = (
                    ev.payload.get("error")
                    or ev.payload.get("detail")
                    or {"reason": ev.payload.get("reason", "action_failed")}
                )
                current_acb.finished_at = ev.created_at
            self._upsert_projection(current_acb)

    # ── Internal ───────────────────────────────────────────────────────────

    def _upsert_projection(self, acb: ActionControlBlock) -> None:
        with self._storage.transaction() as tx:
            self._upsert_projection_tx(tx, acb)

    @staticmethod
    def _upsert_projection_tx(tx: Any, acb: ActionControlBlock) -> None:
        tx.execute(
            """INSERT INTO actions_projection
               (action_id, pid, device_type, operation, arguments_json, state,
                resource_claims_json, lease_ids_json, fencing_tokens_json,
                idempotency_key, side_effect_class, recovery_policy,
                timeout_seconds, result_json, error_json, submitted_at,
                finished_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(action_id) DO UPDATE SET
                state = excluded.state,
                resource_claims_json = excluded.resource_claims_json,
                lease_ids_json = excluded.lease_ids_json,
                fencing_tokens_json = excluded.fencing_tokens_json,
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
                SQLiteStorage.dumps(acb.resource_claims),
                SQLiteStorage.dumps(acb.lease_ids),
                SQLiteStorage.dumps(acb.fencing_tokens),
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
    def _validate_fencing_contract_tx(
        tx: Any,
        acb: ActionControlBlock,
        now: datetime,
    ) -> str | None:
        claims = acb.resource_claims
        lease_ids = acb.lease_ids
        tokens = acb.fencing_tokens
        if not claims:
            if lease_ids:
                return "unexpected_leases"
            if tokens:
                return "unexpected_fencing_tokens"
            return None
        if len(lease_ids) != len(claims) or len(set(lease_ids)) != len(lease_ids):
            return "lease_count_mismatch"
        expected_resources = {str(claim["resource_id"]) for claim in claims}
        if set(tokens) != expected_resources:
            return "fencing_token_count_mismatch"

        actual: list[tuple[str, str]] = []
        for lease_id in lease_ids:
            lease = tx.query_one(
                "SELECT * FROM leases_projection WHERE lease_id = ?",
                (lease_id,),
            )
            if lease is None:
                return "lease_missing"
            if lease["owner_pid"] != acb.pid:
                return "lease_owner_mismatch"
            expiry = datetime.fromisoformat(lease["expires_at"])
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=UTC)
            if expiry <= now:
                return "lease_expired"
            resource_id = str(lease["resource_id"])
            token = int(lease.get("fencing_token", 0))
            if token <= 0 or tokens.get(resource_id) != token:
                return "lease_fencing_token_mismatch"
            current = tx.query_one(
                "SELECT last_token FROM resource_fencing_tokens WHERE resource_id = ?",
                (resource_id,),
            )
            if current is None or int(current["last_token"]) != token:
                return "resource_fencing_token_superseded"
            actual.append((resource_id, str(lease["mode"])))

        expected = sorted(
            (
                str(claim["resource_id"]),
                str(claim.get("mode", "exclusive")),
            )
            for claim in claims
        )
        if sorted(actual) != expected:
            return "lease_claim_mismatch"
        return None

    @staticmethod
    def _row_to_acb(row: dict[str, Any]) -> ActionControlBlock:
        return ActionControlBlock(
            action_id=row["action_id"],
            pid=row["pid"],
            device_type=row["device_type"],
            operation=row["operation"],
            arguments=SQLiteStorage.loads(row["arguments_json"]),
            state=ActionState(row["state"]),
            resource_claims=SQLiteStorage.loads(row.get("resource_claims_json") or "[]"),
            lease_ids=SQLiteStorage.loads(row.get("lease_ids_json") or "[]"),
            fencing_tokens={
                str(resource_id): int(token)
                for resource_id, token in SQLiteStorage.loads(
                    row.get("fencing_tokens_json") or "{}"
                ).items()
            },
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
