"""System Call Dispatcher — routes KernelRequests to appropriate services.

Each syscall type has a handler that:
1. Validates the request
2. Performs capability check (if needed)
3. Executes the operation
4. Returns a KernelEvent (or list of events)
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from lhos.agent_os.kernel.models import (
    AcquireResourceRequest,
    CancelActionRequest,
    CheckpointRequest,
    ExitRequest,
    InspectActionRequest,
    KernelEvent,
    KernelRequest,
    ProcessCheckpoint,
    ProcessState,
    ReleaseResourceRequest,
    RestoreRequest,
    SignalRequest,
    SpawnRequest,
    SubmitActionRequest,
    WaitRequest,
)
from lhos.agent_os.services.action_service import ActionService
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.services.process_service import ProcessService
from lhos.agent_os.services.signal_service import SignalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


class SyscallDispatcher:
    """Routes kernel requests to services."""

    def __init__(
        self,
        storage: SQLiteStorage,
        journal: JournalService,
        process_service: ProcessService,
        action_service: ActionService,
        capability_service: CapabilityService,
        lease_service: LeaseService,
        signal_service: SignalService,
    ):
        self._storage = storage
        self._journal = journal
        self._process_service = process_service
        self._action_service = action_service
        self._capability_service = capability_service
        self._lease_service = lease_service
        self._signal_service = signal_service
        self._handlers: dict[str, Callable[..., Awaitable[KernelEvent | None]]] = {
            "spawn": self._handle_spawn,
            "submit_action": self._handle_submit_action,
            "inspect_action": self._handle_inspect_action,
            "cancel_action": self._handle_cancel_action,
            "acquire": self._handle_acquire,
            "release": self._handle_release,
            "wait": self._handle_wait,
            "signal": self._handle_signal,
            "checkpoint": self._handle_checkpoint,
            "restore": self._handle_restore,
            "exit": self._handle_exit,
        }

    async def dispatch(self, request: KernelRequest) -> KernelEvent | None:
        """Dispatch a kernel request. Returns the resulting event (or None)."""
        handler = self._handlers.get(request.request_type)
        if handler is None:
            raise ValueError(f"Unknown request type: {request.request_type}")
        return await handler(request)

    # ── Spawn ──────────────────────────────────────────────────────────────

    async def _handle_spawn(self, req: SpawnRequest) -> KernelEvent:
        pcb = self._process_service.spawn(
            program_id=req.program_id,
            namespace_id=req.namespace_id or "default",
            capability_set_id=None,
            parent_pid=req.parent_pid,
            resource_group_id=req.resource_group_id,
            program_state=req.initial_state,
        )
        # Grant default capabilities
        from lhos.agent_os.services.capability_service import DEFAULT_CAPABILITIES

        cap_set = self._capability_service.create_capability_set(
            pcb.pid,
            capabilities=DEFAULT_CAPABILITIES["full"],
        )
        # Update PCB with capability set id
        pcb.capability_set_id = cap_set.set_id
        self._process_service._upsert_projection(pcb)

        ev = KernelEvent(
            pid=pcb.pid,
            event_type="SPAWN_COMPLETE",
            payload={"pid": pcb.pid, "program_id": pcb.program_id},
        )
        self._journal.append_event(ev)
        return ev

    # ── Submit Action ──────────────────────────────────────────────────────

    async def _handle_submit_action(self, req: SubmitActionRequest) -> KernelEvent:
        # Capability check
        resource = f"device:{req.device_type}"
        self._capability_service.enforce(req.pid, resource, "invoke")

        # Submit
        acb = self._action_service.submit(
            pid=req.pid,
            device_type=req.device_type,
            operation=req.operation,
            arguments=req.arguments,
            side_effect_class=req.side_effect_class,
            idempotency_key=req.idempotency_key,
            timeout_seconds=req.timeout_seconds,
        )

        # Admit (capability already checked)
        self._action_service.admit(acb.action_id)

        # Acquire leases atomically
        if req.resource_claims:
            leases = self._lease_service.atomic_acquire(
                req.pid,
                req.resource_claims,
            )
            lease_ids = [lease.lease_id for lease in leases]
            self._action_service.mark_intent_durable(acb.action_id, lease_ids)
        else:
            # Still mark intent durable (no leases needed)
            self._action_service.mark_intent_durable(acb.action_id, [])

        return KernelEvent(
            pid=req.pid,
            event_type="ACTION_READY_FOR_DISPATCH",
            payload={"action_id": acb.action_id, "device_type": req.device_type},
        )

    # ── Inspect Action ─────────────────────────────────────────────────────

    async def _handle_inspect_action(self, req: InspectActionRequest) -> KernelEvent:
        acb = self._action_service.get_action(req.action_id)
        if acb is None:
            ev = KernelEvent(
                pid=req.pid,
                event_type="ACTION_INSPECT_FAILED",
                payload={"action_id": req.action_id, "reason": "not_found"},
            )
            self._journal.append_event(ev)
            return ev

        ev = KernelEvent(
            pid=req.pid,
            event_type="ACTION_INSPECTED",
            payload={
                "action_id": req.action_id,
                "state": acb.state.value,
                "result": acb.result,
                "error": acb.error,
            },
        )
        self._journal.append_event(ev)
        return ev

    # ── Cancel Action ──────────────────────────────────────────────────────

    async def _handle_cancel_action(self, req: CancelActionRequest) -> KernelEvent:
        acb = self._action_service.get_action(req.action_id)
        if acb is None:
            return KernelEvent(
                pid=req.pid,
                event_type="ACTION_CANCEL_FAILED",
                payload={"action_id": req.action_id, "reason": "not_found"},
            )
        self._action_service.cancel(req.action_id)
        # Release leases
        self._lease_service.release(acb.lease_ids)
        return KernelEvent(
            pid=req.pid,
            event_type="ACTION_CANCELLED",
            payload={"action_id": req.action_id},
        )

    # ── Acquire Resource ───────────────────────────────────────────────────

    async def _handle_acquire(self, req: AcquireResourceRequest) -> KernelEvent:
        # Capability check for each resource
        for claim in req.claims:
            resource_id = claim["resource_id"]
            self._capability_service.enforce(req.pid, resource_id, "acquire")

        leases = self._lease_service.atomic_acquire(req.pid, req.claims)
        return KernelEvent(
            pid=req.pid,
            event_type="RESOURCE_ACQUIRED",
            payload={
                "lease_ids": [lease.lease_id for lease in leases],
                "resources": [lease.resource_id for lease in leases],
            },
        )

    # ── Release Resource ───────────────────────────────────────────────────

    async def _handle_release(self, req: ReleaseResourceRequest) -> KernelEvent:
        count = self._lease_service.release(req.lease_ids)
        return KernelEvent(
            pid=req.pid,
            event_type="RESOURCE_RELEASED",
            payload={"count": count, "lease_ids": req.lease_ids},
        )

    # ── Wait ───────────────────────────────────────────────────────────────

    async def _handle_wait(self, req: WaitRequest) -> KernelEvent:
        self._process_service.transition(
            req.pid,
            ProcessState.BLOCKED,
            wait_condition=req.condition,
        )
        return KernelEvent(
            pid=req.pid,
            event_type="PROCESS_BLOCKED",
            payload={"condition": req.condition},
        )

    # ── Signal ─────────────────────────────────────────────────────────────

    async def _handle_signal(self, req: SignalRequest) -> KernelEvent:
        # Capability check
        resource = f"process:signal/{req.target_pid}"
        self._capability_service.enforce(req.pid, resource, "send")

        signal = self._signal_service.send(
            target_pid=req.target_pid,
            signal_type=req.signal_type,
            source_pid=req.pid,
            payload=req.payload,
        )
        return KernelEvent(
            pid=req.pid,
            event_type="SIGNAL_SENT",
            payload={"signal_id": signal.signal_id, "target_pid": req.target_pid},
        )

    # ── Checkpoint ───────────────────────────────────────────────────────

    async def _handle_checkpoint(self, req: CheckpointRequest) -> KernelEvent:
        pcb = self._process_service.get_process(req.pid)
        if pcb is None:
            raise ValueError(f"Process not found: {req.pid}")

        self._process_service.get_program_state(req.pid)
        offset = self._journal.next_offset()

        checkpoint = ProcessCheckpoint(
            pid=req.pid,
            journal_offset=offset,
            process_sequence=pcb.event_cursor,
            pcb_snapshot=pcb.model_dump(mode="json"),
            program_state_ref=pcb.program_state_ref,
            wait_condition=pcb.wait_condition,
            mailbox_cursor=0,
        )

        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO checkpoints
                   (checkpoint_id, pid, journal_offset, process_sequence,
                    pcb_snapshot_json, program_state_ref, wait_condition_json,
                    mailbox_cursor, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    checkpoint.checkpoint_id,
                    checkpoint.pid,
                    checkpoint.journal_offset,
                    checkpoint.process_sequence,
                    SQLiteStorage.dumps(checkpoint.pcb_snapshot),
                    checkpoint.program_state_ref,
                    SQLiteStorage.dumps(checkpoint.wait_condition)
                    if checkpoint.wait_condition
                    else None,
                    checkpoint.mailbox_cursor,
                    checkpoint.created_at.isoformat(),
                ),
            )

        pcb.checkpoint_ref = checkpoint.checkpoint_id
        self._process_service._upsert_projection(pcb)

        ev = KernelEvent(
            pid=req.pid,
            event_type="CHECKPOINT_CREATED",
            payload={
                "checkpoint_id": checkpoint.checkpoint_id,
                "journal_offset": offset,
            },
        )
        self._journal.append_event(ev)
        return ev

    # ── Restore ────────────────────────────────────────────────────────────

    async def _handle_restore(self, req: RestoreRequest) -> KernelEvent:
        row = self._storage.query_one(
            "SELECT * FROM checkpoints WHERE checkpoint_id = ?",
            (req.checkpoint_id,),
        )
        if row is None:
            raise ValueError(f"Checkpoint not found: {req.checkpoint_id}")

        ev = KernelEvent(
            pid=row["pid"],
            event_type="CHECKPOINT_RESTORED",
            payload={
                "checkpoint_id": req.checkpoint_id,
                "journal_offset": row["journal_offset"],
            },
        )
        self._journal.append_event(ev)
        return ev

    # ── Exit ───────────────────────────────────────────────────────────────

    async def _handle_exit(self, req: ExitRequest) -> KernelEvent:
        # Release all leases
        self._lease_service.release_all_for_pid(req.pid)
        # Transition to EXITED
        self._process_service.transition(req.pid, ProcessState.EXITED)

        pcb = self._process_service.get_process(req.pid)
        if pcb:
            pcb.exit_code = req.exit_code
            pcb.result_ref = req.result_ref
            self._process_service._upsert_projection(pcb)

        ev = KernelEvent(
            pid=req.pid,
            event_type="PROCESS_EXITED",
            payload={"exit_code": req.exit_code, "result_ref": req.result_ref},
        )
        self._journal.append_event(ev)
        return ev
