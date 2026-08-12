"""System Call Dispatcher — routes KernelRequests to appropriate services.

Each syscall type has a handler that:
1. Validates the request
2. Performs capability check (if needed)
3. Executes the operation
4. Returns a KernelEvent (or list of events)
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import suppress

from lhos.agent_os.kernel.models import (
    AcquireResourceRequest,
    ActionState,
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
        # ``pid=""`` is reserved for the trusted kernel bootstrap path used by
        # AgentKernel.spawn().  A process-originated request is rebound to the
        # executing PCB before it reaches the dispatcher, so its authenticated
        # caller is req.pid.  Do not let request payload choose a different
        # parent or mint a child with the kernel's default/full capabilities.
        parent_capabilities = None
        if req.pid:
            parent = self._process_service.get_process(req.pid)
            if parent is None or req.parent_pid not in (None, req.pid):
                ev = KernelEvent(
                    pid=req.pid,
                    event_type="SPAWN_FAILED",
                    payload={"reason": "invalid_parent"},
                )
                self._journal.append_event(ev)
                return ev
            parent_pid = req.pid
            parent_cap_set = self._capability_service.get_capability_set(req.pid)
            parent_capabilities = (
                [cap.model_copy(deep=True) for cap in parent_cap_set.capabilities]
                if parent_cap_set is not None
                else []
            )
            # Namespace and resource-group membership are process security
            # context, not child-selected spawn parameters.  Bind both to the
            # authenticated parent just like the capability set.
            namespace_id = parent.namespace_id
            resource_group_id = parent.resource_group_id
        else:
            if req.parent_pid is not None:
                ev = KernelEvent(
                    pid="",
                    event_type="SPAWN_FAILED",
                    payload={"reason": "invalid_parent"},
                )
                self._journal.append_event(ev)
                return ev
            parent_pid = None
            namespace_id = req.namespace_id or "default"
            resource_group_id = req.resource_group_id

        pcb = self._process_service.spawn(
            program_id=req.program_id,
            namespace_id=namespace_id,
            capability_set_id=None,
            parent_pid=parent_pid,
            resource_group_id=resource_group_id,
            program_state=req.initial_state,
        )
        # Kernel-created root processes retain the historical default template.
        # Child processes inherit the authenticated parent's current set.  The
        # request's legacy ``capabilities`` string field is intentionally not an
        # authority source: it cannot be used to escalate a child.
        from lhos.agent_os.services.capability_service import DEFAULT_CAPABILITIES

        cap_set = self._capability_service.create_capability_set(
            pcb.pid,
            capabilities=(
                DEFAULT_CAPABILITIES["full"] if parent_capabilities is None else parent_capabilities
            ),
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
        # Capability checks are admission preconditions.  Validate the device
        # and the complete resource bundle before creating an Action or asking
        # LeaseService to record leases/waiters, so denial is side-effect free.
        resource = f"device:{req.device_type}"
        self._capability_service.enforce(req.pid, resource, "invoke")
        for claim in req.resource_claims:
            self._capability_service.enforce(
                req.pid,
                claim["resource_id"],
                "acquire",
            )

        # Submit
        acb = self._action_service.submit(
            pid=req.pid,
            device_type=req.device_type,
            operation=req.operation,
            arguments=req.arguments,
            side_effect_class=req.side_effect_class,
            idempotency_key=req.idempotency_key,
            timeout_seconds=req.timeout_seconds,
            resource_claims=req.resource_claims,
        )

        # Resource acquisition is part of admission. A failed acquisition must
        # never leave a dispatchable ADMITTED action behind.
        try:
            leases = (
                self._lease_service.atomic_acquire(req.pid, req.resource_claims)
                if req.resource_claims
                else []
            )
        except Exception as exc:
            self._action_service.reject(acb.action_id, reason=str(exc))
            self._lease_service.clear_waiters_for_pid(req.pid)
            raise

        lease_ids = [lease.lease_id for lease in leases]
        fencing_tokens = {lease.resource_id: lease.fencing_token for lease in leases}
        try:
            self._action_service.admit(acb.action_id)
            self._action_service.mark_intent_durable(
                acb.action_id,
                lease_ids,
                fencing_tokens=fencing_tokens,
            )
        except Exception as exc:
            # Admission is a mini-transaction spanning ActionService and
            # LeaseService. Compensate whichever state was durably reached so
            # no SUBMITTED/ADMITTED action can later be dispatched without a
            # complete resource contract. Cleanup is best effort and must not
            # replace the original admission exception.
            try:
                current = self._action_service.get_action(acb.action_id)
                if current is not None:
                    error = {
                        "reason": "admission_failed",
                        "detail": type(exc).__name__,
                    }
                    if current.state == ActionState.SUBMITTED:
                        self._action_service.reject(acb.action_id, reason=str(exc))
                    elif current.state == ActionState.ADMITTED:
                        self._action_service.fail(acb.action_id, error=error)
            except Exception:
                pass
            finally:
                with suppress(Exception):
                    self._lease_service.release(lease_ids)
                with suppress(Exception):
                    self._lease_service.clear_waiters_for_pid(req.pid)
            raise

        return KernelEvent(
            pid=req.pid,
            event_type="ACTION_READY_FOR_DISPATCH",
            payload={"action_id": acb.action_id, "device_type": req.device_type},
        )

    # ── Inspect Action ─────────────────────────────────────────────────────

    async def _handle_inspect_action(self, req: InspectActionRequest) -> KernelEvent:
        acb = self._action_service.get_action(req.action_id)
        if acb is None or acb.pid != req.pid:
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
        if acb is None or acb.pid != req.pid:
            return KernelEvent(
                pid=req.pid,
                event_type="ACTION_CANCEL_FAILED",
                payload={"action_id": req.action_id, "reason": "not_found"},
            )
        self._action_service.cancel(req.action_id)
        # Release leases
        self._lease_service.release(acb.lease_ids)
        self._lease_service.clear_waiters_for_pid(acb.pid)
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
        # Validate the complete bundle before releasing anything. Possession
        # of a lease ID must not grant control over another process's resource,
        # and mixed owned/foreign requests must not partially succeed.
        leases = [self._lease_service.get_lease(lease_id) for lease_id in req.lease_ids]
        if any(lease is None or lease.owner_pid != req.pid for lease in leases):
            return KernelEvent(
                pid=req.pid,
                event_type="RESOURCE_RELEASE_FAILED",
                payload={"count": 0, "reason": "not_found"},
            )

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
            "SELECT * FROM checkpoints WHERE checkpoint_id = ? AND pid = ?",
            (req.checkpoint_id, req.pid),
        )
        if row is None:
            ev = KernelEvent(
                pid=req.pid,
                event_type="CHECKPOINT_RESTORE_FAILED",
                payload={"checkpoint_id": req.checkpoint_id, "reason": "not_found"},
            )
            self._journal.append_event(ev)
            return ev

        ev = KernelEvent(
            pid=req.pid,
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
        self._lease_service.clear_waiters_for_pid(req.pid)
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
