"""Agent Kernel — the main event loop.

tick():
1. Reclaim expired leases
2. Deliver pending signals
3. Recover incomplete actions
4. Detect deadlocks
5. Schedule ready processes (FIFO)
6. Run one step per ready process
"""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from lhos.agent_os.drivers.base import DriverInspect, DriverResult
from lhos.agent_os.drivers.mock_device import MockDeviceDriver
from lhos.agent_os.drivers.mock_model import MockModelDriver
from lhos.agent_os.kernel.dispatcher import SyscallDispatcher
from lhos.agent_os.kernel.errors import IllegalStateTransition, LeaseAcquisitionFailed
from lhos.agent_os.kernel.models import (
    ActionState,
    Clock,
    ExitRequest,
    KernelEvent,
    KernelRequest,
    ProcessControlBlock,
    ProcessState,
    RecoveryPolicy,
    SideEffectClass,
    SubmitActionRequest,
    WaitRequest,
)
from lhos.agent_os.programs.base import ProgramStepResult
from lhos.agent_os.services.action_service import ActionService
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.services.process_service import ProcessService
from lhos.agent_os.services.signal_service import SignalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


class FIFOScheduler:
    """Simple FIFO scheduler — selects ready processes in creation order."""

    def select(self, ready: list[ProcessControlBlock]) -> list[ProcessControlBlock]:
        return sorted(ready, key=lambda p: (p.priority, p.created_at))


class AgentKernel:
    """Minimal Agent OS Kernel."""

    def __init__(
        self,
        storage: SQLiteStorage,
        journal: JournalService,
        process_service: ProcessService,
        action_service: ActionService,
        capability_service: CapabilityService,
        lease_service: LeaseService,
        signal_service: SignalService,
        dispatcher: SyscallDispatcher,
        clock: Clock,
        *,
        strict_effect_contracts: bool = False,
    ):
        self._storage = storage
        self._journal = journal
        self._process_service = process_service
        self._action_service = action_service
        self._capability_service = capability_service
        self._lease_service = lease_service
        self._signal_service = signal_service
        self._dispatcher = dispatcher
        self._clock = clock
        self._strict_effect_contracts = bool(strict_effect_contracts)

        # Program registry: pid → AgentProgram
        self._programs: dict[str, Any] = {}
        # Driver registry: device_type → driver
        self._drivers: dict[str, Any] = {}

        self.scheduler = FIFOScheduler()

        # Register default mock drivers
        self.register_driver("model/mock", MockModelDriver())
        self.register_driver("tool/mock", MockDeviceDriver())

    def close(self) -> None:
        """Release the kernel's storage handle."""
        self._storage.close()

    def register_driver(self, device_type: str, driver: Any) -> None:
        self._drivers[device_type] = driver

    def get_driver(self, device_type: str) -> Any:
        return self._drivers.get(device_type)

    def register_program(self, pid: str, program: Any) -> None:
        self._programs[pid] = program

    # ── Main loop ──────────────────────────────────────────────────────────

    async def tick(self) -> None:
        """One kernel tick."""
        self._clock.tick()

        # 1. Reclaim expired leases
        self._lease_service.reclaim_expired(self._clock.now())

        # 2. Deliver pending signals
        self._signal_service.deliver_pending()

        # 3. Recover incomplete actions
        await self.recover_incomplete_actions()

        # 4. Detect deadlocks
        deadlocks = self._lease_service.detect_deadlocks()
        for cycle in deadlocks:
            await self._recover_deadlock(cycle)

        # 5. Dispatch pending actions to drivers
        await self._dispatch_pending_actions()

        # 6. Deliver signals again (after action completion)
        self._signal_service.deliver_pending()

        # 7. Schedule and run ready processes
        ready = self._process_service.list_ready()
        for process in self.scheduler.select(ready):
            await self._run_process_step(process)

    # ── Run a single process step ──────────────────────────────────────────

    async def _run_process_step(self, pcb: ProcessControlBlock) -> None:
        """Run one step of a process."""
        if pcb.state != ProcessState.READY:
            return

        program = self._programs.get(pcb.pid)
        if program is None:
            # No program registered — skip
            return

        # READY → RUNNING
        try:
            self._process_service.transition(pcb.pid, ProcessState.RUNNING)
        except IllegalStateTransition:
            return

        # Re-fetch PCB so we have the current state (RUNNING)
        current_pcb: ProcessControlBlock | None = self._process_service.get_process(pcb.pid)
        if current_pcb is None:
            return
        # Use the freshly fetched PCB
        pcb = current_pcb

        # Get program state
        state = self._process_service.get_program_state(pcb.pid)
        state["pid"] = pcb.pid

        # Get last event for this process (if any).  Indexed lookup instead
        # of the previous 100-event window scan: the window dropped the
        # process's own events entirely once more than 100 events interleaved
        # between two of its steps (the cursor jumps to next_offset
        # unconditionally), while this query is exact and O(log n).
        last_event = self._journal.read_last_for_pid_since(
            pcb.pid, pcb.event_cursor
        )

        # Run step
        try:
            result: ProgramStepResult = await program.step(state, last_event)
        except Exception as e:
            # Program step failed
            ev = KernelEvent(
                pid=pcb.pid,
                event_type="PROGRAM_STEP_FAILED",
                payload={"error": str(e), "type": type(e).__name__},
            )
            self._journal.append_event(ev)
            # Process → FAILED
            self._process_service.transition(pcb.pid, ProcessState.FAILED)
            self._lease_service.release_all_for_pid(pcb.pid)
            return

        # Save program state
        self._process_service.save_program_state(pcb.pid, result.new_state)

        # Update event cursor
        pcb.event_cursor = self._journal.next_offset()
        self._process_service._upsert_projection(pcb)

        # Handle the request
        if result.request is not None:
            req = result.request
            # The currently executing PCB is the authenticated caller. Program
            # code controls the request payload, so its pid field is untrusted
            # and must not be allowed to impersonate another process.
            if not isinstance(req, KernelRequest):
                ev = KernelEvent(
                    pid=pcb.pid,
                    event_type="PROGRAM_REQUEST_REJECTED",
                    payload={
                        "reason": "invalid_request_type",
                        "type": type(req).__name__,
                    },
                )
                self._journal.append_event(ev)
                self._process_service.transition(pcb.pid, ProcessState.FAILED)
                self._lease_service.release_all_for_pid(pcb.pid)
                return
            req = req.model_copy(update={"pid": pcb.pid})
            if isinstance(req, ExitRequest) or (
                hasattr(req, "request_type") and req.request_type == "exit"
            ):
                # Handle exit directly
                await self._dispatcher.dispatch(req)
            elif isinstance(req, SubmitActionRequest) or (
                hasattr(req, "request_type") and req.request_type == "submit_action"
            ):
                # Submit action
                try:
                    dispatch_event = await self._dispatcher.dispatch(req)
                except LeaseAcquisitionFailed:
                    # Resource contention is a normal admission failure. The
                    # dispatcher has already made the action terminal.
                    self._process_service.transition(pcb.pid, ProcessState.READY)
                    return
                if dispatch_event is not None and dispatch_event.event_type == "ACTION_REJECTED":
                    # Strict effect-contract rejection creates no Action
                    # projection. Do not infer a wait target from an older
                    # action belonging to this process.
                    self._process_service.transition(pcb.pid, ProcessState.READY)
                    return
                # Process goes BLOCKED (waiting for action)
                # The action_id is in the program state
                action_id = self._get_last_action_id(pcb.pid)
                if action_id:
                    self._process_service.transition(
                        pcb.pid,
                        ProcessState.BLOCKED,
                        # Every terminal outcome emitted by the driver/recovery
                        # paths must wake the owner. Waiting only for COMPLETED
                        # strands processes forever after FAILED/UNCERTAIN.
                        wait_condition={
                            "signal_types": [
                                "ACTION_COMPLETED",
                                "ACTION_FAILED",
                                "ACTION_UNCERTAIN",
                            ],
                            "action_id": action_id,
                        },
                    )
                else:
                    # No action to wait for → back to READY
                    self._process_service.transition(pcb.pid, ProcessState.READY)
            elif isinstance(req, WaitRequest) or (
                hasattr(req, "request_type") and req.request_type == "wait"
            ):
                await self._dispatcher.dispatch(req)
            else:
                # Other requests → dispatch and go back to READY
                await self._dispatcher.dispatch(req)
                self._process_service.transition(pcb.pid, ProcessState.READY)
        else:
            # No request → back to READY (or exit if exit_code set)
            if result.exit_code:
                # Program wants to exit
                exit_req = ExitRequest(
                    pid=pcb.pid, exit_code=result.exit_code, result_ref=result.result_ref
                )
                await self._dispatcher.dispatch(exit_req)
            else:
                self._process_service.transition(pcb.pid, ProcessState.READY)

    def _get_last_action_id(self, pid: str) -> str | None:
        """Get the most recently submitted action_id for a pid."""
        actions = self._action_service.list_by_pid(pid)
        if not actions:
            return None
        return actions[-1].action_id

    def _fail_action_if_running(
        self,
        action_id: str,
        error: dict[str, Any] | None = None,
    ) -> bool:
        """Conditionally fail an in-flight action after an async race.

        Driver completion/recovery runs concurrently with cancellation,
        timeout, or another recovery pass.  A stale callback must not attempt
        to overwrite a terminal Action or emit a contradictory signal.
        """
        return self._action_service.fail_if_running(action_id, error)

    def _mark_action_uncertain_if_running(
        self,
        action_id: str,
        detail: dict[str, Any] | None = None,
    ) -> bool:
        return self._action_service.mark_uncertain_if_running(action_id, detail)

    def _action_is_running(self, action_id: str) -> bool:
        """Return whether an action is still eligible for an external retry.

        This check closes the common cancellation/recovery window between an
        ``unknown`` driver result and a PURE-action retry.  It is deliberately
        read-only; the terminal transition itself remains linearized by the
        ActionService conditional methods.
        """
        action = self._action_service.get_action(action_id)
        return action is not None and action.state == ActionState.RUNNING

    def _retry_reservation_is_owned_elsewhere(
        self,
        action_id: str,
        reservation_error: str | None,
    ) -> bool:
        """Return whether a durable retry reservation already has an owner.

        Concurrent recovery workers can inspect the same RUNNING Action before
        either reserves its single retry. The transaction winner consumes the
        durable budget; the loser observes ``retry_budget_exhausted``. Re-read
        the projection before treating that response as benign so corruption
        or a different reservation failure still follows the fail-closed path.
        The winner may already have terminalized the Action by the time this
        read happens, but its durable reservation still proves loser status.
        """
        if reservation_error != "retry_budget_exhausted":
            return False

        current = self._action_service.get_action(action_id)
        if current is None:
            return False
        try:
            side_effect = SideEffectClass(current.side_effect_class)
            policy = RecoveryPolicy(current.recovery_policy)
        except (TypeError, ValueError):
            return False
        return (
            side_effect == SideEffectClass.PURE
            and policy == RecoveryPolicy.RETRY
            and current.retry_count == 1
        )

    @staticmethod
    def _unknown_recovery_mode(acb: Any) -> str:
        """Return the fail-closed disposition for an unknown driver outcome.

        ``recovery_policy`` is an explicit *permission* for recovery, not a
        blanket override of side-effect safety.  A blind redispatch is only
        safe for a PURE action.  ``INSPECT`` is safe for every class because it
        is a read-only reconciliation operation; all other classes require an
        explicit inspect policy before the Kernel will query the sink.

        Durable rows may contain values written by an older/corrupt process.
        Parsing failures therefore return ``uncertain`` instead of silently
        defaulting to ``retry``.
        """
        try:
            policy = RecoveryPolicy(acb.recovery_policy)
            side_effect = SideEffectClass(acb.side_effect_class)
        except (TypeError, ValueError):
            return "uncertain"

        if policy == RecoveryPolicy.INSPECT:
            return "inspect"
        if policy in (RecoveryPolicy.UNCERTAIN, RecoveryPolicy.DEAD_LETTER):
            return "uncertain"
        if side_effect == SideEffectClass.PURE and policy == RecoveryPolicy.RETRY:
            return "retry" if acb.retry_count < 1 else "inspect"
        if side_effect == SideEffectClass.IDEMPOTENT:
            # Idempotency is a sink contract, not proof that this particular
            # invocation was observed.  Reconcile first instead of blindly
            # dispatching a second request.
            return "inspect"
        return "uncertain"

    @staticmethod
    def _recovery_detail(acb: Any, detail: dict[str, Any] | None = None) -> dict[str, Any]:
        """Add durable classification metadata to an uncertainty diagnostic."""
        result = dict(detail or {})
        try:
            result.setdefault("side_effect_class", SideEffectClass(acb.side_effect_class).value)
        except (TypeError, ValueError):
            result.setdefault("side_effect_class", "unknown")
        try:
            policy = RecoveryPolicy(acb.recovery_policy)
            result.setdefault("recovery_policy", policy.value)
            if policy == RecoveryPolicy.DEAD_LETTER:
                result.setdefault("recovery_disposition", "dead_letter")
        except (TypeError, ValueError):
            result.setdefault("recovery_policy", "malformed")
            result.setdefault("recovery_disposition", "uncertain")
        return result

    def _mark_uncertain_and_release(
        self,
        acb: Any,
        detail: dict[str, Any] | None = None,
    ) -> bool:
        """Conditionally terminalize an action and release its old lease set."""
        marked = self._mark_action_uncertain_if_running(
            acb.action_id,
            detail=self._recovery_detail(acb, detail),
        )
        self._lease_service.release(acb.lease_ids)
        if marked:
            self._signal_service.send(
                target_pid=acb.pid,
                signal_type="ACTION_UNCERTAIN",
                source_pid="kernel",
                payload={
                    "action_id": acb.action_id,
                    "detail": self._recovery_detail(acb, detail),
                },
            )
        return marked

    async def _inspect_after_unknown(
        self,
        acb: Any,
        driver: Any,
        *,
        reason: str,
    ) -> None:
        """Reconcile an uncertain action without dispatching a new effect."""
        if not self._action_is_running(acb.action_id):
            self._lease_service.release(acb.lease_ids)
            return
        try:
            inspect: DriverInspect = await driver.inspect(acb.action_id)
        except Exception as exc:
            self._mark_uncertain_and_release(
                acb,
                {
                    "reason": "inspect_failed",
                    "source": reason,
                    "error": str(exc),
                    "type": type(exc).__name__,
                },
            )
            return

        if inspect.status == "completed":
            committed, fence_error = self._action_service.commit_if_fenced(
                acb.action_id,
                result=inspect.output,
                now=self._clock.now(),
            )
            if not committed:
                stale_error = {
                    "reason": "stale_fenced_completion",
                    "source": reason,
                    "detail": fence_error,
                }
                failed = self._fail_action_if_running(acb.action_id, error=stale_error)
                self._lease_service.release(acb.lease_ids)
                if failed:
                    self._signal_service.send(
                        target_pid=acb.pid,
                        signal_type="ACTION_FAILED",
                        source_pid="kernel",
                        payload={"action_id": acb.action_id, "error": stale_error},
                    )
                return
            self._lease_service.release(acb.lease_ids)
            self._signal_service.send(
                target_pid=acb.pid,
                signal_type="ACTION_COMPLETED",
                source_pid="kernel",
                payload={"action_id": acb.action_id, "result": inspect.output},
            )
        elif inspect.status == "failed":
            error = inspect.error or {"reason": "inspection_failed"}
            failed = self._fail_action_if_running(acb.action_id, error=error)
            self._lease_service.release(acb.lease_ids)
            if failed:
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_FAILED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "error": error},
                )
        elif inspect.status == "unknown":
            self._mark_uncertain_and_release(
                acb,
                {"reason": "inspection_unknown", "source": reason},
            )
        # ``running`` is an affirmative in-flight observation. Keep the
        # Action and lease bundle for a later recovery pass.

    # ── Dispatch pending actions to drivers ────────────────────────────────

    async def _dispatch_pending_actions(self) -> None:
        """Find actions in ADMITTED state (intent durable) and dispatch to driver."""
        # Actions that have been admitted and have intent durable
        # We need to check for ACTION_READY_FOR_DISPATCH events
        # that haven't been dispatched yet.
        # Simplest: look for actions in ADMITTED state.
        admitted = [
            acb
            for acb in self._action_service.list_non_terminal()
            if acb.state == ActionState.ADMITTED
        ]

        for acb in admitted:
            valid_contract, contract_error = self._lease_service.validate_action_contract(
                acb.pid,
                acb.resource_claims,
                acb.lease_ids,
                self._clock.now(),
            )
            if not valid_contract:
                error = {
                    "reason": "invalid_resource_contract",
                    "detail": contract_error,
                }
                self._action_service.fail(acb.action_id, error=error)
                self._lease_service.release(acb.lease_ids)
                self._lease_service.clear_waiters_for_pid(acb.pid)
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_FAILED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "error": error},
                )
                continue

            driver = self._drivers.get(acb.device_type)
            if driver is None:
                # No driver — fail the action
                error = {"reason": "no_driver"}
                self._action_service.fail(acb.action_id, error=error)
                self._lease_service.release(acb.lease_ids)
                self._lease_service.clear_waiters_for_pid(acb.pid)
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_FAILED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "error": error},
                )
                continue

            # Dispatch to driver
            self._action_service.dispatch(acb.action_id)
            # Capture the exact lease/fencing contract after the RUNNING
            # transition. This snapshot is persisted at admission and is
            # rechecked atomically before every terminal commit.
            fencing_tokens = dict(acb.fencing_tokens)

            try:
                result: DriverResult = await driver.dispatch(
                    acb.action_id,
                    acb.operation,
                    acb.arguments,
                )
            except Exception as e:
                # A driver exception is not proof that no external effect
                # happened: a sink may have committed its effect and failed
                # before returning/acknowledging it.  PURE actions are the
                # only class for which a single recovery redispatch is safe by
                # contract.  Every other class fails closed to UNCERTAIN and
                # requires reconciliation rather than a blind redispatch.
                initial_error = {
                    "reason": "driver_dispatch_exception",
                    "error": str(e),
                    "type": type(e).__name__,
                    "side_effect_class": acb.side_effect_class.value,
                }
                recovery_mode = self._unknown_recovery_mode(acb)
                if recovery_mode == "retry":
                    # A failed call can still have an in-flight external
                    # result.  Before issuing the one permitted PURE retry,
                    # re-read the durable Action and validate its current
                    # lease/fencing contract.  This closes the common
                    # cancellation/lease-supersession window and prevents a
                    # stale owner from dispatching a new effect.
                    retry_lease_ids = list(acb.lease_ids)
                    current = self._action_service.get_action(acb.action_id)
                    if current is None or current.state != ActionState.RUNNING:
                        self._lease_service.release(retry_lease_ids)
                        continue

                    retry_reserved, retry_contract_error = (
                        self._action_service.reserve_retry_if_fenced(
                            current.action_id,
                            now=self._clock.now(),
                        )
                    )
                    if not retry_reserved:
                        if self._retry_reservation_is_owned_elsewhere(
                            current.action_id,
                            retry_contract_error,
                        ):
                            continue
                        stale_retry_error = self._recovery_detail(
                            current,
                            {
                                "reason": "retry_fencing_contract_invalid",
                                "source": "dispatch_exception",
                                "detail": retry_contract_error,
                                "initial_error": initial_error,
                                "fencing_tokens": dict(current.fencing_tokens),
                            },
                        )
                        terminalized = self._mark_action_uncertain_if_running(
                            acb.action_id,
                            detail=stale_retry_error,
                        )
                        self._lease_service.release(retry_lease_ids)
                        if terminalized:
                            self._signal_service.send(
                                target_pid=acb.pid,
                                signal_type="ACTION_UNCERTAIN",
                                source_pid="kernel",
                                payload={
                                    "action_id": acb.action_id,
                                    "detail": stale_retry_error,
                                },
                            )
                        continue

                    # Re-check the terminal state after the potentially
                    # expensive contract read.  This is still intentionally
                    # conditional: the ActionService fencing commit remains
                    # the final authority if cancellation wins concurrently.
                    if not self._action_is_running(acb.action_id):
                        self._lease_service.release(retry_lease_ids)
                        continue
                    retry_authorized, retry_validation_error = (
                        self._action_service.validate_reserved_retry_if_fenced(
                            acb.action_id,
                            now=self._clock.now(),
                        )
                    )
                    if not retry_authorized:
                        retry_error = self._recovery_detail(
                            current,
                            {
                                "reason": "retry_dispatch_contract_invalid",
                                "source": "dispatch_exception",
                                "detail": retry_validation_error,
                                "initial_error": initial_error,
                            },
                        )
                        terminalized = self._mark_action_uncertain_if_running(
                            acb.action_id,
                            detail=retry_error,
                        )
                        self._lease_service.release(retry_lease_ids)
                        if terminalized:
                            self._signal_service.send(
                                target_pid=acb.pid,
                                signal_type="ACTION_UNCERTAIN",
                                source_pid="kernel",
                                payload={
                                    "action_id": acb.action_id,
                                    "detail": retry_error,
                                },
                            )
                        continue

                    try:
                        retry_result = await driver.dispatch(
                            acb.action_id,
                            acb.operation,
                            acb.arguments,
                        )
                    except Exception as retry_exc:
                        retry_error = self._recovery_detail(
                            current,
                            {
                                "reason": "retry_failed",
                                "source": "dispatch_exception",
                                "error": str(retry_exc),
                                "type": type(retry_exc).__name__,
                                "initial_error": initial_error,
                            },
                        )
                        terminalized = self._mark_action_uncertain_if_running(
                            acb.action_id,
                            detail=retry_error,
                        )
                        self._lease_service.release(retry_lease_ids)
                        if terminalized:
                            self._signal_service.send(
                                target_pid=acb.pid,
                                signal_type="ACTION_UNCERTAIN",
                                source_pid="kernel",
                                payload={
                                    "action_id": acb.action_id,
                                    "detail": retry_error,
                                },
                            )
                        continue

                    if retry_result.status == "completed":
                        committed, fence_error = self._action_service.commit_if_fenced(
                            acb.action_id,
                            result=retry_result.output,
                            now=self._clock.now(),
                        )
                        if not committed:
                            stale_retry_error = self._recovery_detail(
                                current,
                                {
                                    "reason": "stale_fenced_completion",
                                    "source": "dispatch_exception_retry",
                                    "detail": fence_error,
                                    "initial_error": initial_error,
                                    "fencing_tokens": dict(current.fencing_tokens),
                                },
                            )
                            terminalized = self._fail_action_if_running(
                                acb.action_id,
                                error=stale_retry_error,
                            )
                            self._lease_service.release(retry_lease_ids)
                            if terminalized:
                                self._signal_service.send(
                                    target_pid=acb.pid,
                                    signal_type="ACTION_FAILED",
                                    source_pid="kernel",
                                    payload={
                                        "action_id": acb.action_id,
                                        "error": stale_retry_error,
                                    },
                                )
                            continue
                        self._lease_service.release(retry_lease_ids)
                        self._signal_service.send(
                            target_pid=acb.pid,
                            signal_type="ACTION_COMPLETED",
                            source_pid="kernel",
                            payload={
                                "action_id": acb.action_id,
                                "result": retry_result.output,
                            },
                        )
                    elif retry_result.status == "failed":
                        retry_error = self._recovery_detail(
                            current,
                            {
                                **(retry_result.error or {}),
                                "reason": (retry_result.error or {}).get(
                                    "reason",
                                    "retry_failed",
                                ),
                                "source": "dispatch_exception_retry",
                                "initial_error": initial_error,
                            },
                        )
                        terminalized = self._mark_action_uncertain_if_running(
                            acb.action_id,
                            detail=retry_error,
                        )
                        self._lease_service.release(retry_lease_ids)
                        if terminalized:
                            self._signal_service.send(
                                target_pid=acb.pid,
                                signal_type="ACTION_UNCERTAIN",
                                source_pid="kernel",
                                payload={
                                    "action_id": acb.action_id,
                                    "detail": retry_error,
                                },
                            )
                    elif retry_result.status == "unknown":
                        retry_unknown = self._recovery_detail(
                            current,
                            {
                                "reason": "retry_unknown",
                                "source": "dispatch_exception_retry",
                                "initial_error": initial_error,
                            },
                        )
                        terminalized = self._mark_action_uncertain_if_running(
                            acb.action_id,
                            detail=retry_unknown,
                        )
                        self._lease_service.release(retry_lease_ids)
                        if terminalized:
                            self._signal_service.send(
                                target_pid=acb.pid,
                                signal_type="ACTION_UNCERTAIN",
                                source_pid="kernel",
                                payload={
                                    "action_id": acb.action_id,
                                    "detail": retry_unknown,
                                },
                            )
                    # ``running`` means the retry is still in flight. Preserve
                    # RUNNING and its leases for the recovery inspector.
                    continue
                elif recovery_mode == "inspect":
                    await self._inspect_after_unknown(
                        acb,
                        driver,
                        reason="dispatch_exception",
                    )
                    continue
                else:
                    error = self._recovery_detail(acb, initial_error)
                    terminalized = self._mark_action_uncertain_if_running(
                        acb.action_id,
                        detail=error,
                    )
                    signal_type = "ACTION_UNCERTAIN"

                # Release leases regardless of which terminal transition won.
                self._lease_service.release(acb.lease_ids)
                if terminalized:
                    self._signal_service.send(
                        target_pid=acb.pid,
                        signal_type=signal_type,
                        source_pid="kernel",
                        payload=(
                            {"action_id": acb.action_id, "error": error}
                            if signal_type == "ACTION_FAILED"
                            else {"action_id": acb.action_id, "detail": error}
                        ),
                    )
                continue

            # Process result based on side effect class
            if result.status == "completed":
                committed, fence_error = self._action_service.commit_if_fenced(
                    acb.action_id,
                    result=result.output,
                    now=self._clock.now(),
                )
                if not committed:
                    stale_error: dict[str, Any] = {
                        "reason": "stale_fenced_completion",
                        "detail": fence_error,
                        "fencing_tokens": fencing_tokens,
                    }
                    failed = self._fail_action_if_running(acb.action_id, error=stale_error)
                    self._lease_service.release(acb.lease_ids)
                    if failed:
                        self._signal_service.send(
                            target_pid=acb.pid,
                            signal_type="ACTION_FAILED",
                            source_pid="kernel",
                            payload={
                                "action_id": acb.action_id,
                                "error": {
                                    "reason": "stale_fenced_completion",
                                    "detail": fence_error,
                                },
                            },
                        )
                    continue
                self._lease_service.release(acb.lease_ids)
                # Send signal
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_COMPLETED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "result": result.output},
                )
            elif result.status == "failed":
                error = result.error or {}
                failed = self._fail_action_if_running(acb.action_id, error=error)
                self._lease_service.release(acb.lease_ids)
                if failed:
                    self._signal_service.send(
                        target_pid=acb.pid,
                        signal_type="ACTION_FAILED",
                        source_pid="kernel",
                        payload={"action_id": acb.action_id, "error": error},
                    )
            elif result.status == "unknown":
                # Side effect may or may not have happened.  The durable
                # recovery policy decides whether this invocation may be
                # retried, inspected, or must remain fail-closed uncertain.
                recovery_mode = self._unknown_recovery_mode(acb)
                if recovery_mode == "retry":
                    retry_reserved, retry_reservation_error = (
                        self._action_service.reserve_retry_if_fenced(
                            acb.action_id,
                            now=self._clock.now(),
                        )
                    )
                    if not retry_reserved:
                        # Another recovery worker may have atomically consumed
                        # the one retry reservation.  That worker still owns
                        # the RUNNING Action and its lease bundle; do not
                        # terminalize the Action or release its leases here.
                        if self._retry_reservation_is_owned_elsewhere(
                            acb.action_id,
                            retry_reservation_error,
                        ):
                            continue
                        self._mark_uncertain_and_release(
                            acb,
                            {
                                "reason": "retry_reservation_failed",
                                "detail": retry_reservation_error,
                            },
                        )
                        continue
                    retry_authorized, retry_validation_error = (
                        self._action_service.validate_reserved_retry_if_fenced(
                            acb.action_id,
                            now=self._clock.now(),
                        )
                    )
                    if not retry_authorized:
                        self._mark_uncertain_and_release(
                            acb,
                            {
                                "reason": "retry_dispatch_contract_invalid",
                                "detail": retry_validation_error,
                            },
                        )
                        continue
                    try:
                        retry_result = await driver.dispatch(
                            acb.action_id,
                            acb.operation,
                            acb.arguments,
                        )
                    except Exception as exc:
                        self._mark_uncertain_and_release(
                            acb,
                            {"reason": "retry_failed", "error": str(exc)},
                        )
                        continue
                    if retry_result.status == "completed":
                        committed, fence_error = self._action_service.commit_if_fenced(
                            acb.action_id,
                            result=retry_result.output,
                            now=self._clock.now(),
                        )
                        if not committed:
                            error = {
                                "reason": "stale_fenced_completion",
                                "detail": fence_error,
                            }
                            failed = self._fail_action_if_running(
                                acb.action_id,
                                error=error,
                            )
                            self._lease_service.release(acb.lease_ids)
                            if failed:
                                self._signal_service.send(
                                    target_pid=acb.pid,
                                    signal_type="ACTION_FAILED",
                                    source_pid="kernel",
                                    payload={"action_id": acb.action_id, "error": error},
                                )
                            continue
                        self._lease_service.release(acb.lease_ids)
                        self._signal_service.send(
                            target_pid=acb.pid,
                            signal_type="ACTION_COMPLETED",
                            source_pid="kernel",
                            payload={"action_id": acb.action_id, "result": retry_result.output},
                        )
                    elif retry_result.status == "failed":
                        self._mark_uncertain_and_release(
                            acb,
                            {
                                **(retry_result.error or {}),
                                "reason": (retry_result.error or {}).get(
                                    "reason",
                                    "retry_failed",
                                ),
                            },
                        )
                    elif retry_result.status == "unknown":
                        self._mark_uncertain_and_release(
                            acb,
                            {"reason": "retry_unknown"},
                        )
                    # ``running`` means the retry is still in flight. Preserve
                    # RUNNING and its leases for the recovery inspector.
                elif recovery_mode == "inspect":
                    await self._inspect_after_unknown(
                        acb,
                        driver,
                        reason="dispatch_unknown",
                    )
                else:
                    self._mark_uncertain_and_release(
                        acb,
                        {
                            "reason": "recovery_policy_forbids_blind_retry",
                            "disposition": "uncertain",
                        },
                    )

    # ── Recover incomplete actions ─────────────────────────────────────────

    async def recover_incomplete_actions(self) -> None:
        """Recover actions that are in RUNNING state (crash recovery)."""
        running = [
            acb
            for acb in self._action_service.list_non_terminal()
            if acb.state == ActionState.RUNNING
        ]

        for acb in running:
            driver = self._drivers.get(acb.device_type)
            if driver is None:
                error = {"reason": "no_driver"}
                failed = self._fail_action_if_running(acb.action_id, error=error)
                self._lease_service.release(acb.lease_ids)
                if failed:
                    self._signal_service.send(
                        target_pid=acb.pid,
                        signal_type="ACTION_FAILED",
                        source_pid="kernel",
                        payload={"action_id": acb.action_id, "error": error},
                    )
                continue

            # ``running`` was collected from a projection snapshot.  A
            # cancellation or timeout may have won since that snapshot was
            # read, so avoid making a stale external inspect call when the
            # Action is already terminal.  This narrows (but cannot entirely
            # eliminate) the check-to-driver-call window; drivers still need a
            # cancellable/fencing-aware contract for strict external fencing.
            if not self._action_is_running(acb.action_id):
                self._lease_service.release(acb.lease_ids)
                continue

            # A recovery policy is an admission-time safety decision.  Do not
            # even call a sink's inspection API for actions that were
            # explicitly quarantined (or whose durable classification is
            # malformed).  This matters for irreversible/unknown effects:
            # inspection itself may be an external operation with cost or
            # side effects, and the only safe default is UNCERTAIN.
            recovery_mode = self._unknown_recovery_mode(acb)
            if recovery_mode == "uncertain":
                self._mark_uncertain_and_release(
                    acb,
                    {
                        "reason": "recovery_policy_forbids_inspection",
                        "disposition": "uncertain",
                    },
                )
                continue

            # Inspect driver state
            try:
                inspect = await driver.inspect(acb.action_id)
            except Exception as e:
                detail = {"reason": "inspect_failed", "error": str(e)}
                uncertain = self._mark_action_uncertain_if_running(
                    acb.action_id,
                    detail=detail,
                )
                self._lease_service.release(acb.lease_ids)
                if uncertain:
                    self._signal_service.send(
                        target_pid=acb.pid,
                        signal_type="ACTION_UNCERTAIN",
                        source_pid="kernel",
                        payload={"action_id": acb.action_id, "detail": detail},
                    )
                continue

            if inspect.status == "completed":
                committed, fence_error = self._action_service.commit_if_fenced(
                    acb.action_id,
                    result=inspect.output,
                    now=self._clock.now(),
                )
                if not committed:
                    stale_error: dict[str, Any] = {
                        "reason": "stale_fenced_completion",
                        "detail": fence_error,
                    }
                    failed = self._fail_action_if_running(
                        acb.action_id,
                        error=stale_error,
                    )
                    self._lease_service.release(acb.lease_ids)
                    if failed:
                        self._signal_service.send(
                            target_pid=acb.pid,
                            signal_type="ACTION_FAILED",
                            source_pid="kernel",
                            payload={"action_id": acb.action_id, "error": stale_error},
                        )
                    continue
                self._lease_service.release(acb.lease_ids)
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_COMPLETED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "result": inspect.output},
                )
            elif inspect.status == "failed":
                error = inspect.error or {}
                failed = self._fail_action_if_running(acb.action_id, error=error)
                self._lease_service.release(acb.lease_ids)
                if failed:
                    self._signal_service.send(
                        target_pid=acb.pid,
                        signal_type="ACTION_FAILED",
                        source_pid="kernel",
                        payload={"action_id": acb.action_id, "error": error},
                    )
            elif inspect.status == "unknown":
                if recovery_mode == "retry":
                    # Pure actions can be retried after an affirmative
                    # ``unknown`` inspection.  Keep the existing fencing and
                    # terminal-race checks around the second dispatch.
                    try:
                        retry_reserved, retry_reservation_error = (
                            self._action_service.reserve_retry_if_fenced(
                                acb.action_id,
                                now=self._clock.now(),
                            )
                        )
                        if not retry_reserved:
                            # A concurrent recovery path owns the durable
                            # retry reservation.  Its retry must proceed with
                            # the original lease bundle; this loser must not
                            # mark the Action UNCERTAIN or release those leases.
                            if self._retry_reservation_is_owned_elsewhere(
                                acb.action_id,
                                retry_reservation_error,
                            ):
                                continue
                            self._mark_uncertain_and_release(
                                acb,
                                {
                                    "reason": "retry_reservation_failed",
                                    "detail": retry_reservation_error,
                                },
                            )
                            continue
                        retry_authorized, retry_validation_error = (
                            self._action_service.validate_reserved_retry_if_fenced(
                                acb.action_id,
                                now=self._clock.now(),
                            )
                        )
                        if not retry_authorized:
                            self._mark_uncertain_and_release(
                                acb,
                                {
                                    "reason": "retry_dispatch_contract_invalid",
                                    "detail": retry_validation_error,
                                },
                            )
                            continue
                        result = await driver.dispatch(acb.action_id, acb.operation, acb.arguments)
                        if result.status == "completed":
                            committed, fence_error = self._action_service.commit_if_fenced(
                                acb.action_id,
                                result=result.output,
                                now=self._clock.now(),
                            )
                            if not committed:
                                stale_retry_error: dict[str, Any] = {
                                    "reason": "stale_fenced_completion",
                                    "detail": fence_error,
                                }
                                failed = self._fail_action_if_running(
                                    acb.action_id,
                                    error=stale_retry_error,
                                )
                                self._lease_service.release(acb.lease_ids)
                                if failed:
                                    self._signal_service.send(
                                        target_pid=acb.pid,
                                        signal_type="ACTION_FAILED",
                                        source_pid="kernel",
                                        payload={
                                            "action_id": acb.action_id,
                                            "error": stale_retry_error,
                                        },
                                    )
                                continue
                            self._lease_service.release(acb.lease_ids)
                            self._signal_service.send(
                                target_pid=acb.pid,
                                signal_type="ACTION_COMPLETED",
                                source_pid="kernel",
                                payload={"action_id": acb.action_id, "result": result.output},
                            )
                        elif result.status == "failed":
                            self._mark_uncertain_and_release(
                                acb,
                                {
                                    **(result.error or {}),
                                    "reason": (result.error or {}).get(
                                        "reason",
                                        "retry_failed",
                                    ),
                                },
                            )
                        elif result.status == "unknown":
                            self._mark_uncertain_and_release(
                                acb,
                                {"reason": "retry_unknown"},
                            )
                        # ``running`` remains in flight with leases retained.
                    except Exception as exc:
                        self._mark_uncertain_and_release(
                            acb,
                            {"reason": "retry_failed", "error": str(exc)},
                        )
                elif recovery_mode == "inspect":
                    # The first inspect already returned UNKNOWN.  Keep the
                    # action uncertain rather than redispatching or spinning
                    # in a tight recovery loop.
                    self._mark_uncertain_and_release(
                        acb,
                        {"reason": "inspection_unknown", "source": "recovery"},
                    )
                else:
                    self._mark_uncertain_and_release(
                        acb,
                        {
                            "reason": "recovery_policy_forbids_blind_retry",
                            "disposition": "uncertain",
                        },
                    )
            # ``running`` is an affirmative in-flight observation. Preserve
            # RUNNING and its lease bundle; a future recovery pass will inspect
            # it again without redispatching the external operation.

    # ── Deadlock recovery ──────────────────────────────────────────────────

    async def _recover_deadlock(self, cycle: list[str]) -> None:
        """Recover from a deadlock by selecting a victim."""
        # Select victim: lowest priority → fewest leases → pid tiebreak
        victim_pid = self._select_victim(cycle)

        # Journal deadlock detection
        ev = KernelEvent(
            pid=victim_pid,
            event_type="DEADLOCK_DETECTED",
            payload={"cycle": cycle, "victim": victim_pid},
        )
        self._journal.append_event(ev)

        # Mark the victim terminal first, closing new lease admission before
        # releasing its existing ownership.
        with suppress(IllegalStateTransition):
            self._process_service.transition(victim_pid, ProcessState.FAILED)
        self._lease_service.release_all_for_pid(victim_pid)

        # Journal recovery
        ev2 = KernelEvent(
            pid=victim_pid,
            event_type="DEADLOCK_RECOVERED",
            payload={"cycle": cycle, "victim": victim_pid},
        )
        self._journal.append_event(ev2)

    def _select_victim(self, cycle: list[str]) -> str:
        """Deterministic victim selection:
        1. Lower priority wins
        2. Fewer held leases wins
        3. Lexicographic pid as tiebreaker
        """
        candidates = []
        for pid in cycle:
            pcb = self._process_service.get_process(pid)
            if pcb is None:
                continue
            lease_count = len(self._lease_service.list_leases_for_pid(pid))
            candidates.append((pcb.priority, lease_count, pid))

        if not candidates:
            return cycle[0]

        # Sort by (priority, lease_count, pid) — all ascending
        candidates.sort()
        return candidates[0][2]

    # ── Spawn helper ───────────────────────────────────────────────────────

    async def spawn(
        self,
        program: Any,
        namespace_id: str = "default",
        initial_state: dict[str, Any] | None = None,
    ) -> str:
        """Spawn a process with the given program."""
        from lhos.agent_os.kernel.models import SpawnRequest

        req = SpawnRequest(
            pid="",
            program_id=program.program_id,
            namespace_id=namespace_id,
            initial_state=initial_state or {},
        )
        await self._dispatcher.dispatch(req)

        # Find the spawned pid
        processes = self._process_service.list_all()
        # Get the last spawned one with this program_id
        for pcb in reversed(processes):
            if pcb.program_id == program.program_id:
                self.register_program(pcb.pid, program)
                return pcb.pid

        raise RuntimeError("Failed to spawn process")

    # ── Run until idle ─────────────────────────────────────────────────────

    async def run_until_idle(self, max_ticks: int = 1000) -> None:
        """Run ticks until no more work or max_ticks reached."""
        for _ in range(max_ticks):
            ready = self._process_service.list_ready()
            incomplete = self._action_service.list_non_terminal()
            if not ready and not incomplete:
                break
            await self.tick()
