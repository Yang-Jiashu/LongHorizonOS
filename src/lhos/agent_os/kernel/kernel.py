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

from lhos.agent_os.drivers.base import DriverResult
from lhos.agent_os.drivers.mock_device import MockDeviceDriver
from lhos.agent_os.drivers.mock_model import MockModelDriver
from lhos.agent_os.kernel.dispatcher import SyscallDispatcher
from lhos.agent_os.kernel.errors import IllegalStateTransition
from lhos.agent_os.kernel.models import (
    ActionState,
    Clock,
    ExitRequest,
    KernelEvent,
    ProcessControlBlock,
    ProcessState,
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

        # Get last event for this process (if any)
        last_events = self._journal.read_from_offset(pcb.event_cursor, limit=100)
        last_event = None
        for ev in reversed(last_events):
            if ev.pid == pcb.pid:
                last_event = ev
                break

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
            self._lease_service.release_all_for_pid(pcb.pid)
            self._process_service.transition(pcb.pid, ProcessState.FAILED)
            return

        # Save program state
        self._process_service.save_program_state(pcb.pid, result.new_state)

        # Update event cursor
        pcb.event_cursor = self._journal.next_offset()
        self._process_service._upsert_projection(pcb)

        # Handle the request
        if result.request is not None:
            req = result.request
            if isinstance(req, ExitRequest) or (
                hasattr(req, "request_type") and req.request_type == "exit"
            ):
                # Handle exit directly
                await self._dispatcher.dispatch(req)
            elif isinstance(req, SubmitActionRequest) or (
                hasattr(req, "request_type") and req.request_type == "submit_action"
            ):
                # Submit action
                await self._dispatcher.dispatch(req)
                # Process goes BLOCKED (waiting for action)
                # The action_id is in the program state
                action_id = self._get_last_action_id(pcb.pid)
                if action_id:
                    self._process_service.transition(
                        pcb.pid,
                        ProcessState.BLOCKED,
                        wait_condition={"signal_type": "ACTION_COMPLETED", "action_id": action_id},
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
            driver = self._drivers.get(acb.device_type)
            if driver is None:
                # No driver — fail the action
                self._action_service.fail(acb.action_id, error={"reason": "no_driver"})
                continue

            # Dispatch to driver
            self._action_service.dispatch(acb.action_id)

            try:
                result: DriverResult = await driver.dispatch(
                    acb.action_id,
                    acb.operation,
                    acb.arguments,
                )
            except Exception as e:
                self._action_service.fail(acb.action_id, error={"reason": str(e)})
                # Release leases
                self._lease_service.release(acb.lease_ids)
                continue

            # Process result based on side effect class
            if result.status == "completed":
                self._action_service.commit(acb.action_id, result=result.output)
                self._lease_service.release(acb.lease_ids)
                # Send signal
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_COMPLETED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "result": result.output},
                )
            elif result.status == "failed":
                self._action_service.fail(acb.action_id, error=result.error or {})
                self._lease_service.release(acb.lease_ids)
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_FAILED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "error": result.error or {}},
                )
            elif result.status == "unknown":
                # Side effect may or may not have happened
                if acb.side_effect_class == SideEffectClass.PURE:
                    # Safe to retry
                    retry_result = await driver.dispatch(
                        acb.action_id, acb.operation, acb.arguments
                    )
                    if retry_result.status == "completed":
                        self._action_service.commit(acb.action_id, result=retry_result.output)
                        self._lease_service.release(acb.lease_ids)
                        self._signal_service.send(
                            target_pid=acb.pid,
                            signal_type="ACTION_COMPLETED",
                            source_pid="kernel",
                            payload={"action_id": acb.action_id, "result": retry_result.output},
                        )
                    else:
                        self._action_service.mark_uncertain(acb.action_id)
                        self._lease_service.release(acb.lease_ids)
                        self._signal_service.send(
                            target_pid=acb.pid,
                            signal_type="ACTION_UNCERTAIN",
                            source_pid="kernel",
                            payload={"action_id": acb.action_id},
                        )
                elif acb.side_effect_class == SideEffectClass.IDEMPOTENT:
                    # Inspect to check if effect happened
                    inspect = await driver.inspect(acb.action_id)
                    if inspect.status == "completed":
                        self._action_service.commit(acb.action_id, result=inspect.output)
                        self._lease_service.release(acb.lease_ids)
                        self._signal_service.send(
                            target_pid=acb.pid,
                            signal_type="ACTION_COMPLETED",
                            source_pid="kernel",
                            payload={"action_id": acb.action_id, "result": inspect.output},
                        )
                    else:
                        self._action_service.mark_uncertain(acb.action_id)
                        self._lease_service.release(acb.lease_ids)
                        self._signal_service.send(
                            target_pid=acb.pid,
                            signal_type="ACTION_UNCERTAIN",
                            source_pid="kernel",
                            payload={"action_id": acb.action_id},
                        )
                else:
                    # NON_REVERSIBLE or UNKNOWN → UNCERTAIN, no auto retry
                    self._action_service.mark_uncertain(acb.action_id)
                    self._lease_service.release(acb.lease_ids)
                    self._signal_service.send(
                        target_pid=acb.pid,
                        signal_type="ACTION_UNCERTAIN",
                        source_pid="kernel",
                        payload={"action_id": acb.action_id},
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
                self._action_service.fail(acb.action_id, error={"reason": "no_driver"})
                self._lease_service.release(acb.lease_ids)
                continue

            # Inspect driver state
            try:
                inspect = await driver.inspect(acb.action_id)
            except Exception as e:
                self._action_service.mark_uncertain(
                    acb.action_id,
                    detail={"reason": "inspect_failed", "error": str(e)},
                )
                self._lease_service.release(acb.lease_ids)
                continue

            if inspect.status == "completed":
                self._action_service.commit(acb.action_id, result=inspect.output)
                self._lease_service.release(acb.lease_ids)
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_COMPLETED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "result": inspect.output},
                )
            elif inspect.status == "failed":
                self._action_service.fail(acb.action_id, error=inspect.error or {})
                self._lease_service.release(acb.lease_ids)
                self._signal_service.send(
                    target_pid=acb.pid,
                    signal_type="ACTION_FAILED",
                    source_pid="kernel",
                    payload={"action_id": acb.action_id, "error": inspect.error or {}},
                )
            else:
                # Unknown → UNCERTAIN for non-pure actions
                if acb.side_effect_class == SideEffectClass.PURE:
                    # Pure actions can be retried
                    try:
                        result = await driver.dispatch(acb.action_id, acb.operation, acb.arguments)
                        if result.status == "completed":
                            self._action_service.commit(acb.action_id, result=result.output)
                            self._lease_service.release(acb.lease_ids)
                            self._signal_service.send(
                                target_pid=acb.pid,
                                signal_type="ACTION_COMPLETED",
                                source_pid="kernel",
                                payload={"action_id": acb.action_id, "result": result.output},
                            )
                        else:
                            self._action_service.mark_uncertain(acb.action_id)
                            self._lease_service.release(acb.lease_ids)
                    except Exception:
                        self._action_service.mark_uncertain(acb.action_id)
                        self._lease_service.release(acb.lease_ids)
                else:
                    self._action_service.mark_uncertain(acb.action_id)
                    self._lease_service.release(acb.lease_ids)
                    self._signal_service.send(
                        target_pid=acb.pid,
                        signal_type="ACTION_UNCERTAIN",
                        source_pid="kernel",
                        payload={"action_id": acb.action_id},
                    )

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

        # Release victim's leases
        self._lease_service.release_all_for_pid(victim_pid)

        # Victim → FAILED
        with suppress(IllegalStateTransition):
            self._process_service.transition(victim_pid, ProcessState.FAILED)

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
