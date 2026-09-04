"""Audit: Process state machine mutation audit.

Tests that breaking process state machine invariants is detected.
"""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.errors import (
    IllegalStateTransition,
    TerminalStateError,
    WaitConditionMissing,
)
from lhos.agent_os.kernel.models import (
    ProcessControlBlock,
    ProcessState,
)
from lhos.agent_os.kernel.state_machine import (
    _PROCESS_TRANSITIONS,
    validate_process_transition,
)
from lhos.agent_os.sdk.client import create_kernel


class TestProcessStateMachineAudit:
    """Verify process state machine invariants."""

    def test_blocked_requires_wait_condition(self) -> None:
        """BLOCKED state without wait_condition must raise."""
        pcb = ProcessControlBlock(
            pid="p1",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.RUNNING,
        )
        with pytest.raises(WaitConditionMissing):
            validate_process_transition(pcb, ProcessState.BLOCKED)

        # With wait_condition, it should work
        validate_process_transition(
            pcb, ProcessState.BLOCKED, wait_condition={"signal_type": "WAKE"}
        )

    def test_terminal_process_cannot_resume(self) -> None:
        """EXITED process cannot transition to READY."""
        pcb = ProcessControlBlock(
            pid="p1",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.EXITED,
        )
        with pytest.raises(TerminalStateError):
            validate_process_transition(pcb, ProcessState.READY)

    def test_failed_process_cannot_step(self) -> None:
        """FAILED process cannot transition to RUNNING."""
        pcb = ProcessControlBlock(
            pid="p1",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.FAILED,
        )
        with pytest.raises(TerminalStateError):
            validate_process_transition(pcb, ProcessState.RUNNING)

    def test_one_process_has_at_most_one_active_step(self) -> None:
        """A process can only be RUNNING once at a time."""
        kernel = create_kernel(":memory:")

        # Create a process in RUNNING state
        pcb = ProcessControlBlock(
            pid="p_single",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.READY,
        )
        kernel._process_service._upsert_projection(pcb)

        # READY → RUNNING
        kernel._process_service.transition("p_single", ProcessState.RUNNING)

        # Verify it's RUNNING
        running_pcb = kernel._process_service.get_process("p_single")
        assert running_pcb is not None
        assert running_pcb.state == ProcessState.RUNNING

        # Try RUNNING → RUNNING (should fail — no self-transition)
        with pytest.raises(IllegalStateTransition):
            kernel._process_service.transition("p_single", ProcessState.RUNNING)

    def test_suspended_process_is_not_scheduled(self) -> None:
        """SUSPENDED processes should not appear in list_ready()."""
        kernel = create_kernel(":memory:")

        # Create processes
        ready_pcb = ProcessControlBlock(
            pid="p_ready",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.READY,
        )
        suspended_pcb = ProcessControlBlock(
            pid="p_suspended",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.SUSPENDED,
        )
        kernel._process_service._upsert_projection(ready_pcb)
        kernel._process_service._upsert_projection(suspended_pcb)

        ready_list = kernel._process_service.list_ready()
        pids = [p.pid for p in ready_list]

        assert "p_ready" in pids
        assert "p_suspended" not in pids

    def test_blocked_process_not_in_ready_list(self) -> None:
        """BLOCKED processes should not appear in list_ready()."""
        kernel = create_kernel(":memory:")

        blocked_pcb = ProcessControlBlock(
            pid="p_blocked",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.BLOCKED,
            wait_condition={"signal_type": "WAKE"},
        )
        kernel._process_service._upsert_projection(blocked_pcb)

        ready_list = kernel._process_service.list_ready()
        pids = [p.pid for p in ready_list]
        assert "p_blocked" not in pids

    def test_exited_process_not_in_ready_list(self) -> None:
        """EXITED processes should not appear in list_ready()."""
        kernel = create_kernel(":memory:")

        exited_pcb = ProcessControlBlock(
            pid="p_exited",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.EXITED,
        )
        kernel._process_service._upsert_projection(exited_pcb)

        ready_list = kernel._process_service.list_ready()
        pids = [p.pid for p in ready_list]
        assert "p_exited" not in pids

    def test_failed_process_not_in_ready_list(self) -> None:
        """FAILED processes should not appear in list_ready()."""
        kernel = create_kernel(":memory:")

        failed_pcb = ProcessControlBlock(
            pid="p_failed",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.FAILED,
        )
        kernel._process_service._upsert_projection(failed_pcb)

        ready_list = kernel._process_service.list_ready()
        pids = [p.pid for p in ready_list]
        assert "p_failed" not in pids


class TestProcessMutationAudit:
    """Verify that mutations to process state machine are caught."""

    def test_mutation_blocked_without_wait_condition_detected(self) -> None:
        """If we remove the wait_condition guard, the test should catch it."""
        # The guard is in validate_process_transition:
        # if target == ProcessState.BLOCKED and wait_condition is None:
        #     raise WaitConditionMissing(pcb.pid)
        # This is a code-level guard, not a transition table entry.
        # If removed, BLOCKED without wait_condition would be allowed.
        # Our test_blocked_requires_wait_condition would fail.
        # This test verifies the guard exists.
        pcb = ProcessControlBlock(
            pid="p1",
            program_id="test",
            capability_set_id="cs1",
            namespace_id="ns1",
            state=ProcessState.RUNNING,
        )
        with pytest.raises(WaitConditionMissing):
            validate_process_transition(pcb, ProcessState.BLOCKED)

    def test_mutation_exited_can_resume_detected(self) -> None:
        """If EXITED→READY is added, terminal check still catches it."""
        original = dict(_PROCESS_TRANSITIONS)
        _PROCESS_TRANSITIONS[(ProcessState.EXITED, ProcessState.READY)] = "mutation"

        try:
            pcb = ProcessControlBlock(
                pid="p1",
                program_id="test",
                capability_set_id="cs1",
                namespace_id="ns1",
                state=ProcessState.EXITED,
            )
            # Terminal state check happens before transition lookup
            with pytest.raises(TerminalStateError):
                validate_process_transition(pcb, ProcessState.READY)
        finally:
            _PROCESS_TRANSITIONS.clear()
            _PROCESS_TRANSITIONS.update(original)
