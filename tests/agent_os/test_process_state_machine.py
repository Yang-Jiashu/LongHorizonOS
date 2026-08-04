"""Test Process state machine transitions."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.errors import (
    IllegalStateTransition,
    TerminalStateError,
    WaitConditionMissing,
)
from lhos.agent_os.kernel.models import ProcessControlBlock, ProcessState
from lhos.agent_os.kernel.state_machine import (
    apply_process_transition,
    validate_process_transition,
)


def _make_pcb(state: ProcessState = ProcessState.CREATED) -> ProcessControlBlock:
    return ProcessControlBlock(
        pid="p1",
        program_id="test",
        state=state,
        capability_set_id="cs1",
        namespace_id="ns1",
    )


class TestValidTransitions:
    """Test all valid process state transitions."""

    @pytest.mark.parametrize(
        "old,new",
        [
            (ProcessState.CREATED, ProcessState.READY),
            (ProcessState.READY, ProcessState.RUNNING),
            (ProcessState.RUNNING, ProcessState.READY),
            (ProcessState.RUNNING, ProcessState.BLOCKED),
            (ProcessState.BLOCKED, ProcessState.READY),
            (ProcessState.READY, ProcessState.SUSPENDED),
            (ProcessState.BLOCKED, ProcessState.SUSPENDED),
            (ProcessState.SUSPENDED, ProcessState.READY),
            (ProcessState.RUNNING, ProcessState.EXITED),
            (ProcessState.READY, ProcessState.EXITED),
            (ProcessState.BLOCKED, ProcessState.EXITED),
            (ProcessState.RUNNING, ProcessState.FAILED),
            (ProcessState.READY, ProcessState.FAILED),
            (ProcessState.BLOCKED, ProcessState.FAILED),
            (ProcessState.SUSPENDED, ProcessState.FAILED),
        ],
    )
    def test_valid_transition(self, old: ProcessState, new: ProcessState) -> None:
        pcb = _make_pcb(old)
        if new == ProcessState.BLOCKED:
            reason = validate_process_transition(pcb, new, wait_condition={"signal_type": "TEST"})
        else:
            reason = validate_process_transition(pcb, new)
        assert reason is not None

    def test_blocked_sets_wait_condition(self) -> None:
        pcb = _make_pcb(ProcessState.RUNNING)
        apply_process_transition(pcb, ProcessState.BLOCKED, wait_condition={"signal_type": "X"})
        assert pcb.wait_condition == {"signal_type": "X"}

    def test_ready_clears_wait_condition(self) -> None:
        pcb = _make_pcb(ProcessState.BLOCKED)
        pcb.wait_condition = {"signal_type": "X"}
        apply_process_transition(pcb, ProcessState.READY)
        assert pcb.wait_condition is None

    def test_running_clears_wait_condition(self) -> None:
        pcb = _make_pcb(ProcessState.READY)
        pcb.wait_condition = {"signal_type": "X"}
        apply_process_transition(pcb, ProcessState.RUNNING)
        assert pcb.wait_condition is None


class TestInvalidTransitions:
    """Test that invalid transitions are rejected."""

    @pytest.mark.parametrize(
        "old,new",
        [
            (ProcessState.CREATED, ProcessState.RUNNING),
            (ProcessState.CREATED, ProcessState.BLOCKED),
            (ProcessState.EXITED, ProcessState.READY),
            (ProcessState.FAILED, ProcessState.READY),
            (ProcessState.EXITED, ProcessState.RUNNING),
            (ProcessState.FAILED, ProcessState.RUNNING),
        ],
    )
    def test_invalid_transition(self, old: ProcessState, new: ProcessState) -> None:
        pcb = _make_pcb(old)
        with pytest.raises((IllegalStateTransition, TerminalStateError)):
            validate_process_transition(pcb, new)

    def test_blocked_without_wait_condition_raises(self) -> None:
        pcb = _make_pcb(ProcessState.RUNNING)
        with pytest.raises(WaitConditionMissing):
            validate_process_transition(pcb, ProcessState.BLOCKED)

    def test_terminal_state_rejects_any_transition(self) -> None:
        for terminal in [ProcessState.EXITED, ProcessState.FAILED]:
            pcb = _make_pcb(terminal)
            with pytest.raises(TerminalStateError):
                validate_process_transition(pcb, ProcessState.READY)
