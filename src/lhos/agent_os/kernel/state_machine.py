"""Explicit state machines for Process and Action.

All transitions are validated, journaled, and projected.
No direct state mutation is allowed outside these functions.
"""

from __future__ import annotations

from typing import Any

from lhos.agent_os.kernel.errors import (
    IllegalStateTransition,
    TerminalStateError,
    WaitConditionMissing,
)
from lhos.agent_os.kernel.models import (
    TERMINAL_ACTION_STATES,
    TERMINAL_PROCESS_STATES,
    ActionControlBlock,
    ActionState,
    ProcessControlBlock,
    ProcessState,
)

# ── Process State Machine ────────────────────────────────────────────────────

_PROCESS_TRANSITIONS: dict[tuple[ProcessState, ProcessState], str] = {
    (ProcessState.CREATED, ProcessState.READY): "spawn_complete",
    (ProcessState.READY, ProcessState.RUNNING): "scheduled",
    (ProcessState.RUNNING, ProcessState.READY): "yielded",
    (ProcessState.RUNNING, ProcessState.BLOCKED): "blocked",
    (ProcessState.BLOCKED, ProcessState.READY): "woken",
    (ProcessState.READY, ProcessState.SUSPENDED): "paused",
    (ProcessState.BLOCKED, ProcessState.SUSPENDED): "paused",
    (ProcessState.SUSPENDED, ProcessState.READY): "resumed",
    (ProcessState.RUNNING, ProcessState.EXITED): "exit",
    (ProcessState.READY, ProcessState.EXITED): "exit",
    (ProcessState.BLOCKED, ProcessState.EXITED): "exit",
    (ProcessState.RUNNING, ProcessState.FAILED): "error",
    (ProcessState.READY, ProcessState.FAILED): "error",
    (ProcessState.BLOCKED, ProcessState.FAILED): "error",
    (ProcessState.SUSPENDED, ProcessState.FAILED): "error",
}


def validate_process_transition(
    pcb: ProcessControlBlock,
    target: ProcessState,
    wait_condition: dict[str, Any] | None = None,
) -> str:
    """Validate a process state transition. Returns the reason string.

    Raises IllegalStateTransition if invalid.
    """
    if pcb.state in TERMINAL_PROCESS_STATES:
        raise TerminalStateError(pcb.pid, pcb.state.value)

    key = (pcb.state, target)
    if key not in _PROCESS_TRANSITIONS:
        raise IllegalStateTransition(
            pcb.pid,
            pcb.state.value,
            target.value,
            f"no transition defined from {pcb.state} to {target}",
        )

    reason = _PROCESS_TRANSITIONS[key]

    # Guards
    if target == ProcessState.BLOCKED and wait_condition is None:
        raise WaitConditionMissing(pcb.pid)

    return reason


def apply_process_transition(
    pcb: ProcessControlBlock,
    target: ProcessState,
    wait_condition: dict[str, Any] | None = None,
) -> str:
    """Validate and apply a process state transition in-place.

    Returns the reason string. Does NOT journal (caller's responsibility).
    """
    reason = validate_process_transition(pcb, target, wait_condition)
    pcb.state = target
    if target == ProcessState.BLOCKED:
        pcb.wait_condition = wait_condition
    elif target in (ProcessState.READY, ProcessState.RUNNING) or target in TERMINAL_PROCESS_STATES:
        pcb.wait_condition = None
    return reason


# ── Action State Machine ─────────────────────────────────────────────────────

_ACTION_TRANSITIONS: dict[tuple[ActionState, ActionState], str] = {
    (ActionState.SUBMITTED, ActionState.ADMITTED): "admission_pass",
    (ActionState.SUBMITTED, ActionState.FAILED): "admission_fail",
    (ActionState.ADMITTED, ActionState.RUNNING): "dispatched",
    (ActionState.ADMITTED, ActionState.CANCELLED): "cancelled_pre_dispatch",
    (ActionState.RUNNING, ActionState.COMMITTED): "driver_success",
    (ActionState.RUNNING, ActionState.FAILED): "driver_failure",
    (ActionState.RUNNING, ActionState.TIMED_OUT): "deadline_exceeded",
    (ActionState.RUNNING, ActionState.CANCELLED): "cancelled_in_flight",
    (ActionState.RUNNING, ActionState.UNCERTAIN): "driver_unknown",
}


def validate_action_transition(
    acb: ActionControlBlock,
    target: ActionState,
) -> str:
    """Validate an action state transition. Returns the reason string.

    Raises IllegalStateTransition or TerminalStateError if invalid.
    """
    if acb.state in TERMINAL_ACTION_STATES:
        raise TerminalStateError(acb.action_id, acb.state.value)

    key = (acb.state, target)
    if key not in _ACTION_TRANSITIONS:
        raise IllegalStateTransition(
            acb.action_id,
            acb.state.value,
            target.value,
            f"no transition defined from {acb.state} to {target}",
        )

    return _ACTION_TRANSITIONS[key]


def apply_action_transition(
    acb: ActionControlBlock,
    target: ActionState,
) -> str:
    """Validate and apply an action state transition in-place.

    Returns the reason string. Does NOT journal (caller's responsibility).
    """
    reason = validate_action_transition(acb, target)
    acb.state = target
    return reason
