"""Test Action state machine transitions."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.errors import IllegalStateTransition, TerminalStateError
from lhos.agent_os.kernel.models import ActionControlBlock, ActionState
from lhos.agent_os.kernel.state_machine import (
    apply_action_transition,
    validate_action_transition,
)


def _make_acb(state: ActionState = ActionState.SUBMITTED) -> ActionControlBlock:
    return ActionControlBlock(
        pid="p1",
        device_type="model/mock",
        operation="generate",
        state=state,
    )


class TestValidActionTransitions:
    @pytest.mark.parametrize(
        "old,new",
        [
            (ActionState.SUBMITTED, ActionState.ADMITTED),
            (ActionState.SUBMITTED, ActionState.FAILED),
            (ActionState.ADMITTED, ActionState.RUNNING),
            (ActionState.ADMITTED, ActionState.CANCELLED),
            (ActionState.RUNNING, ActionState.COMMITTED),
            (ActionState.RUNNING, ActionState.FAILED),
            (ActionState.RUNNING, ActionState.TIMED_OUT),
            (ActionState.RUNNING, ActionState.CANCELLED),
            (ActionState.RUNNING, ActionState.UNCERTAIN),
        ],
    )
    def test_valid_transition(self, old: ActionState, new: ActionState) -> None:
        acb = _make_acb(old)
        reason = validate_action_transition(acb, new)
        assert reason is not None


class TestInvalidActionTransitions:
    @pytest.mark.parametrize(
        "old,new",
        [
            (ActionState.SUBMITTED, ActionState.RUNNING),
            (ActionState.SUBMITTED, ActionState.COMMITTED),
            (ActionState.ADMITTED, ActionState.COMMITTED),
            (ActionState.ADMITTED, ActionState.UNCERTAIN),
            (ActionState.COMMITTED, ActionState.FAILED),
            (ActionState.FAILED, ActionState.COMMITTED),
            (ActionState.UNCERTAIN, ActionState.COMMITTED),
            (ActionState.CANCELLED, ActionState.RUNNING),
        ],
    )
    def test_invalid_transition(self, old: ActionState, new: ActionState) -> None:
        acb = _make_acb(old)
        with pytest.raises((IllegalStateTransition, TerminalStateError)):
            validate_action_transition(acb, new)

    @pytest.mark.parametrize(
        "terminal",
        [
            ActionState.COMMITTED,
            ActionState.FAILED,
            ActionState.CANCELLED,
            ActionState.TIMED_OUT,
            ActionState.UNCERTAIN,
        ],
    )
    def test_terminal_rejects_all(self, terminal: ActionState) -> None:
        acb = _make_acb(terminal)
        for target in [
            ActionState.SUBMITTED,
            ActionState.ADMITTED,
            ActionState.RUNNING,
            ActionState.COMMITTED,
        ]:
            with pytest.raises(TerminalStateError):
                validate_action_transition(acb, target)


class TestActionSingleTerminalState:
    """An Action can only enter one terminal state."""

    def test_committed_cannot_fail(self) -> None:
        acb = _make_acb(ActionState.RUNNING)
        apply_action_transition(acb, ActionState.COMMITTED)
        with pytest.raises(TerminalStateError):
            apply_action_transition(acb, ActionState.FAILED)

    def test_failed_cannot_commit(self) -> None:
        acb = _make_acb(ActionState.RUNNING)
        apply_action_transition(acb, ActionState.FAILED)
        with pytest.raises(TerminalStateError):
            apply_action_transition(acb, ActionState.COMMITTED)

    def test_uncertain_cannot_commit(self) -> None:
        acb = _make_acb(ActionState.RUNNING)
        apply_action_transition(acb, ActionState.UNCERTAIN)
        with pytest.raises(TerminalStateError):
            apply_action_transition(acb, ActionState.COMMITTED)
