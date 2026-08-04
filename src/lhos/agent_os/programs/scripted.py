"""ScriptedProgram — executes a predefined list of steps for demos.

Each step is a function that receives (state, event) and returns ProgramStepResult.
This allows us to script exact demo scenarios without a real LLM.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import uuid4

from lhos.agent_os.kernel.models import (
    ExitRequest,
    KernelEvent,
    SubmitActionRequest,
    WaitRequest,
)
from lhos.agent_os.programs.base import ProgramStepResult

StepFn = Callable[[dict[str, Any], KernelEvent | None], ProgramStepResult]


class ScriptedProgram:
    """A program that follows a predefined script of steps."""

    def __init__(
        self,
        program_id: str | None = None,
        steps: list[StepFn] | None = None,
    ):
        self._program_id = program_id or f"scripted-{uuid4().hex[:8]}"
        self._steps = steps or []
        self._step_index = 0

    @property
    def program_id(self) -> str:
        return self._program_id

    def add_step(self, fn: StepFn) -> None:
        self._steps.append(fn)

    async def step(
        self,
        state: dict[str, Any],
        event: KernelEvent | None,
    ) -> ProgramStepResult:
        if self._step_index >= len(self._steps):
            # No more steps → exit
            return ProgramStepResult(
                new_state=state,
                request=ExitRequest(pid=state.get("pid", "")),
                exit_code="ok",
            )

        fn = self._steps[self._step_index]
        self._step_index += 1
        result = fn(state, event)
        # Merge state
        merged = {**state, **result.new_state}
        result.new_state = merged
        return result

    def reset(self) -> None:
        self._step_index = 0


# ── Convenience builders for common step patterns ─────────────────────────────


def submit_model_action(
    pid: str,
    operation: str = "generate",
    arguments: dict[str, Any] | None = None,
    side_effect_class: str = "pure",
) -> StepFn:
    """Step that submits a model action."""
    from lhos.agent_os.kernel.models import SideEffectClass

    def _fn(state: dict[str, Any], event: KernelEvent | None) -> ProgramStepResult:
        req = SubmitActionRequest(
            pid=pid,
            device_type="model/mock",
            operation=operation,
            arguments=arguments or {"prompt": "hello"},
            side_effect_class=SideEffectClass(side_effect_class),
        )
        return ProgramStepResult(
            new_state={"last_request_id": req.request_id, "phase": "action_submitted"},
            request=req,
        )

    return _fn


def submit_device_action(
    pid: str,
    operation: str = "execute",
    arguments: dict[str, Any] | None = None,
    side_effect_class: str = "pure",
    resource_claims: list[dict[str, Any]] | None = None,
) -> StepFn:
    """Step that submits a device action."""
    from lhos.agent_os.kernel.models import SideEffectClass

    def _fn(state: dict[str, Any], event: KernelEvent | None) -> ProgramStepResult:
        req = SubmitActionRequest(
            pid=pid,
            device_type="tool/mock",
            operation=operation,
            arguments=arguments or {"task": "noop"},
            side_effect_class=SideEffectClass(side_effect_class),
            resource_claims=resource_claims or [],
        )
        return ProgramStepResult(
            new_state={"last_request_id": req.request_id, "phase": "action_submitted"},
            request=req,
        )

    return _fn


def wait_for_action(pid: str, action_id_key: str = "last_action_id") -> StepFn:
    """Step that waits for an action to complete."""

    def _fn(state: dict[str, Any], event: KernelEvent | None) -> ProgramStepResult:
        action_id = state.get(action_id_key, "")
        req = WaitRequest(
            pid=pid,
            condition={"signal_type": "ACTION_COMPLETED", "action_id": action_id},
        )
        return ProgramStepResult(
            new_state={"phase": "waiting"},
            request=req,
        )

    return _fn


def exit_step(pid: str, exit_code: str = "ok") -> StepFn:
    """Step that exits the process."""

    def _fn(state: dict[str, Any], event: KernelEvent | None) -> ProgramStepResult:
        return ProgramStepResult(
            new_state={"phase": "exited"},
            request=ExitRequest(pid=pid, exit_code=exit_code),
            exit_code=exit_code,
        )

    return _fn


def process_event_step(pid: str) -> StepFn:
    """Step that processes an event and advances state."""

    def _fn(state: dict[str, Any], event: KernelEvent | None) -> ProgramStepResult:
        if event is None:
            return ProgramStepResult(new_state=state)
        # Store the event result
        new_state = dict(state)
        if event.event_type == "ACTION_COMMITTED":
            new_state["last_action_result"] = event.payload.get("result", {})
            new_state["phase"] = "action_completed"
        elif event.event_type == "ACTION_FAILED":
            new_state["last_action_error"] = event.payload.get("error", {})
            new_state["phase"] = "action_failed"
        return ProgramStepResult(new_state=new_state)

    return _fn
