"""Audit: BLOCKED process zero-polling verification."""

from __future__ import annotations

from typing import Any

import pytest

from lhos.agent_os.drivers.base import DriverInspect, DriverResult
from lhos.agent_os.kernel.models import (
    KernelEvent,
    ProcessState,
)
from lhos.agent_os.programs.base import ProgramStepResult
from lhos.agent_os.programs.scripted import (
    ScriptedProgram,
    exit_step,
    process_event_step,
    submit_device_action,
)
from lhos.agent_os.sdk.client import create_kernel


class CountingProgram:
    """Program wrapper that counts step calls."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.step_count = 0
        self.dispatch_count = 0

    @property
    def program_id(self) -> str:
        return self._inner.program_id

    async def step(self, state: dict, event: KernelEvent | None) -> ProgramStepResult:
        self.step_count += 1
        return await self._inner.step(state, event)

    def reset(self) -> None:
        self._inner.reset()
        self.step_count = 0


class PendingDriver:
    """Driver that returns 'running' on dispatch — action never completes."""

    device_type = "tool/mock"

    def __init__(self) -> None:
        self.dispatch_count = 0
        self.inspect_count = 0
        self._complete = False

    async def dispatch(self, action_id: str, operation: str, arguments: dict) -> DriverResult:
        self.dispatch_count += 1
        if self._complete:
            return DriverResult(status="completed", output={"result": "done"})
        return DriverResult(status="running")

    async def inspect(self, action_id: str) -> DriverInspect:
        self.inspect_count += 1
        if self._complete:
            return DriverInspect(status="completed", output={"result": "done"})
        return DriverInspect(status="running")

    def reset(self) -> None:
        self.dispatch_count = 0
        self.inspect_count = 0
        self._complete = False

    def complete(self) -> None:
        self._complete = True


class CountingModelDriver:
    """Model driver that counts dispatch calls."""

    device_type = "model/mock"

    def __init__(self) -> None:
        self.dispatch_count = 0

    async def dispatch(self, action_id: str, operation: str, arguments: dict) -> DriverResult:
        self.dispatch_count += 1
        return DriverResult(status="completed", output={"result": "ok"})

    async def inspect(self, action_id: str) -> DriverInspect:
        return DriverInspect(status="unknown")

    def reset(self) -> None:
        self.dispatch_count = 0


class TestBlockedZeroPolling:
    """Verify that BLOCKED processes are never polled."""

    @pytest.mark.asyncio
    async def test_blocked_process_has_zero_program_polling(self) -> None:
        """While BLOCKED, program.step() must not be called."""
        kernel = create_kernel(":memory:")

        pending_driver = PendingDriver()
        kernel.register_driver("tool/mock", pending_driver)

        program = ScriptedProgram(program_id="zero_poll", steps=[])
        counting_program = CountingProgram(program)
        pid = await kernel.spawn(counting_program)

        program._steps = [
            submit_device_action(pid, operation="slow", side_effect_class="idempotent"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        # Tick 1: process submits action → BLOCKED
        await kernel.tick()
        assert counting_program.step_count == 1

        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        assert pcb.state == ProcessState.BLOCKED

        # Tick 2: action dispatched → driver returns "running" → no signal
        await kernel.tick()
        assert counting_program.step_count == 1  # Still 1, no polling!

        # Tick 3: recovery runs, action goes UNCERTAIN
        # Note: ACTION_UNCERTAIN signal doesn't match wait_condition (ACTION_COMPLETED)
        # so the process stays BLOCKED. This is a known Phase B limitation:
        # the kernel should also wake processes on ACTION_UNCERTAIN/ACTION_FAILED.
        await kernel.tick()
        assert counting_program.step_count == 1  # Still no polling!

        # Manually send a matching signal to wake the process
        actions = kernel._action_service.list_by_pid(pid)
        if actions:
            action_id = actions[-1].action_id
            kernel._signal_service.send(
                target_pid=pid,
                signal_type="ACTION_COMPLETED",
                source_pid="kernel",
                payload={"action_id": action_id, "result": {"manual": True}},
            )

        # Tick 4: signal delivered, process wakes
        await kernel.tick()
        assert counting_program.step_count >= 2  # Now stepped

    @pytest.mark.asyncio
    async def test_blocked_process_has_zero_model_polling(self) -> None:
        """While BLOCKED, model driver dispatch must not be called."""
        kernel = create_kernel(":memory:")

        model_driver = CountingModelDriver()
        kernel.register_driver("model/mock", model_driver)

        pending_driver = PendingDriver()
        kernel.register_driver("tool/mock", pending_driver)

        program = ScriptedProgram(program_id="zero_model", steps=[])
        pid = await kernel.spawn(program)

        program._steps = [
            submit_device_action(pid, operation="slow", side_effect_class="idempotent"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        # Tick 1: submit device action → BLOCKED
        await kernel.tick()
        assert model_driver.dispatch_count == 0  # No model calls

        # Tick 2: device action dispatched → "running"
        await kernel.tick()
        assert model_driver.dispatch_count == 0  # Still no model calls

    @pytest.mark.asyncio
    async def test_matching_completion_wakes_exactly_once(self) -> None:
        """When action completes, process wakes exactly once."""
        kernel = create_kernel(":memory:")

        program = ScriptedProgram(program_id="wake_once", steps=[])
        counting_program = CountingProgram(program)
        pid = await kernel.spawn(counting_program)

        program._steps = [
            submit_device_action(pid, operation="test", side_effect_class="pure"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        # Run to completion
        await kernel.run_until_idle(max_ticks=20)

        # Process should have exited
        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        assert pcb.state == ProcessState.EXITED

        # Step count: 1 (submit) + 1 (process event) + 1 (exit) = 3
        assert counting_program.step_count == 3

        # Run more ticks — step count should not increase
        for _ in range(5):
            await kernel.tick()
        assert counting_program.step_count == 3
