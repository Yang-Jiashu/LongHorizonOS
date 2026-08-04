"""Test Kernel Loop — tick lifecycle, scheduling, no infinite loops."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.models import ProcessState
from lhos.agent_os.programs.scripted import (
    ScriptedProgram,
    exit_step,
    process_event_step,
    submit_device_action,
    submit_model_action,
)
from lhos.agent_os.sdk.client import create_kernel


@pytest.fixture
def kernel():
    return create_kernel(":memory:")


class TestKernelTick:
    @pytest.mark.asyncio
    async def test_tick_does_not_infinite_loop(self, kernel) -> None:
        """A single tick must terminate."""
        await kernel.tick()  # Should return

    @pytest.mark.asyncio
    async def test_run_until_idle_terminates(self, kernel) -> None:
        program = ScriptedProgram(
            program_id="simple",
            steps=[exit_step("PLACEHOLDER")],
        )
        pid = await kernel.spawn(program)
        program._steps = [exit_step(pid)]
        program.reset()

        await kernel.run_until_idle(max_ticks=10)

        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        assert pcb.state == ProcessState.EXITED

    @pytest.mark.asyncio
    async def test_one_step_per_tick(self, kernel) -> None:
        """Each tick runs at most one step per ready process."""
        program = ScriptedProgram(
            program_id="multi_step",
            steps=[],
        )
        pid = await kernel.spawn(program)
        program._steps = [
            submit_model_action(pid),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        # Tick 1: spawn → READY, run step 1 (submit action) → BLOCKED
        await kernel.tick()

        # Tick 2: dispatch action, deliver signal → READY
        await kernel.tick()

        # Tick 3: run step 2 (process event) → READY
        await kernel.tick()

        # Tick 4: run step 3 (exit) → EXITED
        await kernel.tick()

        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        assert pcb.state == ProcessState.EXITED


class TestBlockedProcessNoModelPolling:
    """When a process is BLOCKED, no model actions should be dispatched for it."""

    @pytest.mark.asyncio
    async def test_blocked_process_no_model_call(self, kernel) -> None:
        # Use delayed device action
        device_driver = kernel.get_driver("tool/mock")
        device_driver.set_default_behavior("delayed_success")

        program = ScriptedProgram(program_id="blocked_test", steps=[])
        pid = await kernel.spawn(program)
        program._steps = [
            submit_device_action(pid, operation="slow_task", side_effect_class="pure"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        # Tick 1: submit action → BLOCKED
        await kernel.tick()

        # Process should be BLOCKED
        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        assert pcb.state == ProcessState.BLOCKED

        # Count actions submitted so far
        actions_before = kernel._action_service.list_by_pid(pid)

        # Multiple ticks while blocked — should not create new actions
        for _ in range(5):
            await kernel.tick()

        actions_after = kernel._action_service.list_by_pid(pid)
        assert len(actions_after) == len(actions_before)
