"""Audit: Starvation — FIFO scheduler fairness."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.models import ProcessState
from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step
from lhos.agent_os.sdk.client import create_kernel


class TestStarvationAudit:
    """Verify FIFO scheduler does not starve processes."""

    @pytest.mark.asyncio
    async def test_all_ready_processes_eventually_run(self) -> None:
        """All READY processes must eventually be scheduled."""
        kernel = create_kernel(":memory:")

        pids = []
        for i in range(20):
            prog = ScriptedProgram(
                program_id=f"starve_{i}",
                steps=[],
            )
            pid = await kernel.spawn(prog)
            prog._steps = [exit_step(pid)]
            prog.reset()
            pids.append(pid)

        await kernel.run_until_idle(max_ticks=100)

        # All should be EXITED
        for pid in pids:
            pcb = kernel._process_service.get_process(pid)
            assert pcb is not None
            assert pcb.state == ProcessState.EXITED, f"Process {pid} did not exit: {pcb.state}"

    @pytest.mark.asyncio
    async def test_one_process_cannot_monopolize_single_tick(self) -> None:
        """A single tick should not run the same process multiple times."""
        kernel = create_kernel(":memory:")

        step_counts: dict[str, int] = {}

        class MultiStepProgram:
            def __init__(self, pid: str, n_steps: int) -> None:
                self._pid = pid
                self._n_steps = n_steps
                self._step = 0
                self.program_id = f"multi_{pid}"

            async def step(self, state, event):
                from lhos.agent_os.programs.base import ProgramStepResult

                self._step += 1
                step_counts[self._pid] = step_counts.get(self._pid, 0) + 1

                if self._step >= self._n_steps:
                    from lhos.agent_os.kernel.models import ExitRequest

                    return ProgramStepResult(
                        new_state=state,
                        request=ExitRequest(pid=state.get("pid", self._pid)),
                        exit_code="ok",
                    )
                return ProgramStepResult(new_state=state)

            def reset(self) -> None:
                self._step = 0

        prog = MultiStepProgram("p_multi", 5)
        pid = await kernel.spawn(prog)
        prog._pid = pid  # Update with actual pid

        # Run one tick
        await kernel.tick()

        # Should have run at most 1 step in this tick
        assert step_counts.get(pid, 0) <= 1

        # Run to completion
        await kernel.run_until_idle(max_ticks=50)
        assert step_counts.get(pid, 0) == 5

    @pytest.mark.asyncio
    async def test_fifo_order_is_respected(self) -> None:
        """FIFO scheduler processes in creation order."""
        kernel = create_kernel(":memory:")

        execution_order: list[str] = []

        class OrderTrackingProgram:
            def __init__(self, name: str) -> None:
                self._name = name
                self.program_id = f"order_{name}"
                self._step = 0

            async def step(self, state, event):
                from lhos.agent_os.programs.base import ProgramStepResult

                self._step += 1
                if self._step == 1:
                    execution_order.append(self._name)
                if self._step >= 2:
                    from lhos.agent_os.kernel.models import ExitRequest

                    return ProgramStepResult(
                        new_state=state,
                        request=ExitRequest(pid=state.get("pid", "")),
                        exit_code="ok",
                    )
                return ProgramStepResult(new_state=state)

            def reset(self) -> None:
                self._step = 0

        programs = [OrderTrackingProgram(f"p{i:02d}") for i in range(10)]
        for prog in programs:
            await kernel.spawn(prog)

        await kernel.run_until_idle(max_ticks=50)

        # First 10 should be in creation order (FIFO)
        assert len(execution_order) == 10
        assert execution_order == [f"p{i:02d}" for i in range(10)]

    @pytest.mark.asyncio
    async def test_100_processes_all_complete(self) -> None:
        """100 processes each doing 10 steps must all complete."""
        kernel = create_kernel(":memory:")

        class TenStepProgram:
            def __init__(self, name: str) -> None:
                self._name = name
                self.program_id = f"bench_{name}"
                self._step = 0

            async def step(self, state, event):
                from lhos.agent_os.programs.base import ProgramStepResult

                self._step += 1
                if self._step >= 10:
                    from lhos.agent_os.kernel.models import ExitRequest

                    return ProgramStepResult(
                        new_state={**state, "step": self._step},
                        request=ExitRequest(pid=state.get("pid", "")),
                        exit_code="ok",
                    )
                return ProgramStepResult(new_state={**state, "step": self._step})

            def reset(self) -> None:
                self._step = 0

        pids = []
        for i in range(100):
            prog = TenStepProgram(f"p{i:03d}")
            pid = await kernel.spawn(prog)
            pids.append(pid)

        await kernel.run_until_idle(max_ticks=5000)

        exited_count = 0
        for pid in pids:
            pcb = kernel._process_service.get_process(pid)
            if pcb and pcb.state == ProcessState.EXITED:
                exited_count += 1

        assert exited_count == 100, f"Only {exited_count}/100 processes exited"
