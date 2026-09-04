"""Test the five required Demos (A-E)."""

from __future__ import annotations

from pathlib import Path

import pytest

from lhos.agent_os.kernel.models import ActionState, ProcessState, SideEffectClass
from lhos.agent_os.programs.scripted import (
    ScriptedProgram,
    exit_step,
    process_event_step,
    submit_device_action,
    submit_model_action,
)
from lhos.agent_os.sdk.client import create_kernel

ARTIFACTS_DIR = Path(__file__).parent.parent.parent / "artifacts" / "agent_os_phase_b"


class TestDemoA_NormalAction:
    """Demo A: Normal Model Action lifecycle."""

    @pytest.mark.asyncio
    async def test_demo_a(self) -> None:
        kernel = create_kernel(":memory:")

        program = ScriptedProgram(program_id="demo_a", steps=[])
        pid = await kernel.spawn(program)

        program._steps = [
            submit_model_action(pid, operation="generate", side_effect_class="pure"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        await kernel.run_until_idle(max_ticks=20)

        # Verify process exited
        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        assert pcb.state == ProcessState.EXITED

        # Verify action was committed
        actions = kernel._action_service.list_by_pid(pid)
        committed = [a for a in actions if a.state == ActionState.COMMITTED]
        assert len(committed) >= 1

        # Verify leases released
        leases = kernel._lease_service.list_leases_for_pid(pid)
        assert len(leases) == 0

        # Verify journal has complete lifecycle
        events = kernel._journal.read_all()
        event_types = [e.event_type for e in events]
        assert "ACTION_SUBMITTED" in event_types
        assert "ACTION_ADMITTED" in event_types
        assert "ACTION_INTENT_DURABLE" in event_types
        assert "ACTION_RUNNING" in event_types
        assert "ACTION_COMMITTED" in event_types
        assert "PROCESS_EXITED" in event_types


class TestDemoB_AsyncDevice:
    """Demo B: Async Device Action — process blocks, no extra model calls."""

    @pytest.mark.asyncio
    async def test_demo_b(self) -> None:
        kernel = create_kernel(":memory:")

        device_driver = kernel.get_driver("tool/mock")
        device_driver.set_default_behavior("delayed_success")

        program = ScriptedProgram(program_id="demo_b", steps=[])
        pid = await kernel.spawn(program)

        program._steps = [
            submit_device_action(pid, operation="slow_task", side_effect_class="pure"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        # Tick 1: submit → BLOCKED
        await kernel.tick()
        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        assert pcb.state == ProcessState.BLOCKED

        # Multiple ticks — no extra actions
        actions_count = len(kernel._action_service.list_by_pid(pid))
        for _ in range(3):
            await kernel.tick()
        assert len(kernel._action_service.list_by_pid(pid)) == actions_count

        # Continue until done
        await kernel.run_until_idle(max_ticks=20)

        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        assert pcb.state == ProcessState.EXITED

        actions = kernel._action_service.list_by_pid(pid)
        committed = [a for a in actions if a.state == ActionState.COMMITTED]
        assert len(committed) >= 1


class TestDemoC_CrashRecovery:
    """Demo C: Crash Recovery with different side-effect classes."""

    @pytest.mark.asyncio
    async def test_demo_c_idempotent(self) -> None:
        kernel = create_kernel(":memory:")

        device_driver = kernel.get_driver("tool/mock")
        device_driver.set_default_behavior("crash_after_effect")

        program = ScriptedProgram(program_id="demo_c_idem", steps=[])
        pid = await kernel.spawn(program)

        program._steps = [
            submit_device_action(pid, operation="write", side_effect_class="idempotent"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        await kernel.tick()  # submit
        await kernel.tick()  # dispatch → crash_after_effect

        # Action should be UNCERTAIN (idempotent with unknown inspect)
        # But effect was recorded, so inspect should find it
        # On next tick, recover_incomplete_actions should inspect and commit
        await kernel.tick()  # recovery tick

        actions = kernel._action_service.list_by_pid(pid)
        # The action should either be committed (if inspect found effect) or uncertain
        terminal = [a for a in actions if a.state in (ActionState.COMMITTED, ActionState.UNCERTAIN)]
        assert len(terminal) >= 1

        # Leases should be released
        leases = kernel._lease_service.list_leases_for_pid(pid)
        assert len(leases) == 0

    @pytest.mark.asyncio
    async def test_demo_c_non_reversible(self) -> None:
        kernel = create_kernel(":memory:")

        device_driver = kernel.get_driver("tool/mock")
        device_driver.set_default_behavior("crash_after_effect")

        program = ScriptedProgram(program_id="demo_c_nr", steps=[])
        pid = await kernel.spawn(program)

        program._steps = [
            submit_device_action(pid, operation="dangerous", side_effect_class="non_reversible"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        await kernel.tick()  # submit
        await kernel.tick()  # dispatch → crash
        await kernel.tick()  # recovery

        actions = kernel._action_service.list_by_pid(pid)
        uncertain = [a for a in actions if a.state == ActionState.UNCERTAIN]
        assert len(uncertain) >= 1

        # No auto retry
        for a in uncertain:
            assert a.side_effect_class == SideEffectClass.NON_REVERSIBLE


class TestDemoD_Deadlock:
    """Demo D: Deadlock prevention and detection."""

    def test_atomic_acquire_prevents_deadlock(self) -> None:
        kernel = create_kernel(":memory:")

        # p1 acquires R1
        kernel._lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        # p2 acquires R2
        kernel._lease_service.atomic_acquire(
            "p2", [{"resource_id": "resource:R2", "mode": "exclusive"}]
        )

        # p1 tries to get both R1+R2 atomically → fails (R2 held by p2)
        from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed

        with pytest.raises(LeaseAcquisitionFailed):
            kernel._lease_service.atomic_acquire(
                "p1",
                [
                    {"resource_id": "resource:R1", "mode": "exclusive"},
                    {"resource_id": "resource:R2", "mode": "exclusive"},
                ],
            )

        # p1 still only holds R1 — no deadlock
        cycles = kernel._lease_service.detect_deadlocks()
        assert cycles == []

    def test_deadlock_detected_and_recovered(self) -> None:
        kernel = create_kernel(":memory:")

        # Manually create a deadlock scenario
        kernel._lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        kernel._lease_service.atomic_acquire(
            "p2", [{"resource_id": "resource:R2", "mode": "exclusive"}]
        )
        kernel._lease_service._add_waiter("p1", "resource:R2")
        kernel._lease_service._add_waiter("p2", "resource:R1")

        cycles = kernel._lease_service.detect_deadlocks()
        assert len(cycles) >= 1

        # Verify the cycle contains both pids
        cycle = cycles[0]
        assert "p1" in cycle
        assert "p2" in cycle


class TestDemoE_Isolation:
    """Demo E: Namespace and capability isolation."""

    @pytest.mark.asyncio
    async def test_workspace_isolation(self) -> None:
        kernel = create_kernel(":memory:")

        from lhos.agent_os.kernel.errors import CapabilityDenied
        from lhos.agent_os.kernel.models import Capability

        program1 = ScriptedProgram(program_id="iso_p1", steps=[exit_step("PLACEHOLDER")])
        pid1 = await kernel.spawn(program1, namespace_id="ns1")

        # Replace p1 caps with restricted (only own workspace, no wildcard)
        cap_set = kernel._capability_service.get_capability_set(pid1)
        assert cap_set is not None
        cap_set.capabilities = [
            Capability(resource_pattern="resource:workspace/p1", operations={"acquire"}),
            Capability(resource_pattern="device:model/mock", operations={"invoke"}),
        ]
        kernel._capability_service._upsert_capability_set(cap_set)

        # P1 should be denied access to workspace:p2
        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid1, "resource:workspace/p2", "acquire")

        # Journal should have denial event
        events = kernel._journal.read_all()
        denials = [e for e in events if e.event_type == "CAPABILITY_DENIED"]
        assert len(denials) >= 1

    @pytest.mark.asyncio
    async def test_unauthorized_signal_denied(self) -> None:
        kernel = create_kernel(":memory:")

        from lhos.agent_os.kernel.errors import CapabilityDenied

        program1 = ScriptedProgram(program_id="sig_p1", steps=[exit_step("PLACEHOLDER")])
        pid1 = await kernel.spawn(program1)

        # Remove signal capability from p1
        cap_set = kernel._capability_service.get_capability_set(pid1)
        if cap_set:
            cap_set.capabilities = [
                c
                for c in cap_set.capabilities
                if not c.resource_pattern.startswith("process:signal")
            ]
            kernel._capability_service._upsert_capability_set(cap_set)

        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid1, "process:signal/other_pid", "send")

    @pytest.mark.asyncio
    async def test_unauthorized_device_denied(self) -> None:
        kernel = create_kernel(":memory:")

        from lhos.agent_os.kernel.errors import CapabilityDenied

        program1 = ScriptedProgram(program_id="dev_p1", steps=[exit_step("PLACEHOLDER")])
        pid1 = await kernel.spawn(program1)

        # Remove device capability
        cap_set = kernel._capability_service.get_capability_set(pid1)
        if cap_set:
            cap_set.capabilities = [
                c for c in cap_set.capabilities if not c.resource_pattern.startswith("device:")
            ]
            kernel._capability_service._upsert_capability_set(cap_set)

        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid1, "device:tool/mock", "invoke")
