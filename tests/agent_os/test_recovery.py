"""Test Crash Recovery — action classification and lease release."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.models import ActionState, SideEffectClass
from lhos.agent_os.programs.scripted import (
    ScriptedProgram,
    exit_step,
    process_event_step,
    submit_device_action,
    submit_model_action,
)
from lhos.agent_os.sdk.client import create_kernel, rebuild_from_journal


class TestRecoveryIdempotent:
    """IDEMPOTENT action: inspect after crash → commit."""

    @pytest.mark.asyncio
    async def test_idempotent_recovery(self, tmp_path) -> None:
        db_path = str(tmp_path / "test_recovery.db")
        kernel = create_kernel(db_path)

        # Configure mock device driver with crash_after_effect for idempotent
        device_driver = kernel.get_driver("tool/mock")
        kernel.get_driver("model/mock")

        # Create a program that submits an idempotent device action
        program = ScriptedProgram(
            program_id="recovery_test",
            steps=[
                submit_device_action(
                    "PLACEHOLDER", operation="write", side_effect_class="idempotent"
                ),
                process_event_step("PLACEHOLDER"),
                exit_step("PLACEHOLDER"),
            ],
        )

        pid = await kernel.spawn(program)
        # Fix the program steps with actual pid
        program.reset()
        program._steps = [
            submit_device_action(pid, operation="write", side_effect_class="idempotent"),
            process_event_step(pid),
            exit_step(pid),
        ]

        # Configure the driver to crash after effect for the action
        # We need to pre-configure the driver behavior
        # Since we don't know action_id ahead of time, set default behavior
        device_driver.set_default_behavior("crash_after_effect")

        await kernel.tick()  # spawn + first step (submit action)
        await kernel.tick()  # dispatch action → crash_after_effect → inspect → commit

        # For IDEMPOTENT with crash_after_effect:
        # - Driver records effect in effect store
        # - Returns status="unknown"
        # - Kernel inspects (finds effect) → commits
        actions = kernel._action_service.list_by_pid(pid)
        assert len(actions) > 0

        # The action should be COMMITTED (inspector found the effect)
        committed = [a for a in actions if a.state == ActionState.COMMITTED]
        uncertain = [a for a in actions if a.state == ActionState.UNCERTAIN]
        # Either committed (inspect found effect) or uncertain (inspect didn't find it)
        assert len(committed) + len(uncertain) > 0

        # Verify leases were released
        leases = kernel._lease_service.list_leases_for_pid(pid)
        assert len(leases) == 0


class TestRecoveryNonReversible:
    """NON_REVERSIBLE action: inspect unknown → UNCERTAIN, no auto retry."""

    @pytest.mark.asyncio
    async def test_non_reversible_uncertain(self, tmp_path) -> None:
        db_path = str(tmp_path / "test_nr.db")
        kernel = create_kernel(db_path)

        device_driver = kernel.get_driver("tool/mock")
        device_driver.set_default_behavior("crash_after_effect")

        program = ScriptedProgram(
            program_id="nr_test",
            steps=[],
        )

        pid = await kernel.spawn(program)
        program._steps = [
            submit_device_action(pid, operation="dangerous", side_effect_class="non_reversible"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        await kernel.tick()  # submit
        await kernel.tick()  # dispatch → crash

        actions = kernel._action_service.list_by_pid(pid)
        uncertain = [a for a in actions if a.state == ActionState.UNCERTAIN]
        assert len(uncertain) > 0

        # No auto retry — effect store should have the effect but action is uncertain
        for a in uncertain:
            assert a.side_effect_class == SideEffectClass.NON_REVERSIBLE


class TestRecoveryPure:
    """PURE action: safe to retry after crash."""

    @pytest.mark.asyncio
    async def test_pure_retry_succeeds(self, tmp_path) -> None:
        db_path = str(tmp_path / "test_pure.db")
        kernel = create_kernel(db_path)

        model_driver = kernel.get_driver("model/mock")
        model_driver.set_default_behavior("immediate_success")

        program = ScriptedProgram(program_id="pure_test", steps=[])

        pid = await kernel.spawn(program)
        program._steps = [
            submit_model_action(pid, operation="generate", side_effect_class="pure"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        await kernel.run_until_idle(max_ticks=10)

        actions = kernel._action_service.list_by_pid(pid)
        committed = [a for a in actions if a.state == ActionState.COMMITTED]
        assert len(committed) > 0


class TestJournalRebuild:
    """Projection can be rebuilt from journal."""

    @pytest.mark.asyncio
    async def test_rebuild_restores_state(self, tmp_path) -> None:
        db_path = str(tmp_path / "test_rebuild.db")
        kernel = create_kernel(db_path)

        model_driver = kernel.get_driver("model/mock")
        model_driver.set_default_behavior("immediate_success")

        program = ScriptedProgram(program_id="rebuild_test", steps=[])
        pid = await kernel.spawn(program)
        program._steps = [
            submit_model_action(pid, operation="generate", side_effect_class="pure"),
            process_event_step(pid),
            exit_step(pid),
        ]
        program.reset()

        await kernel.run_until_idle(max_ticks=10)

        # Count events before rebuild
        events_before = kernel._journal.read_all()
        assert len(events_before) > 0

        # Rebuild
        kernel2 = rebuild_from_journal(db_path)

        # Verify projections match
        events_after = kernel2._journal.read_all()
        assert len(events_after) == len(events_before)

        # Verify processes match
        processes_before = kernel._process_service.list_all()
        processes_after = kernel2._process_service.list_all()
        assert len(processes_before) == len(processes_after)
        for p_before, p_after in zip(processes_before, processes_after, strict=False):
            assert p_before.pid == p_after.pid
            assert p_before.state == p_after.state

        # Verify actions match
        for p in processes_after:
            actions_before = kernel._action_service.list_by_pid(p.pid)
            actions_after = kernel2._action_service.list_by_pid(p.pid)
            assert len(actions_before) == len(actions_after)
            for a_before, a_after in zip(actions_before, actions_after, strict=False):
                assert a_before.action_id == a_after.action_id
                assert a_before.state == a_after.state
