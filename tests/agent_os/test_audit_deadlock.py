"""Audit: Deadlock prevention, detection, recovery, and non-deadlock scenarios."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed
from lhos.agent_os.kernel.models import ProcessState
from lhos.agent_os.sdk.client import create_kernel


class TestDeadlockAudit:
    """Comprehensive deadlock audit."""

    def test_atomic_acquire_prevents_hold_and_wait(self) -> None:
        """Two processes using atomic multi-acquire cannot deadlock."""
        kernel = create_kernel(":memory:")

        # p1 atomically acquires R1+R2
        kernel._lease_service.atomic_acquire(
            "p1",
            [
                {"resource_id": "resource:R1", "mode": "exclusive"},
                {"resource_id": "resource:R2", "mode": "exclusive"},
            ],
        )

        # p2 tries to atomically acquire R1+R2 — fails (both held)
        with pytest.raises(LeaseAcquisitionFailed):
            kernel._lease_service.atomic_acquire(
                "p2",
                [
                    {"resource_id": "resource:R1", "mode": "exclusive"},
                    {"resource_id": "resource:R2", "mode": "exclusive"},
                ],
            )

        # p2 holds nothing — no deadlock
        cycles = kernel._lease_service.detect_deadlocks()
        assert cycles == []

    def test_deadlock_detected_via_wait_for_graph(self) -> None:
        """DFS cycle detection finds circular wait."""
        kernel = create_kernel(":memory:")

        # p1 holds R1, p2 holds R2
        kernel._lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        kernel._lease_service.atomic_acquire(
            "p2", [{"resource_id": "resource:R2", "mode": "exclusive"}]
        )

        # p1 waits for R2 (held by p2), p2 waits for R1 (held by p1)
        kernel._lease_service._add_waiter("p1", "resource:R2")
        kernel._lease_service._add_waiter("p2", "resource:R1")

        cycles = kernel._lease_service.detect_deadlocks()
        assert len(cycles) >= 1
        assert "p1" in cycles[0]
        assert "p2" in cycles[0]

    @pytest.mark.asyncio
    async def test_deadlock_recovery_selects_victim(self) -> None:
        """Deadlock recovery selects a victim deterministically."""
        kernel = create_kernel(":memory:")

        # Create two processes
        from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step

        p1 = ScriptedProgram(program_id="dl_p1", steps=[exit_step("p1")])
        p2 = ScriptedProgram(program_id="dl_p2", steps=[exit_step("p2")])
        pid1 = await kernel.spawn(p1)
        pid2 = await kernel.spawn(p2)

        # p1 holds R1, p2 holds R2
        kernel._lease_service.atomic_acquire(
            pid1, [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        kernel._lease_service.atomic_acquire(
            pid2, [{"resource_id": "resource:R2", "mode": "exclusive"}]
        )

        # Create wait-for cycle
        kernel._lease_service._add_waiter(pid1, "resource:R2")
        kernel._lease_service._add_waiter(pid2, "resource:R1")

        cycles = kernel._lease_service.detect_deadlocks()
        assert len(cycles) >= 1

        # Recover
        await kernel._recover_deadlock(cycles[0])

        # Verify DEADLOCK_DETECTED and DEADLOCK_RECOVERED events
        events = kernel._journal.read_all()
        detected = [e for e in events if e.event_type == "DEADLOCK_DETECTED"]
        recovered = [e for e in events if e.event_type == "DEADLOCK_RECOVERED"]
        assert len(detected) >= 1
        assert len(recovered) >= 1

        # Victim should be FAILED
        victim_pid = detected[0].payload["victim"]
        victim_pcb = kernel._process_service.get_process(victim_pid)
        assert victim_pcb is not None
        assert victim_pcb.state == ProcessState.FAILED

        # Victim's leases should be released
        victim_leases = kernel._lease_service.list_leases_for_pid(victim_pid)
        assert len(victim_leases) == 0

    def test_non_deadlock_wait_not_detected(self) -> None:
        """Waiting for a timer or user message is not a deadlock."""
        kernel = create_kernel(":memory:")

        # p1 holds R1, p2 waits for R1 (but p2 holds nothing)
        kernel._lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        kernel._lease_service._add_waiter("p2", "resource:R1")

        # No cycle — p2 waits for p1, but p1 doesn't wait for p2
        cycles = kernel._lease_service.detect_deadlocks()
        assert cycles == []

    def test_non_deadlock_chain_not_detected(self) -> None:
        """A chain (p1→p2→p3) without a cycle is not a deadlock."""
        kernel = create_kernel(":memory:")

        # p1 holds R1, p2 holds R2, p3 holds R3
        kernel._lease_service.atomic_acquire(
            "p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        kernel._lease_service.atomic_acquire(
            "p2", [{"resource_id": "resource:R2", "mode": "exclusive"}]
        )
        kernel._lease_service.atomic_acquire(
            "p3", [{"resource_id": "resource:R3", "mode": "exclusive"}]
        )

        # p2 waits for R1 (held by p1), p3 waits for R2 (held by p2)
        # But p1 doesn't wait for anyone — no cycle
        kernel._lease_service._add_waiter("p2", "resource:R1")
        kernel._lease_service._add_waiter("p3", "resource:R2")

        cycles = kernel._lease_service.detect_deadlocks()
        assert cycles == []

    def test_victim_selection_is_deterministic(self) -> None:
        """Same deadlock → same victim."""
        kernel = create_kernel(":memory:")

        # Create processes with different priorities
        from lhos.agent_os.kernel.models import ProcessControlBlock

        p1 = ProcessControlBlock(
            pid="vic_p1",
            program_id="t",
            capability_set_id="c",
            namespace_id="n",
            state=ProcessState.BLOCKED,
            wait_condition={"x": 1},
            priority=5,
        )
        p2 = ProcessControlBlock(
            pid="vic_p2",
            program_id="t",
            capability_set_id="c",
            namespace_id="n",
            state=ProcessState.BLOCKED,
            wait_condition={"x": 1},
            priority=10,
        )
        kernel._process_service._upsert_projection(p1)
        kernel._process_service._upsert_projection(p2)

        # p1 holds R1, p2 holds R2
        kernel._lease_service.atomic_acquire(
            "vic_p1", [{"resource_id": "resource:R1", "mode": "exclusive"}]
        )
        kernel._lease_service.atomic_acquire(
            "vic_p2", [{"resource_id": "resource:R2", "mode": "exclusive"}]
        )
        kernel._lease_service._add_waiter("vic_p1", "resource:R2")
        kernel._lease_service._add_waiter("vic_p2", "resource:R1")

        cycle = ["vic_p1", "vic_p2"]
        victim = kernel._select_victim(cycle)

        # Lower priority (5) should be selected as victim
        assert victim == "vic_p1"
