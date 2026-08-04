"""Test Deadlock Detection and Recovery."""

from __future__ import annotations

import pytest

from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed
from lhos.agent_os.kernel.models import ProcessState
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture
def storage() -> SQLiteStorage:
    return SQLiteStorage(":memory:")


@pytest.fixture
def journal(storage: SQLiteStorage) -> JournalService:
    return JournalService(storage)


@pytest.fixture
def lease_service(storage: SQLiteStorage, journal: JournalService) -> LeaseService:
    return LeaseService(storage, journal)


class TestAtomicAcquirePreventsDeadlock:
    """Atomic acquire prevents hold-and-wait deadlock pattern."""

    def test_atomic_acquire_prevents_partial_hold(self, lease_service: LeaseService) -> None:
        # p1 acquires R1
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "exclusive"}])

        # p2 acquires R2
        lease_service.atomic_acquire("p2", [{"resource_id": "resource:R2", "mode": "exclusive"}])

        # p1 tries to get R2 (would need to wait) — cannot get R1+R2 atomically
        with pytest.raises(LeaseAcquisitionFailed):
            lease_service.atomic_acquire(
                "p1",
                [
                    {"resource_id": "resource:R1", "mode": "exclusive"},
                    {"resource_id": "resource:R2", "mode": "exclusive"},
                ],
            )

        # p1 still only holds R1
        p1_leases = lease_service.list_leases_for_pid("p1")
        assert len(p1_leases) == 1
        assert p1_leases[0].resource_id == "resource:R1"


class TestDeadlockDetection:
    """Detect cycles in wait-for graph."""

    def test_no_cycle_when_no_waiting(self, lease_service: LeaseService) -> None:
        cycles = lease_service.detect_deadlocks()
        assert cycles == []

    def test_detect_simple_cycle(self, lease_service: LeaseService) -> None:
        # p1 holds R1, waits for R2
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "exclusive"}])
        lease_service.atomic_acquire("p2", [{"resource_id": "resource:R2", "mode": "exclusive"}])

        # Simulate waiting (add waiters manually since atomic_acquire fails)
        lease_service._add_waiter("p1", "resource:R2")
        lease_service._add_waiter("p2", "resource:R1")

        cycles = lease_service.detect_deadlocks()
        assert len(cycles) >= 1

    def test_no_cycle_for_one_way_wait(self, lease_service: LeaseService) -> None:
        # p1 holds R1, p2 waits for R1 — no cycle
        lease_service.atomic_acquire("p1", [{"resource_id": "resource:R1", "mode": "exclusive"}])
        lease_service._add_waiter("p2", "resource:R1")

        cycles = lease_service.detect_deadlocks()
        assert cycles == []


class TestDeadlockRecoveryVictimSelection:
    """Victim selection is deterministic."""

    def test_lower_priority_is_victim(
        self, storage: SQLiteStorage, journal: JournalService
    ) -> None:
        from lhos.agent_os.kernel.dispatcher import SyscallDispatcher
        from lhos.agent_os.kernel.kernel import AgentKernel
        from lhos.agent_os.kernel.models import Clock
        from lhos.agent_os.services.action_service import ActionService
        from lhos.agent_os.services.capability_service import CapabilityService
        from lhos.agent_os.services.process_service import ProcessService
        from lhos.agent_os.services.signal_service import SignalService

        clock = Clock()
        ps = ProcessService(storage, journal, clock)
        acs = ActionService(storage, journal)
        cs = CapabilityService(storage, journal)
        ls = LeaseService(storage, journal)
        ss = SignalService(storage, journal, ps)
        disp = SyscallDispatcher(storage, journal, ps, acs, cs, ls, ss)
        kernel = AgentKernel(storage, journal, ps, acs, cs, ls, ss, disp, clock)

        # p1: priority=5, holds R1
        pcb1 = ps.spawn("prog1", priority=5)
        ls.atomic_acquire(pcb1.pid, [{"resource_id": "resource:R1", "mode": "exclusive"}])

        # p2: priority=10, holds R2
        pcb2 = ps.spawn("prog2", priority=10)
        ls.atomic_acquire(pcb2.pid, [{"resource_id": "resource:R2", "mode": "exclusive"}])

        # Create cycle
        ls._add_waiter(pcb1.pid, "resource:R2")
        ls._add_waiter(pcb2.pid, "resource:R1")

        # Both go to BLOCKED
        ps.transition(pcb1.pid, ProcessState.RUNNING)
        ps.transition(pcb1.pid, ProcessState.BLOCKED, wait_condition={"resource": "R2"})
        ps.transition(pcb2.pid, ProcessState.RUNNING)
        ps.transition(pcb2.pid, ProcessState.BLOCKED, wait_condition={"resource": "R1"})

        cycle = [pcb1.pid, pcb2.pid]
        victim = kernel._select_victim(cycle)

        # p1 has lower priority (5 < 10) → should be victim
        assert victim == pcb1.pid
