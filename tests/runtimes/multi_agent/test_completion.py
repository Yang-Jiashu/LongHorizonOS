"""Task VERIFIED-driven completion flow (Section 24).

observe_vpg() derives claim COMPLETED from VPG task validity and releases
the backing Kernel lease.
"""

from __future__ import annotations

from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    AgentRegistry,
    ClaimState,
    create_scheduler,
)
from tests.runtimes.multi_agent.helpers import FakeVPG


def test_observe_vpg_completes_verified_claim():
    vpg = FakeVPG()
    reg = AgentRegistry()
    reg.register(AgentDescriptor(
        agent_id="a", process_id="pid-a",
        supported_task_kinds=("*",), specializations=("python",),
    ))
    sch = create_scheduler(
        reg, vpg=vpg, process_provider=_NullProc(),
        lease_provider=_LeaseRecorder(), capability_provider=_NullCap(),
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    # Simulate the VPG deriving the task as VERIFIED after a successful run.
    vpg.set_validity("t1", "verified")
    tally = sch.observe_vpg(vpg.graph_id)
    assert tally["claims_completed"] == 1
    completed = [c for c in sch.claims if c.state == ClaimState.COMPLETED]
    assert len(completed) == 1


def test_observe_vpg_is_idempotent():
    """Calling observe_vpg on an already-COMPLETED claim must not double-count."""
    vpg = FakeVPG()
    reg = AgentRegistry()
    reg.register(AgentDescriptor(
        agent_id="a", process_id="pid-a",
        supported_task_kinds=("*",), specializations=("python",),
    ))
    sch = create_scheduler(
        reg, vpg=vpg, process_provider=_NullProc(),
        lease_provider=_NullLease(), capability_provider=_NullCap(),
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    vpg.set_validity("t1", "verified")
    first = sch.observe_vpg(vpg.graph_id)
    second = sch.observe_vpg(vpg.graph_id)
    assert first["claims_completed"] == 1
    assert second["claims_completed"] == 0


def test_releasing_completed_claim_calls_kernel():
    """Completion path must release the backing Kernel lease."""
    vpg = FakeVPG()
    reg = AgentRegistry()
    reg.register(AgentDescriptor(
        agent_id="a", process_id="pid-a",
        supported_task_kinds=("*",), specializations=("python",),
    ))
    leases = _LeaseRecorder()
    sch = create_scheduler(
        reg, vpg=vpg, process_provider=_NullProc(),
        lease_provider=leases, capability_provider=_NullCap(),
    )
    vpg.add_ready_task("t1", required_specializations=("python",))
    sch.schedule_once(vpg.graph_id)
    vpg.set_validity("t1", "verified")
    sch.observe_vpg(vpg.graph_id)
    assert leases.released, "no lease was released on completion"


class _NullProc:
    def get(self, pid):
        return _P(pid)

    def list_all(self):
        return [_P("pid-a")]


class _P:
    def __init__(self, pid):
        self.pid = pid
        self.state = "ready"


class _NullLease:
    def acquire_exclusive(self, pid, resource_id, ttl):
        return _L(resource_id)

    def release(self, lease_id):
        return True

    def release_all_for_pid(self, pid):
        return 0

    def get(self, lease_id):
        return None

    def list_for_resource(self, resource_id):
        return []

    def list_for_pid(self, pid):
        return []

    def reclaim_expired(self):
        return 0


class _LeaseRecorder(_NullLease):
    def __init__(self):
        self.released = False

    def release(self, lease_id):
        self.released = True
        return True


class _L:
    def __init__(self, resource_id):
        from datetime import datetime, timedelta, timezone
        self.lease_id = f"lease-{resource_id}"
        self.resource_id = resource_id
        self.expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)


class _NullCap:
    def check(self, pid, resource, operation):
        return True

    def capabilities_for(self, pid):
        return []
