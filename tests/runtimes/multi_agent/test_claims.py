"""Claim lifecycle and Kernel-lease binding."""
from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent import ClaimState, TaskClaim
from lhos.runtimes.multi_agent.claims import ClaimManager
from lhos.runtimes.multi_agent.lease_adapter import claim_resource_uri


class FakeLease:
    def __init__(self, lease_id, resource_id, owner_pid):
        self.lease_id = lease_id
        self.resource_id = resource_id
        self.owner_pid = owner_pid
        self.mode = "exclusive"


class FakeLeaseProvider:
    def __init__(self, fail_first=False):
        self.acquired = []
        self.released = []
        self._live_leases: dict[str, FakeLease] = {}
        self.fail_first = fail_first

    def acquire_exclusive(self, pid, resource_id, ttl):
        if self.fail_first:
            return None
        lease = FakeLease(
            lease_id=f"lease-{pid}-{resource_id}",
            resource_id=resource_id,
            owner_pid=pid,
        )
        self.acquired.append((pid, resource_id))
        self._live_leases[lease.lease_id] = lease
        return lease

    def release(self, lease_id):
        if lease_id is None or lease_id not in self._live_leases:
            return False
        self._live_leases.pop(lease_id)
        self.released.append(lease_id)
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


def _adapter():
    from lhos.runtimes.multi_agent.lease_adapter import LeaseAdapter

    return LeaseAdapter(FakeLeaseProvider())


def _adapter_failing():
    from lhos.runtimes.multi_agent.lease_adapter import LeaseAdapter

    return LeaseAdapter(FakeLeaseProvider(fail_first=True))


def test_claim_lifecycle_proposed_to_active():
    mgr = ClaimManager(_adapter())
    c = mgr.propose(
        claim_id="c1",
        graph_id="g1",
        graph_version=1,
        task_id="t1",
        agent_id="a1",
        process_id="p1",
        lease_resource=claim_resource_uri("g1", "t1"),
    )
    assert c.state == ClaimState.PROPOSED
    mgr.mark_acquiring(c)
    assert c.state == ClaimState.ACQUIRING
    assert mgr.try_acquire_lease(c) is True
    assert c.state == ClaimState.ACTIVE
    assert c.lease_id is not None


def test_claim_lease_refusal_marks_rejected():
    mgr = ClaimManager(_adapter_failing())
    c = mgr.propose(
        claim_id="c2",
        graph_id="g1",
        graph_version=1,
        task_id="t1",
        agent_id="a1",
        process_id="p1",
        lease_resource=claim_resource_uri("g1", "t1"),
    )
    mgr.mark_acquiring(c)
    assert mgr.try_acquire_lease(c) is False
    assert c.state == ClaimState.REJECTED


def test_claim_completes_and_releases_lease():
    from lhos.runtimes.multi_agent.lease_adapter import LeaseAdapter
    provider = FakeLeaseProvider()
    mgr = ClaimManager(LeaseAdapter(provider))
    c = mgr.propose(
        claim_id="c3",
        graph_id="g1",
        graph_version=1,
        task_id="t1",
        agent_id="a1",
        process_id="p1",
        lease_resource=claim_resource_uri("g1", "t1"),
    )
    mgr.mark_acquiring(c)
    mgr.try_acquire_lease(c)
    mgr.complete(c)
    assert c.state == ClaimState.COMPLETED
    assert provider.released == [c.lease_id]


def test_claim_lost_path():
    mgr = ClaimManager(_adapter())
    c = mgr.propose(
        claim_id="c4",
        graph_id="g1",
        graph_version=1,
        task_id="t1",
        agent_id="a1",
        process_id="p1",
        lease_resource=claim_resource_uri("g1", "t1"),
    )
    mgr.mark_acquiring(c)
    mgr.try_acquire_lease(c)
    mgr.mark_lost(c, reason="process_dead")
    assert c.state == ClaimState.LOST


def test_active_claim_counts():
    mgr = ClaimManager(_adapter())
    c1 = mgr.propose(
        claim_id="c5",
        graph_id="g1",
        graph_version=1,
        task_id="t1",
        agent_id="a1",
        process_id="p1",
        lease_resource=claim_resource_uri("g1", "t1"),
    )
    c2 = mgr.propose(
        claim_id="c6",
        graph_id="g1",
        graph_version=1,
        task_id="t2",
        agent_id="a1",
        process_id="p1",
        lease_resource=claim_resource_uri("g1", "t2"),
    )
    for c in (c1, c2):
        mgr.mark_acquiring(c)
        mgr.try_acquire_lease(c)
    assert mgr.current_active_claims("a1") == 2
    assert len(mgr.active_claims_for_task("g1", "t1")) == 1
    mgr.release(c1)
    assert mgr.current_active_claims("a1") == 1


def test_release_is_idempotent_on_lease():
    from lhos.runtimes.multi_agent.lease_adapter import LeaseAdapter
    provider = FakeLeaseProvider()
    mgr = ClaimManager(LeaseAdapter(provider))
    c = mgr.propose(
        claim_id="c7",
        graph_id="g1",
        graph_version=1,
        task_id="t1",
        agent_id="a1",
        process_id="p1",
        lease_resource=claim_resource_uri("g1", "t1"),
    )
    mgr.mark_acquiring(c)
    mgr.try_acquire_lease(c)
    n0 = len(provider.released)
    mgr.release(c)
    mgr.release(c)  # second release must not double-release
    assert len(provider.released) == n0 + 1
