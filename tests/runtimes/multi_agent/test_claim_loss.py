"""Claim LOST transitions (Section 25)."""

from __future__ import annotations

from datetime import UTC

from lhos.runtimes.multi_agent.claims import ClaimManager
from lhos.runtimes.multi_agent.lease_adapter import LeaseAdapter
from lhos.runtimes.multi_agent.models import ClaimState


class _L:
    def __init__(self, lease_id, resource_id):
        from datetime import datetime, timedelta
        self.lease_id = lease_id
        self.resource_id = resource_id
        self.expires_at = datetime.now(UTC) + timedelta(minutes=30)


class _Leases:
    def __init__(self):
        self.acquired = []
        self.released = []

    def acquire_exclusive(self, pid, resource_id, ttl):
        self.acquired.append((pid, resource_id))
        return _L(f"lease-{resource_id}", resource_id)

    def release(self, lease_id):
        self.released.append(lease_id)
        return True

    def release_all_for_pid(self, pid):
        return 0

    def get(self, lease_id):
        return _L(lease_id, lease_id)

    def list_for_resource(self, resource_id):
        return [_L(f"lease-{resource_id}", resource_id)]

    def list_for_pid(self, pid):
        return []

    def reclaim_expired(self):
        return 0


def _mgr():
    return ClaimManager(LeaseAdapter(_Leases()))


def _propose(mgr, claim_id="c1", task_id="t1"):
    c = mgr.propose(
        claim_id=claim_id, graph_id="g", graph_version=1, task_id=task_id,
        agent_id="a", process_id="p", lease_resource=f"vpg://g/task/{task_id}/claim",
    )
    mgr.mark_acquiring(c)
    return c


def test_lease_refusal_marks_rejected_then_lost():
    """If the Kernel refuses the exclusive lease, claim is REJECTED and can
    be flipped to LOST by the caller (reconciliation/recovery)."""

    class _Refusing(_Leases):
        def acquire_exclusive(self, pid, resource_id, ttl):
            return None

    mgr = ClaimManager(LeaseAdapter(_Refusing()))
    c = _propose(mgr)
    assert mgr.try_acquire_lease(c) is False
    assert c.state == ClaimState.REJECTED
    mgr.mark_lost(c, reason="after_refusal_cleanup")
    assert c.state == ClaimState.LOST
    assert c.released_at is not None


def test_assert_active_has_live_lease_raises_when_missing():
    from lhos.runtimes.multi_agent.errors import KernelLeaseRequired
    mgr = _mgr()
    c = _propose(mgr)
    # PROPOSED claim: invariant only fires for ACTIVE.
    try:
        mgr.assert_active_has_live_lease(c)
    except KernelLeaseRequired as exc:
        raise AssertionError("PROPOSED claim should not trip ACTIVE-only invariant") from exc
    # Force an ACTIVE claim without lease_id to trip the invariant.
    c.state = ClaimState.ACTIVE
    c.lease_id = None
    try:
        mgr.assert_active_has_live_lease(c)
    except KernelLeaseRequired:
        return
    raise AssertionError("ACTIVE claim with no lease must raise KernelLeaseRequired")


def test_release_method_marks_released():
    mgr = _mgr()
    c = _propose(mgr)
    assert mgr.try_acquire_lease(c)
    mgr.release(c, reason="abort")
    assert c.state == ClaimState.RELEASED
    assert c.reason == "abort"


def test_complete_releases_lease():
    leases = _Leases()
    mgr = ClaimManager(LeaseAdapter(leases))
    c = _propose(mgr)
    assert mgr.try_acquire_lease(c)
    mgr.complete(c)
    assert c.state == ClaimState.COMPLETED
    assert c.lease_id in leases.released


def test_current_active_claims_count():
    mgr = _mgr()
    c1 = _propose(mgr, claim_id="c1", task_id="t1")
    c2 = _propose(mgr, claim_id="c2", task_id="t2")
    assert mgr.try_acquire_lease(c1)
    assert mgr.try_acquire_lease(c2)
    assert mgr.current_active_claims("a") == 2
    assert mgr.active_claims_for_agent("a") == [c1, c2]
