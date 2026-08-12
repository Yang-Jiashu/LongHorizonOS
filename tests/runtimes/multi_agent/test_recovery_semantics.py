"""Recovery semantics across Scheduler, VPG, and Kernel lease authority."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed
from lhos.agent_os.sdk.client import create_kernel
from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry, ClaimState, create_scheduler
from lhos.runtimes.multi_agent.lease_adapter import claim_resource_uri
from lhos.sdk.providers import KernelLeaseProvider
from tests.runtimes.multi_agent.helpers import FakeVPG


class _Proc:
    def __init__(self, alive: bool = True) -> None:
        self.alive = alive

    def get(self, pid: str) -> Any | None:
        if not self.alive:
            return None
        return type("Proc", (), {"pid": pid, "state": "ready"})()

    def list_all(self) -> list[Any]:
        return []


class _Cap:
    def check(self, pid: str, resource: str, operation: str) -> bool:
        return True

    def capabilities_for(self, pid: str) -> list[Any]:
        return []


class _Lease:
    def __init__(self, lease_id: str, resource_id: str, owner_pid: str) -> None:
        self.lease_id = lease_id
        self.resource_id = resource_id
        self.owner_pid = owner_pid
        self.expires_at = None


class _TrackingLeases:
    def __init__(self, *, refuse: bool = False, raise_refusal: bool = False) -> None:
        self.refuse = refuse
        self.raise_refusal = raise_refusal
        self.live = True
        self.released: list[str] = []
        self._leases: dict[str, _Lease] = {}
        self._next = 0

    def acquire_exclusive(self, pid: str, resource_id: str, ttl: Any) -> Any | None:
        if self.raise_refusal:
            raise LeaseAcquisitionFailed(pid, resource_id)
        if self.refuse:
            return None
        self._next += 1
        lease = _Lease(f"lease-{self._next}", resource_id, pid)
        self._leases[lease.lease_id] = lease
        return lease

    def release(self, lease_id: str) -> bool:
        self.released.append(lease_id)
        return self._leases.pop(lease_id, None) is not None

    def release_all_for_pid(self, pid: str) -> int:
        matching = [lid for lid, lease in self._leases.items() if lease.owner_pid == pid]
        for lease_id in matching:
            self.release(lease_id)
        return len(matching)

    def get(self, lease_id: str) -> Any | None:
        return self._leases.get(lease_id) if self.live else None

    def list_for_resource(self, resource_id: str) -> list[Any]:
        if not self.live:
            return []
        return [lease for lease in self._leases.values() if lease.resource_id == resource_id]

    def list_for_pid(self, pid: str) -> list[Any]:
        if not self.live:
            return []
        return [lease for lease in self._leases.values() if lease.owner_pid == pid]

    def reclaim_expired(self) -> int:
        return 0


def _scheduler(vpg: FakeVPG, leases: Any, process: _Proc | None = None):
    registry = AgentRegistry()
    registry.register(
        AgentDescriptor(
            agent_id="a",
            process_id="pid-a",
            supported_task_kinds=("*",),
            specializations=("python",),
        )
    )
    return create_scheduler(
        registry,
        vpg=vpg,
        process_provider=process or _Proc(),
        lease_provider=leases,
        capability_provider=_Cap(),
    )


def _ready() -> FakeVPG:
    vpg = FakeVPG()
    vpg.add_ready_task("t1", required_specializations=("python",))
    return vpg


def test_kernel_provider_normalizes_only_lease_contention() -> None:
    kernel = create_kernel(":memory:")
    provider = KernelLeaseProvider(kernel)
    pid1 = kernel._process_service.spawn("owner-1").pid
    pid2 = kernel._process_service.spawn("owner-2").pid
    resource = claim_resource_uri("g", "t")

    first = provider.acquire_exclusive(pid1, resource, timedelta(minutes=1))
    assert first is not None
    assert provider.acquire_exclusive(pid2, resource, timedelta(minutes=1)) is None


def test_unexpected_provider_error_still_propagates() -> None:
    class _Broken(_TrackingLeases):
        def acquire_exclusive(self, pid: str, resource_id: str, ttl: Any) -> Any | None:
            raise RuntimeError("lease database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        _scheduler(_ready(), _Broken()).schedule_once("graph-1")


def test_refused_lease_records_rejected_claim_and_can_retry() -> None:
    vpg = _ready()
    leases = _TrackingLeases(refuse=True)
    scheduler = _scheduler(vpg, leases)

    first = scheduler.schedule_once(vpg.graph_id)

    assert first.dispatched == []
    assert first.skipped == [("t1", "claim race lost / kernel refused lease")]
    assert scheduler.claims[-1].state == ClaimState.REJECTED
    assert scheduler.attempts == []

    leases.refuse = False
    second = scheduler.schedule_once(vpg.graph_id)
    assert len(second.dispatched) == 1
    assert scheduler.active_claim_for_task("t1", vpg.graph_id) is not None


def test_stale_active_claim_releases_lease_and_allows_fresh_claim() -> None:
    vpg = _ready()
    leases = _TrackingLeases()
    scheduler = _scheduler(vpg, leases)
    first = scheduler.schedule_once(vpg.graph_id)
    old_claim = scheduler.claims[-1]

    vpg.set_validity("t1", "stale")
    reconciled = scheduler.reconcile()

    assert reconciled.claims_marked_lost == 1
    assert old_claim.state == ClaimState.LOST
    assert old_claim.reason == "vpg_task_stale_claim_lost"
    assert old_claim.lease_id in leases.released

    vpg.set_validity("t1", "unverified")
    second = scheduler.schedule_once(vpg.graph_id)
    assert len(second.dispatched) == 1
    assert second.dispatched[0]["claim_id"] != first.dispatched[0]["claim_id"]


def test_vanished_lease_clears_idempotency_for_reassignment() -> None:
    vpg = _ready()
    leases = _TrackingLeases()
    scheduler = _scheduler(vpg, leases)
    first = scheduler.schedule_once(vpg.graph_id)

    leases.live = False
    reconciled = scheduler.reconcile()
    assert reconciled.claims_marked_lost == 1
    assert scheduler.claims[-1].state == ClaimState.LOST

    leases.live = True
    second = scheduler.schedule_once(vpg.graph_id)
    assert len(second.dispatched) == 1
    assert second.dispatched[0]["claim_id"] != first.dispatched[0]["claim_id"]
    assert all(reason != "idempotent replay" for _, reason in second.skipped)


def test_dead_process_loss_clears_idempotency_for_reassignment() -> None:
    vpg = _ready()
    leases = _TrackingLeases()
    process = _Proc()
    scheduler = _scheduler(vpg, leases, process)
    first = scheduler.schedule_once(vpg.graph_id)

    process.alive = False
    scheduler.reconcile()
    assert scheduler.claims[-1].state == ClaimState.LOST

    process.alive = True
    second = scheduler.schedule_once(vpg.graph_id)
    assert len(second.dispatched) == 1
    assert second.dispatched[0]["claim_id"] != first.dispatched[0]["claim_id"]
