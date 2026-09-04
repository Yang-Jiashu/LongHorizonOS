"""Scheduler integration for atomic resource-vector reservations."""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from lhos.runtimes.multi_agent import (
    ClaimState,
    ResourceVector,
    SchedulerStateCorruption,
)
from lhos.runtimes.multi_agent.durable_state import _canonical_json, _hash_state
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def _task_resources(**resources: Any) -> dict[str, Any]:
    return {"scheduler": {"resources": resources}}


def _agent(
    *,
    cost_weight: int = 100,
    cpu_millis: int = 0,
    gpu_count: int = 0,
) -> dict[str, Any]:
    return {
        "supported_task_kinds": ("*",),
        "max_concurrency": 4,
        "cost_weight": cost_weight,
        "resource_capacity": ResourceVector(
            cpu_millis=cpu_millis,
            gpu_count=gpu_count,
        ),
    }


def test_resource_eligibility_selects_an_alternate_capable_agent() -> None:
    vpg = FakeVPG()
    scheduler = fake_scheduler(
        {
            "cheap-cpu": _agent(cost_weight=1, cpu_millis=4000),
            "gpu-agent": _agent(cost_weight=100, cpu_millis=4000, gpu_count=1),
        },
        fake_vpg=vpg,
    )
    vpg.add_ready_task(
        "gpu-task",
        metadata_extra=_task_resources(cpu_millis=1000, gpu_count=1),
    )

    result = scheduler.schedule_once(vpg.graph_id)

    assert result.dispatched[0]["agent_id"] == "gpu-agent"
    claim = scheduler.active_claim_for_task("gpu-task", vpg.graph_id)
    assert claim.reserved_resources == ResourceVector(cpu_millis=1000, gpu_count=1)
    assert claim.resource_reservation_id


def test_active_reservation_blocks_second_claim_then_release_permits_retry() -> None:
    vpg = FakeVPG()
    scheduler = fake_scheduler(
        {"agent": _agent(cpu_millis=1000)},
        fake_vpg=vpg,
    )
    for task_id in ("t1", "t2"):
        vpg.add_ready_task(
            task_id,
            metadata_extra=_task_resources(cpu_millis=1000),
        )

    first = scheduler.schedule_once(vpg.graph_id)

    assert [item["task_id"] for item in first.dispatched] == ["t1"]
    assert any(
        task_id == "t2" and "insufficient resources: cpu_millis=1000" in reason
        for task_id, reason in first.skipped
    )
    scheduler.release_task(vpg.graph_id, "t1")
    vpg.ready_candidates = [
        candidate for candidate in vpg.ready_candidates if candidate.task_id != "t1"
    ]
    second = scheduler.schedule_once(vpg.graph_id)
    assert [item["task_id"] for item in second.dispatched] == ["t2"]


class _TrackedLeaseProvider:
    def __init__(
        self,
        *,
        refuse: bool = False,
        acquire_error: BaseException | None = None,
        release_error: BaseException | None = None,
    ) -> None:
        self.refuse = refuse
        self.acquire_error = acquire_error
        self.release_error = release_error
        self.leases: dict[str, Any] = {}

    def acquire_exclusive(self, pid: str, resource_id: str, ttl: Any) -> Any | None:
        if self.acquire_error is not None:
            raise self.acquire_error
        if self.refuse:
            return None
        from datetime import UTC, datetime, timedelta

        lease = type(
            "Lease",
            (),
            {
                "lease_id": f"lease:{resource_id}",
                "resource_id": resource_id,
                "owner_pid": pid,
                "fencing_token": 1,
                "expires_at": datetime.now(UTC) + timedelta(minutes=30),
            },
        )()
        self.leases[lease.lease_id] = lease
        return lease

    def release(self, lease_id: str) -> bool:
        if self.release_error is not None:
            raise self.release_error
        return self.leases.pop(lease_id, None) is not None

    def release_all_for_pid(self, pid: str) -> int:
        return 0

    def get(self, lease_id: str) -> Any | None:
        return self.leases.get(lease_id)

    def list_for_resource(self, resource_id: str) -> list[Any]:
        return [lease for lease in self.leases.values() if lease.resource_id == resource_id]

    def list_for_pid(self, pid: str) -> list[Any]:
        return [lease for lease in self.leases.values() if lease.owner_pid == pid]

    def reclaim_expired(self) -> int:
        return 0


def _scheduler_with_lease_provider(
    vpg: FakeVPG,
    lease_provider: _TrackedLeaseProvider,
    **scheduler_kwargs: Any,
):
    from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry, create_scheduler

    registry = AgentRegistry()
    registry.register(
        AgentDescriptor(
            agent_id="agent",
            process_id="pid-agent",
            supported_task_kinds=("*",),
            max_concurrency=4,
            resource_capacity=ResourceVector(cpu_millis=1000),
        )
    )

    class _Proc:
        def get(self, pid: str) -> Any:
            return type("Process", (), {"pid": pid, "state": "ready"})()

    return create_scheduler(
        registry,
        vpg=vpg,
        process_provider=_Proc(),
        lease_provider=lease_provider,
        **scheduler_kwargs,
    )


def test_kernel_lease_refusal_releases_resource_reservation() -> None:
    vpg = FakeVPG()
    leases = _TrackedLeaseProvider(refuse=True)
    scheduler = _scheduler_with_lease_provider(vpg, leases)
    vpg.add_ready_task("t1", metadata_extra=_task_resources(cpu_millis=1000))

    result = scheduler.schedule_once(vpg.graph_id)

    assert result.dispatched == []
    assert scheduler._s.resource_manager.available("agent") == ResourceVector(cpu_millis=1000)
    assert scheduler.claims[0].state == ClaimState.REJECTED


def test_kernel_lease_exception_is_durable_rejected_and_does_not_leak() -> None:
    vpg = FakeVPG()
    leases = _TrackedLeaseProvider(acquire_error=RuntimeError("lease service down"))
    scheduler = _scheduler_with_lease_provider(vpg, leases)
    vpg.add_ready_task("t1", metadata_extra=_task_resources(cpu_millis=1000))

    with pytest.raises(RuntimeError, match="lease service down"):
        scheduler.schedule_once(vpg.graph_id)

    assert scheduler.claims[0].state == ClaimState.REJECTED
    assert scheduler._s.resource_manager.available("agent") == ResourceVector(cpu_millis=1000)


def test_release_failure_keeps_active_reservation_fail_closed() -> None:
    vpg = FakeVPG()
    leases = _TrackedLeaseProvider()
    scheduler = _scheduler_with_lease_provider(vpg, leases)
    vpg.add_ready_task("t1", metadata_extra=_task_resources(cpu_millis=1000))
    scheduler.schedule_once(vpg.graph_id)
    claim = scheduler.active_claim_for_task("t1", vpg.graph_id)
    leases.release_error = RuntimeError("lease release failed")

    with pytest.raises(Exception, match="lease release failed"):
        scheduler._s.release_claim(claim)

    assert claim.state == ClaimState.ACTIVE
    assert scheduler._s.resource_manager.available("agent") == ResourceVector()


def test_reconcile_lost_claim_releases_resource_reservation() -> None:
    vpg = FakeVPG()
    leases = _TrackedLeaseProvider()
    scheduler = _scheduler_with_lease_provider(vpg, leases)
    vpg.add_ready_task("t1", metadata_extra=_task_resources(cpu_millis=1000))
    scheduler.schedule_once(vpg.graph_id)
    claim = scheduler.active_claim_for_task("t1", vpg.graph_id)
    leases.leases.clear()

    result = scheduler.reconcile()

    assert result.claims_marked_lost == 1
    assert claim.state == ClaimState.LOST
    assert scheduler._s.resource_manager.available("agent") == ResourceVector(cpu_millis=1000)


def test_reconcile_recovers_kernel_lease_for_active_row_without_lease_id() -> None:
    """Durable ACTIVE rows missing a lease id must not leak discovered leases."""
    vpg = FakeVPG()
    leases = _TrackedLeaseProvider()
    scheduler = _scheduler_with_lease_provider(vpg, leases)
    vpg.add_ready_task("t1", metadata_extra=_task_resources(cpu_millis=1000))
    scheduler.schedule_once(vpg.graph_id)
    claim = scheduler.active_claim_for_task("t1", vpg.graph_id)
    lease_id = claim.lease_id

    # Simulate a torn projection write: the Kernel lease still exists, but
    # the claim's durable lease binding was lost.
    claim.lease_id = None
    result = scheduler.reconcile()

    assert result.claims_marked_lost == 0
    assert result.claims_completed == 0
    assert claim.state == ClaimState.ACTIVE
    assert claim.lease_id == lease_id
    assert lease_id in leases.leases


def test_retire_agent_process_recovers_and_releases_missing_lease_id() -> None:
    """Process fencing must clean up a lease found from authoritative state."""
    vpg = FakeVPG()
    leases = _TrackedLeaseProvider()
    scheduler = _scheduler_with_lease_provider(vpg, leases)
    vpg.add_ready_task("t1")
    scheduler.schedule_once(vpg.graph_id)
    claim = scheduler.active_claim_for_task("t1", vpg.graph_id)
    lease_id = claim.lease_id
    claim.lease_id = None

    retired = scheduler.retire_agent_process("agent", "pid-new")

    assert retired == 1
    assert claim.state == ClaimState.LOST
    assert claim.lease_id == lease_id
    assert lease_id not in leases.leases


def test_durable_restart_restores_active_resource_reservation(tmp_path) -> None:
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    first = fake_scheduler(
        {"agent": _agent(cpu_millis=1000)},
        fake_vpg=vpg,
        state_path=str(db),
    )
    for task_id in ("t1", "t2"):
        vpg.add_ready_task(
            task_id,
            metadata_extra=_task_resources(cpu_millis=1000),
        )
    first.schedule_once(vpg.graph_id)

    reopened = fake_scheduler(
        {"agent": _agent(cpu_millis=1000)},
        fake_vpg=vpg,
        state_path=str(db),
    )
    assert reopened._s.resource_manager.available("agent") == ResourceVector()
    replay = reopened.schedule_once(vpg.graph_id)
    assert replay.dispatched == []
    assert any(
        task_id == "t2" and "insufficient resources" in reason for task_id, reason in replay.skipped
    )


def test_restart_reclaims_kernel_lease_from_uncommitted_acquiring_claim(tmp_path) -> None:
    from lhos.runtimes.multi_agent import SchedulerStateStore
    from lhos.runtimes.multi_agent.events import SchedulerEventType, record_event
    from lhos.runtimes.multi_agent.models import TaskClaim

    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    leases = _TrackedLeaseProvider()
    resource = f"vpg://{vpg.graph_id}/task/t1/claim"
    lease = leases.acquire_exclusive("pid-agent", resource, None)
    assert lease is not None
    claim = TaskClaim(
        claim_id="claim-crash-window",
        graph_id=vpg.graph_id,
        graph_version=0,
        task_id="t1",
        agent_id="agent",
        process_id="pid-agent",
        lease_resource=resource,
        resource_reservation_id="reservation:agent:claim-crash-window",
        reserved_resources=ResourceVector(cpu_millis=1000),
        state=ClaimState.ACQUIRING,
    )
    store = SchedulerStateStore(db)
    store.append_event(
        record_event(
            SchedulerEventType.CLAIM_PROPOSED,
            graph_id=vpg.graph_id,
            task_id="t1",
            agent_id="agent",
            claim_id=claim.claim_id,
        ),
        claims=[claim],
        attempts=[],
        match_log=[],
        idempotent_keys=[],
    )

    reopened = _scheduler_with_lease_provider(vpg, leases, state_path=str(db))
    assert reopened._s.resource_manager.available("agent") == ResourceVector()

    result = reopened.reconcile()

    assert result.claims_marked_lost == 1
    assert result.orphan_leases_released == 1
    assert reopened.claims[0].state == ClaimState.LOST
    assert leases.leases == {}
    assert reopened._s.resource_manager.available("agent") == ResourceVector(cpu_millis=1000)


def test_schedule_auto_reconciles_zero_resource_pending_claim_then_retries(tmp_path) -> None:
    from lhos.runtimes.multi_agent import SchedulerStateStore
    from lhos.runtimes.multi_agent.events import SchedulerEventType, record_event
    from lhos.runtimes.multi_agent.models import TaskClaim

    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    vpg.add_ready_task("t1")
    claim = TaskClaim(
        claim_id="pending-zero",
        graph_id=vpg.graph_id,
        graph_version=0,
        task_id="t1",
        agent_id="agent",
        process_id="pid-agent",
        lease_resource=f"vpg://{vpg.graph_id}/task/t1/claim",
        state=ClaimState.ACQUIRING,
    )
    store = SchedulerStateStore(db)
    store.append_event(
        record_event(
            SchedulerEventType.CLAIM_PROPOSED,
            graph_id=vpg.graph_id,
            task_id="t1",
            agent_id="agent",
            claim_id=claim.claim_id,
        ),
        claims=[claim],
        attempts=[],
        match_log=[],
        idempotent_keys=[],
    )
    scheduler = fake_scheduler(
        {"agent": _agent()},
        fake_vpg=vpg,
        state_path=str(db),
    )

    result = scheduler.schedule_once(vpg.graph_id)

    assert len(result.dispatched) == 1
    assert scheduler.claims[0].state == ClaimState.LOST
    assert scheduler.claims[1].state == ClaimState.ACTIVE


def test_durable_restore_rejects_resource_overcommit_even_with_valid_hash(tmp_path) -> None:
    db = tmp_path / "scheduler.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(
        {"agent": _agent(cpu_millis=1000)},
        fake_vpg=vpg,
        state_path=str(db),
    )
    vpg.add_ready_task("t1", metadata_extra=_task_resources(cpu_millis=1000))
    scheduler.schedule_once(vpg.graph_id)

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT state_json FROM scheduler_snapshot WHERE snapshot_id = 1"
        ).fetchone()
        assert row is not None
        import json

        state = json.loads(row[0])
        duplicate = dict(state["claims"][0])
        duplicate["claim_id"] = "duplicate-active-claim"
        duplicate["task_id"] = "t2"
        duplicate["resource_reservation_id"] = "reservation:agent:duplicate-active-claim"
        state["claims"].append(duplicate)
        state_json = _canonical_json(state)
        conn.execute(
            "UPDATE scheduler_snapshot SET state_json = ?, state_hash = ? WHERE snapshot_id = 1",
            (state_json, _hash_state(state_json)),
        )

    with pytest.raises(SchedulerStateCorruption, match="resource reservations"):
        fake_scheduler(
            {"agent": _agent(cpu_millis=1000)},
            fake_vpg=vpg,
            state_path=str(db),
        )


def test_post_lease_durable_failure_releases_lease_and_resources(tmp_path) -> None:
    from lhos.runtimes.multi_agent import SchedulerStateStore

    vpg = FakeVPG()
    leases = _TrackedLeaseProvider()
    store = SchedulerStateStore(tmp_path / "scheduler.sqlite")
    scheduler = _scheduler_with_lease_provider(vpg, leases, state_store=store)
    vpg.add_ready_task("t1", metadata_extra=_task_resources(cpu_millis=1000))
    original = store.append_events

    def fail_post_lease(events, **kwargs):
        event_types = {event.event_type.value for event in events}
        if "execution_dispatched" in event_types:
            raise RuntimeError("disk full after lease")
        return original(events, **kwargs)

    store.append_events = fail_post_lease  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="disk full after lease"):
        scheduler.schedule_once(vpg.graph_id)

    claim = scheduler.claims[0]
    assert claim.state == ClaimState.RELEASED
    assert leases.leases == {}
    assert scheduler._s.resource_manager.available("agent") == ResourceVector(cpu_millis=1000)
    reopened = _scheduler_with_lease_provider(vpg, leases, state_store=store)
    assert reopened.claims[0].state == ClaimState.RELEASED
    assert reopened._s.resource_manager.available("agent") == ResourceVector(cpu_millis=1000)
