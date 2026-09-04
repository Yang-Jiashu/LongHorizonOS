"""Cooperative claim-heartbeat and lease-renewal tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from lhos.runtimes.multi_agent import (
    AgentDescriptor,
    AgentRegistry,
    ClaimState,
    create_scheduler,
)
from lhos.runtimes.multi_agent.claims import ClaimManager
from lhos.runtimes.multi_agent.events import SchedulerEventType
from lhos.runtimes.multi_agent.lease_adapter import LeaseAdapter, claim_resource_uri
from lhos.runtimes.verified_progress.models import ReadinessProof, TaskDispatchCandidate


class _Lease:
    def __init__(
        self,
        lease_id: str,
        resource_id: str,
        owner_pid: str,
        token: int,
        expires_at: datetime,
    ) -> None:
        self.lease_id = lease_id
        self.resource_id = resource_id
        self.owner_pid = owner_pid
        self.fencing_token = token
        self.expires_at = expires_at


class _RenewProvider:
    def __init__(self, *, mutate: str | None = None) -> None:
        self.leases: dict[str, _Lease] = {}
        self.mutate = mutate
        self.renew_calls = 0

    def acquire_exclusive(self, pid: str, resource_id: str, ttl: timedelta) -> _Lease:
        lease = _Lease(
            "lease-1",
            resource_id,
            pid,
            11,
            datetime.now(UTC) + ttl,
        )
        self.leases[lease.lease_id] = lease
        return lease

    def renew(self, lease_id: str, ttl: timedelta) -> _Lease | None:
        self.renew_calls += 1
        lease = self.leases.get(lease_id)
        if lease is None:
            return None
        if lease.expires_at <= datetime.now(UTC):
            return None
        if self.mutate == "token":
            return _Lease(
                lease.lease_id,
                lease.resource_id,
                lease.owner_pid,
                lease.fencing_token + 1,
                datetime.now(UTC) + ttl,
            )
        if self.mutate == "resource":
            return _Lease(
                lease.lease_id,
                "wrong-resource",
                lease.owner_pid,
                lease.fencing_token,
                datetime.now(UTC) + ttl,
            )
        lease.expires_at = datetime.now(UTC) + ttl
        return lease

    def release(self, lease_id: str) -> bool:
        return self.leases.pop(lease_id, None) is not None

    def release_all_for_pid(self, pid: str) -> int:
        ids = [lid for lid, lease in self.leases.items() if lease.owner_pid == pid]
        for lid in ids:
            self.leases.pop(lid, None)
        return len(ids)

    def get(self, lease_id: str) -> _Lease | None:
        return self.leases.get(lease_id)

    def list_for_resource(self, resource_id: str) -> list[_Lease]:
        return [lease for lease in self.leases.values() if lease.resource_id == resource_id]

    def list_for_pid(self, pid: str) -> list[_Lease]:
        return [lease for lease in self.leases.values() if lease.owner_pid == pid]

    def reclaim_expired(self) -> int:
        return 0


class _Proc:
    def __init__(self, pid: str) -> None:
        self.pid = pid
        self.state = "ready"
        self.capability_set_id = ""
        self.program_id = ""


class _Processes:
    def get(self, pid: str) -> _Proc:
        return _Proc(pid)

    def list_all(self) -> list[_Proc]:
        return [_Proc("pid-1")]


class _Capabilities:
    def check(self, pid: str, resource: str, operation: str) -> bool:
        return True

    def capabilities_for(self, pid: str) -> list[Any]:
        return []


class _VPG:
    graph_id = "g-heartbeat"

    def __init__(self) -> None:
        self.version = 0
        self.validity = "unverified"
        self.payload = {
            "created_in_version": 0,
            "metadata": {
                "scheduler": {
                    "task_kind": "code_review",
                    "required_specializations": ["python"],
                }
            },
        }
        self.frontier = [
            TaskDispatchCandidate(
                graph_id=self.graph_id,
                graph_version=0,
                task_id="task-1",
                readiness_proof=ReadinessProof(
                    graph_id=self.graph_id,
                    graph_version=0,
                    task_id="task-1",
                    lifecycle_ok=True,
                    validity_ok=True,
                    all_deps_verified=True,
                    has_execution_attempt=False,
                ),
                execution_spec={},
            )
        ]

    def ready_frontier(self, graph_id: str) -> list[Any]:
        return list(self.frontier) if graph_id == self.graph_id else []

    def current_graph_version(self, graph_id: str) -> int:
        return self.version

    def task_node_payload(self, graph_id: str, task_id: str) -> dict[str, Any] | None:
        return self.payload if graph_id == self.graph_id and task_id == "task-1" else None

    def task_validity(self, graph_id: str, task_id: str) -> str | None:
        return self.validity if graph_id == self.graph_id and task_id == "task-1" else None


def _claim_manager(
    provider: _RenewProvider, *, mutate: str | None = None
) -> tuple[ClaimManager, Any]:
    if mutate is not None:
        provider.mutate = mutate
    manager = ClaimManager(LeaseAdapter(provider, ttl=timedelta(seconds=30)))
    claim = manager.propose(
        claim_id="claim-1",
        graph_id="g",
        graph_version=1,
        task_id="task-1",
        agent_id="agent-1",
        process_id="pid-1",
        lease_resource=claim_resource_uri("g", "task-1"),
    )
    manager.mark_acquiring(claim)
    assert manager.try_acquire_lease(claim)
    return manager, claim


def test_claim_renewal_preserves_fencing_token_and_updates_expiry() -> None:
    provider = _RenewProvider()
    manager, claim = _claim_manager(provider)
    before = claim.lease_expires_at

    lease = manager.renew_claim(claim, timedelta(minutes=5))

    assert lease is not None
    assert claim.state == ClaimState.ACTIVE
    assert claim.lease_fencing_token == 11
    assert claim.lease_expires_at is not None
    assert before is not None
    assert claim.lease_expires_at > before


def test_claim_renewal_fails_closed_on_superseded_fence_or_resource() -> None:
    for mutate in ("token", "resource"):
        provider = _RenewProvider()
        manager, claim = _claim_manager(provider, mutate=mutate)
        before = claim.lease_expires_at

        assert manager.renew_claim(claim, timedelta(minutes=5)) is None
        assert claim.state == ClaimState.ACTIVE
        assert claim.lease_expires_at == before


def test_claim_renewal_fails_closed_on_malformed_fencing_token() -> None:
    provider = _RenewProvider()
    manager, claim = _claim_manager(provider)
    before = claim.lease_expires_at

    class _Malformed(_RenewProvider):
        def renew(self, lease_id: str, ttl: timedelta) -> _Lease | None:
            lease = super().renew(lease_id, ttl)
            if lease is not None:
                lease.fencing_token = "not-an-integer"
            return lease

    malformed = _Malformed()
    malformed.leases = provider.leases
    manager = ClaimManager(LeaseAdapter(malformed, ttl=timedelta(seconds=30)))
    assert manager.renew_claim(claim, timedelta(minutes=5)) is None
    assert claim.state == ClaimState.ACTIVE
    assert claim.lease_expires_at == before


def test_scheduler_heartbeat_is_attempt_scoped_and_journaled() -> None:
    provider = _RenewProvider()
    vpg = _VPG()
    registry = AgentRegistry()
    registry.register(
        AgentDescriptor(
            agent_id="agent-1",
            process_id="pid-1",
            specializations=("python",),
            supported_task_kinds=("*",),
        )
    )
    session = create_scheduler(
        registry,
        vpg=vpg,
        process_provider=_Processes(),
        lease_provider=provider,
        capability_provider=_Capabilities(),
        lease_ttl=timedelta(seconds=30),
    )
    result = session.schedule_once(vpg.graph_id)
    assert len(result.dispatched) == 1
    claim = session.active_claim_for_task("task-1", vpg.graph_id)
    assert claim is not None
    attempt = session.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    before = claim.lease_expires_at

    assert session.renew_claim(
        claim.claim_id,
        timedelta(minutes=5),
        expected_attempt_id=attempt.attempt_id,
        expected_semantic_epoch=attempt.semantic_epoch,
    )
    assert claim.lease_expires_at is not None and before is not None
    assert claim.lease_expires_at > before
    assert claim.lease_fencing_token == 11
    assert any(
        event.event_type == SchedulerEventType.CLAIM_LEASE_RENEWED for event in session.events
    )

    assert not session.heartbeat(
        claim.claim_id,
        expected_attempt_id="stale-attempt",
    )


def test_failed_heartbeat_on_expired_lease_reconciles_to_lost() -> None:
    provider = _RenewProvider()
    vpg = _VPG()
    registry = AgentRegistry()
    registry.register(
        AgentDescriptor(
            agent_id="agent-1",
            process_id="pid-1",
            specializations=("python",),
            supported_task_kinds=("*",),
        )
    )
    session = create_scheduler(
        registry,
        vpg=vpg,
        process_provider=_Processes(),
        lease_provider=provider,
        capability_provider=_Capabilities(),
        lease_ttl=timedelta(seconds=30),
    )
    result = session.schedule_once(vpg.graph_id)
    claim = session.active_claim_for_task("task-1", vpg.graph_id)
    assert len(result.dispatched) == 1 and claim is not None
    lease = provider.leases[claim.lease_id or ""]
    lease.expires_at = datetime.now(UTC) - timedelta(seconds=1)

    assert not session.heartbeat(claim.claim_id)
    reconciled = session.reconcile()

    assert reconciled.claims_marked_lost == 1
    assert claim.state == ClaimState.LOST


def test_heartbeat_projection_replays_expiry_and_provenance(tmp_path) -> None:
    provider = _RenewProvider()
    vpg = _VPG()
    registry = AgentRegistry()
    registry.register(
        AgentDescriptor(
            agent_id="agent-1",
            process_id="pid-1",
            specializations=("python",),
            supported_task_kinds=("*",),
        )
    )
    db = tmp_path / "heartbeat-replay.sqlite"
    first = create_scheduler(
        registry,
        vpg=vpg,
        process_provider=_Processes(),
        lease_provider=provider,
        capability_provider=_Capabilities(),
        lease_ttl=timedelta(seconds=30),
        state_path=str(db),
    )
    result = first.schedule_once(vpg.graph_id)
    claim = first.active_claim_for_task("task-1", vpg.graph_id)
    assert len(result.dispatched) == 1 and claim is not None
    attempt = first.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    digest = "a" * 64
    assert first.bind_attempt_provenance(claim.claim_id, digest)
    before = claim.lease_expires_at
    assert first.heartbeat(claim.claim_id, timedelta(minutes=5))
    assert claim.lease_expires_at is not None and before is not None
    assert claim.lease_expires_at > before
    expected_expiry = claim.lease_expires_at
    first.close()

    reopened = create_scheduler(
        registry,
        vpg=vpg,
        process_provider=_Processes(),
        lease_provider=provider,
        capability_provider=_Capabilities(),
        lease_ttl=timedelta(seconds=30),
        state_path=str(db),
    )
    restored_claim = reopened.active_claim_for_task("task-1", vpg.graph_id)
    restored_attempt = reopened.attempt_for_claim(claim.claim_id)
    assert restored_claim is not None
    assert restored_attempt is not None
    assert restored_claim.lease_expires_at == expected_expiry
    assert restored_attempt.provenance_digest == digest
    assert any(
        event.event_type == SchedulerEventType.CLAIM_LEASE_RENEWED for event in reopened.events
    )
    reopened.close()
