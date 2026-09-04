"""Adversarial claim/attempt/evidence causal-binding tests.

These tests exercise the scheduler's *reconciliation* path as well as the
normal ``observe_vpg`` hook.  In particular, a VPG ``verified`` bit alone must
not complete an ACTIVE claim when the supporting Evidence belongs to another
worker, an older semantic epoch, or an obsolete lease generation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry, ClaimState, create_scheduler
from lhos.runtimes.verified_progress.models import ReadinessProof, TaskDispatchCandidate


class _Process:
    def __init__(self, pid: str) -> None:
        self.pid = pid
        self.state = "ready"


class _ProcessProvider:
    def get(self, pid: str) -> _Process:
        return _Process(pid)

    def list_all(self) -> list[_Process]:
        return [_Process("pid-worker")]


class _Lease:
    def __init__(self, lease_id: str, resource_id: str, owner_pid: str, token: int) -> None:
        self.lease_id = lease_id
        self.resource_id = resource_id
        self.owner_pid = owner_pid
        self.fencing_token = token
        self.expires_at = datetime.now(UTC) + timedelta(minutes=30)


class _LiveLeaseProvider:
    """Tiny authoritative lease provider retaining live leases for reconcile."""

    def __init__(self) -> None:
        self._next_token = 0
        self._leases: dict[str, _Lease] = {}

    def acquire_exclusive(self, pid: str, resource_id: str, ttl: Any) -> _Lease:
        del ttl
        self._next_token += 1
        lease = _Lease(
            f"lease-{self._next_token}",
            resource_id,
            pid,
            self._next_token,
        )
        self._leases[lease.lease_id] = lease
        return lease

    def release(self, lease_id: str) -> bool:
        return self._leases.pop(lease_id, None) is not None

    def release_all_for_pid(self, pid: str) -> int:
        ids = [lid for lid, lease in self._leases.items() if lease.owner_pid == pid]
        for lid in ids:
            self._leases.pop(lid, None)
        return len(ids)

    def get(self, lease_id: str) -> _Lease | None:
        return self._leases.get(lease_id)

    def list_for_resource(self, resource_id: str) -> list[_Lease]:
        return [lease for lease in self._leases.values() if lease.resource_id == resource_id]

    def list_for_pid(self, pid: str) -> list[_Lease]:
        return [lease for lease in self._leases.values() if lease.owner_pid == pid]

    def reclaim_expired(self) -> int:
        return 0


class _CapabilityProvider:
    def check(self, pid: str, resource: str, operation: str) -> bool:
        del pid, resource, operation
        return True

    def capabilities_for(self, pid: str) -> list[Any]:
        del pid
        return []


class _VPG:
    graph_id = "g-causal"

    def __init__(self) -> None:
        self.version = 0
        self.validity = "unverified"
        self.bindings: list[dict[str, Any]] = []
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
                task_id="task",
                readiness_proof=ReadinessProof(
                    graph_id=self.graph_id,
                    graph_version=0,
                    task_id="task",
                    lifecycle_ok=True,
                    validity_ok=True,
                    all_deps_verified=True,
                    has_execution_attempt=False,
                ),
                execution_spec={},
            )
        ]

    def ready_frontier(self, graph_id: str) -> list[TaskDispatchCandidate]:
        return list(self.frontier) if graph_id == self.graph_id else []

    def current_graph_version(self, graph_id: str) -> int:
        if graph_id != self.graph_id:
            raise KeyError(graph_id)
        return self.version

    def task_node_payload(self, graph_id: str, task_id: str) -> dict[str, Any] | None:
        if graph_id != self.graph_id or task_id != "task":
            return None
        return self.payload

    def task_validity(self, graph_id: str, task_id: str) -> str | None:
        if graph_id != self.graph_id or task_id != "task":
            return None
        return self.validity

    def task_evidence_bindings(self, graph_id: str, task_id: str) -> list[dict[str, Any]]:
        if graph_id != self.graph_id or task_id != "task":
            return []
        return list(self.bindings)


def _scheduler() -> tuple[Any, _VPG, _LiveLeaseProvider]:
    vpg = _VPG()
    leases = _LiveLeaseProvider()
    registry = AgentRegistry()
    registry.register(
        AgentDescriptor(
            agent_id="worker",
            process_id="pid-worker",
            supported_task_kinds=("*",),
            specializations=("python",),
            max_concurrency=2,
        )
    )
    scheduler = create_scheduler(
        registry,
        vpg=vpg,
        process_provider=_ProcessProvider(),
        lease_provider=leases,
        capability_provider=_CapabilityProvider(),
    )
    return scheduler, vpg, leases


def _dispatch() -> tuple[Any, _VPG, _LiveLeaseProvider, Any, Any]:
    scheduler, vpg, leases = _scheduler()
    result = scheduler.schedule_once(vpg.graph_id)
    assert len(result.dispatched) == 1
    claim = scheduler.active_claim_for_task("task", vpg.graph_id)
    assert claim is not None
    attempt = scheduler.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    # Evidence may close a claim only after the scheduler-owned attempt has
    # crossed the operational-success boundary.
    assert scheduler.mark_execution_started(claim.claim_id) is attempt
    assert scheduler.mark_execution_operationally_succeeded(claim.claim_id) is attempt
    return scheduler, vpg, leases, claim, attempt


def _binding(
    claim: Any,
    attempt: Any,
    *,
    claim_id: str | None = None,
    epoch: int | None = None,
    token: int | None = None,
) -> dict[str, Any]:
    return {
        "evidence_id": "evidence",
        "claim_id": claim.claim_id if claim_id is None else claim_id,
        "attempt_id": attempt.attempt_id,
        "semantic_epoch": attempt.semantic_epoch if epoch is None else epoch,
        "lease_fencing_token": claim.lease_fencing_token if token is None else token,
    }


def test_reconcile_rejects_verified_evidence_from_another_worker() -> None:
    scheduler, vpg, _leases, claim, attempt = _dispatch()
    vpg.validity = "verified"
    vpg.bindings = [_binding(claim, attempt, claim_id="claim-other-worker")]

    observed = scheduler.observe_vpg(vpg.graph_id)
    reconciled = scheduler.reconcile()

    assert observed["claims_completed"] == 0
    assert reconciled.claims_completed == 0
    assert claim.state == ClaimState.ACTIVE


def test_reconcile_rejects_verified_evidence_from_stale_semantic_epoch() -> None:
    scheduler, vpg, _leases, claim, attempt = _dispatch()
    vpg.validity = "verified"
    vpg.bindings = [_binding(claim, attempt, epoch=attempt.semantic_epoch - 1)]

    observed = scheduler.observe_vpg(vpg.graph_id)
    reconciled = scheduler.reconcile()

    assert observed["claims_completed"] == 0
    assert reconciled.claims_completed == 0
    assert claim.state == ClaimState.ACTIVE


def test_reconcile_rejects_evidence_from_superseded_lease_generation() -> None:
    scheduler, vpg, leases, claim, attempt = _dispatch()
    vpg.validity = "verified"
    # Keep the claim's lease live, but present an Evidence token from a
    # previous ownership generation.
    old_token = max(0, int(claim.lease_fencing_token or 1) - 1)
    vpg.bindings = [_binding(claim, attempt, token=old_token)]

    observed = scheduler.observe_vpg(vpg.graph_id)
    reconciled = scheduler.reconcile()

    assert observed["claims_completed"] == 0
    assert reconciled.claims_completed == 0
    assert claim.state == ClaimState.ACTIVE
    assert leases.get(claim.lease_id or "") is not None


def test_duplicate_matching_evidence_completes_claim_once() -> None:
    scheduler, vpg, _leases, claim, attempt = _dispatch()
    vpg.validity = "verified"
    matching = _binding(claim, attempt)
    vpg.bindings = [matching, dict(matching)]

    first = scheduler.observe_vpg(vpg.graph_id)
    second = scheduler.observe_vpg(vpg.graph_id)

    assert first["claims_completed"] == 1
    assert second["claims_completed"] == 0
    assert claim.state == ClaimState.COMPLETED
