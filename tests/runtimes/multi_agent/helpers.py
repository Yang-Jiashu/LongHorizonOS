"""Shared helpers for D2 integration tests.

Provides a controllable FakeVPG driver so integration tests can drive the
Scheduler deterministically without going through VPG's full ADMISSION /
VERIFICATION state machine.  The Scheduler only consumes these four VPG
methods:

    vpg.ready_frontier(graph_id)         -> list[TaskDispatchCandidate]
    vpg.current_graph_version(graph_id)  -> int
    vpg.task_node_payload(graph_id, tid) -> dict | None
    vpg.task_validity(graph_id, tid)     -> str | None

FakeVPG exposes exactly that surface with mutable internal state.
"""

from __future__ import annotations

from datetime import UTC
from typing import Any

from lhos.runtimes.verified_progress.models import (
    ReadinessProof,
    TaskDispatchCandidate,
)


class FakeVPG:
    """A hand-rolled VPG surface for Scheduler integration tests.

    State:
      - current_version (int)
      - graph_id (str)   — every ready_frontier() result is tagged with this
      - ready_candidates: list[TaskDispatchCandidate]
      - payloads: dict[task_id, dict]
      - validities: dict[task_id, str]  ("unverified" / "verified" / ...)
    """

    def __init__(self, graph_id: str = "graph-1") -> None:
        self.graph_id = graph_id
        self.current_version: int = 0
        self.ready_candidates: list[TaskDispatchCandidate] = []
        self.payloads: dict[str, dict] = {}
        self.validities: dict[str, str] = {}
        self._raise_on_frontier: dict[str, BaseException | None] = {}

    # ── VPG surface consumed by the Scheduler ────────────────────────────
    def ready_frontier(self, graph_id: str) -> list[TaskDispatchCandidate]:
        if graph_id != self.graph_id:
            return []
        exc = self._raise_on_frontier.get(graph_id)
        if exc is not None:
            raise exc
        return list(self.ready_candidates)

    def current_graph_version(self, graph_id: str) -> int:
        if graph_id != self.graph_id:
            raise KeyError(graph_id)
        return self.current_version

    def task_node_payload(self, graph_id: str, task_id: str) -> dict | None:
        if graph_id != self.graph_id:
            return None
        return self.payloads.get(task_id)

    def task_validity(self, graph_id: str, task_id: str) -> str | None:
        if graph_id != self.graph_id:
            return None
        return self.validities.get(task_id)

    # ── mutators ─────────────────────────────────────────────────────────
    def bump_version(self, n: int = 1) -> None:
        self.current_version += n

    def add_ready_task(
        self,
        task_id: str,
        *,
        task_kind: str = "code_review",
        required_specializations: tuple[str, ...] = (),
        required_tools: tuple[str, ...] = (),
        metadata_extra: dict[str, Any] | None = None,
        validity: str = "unverified",
        version: int | None = None,
    ) -> None:
        """Register a READY task with the Scheduler-consumable payload."""
        v = version if version is not None else self.current_version
        metadata: dict[str, Any] = {
            "scheduler": {
                "task_kind": task_kind,
                "required_specializations": list(required_specializations),
                "required_tools": list(required_tools),
            }
        }
        if metadata_extra:
            metadata.update(metadata_extra)
        self.payloads[task_id] = {"metadata": metadata}
        self.validities[task_id] = validity
        cand = TaskDispatchCandidate(
            graph_id=self.graph_id,
            graph_version=v,
            task_id=task_id,
            readiness_proof=ReadinessProof(
                graph_id=self.graph_id,
                graph_version=v,
                task_id=task_id,
                lifecycle_ok=True,
                validity_ok=True,
                all_deps_verified=True,
                has_execution_attempt=False,
            ),
            execution_spec={},
        )
        # de-dup by task_id
        self.ready_candidates = [c for c in self.ready_candidates if c.task_id != task_id]
        self.ready_candidates.append(cand)

    def set_validity(self, task_id: str, validity: str) -> None:
        self.validities[task_id] = validity

    def clear_frontier(self) -> None:
        self.ready_candidates = []


def scheduler_with_agents(world, agent_specs):
    """Build a SchedulerSession with registered Kernel-backed agents.

    agent_specs: dict[agent_id, dict] where dict may contain:
        specializations, supported_task_kinds, supported_tools,
        max_concurrency, cost_weight
    """
    from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry, create_scheduler

    reg = AgentRegistry()
    for aid, kw in agent_specs.items():
        pid = world.kernel._process_service.spawn(aid).pid
        reg.register(AgentDescriptor(agent_id=aid, process_id=pid, **kw))
    return create_scheduler(
        reg,
        vpg=world.vpg,
        process_provider=world.proc,
        lease_provider=world.lease,
        capability_provider=world.cap,
    )


def fake_scheduler(agent_specs, *, fake_vpg):
    """Build a scheduler with a FakeVPG — no Kernel involved beyond the agent
    processes used solely for liveness checks (caller must supply a Kernel
    world or a NullProcessProvider)."""
    from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry, create_scheduler

    reg = AgentRegistry()
    for aid, kw in agent_specs.items():
        reg.register(AgentDescriptor(agent_id=aid, process_id=f"pid-{aid}", **kw))

    class _NullProc:
        def get(self, pid: str) -> Any:
            return _ProcStub(pid, "ready")

        def list_all(self) -> list[Any]:
            return [_ProcStub(f"pid-{aid}", "ready") for aid in agent_specs]

    class _NullCap:
        def check(self, pid: str, resource: str, operation: str) -> bool:
            return True

        def capabilities_for(self, pid: str) -> list[Any]:
            return []

    class _NullLease:
        def acquire_exclusive(self, pid: str, resource_id: str, ttl: Any) -> Any:
            return _LeaseStub(resource_id, pid, ttl)

        def release(self, lease_id: str) -> bool:
            return True

        def release_all_for_pid(self, pid: str) -> int:
            return 0

        def get(self, lease_id: str) -> Any | None:
            return None

        def list_for_resource(self, resource_id: str) -> list[Any]:
            return []

        def list_for_pid(self, pid: str) -> list[Any]:
            return []

        def reclaim_expired(self) -> int:
            return 0

    return create_scheduler(
        reg,
        vpg=fake_vpg,
        process_provider=_NullProc(),
        lease_provider=_NullLease(),
        capability_provider=_NullCap(),
    )


class _DeadProc:
    """Process provider that reports a configurable dead set as non-existent,
    simulating crashed agents for crash-reassignment demos/tests."""

    def __init__(self, dead: set[str] | None = None) -> None:
        self.dead = set(dead or set())

    def get(self, pid: str) -> Any:
        if pid in self.dead:
            return None
        return _ProcStub(pid, "ready")

    def list_all(self) -> list[Any]:
        return [_ProcStub("pid-alive", "ready")]


class _ProcStub:
    def __init__(self, pid: str, state: str) -> None:
        self.pid = pid
        self.state = state


class _LeaseStub:
    def __init__(self, resource_id: str, pid: str, ttl: Any) -> None:
        from datetime import datetime, timedelta

        self.lease_id = f"lease-{resource_id}"
        self.resource_id = resource_id
        self.owner_pid = pid
        self.acquired_at = datetime.now(UTC)
        ttl_secs = int(ttl.total_seconds()) if isinstance(ttl, timedelta) else 1800
        self.expires_at = self.acquired_at + timedelta(seconds=ttl_secs)
