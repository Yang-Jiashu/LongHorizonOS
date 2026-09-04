"""Bounded online resource-replanning through the public AgentOS path.

The test intentionally models *logical* host capacity, not physical device
placement.  A caller samples telemetry, explicitly applies it to one named
pool, runs one bounded ``run_async`` invocation, resamples, applies the new
capacity, and invokes ``run_async`` again.  There is no background watcher.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lhos.runtimes.multi_agent import AttemptState, ClaimState, ResourceVector
from lhos.runtimes.multi_agent.lease_adapter import claim_resource_uri
from lhos.sdk import (
    Agent,
    AgentOS,
    ConflictGraph,
    Goal,
    TaskAccessSet,
    VerificationOutcome,
)
from lhos.sdk.host_capacity import HostCapacityPolicy
from lhos.sdk.resource_telemetry import (
    HostResourceTelemetry,
    ResourceTelemetryMetric,
)


def _metric(
    *,
    total: int | None,
    used: int | None,
    available: int | None,
    unit: str,
    is_available: bool = True,
    reason: str | None = None,
) -> ResourceTelemetryMetric:
    return ResourceTelemetryMetric(
        total=total,
        used=used,
        available=available,
        unit=unit,
        source="bounded-replanning-test",
        is_available=is_available,
        reason=reason,
    )


def _telemetry(*, ram_available: int) -> HostResourceTelemetry:
    """Build a deterministic CPU/RAM sample with optional GPU unavailable."""

    return HostResourceTelemetry(
        observed_at=datetime(
            2026,
            8,
            15,
            12,
            0,
            0 if ram_available >= 1_000 else 1,
            tzinfo=UTC,
        ),
        platform="bounded-replanning-test",
        cpu=_metric(total=1, used=0, available=1, unit="cores"),
        ram=_metric(
            total=ram_available,
            used=0,
            available=ram_available,
            unit="bytes",
        ),
        gpu=_metric(
            total=None,
            used=None,
            available=None,
            unit="devices",
            is_available=False,
            reason="GPU probe intentionally omitted",
        ),
        vram=_metric(
            total=None,
            used=None,
            available=None,
            unit="bytes",
            is_available=False,
            reason="VRAM probe intentionally omitted",
        ),
        available=True,
        complete=False,
        unavailable=("gpu", "vram"),
    )


def _verification(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"replanning-{task_id}",
        version=1,
        content=f"{task_id}:verified",
        evidence_note="bounded resource-replanning E2E",
    )


def _conflict_graph(task_ids: tuple[str, ...]) -> ConflictGraph:
    return ConflictGraph.from_access_sets(
        [
            TaskAccessSet(
                task_id=task_id,
                read_set=(f"workspace://input/{task_id}",),
                write_set=(f"workspace://output/{task_id}",),
            )
            for task_id in task_ids
        ]
    )


@pytest.mark.asyncio
async def test_bounded_online_resource_replanning_reduces_parallelism_and_closes_goal() -> None:
    """Re-observation changes the next policy batch without daemon behavior."""

    task_ids = ("a", "b", "c", "d")
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)

    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("bounded-replanning",),
                max_concurrency=2,
                resource_capacity=ResourceVector(ram_bytes=1_000),
            )
        )
        goal = Goal("bounded-resource-replanning")
        for task_id in task_ids:
            goal.task(
                task_id,
                agent="worker",
                required_specializations=("bounded-replanning",),
                resources=ResourceVector(ram_bytes=500),
                inputs=(f"workspace://input/{task_id}",),
                outputs=(f"workspace://output/{task_id}",),
                verify=lambda task_id=task_id: _verification(task_id),
            )
        conflict_graph = _conflict_graph(task_ids)
        cpu_only_policy = HostCapacityPolicy(
            require_gpu=False,
            cpu_reserve_fraction=0.0,
            ram_reserve_fraction=0.0,
        )

        # Caller-owned initial observation and explicit application.  This is
        # a logical Scheduler pool update, not a host watcher or placement API.
        initial_apply = runtime.apply_host_capacity(
            "worker",
            _telemetry(ram_available=1_000),
            cpu_only_policy,
        )
        assert initial_apply.applied is True
        assert initial_apply.applied_capacity == ResourceVector(
            cpu_millis=1_000,
            ram_bytes=1_000,
        )

        first = await runtime.run_async(
            goal,
            max_dispatches=2,
            max_steps=1,
            max_concurrency=2,
            adaptive=True,
            resource_aware=True,
            conflict_graph=conflict_graph,
            max_parallelism=2,
            persist_adaptive_epochs=False,
            automatic_rebase=False,
        )
        first_epoch = first.meta["adaptive_epochs"][0]
        assert first.goal_state == "open"
        assert len(first_epoch["selected_task_ids"]) == 2
        assert first_epoch["selected_task_ids"] == ("a", "b")
        assert first_epoch["actual_dispatched_task_ids"] == ("a", "b")

        observed_before = runtime.runtime_state(goal)
        assert set(observed_before.progress.ready_frontier) == {"c", "d"}
        assert observed_before.resources.pools[0].available.ram_bytes == 1_000

        # A second caller-owned sample lowers only this named logical pool.
        # The next bounded invocation must observe it and re-pack the frontier.
        reduced_apply = runtime.apply_host_capacity(
            "worker",
            _telemetry(ram_available=500),
            cpu_only_policy,
        )
        assert reduced_apply.applied is True
        assert reduced_apply.previous_capacity == ResourceVector(
            cpu_millis=1_000,
            ram_bytes=1_000,
        )
        assert reduced_apply.applied_capacity == ResourceVector(
            cpu_millis=1_000,
            ram_bytes=500,
        )

        observed_after = runtime.runtime_state(goal)
        assert observed_after.graph_id == observed_before.graph_id
        assert observed_after.resources.pools[0].available.ram_bytes == 500

        second = await runtime.run_async(
            goal,
            max_dispatches=2,
            max_steps=2,
            max_concurrency=2,
            adaptive=True,
            resource_aware=True,
            conflict_graph=conflict_graph,
            max_parallelism=2,
            persist_adaptive_epochs=False,
            automatic_rebase=False,
        )
        second_epochs = tuple(second.meta["adaptive_epochs"])
        assert second.goal_state == "closed"
        assert set(second.verified) == set(task_ids)
        assert [len(epoch["selected_task_ids"]) for epoch in second_epochs] == [1, 1]
        assert all(len(epoch["actual_dispatched_task_ids"]) <= 1 for epoch in second_epochs)
        assert all(epoch["fallback_attempted"] is False for epoch in second_epochs)
        assert second.meta["adaptive_policy"] == "resource-aware-conflict"
        assert second.meta["resource_aware"] is True

        # The complete path left durable attempt/claim identities and semantic
        # verification evidence; all ownership is released after commit.
        assert executed == list(task_ids)
        assert len(runtime.scheduler.attempts) == len(task_ids)
        assert all(
            attempt.state is AttemptState.VERIFIED_SEMANTICALLY
            and attempt.claim_id
            and attempt.agent_snapshot is not None
            for attempt in runtime.scheduler.attempts
        )
        assert len(runtime.scheduler.claims) == len(task_ids)
        assert all(
            claim.state is ClaimState.COMPLETED and claim.lease_id
            for claim in runtime.scheduler.claims
        )
        gid = runtime._gid_for(goal.goal_id)
        assert gid is not None
        assert all(
            runtime.kernel._lease_service.list_active_leases_for_resource(
                claim_resource_uri(gid, task_id)
            )
            == []
            for task_id in task_ids
        )
        # ``run_async`` keeps the scheduler/VPG authorities in the execution
        # path; the completed attempt state above is the semantic evidence
        # boundary.  The richer authority labels are added by the
        # ``execute_online_epoch`` wrapper, not by this lower-level API.
    finally:
        runtime.close()
