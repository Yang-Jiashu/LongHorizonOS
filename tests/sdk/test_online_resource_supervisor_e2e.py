"""End-to-end resource re-planning through the caller-owned supervisor."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lhos.runtimes.multi_agent import ResourceVector
from lhos.sdk import (
    Agent,
    AgentOS,
    ConflictGraph,
    Goal,
    HostCapacityPolicy,
    HostResourceTelemetry,
    ResourceTelemetryMetric,
    SupervisorStepStatus,
    TaskAccessSet,
    VerificationOutcome,
)


def _metric(
    *,
    total: int | None,
    available: int | None,
    unit: str,
    is_available: bool = True,
    reason: str | None = None,
) -> ResourceTelemetryMetric:
    return ResourceTelemetryMetric(
        total=total,
        used=0 if available is not None else None,
        available=available,
        unit=unit,
        source="online-resource-supervisor-test",
        is_available=is_available,
        reason=reason,
    )


def _telemetry(ram_available: int) -> HostResourceTelemetry:
    return HostResourceTelemetry(
        observed_at=datetime(2026, 8, 15, 6, 30, ram_available % 2, tzinfo=UTC),
        platform="online-resource-supervisor-test",
        cpu=_metric(total=1, available=1, unit="cores"),
        ram=_metric(total=ram_available, available=ram_available, unit="bytes"),
        gpu=_metric(
            total=None,
            available=None,
            unit="devices",
            is_available=False,
            reason="GPU probe omitted",
        ),
        vram=_metric(
            total=None,
            available=None,
            unit="bytes",
            is_available=False,
            reason="VRAM probe omitted",
        ),
        available=True,
        complete=False,
        unavailable=("gpu", "vram"),
    )


def _verify(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"online-resource-{task_id}",
        version=1,
        content=f"{task_id}:verified",
    )


@pytest.mark.asyncio
async def test_supervisor_replans_parallelism_after_explicit_capacity_change() -> None:
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
                max_concurrency=2,
                resource_capacity=ResourceVector(ram_bytes=1_000),
            )
        )
        goal = Goal("online-resource-supervisor")
        for task_id in task_ids:
            goal.task(
                task_id,
                agent="worker",
                resources=ResourceVector(ram_bytes=500),
                inputs=(f"workspace://input/{task_id}",),
                outputs=(f"workspace://output/{task_id}",),
                verify=lambda task_id=task_id: _verify(task_id),
            )
        goal.compile(runtime)
        conflicts = ConflictGraph.from_access_sets(
            [
                TaskAccessSet(
                    task_id=task_id,
                    read_set=(f"workspace://input/{task_id}",),
                    write_set=(f"workspace://output/{task_id}",),
                )
                for task_id in task_ids
            ]
        )
        capacity_policy = HostCapacityPolicy(
            require_gpu=False,
            cpu_reserve_fraction=0.0,
            ram_reserve_fraction=0.0,
        )
        runtime.apply_host_capacity("worker", _telemetry(1_000), capacity_policy)
        supervisor = runtime.event_supervisor(
            goal,
            max_epochs=3,
            max_concurrency=2,
            max_dispatches_per_epoch=2,
            max_parallelism=2,
            resource_aware=True,
            conflict_graph=conflicts,
            persist_epoch=False,
        )

        first = await supervisor.step()
        first_online = first.execution_result.meta["online_epoch"]
        assert first.status is SupervisorStepStatus.EXECUTED
        assert first_online["resource_aware"] is True
        assert first_online["actual_dispatched_task_ids"] == ("a", "b")

        runtime.apply_host_capacity("worker", _telemetry(500), capacity_policy)
        second = await supervisor.step()
        third = await supervisor.step()

        assert second.status is SupervisorStepStatus.EXECUTED
        assert third.status is SupervisorStepStatus.CLOSED
        assert second.execution_result.meta["online_epoch"]["actual_dispatched_task_ids"] == ("c",)
        assert third.execution_result.meta["online_epoch"]["actual_dispatched_task_ids"] == ("d",)
        assert executed == list(task_ids)
        assert supervisor.snapshot.goal_state == "closed"
        assert supervisor.snapshot.epochs_attempted == 3
    finally:
        runtime.close()
