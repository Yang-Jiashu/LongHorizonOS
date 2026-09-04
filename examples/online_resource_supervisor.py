"""Caller-owned online resource re-planning through EventDrivenSupervisor.

The example changes one named logical RAM pool between explicit supervisor
steps.  It demonstrates online policy re-evaluation, not physical placement,
continuous telemetry, or a background daemon.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from lhos.runtimes.multi_agent import ResourceVector
from lhos.sdk import (
    Agent,
    AgentOS,
    ConflictGraph,
    Goal,
    HostCapacityPolicy,
    HostResourceTelemetry,
    ResourceTelemetryMetric,
    TaskAccessSet,
    VerificationOutcome,
)


def _metric(
    total: int | None,
    available: int | None,
    unit: str,
    *,
    is_available: bool = True,
    reason: str | None = None,
) -> ResourceTelemetryMetric:
    return ResourceTelemetryMetric(
        total=total,
        used=0 if available is not None else None,
        available=available,
        unit=unit,
        source="online-resource-supervisor-example",
        is_available=is_available,
        reason=reason,
    )


def _telemetry(ram_available: int) -> HostResourceTelemetry:
    return HostResourceTelemetry(
        observed_at=datetime(2026, 8, 15, 6, 30, ram_available % 2, tzinfo=UTC),
        platform="online-resource-supervisor-example",
        cpu=_metric(1, 1, "cores"),
        ram=_metric(ram_available, ram_available, "bytes"),
        gpu=_metric(
            None,
            None,
            "devices",
            is_available=False,
            reason="GPU probe omitted",
        ),
        vram=_metric(
            None,
            None,
            "bytes",
            is_available=False,
            reason="VRAM probe omitted",
        ),
        available=True,
        complete=False,
        unavailable=("gpu", "vram"),
    )


async def run() -> dict[str, object]:
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
        goal = Goal("online-resource-supervisor-example")
        for task_id in task_ids:
            goal.task(
                task_id,
                agent="worker",
                resources=ResourceVector(ram_bytes=500),
                inputs=(f"workspace://input/{task_id}",),
                outputs=(f"workspace://output/{task_id}",),
                verify=lambda task_id=task_id: VerificationOutcome(
                    passed=True,
                    artifact_id=f"example-{task_id}",
                    version=1,
                    content=f"{task_id}:verified",
                ),
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
        policy = HostCapacityPolicy(
            require_gpu=False,
            cpu_reserve_fraction=0.0,
            ram_reserve_fraction=0.0,
        )
        runtime.apply_host_capacity("worker", _telemetry(1_000), policy)
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

        steps = [await supervisor.step()]
        runtime.apply_host_capacity("worker", _telemetry(500), policy)
        steps.extend((await supervisor.step(), await supervisor.step()))

        return {
            "selected_batches": [
                list(step.execution_result.meta["online_epoch"]["actual_dispatched_task_ids"])
                for step in steps
            ],
            "executed": executed,
            "goal_state": supervisor.snapshot.goal_state,
            "epochs_attempted": supervisor.snapshot.epochs_attempted,
            "scope": {
                "caller_owned": True,
                "resource_aware": True,
                "logical_capacity_only": True,
                "daemon": False,
                "physical_placement": False,
            },
        }
    finally:
        runtime.close()


def main() -> int:
    print(json.dumps(asyncio.run(run()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
