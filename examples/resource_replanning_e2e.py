"""Run the bounded online resource-replanning example.

This is a deterministic logical-capacity demonstration.  The caller owns
both telemetry samples and explicitly invokes ``apply_host_capacity`` between
two bounded ``run_async`` calls.  It is not a daemon, placement engine, or
physical GPU benchmark.
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
    TaskAccessSet,
    VerificationOutcome,
)
from lhos.sdk.host_capacity import HostCapacityPolicy
from lhos.sdk.resource_telemetry import HostResourceTelemetry, ResourceTelemetryMetric


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
        source="resource-replanning-example",
        is_available=is_available,
        reason=reason,
    )


def _telemetry(ram_available: int) -> HostResourceTelemetry:
    return HostResourceTelemetry(
        observed_at=datetime(
            2026,
            8,
            15,
            13,
            0,
            0 if ram_available >= 1_000 else 1,
            tzinfo=UTC,
        ),
        platform="resource-replanning-example",
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
        artifact_id=f"example-{task_id}",
        version=1,
        content=f"{task_id}:verified",
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
                specializations=("resource-replanning-example",),
                max_concurrency=2,
                resource_capacity=ResourceVector(ram_bytes=1_000),
            )
        )
        goal = Goal("resource-replanning-example")
        for task_id in task_ids:
            goal.task(
                task_id,
                agent="worker",
                required_specializations=("resource-replanning-example",),
                resources=ResourceVector(ram_bytes=500),
                inputs=(f"example://input/{task_id}",),
                outputs=(f"example://output/{task_id}",),
                verify=lambda task_id=task_id: _verify(task_id),
            )
        conflict_graph = ConflictGraph.from_access_sets(
            [
                TaskAccessSet(
                    task_id=task_id,
                    read_set=(f"example://input/{task_id}",),
                    write_set=(f"example://output/{task_id}",),
                )
                for task_id in task_ids
            ]
        )
        policy = HostCapacityPolicy(
            require_gpu=False,
            cpu_reserve_fraction=0.0,
            ram_reserve_fraction=0.0,
        )

        first_capacity = runtime.apply_host_capacity("worker", _telemetry(1_000), policy)
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

        reduced_capacity = runtime.apply_host_capacity("worker", _telemetry(500), policy)
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
        return {
            "scope": {
                "caller_owned_resampling": True,
                "daemon": False,
                "physical_placement": False,
                "physical_gpu_benchmark": False,
            },
            "initial_capacity": first_capacity.applied_capacity.model_dump(mode="json")
            if first_capacity.applied_capacity
            else None,
            "reduced_capacity": reduced_capacity.applied_capacity.model_dump(mode="json")
            if reduced_capacity.applied_capacity
            else None,
            "first_selected": list(first.meta["adaptive_epochs"][0]["selected_task_ids"]),
            "second_selected": [
                list(epoch["selected_task_ids"]) for epoch in second.meta["adaptive_epochs"]
            ],
            "executed": executed,
            "goal_state": second.goal_state,
            "verified": list(second.verified),
        }
    finally:
        runtime.close()


def main() -> int:
    print(json.dumps(asyncio.run(run()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
