"""Deterministic logical-resource benchmark for adaptive AgentOS execution.

The benchmark runs the same four-task graph twice through the public
``AgentOS.run_async`` path:

``static``
    A fixed-parallelism, conflict-aware policy that does not fit the selected
    batch to logical resource capacity.  The authoritative Scheduler still
    prevents over-admission, so an over-capacity proposal is rejected and
    replanned in a later epoch.

``resource_aware``
    The opt-in resource-aware adaptive policy fits the same READY frontier to
    the same declared logical capacity before Scheduler admission.

The controlled workload has one logical pool with 1,000 CPU millicores and
four independent requests: 700, 700, 300, and 300 millicores.  With a
parallelism bound of two, the static policy first proposes the two 700-unit
tasks, while the resource-aware policy packs one 700-unit task with one
300-unit task.

This is an offline systems regression benchmark.  It does not measure
physical CPU/GPU/RAM/VRAM use, model quality, provider latency, or production
throughput.  In particular, a ``proposal_capacity_violation`` means that an
advisory selected batch exceeded the declared logical capacity; it does *not*
mean the Scheduler admitted or executed an unsafe batch.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Final

from lhos.runtimes.multi_agent import ResourceVector
from lhos.sdk import (
    Agent,
    AgentOS,
    ConflictGraph,
    Goal,
    TaskAccessSet,
    VerificationOutcome,
)

BENCHMARK_NAME: Final[str] = "resource_aware_adaptive_runtime"
BENCHMARK_VERSION: Final[int] = 1
AGENT_ID: Final[str] = "resource-worker"
MAX_CONCURRENCY: Final[int] = 2
MAX_PARALLELISM: Final[int] = 2
LOGICAL_CAPACITY: Final[ResourceVector] = ResourceVector(cpu_millis=1_000)
TASK_REQUESTS: Final[dict[str, ResourceVector]] = {
    "a-heavy": ResourceVector(cpu_millis=700),
    "b-heavy": ResourceVector(cpu_millis=700),
    "c-light": ResourceVector(cpu_millis=300),
    "d-light": ResourceVector(cpu_millis=300),
}
TASK_IDS: Final[tuple[str, ...]] = tuple(TASK_REQUESTS)


@dataclass
class _ResourceWorkload:
    """Small executor-side audit of the resources Scheduler actually admitted."""

    active: ResourceVector = field(default_factory=ResourceVector)
    peak: ResourceVector = field(default_factory=ResourceVector)
    completed: list[str] = field(default_factory=list)
    capacity_violation_events: int = 0

    async def execute(self, task_id: str) -> None:
        request = TASK_REQUESTS[task_id]
        self.active = self.active.plus(request)
        self.peak = _component_max(self.peak, self.active)
        if self.active.shortages(LOGICAL_CAPACITY):
            self.capacity_violation_events += 1

        # Yield once so tasks admitted in the same Scheduler pass overlap.
        # No wall-clock value is measured or reported.
        await asyncio.sleep(0)

        self.completed.append(task_id)
        self.active = self.active.minus(request)


def _component_max(left: ResourceVector, right: ResourceVector) -> ResourceVector:
    slot_names = set(left.model_slots) | set(right.model_slots)
    return ResourceVector(
        cpu_millis=max(left.cpu_millis, right.cpu_millis),
        ram_bytes=max(left.ram_bytes, right.ram_bytes),
        gpu_count=max(left.gpu_count, right.gpu_count),
        vram_bytes=max(left.vram_bytes, right.vram_bytes),
        model_slots={
            name: max(left.model_slots.get(name, 0), right.model_slots.get(name, 0))
            for name in slot_names
        },
    )


def _verification(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"resource-benchmark-{task_id}",
        version=1,
        content=f"{task_id}:verified",
        evidence_note="deterministic logical-resource benchmark",
    )


def _conflict_graph() -> ConflictGraph:
    # Every task has a known, independent access declaration.  This isolates
    # resource fitting from conflict serialization.
    return ConflictGraph.from_access_sets(
        [
            TaskAccessSet(
                task_id=task_id,
                read_set=(f"workspace://input/{task_id}",),
                write_set=(f"workspace://output/{task_id}",),
            )
            for task_id in TASK_IDS
        ]
    )


def _sum_requests(task_ids: tuple[str, ...]) -> ResourceVector:
    total = ResourceVector()
    for task_id in task_ids:
        total = total.plus(TASK_REQUESTS[task_id])
    return total


def _batch_audit(task_ids: tuple[str, ...]) -> dict[str, Any]:
    requested = _sum_requests(task_ids)
    shortages = requested.shortages(LOGICAL_CAPACITY)
    return {
        "task_ids": list(task_ids),
        "requested": requested.model_dump(mode="json"),
        "shortages": dict(sorted(shortages.items())),
        "exceeds_logical_capacity": bool(shortages),
    }


def _resource_rejection_count(epochs: tuple[dict[str, Any], ...]) -> int:
    count = 0
    for epoch in epochs:
        for _task_id, reason in epoch.get("scheduler_skipped", ()):
            if "insufficient resources" in str(reason).lower():
                count += 1
    return count


async def _run_case(*, mode: str, resource_aware: bool) -> dict[str, Any]:
    workload = _ResourceWorkload()
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                AGENT_ID,
                executor=workload.execute,
                specializations=("resource-benchmark",),
                max_concurrency=MAX_CONCURRENCY,
                resource_capacity=LOGICAL_CAPACITY,
            )
        )
        goal = Goal(f"resource-aware-benchmark-{mode}")
        for task_id, request in TASK_REQUESTS.items():
            goal.task(
                task_id,
                agent=AGENT_ID,
                required_specializations=("resource-benchmark",),
                resources=request,
                inputs=(f"workspace://input/{task_id}",),
                outputs=(f"workspace://output/{task_id}",),
                verify=lambda task_id=task_id: _verification(task_id),
            )

        result = await runtime.run_async(
            goal,
            max_dispatches=len(TASK_IDS),
            max_steps=len(TASK_IDS) * 2,
            max_concurrency=MAX_CONCURRENCY,
            adaptive=True,
            resource_aware=resource_aware,
            conflict_graph=_conflict_graph(),
            max_parallelism=MAX_PARALLELISM,
            persist_adaptive_epochs=False,
            automatic_rebase=False,
        )

        epochs = tuple(dict(epoch) for epoch in result.meta.get("adaptive_epochs", ()))
        selected_batches = tuple(
            tuple(str(task_id) for task_id in epoch.get("selected_task_ids", ()))
            for epoch in epochs
        )
        admitted_batches = tuple(
            tuple(str(task_id) for task_id in epoch.get("actual_dispatched_task_ids", ()))
            for epoch in epochs
        )
        selected_audits = tuple(_batch_audit(batch) for batch in selected_batches)
        admitted_audits = tuple(_batch_audit(batch) for batch in admitted_batches)
        proposal_violations = sum(
            bool(item["exceeds_logical_capacity"]) for item in selected_audits
        )
        admitted_violations = sum(
            bool(item["exceeds_logical_capacity"]) for item in admitted_audits
        )
        verified = tuple(sorted(str(task_id) for task_id in result.verified))
        closure = result.goal_state == "closed" and verified == tuple(sorted(TASK_IDS))

        return {
            "mode": mode,
            "policy": (
                "resource-aware-conflict-greedy.v1"
                if resource_aware
                else "conflict-aware-fixed-parallelism.v1"
            ),
            "resource_aware": resource_aware,
            "closure": closure,
            "goal_state": result.goal_state,
            "verified_task_ids": list(verified),
            "epochs": len(epochs),
            "selected_batches": [list(batch) for batch in selected_batches],
            "admitted_batches": [list(batch) for batch in admitted_batches],
            "selected_batch_audit": list(selected_audits),
            "admitted_batch_audit": list(admitted_audits),
            "proposal_capacity_violations": proposal_violations,
            "admitted_capacity_violations": admitted_violations,
            "executor_capacity_violation_events": workload.capacity_violation_events,
            "scheduler_resource_rejections": _resource_rejection_count(epochs),
            "dispatched_attempts": int(result.meta.get("dispatched", 0)),
            "executor_completed_task_ids": sorted(workload.completed),
            "executor_peak_resources": workload.peak.model_dump(mode="json"),
        }
    finally:
        runtime.close()


async def run_benchmark_async() -> dict[str, Any]:
    """Run the resource-blind and resource-aware cases on the same graph."""

    static = await _run_case(mode="static", resource_aware=False)
    resource_aware = await _run_case(mode="resource_aware", resource_aware=True)
    comparison = {
        "same_verified_goal": (
            static["closure"]
            and resource_aware["closure"]
            and static["verified_task_ids"] == resource_aware["verified_task_ids"]
        ),
        "epoch_reduction": static["epochs"] - resource_aware["epochs"],
        "proposal_capacity_violation_reduction": (
            static["proposal_capacity_violations"] - resource_aware["proposal_capacity_violations"]
        ),
        "scheduler_resource_rejection_reduction": (
            static["scheduler_resource_rejections"]
            - resource_aware["scheduler_resource_rejections"]
        ),
    }
    violations: list[str] = []
    for label, case in (("static", static), ("resource_aware", resource_aware)):
        if not case["closure"]:
            violations.append(f"{label} did not close the verified goal")
        if case["admitted_capacity_violations"]:
            violations.append(f"{label} Scheduler admitted an over-capacity batch")
        if case["executor_capacity_violation_events"]:
            violations.append(f"{label} executor exceeded declared logical capacity")
    if static["proposal_capacity_violations"] < 1:
        violations.append("static baseline did not exercise an over-capacity proposal")
    if resource_aware["proposal_capacity_violations"] != 0:
        violations.append("resource-aware policy proposed an over-capacity batch")
    if resource_aware["epochs"] >= static["epochs"]:
        violations.append("resource-aware policy did not reduce scheduling epochs")

    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "workload": {
            "task_ids": list(TASK_IDS),
            "logical_capacity": LOGICAL_CAPACITY.model_dump(mode="json"),
            "task_requests": {
                task_id: request.model_dump(mode="json")
                for task_id, request in TASK_REQUESTS.items()
            },
            "max_concurrency": MAX_CONCURRENCY,
            "max_parallelism": MAX_PARALLELISM,
            "declared_conflicts": [],
        },
        "static": static,
        "resource_aware": resource_aware,
        "comparison": comparison,
        "valid": not violations,
        "violations": violations,
        "scope": {
            "offline": True,
            "deterministic": True,
            "same_graph_shape_and_requests": True,
            "public_agentos_run_async": True,
            "authoritative_path": (
                "adaptive policy -> Scheduler logical admission -> TaskClaim -> "
                "Kernel Lease -> AsyncWorkerPool -> verifier -> VPG Evidence"
            ),
            "static_baseline": (
                "conflict-aware fixed parallelism without policy-side resource fitting"
            ),
            "capacity_semantics": (
                "proposal violations are advisory over-capacity selections; "
                "Scheduler admission remains authoritative"
            ),
            "does_not_measure": (
                "physical CPU/GPU/RAM/VRAM utilization, provider/model quality, "
                "wall-clock speedup, distributed placement, or production throughput"
            ),
        },
    }


def run_benchmark() -> dict[str, Any]:
    """Synchronous wrapper for tests, scripts, and ``python -m``."""

    return asyncio.run(run_benchmark_async())


def main() -> int:
    report = run_benchmark()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BENCHMARK_NAME",
    "BENCHMARK_VERSION",
    "LOGICAL_CAPACITY",
    "MAX_CONCURRENCY",
    "MAX_PARALLELISM",
    "TASK_IDS",
    "TASK_REQUESTS",
    "main",
    "run_benchmark",
    "run_benchmark_async",
]
