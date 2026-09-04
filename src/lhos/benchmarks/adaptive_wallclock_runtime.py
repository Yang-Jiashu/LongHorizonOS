"""Bounded real-wall-clock benchmark for graph/resource/conflict-aware AgentOS.

The benchmark executes the same four-task READY frontier twice through the
public :meth:`lhos.sdk.AgentOS.run_async` path:

``static``
    A conflict-aware but resource-blind fixed-parallelism batch proposes the
    first two lexical tasks.  They request 700 + 700 CPU millicores against a
    declared 1,000-millicore pool, so the authoritative Scheduler rejects one
    proposal and needs a third scheduling epoch.

``adaptive``
    The resource- and conflict-aware policy packs 700 + 300 millicores in each
    epoch and closes the same VERIFIED Goal in two epochs.

Executors perform actual ``asyncio.sleep`` calls and elapsed time is measured
with ``time.perf_counter``.  This is therefore a real local wall-clock
measurement, not a simulated clock.  It remains a deterministic synthetic I/O
workload: it is not an LLM, GPU, physical-resource, or production benchmark.
Wall-clock values are reported but are deliberately not used as a correctness
gate because operating-system and CI scheduling noise can dominate tiny runs.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Final

from lhos.runtimes.multi_agent import AttemptState, ClaimState, ResourceVector
from lhos.runtimes.verified_progress.models import EvidenceNode, EvidenceResult
from lhos.sdk import (
    Agent,
    AgentOS,
    ConflictGraph,
    Goal,
    TaskAccessSet,
    VerificationOutcome,
)

BENCHMARK_NAME: Final[str] = "adaptive_wallclock_runtime"
BENCHMARK_VERSION: Final[int] = 1
AGENT_ID: Final[str] = "wallclock-worker"
DEFAULT_DELAY_SECONDS: Final[float] = 0.02
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
SHARED_CONFLICT_RESOURCE: Final[str] = "workspace://shared-control"


@dataclass
class _WallClockWorkload:
    """Instrument real executor overlap and declared logical resource use."""

    delay_seconds: float
    active: ResourceVector = field(default_factory=ResourceVector)
    peak: ResourceVector = field(default_factory=ResourceVector)
    active_count: int = 0
    peak_parallelism: int = 0
    completed: list[str] = field(default_factory=list)
    capacity_violation_events: int = 0
    started_at: dict[str, float] = field(default_factory=dict)
    finished_at: dict[str, float] = field(default_factory=dict)

    async def execute(self, task_id: str) -> None:
        request = TASK_REQUESTS[task_id]
        self.started_at[task_id] = time.perf_counter()
        self.active = self.active.plus(request)
        self.peak = _component_max(self.peak, self.active)
        self.active_count += 1
        self.peak_parallelism = max(self.peak_parallelism, self.active_count)
        if self.active.shortages(LOGICAL_CAPACITY):
            self.capacity_violation_events += 1
        try:
            await asyncio.sleep(self.delay_seconds)
            self.completed.append(task_id)
        finally:
            self.finished_at[task_id] = time.perf_counter()
            self.active = self.active.minus(request)
            self.active_count -= 1


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
        artifact_id=f"wallclock-benchmark-{task_id}",
        version=1,
        content=f"{task_id}:verified",
        evidence_note="deterministic real-sleep wall-clock benchmark",
    )


def _conflict_graph() -> ConflictGraph:
    # a-heavy and d-light form one explicit write/write conflict.  The
    # resource-aware packing trace never groups them, while also fitting the
    # logical capacity.  This gives the benchmark a real conflict constraint
    # without hiding the resource-rejection difference being measured.
    return ConflictGraph.from_access_sets(
        [
            TaskAccessSet(
                task_id=task_id,
                read_set=(f"workspace://input/{task_id}",),
                write_set=(
                    (SHARED_CONFLICT_RESOURCE,)
                    if task_id in {"a-heavy", "d-light"}
                    else (f"workspace://output/{task_id}",)
                ),
            )
            for task_id in TASK_IDS
        ]
    )


def _batch_is_conflict_safe(
    task_ids: tuple[str, ...],
    conflict_graph: ConflictGraph,
) -> bool:
    return all(
        not conflict_graph.conflicts_with(left, right)
        for index, left in enumerate(task_ids)
        for right in task_ids[index + 1 :]
    )


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _runtime_audit(runtime: AgentOS, graph_id: str) -> dict[str, Any]:
    claims = [claim for claim in runtime.scheduler.claims if claim.graph_id == graph_id]
    attempts = [attempt for attempt in runtime.scheduler.attempts if attempt.graph_id == graph_id]
    active_states = {
        ClaimState.PROPOSED.value,
        ClaimState.ACQUIRING.value,
        ClaimState.ACTIVE.value,
    }
    nodes, _edges = runtime.vpg.snapshot_projection(graph_id)
    pass_evidence = [
        node
        for node in nodes.values()
        if isinstance(node, EvidenceNode) and node.result is EvidenceResult.PASS
    ]
    return {
        "graph_id": graph_id,
        "graph_version": runtime.vpg.get_graph(graph_id).current_version,
        "scheduler_attempts": len(attempts),
        "attempt_state_counts": dict(
            sorted(Counter(_enum_value(attempt.state) for attempt in attempts).items())
        ),
        "claims_created": len(claims),
        "claims_with_kernel_lease": sum(bool(claim.lease_id) for claim in claims),
        "claims_with_positive_fence": sum(
            isinstance(claim.lease_fencing_token, int)
            and not isinstance(claim.lease_fencing_token, bool)
            and claim.lease_fencing_token > 0
            for claim in claims
        ),
        "active_claims_after_run": sum(
            _enum_value(claim.state) in active_states for claim in claims
        ),
        "active_reservations_after_run": len(runtime.scheduler.resource_manager.list_active()),
        "live_kernel_leases_after_run": (
            0 if runtime.kernel is None else len(runtime.kernel._lease_service.list_all_leases())
        ),
        "pass_evidence_nodes": len(pass_evidence),
        "valid_evidence_bindings_by_task": {
            task_id: len(runtime._vpg_surface.task_evidence_bindings(graph_id, task_id))
            for task_id in TASK_IDS
        },
    }


def _resource_rejection_count(epochs: tuple[dict[str, Any], ...]) -> int:
    return sum(
        "insufficient resources" in str(reason).lower()
        for epoch in epochs
        for _task_id, reason in epoch.get("scheduler_skipped", ())
    )


async def _run_case(
    *,
    mode: str,
    resource_aware: bool,
    delay_seconds: float,
) -> dict[str, Any]:
    workload = _WallClockWorkload(delay_seconds=delay_seconds)
    runtime = AgentOS(":memory:")
    conflict_graph = _conflict_graph()
    try:
        runtime.add_agent(
            Agent(
                AGENT_ID,
                executor=workload.execute,
                specializations=("wallclock-benchmark",),
                max_concurrency=MAX_CONCURRENCY,
                resource_capacity=LOGICAL_CAPACITY,
            )
        )
        goal = Goal(f"adaptive-wallclock-{mode}")
        for task_id, request in TASK_REQUESTS.items():
            goal.task(
                task_id,
                agent=AGENT_ID,
                required_specializations=("wallclock-benchmark",),
                resources=request,
                inputs=(f"workspace://input/{task_id}",),
                outputs=(
                    (SHARED_CONFLICT_RESOURCE,)
                    if task_id in {"a-heavy", "d-light"}
                    else (f"workspace://output/{task_id}",)
                ),
                verify=lambda task_id=task_id: _verification(task_id),
            )

        started = time.perf_counter()
        result = await runtime.run_async(
            goal,
            max_dispatches=len(TASK_IDS),
            max_steps=len(TASK_IDS) * 2,
            max_concurrency=MAX_CONCURRENCY,
            adaptive=True,
            resource_aware=resource_aware,
            conflict_graph=conflict_graph,
            max_parallelism=MAX_PARALLELISM,
            persist_adaptive_epochs=False,
            automatic_rebase=False,
        )
        elapsed_seconds = time.perf_counter() - started
        epochs = tuple(dict(epoch) for epoch in result.meta.get("adaptive_epochs", ()))
        selected_batches = tuple(
            tuple(str(task_id) for task_id in epoch.get("selected_task_ids", ()))
            for epoch in epochs
        )
        admitted_batches = tuple(
            tuple(str(task_id) for task_id in epoch.get("actual_dispatched_task_ids", ()))
            for epoch in epochs
        )
        graph_id = runtime._goal_gid[goal.goal_id]
        audit = _runtime_audit(runtime, graph_id)
        verified = tuple(sorted(str(task_id) for task_id in result.verified))
        closure = result.goal_state == "closed" and verified == tuple(sorted(TASK_IDS))
        correctness = {
            "goal_closed": closure,
            "all_tasks_verified": verified == tuple(sorted(TASK_IDS)),
            "executor_completed_all_tasks": (
                tuple(sorted(workload.completed)) == tuple(sorted(TASK_IDS))
            ),
            "all_attempts_crossed_claim_and_lease": (
                audit["scheduler_attempts"] == len(TASK_IDS)
                and audit["claims_created"] == len(TASK_IDS)
                and audit["claims_with_kernel_lease"] == len(TASK_IDS)
                and audit["claims_with_positive_fence"] == len(TASK_IDS)
            ),
            "semantic_attempt_count_matches_verified_tasks": (
                audit["attempt_state_counts"].get(AttemptState.VERIFIED_SEMANTICALLY.value, 0)
                == len(TASK_IDS)
            ),
            "all_verified_tasks_have_valid_evidence": all(
                audit["valid_evidence_bindings_by_task"].get(task_id, 0) == 1
                for task_id in TASK_IDS
            ),
            "no_executor_capacity_violation": (workload.capacity_violation_events == 0),
            "selected_batches_conflict_safe": all(
                _batch_is_conflict_safe(batch, conflict_graph) for batch in selected_batches
            ),
            "admitted_batches_conflict_safe": all(
                _batch_is_conflict_safe(batch, conflict_graph) for batch in admitted_batches
            ),
            "no_active_claims": audit["active_claims_after_run"] == 0,
            "no_active_reservations": (audit["active_reservations_after_run"] == 0),
            "no_live_kernel_leases": audit["live_kernel_leases_after_run"] == 0,
        }
        return {
            "mode": mode,
            "policy": (
                "resource-aware-conflict-greedy.v1"
                if resource_aware
                else "conflict-aware-fixed-parallelism.v1"
            ),
            "resource_aware": resource_aware,
            "elapsed_seconds": round(elapsed_seconds, 6),
            "closure": closure,
            "goal_state": result.goal_state,
            "verified_task_ids": list(verified),
            "epochs": len(epochs),
            "selected_batches": [list(batch) for batch in selected_batches],
            "admitted_batches": [list(batch) for batch in admitted_batches],
            "scheduler_resource_rejections": _resource_rejection_count(epochs),
            "dispatched_attempts": int(result.meta.get("dispatched", 0)),
            "executor_completed_task_ids": sorted(workload.completed),
            "executor_peak_parallelism": workload.peak_parallelism,
            "executor_peak_resources": workload.peak.model_dump(mode="json"),
            "executor_capacity_violation_events": (workload.capacity_violation_events),
            "correctness": correctness,
            "runtime_audit": audit,
        }
    finally:
        runtime.close()


async def run_benchmark_async(
    *,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
) -> dict[str, Any]:
    """Run both cases and report real local elapsed time without speed gates."""

    if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)):
        raise ValueError("delay_seconds must be a positive number")
    if delay_seconds <= 0:
        raise ValueError("delay_seconds must be positive")
    delay = float(delay_seconds)
    static = await _run_case(
        mode="static",
        resource_aware=False,
        delay_seconds=delay,
    )
    adaptive = await _run_case(
        mode="adaptive",
        resource_aware=True,
        delay_seconds=delay,
    )
    static_elapsed = float(static["elapsed_seconds"])
    adaptive_elapsed = float(adaptive["elapsed_seconds"])
    comparison = {
        "same_verified_goal": (
            static["closure"]
            and adaptive["closure"]
            and static["verified_task_ids"] == adaptive["verified_task_ids"]
        ),
        "epoch_reduction": static["epochs"] - adaptive["epochs"],
        "scheduler_resource_rejection_reduction": (
            static["scheduler_resource_rejections"] - adaptive["scheduler_resource_rejections"]
        ),
        "wall_time_reduction_seconds_observed": round(static_elapsed - adaptive_elapsed, 6),
        "observed_speedup": (
            round(static_elapsed / adaptive_elapsed, 6) if adaptive_elapsed > 0 else None
        ),
        "wall_clock_is_informational_not_a_gate": True,
    }
    violations: list[str] = []
    for label, case in (("static", static), ("adaptive", adaptive)):
        if not all(case["correctness"].values()):
            violations.append(f"{label} correctness contract failed")
    if static["selected_batches"] != [
        ["a-heavy", "b-heavy"],
        ["b-heavy", "c-light"],
        ["d-light"],
    ]:
        violations.append("static batch trace changed")
    if adaptive["selected_batches"] != [
        ["a-heavy", "c-light"],
        ["b-heavy", "d-light"],
    ]:
        violations.append("adaptive batch trace changed")
    if static["scheduler_resource_rejections"] < 1:
        violations.append("static baseline did not exercise Scheduler rejection")
    if adaptive["scheduler_resource_rejections"] != 0:
        violations.append("adaptive policy incurred a Scheduler resource rejection")
    if adaptive["epochs"] >= static["epochs"]:
        violations.append("adaptive policy did not reduce scheduling epochs")

    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "workload": {
            "task_ids": list(TASK_IDS),
            "delay_seconds_per_task": delay,
            "logical_capacity": LOGICAL_CAPACITY.model_dump(mode="json"),
            "task_requests": {
                task_id: request.model_dump(mode="json")
                for task_id, request in TASK_REQUESTS.items()
            },
            "max_concurrency": MAX_CONCURRENCY,
            "max_parallelism": MAX_PARALLELISM,
            "declared_conflicts": [
                {
                    "left_task_id": "a-heavy",
                    "right_task_id": "d-light",
                    "resource": SHARED_CONFLICT_RESOURCE,
                    "reason": "write_write",
                }
            ],
        },
        "static": static,
        "adaptive": adaptive,
        "comparison": comparison,
        "valid": not violations,
        "violations": violations,
        "scope": {
            "offline": True,
            "bounded": True,
            "deterministic_workload": True,
            "real_asyncio_sleep": True,
            "real_perf_counter_wall_clock": True,
            "simulated_clock": False,
            "public_agentos_run_async": True,
            "authoritative_path": (
                "adaptive policy -> Scheduler logical admission -> TaskClaim -> "
                "Kernel Lease -> AsyncWorkerPool -> async executor -> verifier -> "
                "VPG Evidence"
            ),
            "wall_clock_gate": False,
            "wall_clock_caveat": (
                "elapsed values are local observations; correctness gates use "
                "graph closure, epochs, rejection counts, ownership, and Evidence"
            ),
            "does_not_measure": (
                "real LLM/provider quality or latency, tokens, dollars, physical "
                "CPU/GPU/RAM/VRAM utilization or isolation, distributed placement, "
                "or production throughput"
            ),
        },
    }


def run_benchmark(**kwargs: Any) -> dict[str, Any]:
    """Synchronous wrapper for tests, CLI, and ``python -m``."""

    return asyncio.run(run_benchmark_async(**kwargs))


def main() -> int:
    report = run_benchmark()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BENCHMARK_NAME",
    "BENCHMARK_VERSION",
    "DEFAULT_DELAY_SECONDS",
    "LOGICAL_CAPACITY",
    "MAX_CONCURRENCY",
    "MAX_PARALLELISM",
    "TASK_IDS",
    "TASK_REQUESTS",
    "main",
    "run_benchmark",
    "run_benchmark_async",
]
