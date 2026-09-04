"""Controlled benchmark for composing budget, conflict, and resource policy.

The existing benchmark modules isolate one mechanism at a time.  This module
keeps the workload fixed and compares four deterministic *policy shapes*:

``static_fifo``
    lexical FIFO with a fixed parallelism bound; it is blind to budget,
    conflicts, and resource fit.
``budget_only``
    ``VerifiedProgressBudgetPolicy`` followed by authoritative logical
    resource admission, but no conflict-aware proposal.
``resource_conflict``
    ``ResourceAwareParallelismPolicy`` with no compute-budget ordering.
``unified``
    budget-ranked candidates are then filtered by the same explicit conflict
    and logical-resource constraints.  A blocked high-utility candidate does
    not prevent a later safe candidate from filling the batch.

The workload is intentionally synthetic and in-memory.  It exercises the
public policy DTOs, not real LLM calls, provider billing, physical CPU/GPU
telemetry, or production throughput.  ``budget_usage`` is therefore declared
estimate accounting, not measured provider consumption.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Final, Literal

from lhos.sdk import (
    AgentCognitionState,
    ComputeBudgetLimits,
    ComputeBudgetUsage,
    ConflictGraph,
    ContextRuntimeState,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceAwareParallelismPolicy,
    ResourcePoolState,
    ResourceRuntimeState,
    ResourceTaskRequest,
    ResourceVectorState,
    TaskAccessSet,
    TaskComputeEstimate,
    VerifiedProgressBudgetPolicy,
)
from lhos.sdk.unified_policy import UnifiedAdaptivePolicy

BENCHMARK_NAME: Final[str] = "unified_compute_control"
BENCHMARK_VERSION: Final[int] = 1
MAX_PARALLELISM: Final[int] = 2
MAX_EPOCHS: Final[int] = 16
CAPACITY: Final[ResourceVectorState] = ResourceVectorState(cpu_millis=1_000)
ZERO_RESOURCES: Final[ResourceVectorState] = ResourceVectorState()
LIMITS: Final[ComputeBudgetLimits] = ComputeBudgetLimits(
    max_tokens=700,
    max_wall_time_ms=700,
    max_cost_microusd=70,
    max_context_tokens=400,
    max_verification_tokens=120,
)

PolicyMode = Literal["static_fifo", "budget_only", "resource_conflict", "unified"]


@dataclass(frozen=True)
class _Task:
    task_id: str
    progress: int
    tokens: int
    wall_time_ms: int
    cost_microusd: int
    context_tokens: int
    verification_tokens: int
    normalized_cost: int
    resources: ResourceVectorState
    read_set: tuple[str, ...]
    write_set: tuple[str, ...]
    risk_units: int

    def estimate(self) -> TaskComputeEstimate:
        return TaskComputeEstimate(
            task_id=self.task_id,
            verified_progress_units=self.progress,
            success_basis_points=10_000,
            input_stability_basis_points=10_000,
            normalized_cost_units=self.normalized_cost,
            expected_rework_cost_units=0,
            estimated_tokens=self.tokens,
            estimated_wall_time_ms=self.wall_time_ms,
            estimated_cost_microusd=self.cost_microusd,
            estimated_context_tokens=self.context_tokens,
            estimated_verification_tokens=self.verification_tokens,
            known=True,
        )


@dataclass
class _Run:
    mode: PolicyMode
    verified: set[str]
    pending: set[str]
    stale: set[str]
    usage: ComputeBudgetUsage
    proposed_batches: list[list[str]]
    admitted_batches: list[list[str]]
    epochs: int
    scheduler_rejections: int
    stale_attempts: int
    stale_risk_units: int
    attempts: dict[str, int]
    budget_plan_batches: list[list[str]]
    policy_decision_hashes: list[str]
    budget_overrun_dimensions: tuple[str, ...]
    dispatches: int


def _tasks() -> dict[str, _Task]:
    # ``a-repair`` and ``c-frontend`` are the first budget-ranked pair.  The
    # latter does not fit after the repair; the unified policy must continue
    # scanning and safely choose ``d-docs`` instead.  ``a-repair`` and
    # ``b-backend`` have a write/write conflict, creating controlled stale
    # work for the FIFO baseline.
    return {
        "a-repair": _Task(
            "a-repair",
            progress=10,
            tokens=100,
            wall_time_ms=100,
            cost_microusd=10,
            context_tokens=50,
            verification_tokens=20,
            normalized_cost=50,
            resources=ResourceVectorState(cpu_millis=400),
            read_set=("artifact://requirements",),
            write_set=("artifact://api",),
            risk_units=8,
        ),
        "b-backend": _Task(
            "b-backend",
            progress=30,
            tokens=240,
            wall_time_ms=240,
            cost_microusd=24,
            context_tokens=120,
            verification_tokens=40,
            normalized_cost=120,
            resources=ResourceVectorState(cpu_millis=500),
            read_set=("artifact://api",),
            write_set=("artifact://api",),
            risk_units=10,
        ),
        "c-frontend": _Task(
            "c-frontend",
            progress=40,
            tokens=180,
            wall_time_ms=180,
            cost_microusd=18,
            context_tokens=90,
            verification_tokens=30,
            normalized_cost=90,
            resources=ResourceVectorState(cpu_millis=700),
            read_set=("artifact://api",),
            write_set=("artifact://frontend",),
            risk_units=6,
        ),
        "d-docs": _Task(
            "d-docs",
            progress=20,
            tokens=120,
            wall_time_ms=120,
            cost_microusd=12,
            context_tokens=60,
            verification_tokens=20,
            normalized_cost=80,
            resources=ResourceVectorState(cpu_millis=500),
            read_set=("artifact://requirements",),
            write_set=("artifact://docs",),
            risk_units=2,
        ),
    }


def _conflicts(tasks: dict[str, _Task]) -> ConflictGraph:
    return ConflictGraph.from_access_sets(
        TaskAccessSet(
            task_id=task.task_id,
            read_set=task.read_set,
            write_set=task.write_set,
        )
        for task in tasks.values()
    )


def _requests(tasks: dict[str, _Task]) -> dict[str, ResourceTaskRequest]:
    return {
        task_id: ResourceTaskRequest(task_id=task_id, resources=task.resources)
        for task_id, task in tasks.items()
    }


def _hash_projection(epoch: int, pending: set[str], verified: set[str], stale: set[str]) -> str:
    payload = {
        "epoch": epoch,
        "pending": sorted(pending),
        "verified": sorted(verified),
        "stale": sorted(stale),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _state(
    *,
    epoch: int,
    pending: set[str],
    verified: set[str],
    stale: set[str],
) -> GlobalRuntimeState:
    pending_ids = tuple(sorted(pending))
    stale_ids = tuple(sorted(stale & pending))
    return GlobalRuntimeState(
        goal_id="unified-compute-goal",
        graph_id="unified-compute-graph",
        progress=ProgressSemanticState(
            graph_id="unified-compute-graph",
            graph_version=epoch + 1,
            projection_hash=_hash_projection(epoch, pending, verified, stale),
            graph_closed=False,
            goal_closed=False,
            ready_frontier=pending_ids,
            repair_ready_frontier=stale_ids,
            verified_task_ids=tuple(sorted(verified)),
            stale_task_ids=stale_ids,
            invalid_task_ids=(),
            unverified_task_ids=pending_ids,
        ),
        agent_cognition=AgentCognitionState(available=True),
        context=ContextRuntimeState(available=True),
        resources=ResourceRuntimeState(
            available=True,
            pools=(
                ResourcePoolState(
                    pool_id="cpu",
                    capacity=CAPACITY,
                    reserved=ZERO_RESOURCES,
                    available=CAPACITY,
                ),
            ),
        ),
    )


def _usage_for(tasks: dict[str, _Task], task_ids: list[str]) -> ComputeBudgetUsage:
    total = ComputeBudgetUsage()
    for task_id in task_ids:
        total = total.plus(tasks[task_id].estimate().total_budget_delta)
    return total


def _overrun_dimensions(usage: ComputeBudgetUsage) -> tuple[str, ...]:
    pairs = (
        ("tokens", LIMITS.max_tokens),
        ("wall_time_ms", LIMITS.max_wall_time_ms),
        ("cost_microusd", LIMITS.max_cost_microusd),
        ("context_tokens", LIMITS.max_context_tokens),
        ("verification_tokens", LIMITS.max_verification_tokens),
    )
    return tuple(
        name for name, limit in pairs if limit is not None and getattr(usage, name) > limit
    )


def _resource_fits(
    tasks: dict[str, _Task],
    task_ids: list[str],
) -> bool:
    used = 0
    for task_id in task_ids:
        used += tasks[task_id].resources.cpu_millis
    return used <= CAPACITY.cpu_millis


def _resource_conflict_safe(
    task_ids: list[str],
    graph: ConflictGraph,
) -> bool:
    return all(
        not graph.conflicts_with(left, right)
        for index, left in enumerate(task_ids)
        for right in task_ids[index + 1 :]
    )


def _resource_conflict_batch(
    state: GlobalRuntimeState,
    tasks: dict[str, _Task],
    graph: ConflictGraph,
    *,
    epoch: int,
) -> tuple[list[str], str]:
    suggestion = ResourceAwareParallelismPolicy(max_parallelism=MAX_PARALLELISM).suggest(
        state,
        graph,
        _requests(tasks),
        epoch_id=epoch,
    )
    return list(suggestion.selected_task_ids), suggestion.decision_hash


def _run_case(mode: PolicyMode) -> _Run:
    tasks = _tasks()
    graph = _conflicts(tasks)
    estimates = {task_id: task.estimate() for task_id, task in tasks.items()}
    pending = set(tasks)
    verified: set[str] = set()
    stale: set[str] = {"a-repair"}
    usage = ComputeBudgetUsage()
    proposed_batches: list[list[str]] = []
    admitted_batches: list[list[str]] = []
    budget_plan_batches: list[list[str]] = []
    decision_hashes: list[str] = []
    attempts = {task_id: 0 for task_id in tasks}
    scheduler_rejections = 0
    stale_attempts = 0
    stale_risk_units = 0
    epoch = 0

    while pending and epoch < MAX_EPOCHS:
        state = _state(epoch=epoch, pending=pending, verified=verified, stale=stale)
        budget_plan = None
        if mode in {"budget_only", "unified"}:
            budget_plan = VerifiedProgressBudgetPolicy().plan(
                state,
                estimates,
                LIMITS,
                usage,
                epoch_id=epoch,
                max_parallelism=MAX_PARALLELISM,
            )
            if mode == "budget_only":
                budget_plan_batches.append(list(budget_plan.selected_task_ids))
                decision_hashes.append(budget_plan.decision_hash)

        if mode == "static_fifo":
            proposed = sorted(pending)[:MAX_PARALLELISM]
        elif mode == "budget_only":
            proposed = list(budget_plan.selected_task_ids) if budget_plan is not None else []
        elif mode == "resource_conflict":
            proposed, decision_hash = _resource_conflict_batch(
                state,
                tasks,
                graph,
                epoch=epoch,
            )
            decision_hashes.append(decision_hash)
        else:
            assert budget_plan is not None
            unified_plan = UnifiedAdaptivePolicy(max_parallelism=MAX_PARALLELISM).plan(
                state,
                graph,
                _requests(tasks),
                estimates,
                LIMITS,
                usage,
                epoch_id=epoch,
            )
            budget_plan_batches.append(list(unified_plan.selected_task_ids))
            decision_hashes.append(unified_plan.decision_hash)
            proposed = list(unified_plan.selected_task_ids)

        proposed_batches.append(list(proposed))

        # The baseline and budget-only modes intentionally leave authoritative
        # resource admission to this small deterministic gate.  The other two
        # modes already fit resources, but passing through the gate keeps the
        # report's "admitted" field comparable.
        admitted: list[str] = []
        used_cpu = 0
        for task_id in proposed:
            request_cpu = tasks[task_id].resources.cpu_millis
            if used_cpu + request_cpu > CAPACITY.cpu_millis:
                scheduler_rejections += 1
                continue
            admitted.append(task_id)
            used_cpu += request_cpu
        admitted_batches.append(list(admitted))

        if not admitted:
            # A hard budget exhaustion or malformed policy must fail closed
            # rather than spin forever.
            break

        for task_id in admitted:
            attempts[task_id] += 1
            usage = usage.plus(tasks[task_id].estimate().total_budget_delta)

        # Conflict-blind modes may dispatch overlapping writes.  The lexical
        # winner is the only verified result; the loser remains pending and is
        # charged as stale/rework.
        for index, task_id in enumerate(admitted):
            conflict = any(graph.conflicts_with(task_id, prior) for prior in admitted[:index])
            if conflict:
                stale_attempts += 1
                stale_risk_units += tasks[task_id].risk_units
                stale.add(task_id)
                continue
            verified.add(task_id)
            pending.discard(task_id)
            stale.discard(task_id)
        epoch += 1

    overrun = _overrun_dimensions(usage)
    return _Run(
        mode=mode,
        verified=verified,
        pending=pending,
        stale=stale,
        usage=usage,
        proposed_batches=proposed_batches,
        admitted_batches=admitted_batches,
        epochs=epoch,
        scheduler_rejections=scheduler_rejections,
        stale_attempts=stale_attempts,
        stale_risk_units=stale_risk_units,
        attempts=attempts,
        budget_plan_batches=budget_plan_batches,
        policy_decision_hashes=decision_hashes,
        budget_overrun_dimensions=overrun,
        dispatches=sum(attempts.values()),
    )


def _run_payload(run: _Run, tasks: dict[str, _Task]) -> dict[str, Any]:
    total_progress = sum(task.progress for task in tasks.values())
    verified_progress = sum(tasks[task_id].progress for task_id in run.verified)
    return {
        "mode": run.mode,
        "goal_closed": not run.pending and run.verified == set(tasks),
        "verified_task_ids": sorted(run.verified),
        "pending_task_ids": sorted(run.pending),
        "declared_verified_progress_units": verified_progress,
        "declared_total_progress_units": total_progress,
        "progress_ratio": (verified_progress / total_progress if total_progress else 1.0),
        "epochs": run.epochs,
        "proposed_batches": run.proposed_batches,
        "admitted_batches": run.admitted_batches,
        "scheduler_rejections": run.scheduler_rejections,
        "stale_attempts": run.stale_attempts,
        "stale_risk_units": run.stale_risk_units,
        "attempts_by_task": dict(sorted(run.attempts.items())),
        "dispatches": run.dispatches,
        "budget_plan_batches": run.budget_plan_batches,
        "policy_decision_hashes": run.policy_decision_hashes,
        "budget_usage": run.usage.model_dump(mode="json"),
        "budget_overrun_dimensions": list(run.budget_overrun_dimensions),
        "policy_ids": {
            "budget": (
                "verified-progress-budget.v1" if run.mode in {"budget_only", "unified"} else None
            ),
            "resource_conflict": (
                "resource-aware-conflict-greedy.v1"
                if run.mode in {"resource_conflict", "unified"}
                else None
            ),
        },
    }


def run_benchmark() -> dict[str, Any]:
    """Run all policy shapes against one fixed synthetic graph."""

    tasks = _tasks()
    graph = _conflicts(tasks)
    runs = {
        mode: _run_payload(_run_case(mode), tasks)
        for mode in (
            "static_fifo",
            "budget_only",
            "resource_conflict",
            "unified",
        )
    }
    static = runs["static_fifo"]
    unified = runs["unified"]
    all_goal_closed = all(runs[mode]["goal_closed"] for mode in runs)
    same_verified_goal = all(runs[mode]["verified_task_ids"] == sorted(tasks) for mode in runs)
    comparison = {
        "same_verified_goal": bool(all_goal_closed and same_verified_goal),
        "unified_declared_verified_progress_units": unified["declared_verified_progress_units"],
        "static_declared_verified_progress_units": static["declared_verified_progress_units"],
        "unified_verified_progress_per_token": (
            unified["declared_verified_progress_units"] / unified["budget_usage"]["tokens"]
        ),
        "static_verified_progress_per_token": (
            static["declared_verified_progress_units"] / static["budget_usage"]["tokens"]
        ),
        "rework_risk_avoidance_units": static["stale_risk_units"] - unified["stale_risk_units"],
        "stale_attempt_reduction": static["stale_attempts"] - unified["stale_attempts"],
        "scheduler_rejection_reduction": (
            static["scheduler_rejections"] - unified["scheduler_rejections"]
        ),
        "epoch_reduction": static["epochs"] - unified["epochs"],
        "token_reduction": static["budget_usage"]["tokens"] - unified["budget_usage"]["tokens"],
        "wall_time_reduction_ms": (
            static["budget_usage"]["wall_time_ms"] - unified["budget_usage"]["wall_time_ms"]
        ),
        "unified_budget_within_declared_limits": not unified["budget_overrun_dimensions"],
        "static_budget_overrun": bool(static["budget_overrun_dimensions"]),
    }
    violations: list[str] = []
    if not comparison["same_verified_goal"]:
        violations.append("policy modes did not close the same VERIFIED Goal")
    if unified["stale_attempts"] != 0:
        violations.append("unified policy executed stale/conflicting work")
    if unified["scheduler_rejections"] != 0:
        violations.append("unified policy proposed an over-capacity batch")
    if not comparison["unified_budget_within_declared_limits"]:
        violations.append("unified budget usage exceeded a declared limit")
    if comparison["rework_risk_avoidance_units"] <= 0:
        violations.append("scenario did not expose avoidable rework risk")
    if comparison["scheduler_rejection_reduction"] <= 0:
        violations.append("scenario did not expose scheduler rejection reduction")
    return {
        "benchmark": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "valid": not violations,
        "violations": violations,
        "scenario": {
            "task_ids": sorted(tasks),
            "task_progress_units": {
                task_id: task.progress for task_id, task in sorted(tasks.items())
            },
            "task_resources": {
                task_id: task.resources.model_dump(mode="json")
                for task_id, task in sorted(tasks.items())
            },
            "declared_accesses": {
                task_id: {
                    "read_set": list(task.read_set),
                    "write_set": list(task.write_set),
                }
                for task_id, task in sorted(tasks.items())
            },
            "conflict_pairs": [pair.model_dump(mode="json") for pair in graph.conflicts],
            "capacity": CAPACITY.model_dump(mode="json"),
            "limits": LIMITS.model_dump(mode="json"),
            "max_parallelism": MAX_PARALLELISM,
        },
        **runs,
        "comparison": comparison,
        "scope": {
            "offline": True,
            "deterministic": True,
            "same_graph_and_estimates": True,
            "policy_dto_dependencies": [
                "VerifiedProgressBudgetPolicy",
                "ResourceAwareParallelismPolicy",
                "ConflictGraph",
            ],
            "budget_semantics": (
                "declared estimate accounting only; no provider billing or measured usage"
            ),
            "resource_semantics": (
                "logical Scheduler capacity; no physical CPU/GPU/RAM/VRAM telemetry"
            ),
            "does_not_measure": (
                "real LLM quality/latency, model routing, context reconstruction, "
                "physical resources, distributed scheduling, or production throughput"
            ),
            "interpretation": (
                "The report validates composition mechanics and controlled rework/"
                "admission contrasts. It is not evidence of real-world speed or cost savings."
            ),
        },
    }


def main() -> int:
    report = run_benchmark()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BENCHMARK_NAME",
    "BENCHMARK_VERSION",
    "LIMITS",
    "MAX_PARALLELISM",
    "main",
    "run_benchmark",
]
