"""Focused tests for the unified adaptive admission policy."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk.compute_budget import (
    ComputeBudgetLimits,
    ComputeBudgetUsage,
    TaskComputeEstimate,
)
from lhos.sdk.conflict_graph import ConflictGraph, TaskAccessSet
from lhos.sdk.frontier_policy import FrontierAction
from lhos.sdk.runtime_state import (
    AgentCognitionState,
    CognitionAttemptState,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourcePoolState,
    ResourceRuntimeState,
    ResourceVectorState,
)
from lhos.sdk.unified_policy import UnifiedAdaptivePolicy


def _vector(*, cpu: int = 0, gpu: int = 0) -> ResourceVectorState:
    return ResourceVectorState(cpu_millis=cpu, gpu_count=gpu)


def _state(
    *,
    ready: tuple[str, ...] = ("a", "b", "c"),
    repair_ready: tuple[str, ...] = (),
    stale: tuple[str, ...] = (),
    active: tuple[CognitionAttemptState, ...] = (),
    projection_hash: str = "p" * 64,
    graph_id: str = "graph",
    progress_graph_id: str = "graph",
    cognition_available: bool = True,
    resources_available: bool = True,
    pools: tuple[ResourcePoolState, ...] | None = None,
) -> GlobalRuntimeState:
    if pools is None:
        capacity = _vector(cpu=1_000)
        pools = (
            ResourcePoolState(
                pool_id="worker",
                capacity=capacity,
                reserved=_vector(),
                available=capacity,
            ),
        )
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id=graph_id,
        progress=ProgressSemanticState(
            graph_id=progress_graph_id,
            graph_version=3,
            projection_hash=projection_hash,
            graph_closed=False,
            goal_closed=False,
            ready_frontier=ready,
            repair_ready_frontier=repair_ready,
            verified_task_ids=(),
            stale_task_ids=stale,
            invalid_task_ids=(),
            unverified_task_ids=ready,
        ),
        agent_cognition=AgentCognitionState(
            available=cognition_available,
            reason=None if cognition_available else "cognition unavailable",
            current_attempts=active,
        ),
        context={"available": False},
        resources=ResourceRuntimeState(
            available=resources_available,
            reason=None if resources_available else "resources unavailable",
            pools=pools,
        ),
    )


def _estimate(
    task_id: str,
    *,
    progress: int = 1,
    cost: int = 1,
    tokens: int = 1,
    known: bool = True,
) -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=progress,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=cost,
        estimated_tokens=tokens,
        estimated_wall_time_ms=tokens,
        estimated_cost_microusd=tokens,
        estimated_context_tokens=tokens,
        estimated_verification_tokens=tokens,
        known=known,
    )


def _limits(
    *,
    tokens: int | None = None,
    wall_time_ms: int | None = None,
    cost_microusd: int | None = None,
    context_tokens: int | None = None,
    verification_tokens: int | None = None,
) -> ComputeBudgetLimits:
    return ComputeBudgetLimits(
        max_tokens=tokens,
        max_wall_time_ms=wall_time_ms,
        max_cost_microusd=cost_microusd,
        max_context_tokens=context_tokens,
        max_verification_tokens=verification_tokens,
    )


def _graph(*task_ids: str, conflict: tuple[str, str] | None = None) -> ConflictGraph:
    access = [
        TaskAccessSet(task_id=task_id, write_set=(f"out://{task_id}",)) for task_id in task_ids
    ]
    if conflict is not None:
        left, right = conflict
        access = [
            TaskAccessSet(
                task_id=task_id,
                write_set=("workspace://shared",)
                if task_id in {left, right}
                else (f"out://{task_id}",),
            )
            for task_id in task_ids
        ]
    return ConflictGraph.from_access_sets(access)


def _requests(*task_ids: str, cpu: int = 100) -> dict[str, dict[str, int]]:
    return {task_id: {"cpu_millis": cpu} for task_id in task_ids}


def test_exact_utility_order_and_repair_priority() -> None:
    state = _state(
        ready=("slow", "fast", "repair"),
        repair_ready=("repair",),
        stale=("repair",),
    )
    estimates = {
        "slow": _estimate("slow", progress=2, cost=3),
        "fast": _estimate("fast", progress=3, cost=2),
        "repair": _estimate("repair", progress=1, cost=100),
    }
    plan = UnifiedAdaptivePolicy(max_parallelism=3).plan(
        state,
        _graph("slow", "fast", "repair"),
        _requests("slow", "fast", "repair"),
        estimates,
        _limits(),
        ComputeBudgetUsage(),
    )
    assert plan.candidate_task_ids == ("repair", "fast", "slow")
    assert plan.selected_task_ids == plan.candidate_task_ids
    assert plan.decisions[0].tier == "repair"
    assert plan.decisions[0].reason == "selected_repair"


def test_conflict_rejection_backfills_independent_candidate() -> None:
    state = _state(ready=("a", "b", "c"))
    graph = _graph("a", "b", "c", conflict=("a", "b"))
    estimates = {
        "a": _estimate("a", progress=100),
        "b": _estimate("b", progress=90),
        "c": _estimate("c", progress=1),
    }
    plan = UnifiedAdaptivePolicy(max_parallelism=3).plan(
        state,
        graph,
        _requests("a", "b", "c"),
        estimates,
        _limits(),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ("a", "c")
    assert next(item for item in plan.decisions if item.task_id == "b").reason == "conflict"
    assert next(item for item in plan.decisions if item.task_id == "b").conflict_blockers == ("a",)


def test_resource_rejection_backfills_smaller_candidate() -> None:
    state = _state(ready=("large", "small"))
    plan = UnifiedAdaptivePolicy(max_parallelism=2).plan(
        state,
        _graph("large", "small"),
        {"large": {"cpu_millis": 1_001}, "small": {"cpu_millis": 100}},
        {"large": _estimate("large", progress=100), "small": _estimate("small")},
        _limits(),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ("small",)
    large = next(item for item in plan.decisions if item.task_id == "large")
    assert large.reason == "insufficient_resources"
    assert large.resource_blockers


def test_budget_rejection_backfills_and_accumulates_only_selected_usage() -> None:
    state = _state(ready=("too_expensive", "fits", "also_fits"))
    estimates = {
        "too_expensive": _estimate("too_expensive", progress=100, tokens=6),
        "fits": _estimate("fits", progress=2, tokens=2),
        "also_fits": _estimate("also_fits", progress=1, tokens=2),
    }
    plan = UnifiedAdaptivePolicy(max_parallelism=3).plan(
        state,
        _graph("too_expensive", "fits", "also_fits"),
        _requests("too_expensive", "fits", "also_fits"),
        estimates,
        _limits(tokens=4),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ("fits", "also_fits")
    assert plan.usage_after.tokens == 4
    assert next(item for item in plan.decisions if item.task_id == "too_expensive").reason == (
        "budget_exceeded"
    )
    assert plan.remaining_after.tokens == 0


def test_conflict_resource_budget_combination_backfills() -> None:
    state = _state(ready=("conflict", "oversized", "good"))
    graph = _graph("conflict", "oversized", "good", conflict=("conflict", "good"))
    estimates = {
        "conflict": _estimate("conflict", progress=100),
        "oversized": _estimate("oversized", progress=90, tokens=9),
        "good": _estimate("good", progress=1, tokens=1),
    }
    plan = UnifiedAdaptivePolicy(max_parallelism=3).plan(
        state,
        graph,
        {
            "conflict": {"cpu_millis": 100},
            "oversized": {"cpu_millis": 1_001},
            "good": {"cpu_millis": 100},
        },
        estimates,
        _limits(tokens=2),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ("conflict",)
    assert next(item for item in plan.decisions if item.task_id == "oversized").reason == (
        "budget_exceeded"
    )
    assert next(item for item in plan.decisions if item.task_id == "good").reason == "conflict"


def test_unknown_contracts_fail_closed_but_unknown_access_can_run_serially() -> None:
    state = _state(ready=("unknown_access", "missing_request", "unknown_estimate"))
    graph = ConflictGraph.from_access_sets(())
    plan = UnifiedAdaptivePolicy(max_parallelism=3).plan(
        state,
        graph,
        {"unknown_access": {"cpu_millis": 100}},
        {
            "unknown_access": _estimate("unknown_access"),
            "missing_request": _estimate("missing_request"),
            "unknown_estimate": _estimate("unknown_estimate", known=False),
        },
        _limits(),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ("unknown_access",)
    assert plan.safe_under_constraints is False
    assert plan.safe_under_declared_resources is False
    assert {item.reason for item in plan.decisions} >= {
        "unknown_access_serial_only",
        "resource_request_unknown",
        "estimate_unknown",
    }
    assert any(item.name == "estimates.unknown_estimate" for item in plan.unavailable)
    assert any(item.name == "task_resources.missing_request" for item in plan.unavailable)


def test_unknown_access_selection_blocks_later_known_candidate_when_graph_omits_it() -> None:
    state = _state(ready=("hidden", "known"))
    graph = ConflictGraph.from_access_sets(
        (TaskAccessSet(task_id="known", write_set=("workspace://known",)),)
    )
    plan = UnifiedAdaptivePolicy(max_parallelism=2).plan(
        state,
        graph,
        _requests("hidden", "known"),
        {
            "hidden": _estimate("hidden", progress=10),
            "known": _estimate("known", progress=1),
        },
        _limits(),
        ComputeBudgetUsage(),
    )

    assert plan.selected_task_ids == ("hidden",)
    known = next(item for item in plan.decisions if item.task_id == "known")
    assert known.reason == "unknown_access_serial_only"
    assert known.conflict_blockers == ("hidden",)
    assert plan.safe_under_constraints is False


def test_active_conflict_blocks_candidate_and_allows_independent_backfill() -> None:
    active = CognitionAttemptState(
        claim_id="claim-live",
        claim_state="active",
        task_id="live",
        agent_id="worker",
        process_id="process",
        attempt_id="attempt-live",
        read_set=(
            {
                "operation": "read",
                "resource_uri": "workspace://shared",
                "known": True,
            },
        ),
        write_set=(),
    )
    state = _state(ready=("blocked", "free"), active=(active,))
    graph = ConflictGraph.from_access_sets(
        [
            TaskAccessSet(task_id="blocked", write_set=("workspace://shared",)),
            TaskAccessSet(task_id="free", write_set=("workspace://free",)),
        ]
    )
    plan = UnifiedAdaptivePolicy(max_parallelism=2).plan(
        state,
        graph,
        _requests("blocked", "free"),
        {"blocked": _estimate("blocked"), "free": _estimate("free")},
        _limits(),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ("free",)
    blocked = next(item for item in plan.decisions if item.task_id == "blocked")
    assert blocked.reason == "active_conflict"
    assert blocked.active_conflict_blockers == ("active:attempt-live",)


def test_unbounded_remaining_and_hash_are_deterministic_and_frozen() -> None:
    state = _state(ready=("b", "a"))
    graph = _graph("a", "b")
    kwargs = (
        state,
        graph,
        _requests("a", "b"),
        {"a": _estimate("a"), "b": _estimate("b")},
        _limits(),
        ComputeBudgetUsage(tokens=1),
    )
    first = UnifiedAdaptivePolicy(max_parallelism=2).plan(*kwargs, epoch_id=8)
    second = UnifiedAdaptivePolicy(max_parallelism=2).plan(
        state,
        graph,
        dict(reversed(list(_requests("a", "b").items()))),
        {"b": _estimate("b"), "a": _estimate("a")},
        _limits(),
        ComputeBudgetUsage(tokens=1),
        epoch_id=8,
    )
    assert first == second
    assert first.decision_hash == second.decision_hash
    assert first.remaining_before.tokens is None
    assert first.remaining_after.tokens is None
    with pytest.raises(ValidationError):
        first.selected_task_ids += ("x",)  # type: ignore[misc]


def test_graph_projection_and_resource_cognition_unknowns_do_not_run() -> None:
    state = _state(
        graph_id="graph-a",
        progress_graph_id="graph-b",
        projection_hash="",
        cognition_available=False,
        resources_available=False,
    )
    plan = UnifiedAdaptivePolicy().plan(
        state,
        _graph("a"),
        _requests("a"),
        {"a": _estimate("a")},
        _limits(),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ()
    assert plan.decisions[0].action is FrontierAction.DEFER
    assert plan.decisions[0].reason == "graph_fence_invalid"
    assert plan.safe_under_constraints is False
    names = {item.name for item in plan.unavailable}
    assert {"graph_fence", "projection_hash", "agent_cognition", "resources"} <= names
