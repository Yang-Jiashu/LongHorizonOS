"""Additional fail-closed contract tests for compute-budget DTOs/policy."""

from __future__ import annotations

import pytest

from lhos.sdk.compute_budget import (
    ComputeBudgetLimits,
    ComputeBudgetRemaining,
    ComputeBudgetUsage,
    TaskComputeEstimate,
    VerifiedProgressBudgetPolicy,
)
from lhos.sdk.runtime_state import (
    AgentCognitionState,
    ContextRuntimeState,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceRuntimeState,
)


def _state(*, projection_hash: str = "p" * 64) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=1,
            projection_hash=projection_hash,
            graph_closed=False,
            goal_closed=False,
            ready_frontier=("task",),
            repair_ready_frontier=(),
            verified_task_ids=(),
            stale_task_ids=(),
            invalid_task_ids=(),
            unverified_task_ids=("task",),
        ),
        agent_cognition=AgentCognitionState(available=True),
        context=ContextRuntimeState(available=False, reason="not bound"),
        resources=ResourceRuntimeState(available=True),
    )


def _estimate() -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id="task",
        verified_progress_units=1,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=1,
        known=True,
    )


def test_compute_budget_usage_plus_rejects_non_usage_values() -> None:
    with pytest.raises(TypeError, match="ComputeBudgetUsage"):
        ComputeBudgetUsage().plus({"tokens": 1})  # type: ignore[arg-type]


def test_remaining_capacity_distinguishes_unbounded_from_exhausted() -> None:
    remaining = ComputeBudgetRemaining.from_limits(
        ComputeBudgetLimits(max_tokens=10, max_wall_time_ms=None),
        ComputeBudgetUsage(tokens=12, wall_time_ms=999),
    )

    assert remaining.tokens == 0
    assert remaining.wall_time_ms is None
    assert remaining.unbounded_dimensions == (
        "wall_time_ms",
        "cost_microusd",
        "context_tokens",
        "verification_tokens",
    )


def test_plan_remaining_uses_none_for_unbounded_dimensions() -> None:
    plan = VerifiedProgressBudgetPolicy().plan(
        _state(),
        {"task": _estimate()},
        ComputeBudgetLimits(max_tokens=10),
        ComputeBudgetUsage(tokens=1),
    )

    assert plan.remaining_before.tokens == 9
    assert plan.remaining_before.wall_time_ms is None
    assert plan.remaining_after.tokens == 9
    assert plan.remaining_after.wall_time_ms is None


@pytest.mark.parametrize("projection_hash", ["", "   "])
def test_empty_projection_hash_fails_closed(projection_hash: str) -> None:
    plan = VerifiedProgressBudgetPolicy().plan(
        _state(projection_hash=projection_hash),
        {"task": _estimate()},
        ComputeBudgetLimits(max_tokens=10),
        ComputeBudgetUsage(),
    )

    assert plan.selected_task_ids == ()
    assert plan.deferred_task_ids == ("task",)
    assert plan.decisions[0].reason == "projection_hash_unavailable"
    assert any(item.name == "projection_hash" for item in plan.unavailable)
    assert plan.safe_under_declared_budget is False
