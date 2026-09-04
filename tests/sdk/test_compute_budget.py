"""Tests for the deterministic verified-progress compute budget policy."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    AgentCognitionState,
    ComputeBudgetLimits,
    ComputeBudgetUsage,
    FrontierAction,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceRuntimeState,
    TaskComputeEstimate,
    VerifiedProgressBudgetPlan,
    VerifiedProgressBudgetPolicy,
)


def _state(
    *,
    ready: tuple[str, ...] = ("a", "b", "c"),
    repair_ready: tuple[str, ...] = (),
    verified: tuple[str, ...] = (),
    stale: tuple[str, ...] = (),
    invalid: tuple[str, ...] = (),
    graph_closed: bool = False,
    goal_closed: bool = False,
    resources_available: bool = True,
    cognition_available: bool = True,
    active_tasks: tuple[str, ...] = (),
    graph_id: str = "graph",
    progress_graph_id: str = "graph",
) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id=graph_id,
        progress=ProgressSemanticState(
            graph_id=progress_graph_id,
            graph_version=7,
            projection_hash="p" * 64,
            graph_closed=graph_closed,
            goal_closed=goal_closed,
            ready_frontier=ready,
            repair_ready_frontier=repair_ready,
            verified_task_ids=verified,
            stale_task_ids=stale,
            invalid_task_ids=invalid,
            unverified_task_ids=ready,
        ),
        agent_cognition=AgentCognitionState(
            available=cognition_available,
            reason=None if cognition_available else "cognition unavailable",
            current_attempts=tuple(
                {
                    "claim_id": f"claim-{task_id}",
                    "claim_state": "active",
                    "task_id": task_id,
                    "agent_id": "worker",
                    "process_id": "process",
                }
                for task_id in active_tasks
            ),
        ),
        context={
            "available": False,
            "reason": "not bound",
        },
        resources=ResourceRuntimeState(
            available=resources_available,
            reason=None if resources_available else "resources unavailable",
        ),
    )


def _estimate(
    task_id: str,
    *,
    progress: int = 1,
    success: int = 10_000,
    stability: int = 10_000,
    cost: int = 1,
    rework: int = 0,
    tokens: int = 1,
    wall_time_ms: int = 1,
    cost_microusd: int = 1,
    context_tokens: int = 1,
    verification_tokens: int = 1,
    known: bool = True,
) -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=progress,
        success_basis_points=success,
        input_stability_basis_points=stability,
        normalized_cost_units=cost,
        expected_rework_cost_units=rework,
        estimated_tokens=tokens,
        estimated_wall_time_ms=wall_time_ms,
        estimated_cost_microusd=cost_microusd,
        estimated_context_tokens=context_tokens,
        estimated_verification_tokens=verification_tokens,
        known=known,
    )


def _limits(**kwargs: int | None) -> ComputeBudgetLimits:
    return ComputeBudgetLimits(
        max_tokens=kwargs.get("tokens"),
        max_wall_time_ms=kwargs.get("wall_time_ms"),
        max_cost_microusd=kwargs.get("cost_microusd"),
        max_context_tokens=kwargs.get("context_tokens"),
        max_verification_tokens=kwargs.get("verification_tokens"),
    )


def test_exact_ratio_order_is_integer_and_deterministic() -> None:
    state = _state(ready=("slow", "fast", "tie"))
    estimates = {
        "slow": _estimate("slow", progress=2, cost=3),
        "fast": _estimate("fast", progress=3, cost=2),
        "tie": _estimate("tie", progress=1, cost=1),
    }
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        estimates,
        _limits(),
        ComputeBudgetUsage(),
        epoch_id=4,
        max_parallelism=3,
    )
    assert plan.candidate_task_ids == ("fast", "tie", "slow")
    assert plan.selected_task_ids == plan.candidate_task_ids
    assert all(item.utility_numerator is not None for item in plan.decisions)
    assert (
        plan.decision_hash
        == VerifiedProgressBudgetPolicy()
        .plan(
            state,
            dict(reversed(list(estimates.items()))),
            _limits(),
            ComputeBudgetUsage(),
            epoch_id=4,
            max_parallelism=3,
        )
        .decision_hash
    )


def test_repair_priority_cannot_be_crossed_by_utility() -> None:
    state = _state(
        ready=("ordinary", "repair"),
        repair_ready=("repair",),
        stale=("repair",),
    )
    estimates = {
        "ordinary": _estimate("ordinary", progress=100, cost=1),
        "repair": _estimate("repair", progress=1, cost=100),
    }
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        estimates,
        _limits(),
        ComputeBudgetUsage(),
        max_parallelism=1,
    )
    assert plan.selected_task_ids == ("repair",)
    assert plan.decisions[0].tier == "repair"
    assert plan.decisions[0].reason == "selected_repair"


def test_budget_checks_each_dimension_and_audits_before_after() -> None:
    state = _state(ready=("fits", "too-many-tokens", "too-much-time"))
    estimates = {
        "fits": _estimate("fits", tokens=2, wall_time_ms=2, cost_microusd=2),
        "too-many-tokens": _estimate("too-many-tokens", tokens=5),
        "too-much-time": _estimate("too-much-time", wall_time_ms=5),
    }
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        estimates,
        _limits(
            tokens=4, wall_time_ms=4, cost_microusd=10, context_tokens=10, verification_tokens=10
        ),
        ComputeBudgetUsage(tokens=1, wall_time_ms=1, cost_microusd=1),
        max_parallelism=3,
    )
    assert plan.selected_task_ids == ("fits",)
    assert plan.usage_before.tokens == 1
    assert plan.usage_after.tokens == 3
    assert plan.remaining_before.tokens == 3
    assert plan.remaining_after.tokens == 1
    reasons = {item.task_id: item for item in plan.decisions}
    assert reasons["too-many-tokens"].reason == "budget_exceeded"
    assert "tokens" in reasons["too-many-tokens"].budget_blockers
    assert reasons["too-much-time"].reason == "budget_exceeded"
    assert "wall_time_ms" in reasons["too-much-time"].budget_blockers


def test_unknown_or_missing_estimate_fails_closed() -> None:
    state = _state(ready=("known", "unknown", "missing"))
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        {
            "known": _estimate("known"),
            "unknown": _estimate("unknown", known=False),
        },
        _limits(),
        ComputeBudgetUsage(),
        max_parallelism=3,
    )
    assert plan.selected_task_ids == ("known",)
    assert set(plan.deferred_task_ids) == {"unknown", "missing"}
    assert {item.task_id: item.reason for item in plan.decisions}["unknown"] == "estimate_unknown"
    assert {item.task_id: item.reason for item in plan.decisions}["missing"] == "estimate_unknown"
    assert not plan.safe_under_declared_budget
    assert {item.name for item in plan.unavailable} == {
        "estimates.missing",
        "estimates.unknown",
    }


def test_mismatched_mapping_key_and_estimate_task_id_fails_closed() -> None:
    state = _state(ready=("a",))
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        {"a": _estimate("different-task")},
        _limits(tokens=10),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ()
    assert plan.deferred_task_ids == ("a",)
    assert plan.decisions[0].reason == "estimate_unknown"
    assert plan.unavailable[0].name == "estimates.a"
    assert "does not match" in plan.unavailable[0].reason
    assert not plan.safe_under_declared_budget


def test_mapping_payload_task_id_mismatch_fails_closed() -> None:
    state = _state(ready=("a",))
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        {
            "a": {
                **_estimate("different-task").model_dump(),
                "task_id": "different-task",
            }
        },
        _limits(tokens=10),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ()
    assert plan.decisions[0].reason == "estimate_unknown"
    assert plan.unavailable[0].name == "estimates.a"


def test_usage_already_over_limit_is_audited_even_without_candidates() -> None:
    plan = VerifiedProgressBudgetPolicy().plan(
        _state(ready=()),
        {},
        _limits(tokens=10),
        ComputeBudgetUsage(tokens=11),
        max_parallelism=1,
    )
    assert plan.selected_task_ids == ()
    assert plan.remaining_before.tokens == 0
    assert plan.remaining_after.tokens == 0
    assert plan.unavailable[0].name == "usage.tokens"
    assert not plan.safe_under_declared_budget


def test_unknown_estimates_are_audited_even_when_an_earlier_guard_defers() -> None:
    plan = VerifiedProgressBudgetPolicy().plan(
        _state(ready=("missing",), graph_closed=True),
        {},
        _limits(tokens=10),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ()
    assert plan.decisions[0].reason == "closed"
    assert plan.unavailable[0].name == "estimates.missing"
    assert not plan.safe_under_declared_budget


def test_duplicate_iterable_estimates_fail_closed() -> None:
    estimate = _estimate("a")
    plan = VerifiedProgressBudgetPolicy().plan(
        _state(ready=("a",)),
        (estimate, estimate),
        _limits(tokens=10),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ()
    assert plan.decisions[0].reason == "estimate_unknown"
    assert plan.unavailable[0].name == "estimates.a"
    assert "duplicate" in plan.unavailable[0].reason


def test_zero_cost_positive_progress_ranks_before_finite_utility() -> None:
    plan = VerifiedProgressBudgetPolicy().plan(
        _state(ready=("finite", "free", "undefined")),
        {
            "finite": _estimate("finite", progress=100, cost=1),
            "free": _estimate("free", progress=1, cost=0),
            "undefined": _estimate("undefined", progress=0, cost=0),
        },
        _limits(),
        ComputeBudgetUsage(),
        max_parallelism=3,
    )
    assert plan.candidate_task_ids == ("free", "finite", "undefined")


def test_decision_hash_covers_usage_limits_and_estimates() -> None:
    state = _state(ready=("a",))
    estimate = _estimate("a", progress=2, tokens=2)
    base = VerifiedProgressBudgetPolicy().plan(
        state,
        {"a": estimate},
        _limits(tokens=10),
        ComputeBudgetUsage(tokens=1),
    )
    changed_usage = VerifiedProgressBudgetPolicy().plan(
        state,
        {"a": estimate},
        _limits(tokens=10),
        ComputeBudgetUsage(tokens=2),
    )
    changed_limit = VerifiedProgressBudgetPolicy().plan(
        state,
        {"a": estimate},
        _limits(tokens=11),
        ComputeBudgetUsage(tokens=1),
    )
    changed_estimate = VerifiedProgressBudgetPolicy().plan(
        state,
        {"a": _estimate("a", progress=3, tokens=2)},
        _limits(tokens=10),
        ComputeBudgetUsage(tokens=1),
    )
    assert (
        len(
            {
                base.decision_hash,
                changed_usage.decision_hash,
                changed_limit.decision_hash,
                changed_estimate.decision_hash,
            }
        )
        == 4
    )


@pytest.mark.parametrize(
    ("kwargs", "expected_reason"),
    [
        ({"active_tasks": ("a",)}, "active_attempt"),
        ({"verified": ("a",)}, "terminal_validity"),
        ({"invalid": ("a",)}, "terminal_validity"),
        ({"stale": ("a",)}, "stale_not_repair_ready"),
        ({"graph_closed": True}, "closed"),
        ({"goal_closed": True}, "closed"),
        ({"cognition_available": False}, "cognition_unavailable"),
        ({"resources_available": False}, "resources_unavailable"),
    ],
)
def test_active_terminal_stale_and_availability_guards(
    kwargs: dict[str, object],
    expected_reason: str,
) -> None:
    state = _state(**kwargs)
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        {"a": _estimate("a")},
        _limits(),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ()
    assert plan.decisions[0].action is FrontierAction.DEFER
    assert plan.decisions[0].reason == expected_reason


def test_graph_fence_mismatch_fails_closed() -> None:
    plan = VerifiedProgressBudgetPolicy().plan(
        _state(graph_id="graph-a", progress_graph_id="graph-b"),
        {"a": _estimate("a")},
        _limits(),
        ComputeBudgetUsage(),
    )
    assert plan.selected_task_ids == ()
    assert plan.decisions[0].reason == "graph_fence_invalid"
    assert "graph_fence" in {item.name for item in plan.unavailable}


def test_active_task_is_not_selected_even_with_high_utility() -> None:
    state = _state(ready=("a", "b"), active_tasks=("a",))
    plan = VerifiedProgressBudgetPolicy().plan(
        state,
        {
            "a": _estimate("a", progress=100),
            "b": _estimate("b", progress=1),
        },
        _limits(),
        ComputeBudgetUsage(),
        max_parallelism=1,
    )
    assert plan.selected_task_ids == ("b",)
    assert next(item for item in plan.decisions if item.task_id == "a").reason == "active_attempt"


def test_output_is_frozen_and_hash_is_byte_stable() -> None:
    state = _state(ready=("b", "a"))
    policy = VerifiedProgressBudgetPolicy()
    first = policy.plan(
        state,
        {"a": _estimate("a"), "b": _estimate("b")},
        _limits(tokens=10),
        ComputeBudgetUsage(tokens=1),
        epoch_id=9,
        max_parallelism=2,
    )
    second = policy.plan(
        state,
        {"b": _estimate("b"), "a": _estimate("a")},
        _limits(tokens=10),
        ComputeBudgetUsage(tokens=1),
        epoch_id=9,
        max_parallelism=2,
    )
    assert first == second
    assert first.model_dump_json() == second.model_dump_json()
    with pytest.raises(ValidationError):
        first.selected_task_ids += ("x",)  # type: ignore[misc]
    with pytest.raises(ValidationError):
        VerifiedProgressBudgetPlan.model_validate({**first.model_dump(), "extra": True})


def test_strict_ids_and_nonnegative_budget_values() -> None:
    with pytest.raises(ValidationError):
        ComputeBudgetLimits(max_tokens=True)
    with pytest.raises(ValidationError):
        ComputeBudgetUsage(tokens=-1)
    with pytest.raises(ValueError):
        VerifiedProgressBudgetPolicy().plan(
            _state(ready=()),
            {},
            _limits(),
            ComputeBudgetUsage(),
            epoch_id=-1,
        )
    with pytest.raises(ValueError):
        VerifiedProgressBudgetPolicy().plan(
            _state(ready=()),
            {},
            _limits(),
            ComputeBudgetUsage(),
            max_parallelism=0,
        )
