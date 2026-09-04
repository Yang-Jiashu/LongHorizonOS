"""AgentOS facade tests for the read-only compute-budget policy."""

from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent.events import SchedulerEventType
from lhos.sdk import AgentOS, ConfigurationError, Goal
from lhos.sdk.compute_budget import (
    ComputeBudgetLimits,
    ComputeBudgetUsage,
    TaskComputeEstimate,
    VerifiedProgressBudgetPolicy,
)


def _compiled_goal(os_: AgentOS, goal_id: str = "budget-facade-goal") -> Goal:
    goal = Goal(goal_id)
    goal.task("task-a", agent="")
    goal.task("task-b", agent="")
    os_._compile_goal(goal)
    return goal


def _estimate(task_id: str, *, cost: int = 1) -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=1,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=cost,
        estimated_tokens=cost,
        estimated_wall_time_ms=cost,
        estimated_cost_microusd=cost,
        estimated_context_tokens=cost,
        estimated_verification_tokens=cost,
        known=True,
    )


def _fingerprint(os_: AgentOS) -> tuple[object, ...]:
    graph_ids = tuple(sorted(os_._goal_gid.values()))
    return (
        tuple(
            event.model_dump_json()
            for graph_id in graph_ids
            for event in os_.vpg.store.get_events(graph_id)
        ),
        tuple(claim.model_dump_json() for claim in os_.scheduler.claims),
        tuple(attempt.model_dump_json() for attempt in os_.scheduler.attempts),
        tuple(event.model_dump_json() for event in os_.scheduler.events),
        tuple(lease.model_dump_json() for lease in os_._kernel._lease_service.list_all_leases()),
    )


def test_budgeted_frontier_matches_pure_policy_and_is_read_only() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_)
        estimates = {
            "task-a": _estimate("task-a", cost=1),
            "task-b": _estimate("task-b", cost=2),
        }
        limits = ComputeBudgetLimits(max_tokens=10)
        usage = ComputeBudgetUsage(tokens=1)
        before = _fingerprint(os_)

        facade_plan = os_.plan_budgeted_frontier(
            goal,
            estimates,
            limits,
            usage,
            epoch_id=4,
            max_parallelism=1,
        )
        expected = VerifiedProgressBudgetPolicy().plan(
            os_.runtime_state(goal),
            estimates,
            limits,
            usage,
            epoch_id=4,
            max_parallelism=1,
        )

        assert facade_plan == expected
        assert facade_plan.selected_task_ids == ("task-a",)
        assert _fingerprint(os_) == before
    finally:
        os_.close()


def test_budgeted_frontier_rejects_uncompiled_goal_without_claim_or_lease() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("not-compiled-budget-goal")
        before = _fingerprint(os_)
        with pytest.raises(ConfigurationError, match="not compiled"):
            os_.plan_budgeted_frontier(
                goal,
                {"task-a": _estimate("task-a")},
                ComputeBudgetLimits(max_tokens=10),
            )
        assert goal.goal_id not in os_._goal_gid
        assert _fingerprint(os_) == before
    finally:
        os_.close()


def test_budgeted_frontier_persist_writes_one_bounded_epoch() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "budget-persist-goal")
        plan = os_.plan_budgeted_frontier(
            goal,
            {"task-a": _estimate("task-a"), "task-b": _estimate("task-b")},
            ComputeBudgetLimits(max_tokens=10),
            persist=True,
            epoch_id=7,
            max_parallelism=2,
        )
        events = [
            event
            for event in os_.scheduler.events
            if event.event_type is SchedulerEventType.SCHEDULING_EPOCH_PLANNED
        ]
        assert len(events) == 1
        event = events[0]
        assert event.graph_id == plan.graph_id
        assert event.graph_version == plan.graph_version
        assert event.decision_hash == plan.decision_hash
        assert event.metadata["schema_version"] == "scheduling-epoch.v1"
        assert event.metadata["policy_id"] == plan.policy_id
        assert event.metadata["candidate_task_ids"] == sorted(plan.candidate_task_ids)
        assert event.metadata["selected_task_ids"] == sorted(plan.selected_task_ids)
        assert "limits" not in event.metadata
        assert "usage_before" not in event.metadata
        assert "decisions" not in event.metadata
    finally:
        os_.close()


def test_budgeted_frontier_persist_is_refused_in_read_only_mode() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "budget-read-only-goal")
        os_._read_only = True
        before_events = tuple(os_.scheduler.events)
        with pytest.raises(
            ConfigurationError,
            match="read-only AgentOS cannot persist SchedulingEpoch audits",
        ):
            os_.plan_budgeted_frontier(
                goal,
                {"task-a": _estimate("task-a")},
                ComputeBudgetLimits(max_tokens=10),
                persist=True,
            )
        assert tuple(os_.scheduler.events) == before_events
    finally:
        os_.close()


@pytest.mark.parametrize(
    ("kwargs", "error", "match"),
    [
        ({"limits": {}}, TypeError, "limits must be ComputeBudgetLimits"),
        ({"usage": {}}, TypeError, "usage must be ComputeBudgetUsage"),
        ({"epoch_id": True}, ValueError, "epoch_id"),
        ({"max_parallelism": 0}, ValueError, "max_parallelism"),
    ],
)
def test_budgeted_frontier_rejects_invalid_policy_inputs(
    kwargs: dict[str, object],
    error: type[Exception],
    match: str,
) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "budget-invalid-input-goal")
        params: dict[str, object] = {
            "estimates": {"task-a": _estimate("task-a")},
            "limits": ComputeBudgetLimits(max_tokens=10),
        }
        params.update(kwargs)
        with pytest.raises(error, match=match):
            os_.plan_budgeted_frontier(goal, **params)  # type: ignore[arg-type]
    finally:
        os_.close()
