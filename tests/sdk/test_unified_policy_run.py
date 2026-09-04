"""Execution-path coverage for the unified adaptive policy.

The unified path is opt-in and graph-relative: it combines the declared
compute budget with logical resources and conflict/access guards, then passes
only the selected task ids to the authoritative Scheduler.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from lhos.sdk import (
    Agent,
    AgentOS,
    ComputeBudgetLimits,
    ComputeBudgetUsage,
    ConfigurationError,
    ConflictGraph,
    Goal,
    TaskAccessSet,
    TaskComputeEstimate,
    VerificationOutcome,
)


def _pass(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=task_id,
        version=1,
        content=f"{task_id}-v1",
    )


def _estimate(task_id: str, *, progress: int = 1, tokens: int = 1) -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=progress,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=1,
        estimated_tokens=tokens,
        estimated_wall_time_ms=1,
        estimated_cost_microusd=1,
        estimated_context_tokens=1,
        estimated_verification_tokens=1,
        known=True,
    )


def _graph(*task_ids: str, shared: tuple[str, ...] = ()) -> ConflictGraph:
    shared_set = set(shared)
    return ConflictGraph.from_access_sets(
        TaskAccessSet(
            task_id=task_id,
            write_set=("workspace://shared",) if task_id in shared_set else (f"out://{task_id}",),
        )
        for task_id in task_ids
    )


def _kwargs(
    estimates: dict[str, TaskComputeEstimate],
    *,
    max_tokens: int = 100,
    usage: ComputeBudgetUsage | None = None,
    max_parallelism: int = 2,
    conflict_graph: ConflictGraph | None = None,
) -> dict[str, object]:
    return {
        "adaptive": True,
        "budget_aware": True,
        "resource_aware": True,
        "budget_estimates": estimates,
        "budget_limits": ComputeBudgetLimits(max_tokens=max_tokens),
        "budget_usage": usage,
        "automatic_rebase": False,
        "max_parallelism": max_parallelism,
        "conflict_graph": conflict_graph,
    }


def test_sync_unified_control_combines_budget_resource_and_conflict_guards() -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                max_concurrency=3,
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("unified-sync")
        goal.task(
            "high",
            agent="worker",
            resources={"cpu_millis": 600},
            verify=lambda: _pass("high"),
        )
        goal.task(
            "conflict",
            agent="worker",
            resources={"cpu_millis": 600},
            verify=lambda: _pass("conflict"),
        )
        goal.task(
            "small",
            agent="worker",
            resources={"cpu_millis": 400},
            verify=lambda: _pass("small"),
        )
        graph = _graph("high", "conflict", "small", shared=("high", "conflict"))

        result = runtime.run(
            goal,
            max_dispatches=3,
            max_steps=4,
            **_kwargs(
                {
                    "high": _estimate("high", progress=10, tokens=3),
                    "conflict": _estimate("conflict", progress=9, tokens=3),
                    "small": _estimate("small", progress=1, tokens=1),
                },
                max_tokens=7,
                conflict_graph=graph,
            ),
        )

        assert result.goal_state == "closed"
        assert set(result.verified) == {"high", "conflict", "small"}
        assert result.meta["adaptive_policy"] == "unified-adaptive-control"
        assert result.meta["budget_aware"] is True
        assert result.meta["resource_aware"] is True
        assert result.meta["unified_control"] is True
        assert result.meta["budget_usage"]["tokens"] == 7
        first = result.meta["adaptive_epochs"][0]
        assert first["fallback_attempted"] is False
        assert first["resource_audit"]["source_schema_version"] == "unified-adaptive-plan.v1"
        assert first["unified_audit"]["schema_version"] == "unified-adaptive-run-audit.v1"
        assert first["unified_audit"]["safe_under_constraints"] is True
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_async_unified_control_respects_capacity_and_accumulates_usage() -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)
        await asyncio.sleep(0)

    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
                max_concurrency=2,
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("unified-async")
        for task_id in ("left", "right"):
            goal.task(
                task_id,
                agent="worker",
                resources={"cpu_millis": 700},
                verify=lambda task_id=task_id: _pass(task_id),
            )

        result = await runtime.run_async(
            goal,
            max_dispatches=2,
            max_steps=4,
            max_concurrency=2,
            **_kwargs(
                {"left": _estimate("left", tokens=2), "right": _estimate("right", tokens=2)},
                max_tokens=4,
                max_parallelism=2,
                conflict_graph=_graph("left", "right"),
            ),
        )

        assert result.goal_state == "closed"
        assert set(executed) == {"left", "right"}
        assert result.meta["adaptive_policy"] == "unified-adaptive-control"
        assert result.meta["budget_usage"]["tokens"] == 4
        assert all(
            len(epoch["selected_task_ids"]) <= 1
            for epoch in result.meta["adaptive_epochs"]
            if epoch["selected_task_ids"]
        )
        assert [
            (
                epoch["budget_audit"]["usage_before"]["tokens"],
                epoch["budget_audit"]["dispatched_declared_usage_after"]["tokens"],
            )
            for epoch in result.meta["adaptive_epochs"]
        ] == [(0, 2), (2, 4)]
    finally:
        runtime.close()


def test_unified_omitted_conflict_graph_is_conservatively_derived() -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                max_concurrency=2,
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("unified-derived-conflict")
        for task_id in ("a", "b"):
            goal.task(
                task_id,
                agent="worker",
                resources={"cpu_millis": 500},
                outputs=("workspace://same-output",),
                verify=lambda task_id=task_id: _pass(task_id),
            )

        result = runtime.run(
            goal,
            max_dispatches=2,
            max_steps=4,
            **_kwargs(
                {"a": _estimate("a", progress=2), "b": _estimate("b", progress=1)},
                max_tokens=2,
                max_parallelism=2,
            ),
        )

        assert result.goal_state == "closed"
        nonempty = [e for e in result.meta["adaptive_epochs"] if e["selected_task_ids"]]
        assert nonempty
        assert len(nonempty[0]["selected_task_ids"]) == 1
        assert nonempty[0]["unified_audit"]["conflict_graph_hash"]
        assert all(e["fallback_attempted"] is False for e in result.meta["adaptive_epochs"])
    finally:
        runtime.close()


def test_unified_runtime_backfills_after_budget_resource_and_conflict_rejections() -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                max_concurrency=3,
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("unified-backfill")
        task_specs = {
            "budget-blocked": 100,
            "resource-blocked": 1_100,
            "anchor": 600,
            "conflict": 300,
            "good": 400,
        }
        for task_id, cpu_millis in task_specs.items():
            goal.task(
                task_id,
                agent="worker",
                resources={"cpu_millis": cpu_millis},
                verify=lambda task_id=task_id: _pass(task_id),
            )
        graph = _graph(
            *task_specs,
            shared=("anchor", "conflict"),
        )

        result = runtime.run(
            goal,
            max_dispatches=3,
            max_steps=1,
            **_kwargs(
                {
                    "budget-blocked": _estimate(
                        "budget-blocked",
                        progress=100,
                        tokens=6,
                    ),
                    "resource-blocked": _estimate(
                        "resource-blocked",
                        progress=90,
                    ),
                    "anchor": _estimate("anchor", progress=80),
                    "conflict": _estimate("conflict", progress=70),
                    "good": _estimate("good", progress=1),
                },
                max_tokens=4,
                max_parallelism=3,
                conflict_graph=graph,
            ),
        )

        epoch = result.meta["adaptive_epochs"][0]
        assert epoch["selected_task_ids"] == ("anchor", "good")
        assert epoch["actual_dispatched_task_ids"] == ("anchor", "good")
        assert result.meta["budget_usage"]["tokens"] == 2
        decisions = {item["task_id"]: item for item in epoch["resource_audit"]["decisions"]}
        assert decisions["budget-blocked"]["reason"] == "budget_exceeded"
        assert decisions["resource-blocked"]["reason"] == "insufficient_resources"
        assert decisions["conflict"]["reason"] == "conflict"
        assert decisions["conflict"]["blockers"] == ("anchor",)
        assert epoch["fallback_attempted"] is False
    finally:
        runtime.close()


def test_unified_partial_admission_charges_only_dispatched_tasks_without_fallback() -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                max_concurrency=2,
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("unified-partial-admission")
        goal.task(
            "ineligible",
            agent="worker",
            required_specializations=("gpu",),
            resources={"cpu_millis": 100},
            verify=lambda: _pass("ineligible"),
        )
        goal.task(
            "eligible",
            agent="worker",
            required_specializations=("python",),
            resources={"cpu_millis": 100},
            verify=lambda: _pass("eligible"),
        )

        result = runtime.run(
            goal,
            max_dispatches=2,
            max_steps=1,
            **_kwargs(
                {
                    "ineligible": _estimate("ineligible", progress=10, tokens=9),
                    "eligible": _estimate("eligible", progress=1, tokens=2),
                },
                max_tokens=20,
                max_parallelism=2,
                conflict_graph=_graph("ineligible", "eligible"),
            ),
        )

        assert result.verified == ["eligible"]
        assert result.meta["budget_usage"]["tokens"] == 2
        epoch = result.meta["adaptive_epochs"][0]
        assert set(epoch["selected_task_ids"]) == {"ineligible", "eligible"}
        assert epoch["actual_dispatched_task_ids"] == ("eligible",)
        assert epoch["fallback_attempted"] is False
    finally:
        runtime.close()


def test_unified_graph_race_does_not_charge_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("unified-graph-race")
        goal.task(
            "task",
            agent="worker",
            resources={"cpu_millis": 100},
            verify=lambda: _pass("task"),
        )

        def stale_run_pass(*_args: object, **_kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(
                dispatched=[],
                skipped=(("task", "graph version changed"),),
                policy_stale=True,
                policy_stale_reason="graph version changed",
                observed_graph_version=99,
                policy_cleanup_required=False,
                policy_cleanup_errors=(),
            )

        monkeypatch.setattr(runtime.scheduler, "run_pass", stale_run_pass)
        result = runtime.run(
            goal,
            max_dispatches=1,
            max_steps=1,
            **_kwargs(
                {"task": _estimate("task", tokens=8)},
                max_tokens=8,
                max_parallelism=1,
                conflict_graph=_graph("task"),
            ),
        )

        assert result.verified == []
        assert result.meta["budget_usage"]["tokens"] == 0
        epoch = result.meta["adaptive_epochs"][0]
        assert epoch["policy_stale"] is True
        assert epoch["budget_audit"]["planned_usage_after"]["tokens"] == 8
        assert epoch["budget_audit"]["dispatched_declared_usage_after"]["tokens"] == 0
    finally:
        runtime.close()


@pytest.mark.parametrize("runner", ["sync", "async"])
def test_budget_only_explicit_conflict_graph_fails_closed(runner: str) -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal(f"unified-invalid-budget-only-{runner}")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))
        kwargs = {
            "adaptive": True,
            "budget_aware": True,
            "budget_estimates": {"task": _estimate("task")},
            "budget_limits": ComputeBudgetLimits(max_tokens=1),
            "automatic_rebase": False,
            "conflict_graph": _graph("task"),
        }
        with pytest.raises(ConfigurationError, match="resource_aware=True"):
            if runner == "sync":
                runtime.run(goal, **kwargs)
            else:
                asyncio.run(runtime.run_async(goal, **kwargs))
    finally:
        runtime.close()


def test_budget_only_resource_only_and_default_execution_remain_distinct() -> None:
    budget_runtime = AgentOS(":memory:")
    try:
        budget_runtime.add_agent(Agent("worker", specializations=("python",)))
        budget_goal = Goal("unified-regression-budget-only")
        budget_goal.task("task", agent="worker", verify=lambda: _pass("task"))
        budget_result = budget_runtime.run(
            budget_goal,
            max_dispatches=1,
            adaptive=True,
            budget_aware=True,
            budget_estimates={"task": _estimate("task")},
            budget_limits=ComputeBudgetLimits(max_tokens=1),
            automatic_rebase=False,
        )
        assert budget_result.meta["adaptive_policy"] == "verified-progress-budget"
        assert budget_result.meta["resource_aware"] is False
        assert budget_result.meta["unified_control"] is False
    finally:
        budget_runtime.close()

    resource_runtime = AgentOS(":memory:")
    try:
        resource_runtime.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        resource_goal = Goal("unified-regression-resource-only")
        resource_goal.task(
            "task",
            agent="worker",
            resources={"cpu_millis": 100},
            outputs=("out://task",),
            verify=lambda: _pass("task"),
        )
        resource_result = resource_runtime.run(
            resource_goal,
            max_dispatches=1,
            adaptive=True,
            resource_aware=True,
        )
        assert resource_result.meta["adaptive_policy"] == "resource-aware-conflict"
        assert resource_result.meta["resource_aware"] is True
        assert resource_result.meta["unified_control"] is False
        assert "budget_aware" not in resource_result.meta
    finally:
        resource_runtime.close()

    default_runtime = AgentOS(":memory:")
    try:
        default_runtime.add_agent(Agent("worker", specializations=("python",)))
        default_goal = Goal("unified-regression-default")
        default_goal.task("task", agent="worker", verify=lambda: _pass("task"))
        default_result = default_runtime.run(default_goal, max_dispatches=1)
        assert default_result.goal_state == "closed"
        assert "adaptive" not in default_result.meta
        assert "unified_control" not in default_result.meta
    finally:
        default_runtime.close()


def test_unified_control_still_rejects_automatic_rebase() -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("unified-invalid-automatic-rebase")
        goal.task(
            "task",
            agent="worker",
            resources={"cpu_millis": 100},
            outputs=("out://task",),
            verify=lambda: _pass("task"),
        )

        with pytest.raises(ConfigurationError, match="automatic_rebase"):
            runtime.run(
                goal,
                adaptive=True,
                budget_aware=True,
                resource_aware=True,
                budget_estimates={"task": _estimate("task")},
                budget_limits=ComputeBudgetLimits(max_tokens=1),
            )
    finally:
        runtime.close()
