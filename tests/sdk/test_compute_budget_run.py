"""Execution-path tests for opt-in compute-budget admission.

The budget runtime intentionally charges declared estimates at Scheduler
dispatch time.  These tests do not claim provider-measured billing accuracy;
they verify the bounded v1 contract and its fail-closed fences.
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


def _estimate(
    task_id: str,
    *,
    progress: int = 1,
    cost: int = 1,
    tokens: int = 1,
    wall_time_ms: int = 1,
    cost_microusd: int = 1,
    context_tokens: int = 1,
    verification_tokens: int = 1,
) -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=progress,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=cost,
        estimated_tokens=tokens,
        estimated_wall_time_ms=wall_time_ms,
        estimated_cost_microusd=cost_microusd,
        estimated_context_tokens=context_tokens,
        estimated_verification_tokens=verification_tokens,
        known=True,
    )


def _budget_kwargs(
    estimates: dict[str, TaskComputeEstimate],
    limits: ComputeBudgetLimits,
    *,
    usage: ComputeBudgetUsage | None = None,
    max_parallelism: int = 1,
) -> dict[str, object]:
    return {
        "adaptive": True,
        "budget_aware": True,
        "budget_estimates": estimates,
        "budget_limits": limits,
        "budget_usage": usage,
        "automatic_rebase": False,
        "max_parallelism": max_parallelism,
    }


def test_sync_budget_aware_run_charges_declared_usage_and_audits_epoch() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("budget-run-sync")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))
        estimate = _estimate(
            "task",
            tokens=3,
            wall_time_ms=5,
            cost_microusd=7,
            context_tokens=11,
            verification_tokens=13,
        )

        result = os_.run(
            goal,
            max_dispatches=1,
            **_budget_kwargs(
                {"task": estimate},
                ComputeBudgetLimits(
                    max_tokens=3,
                    max_wall_time_ms=5,
                    max_cost_microusd=7,
                    max_context_tokens=11,
                    max_verification_tokens=13,
                ),
            ),
        )

        assert result.goal_state == "closed"
        assert result.verified == ["task"]
        assert result.meta["adaptive_policy"] == "verified-progress-budget"
        assert result.meta["budget_aware"] is True
        assert result.meta["budget_usage"] == {
            "tokens": 3,
            "wall_time_ms": 5,
            "cost_microusd": 7,
            "context_tokens": 11,
            "verification_tokens": 13,
        }
        audit = result.meta["adaptive_epochs"][0]["budget_audit"]
        assert audit["selected_task_ids"] == ("task",)
        assert audit["actual_dispatched_task_ids"] == ("task",)
        assert audit["usage_before"]["tokens"] == 0
        assert audit["planned_usage_after"]["tokens"] == 3
        assert audit["dispatched_declared_usage_after"]["tokens"] == 3
    finally:
        os_.close()


async def test_async_budget_aware_run_charges_declared_usage_and_audits_epoch() -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)
        await asyncio.sleep(0)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
            )
        )
        goal = Goal("budget-run-async")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))

        result = await os_.run_async(
            goal,
            max_dispatches=1,
            max_concurrency=1,
            **_budget_kwargs(
                {"task": _estimate("task", tokens=4)},
                ComputeBudgetLimits(max_tokens=4),
            ),
        )

        assert result.goal_state == "closed"
        assert executed == ["task"]
        assert result.meta["adaptive_policy"] == "verified-progress-budget"
        assert result.meta["budget_usage"]["tokens"] == 4
        audit = result.meta["adaptive_epochs"][0]["budget_audit"]
        assert audit["actual_dispatched_task_ids"] == ("task",)
        assert audit["dispatched_declared_usage_after"]["tokens"] == 4
    finally:
        os_.close()


def test_budget_usage_accumulates_across_scheduling_epochs() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("budget-run-cross-epoch")
        first = goal.task("first", agent="worker", verify=lambda: _pass("first"))
        goal.task(
            "second",
            agent="worker",
            depends_on=(first,),
            verify=lambda: _pass("second"),
        )

        result = os_.run(
            goal,
            max_dispatches=2,
            max_steps=3,
            **_budget_kwargs(
                {
                    "first": _estimate("first", tokens=3),
                    "second": _estimate("second", tokens=3),
                },
                ComputeBudgetLimits(max_tokens=7),
                usage=ComputeBudgetUsage(tokens=1),
            ),
        )

        assert result.goal_state == "closed"
        assert result.meta["budget_usage"]["tokens"] == 7
        epochs = result.meta["adaptive_epochs"]
        assert len(epochs) == 2
        assert epochs[0]["budget_audit"]["usage_before"]["tokens"] == 1
        assert epochs[0]["budget_audit"]["dispatched_declared_usage_after"]["tokens"] == 4
        assert epochs[1]["budget_audit"]["usage_before"]["tokens"] == 4
        assert epochs[1]["budget_audit"]["dispatched_declared_usage_after"]["tokens"] == 7
    finally:
        os_.close()


async def test_failed_attempt_still_consumes_dispatched_budget() -> None:
    calls = 0

    async def execute(_task_id: str) -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("budget-run-failed-attempt")
        goal.task(
            "task",
            agent="worker",
            verify=lambda: VerificationOutcome(
                passed=False,
                artifact_id="task",
                version=1,
            ),
        )

        result = await os_.run_async(
            goal,
            max_dispatches=1,
            max_steps=1,
            max_concurrency=1,
            **_budget_kwargs(
                {"task": _estimate("task", tokens=6)},
                ComputeBudgetLimits(max_tokens=6),
            ),
        )

        assert calls == 1
        assert result.goal_state == "open"
        assert any("verification_failed" in item for item in result.failures)
        assert result.meta["budget_usage"]["tokens"] == 6
        assert result.meta["adaptive_epochs"][0]["budget_audit"]["actual_dispatched_task_ids"] == (
            "task",
        )
    finally:
        os_.close()


def test_missing_estimate_fails_closed_without_invoking_scheduler(monkeypatch) -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("budget-run-missing-estimate")
        goal.task("missing", agent="worker", verify=lambda: _pass("missing"))

        def unexpected_run_pass(*_args, **_kwargs):
            raise AssertionError("Scheduler must not run without a known estimate")

        monkeypatch.setattr(os_.scheduler, "run_pass", unexpected_run_pass)
        result = os_.run(
            goal,
            max_dispatches=1,
            **_budget_kwargs({}, ComputeBudgetLimits(max_tokens=10)),
        )

        assert result.goal_state == "open"
        assert result.verified == []
        assert result.meta["budget_usage"]["tokens"] == 0
        epoch = result.meta["adaptive_epochs"][0]
        assert epoch["selected_task_ids"] == ()
        assert epoch["deferred_task_ids"] == ("missing",)
        assert epoch["budget_audit"]["actual_dispatched_task_ids"] == ()
    finally:
        os_.close()


def test_budget_exhaustion_stops_later_work_without_unfiltered_fallback(
    monkeypatch,
) -> None:
    executed: list[str] = []
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("budget-run-exhausted")
        goal.task(
            "first",
            agent="worker",
            verify=lambda: (executed.append("first"), _pass("first"))[1],
        )
        goal.task(
            "second",
            agent="worker",
            verify=lambda: (executed.append("second"), _pass("second"))[1],
        )
        original_run_pass = os_.scheduler.run_pass
        allowed_filters: list[object] = []

        def audited_run_pass(*args, **kwargs):
            allowed_filters.append(kwargs.get("allowed_task_ids"))
            return original_run_pass(*args, **kwargs)

        monkeypatch.setattr(os_.scheduler, "run_pass", audited_run_pass)
        result = os_.run(
            goal,
            max_dispatches=2,
            max_steps=3,
            **_budget_kwargs(
                {
                    "first": _estimate("first", tokens=5),
                    "second": _estimate("second", tokens=5),
                },
                ComputeBudgetLimits(max_tokens=5),
            ),
        )

        assert executed == ["first"]
        assert result.meta["budget_usage"]["tokens"] == 5
        assert allowed_filters == [("first",)]
        assert all(epoch["fallback_attempted"] is False for epoch in result.meta["adaptive_epochs"])
        assert result.task_states["second"] == "unverified"
    finally:
        os_.close()


def test_budget_path_does_not_fallback_when_selected_task_is_ineligible() -> None:
    executed: list[str] = []
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("budget-run-no-fallback")
        goal.task(
            "a-ineligible",
            agent="worker",
            required_specializations=("gpu",),
            verify=lambda: _pass("a-ineligible"),
        )
        goal.task(
            "b-eligible",
            agent="worker",
            required_specializations=("python",),
            verify=lambda: (executed.append("b-eligible"), _pass("b-eligible"))[1],
        )

        result = os_.run(
            goal,
            max_dispatches=1,
            max_steps=1,
            **_budget_kwargs(
                {
                    "a-ineligible": _estimate("a-ineligible", progress=10),
                    "b-eligible": _estimate("b-eligible", progress=1),
                },
                ComputeBudgetLimits(max_tokens=10),
            ),
        )

        assert executed == []
        assert result.verified == []
        assert result.meta["budget_usage"]["tokens"] == 0
        epoch = result.meta["adaptive_epochs"][0]
        assert epoch["selected_task_ids"] == ("a-ineligible",)
        assert epoch["fallback_attempted"] is False
        assert epoch["actual_dispatched_task_ids"] == ()
    finally:
        os_.close()


async def test_partial_scheduler_admission_charges_only_created_jobs() -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)
        await asyncio.sleep(0)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
                max_concurrency=2,
            )
        )
        goal = Goal("budget-run-partial-admission")
        goal.task(
            "a-ineligible",
            agent="worker",
            required_specializations=("gpu",),
            verify=lambda: _pass("a-ineligible"),
        )
        goal.task(
            "b-eligible",
            agent="worker",
            required_specializations=("python",),
            verify=lambda: _pass("b-eligible"),
        )

        result = await os_.run_async(
            goal,
            max_dispatches=2,
            max_steps=1,
            max_concurrency=2,
            **_budget_kwargs(
                {
                    "a-ineligible": _estimate("a-ineligible", tokens=9),
                    "b-eligible": _estimate("b-eligible", tokens=2),
                },
                ComputeBudgetLimits(max_tokens=20),
                max_parallelism=2,
            ),
        )

        assert executed == ["b-eligible"]
        assert result.meta["budget_usage"]["tokens"] == 2
        epoch = result.meta["adaptive_epochs"][0]
        assert set(epoch["selected_task_ids"]) == {"a-ineligible", "b-eligible"}
        assert epoch["actual_dispatched_task_ids"] == ("b-eligible",)
        assert epoch["budget_audit"]["planned_usage_after"]["tokens"] == 11
        assert epoch["budget_audit"]["dispatched_declared_usage_after"]["tokens"] == 2
        assert any(
            task_id == "a-ineligible" and "no eligible agent" in reason
            for task_id, reason in epoch["scheduler_skipped"]
        )
    finally:
        os_.close()


def test_graph_race_without_dispatch_does_not_charge_budget(monkeypatch) -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("budget-run-graph-race")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))

        def stale_run_pass(*_args, **_kwargs):
            return SimpleNamespace(
                dispatched=[],
                skipped=(("task", "graph version changed"),),
                policy_stale=True,
                policy_stale_reason="graph version changed",
                observed_graph_version=999,
                policy_cleanup_required=False,
                policy_cleanup_errors=(),
            )

        monkeypatch.setattr(os_.scheduler, "run_pass", stale_run_pass)
        result = os_.run(
            goal,
            max_dispatches=1,
            max_steps=1,
            **_budget_kwargs(
                {"task": _estimate("task", tokens=8)},
                ComputeBudgetLimits(max_tokens=8),
            ),
        )

        assert result.goal_state == "open"
        assert result.meta["budget_usage"]["tokens"] == 0
        epoch = result.meta["adaptive_epochs"][0]
        assert epoch["policy_stale"] is True
        assert epoch["budget_audit"]["planned_usage_after"]["tokens"] == 8
        assert epoch["budget_audit"]["dispatched_declared_usage_after"]["tokens"] == 0
        assert epoch["budget_audit"]["actual_dispatched_task_ids"] == ()
    finally:
        os_.close()


def test_budget_execution_rejects_unsafe_or_ambiguous_configuration() -> None:
    estimate = {"task": _estimate("task")}
    limits = ComputeBudgetLimits(max_tokens=1)

    def new_goal(goal_id: str) -> tuple[AgentOS, Goal]:
        os_ = AgentOS(":memory:")
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal(goal_id)
        goal.task("task", agent="worker", verify=lambda: _pass("task"))
        return os_, goal

    os_, goal = new_goal("budget-invalid-adaptive")
    try:
        with pytest.raises(ConfigurationError, match="requires adaptive=True"):
            os_.run(
                goal,
                budget_aware=True,
                budget_estimates=estimate,
                budget_limits=limits,
                automatic_rebase=False,
            )
    finally:
        os_.close()

    os_, goal = new_goal("budget-invalid-rebase")
    try:
        with pytest.raises(ConfigurationError, match="automatic_rebase"):
            os_.run(
                goal,
                adaptive=True,
                budget_aware=True,
                budget_estimates=estimate,
                budget_limits=limits,
            )
    finally:
        os_.close()

    os_, goal = new_goal("budget-invalid-resource")
    try:
        # Composing budget with logical-resource admission is legal, but only
        # through the explicit unified mode.  Budget-only execution stays
        # isolated from a caller-supplied conflict graph so a partial
        # composition cannot silently bypass the conflict/resource guards.
        with pytest.raises(ConfigurationError, match="requires resource_aware=True"):
            os_.run(
                goal,
                adaptive=True,
                resource_aware=False,
                budget_aware=True,
                budget_estimates=estimate,
                budget_limits=limits,
                conflict_graph=ConflictGraph.from_access_sets(
                    (TaskAccessSet(task_id="task", read_set=("artifact://task",)),)
                ),
                automatic_rebase=False,
            )
    finally:
        os_.close()

    os_, goal = new_goal("budget-invalid-conflict")
    try:
        with pytest.raises(ConfigurationError, match="conflict_graph"):
            os_.run(
                goal,
                adaptive=True,
                conflict_graph=ConflictGraph.from_access_sets(()),
                budget_aware=True,
                budget_estimates=estimate,
                budget_limits=limits,
                automatic_rebase=False,
            )
    finally:
        os_.close()

    os_, goal = new_goal("budget-invalid-supplied-without-opt-in")
    try:
        with pytest.raises(ConfigurationError, match="require budget_aware=True"):
            os_.run(
                goal,
                budget_estimates=estimate,
                budget_limits=limits,
            )
    finally:
        os_.close()


async def test_default_sync_and_async_paths_do_not_emit_budget_metadata() -> None:
    sync_os = AgentOS(":memory:")
    try:
        sync_os.add_agent(Agent("worker", specializations=("python",)))
        sync_goal = Goal("budget-default-sync")
        sync_goal.task("task", agent="worker", verify=lambda: _pass("task"))
        sync_result = sync_os.run(sync_goal, max_dispatches=1)
        assert "budget_aware" not in sync_result.meta
        assert "budget_limits" not in sync_result.meta
        assert "budget_usage" not in sync_result.meta
    finally:
        sync_os.close()

    async def execute(_task_id: str) -> None:
        await asyncio.sleep(0)

    async_os = AgentOS(":memory:")
    try:
        async_os.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        async_goal = Goal("budget-default-async")
        async_goal.task("task", agent="worker", verify=lambda: _pass("task"))
        async_result = await async_os.run_async(
            async_goal,
            max_dispatches=1,
            max_concurrency=1,
        )
        assert async_result.meta == {
            "execution_mode": "async",
            "dispatched": 1,
            "max_concurrency": 1,
        }
    finally:
        async_os.close()
