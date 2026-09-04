"""End-to-end tests for one bounded online execution epoch."""

from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent import AttemptState, ResourceVector
from lhos.runtimes.multi_agent.lease_adapter import claim_resource_uri
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    ConflictGraph,
    Goal,
    TaskAccessSet,
    VerificationOutcome,
)


def _pass(artifact_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=1,
        content=f"{artifact_id}-v1",
    )


@pytest.mark.asyncio
async def test_execute_online_epoch_runs_scheduler_executor_verifier_and_vpg() -> None:
    executed: list[str] = []
    verified: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)

    def verify() -> VerificationOutcome:
        verified.append("task")
        return _pass("artifact")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("online-execute")
        goal.task("task", agent="worker", verify=verify)

        result = await os_.execute_online_epoch(goal)

        assert result.goal_state == "closed"
        assert result.verified == ["task"]
        assert executed == ["task"]
        assert verified == ["task"]
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        assert result.meta["online_epoch"]["phases"] == (
            "observe",
            "reconcile",
            "plan",
            "admit",
            "execute",
            "verify",
            "commit",
            "observe",
        )
        assert result.meta["online_epoch"]["result_kind"] == "RunResult"
        assert result.meta["online_epoch"]["outcome"] == "completed"
        assert result.meta["online_epoch"]["selected_task_ids"] == ("task",)
        assert result.meta["online_epoch"]["policy_selected_task_ids"] == ("task",)
        assert result.meta["online_epoch"]["actual_dispatched_task_ids"] == ("task",)
        assert result.meta["online_epoch"]["fallback_attempted"] is False
        assert result.meta["online_epoch"]["fallback_dispatched_task_ids"] == ()
        assert result.meta["online_epoch"]["planned_graph_id"] == gid
        assert (
            result.meta["online_epoch"]["final_graph_version"]
            >= result.meta["online_epoch"]["planned_graph_version"]
        )
        assert len(result.meta["online_epoch"]["planned_projection_hash"]) == 64
        assert result.meta["online_epoch"]["scheduler_skipped"] == ()

        assert len(os_.scheduler.attempts) == 1
        attempt = os_.scheduler.attempts[0]
        assert attempt.state is AttemptState.VERIFIED_SEMANTICALLY
        assert os_.scheduler.active_claim_for_task("task", gid) is None
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(
                claim_resource_uri(gid, "task")
            )
            == []
        )
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epoch_releases_failed_claim_without_vpg_commit() -> None:
    executed: list[str] = []

    async def fail(task_id: str) -> None:
        executed.append(task_id)
        raise RuntimeError("boom")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=fail, specializations=("python",)))
        goal = Goal("online-failure")
        goal.task("task", agent="worker", verify=lambda: _pass("artifact"))

        result = await os_.execute_online_epoch(goal)

        assert result.goal_state == "open"
        assert result.task_states["task"] == "unverified"
        assert executed == ["task"]
        assert any("RuntimeError: boom" in item for item in result.failures)
        assert result.meta["online_epoch"]["outcome"] == "completed_with_failures"
        assert result.meta["online_epoch"]["phases"] == (
            "observe",
            "reconcile",
            "plan",
            "admit",
            "execute",
            "verify",
            "observe",
        )
        assert result.meta["online_epoch"]["selected_task_ids"] == ("task",)
        assert result.meta["online_epoch"]["actual_dispatched_task_ids"] == ("task",)

        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        assert os_.scheduler.active_claim_for_task("task", gid) is None
        assert os_.scheduler.attempts[-1].state is AttemptState.FAILED
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(
                claim_resource_uri(gid, "task")
            )
            == []
        )
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epoch_is_noop_when_goal_is_already_verified() -> None:
    calls: list[str] = []

    def execute(task_id: str) -> None:
        calls.append(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("online-noop")
        goal.task("task", agent="worker", verify=lambda: _pass("artifact"))

        first = os_.run(goal, max_dispatches=1)
        assert first.goal_state == "closed"
        assert calls == ["task"]
        attempts_before = len(os_.scheduler.attempts)

        second = await os_.execute_online_epoch(goal)

        assert second.goal_state == "closed"
        assert second.verified == ["task"]
        assert second.meta["online_epoch"]["outcome"] == "no_dispatch"
        assert second.meta["online_epoch"]["phases"] == (
            "observe",
            "reconcile",
            "plan",
            "admit",
            "observe",
        )
        assert second.meta["online_epoch"]["selected_task_ids"] == ()
        assert second.meta["online_epoch"]["actual_dispatched_task_ids"] == ()
        assert len(os_.scheduler.attempts) == attempts_before
        assert calls == ["task"]
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epoch_distinguishes_policy_selection_from_fallback() -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
            )
        )
        goal = Goal("online-fallback-audit")
        goal.task(
            "a-ineligible",
            agent="worker",
            required_specializations=("gpu",),
            verify=lambda: _pass("ineligible"),
        )
        goal.task(
            "b-eligible",
            agent="worker",
            required_specializations=("python",),
            verify=lambda: _pass("eligible"),
        )

        result = await os_.execute_online_epoch(goal)

        online = result.meta["online_epoch"]
        assert executed == ["b-eligible"]
        assert online["policy_selected_task_ids"] == ("a-ineligible",)
        assert online["actual_dispatched_task_ids"] == ("b-eligible",)
        assert online["fallback_attempted"] is True
        assert online["fallback_dispatched_task_ids"] == ("b-eligible",)
        assert any(
            task_id == "a-ineligible" and "no eligible agent" in reason
            for task_id, reason in online["scheduler_skipped"]
        )
        assert online["scheduler_skipped_count"] == len(online["scheduler_skipped"])
        assert online["planned_graph_id"] == result.meta["adaptive_epochs"][0]["graph_id"]
        assert online["planned_graph_version"] == result.meta["adaptive_epochs"][0]["graph_version"]
        assert online["final_graph_version"] >= online["planned_graph_version"]
        adaptive = result.meta["adaptive_epochs"][0]
        assert adaptive["actual_dispatched_task_ids"] == ("b-eligible",)
        assert adaptive["fallback_dispatched_task_ids"] == ("b-eligible",)
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epoch_zero_budget_is_strict_no_work() -> None:
    executed: list[str] = []
    verified: list[str] = []

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=lambda task_id: executed.append(task_id),
                specializations=("python",),
            )
        )
        goal = Goal("online-zero-budget")
        goal.task(
            "task",
            agent="worker",
            verify=lambda: (
                verified.append("task"),
                _pass("artifact"),
            )[1],
        )

        result = await os_.execute_online_epoch(
            goal,
            max_dispatches=0,
            persist_epoch=True,
        )

        online = result.meta["online_epoch"]
        assert online["outcome"] == "no_work_budget"
        assert online["phases"] == ("observe",)
        assert online["executed_phases"] == ("observe",)
        assert online["policy_selected_task_ids"] == ()
        assert online["actual_dispatched_task_ids"] == ()
        assert result.meta["adaptive_epochs"] == []
        assert result.meta["dispatched"] == 0
        assert executed == []
        assert verified == []
        assert os_.scheduler.claims == []
        assert os_.scheduler.attempts == []
        assert not any(
            getattr(event.event_type, "value", event.event_type) == "scheduling_epoch_planned"
            for event in os_.scheduler.events
        )
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epoch_validates_bounds_before_execution() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("online-validation")
        with pytest.raises(ConfigurationError, match="max_concurrency must be >= 1"):
            await os_.execute_online_epoch(goal, max_concurrency=0)
        with pytest.raises(ConfigurationError, match="max_dispatches must be >= 0"):
            await os_.execute_online_epoch(goal, max_dispatches=-1)
        with pytest.raises(ConfigurationError, match="persist_epoch must be a boolean"):
            await os_.execute_online_epoch(goal, persist_epoch="yes")  # type: ignore[arg-type]
        with pytest.raises(ConfigurationError, match="max_parallelism must be >= 1"):
            await os_.execute_online_epoch(goal, max_parallelism=0)
        with pytest.raises(ConfigurationError, match="resource_aware must be a boolean"):
            await os_.execute_online_epoch(goal, resource_aware="yes")  # type: ignore[arg-type]
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epoch_forwards_resource_aware_policy() -> None:
    executed: list[str] = []
    requests = {
        "a-heavy": ResourceVector(cpu_millis=700),
        "b-heavy": ResourceVector(cpu_millis=700),
        "c-light": ResourceVector(cpu_millis=300),
        "d-light": ResourceVector(cpu_millis=300),
    }

    async def execute(task_id: str) -> None:
        executed.append(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                max_concurrency=2,
                resource_capacity=ResourceVector(cpu_millis=1_000),
            )
        )
        goal = Goal("online-resource-aware")
        for task_id, request in requests.items():
            goal.task(
                task_id,
                agent="worker",
                resources=request,
                inputs=(f"workspace://input/{task_id}",),
                outputs=(f"workspace://output/{task_id}",),
                verify=lambda task_id=task_id: _pass(f"artifact-{task_id}"),
            )
        conflicts = ConflictGraph.from_access_sets(
            [
                TaskAccessSet(
                    task_id=task_id,
                    read_set=(f"workspace://input/{task_id}",),
                    write_set=(f"workspace://output/{task_id}",),
                )
                for task_id in requests
            ]
        )

        result = await os_.execute_online_epoch(
            goal,
            max_concurrency=2,
            max_dispatches=2,
            max_parallelism=2,
            resource_aware=True,
            conflict_graph=conflicts,
            persist_epoch=False,
            automatic_rebase=False,
        )

        online = result.meta["online_epoch"]
        assert online["resource_aware"] is True
        assert online["max_parallelism"] == 2
        assert online["policy_selected_task_ids"] == ("a-heavy", "c-light")
        assert online["actual_dispatched_task_ids"] == ("a-heavy", "c-light")
        assert sorted(executed) == ["a-heavy", "c-light"]
        assert sorted(result.verified) == ["a-heavy", "c-light"]
        assert result.goal_state == "open"
    finally:
        os_.close()
