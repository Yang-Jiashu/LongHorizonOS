"""Read-only AgentOS facades for online-compute policy primitives."""

from __future__ import annotations

import pytest

from lhos.sdk import (
    GRAPH_UTILITY_FRONTIER_POLICY_ID,
    Agent,
    AgentOS,
    ConfigurationError,
    ConflictGraph,
    Goal,
    InterruptAction,
    SemanticInterrupt,
    SemanticInterruptKind,
    TaskAccessSet,
)


def _compiled_goal() -> tuple[AgentOS, Goal]:
    os_ = AgentOS(":memory:")
    goal = Goal("policy-facade-goal")
    goal.task("task-a", agent="")
    os_._compile_goal(goal)
    return os_, goal


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
    )


def test_policy_facades_are_deterministic_and_do_not_claim_or_execute() -> None:
    os_, goal = _compiled_goal()
    try:
        before = _fingerprint(os_)
        first = os_.plan_frontier(goal, epoch_id=4, max_parallelism=2)
        second = os_.plan_frontier(goal.goal_id, epoch_id=4, max_parallelism=2)
        assert first == second
        assert first.selected_task_ids == ("task-a",)

        conflicts = ConflictGraph.from_access_sets(
            [TaskAccessSet(task_id="task-a", write_set=("workspace://a.py",))]
        )
        batch = os_.suggest_parallel_batch(
            goal,
            conflicts,
            epoch_id=4,
            max_parallelism=2,
        )
        assert batch.selected_task_ids == ("task-a",)

        interrupt = SemanticInterrupt(
            interrupt_id="interrupt-1",
            graph_id=first.graph_id,
            graph_version=first.graph_version,
            kind=SemanticInterruptKind.TASK_VERIFIED,
            reason="task was verified elsewhere",
            affected_task_ids=("task-a",),
        )
        routed = os_.plan_interrupts(goal, [interrupt], epoch_id=4)
        assert routed.decisions[0].action is InterruptAction.DEFER
        assert _fingerprint(os_) == before
    finally:
        os_.close()


def test_frontier_facade_exposes_graph_utility_as_explicit_opt_in() -> None:
    os_ = AgentOS(":memory:")
    goal = Goal("graph-utility-facade-goal")
    goal.task("a-short", agent="")
    critical_leaf = goal.task("z-critical-leaf", agent="")
    goal.task("z-critical-tail", agent="", depends_on=(critical_leaf,))
    os_._compile_goal(goal)
    try:
        before = _fingerprint(os_)

        default = os_.plan_frontier(goal, max_parallelism=1)
        utility = os_.plan_frontier(
            goal,
            max_parallelism=1,
            ranking_strategy="graph_utility",
        )

        assert default.selected_task_ids == ("a-short",)
        assert utility.selected_task_ids == ("z-critical-leaf",)
        assert default.policy_id == "deterministic-frontier.v1"
        assert utility.policy_id == GRAPH_UTILITY_FRONTIER_POLICY_ID
        assert utility.decisions[0].score > default.decisions[0].score
        assert _fingerprint(os_) == before
    finally:
        os_.close()


def test_resource_aware_facade_uses_compiled_task_requests_read_only() -> None:
    os_ = AgentOS(":memory:")
    os_.add_agent(
        Agent(
            "worker",
            resource_capacity={"cpu_millis": 1_000},
        )
    )
    goal = Goal("resource-policy-facade-goal")
    goal.task(
        "task-a",
        agent="worker",
        resources={"cpu_millis": 100},
    )
    os_._compile_goal(goal)
    try:
        conflicts = ConflictGraph.from_access_sets(
            [TaskAccessSet(task_id="task-a", write_set=("workspace://a.py",))]
        )
        before = _fingerprint(os_)
        suggestion = os_.suggest_resource_aware_batch(
            goal,
            conflicts,
            max_parallelism=2,
        )
        assert suggestion.selected_task_ids == ("task-a",)
        assert suggestion.assignments[0].resources.cpu_millis == 100
        assert _fingerprint(os_) == before
    finally:
        os_.close()


def test_policy_facades_fail_closed_for_uncompiled_goal_without_mutation() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("not-compiled-policy-goal")
        before = (dict(os_._goals), dict(os_._goal_gid), tuple(os_.scheduler.claims))
        with pytest.raises(ConfigurationError, match="not compiled"):
            os_.plan_frontier(goal)
        with pytest.raises(ConfigurationError, match="not compiled"):
            os_.suggest_parallel_batch(
                goal,
                ConflictGraph.from_access_sets(()),
            )
        with pytest.raises(ConfigurationError, match="not compiled"):
            os_.plan_interrupts(goal, ())
        assert dict(os_._goals) == before[0]
        assert dict(os_._goal_gid) == before[1]
        assert tuple(os_.scheduler.claims) == before[2]
    finally:
        os_.close()
