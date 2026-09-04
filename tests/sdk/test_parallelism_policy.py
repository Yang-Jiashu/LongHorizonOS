"""Deterministic AdaptiveParallelismPolicy degree + stability tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    AdaptiveParallelismDecision,
    AdaptiveParallelismPolicy,
    AgentCognitionState,
    ConflictGraph,
    FrontierAction,
    GlobalRuntimeState,
    ProgressSemanticState,
    RecentRuntimeEventState,
    ResourcePoolState,
    ResourceRuntimeState,
    ResourceVectorState,
    TaskAccessSet,
    decide_parallelism,
)


def _vector(cpu: int = 0) -> ResourceVectorState:
    return ResourceVectorState(cpu_millis=cpu)


def _event(
    *,
    node_id: str,
    event_type: str,
    graph_version: int | None,
    event_id: str,
) -> RecentRuntimeEventState:
    return RecentRuntimeEventState(
        event_id=event_id,
        event_type=event_type,
        graph_version=graph_version,
        node_id=node_id,
        payload_hash="0" * 64,
        recorded_at=datetime.now(UTC),
    )


def _state(
    *,
    ready: tuple[str, ...] = ("a", "b", "c"),
    repair: tuple[str, ...] = (),
    stale: tuple[str, ...] = (),
    events: tuple[RecentRuntimeEventState, ...] = (),
    available_cpu: int = 1_000_000,
    cognition_available: bool = True,
    resources_available: bool = True,
    graph_closed: bool = False,
    goal_closed: bool = False,
) -> GlobalRuntimeState:
    pools = (
        (
            ResourcePoolState(
                pool_id="worker",
                capacity=_vector(available_cpu),
                reserved=_vector(),
                available=_vector(available_cpu),
            ),
        )
        if resources_available
        else ()
    )
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=9,
            projection_hash="p" * 64,
            graph_closed=graph_closed,
            goal_closed=goal_closed,
            ready_frontier=ready,
            repair_ready_frontier=repair,
            verified_task_ids=(),
            stale_task_ids=stale,
            invalid_task_ids=(),
            unverified_task_ids=ready,
        ),
        agent_cognition=AgentCognitionState(
            available=cognition_available,
            reason=None if cognition_available else "durable cognition unavailable",
            current_attempts=(),
        ),
        context={"available": False},
        resources=ResourceRuntimeState(
            available=resources_available,
            reason=None if resources_available else "logical resources unavailable",
            pools=pools,
        ),
        recent_events=events,
    )


def _independent(*task_ids: str) -> ConflictGraph:
    return ConflictGraph.from_access_sets(
        [TaskAccessSet(task_id=task_id, write_set=(f"out://{task_id}",)) for task_id in task_ids]
    )


def _all_conflicting(*task_ids: str) -> ConflictGraph:
    return ConflictGraph.from_access_sets(
        [TaskAccessSet(task_id=task_id, write_set=("shared://x",)) for task_id in task_ids]
    )


def _requests(*task_ids: str, cpu: int = 100) -> dict[str, dict[str, int]]:
    return {task_id: {"cpu_millis": cpu} for task_id in task_ids}


def test_degree_equals_conflict_free_antichain_when_independent() -> None:
    decision = decide_parallelism(
        _state(),
        _independent("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=8,
    )
    assert decision.chosen_degree == 3
    assert decision.degree_reason == "conflict_antichain"
    assert decision.dispatch_task_ids == ("a", "b", "c")
    assert decision.frontier_antichain == ("a", "b", "c")
    assert decision.safe


def test_degree_shrinks_when_conflicts_rise() -> None:
    independent = decide_parallelism(
        _state(ready=("a", "b", "c", "d")),
        _independent("a", "b", "c", "d"),
        task_resources=_requests("a", "b", "c", "d"),
        max_parallelism=8,
    )
    # Two write-write conflict pairs collapse the antichain to two tasks.
    conflicted_graph = ConflictGraph.from_access_sets(
        [
            TaskAccessSet(task_id="a", write_set=("shared://one",)),
            TaskAccessSet(task_id="b", write_set=("shared://one",)),
            TaskAccessSet(task_id="c", write_set=("shared://two",)),
            TaskAccessSet(task_id="d", write_set=("shared://two",)),
        ]
    )
    conflicted = decide_parallelism(
        _state(ready=("a", "b", "c", "d")),
        conflicted_graph,
        task_resources=_requests("a", "b", "c", "d"),
        max_parallelism=8,
    )
    assert independent.chosen_degree == 4
    assert conflicted.chosen_degree == 2
    assert conflicted.chosen_degree < independent.chosen_degree
    assert conflicted.degree_reason == "conflict_antichain"


def test_degree_reaches_one_when_everything_conflicts() -> None:
    decision = decide_parallelism(
        _state(),
        _all_conflicting("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=8,
    )
    assert decision.chosen_degree == 1
    assert decision.degree_reason == "conflict_antichain"
    assert len(decision.dispatch_task_ids) == 1
    antichain_bound = next(b for b in decision.bounds if b.name == "conflict_free_antichain")
    assert antichain_bound.value == 1
    assert antichain_bound.binding


def test_degree_never_exceeds_ceiling() -> None:
    decision = decide_parallelism(
        _state(ready=("a", "b", "c", "d", "e")),
        _independent("a", "b", "c", "d", "e"),
        task_resources=_requests("a", "b", "c", "d", "e"),
        max_parallelism=2,
    )
    assert decision.chosen_degree == 2
    assert decision.degree_reason == "ceiling"
    assert decision.chosen_degree <= decision.ceiling


def test_degree_shrinks_when_agent_capacity_is_lower() -> None:
    graph = _independent("a", "b", "c")
    requests = _requests("a", "b", "c", cpu=100)
    ample = decide_parallelism(
        _state(available_cpu=10_000), graph, task_resources=requests, max_parallelism=8
    )
    medium = decide_parallelism(
        _state(available_cpu=250), graph, task_resources=requests, max_parallelism=8
    )
    tight = decide_parallelism(
        _state(available_cpu=100), graph, task_resources=requests, max_parallelism=8
    )
    assert ample.chosen_degree == 3
    assert medium.chosen_degree == 2
    assert tight.chosen_degree == 1
    assert medium.degree_reason == "resource_headroom"
    assert tight.degree_reason == "resource_headroom"


def test_decision_is_deterministic_across_insertion_orders() -> None:
    graph_forward = _independent("a", "b", "c")
    graph_reverse = ConflictGraph.from_access_sets(
        [
            TaskAccessSet(task_id="c", write_set=("out://c",)),
            TaskAccessSet(task_id="a", write_set=("out://a",)),
            TaskAccessSet(task_id="b", write_set=("out://b",)),
        ]
    )
    events_forward = (
        _event(node_id="x", event_type="node.invalid", graph_version=5, event_id="e1"),
        _event(node_id="y", event_type="task.stale.derived", graph_version=6, event_id="e2"),
    )
    events_reverse = (events_forward[1], events_forward[0])
    first = decide_parallelism(
        _state(ready=("a", "b", "c"), events=events_forward),
        graph_forward,
        task_resources=_requests("a", "b", "c"),
        max_parallelism=8,
    )
    second = decide_parallelism(
        _state(ready=("c", "b", "a"), events=events_reverse),
        graph_reverse,
        task_resources={
            "c": {"cpu_millis": 100},
            "b": {"cpu_millis": 100},
            "a": {"cpu_millis": 100},
        },
        max_parallelism=8,
    )
    assert first == second
    assert first.decision_hash == second.decision_hash
    assert first.model_dump_json() == second.model_dump_json()


def test_contention_backoff_shrinks_degree() -> None:
    # Three unrelated tasks were reworked recently; none are on the frontier,
    # so the backoff purely reflects observed graph instability.
    events = (
        _event(node_id="x", event_type="node.invalid", graph_version=5, event_id="e1"),
        _event(node_id="y", event_type="task.stale.derived", graph_version=6, event_id="e2"),
        _event(node_id="z", event_type="task.reopened.derived", graph_version=7, event_id="e3"),
    )
    calm = decide_parallelism(
        _state(),
        _independent("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=8,
    )
    churny = decide_parallelism(
        _state(events=events),
        _independent("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=8,
    )
    assert calm.chosen_degree == 3
    assert calm.contention_backoff == 0
    assert churny.contention_backoff == 3
    assert churny.chosen_degree == 1
    assert churny.degree_reason == "contention_backoff"


def test_serial_when_resource_requests_absent_is_fail_closed() -> None:
    decision = decide_parallelism(
        _state(),
        _independent("a", "b", "c"),
        max_parallelism=8,
    )
    assert decision.chosen_degree == 1
    assert decision.degree_reason == "resource_requests_unavailable_serial"
    assert not decision.resource_headroom_observed
    assert not decision.safe
    assert any(item.name == "resource_headroom" for item in decision.unavailable)


def test_churning_upstream_defers_consumer_while_stable_peers_run() -> None:
    # Task "a" is re-derived stale across two distinct graph versions => churn.
    events = (
        _event(node_id="a", event_type="task.stale.derived", graph_version=7, event_id="e1"),
        _event(node_id="a", event_type="task.stale.derived", graph_version=8, event_id="e2"),
    )
    decision = decide_parallelism(
        _state(events=events),
        _independent("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=8,
    )
    assert decision.deferred_for_churn == ("a",)
    assert "a" not in decision.dispatch_task_ids
    assert decision.dispatch_task_ids == ("b", "c")
    verdict_a = next(v for v in decision.stability if v.task_id == "a")
    assert verdict_a.action is FrontierAction.DEFER
    assert not verdict_a.stable
    assert verdict_a.reason == "upstream_churn_versions"
    assert verdict_a.churn_version_count == 2
    assert len(verdict_a.evidence) == 2
    verdict_b = next(v for v in decision.stability if v.task_id == "b")
    assert verdict_b.stable
    assert verdict_b.action is FrontierAction.RUN


def test_single_stale_transition_does_not_defer_consumer() -> None:
    events = (_event(node_id="a", event_type="task.stale.derived", graph_version=7, event_id="e1"),)
    decision = decide_parallelism(
        _state(ready=("a", "b", "c"), repair=("a",), stale=("a",), events=events),
        _independent("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=8,
    )
    assert decision.deferred_for_churn == ()
    verdict_a = next(v for v in decision.stability if v.task_id == "a")
    assert verdict_a.stable
    assert verdict_a.churn_event_count == 1
    assert verdict_a.reason == "upstream_stable"


def test_upstream_map_defers_on_upstream_artifact_churn() -> None:
    # Churn events name an upstream artifact node, not the consumer task.
    events = (
        _event(
            node_id="artifact-1", event_type="artifact.attached", graph_version=6, event_id="e1"
        ),
        _event(
            node_id="artifact-1", event_type="artifact.attached", graph_version=7, event_id="e2"
        ),
    )
    graph = _independent("consumer", "other")
    requests = _requests("consumer", "other")
    without_map = decide_parallelism(
        _state(ready=("consumer", "other"), events=events),
        graph,
        task_resources=requests,
        max_parallelism=8,
    )
    with_map = decide_parallelism(
        _state(ready=("consumer", "other"), events=events),
        graph,
        task_resources=requests,
        upstream={"consumer": ("artifact-1",)},
        max_parallelism=8,
    )
    assert without_map.deferred_for_churn == ()
    assert with_map.deferred_for_churn == ("consumer",)
    verdict = next(v for v in with_map.stability if v.task_id == "consumer")
    assert verdict.reason == "upstream_churn_versions"
    assert verdict.evidence[0].event_type == "artifact.attached"


def test_fail_closed_cognition_and_resources_yield_zero_degree() -> None:
    graph = _independent("a", "b", "c")
    requests = _requests("a", "b", "c")
    no_cognition = decide_parallelism(
        _state(cognition_available=False), graph, task_resources=requests, max_parallelism=8
    )
    no_resources = decide_parallelism(
        _state(resources_available=False), graph, task_resources=requests, max_parallelism=8
    )
    assert no_cognition.chosen_degree == 0
    assert no_cognition.degree_reason == "cognition_unavailable"
    assert not no_cognition.safe
    assert no_resources.chosen_degree == 0
    assert no_resources.degree_reason == "resources_unavailable"


def test_closed_graph_yields_zero_degree() -> None:
    decision = decide_parallelism(
        _state(goal_closed=True),
        _independent("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=8,
    )
    assert decision.chosen_degree == 0
    assert decision.degree_reason == "closed"
    assert decision.dispatch_task_ids == ()


def test_all_candidates_churning_yields_zero_degree() -> None:
    events = (
        _event(node_id="a", event_type="node.invalid", graph_version=6, event_id="a1"),
        _event(node_id="a", event_type="node.invalid", graph_version=7, event_id="a2"),
        _event(node_id="b", event_type="node.invalid", graph_version=6, event_id="b1"),
        _event(node_id="b", event_type="node.invalid", graph_version=7, event_id="b2"),
    )
    decision = decide_parallelism(
        _state(ready=("a", "b"), events=events),
        _independent("a", "b"),
        task_resources=_requests("a", "b"),
        max_parallelism=8,
    )
    assert decision.deferred_for_churn == ("a", "b")
    assert decision.chosen_degree == 0
    assert decision.degree_reason == "all_candidates_churning"


def test_empty_frontier_yields_zero_degree() -> None:
    decision = decide_parallelism(
        _state(ready=()),
        _independent(),
        task_resources={},
        max_parallelism=8,
    )
    assert decision.candidate_task_ids == ()
    assert decision.chosen_degree == 0
    assert decision.degree_reason == "no_candidates"


def test_ceiling_of_one_is_an_explained_serial_decision() -> None:
    decision = decide_parallelism(
        _state(),
        _independent("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=1,
    )
    assert decision.chosen_degree == 1
    assert decision.degree_reason == "ceiling"


def test_output_is_frozen_and_hash_is_stable() -> None:
    state = _state()
    graph = _independent("a", "b", "c")
    requests = _requests("a", "b", "c")
    first = AdaptiveParallelismPolicy(max_parallelism=4).decide(
        state, graph, task_resources=requests, epoch_id=3
    )
    second = AdaptiveParallelismPolicy(max_parallelism=4).plan(
        state, graph, task_resources=requests, epoch_id=3
    )
    assert first == second
    assert first.decision_hash == second.decision_hash
    with pytest.raises(ValidationError):
        first.dispatch_task_ids += ("x",)  # type: ignore[misc]
    with pytest.raises(ValidationError):
        AdaptiveParallelismDecision.model_validate({**first.model_dump(), "extra_field": True})


def test_invalid_policy_and_epoch_ids_are_rejected() -> None:
    with pytest.raises(ValidationError):
        AdaptiveParallelismPolicy(max_parallelism=0)
    with pytest.raises(ValidationError):
        AdaptiveParallelismPolicy(churn_version_threshold=0)
    with pytest.raises(ValueError):
        decide_parallelism(
            _state(),
            _independent("a"),
            task_resources=_requests("a"),
            epoch_id=-1,
        )


def test_input_state_is_not_mutated() -> None:
    state = _state(
        events=(
            _event(node_id="a", event_type="task.stale.derived", graph_version=7, event_id="e1"),
        )
    )
    before = state.model_dump_json()
    decide_parallelism(
        state,
        _independent("a", "b", "c"),
        task_resources=_requests("a", "b", "c"),
        max_parallelism=4,
    )
    assert state.model_dump_json() == before


def test_assess_upstream_stability_is_usable_standalone() -> None:
    events = (
        _event(node_id="a", event_type="task.stale.derived", graph_version=7, event_id="e1"),
        _event(node_id="a", event_type="task.reopened.derived", graph_version=8, event_id="e2"),
    )
    verdicts = AdaptiveParallelismPolicy().assess_upstream_stability(
        _state(ready=("a", "b"), events=events)
    )
    assert {v.task_id for v in verdicts} == {"a", "b"}
    verdict_a = next(v for v in verdicts if v.task_id == "a")
    assert verdict_a.action is FrontierAction.DEFER
    assert verdict_a.churn_version_count == 2
