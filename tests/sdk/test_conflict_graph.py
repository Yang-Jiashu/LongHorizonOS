"""ConflictGraph and opt-in dynamic-parallelism tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    AgentCognitionState,
    ConflictGraph,
    ConflictReason,
    DynamicParallelismPolicy,
    FrontierAction,
    GlobalRuntimeState,
    ParallelBatchSuggestion,
    ProgressSemanticState,
    ResourceBindingState,
    ResourceRuntimeState,
    TaskAccessSet,
    suggest_parallel_batch,
)


def _state(
    *,
    ready: tuple[str, ...] = ("a", "b", "c"),
    repair_ready: tuple[str, ...] = (),
    stale: tuple[str, ...] = (),
    cognition_available: bool = True,
    resources_available: bool = True,
    active_attempts: tuple[dict[str, object], ...] = (),
) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=3,
            projection_hash="q" * 64,
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
            reason=None if cognition_available else "not restored",
            current_attempts=tuple(active_attempts),
        ),
        context={"available": False},
        resources=ResourceRuntimeState(
            available=resources_available,
            reason=None if resources_available else "no logical admission state",
        ),
    )


def test_conflict_graph_derives_read_write_and_write_write_conflicts() -> None:
    graph = ConflictGraph.from_access_sets(
        [
            TaskAccessSet(task_id="a", write_set=("artifact://api",)),
            TaskAccessSet(task_id="b", read_set=("artifact://api",)),
            TaskAccessSet(task_id="c", write_set=("artifact://api",)),
            TaskAccessSet(task_id="d", read_set=("artifact://other",)),
        ]
    )
    assert graph.task_ids == ("a", "b", "c", "d")
    assert graph.conflicts_with("a", "b")
    assert graph.conflicts_with("a", "c")
    assert not graph.conflicts_with("b", "d")
    pair = next(
        pair for pair in graph.conflicts if pair.left_task_id == "a" and pair.right_task_id == "b"
    )
    assert pair.reasons == (ConflictReason.READ_WRITE,)
    assert pair.resources == ("artifact://api",)


def test_unknown_access_is_recorded_and_duplicate_ids_rejected() -> None:
    graph = ConflictGraph.build(
        [
            {"task_id": "a", "known": False},
            {"task_id": "b", "read_set": ["x"]},
        ]
    )
    assert graph.unknown_task_ids == ("a",)
    assert graph.conflicts[0].reasons == (ConflictReason.UNKNOWN_ACCESS,)
    with pytest.raises(ValueError, match="duplicate"):
        ConflictGraph.build([{"task_id": "a"}, {"task_id": "a"}])


def test_independent_batch_is_deterministic_and_bounded() -> None:
    graph = ConflictGraph.from_access_sets(
        [
            TaskAccessSet(task_id="a", write_set=("a.out",)),
            TaskAccessSet(task_id="b", write_set=("b.out",)),
            TaskAccessSet(task_id="c", write_set=("a.out",)),
        ]
    )
    first = DynamicParallelismPolicy(max_parallelism=2).suggest(_state(), graph, epoch_id=8)
    second = suggest_parallel_batch(_state(), graph, epoch_id=8, max_parallelism=2)
    assert first == second
    assert first.selected_task_ids == ("a", "b")
    assert first.deferred_task_ids == ("c",)
    assert next(item for item in first.decisions if item.task_id == "c").reason == "conflict"
    assert first.parallelism_hint == 2
    assert first.safe_under_declared_accesses
    assert first.decision_hash == second.decision_hash


def test_unknown_or_missing_access_is_serial_only_and_marked_unavailable() -> None:
    graph = ConflictGraph.from_access_sets(
        [TaskAccessSet(task_id="a", write_set=("a.out",), known=False)]
    )
    suggestion = DynamicParallelismPolicy(max_parallelism=4).suggest(_state(), graph)
    assert suggestion.selected_task_ids == ("a",)
    assert suggestion.deferred_task_ids == ("b", "c")
    assert not suggestion.safe_under_declared_accesses
    assert any(item.name == "conflict_graph.a" for item in suggestion.unavailable)
    assert all(
        item.reason == "unknown_access_serial_only"
        for item in suggestion.decisions
        if item.task_id == "a" or item.action is FrontierAction.DEFER
    )


def test_missing_access_after_one_selected_task_is_deferred() -> None:
    graph = ConflictGraph.from_access_sets([TaskAccessSet(task_id="a", write_set=("a.out",))])
    suggestion = DynamicParallelismPolicy(max_parallelism=3).suggest(_state(), graph)
    assert suggestion.selected_task_ids == ("a",)
    assert suggestion.deferred_task_ids == ("b", "c")
    assert {item.reason for item in suggestion.decisions if item.task_id in {"b", "c"}} == {
        "unknown_access_serial_only"
    }
    assert all(
        item.blockers == ("a",) for item in suggestion.decisions if item.task_id in {"b", "c"}
    )


def test_unavailable_cognition_or_resources_fail_closed() -> None:
    graph = ConflictGraph.from_access_sets([TaskAccessSet(task_id="a", write_set=("a.out",))])
    for state in (_state(cognition_available=False), _state(resources_available=False)):
        suggestion = DynamicParallelismPolicy(max_parallelism=4).suggest(state, graph)
        assert suggestion.selected_task_ids == ()
        assert suggestion.parallelism_hint == 0
        assert all(item.action is FrontierAction.DEFER for item in suggestion.decisions)
        assert not suggestion.safe_under_declared_accesses


def test_active_writer_blocks_conflicting_ready_task() -> None:
    graph = ConflictGraph.from_access_sets(
        [TaskAccessSet(task_id="ready", write_set=("workspace://shared.py",))]
    )
    active = {
        "claim_id": "claim-active",
        "claim_state": "active",
        "task_id": "running",
        "agent_id": "worker",
        "process_id": "process",
        "attempt_id": "attempt-active",
        "read_set": (),
        "write_set": (
            ResourceBindingState(operation="write", resource_uri="workspace://shared.py"),
        ),
    }
    suggestion = DynamicParallelismPolicy(max_parallelism=2).suggest(
        _state(ready=("ready",), active_attempts=(active,)),
        graph,
    )
    assert suggestion.selected_task_ids == ()
    decision = suggestion.decisions[0]
    assert decision.reason == "active_conflict"
    assert decision.blockers == ("active:attempt-active",)


def test_unknown_active_access_fails_closed_for_parallel_suggestion() -> None:
    graph = ConflictGraph.from_access_sets(
        [TaskAccessSet(task_id="ready", write_set=("workspace://new.py",))]
    )
    active = {
        "claim_id": "claim-active",
        "claim_state": "active",
        "task_id": "running",
        "agent_id": "worker",
        "process_id": "process",
        "attempt_id": "attempt-active",
        "unavailable": (
            {"name": "read_set", "reason": "AgentSnapshot is not captured"},
            {"name": "write_set", "reason": "AgentSnapshot is not captured"},
        ),
    }
    suggestion = DynamicParallelismPolicy(max_parallelism=2).suggest(
        _state(ready=("ready",), active_attempts=(active,)),
        graph,
    )
    assert suggestion.selected_task_ids == ()
    assert suggestion.parallelism_hint == 0
    assert not suggestion.safe_under_declared_accesses
    assert any(item.name == "active_access.attempt-active" for item in suggestion.unavailable)


def test_explicit_empty_active_access_set_is_known_empty() -> None:
    graph = ConflictGraph.from_access_sets(
        [TaskAccessSet(task_id="ready", write_set=("workspace://new.py",))]
    )
    active = {
        "claim_id": "claim-active",
        "claim_state": "active",
        "task_id": "running",
        "agent_id": "worker",
        "process_id": "process",
        "attempt_id": "attempt-empty",
        "read_set": (),
        "write_set": (),
        "unavailable": (),
    }
    suggestion = DynamicParallelismPolicy(max_parallelism=2).suggest(
        _state(ready=("ready",), active_attempts=(active,)),
        graph,
    )
    assert suggestion.selected_task_ids == ("ready",)
    assert suggestion.safe_under_declared_accesses
    assert not any(item.name == "active_access.attempt-empty" for item in suggestion.unavailable)


def test_frozen_output_and_hash_stability() -> None:
    graph = ConflictGraph.from_access_sets([TaskAccessSet(task_id="a", write_set=("a.out",))])
    suggestion = DynamicParallelismPolicy().suggest(_state(ready=("a",)), graph)
    assert suggestion == DynamicParallelismPolicy().suggest(_state(ready=("a",)), graph)
    with pytest.raises(ValidationError):
        suggestion.selected_task_ids += ("x",)  # type: ignore[misc]
    with pytest.raises(ValidationError):
        ParallelBatchSuggestion.model_validate({**suggestion.model_dump(), "extra_field": True})


def test_access_set_normalization_is_strict_and_exact_match() -> None:
    access = TaskAccessSet(task_id="a", read_set=(" x ", "x", ""))
    assert access.read_set == ("x",)
    with pytest.raises(ValidationError):
        TaskAccessSet(task_id="a", read_set=(1,))  # type: ignore[list-item]
