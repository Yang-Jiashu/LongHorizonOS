"""Focused tests for resource-aware conflict-aware parallelism planning."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    AgentCognitionState,
    CognitionAttemptState,
    ConflictGraph,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceAwareParallelismPolicy,
    ResourcePoolState,
    ResourceRuntimeState,
    ResourceTaskRequest,
    ResourceVectorState,
    TaskAccessSet,
    plan_resource_aware_batch,
    suggest_resource_aware_batch,
)


def _vector(
    *,
    cpu: int = 0,
    ram: int = 0,
    gpu: int = 0,
    vram: int = 0,
    slots: tuple[tuple[str, int], ...] = (),
) -> ResourceVectorState:
    return ResourceVectorState(
        cpu_millis=cpu,
        ram_bytes=ram,
        gpu_count=gpu,
        vram_bytes=vram,
        model_slots=tuple({"name": name, "quantity": quantity} for name, quantity in slots),
    )


def _state(
    *,
    ready: tuple[str, ...] = ("a", "b", "c"),
    available: ResourceVectorState | None = None,
    resources_available: bool = True,
    pools: tuple[ResourcePoolState, ...] | None = None,
) -> GlobalRuntimeState:
    if pools is None:
        pools = (
            ResourcePoolState(
                pool_id="worker",
                capacity=available or _vector(cpu=1_000),
                reserved=_vector(),
                available=available or _vector(cpu=1_000),
            ),
        )
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=3,
            projection_hash="p" * 64,
            graph_closed=False,
            goal_closed=False,
            ready_frontier=ready,
            repair_ready_frontier=(),
            verified_task_ids=(),
            stale_task_ids=(),
            invalid_task_ids=(),
            unverified_task_ids=ready,
        ),
        agent_cognition=AgentCognitionState(
            available=True,
            current_attempts=(),
        ),
        context={"available": False},
        resources=ResourceRuntimeState(
            available=resources_available,
            reason=None if resources_available else "resource projection unavailable",
            pools=pools,
        ),
    )


def _graph(*task_ids: str) -> ConflictGraph:
    return ConflictGraph.from_access_sets(
        [TaskAccessSet(task_id=task_id, write_set=(f"out://{task_id}",)) for task_id in task_ids]
    )


def test_resource_aware_policy_fits_additive_vector_deterministically() -> None:
    state = _state(available=_vector(cpu=1_000))
    requests = {
        "a": {"cpu_millis": 600},
        "b": {"cpu_millis": 400},
        "c": {"cpu_millis": 100},
    }
    graph = _graph("a", "b", "c")

    first = ResourceAwareParallelismPolicy(max_parallelism=3).suggest(
        state, graph, requests, epoch_id=7
    )
    second = suggest_resource_aware_batch(state, graph, requests, epoch_id=7, max_parallelism=3)

    assert first == second
    assert first.selected_task_ids == ("a", "b")
    assert first.deferred_task_ids == ("c",)
    assert [(item.task_id, item.pool_id) for item in first.assignments] == [
        ("a", "worker"),
        ("b", "worker"),
    ]
    assert first.parallelism_hint == 2
    assert first.safe_under_declared_resources
    assert first.safe_under_constraints
    assert first.decision_hash == second.decision_hash


def test_conflict_blocks_even_when_resources_fit() -> None:
    state = _state(available=_vector(cpu=2_000), ready=("a", "b"))
    graph = ConflictGraph.from_access_sets(
        [
            TaskAccessSet(task_id="a", write_set=("workspace://shared",)),
            TaskAccessSet(task_id="b", write_set=("workspace://shared",)),
        ]
    )
    suggestion = plan_resource_aware_batch(
        state,
        graph,
        {"a": {"cpu_millis": 100}, "b": {"cpu_millis": 100}},
        max_parallelism=2,
    )

    assert suggestion.selected_task_ids == ("a",)
    assert suggestion.deferred_task_ids == ("b",)
    decision = suggestion.decisions[-1]
    assert decision.reason == "conflict"
    assert decision.blockers == ("a",)
    assert suggestion.safe_under_constraints


def test_insufficient_capacity_is_deferred_and_explains_shortage() -> None:
    state = _state(available=_vector(cpu=100), ready=("expensive",))
    graph = _graph("expensive")
    suggestion = ResourceAwareParallelismPolicy().suggest(
        state,
        graph,
        {"expensive": {"cpu_millis": 101}},
    )

    assert suggestion.selected_task_ids == ()
    assert suggestion.parallelism_hint == 0
    assert suggestion.decisions[0].reason == "insufficient_resources"
    assert any("cpu_millis=1" in blocker for blocker in suggestion.decisions[0].blockers)
    assert not suggestion.safe_under_constraints


def test_missing_request_and_unknown_capacity_fail_closed() -> None:
    state = _state(ready=("a", "b"))
    graph = _graph("a", "b")
    missing = ResourceAwareParallelismPolicy(max_parallelism=2).suggest(
        state, graph, {"a": {"cpu_millis": 1}}
    )
    assert missing.selected_task_ids == ("a",)
    assert missing.deferred_task_ids == ("b",)
    assert any(item.name == "task_resources.b" for item in missing.unavailable)
    assert not missing.safe_under_declared_resources

    unknown_state = _state(
        resources_available=True,
        pools=(
            ResourcePoolState(
                pool_id="worker",
                capacity=None,
                reserved=_vector(),
                available=None,
            ),
        ),
    )
    unknown = ResourceAwareParallelismPolicy().suggest(
        unknown_state,
        _graph("a"),
        {"a": {"cpu_millis": 1}},
    )
    assert unknown.selected_task_ids == ()
    assert unknown.parallelism_hint == 0
    assert any("available" in item.name for item in unknown.unavailable)


def test_unknown_access_candidate_waits_for_active_occupancy() -> None:
    """Unknown candidate accesses must not overlap a live attempt.

    Without a candidate read/write declaration the policy cannot prove that
    the candidate is independent of an active attempt, even when the active
    attempt has a complete snapshot.  It therefore defers the candidate with
    an explicit active blocker instead of selecting it as the first serial
    task.
    """

    state = _state(ready=("hidden",), available=_vector(cpu=2_000))
    state = state.model_copy(
        update={
            "agent_cognition": AgentCognitionState(
                available=True,
                current_attempts=(
                    CognitionAttemptState(
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
                    ),
                ),
            )
        }
    )
    graph = ConflictGraph.from_access_sets([])
    suggestion = ResourceAwareParallelismPolicy(max_parallelism=2).suggest(
        state,
        graph,
        {
            "hidden": {"cpu_millis": 100},
        },
    )

    hidden = next(item for item in suggestion.decisions if item.task_id == "hidden")
    assert hidden.action.value == "defer"
    assert hidden.reason == "unknown_access_active_occupancy"
    assert hidden.blockers == ("active:attempt-live",)
    assert suggestion.selected_task_ids == ()

    # A later epoch that observes the active attempt gone may safely use the
    # unknown task as the sole serial candidate; the conservative fence is
    # about concurrent occupancy, not a permanent rejection.
    drained = state.model_copy(
        update={
            "agent_cognition": AgentCognitionState(
                available=True,
                current_attempts=(),
            )
        }
    )
    replanned = ResourceAwareParallelismPolicy(max_parallelism=2).suggest(
        drained,
        graph,
        {"hidden": {"cpu_millis": 100}},
    )
    assert replanned.selected_task_ids == ("hidden",)
    assert replanned.decisions[0].reason == "unknown_access_serial_only"


def test_unavailable_runtime_resources_never_selects_work() -> None:
    state = _state(resources_available=False)
    suggestion = ResourceAwareParallelismPolicy(max_parallelism=4).suggest(
        state,
        _graph("a", "b", "c"),
        {"a": {"cpu_millis": 1}, "b": {"cpu_millis": 1}, "c": {"cpu_millis": 1}},
    )
    assert suggestion.selected_task_ids == ()
    assert suggestion.parallelism_hint == 0
    assert all(item.action.value == "defer" for item in suggestion.decisions)
    assert any(item.name == "resources" for item in suggestion.unavailable)


def test_model_slots_and_explicit_pool_are_accounted_for() -> None:
    state = _state(
        ready=("a", "b"),
        pools=(
            ResourcePoolState(
                pool_id="gpu",
                capacity=_vector(gpu=1, slots=(("vision", 1),)),
                reserved=_vector(),
                available=_vector(gpu=1, slots=(("vision", 1),)),
            ),
            ResourcePoolState(
                pool_id="cpu",
                capacity=_vector(cpu=1_000),
                reserved=_vector(),
                available=_vector(cpu=1_000),
            ),
        ),
    )
    graph = _graph("a", "b")
    suggestion = ResourceAwareParallelismPolicy(max_parallelism=2).suggest(
        state,
        graph,
        {
            "a": ResourceTaskRequest(
                task_id="a",
                pool_id="gpu",
                resources=_vector(gpu=1, slots=(("vision", 1),)),
            ),
            "b": {"pool_id": "gpu", "resources": {"gpu_count": 1}},
        },
    )
    assert suggestion.selected_task_ids == ("a",)
    assert suggestion.assignments[0].pool_id == "gpu"
    assert suggestion.decisions[-1].reason == "insufficient_resources"


def test_output_is_frozen_and_aliases_are_stable() -> None:
    state = _state(ready=("a",))
    graph = _graph("a")
    policy = ResourceAwareParallelismPolicy()
    first = policy.plan(state, graph, {"a": {"cpu_millis": 1}})
    second = policy.suggest(state, graph, {"a": {"cpu_millis": 1}})
    assert first == second
    assert first.resource_assignments == first.assignments
    with pytest.raises(ValidationError):
        first.selected_task_ids += ("x",)  # type: ignore[misc]
    with pytest.raises(ValidationError):
        ResourceAwareParallelismPolicy(max_parallelism=0)
