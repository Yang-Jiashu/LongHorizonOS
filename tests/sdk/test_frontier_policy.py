"""Deterministic FrontierPolicy tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    GRAPH_UTILITY_FRONTIER_POLICY_ID,
    AgentCognitionState,
    FrontierAction,
    FrontierPolicy,
    FrontierRankingStrategy,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceRuntimeState,
    SchedulingEpoch,
    TaskUnlockValueState,
    UnavailableField,
    plan_frontier,
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
    critical_path: tuple[str, ...] = (),
    unlock_values: tuple[tuple[str, int], ...] = (),
) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
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
            critical_path=critical_path,
            downstream_unlock_values=tuple(
                TaskUnlockValueState(task_id=task_id, unlock_value=value)
                for task_id, value in unlock_values
            ),
        ),
        agent_cognition=AgentCognitionState(
            available=cognition_available,
            reason=None if cognition_available else "durable cognition unavailable",
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
            "unavailable": (UnavailableField(name="bindings", reason="not bound"),),
        },
        resources=ResourceRuntimeState(
            available=resources_available,
            reason=None if resources_available else "logical resources unavailable",
        ),
    )


def test_empty_frontier_emits_empty_epoch() -> None:
    epoch = FrontierPolicy().plan(_state(ready=()), epoch_id=4)
    assert epoch.epoch_id == 4
    assert epoch.candidate_task_ids == ()
    assert epoch.selected_task_ids == ()
    assert epoch.deferred_task_ids == ()
    assert epoch.decisions == ()
    assert epoch.parallelism_hint == 0


def test_closed_goal_defers_every_candidate() -> None:
    epoch = FrontierPolicy(max_parallelism=4).plan(_state(goal_closed=True))
    assert epoch.selected_task_ids == ()
    assert epoch.deferred_task_ids == ("a", "b", "c")
    assert all(item.action is FrontierAction.DEFER for item in epoch.decisions)
    assert {item.reason for item in epoch.decisions} == {"closed"}


def test_repair_ready_ranks_before_ordinary_ready() -> None:
    epoch = FrontierPolicy(max_parallelism=1).plan(
        _state(ready=("ordinary", "repair"), repair_ready=("repair",), stale=("repair",))
    )
    assert epoch.candidate_task_ids == ("repair", "ordinary")
    assert epoch.selected_task_ids == ("repair",)
    assert epoch.decisions[0].score > epoch.decisions[1].score


def test_default_strategy_preserves_lexical_order_despite_graph_signals() -> None:
    state = _state(
        ready=("z-critical", "a-unlock"),
        critical_path=("z-critical",),
        unlock_values=(("a-unlock", 99),),
    )

    epoch = FrontierPolicy(max_parallelism=1).plan(state)

    assert epoch.policy_id == "deterministic-frontier.v1"
    assert epoch.candidate_task_ids == ("a-unlock", "z-critical")
    assert epoch.selected_task_ids == ("a-unlock",)


def test_graph_utility_prefers_critical_path_then_downstream_unlock() -> None:
    state = _state(
        ready=("a-low", "b-fanout", "z-critical"),
        critical_path=("z-critical", "later-on-path"),
        unlock_values=(("a-low", 1), ("b-fanout", 4), ("z-critical", 0)),
    )

    epoch = FrontierPolicy(
        max_parallelism=3,
        ranking_strategy=FrontierRankingStrategy.GRAPH_UTILITY,
    ).plan(state)

    assert epoch.policy_id == GRAPH_UTILITY_FRONTIER_POLICY_ID
    assert epoch.candidate_task_ids == ("z-critical", "b-fanout", "a-low")
    assert epoch.selected_task_ids == epoch.candidate_task_ids
    assert [decision.score for decision in epoch.decisions] == sorted(
        (decision.score for decision in epoch.decisions),
        reverse=True,
    )


def test_graph_utility_never_overrides_repair_priority_or_safety_filters() -> None:
    state = _state(
        ready=("repair", "critical", "active"),
        repair_ready=("repair",),
        stale=("repair",),
        active_tasks=("active",),
        critical_path=("critical",),
        unlock_values=(("active", 100), ("critical", 10)),
    )

    epoch = FrontierPolicy(
        max_parallelism=2,
        ranking_strategy=FrontierRankingStrategy.GRAPH_UTILITY,
    ).plan(state)

    assert epoch.candidate_task_ids == ("repair", "critical", "active")
    assert epoch.selected_task_ids == ("repair", "critical")
    active = next(decision for decision in epoch.decisions if decision.task_id == "active")
    assert active.action is FrontierAction.DEFER
    assert active.reason == "active_attempt"


def test_graph_utility_helper_is_deterministic_and_auditable() -> None:
    state = _state(
        ready=("a", "b"),
        critical_path=("b",),
        unlock_values=(("a", 5),),
    )
    first = plan_frontier(
        state,
        epoch_id=12,
        ranking_strategy="graph_utility",
    )
    second = plan_frontier(
        state,
        epoch_id=12,
        ranking_strategy=FrontierRankingStrategy.GRAPH_UTILITY,
    )

    assert first == second
    assert first.policy_id == GRAPH_UTILITY_FRONTIER_POLICY_ID
    assert first.selected_task_ids == ("b",)


def test_max_parallelism_is_a_strict_batch_bound() -> None:
    epoch = FrontierPolicy(max_parallelism=2).plan(_state())
    assert len(epoch.selected_task_ids) == 2
    assert len(epoch.deferred_task_ids) == 1
    assert set(epoch.selected_task_ids).isdisjoint(epoch.deferred_task_ids)
    assert set(epoch.selected_task_ids) | set(epoch.deferred_task_ids) == set(
        epoch.candidate_task_ids
    )


def test_repeated_plans_are_byte_stable() -> None:
    state = _state(ready=("z", "a", "m"), repair_ready=("m",), stale=("m",))
    first = plan_frontier(state, epoch_id=9, max_parallelism=2)
    second = plan_frontier(state, epoch_id=9, max_parallelism=2)
    assert first == second
    assert first.model_dump_json() == second.model_dump_json()
    assert first.decision_hash == second.decision_hash


def test_active_attempt_is_not_selected_again() -> None:
    epoch = FrontierPolicy(max_parallelism=3).plan(_state(active_tasks=("b",)))
    assert epoch.selected_task_ids == ("a", "c")
    assert "b" in epoch.deferred_task_ids
    assert next(item for item in epoch.decisions if item.task_id == "b").reason == "active_attempt"


def test_unavailable_resources_fail_closed_without_parallel_claim() -> None:
    epoch = FrontierPolicy(max_parallelism=4).plan(_state(resources_available=False), epoch_id=2)
    assert epoch.selected_task_ids == ()
    assert epoch.parallelism_hint == 0
    assert len(epoch.unavailable) == 1
    assert epoch.unavailable[0].name == "resources"
    assert all(item.reason == "resources_unavailable" for item in epoch.decisions)


def test_unavailable_cognition_fails_closed_without_parallel_claim() -> None:
    epoch = FrontierPolicy(max_parallelism=4).plan(
        _state(cognition_available=False),
        epoch_id=3,
    )
    assert epoch.selected_task_ids == ()
    assert epoch.parallelism_hint == 0
    assert any(item.name == "agent_cognition" for item in epoch.unavailable)
    assert all(item.reason == "cognition_unavailable" for item in epoch.decisions)


def test_terminal_tasks_are_deferred_even_if_projection_is_malformed() -> None:
    epoch = FrontierPolicy().plan(_state(verified=("a",), invalid=("b",)))
    assert epoch.selected_task_ids == ("c",)
    assert {item.task_id for item in epoch.decisions if item.reason == "terminal_validity"} == {
        "a",
        "b",
    }


def test_input_state_is_not_mutated_and_epoch_metadata_is_transparent() -> None:
    state = _state(ready=("b", "a"))
    before = state.model_dump_json()
    epoch = FrontierPolicy().plan(state, epoch_id=11)
    assert state.model_dump_json() == before
    assert epoch.graph_id == "graph"
    assert epoch.graph_version == 7
    assert epoch.projection_hash == "p" * 64


def test_models_are_frozen_and_reject_extra_fields() -> None:
    epoch = FrontierPolicy().plan(_state(ready=()))
    with pytest.raises(ValidationError):
        epoch.parallelism_hint = 99  # type: ignore[misc]
    with pytest.raises(ValidationError):
        SchedulingEpoch.model_validate(
            {
                **epoch.model_dump(),
                "unexpected": True,
            }
        )
    with pytest.raises(ValidationError):
        epoch.selected_task_ids += ("x",)  # type: ignore[misc]


def test_invalid_policy_and_epoch_ids_are_rejected() -> None:
    with pytest.raises(ValidationError):
        FrontierPolicy(max_parallelism=0)
    with pytest.raises(ValidationError):
        FrontierPolicy.model_validate({"ranking_strategy": "unknown"})
    with pytest.raises(ValueError):
        FrontierPolicy().plan(_state(), epoch_id=-1)
