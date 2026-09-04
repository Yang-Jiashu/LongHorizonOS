"""Focused tests for the explicit event-driven scheduling epoch controller."""

from __future__ import annotations

import pytest

from lhos.sdk import (
    AgentCognitionState,
    ConflictGraph,
    EpochController,
    EpochDecisionStatus,
    GlobalRuntimeState,
    InterruptAction,
    ProgressSemanticState,
    ResourceRuntimeState,
    SemanticInterrupt,
    SemanticInterruptKind,
    TaskAccessSet,
)


def _state(
    *,
    version: int = 1,
    ready: tuple[str, ...] = ("a", "b"),
    attempts: tuple = (),
) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=version,
            projection_hash=f"{version:064d}",
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
            current_attempts=attempts,
        ),
        context={"available": True},
        resources=ResourceRuntimeState(
            available=True,
            pools=(),
            active_claims=(),
        ),
    )


def _interrupt(state: GlobalRuntimeState, *, task_id: str = "a") -> SemanticInterrupt:
    return SemanticInterrupt(
        interrupt_id="i-a",
        graph_id=state.graph_id,
        graph_version=state.progress.graph_version,
        kind=SemanticInterruptKind.REQUIREMENT_CHANGED,
        reason="requirement changed",
        affected_task_ids=(task_id,),
    )


def test_step_runs_observe_reconcile_plan_in_order_and_selects_frontier() -> None:
    state = _state()
    phases: list[str] = []

    def reconcile(observed, proposals):
        phases.append("reconcile")
        assert observed is state
        assert proposals == ()
        return observed

    controller = EpochController(state, reconcile=reconcile, max_parallelism=2)
    decision = controller.step()

    assert phases == ["reconcile"]
    assert decision.status is EpochDecisionStatus.PLANNED
    assert decision.selected_task_ids == ("a", "b")
    assert decision.phases == ("observe", "reconcile", "plan")


def test_run_is_off_by_default_but_step_is_explicit() -> None:
    controller = EpochController(_state())
    assert controller.run(max_epochs=3) == ()
    assert controller.step().status is EpochDecisionStatus.PLANNED


def test_empty_frontier_is_reported_without_selection() -> None:
    decision = EpochController(_state(ready=())).step()
    assert decision.status is EpochDecisionStatus.EMPTY_FRONTIER
    assert decision.selected_task_ids == ()
    assert decision.parallelism_hint == 0


def test_graph_version_change_during_reconcile_fails_closed() -> None:
    initial = _state(version=1)

    def reconcile(_state, _events):
        return _state_fn(version=2)

    # Keep construction in a helper so the callback does not accidentally
    # mutate the original immutable projection.
    def _state_fn(*, version: int):
        return _state(version=version)

    decision = EpochController(initial, reconcile=reconcile).step()
    assert decision.status is EpochDecisionStatus.GRAPH_CHANGED
    assert decision.selected_task_ids == ()
    assert "graph version changed" in (decision.reason or "")


def test_interrupt_proposal_blocks_affected_task_and_is_deterministic() -> None:
    state = _state()
    controller = EpochController(state, max_parallelism=2)
    proposal = _interrupt(state)
    first = controller.step(event_proposals=(proposal,))
    second = controller.step(event_proposals=(proposal,))

    assert first == second
    assert first.decision_hash == second.decision_hash
    assert first.interrupts is not None
    # A ready (not currently running) task is conservatively deferred; a
    # running attempt would receive REBASE from the same policy.
    assert first.interrupts.decisions[0].action is InterruptAction.DEFER
    assert first.selected_task_ids == ("b",)
    assert first.interrupt_blocked_task_ids == ("a",)


def test_conflict_graph_is_used_for_parallel_batch_planning() -> None:
    state = _state()
    graph = ConflictGraph.from_access_sets(
        [
            TaskAccessSet(task_id="a", write_set=("workspace://shared",)),
            TaskAccessSet(task_id="b", write_set=("workspace://shared",)),
        ]
    )
    decision = EpochController(
        state,
        conflict_graph=graph,
        max_parallelism=2,
    ).step()
    assert decision.status is EpochDecisionStatus.PLANNED
    assert decision.selected_task_ids == ("a",)
    assert decision.deferred_task_ids == ("b",)


def test_run_respects_bounded_max_epochs() -> None:
    states = iter((_state(ready=("a",)), _state(ready=("b",))))
    controller = EpochController(
        state_provider=lambda: next(states),
        enabled=True,
    )
    decisions = controller.run(max_epochs=1)
    assert len(decisions) == 1
    assert decisions[0].selected_task_ids == ("a",)


@pytest.mark.parametrize("bad", [-1, True, 1.5])
def test_run_rejects_invalid_epoch_bound(bad) -> None:
    with pytest.raises(Exception):
        EpochController(_state()).run(max_epochs=bad)
