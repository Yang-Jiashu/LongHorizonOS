"""Focused tests for the bounded online computation control loop."""

from __future__ import annotations

import pytest

from lhos.sdk import Agent, AgentOS, ConfigurationError
from lhos.sdk.computation_control import (
    ControlActionKind,
    ControlLoopStatus,
    DispatchStatus,
    OnlineComputationController,
)
from lhos.sdk.runtime_state import (
    AgentCognitionState,
    CognitionAttemptState,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceRuntimeState,
)
from lhos.sdk.semantic_interrupt import SemanticInterrupt, SemanticInterruptKind


def _state(
    *,
    version: int = 1,
    ready: tuple[str, ...] = ("a", "b"),
    attempts: tuple[CognitionAttemptState, ...] = (),
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
        agent_cognition=AgentCognitionState(available=True, current_attempts=attempts),
        context={"available": True},
        resources=ResourceRuntimeState(available=True, pools=(), active_claims=()),
    )


def _attempt(task_id: str = "a", *, action_epoch: int = 0) -> CognitionAttemptState:
    return CognitionAttemptState(
        claim_id=f"claim-{task_id}",
        claim_state="active",
        task_id=task_id,
        agent_id=f"agent-{task_id}",
        process_id=f"process-{task_id}",
        graph_version=1,
        attempt_id=f"attempt-{task_id}",
        attempt_state="running",
        semantic_epoch=action_epoch,
    )


def _interrupt(
    state: GlobalRuntimeState,
    *,
    task_id: str,
    kind: SemanticInterruptKind,
    interrupt_id: str,
) -> SemanticInterrupt:
    return SemanticInterrupt(
        interrupt_id=interrupt_id,
        graph_id=state.graph_id,
        graph_version=state.progress.graph_version,
        kind=kind,
        reason=f"{kind.value} observed",
        affected_task_ids=(task_id,),
    )


def test_epochs_reobserve_and_change_selected_batch_and_parallelism() -> None:
    states = iter(
        (
            _state(ready=("a", "b")),
            _state(version=2, ready=("b",)),
        )
    )
    seen: list[str] = []

    def dispatcher(action):
        seen.append(action.task_id)
        return {"status": "applied", "message": "accepted"}

    controller = OnlineComputationController(
        state_provider=lambda: next(states),
        max_parallelism=2,
        dispatcher=dispatcher,
    )
    audits = controller.run(max_epochs=2)

    assert len(audits) == 2
    assert audits[0].selected_task_ids == ("a", "b")
    assert audits[0].parallelism_hint == 2
    assert audits[1].selected_task_ids == ("b",)
    assert audits[1].parallelism_hint == 1
    assert audits[0].graph_version == 1
    assert audits[1].graph_version == 2
    assert seen == ["a", "b", "b"]


def test_interrupt_routes_rebase_and_preempt_to_running_attempt() -> None:
    state = _state(attempts=(_attempt("a"),))
    controller = OnlineComputationController(state, max_parallelism=2)

    rebase = controller.step(
        event_proposals=(
            _interrupt(
                state,
                task_id="a",
                kind=SemanticInterruptKind.ARTIFACT_CHANGED,
                interrupt_id="rebase-1",
            ),
        ),
        dispatch=False,
    )
    assert rebase.status is ControlLoopStatus.PLANNED
    affected = [action for action in rebase.actions if action.task_id == "a"]
    assert len(affected) == 1
    assert affected[0].action is ControlActionKind.REBASE
    assert affected[0].target_kind == "attempt"
    assert not any(
        action.task_id == "a"
        and action.action in {ControlActionKind.START, ControlActionKind.CONTINUE}
        for action in rebase.actions
    )
    affected_dispatch = [
        item for item in rebase.dispatches if item.action_id == affected[0].action_id
    ]
    assert affected_dispatch[0].status is DispatchStatus.NOT_DISPATCHED

    # A new input key is required for a second interrupt; the stronger action
    # must be PREEMPT and the old REBASE action must not leak into the result.
    preempt = controller.step(
        event_proposals=(
            _interrupt(
                state,
                task_id="a",
                kind=SemanticInterruptKind.WRITE_CONFLICT,
                interrupt_id="preempt-1",
            ),
        ),
        dispatch=False,
    )
    assert any(
        action.task_id == "a" and action.action is ControlActionKind.PREEMPT
        for action in preempt.actions
    )


def test_agent_os_computation_controller_is_explicit_and_read_only() -> None:
    """The public facade observes a compiled Goal without claiming/executing."""

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker"))
        goal = os_.goal("facade-goal")
        goal.task("task-a", agent="worker")
        graph_id = os_._compile_goal(goal)
        before_claims = tuple(os_.scheduler.claims)

        controller = os_.computation_controller(goal, max_parallelism=2)
        audit = controller.step(dispatch=False)

        assert audit.graph_id == graph_id
        assert audit.selected_task_ids == ("task-a",)
        assert audit.actions[0].action is ControlActionKind.START
        assert audit.dispatches[0].status is DispatchStatus.NOT_DISPATCHED
        assert tuple(os_.scheduler.claims) == before_claims
        assert os_.online_control(goal).last_audit is None
    finally:
        os_.close()


def test_agent_os_computation_controller_requires_compiled_goal() -> None:
    os_ = AgentOS(":memory:")
    try:
        with pytest.raises(ConfigurationError, match="not compiled"):
            os_.computation_controller("missing-goal")
    finally:
        os_.close()


def test_dispatch_is_idempotent_and_start_never_executes_user_code() -> None:
    state = _state(ready=("a",))
    calls: list[ControlActionKind] = []

    def dispatcher(action):
        calls.append(action.action)
        return {"status": "applied"}

    controller = OnlineComputationController(state, dispatcher=dispatcher)
    first = controller.step()
    second = controller.step()

    assert first == second
    assert first.idempotency_key == second.idempotency_key
    assert calls == [ControlActionKind.START]
    assert first.dispatches[0].status is DispatchStatus.APPLIED


def test_reconcile_graph_change_fails_closed_without_dispatch() -> None:
    initial = _state(version=1, ready=("a",))
    dispatched: list[object] = []

    def reconcile(_observed, _events):
        return _state(version=2, ready=("a",))

    controller = OnlineComputationController(
        initial,
        reconcile=reconcile,
        dispatcher=lambda action: dispatched.append(action),
    )
    audit = controller.step()

    assert audit.status is ControlLoopStatus.GRAPH_CHANGED
    assert audit.actions == ()
    assert audit.dispatches == ()
    assert dispatched == []


def test_observation_failure_is_fail_closed() -> None:
    def broken_provider():
        raise RuntimeError("store unavailable")

    controller = OnlineComputationController(state_provider=broken_provider)
    audit = controller.step()

    assert audit.status is ControlLoopStatus.OBSERVATION_FAILED
    assert audit.actions == ()
    assert audit.parallelism_hint == 0


@pytest.mark.asyncio
async def test_async_dispatcher_is_supported_without_background_loop() -> None:
    state = _state(ready=("a",))
    received: list[str] = []

    async def dispatcher(action):
        received.append(action.action_id)
        return {"status": "applied", "message": "async accepted"}

    controller = OnlineComputationController(state, dispatcher=dispatcher)
    audit = await controller.astep()

    assert audit.status is ControlLoopStatus.DISPATCHED
    assert len(received) == 1
    assert audit.dispatches[0].status is DispatchStatus.APPLIED


@pytest.mark.parametrize("bound", (-1, True, 1.5))
def test_run_rejects_invalid_bound(bound) -> None:
    with pytest.raises(ValueError):
        OnlineComputationController(_state()).run(bound)
