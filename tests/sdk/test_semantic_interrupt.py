"""Deterministic semantic-interrupt routing tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    AgentCognitionState,
    GlobalRuntimeState,
    InterruptAction,
    InterruptEpoch,
    ProgressSemanticState,
    ResourceRuntimeState,
    SemanticInterrupt,
    SemanticInterruptKind,
    SemanticInterruptPolicy,
    UnavailableField,
    plan_interrupts,
)


def _state(
    *,
    attempts: tuple[dict[str, object], ...] = (),
    available: bool = True,
    reason: str | None = None,
    ready: tuple[str, ...] = ("leaf",),
    verified: tuple[str, ...] = (),
) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=7,
            projection_hash="p" * 64,
            graph_closed=False,
            goal_closed=False,
            ready_frontier=ready,
            repair_ready_frontier=(),
            verified_task_ids=verified,
            stale_task_ids=(),
            invalid_task_ids=(),
            unverified_task_ids=ready,
        ),
        agent_cognition=AgentCognitionState(
            available=available,
            reason=reason,
            current_attempts=tuple(attempts),
        ),
        context={
            "available": False,
            "reason": "not bound",
            "unavailable": (UnavailableField(name="bindings", reason="not bound"),),
        },
        resources=ResourceRuntimeState(available=True),
    )


def _interrupt(
    kind: SemanticInterruptKind,
    *,
    task_ids: tuple[str, ...] = ("task-a",),
    attempt_ids: tuple[str, ...] = (),
    metadata: dict[str, object] | None = None,
    graph_version: int = 7,
) -> SemanticInterrupt:
    return SemanticInterrupt(
        interrupt_id=f"int-{kind.value}",
        graph_id="graph",
        graph_version=graph_version,
        kind=kind,
        reason=f"{kind.value} observed",
        affected_task_ids=task_ids,
        affected_attempt_ids=attempt_ids,
        metadata=metadata or {},
    )


def _running_attempt(task_id: str = "task-a", attempt_id: str = "attempt-a") -> dict[str, object]:
    return {
        "claim_id": f"claim-{task_id}",
        "claim_state": "active",
        "task_id": task_id,
        "agent_id": "agent",
        "process_id": "process",
        "attempt_id": attempt_id,
        "attempt_state": "running",
    }


def test_artifact_change_routes_running_attempt_to_rebase() -> None:
    epoch = plan_interrupts(
        _state(attempts=(_running_attempt(),)),
        [_interrupt(SemanticInterruptKind.ARTIFACT_CHANGED)],
        epoch_id=3,
    )

    assert epoch.epoch_id == 3
    assert len(epoch.decisions) == 1
    decision = epoch.decisions[0]
    assert decision.target_kind == "attempt"
    assert decision.target_id == "attempt-a"
    assert decision.action is InterruptAction.REBASE
    assert epoch.unhandled_interrupt_ids == ()


def test_write_conflict_preempt_dominates_rebase_for_same_attempt() -> None:
    epoch = SemanticInterruptPolicy().plan(
        _state(attempts=(_running_attempt(),)),
        [
            _interrupt(SemanticInterruptKind.ARTIFACT_CHANGED),
            _interrupt(
                SemanticInterruptKind.WRITE_CONFLICT,
                attempt_ids=("attempt-a",),
                task_ids=(),
            ),
        ],
    )

    assert len(epoch.decisions) == 1
    assert epoch.decisions[0].action is InterruptAction.PREEMPT
    assert len(epoch.decisions[0].interrupt_ids) == 2


def test_resource_pressure_is_cooperative_and_fail_safe() -> None:
    normal = plan_interrupts(
        _state(attempts=(_running_attempt(),)),
        [
            _interrupt(
                SemanticInterruptKind.RESOURCE_PRESSURE,
                metadata={"preemptible": False},
            )
        ],
    )
    assert normal.decisions[0].action is InterruptAction.CONTINUE

    cooperative = plan_interrupts(
        _state(attempts=(_running_attempt(),)),
        [
            _interrupt(
                SemanticInterruptKind.RESOURCE_PRESSURE,
                metadata={"preemptible": True},
            )
        ],
    )
    assert cooperative.decisions[0].action is InterruptAction.PREEMPT


def test_verified_task_interrupt_defers_ready_duplicate() -> None:
    epoch = plan_interrupts(
        _state(ready=("leaf",), verified=("leaf",)),
        [
            _interrupt(
                SemanticInterruptKind.TASK_VERIFIED,
                task_ids=("leaf",),
            )
        ],
    )
    assert epoch.decisions[0].target_kind == "task"
    assert epoch.decisions[0].action is InterruptAction.DEFER


def test_unavailable_cognition_fails_closed() -> None:
    epoch = plan_interrupts(
        _state(
            available=False,
            reason="durable scheduler state unavailable",
            attempts=(_running_attempt(),),
        ),
        [_interrupt(SemanticInterruptKind.ARTIFACT_CHANGED)],
    )

    assert epoch.decisions == ()
    assert epoch.unhandled_interrupt_ids == ("int-artifact_changed",)
    assert epoch.unavailable[0].name == "agent_cognition"


def test_future_graph_interrupt_is_unhandled() -> None:
    epoch = plan_interrupts(
        _state(attempts=(_running_attempt(),)),
        [_interrupt(SemanticInterruptKind.ARTIFACT_CHANGED, graph_version=8)],
    )
    assert epoch.decisions == ()
    assert epoch.unhandled_interrupt_ids == ("int-artifact_changed",)


def test_repeated_plans_are_byte_stable_and_models_are_frozen() -> None:
    state = _state(attempts=(_running_attempt(),))
    interrupt = _interrupt(SemanticInterruptKind.REQUIREMENT_CHANGED)
    first = plan_interrupts(state, [interrupt], epoch_id=9)
    second = plan_interrupts(state, [interrupt], epoch_id=9)
    assert first == second
    assert first.model_dump_json() == second.model_dump_json()
    assert first.decision_hash == second.decision_hash
    with pytest.raises(ValidationError):
        first.epoch_id = 4  # type: ignore[misc]
    with pytest.raises(ValidationError):
        InterruptEpoch.model_validate({**first.model_dump(), "extra": True})


def test_source_task_shorthand_is_normalized() -> None:
    interrupt = SemanticInterrupt(
        interrupt_id="i",
        graph_id="graph",
        graph_version=1,
        kind=SemanticInterruptKind.AGENT_STALLED,
        reason="stalled",
        source_task_id="task-a",
    )
    assert interrupt.affected_task_ids == ("task-a",)
