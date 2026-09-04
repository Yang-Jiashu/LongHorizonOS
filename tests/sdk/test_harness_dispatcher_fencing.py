"""Action-level fencing tests for the online Harness dispatcher."""

from __future__ import annotations

import asyncio

import pytest

from lhos.sdk.computation_control import (
    ActionDispatchResult,
    ComputationAction,
    ControlActionKind,
    DispatchStatus,
    make_harness_dispatcher,
)
from lhos.sdk.harness import (
    CallableHarnessAdapter,
    HarnessHookOutcome,
    HarnessOperation,
    HarnessSessionIdentity,
)


def _identity() -> HarnessSessionIdentity:
    return HarnessSessionIdentity(
        session_id="session-fence",
        graph_id="graph-fence",
        graph_version=4,
        semantic_epoch=2,
        task_id="task-fence",
        agent_id="agent-fence",
        claim_id="claim-fence",
        attempt_id="attempt-fence",
    )


def _action(
    identity: HarnessSessionIdentity,
    *,
    kind: ControlActionKind = ControlActionKind.CONTINUE,
    operation: HarnessOperation | None = None,
    graph_version: int | None = None,
    semantic_epoch: int | None = None,
    **updates: object,
) -> ComputationAction:
    operation = operation or {
        ControlActionKind.START: HarnessOperation.START,
        ControlActionKind.CONTINUE: HarnessOperation.CONTINUE,
        ControlActionKind.REBASE: HarnessOperation.REBASE,
        ControlActionKind.PREEMPT: HarnessOperation.PREEMPT,
    }.get(kind)
    raw = {
        "graph_id": identity.graph_id,
        "graph_version": identity.graph_version if graph_version is None else graph_version,
        "task_id": identity.task_id,
        "agent_id": identity.agent_id,
        "claim_id": identity.claim_id,
        "attempt_id": identity.attempt_id,
        "semantic_epoch": identity.semantic_epoch if semantic_epoch is None else semantic_epoch,
    }
    raw.update(updates)
    return ComputationAction(
        action_id="a" * 64,
        request_id="b" * 64,
        epoch_id=0,
        target_kind="attempt",
        target_id=identity.attempt_id,
        action=kind,
        harness_operation=operation,
        reason="fencing test",
        source_decision_hash="c" * 64,
        **raw,
    )


def test_valid_action_fence_enters_harness_and_start_may_run_hook() -> None:
    calls: list[HarnessOperation] = []

    def start(request, snapshot):
        calls.append(request.operation)
        return HarnessHookOutcome(progress=0.2)

    harness = CallableHarnessAdapter(_identity(), start=start)
    dispatcher = make_harness_dispatcher({harness.snapshot.identity.attempt_id: harness})
    action = _action(
        harness.snapshot.identity,
        kind=ControlActionKind.START,
        operation=HarnessOperation.START,
    )
    result = asyncio.run(dispatcher(action))

    assert isinstance(result, ActionDispatchResult)
    assert result.status is DispatchStatus.APPLIED
    assert calls == [HarnessOperation.START]
    assert harness.snapshot.progress == 0.2


def test_start_without_owner_fences_is_rejected_before_session_lookup() -> None:
    calls = 0

    def start(request, snapshot):
        nonlocal calls
        calls += 1
        return HarnessHookOutcome()

    harness = CallableHarnessAdapter(_identity(), start=start)
    dispatcher = make_harness_dispatcher({harness.snapshot.identity.task_id: harness})
    action = _action(
        harness.snapshot.identity,
        kind=ControlActionKind.START,
        operation=HarnessOperation.START,
        agent_id=None,
        claim_id=None,
        attempt_id=None,
        semantic_epoch=None,
    )
    result = asyncio.run(dispatcher(action))

    assert result.status is DispatchStatus.REJECTED
    assert "agent_id" in result.message
    assert calls == 0


def test_ambiguous_task_aliases_reject_instead_of_guessing_owner() -> None:
    first = CallableHarnessAdapter(
        _identity(), start=lambda _request, _snapshot: HarnessHookOutcome()
    )
    second_identity = _identity().model_copy(
        update={
            "session_id": "session-fence-2",
            "agent_id": "agent-fence-2",
            "claim_id": "claim-fence-2",
            "attempt_id": "attempt-fence-2",
        }
    )
    second = CallableHarnessAdapter(
        second_identity,
        start=lambda _request, _snapshot: HarnessHookOutcome(),
    )
    dispatcher = make_harness_dispatcher(
        {
            first.snapshot.identity.task_id: first,
            second.snapshot.identity.attempt_id: second,
        }
    )
    action = _action(
        first.snapshot.identity,
        kind=ControlActionKind.START,
        operation=HarnessOperation.START,
    )
    # Deliberately point at an unregistered attempt so task fallback sees two
    # possible owners and must fail closed.
    action = action.model_copy(update={"attempt_id": "missing-attempt"})
    result = asyncio.run(dispatcher(action))

    assert result.status is DispatchStatus.REJECTED
    assert "ambiguous" in result.message


@pytest.mark.parametrize(
    ("field", "value", "needle"),
    (
        ("graph_id", "other-graph", "graph_id"),
        ("graph_version", 3, "graph_version"),
        ("task_id", "other-task", "task_id"),
        ("agent_id", "other-agent", "agent_id"),
        ("claim_id", "other-claim", "claim_id"),
        ("attempt_id", "other-attempt", "attempt_id"),
        ("semantic_epoch", 1, "semantic_epoch"),
    ),
)
def test_action_fence_rejects_every_stale_identity_before_hook(
    field: str,
    value: object,
    needle: str,
) -> None:
    calls = 0

    def start(request, snapshot):
        nonlocal calls
        calls += 1
        return HarnessHookOutcome()

    harness = CallableHarnessAdapter(_identity(), start=start)
    dispatcher = make_harness_dispatcher({harness.snapshot.identity.attempt_id: harness})
    action = _action(
        harness.snapshot.identity,
        kind=ControlActionKind.START,
        operation=HarnessOperation.START,
        **{field: value},
    )
    result = asyncio.run(dispatcher(action))

    assert result.status is DispatchStatus.REJECTED
    assert needle in result.message
    assert calls == 0
    assert harness.snapshot.revision == 0


def test_rebase_requires_current_epoch_and_advances_target_epoch() -> None:
    calls: list[HarnessOperation] = []

    def start(request, snapshot):
        calls.append(request.operation)
        return HarnessHookOutcome()

    def rebase(request, snapshot):
        calls.append(request.operation)
        return HarnessHookOutcome(progress=0.7)

    harness = CallableHarnessAdapter(
        _identity(),
        start=start,
        rebase=rebase,
    )
    harness.control_sync(harness.make_request(HarnessOperation.START))
    dispatcher = make_harness_dispatcher({harness.snapshot.identity.attempt_id: harness})
    action = _action(
        harness.snapshot.identity,
        kind=ControlActionKind.REBASE,
        operation=HarnessOperation.REBASE,
        graph_version=5,
    )
    result = asyncio.run(dispatcher(action))

    assert result.status is DispatchStatus.APPLIED
    assert calls == [HarnessOperation.START, HarnessOperation.REBASE]
    assert harness.snapshot.identity.graph_version == 5
    assert harness.snapshot.identity.semantic_epoch == 3
